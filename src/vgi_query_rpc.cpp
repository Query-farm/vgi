// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_query_rpc.hpp"

#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_attach_parameters.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_settings_defaults.hpp"
#include "vgi_sha256.hpp"
#include "generated/vgi_protocol_schemas.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/exception.hpp"
#include <arrow/util/byte_size.h>
#include <chrono>
#include <set>

namespace duckdb {
namespace vgi {
namespace {

void Require(bool condition, const std::string &message) {
	if (!condition) throw InvalidInputException("VGI query protocol: %s", message);
}

void Text(const std::string &value, const char *name, size_t maximum = 4096, bool allow_empty = false) {
	Require((allow_empty || !value.empty()) && value.size() <= maximum && value.find('\0') == std::string::npos,
	        std::string("invalid ") + name);
}

void Token(const std::vector<uint8_t> &value, const char *name) {
	Require(!value.empty() && value.size() <= VGI_QUERY_MAX_TOKEN_BYTES, std::string("invalid ") + name);
}

void Context(const VgiQueryEvaluationContext &context) {
	Text(context.timezone, "timezone", 256);
	Text(context.collation, "collation", 256);
	Require(context.default_order == "asc" || context.default_order == "desc", "unknown default order");
	const std::set<std::string> null_orders {"nulls_first", "nulls_last", "nulls_first_on_asc_last_on_desc",
	                                        "nulls_last_on_asc_first_on_desc"};
	Require(null_orders.count(context.default_null_order) != 0, "unknown default NULL order");
}

void Capabilities(const VgiQueryCapabilities &capabilities) {
	Require(capabilities.encoding == VGI_QUERY_ENCODING &&
	            capabilities.semantic_profiles == std::vector<std::string> {VGI_QUERY_EXPLICIT_PROFILE} &&
	            capabilities.execution_modes == std::vector<std::string> {"explicit"},
	        "worker selected an unoffered query encoding, profile, or mode");
	Text(capabilities.backend_engine, "backend engine");
	Text(capabilities.backend_version, "backend version");
	Text(capabilities.capability_revision, "capability revision");
	Require(capabilities.max_query_bytes > 0 && capabilities.max_query_bytes <= VGI_QUERY_MAX_SQL_BYTES,
	        "invalid selected maximum SQL size");
	Context(capabilities.default_evaluation_context);
}

void Ordering(const std::string &ordering) {
	Require(ordering == "unspecified" || ordering == "query_order", "unknown result ordering");
}

void StrictBatch(const std::shared_ptr<arrow::RecordBatch> &batch, const std::shared_ptr<arrow::Schema> &schema) {
	Require(batch && batch->num_rows() == 1 && batch->schema()->Equals(*schema, false), "record schema or row count mismatch");
	Require(batch->ValidateFull().ok(), "invalid Arrow record buffers");
	Require(arrow::util::TotalBufferSize(*batch) <= VGI_QUERY_MAX_RECORD_BYTES, "query record is too large");
	for (int i = 0; i < schema->num_fields(); ++i) {
		Require(schema->field(i)->nullable() || !batch->column(i)->IsNull(0), "required query field is NULL");
	}
}

std::shared_ptr<arrow::RecordBatch> StructBatch(const std::shared_ptr<arrow::Array> &array,
                                             const std::shared_ptr<arrow::Schema> &schema) {
	Require(array && array->length() == 1 && array->type()->Equals(arrow::struct_(schema->fields())) && !array->IsNull(0),
	        "invalid inline query record");
	auto fields = std::static_pointer_cast<arrow::StructArray>(array);
	std::vector<std::shared_ptr<arrow::Array>> columns;
	for (int i = 0; i < schema->num_fields(); ++i) columns.push_back(fields->field(i));
	auto batch = arrow::RecordBatch::Make(schema, 1, std::move(columns));
	StrictBatch(batch, schema);
	return batch;
}

std::shared_ptr<arrow::Array> Struct(const std::shared_ptr<arrow::Schema> &schema,
                                   const std::vector<std::shared_ptr<arrow::Array>> &columns) {
	auto result = arrow::StructArray::Make(columns, schema->fields());
	Require(result.ok(), "could not construct inline query record");
	return result.ValueOrDie();
}

std::shared_ptr<arrow::Array> EvaluationContext(const VgiQueryEvaluationContext &context) {
	Context(context);
	return Struct(generated::QueryEvaluationContextSchema(),
	              {BuildStringScalar(context.timezone), BuildStringScalar(context.collation),
	               BuildStringScalar(context.default_order), BuildStringScalar(context.default_null_order),
	               BuildBoolScalar(context.preserve_insertion_order)});
}

void Metadata(const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) {
	if (!metadata) return;
	std::set<std::string> names;
	for (int64_t i = 0; i < metadata->size(); ++i) {
		Require(names.insert(metadata->key(i)).second, "duplicate schema or field metadata key");
	}
}

void FieldMetadata(const std::shared_ptr<arrow::Field> &field, size_t depth) {
	Require(depth <= 64, "query schema nesting is too deep");
	Metadata(field->metadata());
	for (const auto &child : field->type()->fields()) FieldMetadata(child, depth + 1);
	if (field->type()->id() == arrow::Type::DICTIONARY) {
		auto type = std::static_pointer_cast<arrow::DictionaryType>(field->type());
		FieldMetadata(arrow::field("dictionary", type->value_type()), depth + 1);
	}
}

std::shared_ptr<arrow::Schema> ReadSchema(const std::vector<uint8_t> &bytes) {
	Require(!bytes.empty() && bytes.size() <= VGI_QUERY_MAX_SCHEMA_BYTES, "invalid query schema size");
	arrow::io::BufferReader reader(arrow::Buffer::Wrap(bytes.data(), bytes.size()));
	arrow::ipc::DictionaryMemo memo;
	auto result = arrow::ipc::ReadSchema(&reader, &memo);
	Require(result.ok(), "invalid query schema IPC");
	auto position = reader.Tell();
	Require(position.ok() && position.ValueOrDie() == static_cast<int64_t>(bytes.size()), "query schema has trailing data");
	auto schema = result.ValueOrDie();
	Metadata(schema->metadata());
	for (const auto &field : schema->fields()) FieldMetadata(field, 0);
	return schema;
}

void Parameters(const std::shared_ptr<arrow::Schema> &schema) {
	Require(schema && schema->num_fields() <= 1024, "too many query parameters");
	Metadata(schema->metadata());
	Require(schema->metadata() != nullptr, "query parameter mapping metadata is missing");
	auto metadata = schema->metadata();
	auto index = metadata->FindKey("vgi.query.parameters.v1");
	Require(index >= 0, "query parameter mapping metadata is missing");
	auto mode = metadata->value(index);
	Require(mode == "named" || mode == "positional", "invalid query parameter mapping mode");
	std::set<std::string> names;
	for (int i = 0; i < schema->num_fields(); ++i) {
		auto field = schema->field(i);
		Text(field->name(), "query parameter name");
		Require(names.insert(field->name()).second, "duplicate query parameter name");
		if (mode == "positional") Require(field->name() == std::to_string(i + 1), "positional names must be 1 through N");
		else {
			Require(std::string("$:@?").find(field->name()[0]) == std::string::npos &&
			            field->name().find_first_not_of("0123456789") != std::string::npos,
			        "named parameters must use unprefixed, nonnumeric names");
		}
		FieldMetadata(field, 0);
	}
}

} // namespace

bool IsVgiQueryEnabled(ClientContext &context) {
	Value value;
	return context.TryGetCurrentSetting("vgi_query_enabled", value) && !value.IsNull()
	           ? value.GetValue<bool>() : defaults::QUERY_ENABLED;
}

int64_t QueryEpochMillisNow() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::vector<uint8_t> QueryEvaluationContextDigest(const VgiQueryEvaluationContext &context) {
	Context(context);
	std::string bytes("vgi.query.context.v1\0", 21);
	for (const auto &value : std::vector<std::string> {VGI_QUERY_ENCODING, VGI_QUERY_EXPLICIT_PROFILE, context.timezone,
	                                                context.collation, context.default_order, context.default_null_order}) {
		auto size = static_cast<uint32_t>(value.size());
		for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<char>((size >> shift) & 0xff));
		bytes += value;
	}
	bytes.push_back(context.preserve_insertion_order ? 1 : 0);
	auto digest = VgiSha256Raw(bytes);
	Require(digest.size() == 32, "SHA256 is unavailable");
	return std::vector<uint8_t>(digest.begin(), digest.end());
}

std::shared_ptr<arrow::Array> BuildClientQueryCapabilities() {
	return Struct(generated::ClientQueryCapabilitiesSchema(),
	              {BuildStringListScalar({VGI_QUERY_ENCODING}), BuildStringListScalar({VGI_QUERY_EXPLICIT_PROFILE}),
	               BuildStringScalar(DuckDB::LibraryVersion()), BuildInt64Scalar(VGI_QUERY_MAX_SQL_BYTES)});
}

std::optional<VgiQueryCapabilities> ParseVgiQueryCapabilities(const std::shared_ptr<arrow::Array> &array,
                                                            bool offered, const std::string &worker_path) {
	if (!array) return std::nullopt;
	Require(array->length() == 1 && array->type()->Equals(arrow::struct_(generated::QueryCapabilitiesSchema()->fields())),
	        "invalid query capability field shape");
	if (array->IsNull(0)) return std::nullopt;
	Require(offered, "worker selected query support without a client offer");
	auto batch = StructBatch(array, generated::QueryCapabilitiesSchema());
	// The generic catalog accessor skips NULL list entries. A negotiation must
	// retain the exact wire selection, including rejecting malformed elements.
	for (const auto *name : {"semantic_profiles", "execution_modes"}) {
		auto list = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName(name));
		Require(list->value_length(0) == 1 && !list->values()->IsNull(list->value_offset(0)),
		        "query agreement requires one non-NULL explicit profile and mode");
	}
	RecordBatchSingleRow row(batch, 0, "QueryCapabilities", worker_path);
	VgiQueryCapabilities capabilities;
	capabilities.encoding = row["encoding"].value_not_null<std::string>();
	capabilities.semantic_profiles = row["semantic_profiles"].value_not_null<std::vector<std::string>>();
	capabilities.execution_modes = row["execution_modes"].value_not_null<std::vector<std::string>>();
	capabilities.backend_engine = row["backend_engine"].value_not_null<std::string>();
	capabilities.backend_version = row["backend_version"].value_not_null<std::string>();
	capabilities.capability_revision = row["capability_revision"].value_not_null<std::string>();
	capabilities.max_query_bytes = row["max_query_bytes"].value_not_null<int64_t>();
	auto context_batch = StructBatch(batch->GetColumnByName("default_evaluation_context"), generated::QueryEvaluationContextSchema());
	RecordBatchSingleRow context(context_batch, 0, "QueryEvaluationContext", worker_path);
	auto &selected = capabilities.default_evaluation_context;
	selected.timezone = context["timezone"].value_not_null<std::string>();
	selected.collation = context["collation"].value_not_null<std::string>();
	selected.default_order = context["default_order"].value_not_null<std::string>();
	selected.default_null_order = context["default_null_order"].value_not_null<std::string>();
	selected.preserve_insertion_order = context["preserve_insertion_order"].value_not_null<bool>();
	Capabilities(capabilities);
	return capabilities;
}

std::shared_ptr<arrow::RecordBatch> BuildVgiQueryPrepareRequest(
    const CatalogRpcContext &ctx, const VgiQueryCapabilities &capabilities, const std::string &sql,
    const std::shared_ptr<arrow::Schema> &parameter_schema, const std::string &required_ordering) {
	Capabilities(capabilities);
	Ordering(required_ordering);
	Token(ctx.attach_opaque_data, "query attachment");
	if (!ctx.transaction_opaque_data.empty()) Token(ctx.transaction_opaque_data, "query transaction");
	Text(sql, "SQL query", capabilities.max_query_bytes);
	Parameters(parameter_schema);
	auto parameter_bytes = SerializeSchemaToIpcBytes(parameter_schema);
	Require(parameter_bytes.size() <= VGI_QUERY_MAX_SCHEMA_BYTES, "query parameter schema is too large");
	auto schema = generated::CatalogQueryPrepareRequestSchema();
	std::unique_ptr<arrow::ArrayBuilder> references;
	auto status = arrow::MakeBuilder(arrow::default_memory_pool(), schema->field(11)->type(), &references);
	Require(status.ok(), "could not construct empty query object manifest");
	Require(static_cast<arrow::ListBuilder *>(references.get())->Append().ok(), "could not append query object manifest");
	std::shared_ptr<arrow::Array> reference_array;
	Require(references->Finish(&reference_array).ok(), "could not finish query object manifest");
	auto transaction = ctx.transaction_opaque_data.empty() ? std::nullopt
	                    : std::optional<std::vector<uint8_t>>(ctx.transaction_opaque_data);
	auto batch = arrow::RecordBatch::Make(schema, 1,
	    {BuildBinaryScalarRequired(ctx.attach_opaque_data), BuildStringScalar(capabilities.encoding),
	     BuildStringScalar(VGI_QUERY_EXPLICIT_PROFILE), BuildStringScalar("explicit"),
	     BuildStringScalar(capabilities.capability_revision), BuildStringScalar(sql), BuildBinaryScalarRequired(parameter_bytes),
	     EvaluationContext(capabilities.default_evaluation_context), BuildOptionalBinaryScalar(transaction),
	     BuildOptionalInt64Scalar(std::nullopt), BuildOptionalStringScalar(std::nullopt), reference_array,
	     BuildOptionalBinaryScalar(std::nullopt), BuildStringScalar(required_ordering)});
	StrictBatch(batch, schema);
	return batch;
}

std::shared_ptr<arrow::RecordBatch> DeserializeVgiQueryRecord(const std::vector<uint8_t> &bytes) {
	Require(!bytes.empty() && bytes.size() <= VGI_QUERY_MAX_RECORD_BYTES, "invalid query record size");
	auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromVector(bytes));
	auto opened = arrow::ipc::RecordBatchStreamReader::Open(source);
	Require(opened.ok(), "invalid query record IPC");
	auto reader = opened.ValueOrDie();
	std::shared_ptr<arrow::RecordBatch> batch, extra;
	Require(reader->ReadNext(&batch).ok() && batch != nullptr, "query record has no batch");
	Require(reader->ReadNext(&extra).ok() && extra == nullptr, "query record must contain exactly one batch");
	auto position = source->Tell();
	Require(position.ok() && position.ValueOrDie() == static_cast<int64_t>(bytes.size()), "query record has trailing data");
	return batch;
}

VgiQueryPrepareResult ParseVgiQueryPrepareResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                              const VgiQueryCapabilities &capabilities,
                                              const std::string &required_ordering, int64_t now_epoch_ms,
                                              const std::string &worker_path) {
	Capabilities(capabilities);
	Ordering(required_ordering);
	StrictBatch(batch, generated::CatalogQueryPrepareResultSchema());
	RecordBatchSingleRow row(batch, 0, "CatalogQueryPrepareResult", worker_path);
	VgiQueryPrepareResult result;
	auto status = row["status"].value_not_null<std::string>();
	if (status == "declined") {
		result.reason_code = row["reason_code"].value_not_null<std::string>();
		Text(result.reason_code, "query decline reason", 64);
		Require(result.reason_code[0] >= 'a' && result.reason_code[0] <= 'z' &&
		            result.reason_code.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") == std::string::npos,
		        "invalid query decline reason code");
		result.reason_detail = row["reason_detail"].value_or("");
		Text(result.reason_detail, "query decline detail", 4096, true);
		for (int i = 3; i < batch->num_columns(); ++i) Require(batch->column(i)->IsNull(0), "declined query contains acceptance fields");
		return result;
	}
	Require(status == "accepted", "unknown preparation status");
	Require(row["reason_code"].is_null() && row["reason_detail"].is_null(), "accepted query contains decline reasons");
	result.prepared_query_token = row["prepared_query_token"].value_not_null<std::vector<uint8_t>>();
	Token(result.prepared_query_token, "prepared query token");
	result.output_schema_ipc = row["output_schema"].value_not_null<std::vector<uint8_t>>();
	result.output_schema = ReadSchema(result.output_schema_ipc);
	result.result_ordering = row["result_ordering"].value_not_null<std::string>();
	Ordering(result.result_ordering);
	Require(required_ordering != "query_order" || result.result_ordering == "query_order", "query preparation lost required ordering");
	auto digest = row["context_digest"].value_not_null<std::vector<uint8_t>>();
	Require(digest == QueryEvaluationContextDigest(capabilities.default_evaluation_context), "query context digest mismatch");
	result.expires_at_epoch_ms = row["expires_at_epoch_ms"].value_not_null<int64_t>();
	Require(now_epoch_ms >= 0 && result.expires_at_epoch_ms > now_epoch_ms, "query preparation is expired");
	Require(result.expires_at_epoch_ms - now_epoch_ms <= 60 * 60 * 1000,
	        "query preparation lifetime exceeds one hour");
	result.validated_catalog_version = row["validated_catalog_version"].as<int64_t>();
	result.validated_catalog_etag = row["validated_catalog_etag"].as<std::string>();
	result.estimated_rows = row["estimated_rows"].as<int64_t>();
	result.estimated_bytes = row["estimated_bytes"].as<int64_t>();
	for (auto value : {result.validated_catalog_version, result.estimated_rows, result.estimated_bytes}) {
		Require(!value || *value >= 0, "negative query version or estimate");
	}
	if (result.validated_catalog_etag) Text(*result.validated_catalog_etag, "validated catalog etag", 4096, true);
	result.accepted = true;
	return result;
}

} // namespace vgi
} // namespace duckdb
