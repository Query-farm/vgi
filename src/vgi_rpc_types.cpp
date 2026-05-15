#include "vgi_rpc_types.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_schema_registry.hpp"

// Generated request builders — exposes ``duckdb::vgi::generated::Build<Name>Params(...)``
// definitions that mirror the schemas in ``generated/vgi_protocol_schemas.hpp``.
// Including here is the only TU that pulls them in; the file is header-only
// inline functions, so ODR is satisfied by the single inclusion.
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Helper to check Arrow status and throw on failure
void CheckStatus(const arrow::Status &status, const char *operation) {
	if (!status.ok()) {
		throw IOException("Arrow %s failed: %s", operation, status.ToString());
	}
}

// Helper to finalize a builder
template <typename BuilderType>
std::shared_ptr<arrow::Array> FinishArray(BuilderType &builder, const char *name) {
	auto result = builder.Finish();
	if (!result.ok()) {
		throw IOException("Failed to finish Arrow builder for %s: %s", name, result.status().ToString());
	}
	return result.ValueUnsafe();
}

// Helper to build a null dictionary(int16, utf8) array
std::shared_ptr<arrow::Array> BuildNullDictionaryArray(
    const std::shared_ptr<arrow::DataType> &dict_type,
    const std::vector<std::string> &dictionary_values) {
	arrow::Int16Builder index_builder;
	CheckStatus(index_builder.AppendNull(), "append null dict index");
	auto index_arr = FinishArray(index_builder, "null_dict_index");
	arrow::StringBuilder dict_builder;
	for (const auto &v : dictionary_values) {
		CheckStatus(dict_builder.Append(v), "append dict value");
	}
	auto dict_arr = FinishArray(dict_builder, "dict_values");
	auto result = arrow::DictionaryArray::FromArrays(dict_type, index_arr, dict_arr);
	if (!result.ok()) {
		throw IOException("Failed to create null dictionary array: " + result.status().ToString());
	}
	return result.ValueUnsafe();
}

} // namespace

// ============================================================================
// Single-row builders (declared in vgi_rpc_types.hpp).
// ============================================================================
//
// "Required" non-Optional helpers always emit a non-null entry; the existing
// "empty == null" helpers (BuildBinaryScalar, BuildNullableStringScalar) are
// retained inside this TU for the legacy hand-coded BuildXxxParams that still
// rely on the empty-vector-as-null convention. The Optional helpers below are
// what the codegen calls into.

std::shared_ptr<arrow::Array> BuildBinaryScalarRequired(const std::vector<uint8_t> &value) {
	arrow::BinaryBuilder builder;
	CheckStatus(builder.Append(value.data(), value.size()), "append binary required");
	return FinishArray(builder, "binary");
}

std::shared_ptr<arrow::Array> BuildOptionalBinaryScalar(const std::optional<std::vector<uint8_t>> &value) {
	arrow::BinaryBuilder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null binary");
	} else {
		CheckStatus(builder.Append(value->data(), value->size()), "append binary");
	}
	return FinishArray(builder, "optional_binary");
}

std::shared_ptr<arrow::Array> BuildStringScalar(const std::string &value) {
	arrow::StringBuilder builder;
	CheckStatus(builder.Append(value), "append string");
	return FinishArray(builder, "string");
}

std::shared_ptr<arrow::Array> BuildOptionalStringScalar(const std::optional<std::string> &value) {
	arrow::StringBuilder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null string");
	} else {
		CheckStatus(builder.Append(*value), "append string");
	}
	return FinishArray(builder, "optional_string");
}

std::shared_ptr<arrow::Array> BuildBoolScalar(bool value) {
	arrow::BooleanBuilder builder;
	CheckStatus(builder.Append(value), "append bool");
	return FinishArray(builder, "bool");
}

std::shared_ptr<arrow::Array> BuildOptionalBoolScalar(std::optional<bool> value) {
	arrow::BooleanBuilder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null bool");
	} else {
		CheckStatus(builder.Append(*value), "append bool");
	}
	return FinishArray(builder, "optional_bool");
}

std::shared_ptr<arrow::Array> BuildInt32Scalar(int32_t value) {
	arrow::Int32Builder builder;
	CheckStatus(builder.Append(value), "append int32");
	return FinishArray(builder, "int32");
}

std::shared_ptr<arrow::Array> BuildOptionalInt32Scalar(std::optional<int32_t> value) {
	arrow::Int32Builder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null int32");
	} else {
		CheckStatus(builder.Append(*value), "append int32");
	}
	return FinishArray(builder, "optional_int32");
}

std::shared_ptr<arrow::Array> BuildInt64Scalar(int64_t value) {
	arrow::Int64Builder builder;
	CheckStatus(builder.Append(value), "append int64");
	return FinishArray(builder, "int64");
}

std::shared_ptr<arrow::Array> BuildOptionalInt64Scalar(std::optional<int64_t> value) {
	arrow::Int64Builder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null int64");
	} else {
		CheckStatus(builder.Append(*value), "append int64");
	}
	return FinishArray(builder, "optional_int64");
}

std::shared_ptr<arrow::Array> BuildOptionalEnumArray(const std::optional<std::string> &value,
                                                      const std::vector<std::string> &dictionary_values) {
	if (!value.has_value()) {
		auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
		return BuildNullDictionaryArray(dict_type, dictionary_values);
	}
	return BuildEnumArray(*value, dictionary_values);
}

namespace {
// "Empty == null" legacy variants used by hand-coded BuildXxxParams in the
// Complex bucket and a handful of older call sites. New code goes through
// the explicit-optional helpers above.
std::shared_ptr<arrow::Array> BuildBinaryScalar(const std::vector<uint8_t> &bytes) {
	arrow::BinaryBuilder builder;
	if (bytes.empty()) {
		CheckStatus(builder.AppendNull(), "append null binary");
	} else {
		CheckStatus(builder.Append(bytes.data(), bytes.size()), "append binary");
	}
	return FinishArray(builder, "binary");
}

std::shared_ptr<arrow::Array> BuildNullableStringScalar(const std::string &value) {
	arrow::StringBuilder builder;
	if (value.empty()) {
		CheckStatus(builder.AppendNull(), "append null string");
	} else {
		CheckStatus(builder.Append(value), "append string");
	}
	return FinishArray(builder, "nullable_string");
}
} // namespace

// list<T> single-row builders.

std::shared_ptr<arrow::Array> BuildStringListScalar(const std::vector<std::string> &values) {
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start string list");
	for (const auto &v : values) {
		CheckStatus(value_builder->Append(v), "append string item");
	}
	return FinishArray(list_builder, "string_list");
}

std::shared_ptr<arrow::Array> BuildBinaryListScalar(const std::vector<std::vector<uint8_t>> &values) {
	auto value_builder = std::make_shared<arrow::BinaryBuilder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start binary list");
	for (const auto &v : values) {
		CheckStatus(value_builder->Append(v.data(), v.size()), "append binary item");
	}
	return FinishArray(list_builder, "binary_list");
}

std::shared_ptr<arrow::Array> BuildInt32ListScalar(const std::vector<int32_t> &values) {
	auto value_builder = std::make_shared<arrow::Int32Builder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start int32 list");
	for (auto v : values) {
		CheckStatus(value_builder->Append(v), "append int32 item");
	}
	return FinishArray(list_builder, "int32_list");
}

std::shared_ptr<arrow::Array> BuildInt64ListScalar(const std::vector<int64_t> &values) {
	auto value_builder = std::make_shared<arrow::Int64Builder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start int64 list");
	for (auto v : values) {
		CheckStatus(value_builder->Append(v), "append int64 item");
	}
	return FinishArray(list_builder, "int64_list");
}

// map<utf8, utf8> single-row builders.

std::shared_ptr<arrow::Array>
BuildStringMapScalar(const std::vector<std::pair<std::string, std::string>> &entries) {
	auto key_builder = std::make_shared<arrow::StringBuilder>();
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
	CheckStatus(builder.Append(), "start string map");
	for (const auto &[k, v] : entries) {
		CheckStatus(key_builder->Append(k), "append map key");
		CheckStatus(value_builder->Append(v), "append map value");
	}
	return FinishArray(builder, "string_map");
}

std::shared_ptr<arrow::Array>
BuildOptionalStringMapScalar(const std::optional<std::vector<std::pair<std::string, std::string>>> &entries) {
	auto key_builder = std::make_shared<arrow::StringBuilder>();
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
	if (!entries.has_value()) {
		CheckStatus(builder.AppendNull(), "append null map");
	} else {
		CheckStatus(builder.Append(), "start string map");
		for (const auto &[k, v] : *entries) {
			CheckStatus(key_builder->Append(k), "append map key");
			CheckStatus(value_builder->Append(v), "append map value");
		}
	}
	return FinishArray(builder, "optional_string_map");
}

// ============================================================================
// IPC Bytes Serialization
// ============================================================================

std::vector<uint8_t> SerializeToIpcBytes(const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw IOException("Failed to create buffer: " + sink_result.status().ToString());
	}
	auto sink = sink_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(sink, batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create IPC writer: " + writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	CheckStatus(writer->WriteRecordBatch(*batch), "write batch to IPC");
	CheckStatus(writer->Close(), "close IPC writer");

	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish IPC buffer: " + finish_result.status().ToString());
	}
	auto buffer = finish_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const uint8_t *data, size_t len) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for IPC deserialization: %s",
		                  alloc_result.status().ToString());
	}
	auto buffer = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(buffer->data()), data, len);
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> batch;
	auto status = reader->ReadNext(&batch);
	if (!status.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", status.ToString());
	}
	return batch;
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes) {
	return DeserializeFromIpcBytes(bytes.data(), bytes.size());
}

DeserializedBatch DeserializeFromIpcBytesWithMetadata(const uint8_t *data, size_t len) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for IPC deserialization: %s",
		                  alloc_result.status().ToString());
	}
	auto buffer = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(buffer->data()), data, len);
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// ReadNext() returns RecordBatchWithMetadata including custom metadata
	auto result = reader->ReadNext();
	if (!result.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", result.status().ToString());
	}
	auto bwm = result.ValueUnsafe();
	return {bwm.batch, bwm.custom_metadata};
}

std::vector<uint8_t> SerializeSchemaToIpcBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto serialize_result = arrow::ipc::SerializeSchema(*schema);
	if (!serialize_result.ok()) {
		throw IOException("Failed to serialize schema: %s", serialize_result.status().ToString());
	}
	auto buffer = serialize_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

std::vector<uint8_t> SerializeForeignKeyToIpcBytes(const std::vector<std::string> &fk_columns,
                                                    const std::vector<std::string> &pk_columns,
                                                    const std::string &referenced_table,
                                                    const std::string &referenced_schema) {
	// Build a single-row batch matching the Python FK format:
	// fk_columns: list<utf8>, pk_columns: list<utf8>, referenced_table: utf8, referenced_schema: utf8
	auto fk_schema = arrow::schema({
	    arrow::field("fk_columns", arrow::list(arrow::utf8())),
	    arrow::field("pk_columns", arrow::list(arrow::utf8())),
	    arrow::field("referenced_table", arrow::utf8()),
	    arrow::field("referenced_schema", arrow::utf8()),
	});

	// Build fk_columns array
	auto fk_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
	                                                        std::make_shared<arrow::StringBuilder>());
	auto fk_val_builder = dynamic_cast<arrow::StringBuilder *>(fk_builder->value_builder());
	CheckStatus(fk_builder->Append(), "fk list append");
	for (auto &col : fk_columns) {
		CheckStatus(fk_val_builder->Append(col), "fk col append");
	}
	auto fk_arr_result = fk_builder->Finish();
	CheckStatus(fk_arr_result.status(), "fk finish");

	// Build pk_columns array
	auto pk_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
	                                                        std::make_shared<arrow::StringBuilder>());
	auto pk_val_builder = dynamic_cast<arrow::StringBuilder *>(pk_builder->value_builder());
	CheckStatus(pk_builder->Append(), "pk list append");
	for (auto &col : pk_columns) {
		CheckStatus(pk_val_builder->Append(col), "pk col append");
	}
	auto pk_arr_result = pk_builder->Finish();
	CheckStatus(pk_arr_result.status(), "pk finish");

	// Build scalar string arrays
	arrow::StringBuilder ref_table_builder;
	CheckStatus(ref_table_builder.Append(referenced_table), "ref table");
	auto ref_table_result = ref_table_builder.Finish();
	CheckStatus(ref_table_result.status(), "ref table finish");

	arrow::StringBuilder ref_schema_builder;
	CheckStatus(ref_schema_builder.Append(referenced_schema), "ref schema");
	auto ref_schema_result = ref_schema_builder.Finish();
	CheckStatus(ref_schema_result.status(), "ref schema finish");

	auto batch = arrow::RecordBatch::Make(fk_schema, 1,
	    {fk_arr_result.ValueUnsafe(), pk_arr_result.ValueUnsafe(),
	     ref_table_result.ValueUnsafe(), ref_schema_result.ValueUnsafe()});
	return SerializeToIpcBytes(batch);
}

// ============================================================================
// Enum Serialization
// ============================================================================

std::shared_ptr<arrow::Array> BuildEnumArray(const std::string &value,
                                             const std::vector<std::string> &dictionary_values) {
	// Build the dictionary (all enum member names)
	arrow::StringBuilder dict_builder;
	for (const auto &v : dictionary_values) {
		CheckStatus(dict_builder.Append(v), "append enum dict value");
	}
	auto dict_result = dict_builder.Finish();
	if (!dict_result.ok()) {
		throw IOException("Failed to build enum dictionary: " + dict_result.status().ToString());
	}
	auto dictionary = dict_result.ValueUnsafe();

	// Find the index of the value in the dictionary
	int16_t index = -1;
	for (size_t i = 0; i < dictionary_values.size(); i++) {
		if (dictionary_values[i] == value) {
			index = static_cast<int16_t>(i);
			break;
		}
	}
	if (index < 0) {
		throw IOException("Enum value '%s' not found in dictionary", value);
	}

	// Build the index array
	arrow::Int16Builder index_builder;
	CheckStatus(index_builder.Append(index), "append enum index");
	auto index_result = index_builder.Finish();
	if (!index_result.ok()) {
		throw IOException("Failed to build enum index: " + index_result.status().ToString());
	}

	// Create the dictionary array
	auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
	auto dict_array_result = arrow::DictionaryArray::FromArrays(dict_type, index_result.ValueUnsafe(), dictionary);
	if (!dict_array_result.ok()) {
		throw IOException("Failed to create enum array: " + dict_array_result.status().ToString());
	}
	return dict_array_result.ValueUnsafe();
}

// ============================================================================
// BindRequest
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildBindRequest(const std::string &function_name, const std::vector<uint8_t> &arguments_ipc_bytes,
                 const std::string &function_type, const std::vector<uint8_t> &input_schema_bytes,
                 const std::vector<uint8_t> &settings_bytes, const std::vector<uint8_t> &secrets_bytes,
                 const std::vector<uint8_t> &attach_opaque_data, const std::vector<uint8_t> &transaction_opaque_data,
                 bool resolved_secrets_provided) {
	// FunctionType enum: SCALAR, TABLE, AGGREGATE
	static const std::vector<std::string> function_type_values = {"SCALAR", "TABLE", "AGGREGATE"};

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("function_type", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("input_schema", arrow::binary(), true),
	    arrow::field("settings", arrow::binary(), true),
	    arrow::field("secrets", arrow::binary(), true),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	    arrow::field("transaction_opaque_data", arrow::binary(), true),
	    arrow::field("resolved_secrets_provided", arrow::boolean(), false),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildStringScalar(function_name));
	arrays.push_back(BuildBinaryScalar(arguments_ipc_bytes));
	arrays.push_back(BuildEnumArray(function_type, function_type_values));
	arrays.push_back(BuildBinaryScalar(input_schema_bytes));
	arrays.push_back(BuildBinaryScalar(settings_bytes));
	arrays.push_back(BuildBinaryScalar(secrets_bytes));
	arrays.push_back(BuildBinaryScalar(attach_opaque_data));
	arrays.push_back(BuildBinaryScalar(transaction_opaque_data));

	// resolved_secrets_provided: bool
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(resolved_secrets_provided), "append resolved_secrets_provided");
		arrays.push_back(FinishArray(builder, "resolved_secrets_provided"));
	}

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// BindResponse Parsing
// ============================================================================

BindResponseResult ParseBindResponse(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	BindResponseResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty BindResponse from worker [worker: %s]", worker_path);
	}

	// output_schema: binary (required)
	auto schema_col = batch->GetColumnByName("output_schema");
	if (!schema_col) {
		throw IOException("BindResponse missing output_schema [worker: %s]", worker_path);
	}
	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(schema_col);
	if (!binary_array || binary_array->IsNull(0)) {
		throw IOException("BindResponse output_schema is null [worker: %s]", worker_path);
	}
	auto view = binary_array->GetView(0);
	auto schema_buffer = arrow::Buffer::Wrap(view.data(), view.size());
	arrow::io::BufferReader reader(schema_buffer);
	arrow::ipc::DictionaryMemo dict_memo;
	auto schema_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to deserialize output schema: %s [worker: %s]", schema_result.status().ToString(),
		                  worker_path);
	}
	result.output_schema = schema_result.ValueUnsafe();

	// opaque_data: binary (nullable)
	auto opaque_col = batch->GetColumnByName("opaque_data");
	if (opaque_col) {
		auto opaque_array = std::dynamic_pointer_cast<arrow::BinaryArray>(opaque_col);
		if (opaque_array && !opaque_array->IsNull(0)) {
			auto opaque_view = opaque_array->GetView(0);
			result.opaque_data.assign(opaque_view.data(), opaque_view.data() + opaque_view.size());
		}
	}

	return result;
}

// ============================================================================
// BindSecretScopeResponse Parsing
// ============================================================================

std::optional<BindSecretScopeResponseResult> TryParseBindSecretScopeResponse(
    const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!batch || batch->num_rows() == 0) {
		return std::nullopt;
	}

	// Check for lookup_secret_types column — if present and non-empty, this is a scope request
	auto types_col = batch->GetColumnByName("lookup_secret_types");
	if (!types_col) {
		return std::nullopt;
	}

	auto types_list = std::dynamic_pointer_cast<arrow::ListArray>(types_col);
	if (!types_list || types_list->IsNull(0)) {
		return std::nullopt;
	}

	int64_t start = types_list->value_offset(0);
	int64_t end = types_list->value_offset(1);
	int64_t num_lookups = end - start;

	if (num_lookups == 0) {
		return std::nullopt; // Empty list — not a scope request
	}

	auto types_values = std::dynamic_pointer_cast<arrow::StringArray>(types_list->values());
	if (!types_values) {
		return std::nullopt;
	}

	// Parse scopes and names (parallel lists)
	auto scopes_col = batch->GetColumnByName("lookup_scopes");
	auto names_col = batch->GetColumnByName("lookup_names");

	auto scopes_list = scopes_col ? std::dynamic_pointer_cast<arrow::ListArray>(scopes_col) : nullptr;
	auto names_list = names_col ? std::dynamic_pointer_cast<arrow::ListArray>(names_col) : nullptr;

	std::shared_ptr<arrow::StringArray> scopes_values;
	std::shared_ptr<arrow::StringArray> names_values;
	if (scopes_list && !scopes_list->IsNull(0)) {
		scopes_values = std::dynamic_pointer_cast<arrow::StringArray>(scopes_list->values());
	}
	if (names_list && !names_list->IsNull(0)) {
		names_values = std::dynamic_pointer_cast<arrow::StringArray>(names_list->values());
	}

	BindSecretScopeResponseResult result;
	for (int64_t i = start; i < end; i++) {
		BindSecretScopeResponseResult::Lookup lookup;
		lookup.secret_type = types_values->GetString(i);
		if (scopes_values && i < scopes_values->length() && !scopes_values->IsNull(i)) {
			lookup.scope = scopes_values->GetString(i);
		}
		if (names_values && i < names_values->length() && !names_values->IsNull(i)) {
			lookup.name = names_values->GetString(i);
		}
		result.lookups.push_back(std::move(lookup));
	}

	return result;
}

// ============================================================================
// InitRequest
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildInitRequest(const std::vector<uint8_t> &bind_call_bytes, const std::vector<uint8_t> &output_schema_bytes,
                 const std::vector<uint8_t> &bind_opaque_data, const std::vector<int64_t> &projection_ids,
                 std::shared_ptr<arrow::Buffer> pushdown_filters,
                 std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
                 const std::string &phase,
                 const std::vector<uint8_t> &execution_id, const std::vector<uint8_t> &init_opaque_data,
                 const std::string &order_by_column_name, const std::string &order_by_direction,
                 const std::string &order_by_null_order, int64_t order_by_limit,
                 double tablesample_percentage, int64_t tablesample_seed,
                 const std::optional<int64_t> &finalize_state_id) {
	static const std::vector<std::string> phase_values = {
	    "INPUT", "FINALIZE", "BUFFERED_TABLE", "BUFFERED_TABLE_FINALIZE",
	};

	auto phase_type = arrow::dictionary(arrow::int16(), arrow::utf8());

	auto order_direction_type = arrow::dictionary(arrow::int16(), arrow::utf8());
	auto order_null_order_type = arrow::dictionary(arrow::int16(), arrow::utf8());

	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	    arrow::field("projection_ids", arrow::list(arrow::int64()), true),
	    arrow::field("pushdown_filters", arrow::large_binary(), true),
	    arrow::field("join_keys", arrow::list(arrow::large_binary()), true),
	    arrow::field("phase", phase_type, true),
	    arrow::field("execution_id", arrow::binary(), true),
	    arrow::field("init_opaque_data", arrow::binary(), true),
	    arrow::field("order_by_column_name", arrow::utf8(), true),
	    arrow::field("order_by_direction", order_direction_type, true),
	    arrow::field("order_by_null_order", order_null_order_type, true),
	    arrow::field("order_by_limit", arrow::int64(), true),
	    arrow::field("tablesample_percentage", arrow::float64(), true),
	    arrow::field("tablesample_seed", arrow::int64(), true),
	    arrow::field("finalize_state_id", arrow::int64(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;

	// bind_call: binary (required)
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));

	// output_schema: binary (required)
	arrays.push_back(BuildBinaryScalar(output_schema_bytes));

	// bind_opaque_data: binary|null
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));

	// projection_ids: list<int64>|null
	{
		auto value_builder = std::make_shared<arrow::Int64Builder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		if (projection_ids.empty()) {
			CheckStatus(list_builder.AppendNull(), "append null projection_ids");
		} else {
			CheckStatus(list_builder.Append(), "start projection_ids list");
			for (int64_t id : projection_ids) {
				CheckStatus(value_builder->Append(id), "append projection id");
			}
		}
		arrays.push_back(FinishArray(list_builder, "projection_ids"));
	}

	// pushdown_filters: large_binary|null (zero-copy from arrow::Buffer, int64 offsets)
	{
		arrow::LargeBinaryBuilder builder;
		if (!pushdown_filters || pushdown_filters->size() == 0) {
			CheckStatus(builder.AppendNull(), "append null pushdown_filters");
		} else {
			CheckStatus(builder.Append(pushdown_filters->data(), static_cast<int64_t>(pushdown_filters->size())),
			            "append pushdown_filters");
		}
		arrays.push_back(FinishArray(builder, "pushdown_filters"));
	}

	// join_keys: list<large_binary>|null (one entry per IN filter column)
	{
		auto value_builder = std::make_shared<arrow::LargeBinaryBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		if (join_keys.empty()) {
			CheckStatus(list_builder.AppendNull(), "append null join_keys");
		} else {
			CheckStatus(list_builder.Append(), "start join_keys list");
			for (auto &buf : join_keys) {
				CheckStatus(value_builder->Append(buf->data(), static_cast<int64_t>(buf->size())),
				            "append join_keys buffer");
			}
		}
		arrays.push_back(FinishArray(list_builder, "join_keys"));
	}

	// phase: dictionary(int16, utf8)|null
	if (phase.empty()) {
		arrays.push_back(BuildNullDictionaryArray(phase_type, phase_values));
	} else {
		arrays.push_back(BuildEnumArray(phase, phase_values));
	}

	// execution_id: binary|null
	arrays.push_back(BuildBinaryScalar(execution_id));

	// init_opaque_data: binary|null
	arrays.push_back(BuildBinaryScalar(init_opaque_data));

	// order_by_column_name: utf8|null
	arrays.push_back(BuildNullableStringScalar(order_by_column_name));

	// order_by_direction: dictionary(int16, utf8)|null — "ASC" or "DESC"
	{
		static const std::vector<std::string> direction_values = {"ASC", "DESC"};
		if (order_by_direction.empty()) {
			arrays.push_back(BuildNullDictionaryArray(order_direction_type, direction_values));
		} else {
			arrays.push_back(BuildEnumArray(order_by_direction, direction_values));
		}
	}

	// order_by_null_order: dictionary(int16, utf8)|null — "NULLS_FIRST" or "NULLS_LAST"
	{
		static const std::vector<std::string> null_order_values = {"NULLS_FIRST", "NULLS_LAST"};
		if (order_by_null_order.empty()) {
			arrays.push_back(BuildNullDictionaryArray(order_null_order_type, null_order_values));
		} else {
			arrays.push_back(BuildEnumArray(order_by_null_order, null_order_values));
		}
	}

	// order_by_limit: int64|null — combined limit+offset, -1 = null
	{
		arrow::Int64Builder builder;
		if (order_by_limit < 0) {
			CheckStatus(builder.AppendNull(), "append null order_by_limit");
		} else {
			CheckStatus(builder.Append(order_by_limit), "append order_by_limit");
		}
		arrays.push_back(FinishArray(builder, "order_by_limit"));
	}

	// tablesample_percentage: float64|null — -1.0 = null (no sample)
	{
		arrow::DoubleBuilder builder;
		if (tablesample_percentage < 0.0) {
			CheckStatus(builder.AppendNull(), "append null tablesample_percentage");
		} else {
			CheckStatus(builder.Append(tablesample_percentage), "append tablesample_percentage");
		}
		arrays.push_back(FinishArray(builder, "tablesample_percentage"));
	}

	// tablesample_seed: int64|null — -1 = null (no seed)
	{
		arrow::Int64Builder builder;
		if (tablesample_seed < 0) {
			CheckStatus(builder.AppendNull(), "append null tablesample_seed");
		} else {
			CheckStatus(builder.Append(tablesample_seed), "append tablesample_seed");
		}
		arrays.push_back(FinishArray(builder, "tablesample_seed"));
	}

	// finalize_state_id: int64|null — required for phase=BUFFERED_TABLE_FINALIZE
	{
		arrow::Int64Builder builder;
		if (finalize_state_id.has_value()) {
			CheckStatus(builder.Append(*finalize_state_id), "append finalize_state_id");
		} else {
			CheckStatus(builder.AppendNull(), "append null finalize_state_id");
		}
		arrays.push_back(FinishArray(builder, "finalize_state_id"));
	}

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// GlobalInitResponse Parsing
// ============================================================================

GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                 const std::string &worker_path) {
	GlobalInitResponseResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty GlobalInitResponse from worker [worker: %s]", worker_path);
	}

	// execution_id: binary (required)
	auto exec_col = batch->GetColumnByName("execution_id");
	if (exec_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(exec_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			result.execution_id.assign(view.data(), view.data() + view.size());
		}
	}

	// max_workers: int64 (default 1)
	auto workers_col = batch->GetColumnByName("max_workers");
	if (workers_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(workers_col);
		if (int_array && !int_array->IsNull(0)) {
			result.max_workers = int_array->Value(0);
		}
	}
	if (result.max_workers <= 0) {
		result.max_workers = 1;
	}

	// opaque_data: binary|null
	auto opaque_col = batch->GetColumnByName("opaque_data");
	if (opaque_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(opaque_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			result.opaque_data.assign(view.data(), view.data() + view.size());
		}
	}

	return result;
}

// ============================================================================
// Response Item Unwrapping
// ============================================================================

std::vector<std::vector<uint8_t>> UnwrapBinaryResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch) {
	std::vector<std::vector<uint8_t>> items;

	if (!batch || batch->num_rows() == 0) {
		return items;
	}

	auto list_array = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName("items"));
	if (list_array->IsNull(0)) {
		return items;
	}

	auto start = list_array->value_offset(0);
	auto end = list_array->value_offset(1);
	auto values = std::static_pointer_cast<arrow::BinaryArray>(list_array->values());

	for (int64_t i = start; i < end; i++) {
		if (!values->IsNull(i)) {
			auto view = values->GetView(i);
			items.emplace_back(view.data(), view.data() + view.size());
		}
	}

	return items;
}

std::vector<std::shared_ptr<arrow::RecordBatch>>
UnwrapAndValidateItems(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &method_name,
                       const std::string &worker_path) {
	auto bytes_list = UnwrapBinaryResponseItems(batch);
	std::vector<std::shared_ptr<arrow::RecordBatch>> items;
	items.reserve(bytes_list.size());
	for (size_t i = 0; i < bytes_list.size(); i++) {
		auto info_batch = DeserializeFromIpcBytes(bytes_list[i]);
		if (!info_batch || info_batch->num_rows() == 0) {
			continue;
		}
		ValidateItemSchema(info_batch, method_name, worker_path, i);
		items.push_back(std::move(info_batch));
	}
	return items;
}

std::vector<std::string> UnwrapStringResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch) {
	std::vector<std::string> items;

	if (!batch || batch->num_rows() == 0) {
		return items;
	}

	auto list_array = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName("items"));
	if (list_array->IsNull(0)) {
		return items;
	}

	auto start = list_array->value_offset(0);
	auto end = list_array->value_offset(1);
	auto values = std::static_pointer_cast<arrow::StringArray>(list_array->values());

	for (int64_t i = start; i < end; i++) {
		if (!values->IsNull(i)) {
			items.push_back(values->GetString(i));
		}
	}

	return items;
}

// ============================================================================
// TableFunctionCardinalityRequest / TableCardinality
// ============================================================================

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionCardinalityRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                         const std::vector<uint8_t> &bind_opaque_data) {
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionStatisticsRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                        const std::vector<uint8_t> &bind_opaque_data) {
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// TableFunctionDynamicToStringRequest / Response
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildTableFunctionDynamicToStringRequest(const std::vector<uint8_t> &bind_call_bytes,
                                         const std::vector<uint8_t> &bind_opaque_data,
                                         const std::vector<uint8_t> &global_execution_id) {
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	    arrow::field("global_execution_id", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));
	arrays.push_back(BuildBinaryScalarRequired(global_execution_id));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

InsertionOrderPreservingMap<std::string>
ParseTableFunctionDynamicToStringResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                        const std::string &worker_path) {
	(void)worker_path; // for parity with sibling parsers; reserved for richer error context
	InsertionOrderPreservingMap<std::string> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}
	auto keys_col = std::dynamic_pointer_cast<arrow::ListArray>(batch->GetColumnByName("keys"));
	auto values_col = std::dynamic_pointer_cast<arrow::ListArray>(batch->GetColumnByName("values"));
	if (!keys_col || !values_col || keys_col->IsNull(0) || values_col->IsNull(0)) {
		return result;
	}
	auto keys_strings = std::dynamic_pointer_cast<arrow::StringArray>(keys_col->values());
	auto values_strings = std::dynamic_pointer_cast<arrow::StringArray>(values_col->values());
	if (!keys_strings || !values_strings) {
		return result;
	}
	auto k_off = keys_col->value_offset(0);
	auto k_len = keys_col->value_length(0);
	auto v_off = values_col->value_offset(0);
	auto v_len = values_col->value_length(0);
	auto pairs = std::min(k_len, v_len);
	for (int64_t i = 0; i < pairs; ++i) {
		if (keys_strings->IsNull(k_off + i) || values_strings->IsNull(v_off + i)) {
			continue;
		}
		result.insert(keys_strings->GetString(k_off + i), values_strings->GetString(v_off + i));
	}
	return result;
}

TableFunctionCardinalityResult ParseTableFunctionCardinalityResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                                   const std::string &worker_path) {
	TableFunctionCardinalityResult result;

	if (!batch || batch->num_rows() == 0) {
		return result; // Unknown cardinality
	}

	// estimate: int64|null
	auto estimate_col = batch->GetColumnByName("estimate");
	if (estimate_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(estimate_col);
		if (int_array && !int_array->IsNull(0)) {
			result.estimate = int_array->Value(0);
		}
	}

	// max: int64|null
	auto max_col = batch->GetColumnByName("max");
	if (max_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(max_col);
		if (int_array && !int_array->IsNull(0)) {
			result.max = int_array->Value(0);
		}
	}

	return result;
}

// ============================================================================
// Inner request builders (Complex bucket — kept hand-coded)
// ============================================================================

std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(
    const std::string &name,
    const std::vector<uint8_t> &options_ipc_bytes,
    const std::string &data_version_spec,
    const std::string &implementation_version) {
	// Matches vgi-python's CatalogAttachRequest (vgi/protocol.py:193). The
	// pyarrow-inferred wire schema marks `options` as not null (even though
	// the dataclass defaults it to None) and `data_version_spec` /
	// `implementation_version` as nullable. Empty caller-supplied strings
	// must be encoded as null — the worker treats None as "unconstrained",
	// while "" is a concrete (and invalid) version string.
	auto request_schema = arrow::schema({
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("options", arrow::binary(), false),
	    arrow::field("data_version_spec", arrow::utf8(), true),
	    arrow::field("implementation_version", arrow::utf8(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> request_arrays;
	request_arrays.push_back(BuildStringScalar(name));
	request_arrays.push_back(BuildBinaryScalar(options_ipc_bytes));
	request_arrays.push_back(BuildNullableStringScalar(data_version_spec));
	request_arrays.push_back(BuildNullableStringScalar(implementation_version));
	return arrow::RecordBatch::Make(request_schema, 1, request_arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_opaque_data, const std::string &schema_name, const std::string &name,
    const std::shared_ptr<arrow::Schema> &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints, const std::vector<uint8_t> &transaction_opaque_data) {
	static const std::vector<std::string> on_conflict_values = {"ERROR", "IGNORE", "REPLACE"};

	// Serialize the columns schema to IPC bytes
	auto columns_bytes = SerializeSchemaToIpcBytes(columns_schema);

	auto batch_schema = arrow::schema({
	    arrow::field("attach_opaque_data", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("columns", arrow::binary(), false),
	    arrow::field("on_conflict", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("not_null_constraints", arrow::list(arrow::int32()), false),
	    arrow::field("unique_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("check_constraints", arrow::list(arrow::utf8()), false),
	    arrow::field("primary_key_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("foreign_key_constraints", arrow::list(arrow::binary()), false),
	    arrow::field("transaction_opaque_data", arrow::binary(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;

	// attach_opaque_data: binary (required)
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_opaque_data.data(), attach_opaque_data.size()), "append attach_opaque_data");
		arrays.push_back(FinishArray(builder, "attach_opaque_data"));
	}

	// schema_name: utf8
	arrays.push_back(BuildStringScalar(schema_name));

	// name: utf8
	arrays.push_back(BuildStringScalar(name));

	// columns: binary (serialized Arrow schema)
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(columns_bytes.data(), columns_bytes.size()), "append columns");
		arrays.push_back(FinishArray(builder, "columns"));
	}

	// on_conflict: dictionary(int16, utf8)
	arrays.push_back(BuildEnumArray(on_conflict, on_conflict_values));

	// not_null_constraints: list<int32> — always non-null (empty list for no constraints)
	{
		auto value_builder = std::make_shared<arrow::Int32Builder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "start not_null_constraints list");
		for (int idx : not_null_constraints) {
			CheckStatus(value_builder->Append(static_cast<int32_t>(idx)), "append not_null index");
		}
		arrays.push_back(FinishArray(list_builder, "not_null_constraints"));
	}

	// unique_constraints: list<list<int32>> — always non-null
	{
		auto inner_value_builder = std::make_shared<arrow::Int32Builder>();
		auto inner_list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), inner_value_builder);
		arrow::ListBuilder outer_list_builder(arrow::default_memory_pool(), inner_list_builder);
		CheckStatus(outer_list_builder.Append(), "start unique_constraints outer list");
		for (const auto &constraint : unique_constraints) {
			CheckStatus(inner_list_builder->Append(), "start unique_constraints inner list");
			for (int idx : constraint) {
				CheckStatus(inner_value_builder->Append(static_cast<int32_t>(idx)), "append unique index");
			}
		}
		arrays.push_back(FinishArray(outer_list_builder, "unique_constraints"));
	}

	// check_constraints: list<utf8> — always non-null
	{
		auto value_builder = std::make_shared<arrow::StringBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "start check_constraints list");
		for (const auto &expr : check_constraints) {
			CheckStatus(value_builder->Append(expr), "append check constraint");
		}
		arrays.push_back(FinishArray(list_builder, "check_constraints"));
	}

	// primary_key_constraints: list<list<int32>> — always non-null
	{
		auto inner_value_builder = std::make_shared<arrow::Int32Builder>();
		auto inner_list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), inner_value_builder);
		arrow::ListBuilder outer_list_builder(arrow::default_memory_pool(), inner_list_builder);
		CheckStatus(outer_list_builder.Append(), "start primary_key_constraints outer list");
		for (const auto &constraint : primary_key_constraints) {
			CheckStatus(inner_list_builder->Append(), "start primary_key_constraints inner list");
			for (int idx : constraint) {
				CheckStatus(inner_value_builder->Append(static_cast<int32_t>(idx)), "append pk index");
			}
		}
		arrays.push_back(FinishArray(outer_list_builder, "primary_key_constraints"));
	}

	// foreign_key_constraints: list<binary> — always non-null
	{
		auto value_builder = std::make_shared<arrow::BinaryBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "start foreign_key_constraints list");
		for (const auto &fk_bytes : foreign_key_constraints) {
			CheckStatus(value_builder->Append(fk_bytes.data(), fk_bytes.size()), "append fk bytes");
		}
		arrays.push_back(FinishArray(list_builder, "foreign_key_constraints"));
	}

	// transaction_opaque_data: binary (nullable)
	arrays.push_back(BuildBinaryScalar(transaction_opaque_data));

	return arrow::RecordBatch::Make(batch_schema, 1, arrays);
}

} // namespace vgi
} // namespace duckdb
