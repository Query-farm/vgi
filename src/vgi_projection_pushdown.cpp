// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_projection_pushdown.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_query_function.hpp"
#include "vgi_logging.hpp"
#include "vgi_table_function_impl.hpp"
#include "storage/vgi_table_entry.hpp"
#include "generated/vgi_protocol_schemas.hpp"
#include "generated/vgi_request_builders.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <arrow/io/memory.h>
#include <arrow/util/byte_size.h>
#include <arrow/ipc/reader.h>
#include <set>

namespace duckdb {
namespace vgi {
namespace {
constexpr size_t MAX_PROJECTION_BYTES = 8 * 1024 * 1024;
void Require(bool condition, const char *message) {
	if (!condition)
		throw IOException("VGI expression projection: %s", message);
}
void Record(const std::shared_ptr<arrow::RecordBatch> &batch, const std::shared_ptr<arrow::Schema> &schema) {
	Require(batch && batch->num_rows() == 1, "expected one record");
	ValidateVgiQueryResultBatch(batch, schema);
	Require(arrow::util::TotalBufferSize(*batch) <= MAX_PROJECTION_BYTES, "decoded record exceeds 8 MiB");
}
std::vector<uint8_t> Bytes(const std::shared_ptr<arrow::RecordBatch> &batch, const char *name) {
	auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName(name));
	Require(array && !array->IsNull(0), "required binary field is NULL");
	auto view = array->GetView(0);
	Require(!view.empty() && view.size() <= MAX_PROJECTION_BYTES, "invalid binary record size");
	return {view.begin(), view.end()};
}
std::shared_ptr<arrow::Schema> Schema(const std::vector<uint8_t> &bytes) {
	arrow::io::BufferReader reader(arrow::Buffer::Wrap(bytes.data(), bytes.size()));
	arrow::ipc::DictionaryMemo memo;
	auto result = arrow::ipc::ReadSchema(&reader, &memo);
	Require(result.ok(), "invalid Arrow schema");
	auto offset = reader.Tell();
	Require(offset.ok() && offset.ValueOrDie() >= 0 && offset.ValueOrDie() <= static_cast<int64_t>(bytes.size()),
	        "invalid Arrow schema offset");
	// Arrow schema-only streams may include one modern or legacy EOS marker.
	// No second message, repeated EOS or arbitrary trailing bytes are permitted.
	auto tail = std::vector<uint8_t>(bytes.begin() + offset.ValueOrDie(), bytes.end());
	Require(tail.empty() || tail == std::vector<uint8_t>({255, 255, 255, 255, 0, 0, 0, 0}) ||
	            tail == std::vector<uint8_t>({0, 0, 0, 0}),
	        "trailing Arrow schema bytes");
	return result.ValueOrDie();
}
std::shared_ptr<arrow::RecordBatch> Bind(const std::vector<uint8_t> &bytes) {
	auto batch = DeserializeVgiQueryRecord(bytes);
	Record(batch, generated::BindResultSchema());
	for (auto name : {"lookup_secret_types", "lookup_scopes", "lookup_names"}) {
		auto list = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName(name));
		Require(list->value_length(0) == 0, "projection bind cannot request secret lookup");
	}
	return batch;
}
bool SupportedSource(const VgiTableFunctionBindData &data) {
	return data.table_entry && data.projection_response_bytes.empty() && !data.explicit_query &&
	       data.rowid_worker_col_index < 0 && data.partition_kind == VgiPartitionKind::NotPartitioned &&
	       data.table_entry->GetColumns().LogicalColumnCount() ==
	           data.table_entry->GetColumns().PhysicalColumnCount() &&
	       data.table_entry->Cast<VgiTableEntry>().GetTableInfo().required_filters.empty() &&
	       !data.schema_name.empty() && !data.bind_result.bind_response_bytes.empty();
}
void RestrictAcceptedScan(TableFunction &function, VgiTableFunctionBindData &data) {
	data.supports_splits = false;
	data.partition_kind = VgiPartitionKind::NotPartitioned;
	data.partition_column_indices.clear();
	data.order_by_hint.reset();
	data.table_sample_hint.reset();
	function.filter_pushdown = false;
	function.pushdown_expression = nullptr;
	function.pushdown_complex_filter = nullptr;
	function.supports_pushdown_type = nullptr;
	function.supports_pushdown_extract = nullptr;
	function.set_scan_order = nullptr;
	function.statistics = nullptr;
	function.statistics_extended = nullptr;
	function.get_partition_info = nullptr;
	function.get_partition_data = nullptr;
	function.get_partition_stats = nullptr;
	function.late_materialization = false;
	function.sampling_pushdown = false;
}
void Apply(ClientContext &context, VgiTableFunctionBindData &data, TableFunction &function,
           const std::vector<uint8_t> &request, const std::vector<uint8_t> &response,
           const ProjectionAcceptance &accepted) {
	auto parsed = ParseBindResponse(accepted.bind_result);
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	ArrowSchemaToDuckDBTypes(context, accepted.output_schema, c_schema, arrow_table, types, names);
	// All decoding and exact-schema checks completed before altering the bind.
	data.bind_result.output_schema = accepted.output_schema;
	data.bind_result.output_schema_bytes = SerializeSchemaToIpcBytes(accepted.output_schema);
	data.bind_result.opaque_data = std::move(parsed.opaque_data);
	data.bind_result.repeatability = parsed.repeatability;
	data.bind_result.bind_response_bytes = SerializeToIpcBytes(accepted.bind_result);
	std::swap(data.c_schema.arrow_schema, c_schema.arrow_schema);
	data.arrow_table = std::move(arrow_table);
	data.all_column_types = std::move(types);
	data.all_column_names = std::move(names);
	data.projection_request_bytes = request;
	data.projection_response_bytes = response;
	RestrictAcceptedScan(function, data);
}
class ProjectionOptimizer : public OptimizerExtension {
public:
	ProjectionOptimizer() {
		pre_optimize_function = Optimize;
	}

private:
	static void Walk(OptimizerExtensionInput &input, LogicalOperator &op) {
		for (auto &child : op.children)
			Walk(input, *child);
		if (op.type != LogicalOperatorType::LOGICAL_PROJECTION || op.children.size() != 1)
			return;
		auto &projection = op.Cast<LogicalProjection>();
		auto *child = op.children[0].get();
		while (child->type == LogicalOperatorType::LOGICAL_FILTER && child->children.size() == 1 &&
		       child->Cast<LogicalFilter>().projection_map.empty())
			child = child->children[0].get();
		if (child->type != LogicalOperatorType::LOGICAL_GET)
			return;
		auto &get = child->Cast<LogicalGet>();
		if (get.function.function != VgiTableFunctionScan || !get.bind_data || !get.children.empty() ||
		    !get.table_filters.filters.empty() || get.ordinality_idx.IsValid())
			return;
		auto &data = get.bind_data->Cast<VgiTableFunctionBindData>();
		VGI_LOG(input.context, "projection.source",
		        {{"function", data.function_name},
		         {"eligible", SupportedSource(data) ? "true" : "false"},
		         {"profiles", std::to_string(data.bind_result.projection_expression_profiles.size())},
		         {"bind_bytes", std::to_string(data.bind_result.bind_response_bytes.size())}});
		if (!SupportedSource(data))
			return;
		auto &profiles = data.bind_result.projection_expression_profiles;
		if (std::find(profiles.begin(), profiles.end(), VGI_CONSTRAINT_SCALAR_READ) == profiles.end())
			return;
		auto source = data.bind_result.output_schema;
		if (!source || get.returned_types.size() != static_cast<size_t>(source->num_fields()))
			return;
		std::set<string> names;
		for (const auto &field : source->fields()) {
			// Until Unicode case-fold identity is shared, non-ASCII names remain local.
			if (field->name().empty() || field->name().find('\0') != string::npos ||
			    std::any_of(field->name().begin(), field->name().end(), [](unsigned char c) { return c >= 128; }) ||
			    !names.insert(StringUtil::Lower(field->name())).second)
				return;
		}
		VgiQueryEvaluationContext context;
		try {
			context = NativeColumnEvaluationContext(input.context);
		} catch (const InvalidInputException &error) {
			VGI_LOG(input.context, "projection.decline", {{"reason", "context"}, {"detail", error.what()}});
			return;
		}
		std::vector<idx_t> slots;
		std::vector<std::vector<uint8_t>> expressions;
		arrow::FieldVector fields;
		for (idx_t i = 0; i < projection.expressions.size() && slots.size() < 1024; ++i) {
			auto &expression = *projection.expressions[i];
			VGI_LOG(input.context, "projection.expression",
			        {{"class", std::to_string(static_cast<int>(expression.GetExpressionClass()))},
			         {"type", expression.return_type.ToString()}});
			if (expression.GetExpressionClass() != ExpressionClass::BOUND_CAST ||
			    expression.return_type != LogicalType::BIGINT)
				continue;
			auto &cast = expression.Cast<BoundCastExpression>();
			if (cast.try_cast || cast.child->return_type != LogicalType::INTEGER ||
			    cast.child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF)
				continue;
			auto builtin = BoundCastExpression::AddDefaultCastToType(cast.child->Copy(), LogicalType::BIGINT);
			if (builtin->Cast<BoundCastExpression>().bound_cast.function != cast.bound_cast.function ||
			    cast.bound_cast.cast_data || cast.bound_cast.init_local_state) {
				VGI_LOG(
				    input.context, "projection.decline",
				    {{"reason", "cast_implementation"},
				     {"same_function",
				      builtin->Cast<BoundCastExpression>().bound_cast.function == cast.bound_cast.function ? "true"
				                                                                                           : "false"},
				     {"cast_data", cast.bound_cast.cast_data ? "true" : "false"},
				     {"local_state", cast.bound_cast.init_local_state ? "true" : "false"}});
				continue;
			}
			auto &ref = cast.child->Cast<BoundColumnRefExpression>();
			if (ref.depth || ref.binding.table_index != get.table_index ||
			    ref.binding.column_index >= get.GetColumnIds().size())
				continue;
			auto &column = get.GetColumnIds()[ref.binding.column_index];
			if (column.IsVirtualColumn() || column.HasChildren() ||
			    column.GetPrimaryIndex() >= static_cast<idx_t>(source->num_fields()))
				continue;
			auto field = source->field(column.GetPrimaryIndex());
			if (field->type()->id() != arrow::Type::INT32)
				continue;
			string name = "__vgi_projection_" + std::to_string(i);
			while (!names.insert(StringUtil::Lower(name)).second)
				name += "_";
			auto quoted = "\"" + StringUtil::Replace(field->name(), "\"", "\"\"") + "\"";
			expressions.push_back(
			    BuildNativeColumnExpression("CAST(" + quoted + " AS BIGINT)", data.schema_name, context, true));
			fields.push_back(arrow::field(name, arrow::int64(), true));
			slots.push_back(i);
		}
		VGI_LOG(input.context, "projection.candidates", {{"count", std::to_string(slots.size())}});
		if (slots.empty())
			return;
		auto values = std::make_shared<arrow::LargeBinaryBuilder>();
		arrow::ListBuilder list(arrow::default_memory_pool(), values);
		ThrowOnArrowError(list.Append());
		for (auto &expression : expressions)
			ThrowOnArrowError(values->Append(expression.data(), expression.size()));
		std::shared_ptr<arrow::Array> expression_array;
		ThrowOnArrowError(list.Finish(&expression_array));
		auto request =
		    arrow::RecordBatch::Make(generated::TableFunctionProjectionRequestSchema(), 1,
		                             {BuildBinaryScalarRequired(data.bind_result.bind_request_bytes),
		                              BuildBinaryScalarRequired(data.bind_result.bind_response_bytes), expression_array,
		                              BuildBinaryScalarRequired(SerializeSchemaToIpcBytes(arrow::schema(fields)))});
		auto request_bytes = SerializeToIpcBytes(request);
		if (request_bytes.size() > MAX_PROJECTION_BYTES)
			return; // The optional proposal cannot represent this bind.
		CatalogRpcContext rpc {data.attach_params, data.attach_opaque_data, data.transaction_opaque_data};
		auto response = InvokeProtocolUnary(rpc, input.context, VGI_MAIN_PROTOCOL, "table_function_projection",
		                                    generated::BuildTableFunctionProjectionParams(request_bytes));
		auto accepted = ValidateProjectionAcceptance(request_bytes, response);
		if (accepted.indices.empty())
			return;
		auto response_bytes = SerializeToIpcBytes(response);
		Apply(input.context, data, get.function, request_bytes, response_bytes, accepted);
		for (idx_t i = 0; i < accepted.indices.size(); ++i) {
			auto slot = slots[accepted.indices[i]];
			auto index = get.GetColumnIds().size();
			get.AddColumnId(source->num_fields() + i);
			projection.expressions[slot] = make_uniq<BoundColumnRefExpression>(
			    projection.expressions[slot]->alias, LogicalType::BIGINT, ColumnBinding(get.table_index, index));
		}
		get.returned_types = data.all_column_types;
		get.names = data.all_column_names;
		input.optimizer.binder.SetAlwaysRequireRebind();
	}
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Walk(input, *plan);
	}
};
} // namespace

ProjectionAcceptance ValidateProjectionAcceptance(const std::vector<uint8_t> &request_bytes,
                                                  const std::shared_ptr<arrow::RecordBatch> &response) {
	Require(request_bytes.size() <= MAX_PROJECTION_BYTES, "request exceeds 8 MiB");
	auto request = DeserializeVgiQueryRecord(request_bytes);
	Record(request, generated::TableFunctionProjectionRequestSchema());
	auto original = Bind(Bytes(request, "bind_result"));
	auto source = Schema(Bytes(original, "output_schema"));
	auto results = Schema(Bytes(request, "result_schema"));
	auto expressions = std::static_pointer_cast<arrow::ListArray>(request->GetColumnByName("expressions"));
	Require(expressions->value_length(0) > 0 && expressions->value_length(0) <= 1024 &&
	            expressions->value_length(0) == results->num_fields(),
	        "proposal result count mismatch");
	Require(!results->metadata() || results->metadata()->size() == 0, "result schema has schema-level metadata");
	Record(response, generated::TableFunctionProjectionResultSchema());
	auto indices = std::static_pointer_cast<arrow::ListArray>(response->GetColumnByName("accepted_indices"));
	auto values = std::static_pointer_cast<arrow::Int64Array>(indices->values());
	ProjectionAcceptance accepted;
	int64_t previous = -1;
	for (int64_t i = indices->value_offset(0); i < indices->value_offset(0) + indices->value_length(0); ++i) {
		Require(!values->IsNull(i), "accepted index is NULL");
		auto value = values->Value(i);
		Require(value > previous && value < results->num_fields(), "accepted indices must be ordered unique in-range");
		accepted.indices.push_back(value);
		previous = value;
	}
	auto result = response->GetColumnByName("bind_result");
	if (accepted.indices.empty()) {
		Require(result->IsNull(0), "decline returned bind state");
		return accepted;
	}
	accepted.bind_result = Bind(Bytes(response, "bind_result"));
	Require(accepted.bind_result->GetColumnByName("projection_expression_profiles")
	            ->Equals(original->GetColumnByName("projection_expression_profiles")),
	        "acceptance changed projection profiles");
	Require(ParseBindResponse(accepted.bind_result).repeatability == ParseBindResponse(original).repeatability,
	        "acceptance changed repeatability");
	auto fields = source->fields();
	for (auto index : accepted.indices)
		fields.push_back(results->field(index));
	auto expected = arrow::schema(fields, source->metadata());
	accepted.output_schema = Schema(Bytes(accepted.bind_result, "output_schema"));
	Require(accepted.output_schema->Equals(*expected, true),
	        "accepted output schema differs from exact requested schema");
	return accepted;
}

void RestoreProjectionAcceptance(ClientContext &context, VgiTableFunctionBindData &data, TableFunction &function,
                                 const std::vector<uint8_t> &request_bytes,
                                 const std::vector<uint8_t> &response_bytes) {
	Require(SupportedSource(data), "serialized projected source is no longer supported");
	auto request = DeserializeVgiQueryRecord(request_bytes);
	Record(request, generated::TableFunctionProjectionRequestSchema());
	Require(Bytes(request, "bind_call") == data.bind_result.bind_request_bytes, "serialized bind identity changed");
	Require(Bytes(request, "bind_result") == data.bind_result.bind_response_bytes,
	        "serialized source bind state changed; bind a fresh projection");
	auto original = Bind(Bytes(request, "bind_result"));
	Require(Schema(Bytes(original, "output_schema"))->Equals(*data.bind_result.output_schema, true),
	        "serialized source schema changed");
	Require(ParseBindResponse(original).projection_expression_profiles ==
	            data.bind_result.projection_expression_profiles,
	        "serialized source profile offer changed");
	auto response = DeserializeVgiQueryRecord(response_bytes);
	auto accepted = ValidateProjectionAcceptance(request_bytes, response);
	Require(!accepted.indices.empty(), "serialized acceptance declined");
	Apply(context, data, function, request_bytes, response_bytes, accepted);
}
void RegisterVgiProjectionOptimizer(DBConfig &config) {
	OptimizerExtension::Register(config, ProjectionOptimizer());
}
} // namespace vgi
} // namespace duckdb
