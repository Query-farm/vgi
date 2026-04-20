#include "vgi_aggregate_function_impl.hpp"
#include "vgi_aggregate_window_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_transport.hpp"
#include "vgi_unary_rpc.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <arrow/builder.h>
#include <arrow/c/bridge.h>

namespace duckdb {

namespace {

std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names) {
	std::map<std::string, Value> settings;
	for (const auto &name : setting_names) {
		Value value;
		if (context.TryGetCurrentSetting(name, value)) {
			settings[name] = value;
		}
	}
	return settings;
}

// Helper: throw if an Arrow Status is not OK
void ThrowOnArrowError(const arrow::Status &status) {
	if (!status.ok()) {
		throw IOException("Arrow error in VGI aggregate: %s", status.ToString());
	}
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateBindRequest(
    const std::string &function_name, const std::vector<uint8_t> &attach_id,
    const std::shared_ptr<arrow::Schema> &input_schema,
    ClientContext &context, const std::map<std::string, Value> &settings,
    const std::vector<vgi::VgiSecretRequirement> &required_secrets,
    const vgi::ArrowArguments &arrow_arguments = {}) {

	auto schema_buf_result = arrow::ipc::SerializeSchema(*input_schema);
	ThrowOnArrowError(schema_buf_result.status());
	auto &schema_buf = schema_buf_result.ValueUnsafe();
	auto schema_bytes = std::vector<uint8_t>(schema_buf->data(), schema_buf->data() + schema_buf->size());

	// Serialize arguments (const values) as IPC bytes
	std::vector<uint8_t> arguments_bytes;
	if (arrow_arguments.array) {
		auto args_schema = arrow::schema({arrow::field("args", arrow_arguments.type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, arrow_arguments.array->length(),
		                                           {arrow_arguments.array});
		arguments_bytes = vgi::SerializeToIpcBytes(args_batch);
	} else {
		auto empty_struct_type = arrow::struct_({});
		auto empty_result = arrow::MakeEmptyArray(empty_struct_type);
		ThrowOnArrowError(empty_result.status());
		auto args_schema = arrow::schema({arrow::field("args", empty_struct_type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, 0, {empty_result.ValueUnsafe()});
		arguments_bytes = vgi::SerializeToIpcBytes(args_batch);
	}

	// Serialize settings
	std::vector<uint8_t> settings_bytes;
	if (!settings.empty()) {
		auto settings_batch = vgi::BuildSettingsBatch(context, settings);
		settings_bytes = vgi::SerializeToIpcBytes(settings_batch);
	}

	// Serialize secrets
	auto secrets = vgi::ExtractVgiSecrets(context, required_secrets);
	auto secrets_ipc_bytes = vgi::BuildSecretsBatch(context, secrets);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("input_schema", arrow::binary(), true),
	    arrow::field("settings", arrow::binary(), true),
	    arrow::field("secrets", arrow::binary(), true),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	auto fn_builder = arrow::StringBuilder();
	auto args_builder = arrow::BinaryBuilder();
	auto schema_builder = arrow::BinaryBuilder();
	auto settings_builder = arrow::BinaryBuilder();
	auto secrets_builder = arrow::BinaryBuilder();
	auto aid_builder = arrow::BinaryBuilder();

	ThrowOnArrowError(fn_builder.Append(function_name));
	ThrowOnArrowError(args_builder.Append(arguments_bytes.data(), arguments_bytes.size()));
	ThrowOnArrowError(schema_builder.Append(schema_bytes.data(), schema_bytes.size()));
	if (settings_bytes.empty()) {
		ThrowOnArrowError(settings_builder.AppendNull());
	} else {
		ThrowOnArrowError(settings_builder.Append(settings_bytes.data(), settings_bytes.size()));
	}
	if (secrets_ipc_bytes.empty()) {
		ThrowOnArrowError(secrets_builder.AppendNull());
	} else {
		ThrowOnArrowError(secrets_builder.Append(secrets_ipc_bytes.data(), secrets_ipc_bytes.size()));
	}
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_builder.AppendNull());
	} else {
		ThrowOnArrowError(aid_builder.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_arr, args_arr, schema_arr, settings_arr, secrets_arr, aid_arr;
	ThrowOnArrowError(fn_builder.Finish(&fn_arr));
	ThrowOnArrowError(args_builder.Finish(&args_arr));
	ThrowOnArrowError(schema_builder.Finish(&schema_arr));
	ThrowOnArrowError(settings_builder.Finish(&settings_arr));
	ThrowOnArrowError(secrets_builder.Finish(&secrets_arr));
	ThrowOnArrowError(aid_builder.Finish(&aid_arr));

	return vgi::WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_arr, args_arr, schema_arr, settings_arr, secrets_arr, aid_arr}));
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateUpdateRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id,
    const std::shared_ptr<arrow::RecordBatch> &input_batch) {

	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("input_batch", arrow::binary(), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	auto fn_builder = arrow::StringBuilder();
	auto eid_builder = arrow::BinaryBuilder();
	auto batch_builder = arrow::BinaryBuilder();
	auto aid_builder = arrow::BinaryBuilder();

	ThrowOnArrowError(fn_builder.Append(function_name));
	ThrowOnArrowError(eid_builder.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(batch_builder.Append(batch_bytes.data(), batch_bytes.size()));
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_builder.AppendNull());
	} else {
		ThrowOnArrowError(aid_builder.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_arr, eid_arr, batch_arr, aid_arr;
	ThrowOnArrowError(fn_builder.Finish(&fn_arr));
	ThrowOnArrowError(eid_builder.Finish(&eid_arr));
	ThrowOnArrowError(batch_builder.Finish(&batch_arr));
	ThrowOnArrowError(aid_builder.Finish(&aid_arr));

	return vgi::WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_arr, eid_arr, batch_arr, aid_arr}));
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateCombineRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id,
    const std::shared_ptr<arrow::RecordBatch> &merge_batch) {

	auto batch_bytes = vgi::SerializeToIpcBytes(merge_batch);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("merge_batch", arrow::binary(), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	auto fn_builder = arrow::StringBuilder();
	auto eid_builder = arrow::BinaryBuilder();
	auto batch_builder = arrow::BinaryBuilder();
	auto aid_builder = arrow::BinaryBuilder();

	ThrowOnArrowError(fn_builder.Append(function_name));
	ThrowOnArrowError(eid_builder.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(batch_builder.Append(batch_bytes.data(), batch_bytes.size()));
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_builder.AppendNull());
	} else {
		ThrowOnArrowError(aid_builder.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_arr, eid_arr, batch_arr, aid_arr;
	ThrowOnArrowError(fn_builder.Finish(&fn_arr));
	ThrowOnArrowError(eid_builder.Finish(&eid_arr));
	ThrowOnArrowError(batch_builder.Finish(&batch_arr));
	ThrowOnArrowError(aid_builder.Finish(&aid_arr));

	return vgi::WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_arr, eid_arr, batch_arr, aid_arr}));
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateFinalizeRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id,
    const std::shared_ptr<arrow::RecordBatch> &group_ids_batch,
    const std::shared_ptr<arrow::Schema> &output_schema) {

	auto gid_bytes = vgi::SerializeToIpcBytes(group_ids_batch);
	auto os_buf_result = arrow::ipc::SerializeSchema(*output_schema);
	ThrowOnArrowError(os_buf_result.status());
	auto &os_buf = os_buf_result.ValueUnsafe();
	auto schema_bytes = std::vector<uint8_t>(os_buf->data(), os_buf->data() + os_buf->size());

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("group_ids_batch", arrow::binary(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	auto fn_builder = arrow::StringBuilder();
	auto eid_builder = arrow::BinaryBuilder();
	auto gid_builder = arrow::BinaryBuilder();
	auto os_builder = arrow::BinaryBuilder();
	auto aid_builder = arrow::BinaryBuilder();

	ThrowOnArrowError(fn_builder.Append(function_name));
	ThrowOnArrowError(eid_builder.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(gid_builder.Append(gid_bytes.data(), gid_bytes.size()));
	ThrowOnArrowError(os_builder.Append(schema_bytes.data(), schema_bytes.size()));
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_builder.AppendNull());
	} else {
		ThrowOnArrowError(aid_builder.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_arr, eid_arr, gid_arr, os_arr, aid_arr;
	ThrowOnArrowError(fn_builder.Finish(&fn_arr));
	ThrowOnArrowError(eid_builder.Finish(&eid_arr));
	ThrowOnArrowError(gid_builder.Finish(&gid_arr));
	ThrowOnArrowError(os_builder.Finish(&os_arr));
	ThrowOnArrowError(aid_builder.Finish(&aid_arr));

	return vgi::WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_arr, eid_arr, gid_arr, os_arr, aid_arr}));
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateDestructorRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id,
    const std::shared_ptr<arrow::RecordBatch> &group_ids_batch) {

	auto gid_bytes = vgi::SerializeToIpcBytes(group_ids_batch);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("group_ids_batch", arrow::binary(), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	auto fn_builder = arrow::StringBuilder();
	auto eid_builder = arrow::BinaryBuilder();
	auto gid_builder = arrow::BinaryBuilder();
	auto aid_builder = arrow::BinaryBuilder();

	ThrowOnArrowError(fn_builder.Append(function_name));
	ThrowOnArrowError(eid_builder.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(gid_builder.Append(gid_bytes.data(), gid_bytes.size()));
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_builder.AppendNull());
	} else {
		ThrowOnArrowError(aid_builder.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_arr, eid_arr, gid_arr, aid_arr;
	ThrowOnArrowError(fn_builder.Finish(&fn_arr));
	ThrowOnArrowError(eid_builder.Finish(&eid_arr));
	ThrowOnArrowError(gid_builder.Finish(&gid_arr));
	ThrowOnArrowError(aid_builder.Finish(&aid_arr));

	return vgi::WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_arr, eid_arr, gid_arr, aid_arr}));
}

} // anonymous namespace

namespace vgi {

// ============================================================================
// Shared RPC envelope + invoker
// ============================================================================
// Moved out of the anonymous namespace so aggregate_window_impl.cpp can share
// the same connection-pool / subprocess / HTTP transport plumbing.

std::shared_ptr<arrow::RecordBatch> WrapAsRpcParams(const std::shared_ptr<arrow::RecordBatch> &request_batch) {
	auto request_bytes = SerializeToIpcBytes(request_batch);
	auto schema = arrow::schema({arrow::field("request", arrow::binary(), false)});
	arrow::BinaryBuilder builder;
	ThrowOnArrowError(builder.Append(request_bytes.data(), request_bytes.size()));
	std::shared_ptr<arrow::Array> arr;
	ThrowOnArrowError(builder.Finish(&arr));
	return arrow::RecordBatch::Make(schema, 1, {arr});
}

AggregateRpcResult InvokeAggregateRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                      const std::string &method_name,
                                      const std::shared_ptr<arrow::RecordBatch> &params,
                                      bool enable_logging) {
	UnaryRpcOptions opts {context,
	                      bind_data.attach_params->worker_path(),
	                      bind_data.attach_params->worker_debug(),
	                      bind_data.attach_params->use_pool(),
	                      "rpc_aggregate",
	                      bind_data.attach_params->auth(),
	                      enable_logging};
	auto response = InvokePooledUnaryRpc(opts, method_name, params);
	return {response.batch};
}

// ============================================================================
// VgiAggregateBindData
// ============================================================================

unique_ptr<FunctionData> VgiAggregateBindData::Copy() const {
	auto copy = make_uniq<VgiAggregateBindData>();
	copy->attach_params = attach_params;
	copy->attach_id = attach_id;
	copy->function_name = function_name;
	copy->settings = settings;
	copy->required_secrets = required_secrets;
	copy->resolved_output_schema = resolved_output_schema;
	copy->input_schema = input_schema;
	copy->catalog = catalog;
	copy->context = context;
	copy->const_values = const_values;
	copy->exec_state = exec_state;  // Share execution state across copies
	return copy;
}

bool VgiAggregateBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<VgiAggregateBindData>();
	return function_name == other.function_name && attach_id == other.attach_id && settings == other.settings &&
	       const_values == other.const_values;
}

// ============================================================================
// Aggregate callbacks
// ============================================================================

idx_t VgiAggregateStateSize(const AggregateFunction &function) {
	// The same buffer serves both the standard aggregate path and the
	// windowed aggregate path — size it for the larger of the two. The
	// tag field at offset 0 (shared layout) discriminates at teardown time.
	idx_t agg_size = sizeof(VgiAggregateState);
	idx_t win_size = sizeof(VgiAggregateWindowLocalState);
	return agg_size > win_size ? agg_size : win_size;
}

void VgiAggregateInitialize(const AggregateFunction &function, data_ptr_t state) {
	new (state) VgiAggregateState();
	// tag stays UNSET, group_id stays -1. The first-use site (update/combine/
	// finalize for the aggregate path, or VgiAggregateWindowInit for the
	// window path) is responsible for setting the tag. Initialize doesn't
	// receive AggregateInputData, so we can't access the per-query ExecState
	// counters here anyway.
}

void VgiAggregateUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state_vector,
                        idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto &context = *bind_data.context;

	// Get state pointers and assign group_ids to uninitialized states
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetData<VgiAggregateState *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = sdata.sel->get_index(i);
		auto &agg_state = *states[idx];
		if (agg_state.group_id == -1) {
			agg_state.group_id = bind_data.exec_state->group_id_counter.fetch_add(1);
		}
		agg_state.tag = VgiAggregateStateTag::AGGREGATE;
	}

	// Build group_id column
	auto gid_builder = arrow::Int64Builder();
	ThrowOnArrowError(gid_builder.Reserve(count));
	for (idx_t i = 0; i < count; i++) {
		auto idx = sdata.sel->get_index(i);
		gid_builder.UnsafeAppend(states[idx]->group_id);
	}
	std::shared_ptr<arrow::Array> gid_array;
	ThrowOnArrowError(gid_builder.Finish(&gid_array));

	// Build actual input schema from runtime types (may differ from bind-time
	// schema when varargs are present — bind schema has fixed params only)
	vector<LogicalType> actual_input_types;
	vector<string> actual_input_names;
	for (idx_t i = 0; i < input_count; i++) {
		actual_input_types.push_back(inputs[i].GetType());
		if (i < static_cast<idx_t>(bind_data.input_schema->num_fields())) {
			actual_input_names.push_back(bind_data.input_schema->field(i)->name());
		} else {
			actual_input_names.push_back("col_" + std::to_string(i));
		}
	}
	auto actual_schema = BuildArrowSchemaFromDuckDB(context, actual_input_types, actual_input_names);

	// Convert input vectors to Arrow
	std::shared_ptr<arrow::RecordBatch> input_batch;
	if (input_count > 0) {
		DataChunk input_chunk;
		input_chunk.Initialize(context, actual_input_types);
		for (idx_t i = 0; i < input_count; i++) {
			input_chunk.data[i].Reference(inputs[i]);
		}
		input_chunk.SetCardinality(count);
		input_batch = DataChunkToArrow(context, input_chunk, actual_schema);
	}

	// Build full batch: __vgi_group_id + input columns
	std::vector<std::shared_ptr<arrow::Field>> full_fields;
	full_fields.push_back(arrow::field("__vgi_group_id", arrow::int64()));
	for (int i = 0; i < actual_schema->num_fields(); i++) {
		full_fields.push_back(actual_schema->field(i));
	}
	auto full_schema = arrow::schema(full_fields);

	std::vector<std::shared_ptr<arrow::Array>> columns;
	columns.push_back(gid_array);
	if (input_batch) {
		for (int i = 0; i < input_batch->num_columns(); i++) {
			columns.push_back(input_batch->column(i));
		}
	}
	auto full_batch = arrow::RecordBatch::Make(full_schema, count, columns);

	// Build RPC request: aggregate_update(function_name, execution_id, input_batch)
	auto request = BuildAggregateUpdateRequest(bind_data.function_name, bind_data.exec_state->execution_id,
	                                            bind_data.attach_id, full_batch);

	InvokeAggregateRpc(context, bind_data, "aggregate_update", request);
}

void VgiAggregateCombine(Vector &source, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto &context = *bind_data.context;

	UnifiedVectorFormat sdata, cdata;
	source.ToUnifiedFormat(count, sdata);
	combined.ToUnifiedFormat(count, cdata);

	// Assign group_ids to any uninitialized states (states that were never updated)
	auto src_states = UnifiedVectorFormat::GetData<VgiAggregateState *>(sdata);
	auto tgt_states = UnifiedVectorFormat::GetData<VgiAggregateState *>(cdata);
	for (idx_t i = 0; i < count; i++) {
		auto src_idx = sdata.sel->get_index(i);
		auto tgt_idx = cdata.sel->get_index(i);
		if (src_states[src_idx]->group_id == -1) {
			src_states[src_idx]->group_id = bind_data.exec_state->group_id_counter.fetch_add(1);
		}
		if (tgt_states[tgt_idx]->group_id == -1) {
			tgt_states[tgt_idx]->group_id = bind_data.exec_state->group_id_counter.fetch_add(1);
		}
		src_states[src_idx]->tag = VgiAggregateStateTag::AGGREGATE;
		tgt_states[tgt_idx]->tag = VgiAggregateStateTag::AGGREGATE;
	}

	// Build merge batch: (source_group_id, target_group_id)
	auto src_builder = arrow::Int64Builder();
	auto tgt_builder = arrow::Int64Builder();
	ThrowOnArrowError(src_builder.Reserve(count));
	ThrowOnArrowError(tgt_builder.Reserve(count));
	for (idx_t i = 0; i < count; i++) {
		auto src_idx = sdata.sel->get_index(i);
		auto tgt_idx = cdata.sel->get_index(i);
		src_builder.UnsafeAppend(src_states[src_idx]->group_id);
		tgt_builder.UnsafeAppend(tgt_states[tgt_idx]->group_id);
	}
	std::shared_ptr<arrow::Array> src_array, tgt_array;
	ThrowOnArrowError(src_builder.Finish(&src_array));
	ThrowOnArrowError(tgt_builder.Finish(&tgt_array));

	auto merge_schema = arrow::schema({
	    arrow::field("source_group_id", arrow::int64()),
	    arrow::field("target_group_id", arrow::int64()),
	});
	auto merge_batch = arrow::RecordBatch::Make(merge_schema, count, {src_array, tgt_array});

	auto request = BuildAggregateCombineRequest(bind_data.function_name, bind_data.exec_state->execution_id,
	                                             bind_data.attach_id, merge_batch);

	InvokeAggregateRpc(context, bind_data, "aggregate_combine", request);
}

void VgiAggregateFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                          idx_t offset) {
	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto &context = *bind_data.context;

	// Build group_ids batch, assigning ids to any uninitialized states
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetData<VgiAggregateState *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sdata.sel->get_index(i);
		if (states[idx]->group_id == -1) {
			states[idx]->group_id = bind_data.exec_state->group_id_counter.fetch_add(1);
		}
		states[idx]->tag = VgiAggregateStateTag::AGGREGATE;
	}

	auto gid_builder = arrow::Int64Builder();
	ThrowOnArrowError(gid_builder.Reserve(count));
	for (idx_t i = 0; i < count; i++) {
		auto idx = sdata.sel->get_index(i);
		gid_builder.UnsafeAppend(states[idx]->group_id);
	}
	std::shared_ptr<arrow::Array> gid_array;
	ThrowOnArrowError(gid_builder.Finish(&gid_array));

	auto gid_schema = arrow::schema({arrow::field("group_id", arrow::int64())});
	auto gid_batch = arrow::RecordBatch::Make(gid_schema, count, {gid_array});

	// Call aggregate_finalize RPC
	auto request = BuildAggregateFinalizeRequest(bind_data.function_name, bind_data.exec_state->execution_id,
	                                              bind_data.attach_id, gid_batch, bind_data.resolved_output_schema);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_finalize", request);

	// Extract result_batch from response
	if (!rpc_result.response_batch || rpc_result.response_batch->num_rows() == 0) {
		throw IOException("VGI aggregate_finalize returned empty response for '%s'", bind_data.function_name);
	}

	// The response is wrapped in the standard vgi_rpc format:
	// { "result": binary } where result contains the serialized AggregateFinalizeResponse
	auto response_result_col = rpc_result.response_batch->GetColumnByName("result");
	if (!response_result_col) {
		throw IOException("VGI aggregate_finalize response missing 'result' column for '%s'", bind_data.function_name);
	}
	auto response_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(response_result_col);
	if (!response_binary || response_binary->IsNull(0)) {
		throw IOException("VGI aggregate_finalize response has null result for '%s'", bind_data.function_name);
	}
	// Unwrap: result column contains the serialized AggregateFinalizeResponse dataclass
	auto response_view = response_binary->GetView(0);
	auto response_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(response_view.data()), response_view.size());

	// AggregateFinalizeResponse has a "result_batch" binary column
	auto rb_col = response_batch->GetColumnByName("result_batch");
	if (!rb_col) {
		throw IOException("VGI aggregate_finalize response missing 'result_batch' field for '%s'", bind_data.function_name);
	}
	auto rb_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(rb_col);
	if (!rb_binary || rb_binary->IsNull(0)) {
		throw IOException("VGI aggregate_finalize response has null result_batch for '%s'", bind_data.function_name);
	}
	auto rb_view = rb_binary->GetView(0);
	auto result_batch = DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(rb_view.data()), rb_view.size());

	if (!result_batch || result_batch->num_columns() != 1) {
		throw IOException("VGI aggregate_finalize returned invalid result for '%s'", bind_data.function_name);
	}
	if (static_cast<idx_t>(result_batch->num_rows()) != count) {
		throw IOException("VGI aggregate_finalize returned %d rows but expected %d for '%s'",
		                  result_batch->num_rows(), count, bind_data.function_name);
	}

	// Convert Arrow result to DuckDB Vector
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	ArrowSchemaToDuckDBTypes(context, result_batch->schema(), c_schema, arrow_table, output_types, output_names);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(result_batch, *chunk_wrapper);
	ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);

	DataChunk temp_output;
	temp_output.Initialize(context, {result.GetType()});
	temp_output.SetCardinality(count);
	ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), temp_output, false);

	VectorOperations::Copy(temp_output.data[0], result, count, 0, offset);
}

// Forward-declared here so VgiAggregateDestroy can dispatch to the window
// destructor path. Defined in vgi_aggregate_window_impl.cpp.
void SendAggregateWindowDestructorRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                       int64_t partition_id);

void VgiAggregateDestroy(Vector &state_vector, AggregateInputData &aggr_input_data, idx_t count) {
	// Best-effort cleanup — must not throw
	try {
		auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		// The l_state buffer is shared between the aggregate path (VgiAggregateState)
		// and the window path (VgiAggregateWindowLocalState). The tag field at
		// offset 0 is part of both struct layouts; inspect it through a
		// VgiAggregateState* view (safe because layout compatibility is
		// guaranteed for the tag field).
		auto raw_states = UnifiedVectorFormat::GetData<VgiAggregateState *>(sdata);

		int64_t aggregate_initialized = 0;
		// Window states — dispatch destructor RPC per partition_id. Only the
		// GLOBAL state of a window aggregator carries a real partition_id;
		// per-thread LOCAL states stay at partition_id == -1 and are skipped.
		std::vector<int64_t> window_partition_ids;

		for (idx_t i = 0; i < count; i++) {
			auto idx = sdata.sel->get_index(i);
			auto tag = raw_states[idx]->tag;
			if (tag == VgiAggregateStateTag::AGGREGATE) {
				if (raw_states[idx]->group_id != -1) {
					aggregate_initialized++;
				}
			} else if (tag == VgiAggregateStateTag::WINDOW) {
				auto *win = reinterpret_cast<VgiAggregateWindowLocalState *>(raw_states[idx]);
				if (win->partition_id != -1) {
					window_partition_ids.push_back(win->partition_id);
				}
			}
			// UNSET: state was never touched — nothing to clean up.
		}

		auto &context = *bind_data.context;

		// --- Window destructor path ---
		for (auto pid : window_partition_ids) {
			try {
				SendAggregateWindowDestructorRpc(context, bind_data, pid);
			} catch (...) {
				// Swallow — best-effort; the safety sweep in aggregate_destructor
				// will clear any residual partitions for this execution_id.
			}
		}

		// --- Aggregate destructor path (unchanged semantics) ---
		if (aggregate_initialized == 0) {
			return;
		}
		auto prev = bind_data.exec_state->destroy_counter.fetch_add(aggregate_initialized);
		auto total_created = bind_data.exec_state->group_id_counter.load();
		if (prev + aggregate_initialized < total_created) {
			return; // Not the last batch — skip RPC
		}

		auto gid_builder = arrow::Int64Builder();
		ThrowOnArrowError(gid_builder.Append(0));
		std::shared_ptr<arrow::Array> gid_array;
		ThrowOnArrowError(gid_builder.Finish(&gid_array));
		auto gid_schema = arrow::schema({arrow::field("group_id", arrow::int64())});
		auto gid_batch = arrow::RecordBatch::Make(gid_schema, 1, {gid_array});

		auto request = BuildAggregateDestructorRequest(bind_data.function_name, bind_data.exec_state->execution_id,
		                                                bind_data.attach_id, gid_batch);
		// enable_logging=false: this path runs on task-scheduler threads during
		// pipeline teardown. VGI_LOG / DrainToLog are unsafe there (crash #A in
		// the shutdown stacks). Worker stderr stays buffered on the drainer.
		InvokeAggregateRpc(context, bind_data, "aggregate_destructor", request, /*enable_logging=*/false);
	} catch (...) {
		// Swallow all exceptions — destructor must not throw
	}
}

// ============================================================================
// Bind
// ============================================================================

unique_ptr<FunctionData> VgiAggregateFunctionBind(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto &func_info = function.function_info->Cast<VgiAggregateFunctionInfo>();
	auto settings = ExtractVgiSettings(context, func_info.setting_names);

	// ========================================================================
	// Phase 1: Detect and extract const values (same pattern as scalar bind)
	// ========================================================================
	vector<Value> const_values;
	vector<idx_t> const_indices;

	// Re-bind guard: if arguments already match post-erase count, const args
	// were already extracted (plan caching / deserialization).
	idx_t expected_non_const = 0;
	for (idx_t i = 0; i < func_info.positional_is_const.size(); i++) {
		if (!func_info.positional_is_const[i]) {
			expected_non_const++;
		}
	}
	bool const_args_already_erased = !func_info.positional_is_const.empty() &&
	                                  arguments.size() == expected_non_const;

	if (!const_args_already_erased) {
		for (idx_t i = 0; i < arguments.size() && i < func_info.positional_is_const.size(); i++) {
			if (func_info.positional_is_const[i]) {
				auto &expr = arguments[i];
				if (!expr->IsFoldable()) {
					throw BinderException(
					    "VGI aggregate function '%s': parameter '%s' must be a constant value, "
					    "but received a non-constant expression.",
					    func_info.function_name,
					    i < func_info.positional_names.size() ? func_info.positional_names[i] : std::to_string(i));
				}
				Value val = ExpressionExecutor::EvaluateScalar(context, *expr);
				const_values.push_back(val);
				const_indices.push_back(i);
			}
		}
		// Erase const arguments in reverse order to preserve indices
		for (auto it = const_indices.rbegin(); it != const_indices.rend(); ++it) {
			Function::EraseArgument(function, arguments, *it);
		}
	}

	// Ensure function.arguments covers all remaining children so DuckDB's
	// BindAggregateFunction (line 704 of function_binder.cpp) doesn't truncate:
	//   children.resize(MinValue(bound_function.arguments.size(), children.size()));
	// For varargs functions, pad function.arguments with the varargs type.
	if (function.varargs != LogicalType::INVALID) {
		while (function.arguments.size() < arguments.size()) {
			function.arguments.push_back(arguments[function.arguments.size()]->return_type);
		}
	}

	// ========================================================================
	// Phase 2: Build input schema from remaining (non-const) arguments
	// ========================================================================
	vector<LogicalType> input_types;
	vector<string> input_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		input_types.push_back(arguments[i]->return_type);
		input_names.push_back("col_" + std::to_string(i));
	}
	auto input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

	// ========================================================================
	// Phase 3: Build ArrowArguments from const values
	// ========================================================================
	vector<Value> positional_args;
	for (auto &val : const_values) {
		positional_args.push_back(val);
	}
	ArrowArguments arrow_arguments = BuildArgumentsFromValues(context, positional_args, {});

	// Call aggregate_bind RPC
	auto request = BuildAggregateBindRequest(func_info.function_name, func_info.attach_id, input_schema,
	                                          context, settings, func_info.required_secrets,
	                                          arrow_arguments);

	auto bind_data = make_uniq<VgiAggregateBindData>();
	bind_data->attach_params = func_info.attach_params;
	bind_data->attach_id = func_info.attach_id;
	bind_data->function_name = func_info.function_name;
	bind_data->settings = settings;
	bind_data->required_secrets = func_info.required_secrets;
	bind_data->resolved_output_schema = func_info.output_schema;
	bind_data->input_schema = input_schema;
	bind_data->catalog = func_info.catalog;
	bind_data->context = &context;
	bind_data->const_values = const_values;

	// Call aggregate_bind to get execution_id
	auto rpc_result = InvokeAggregateRpc(context, *bind_data, "aggregate_bind", request);

	if (rpc_result.response_batch && rpc_result.response_batch->num_rows() > 0) {
		// Unwrap: {result: binary} -> AggregateBindResponse dataclass
		auto wrapper_col = rpc_result.response_batch->GetColumnByName("result");
		if (wrapper_col) {
			auto wrapper_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(wrapper_col);
			if (wrapper_binary && !wrapper_binary->IsNull(0)) {
				auto wrapper_view = wrapper_binary->GetView(0);
				auto response_batch = DeserializeFromIpcBytes(
				    reinterpret_cast<const uint8_t *>(wrapper_view.data()), wrapper_view.size());

				auto exec_id_col = response_batch->GetColumnByName("execution_id");
				if (exec_id_col) {
					auto binary_arr = std::dynamic_pointer_cast<arrow::BinaryArray>(exec_id_col);
					if (binary_arr && !binary_arr->IsNull(0)) {
						auto view = binary_arr->GetView(0);
						bind_data->exec_state->execution_id.assign(
						    reinterpret_cast<const uint8_t *>(view.data()),
						    reinterpret_cast<const uint8_t *>(view.data()) + view.size());
					}
				}

				// Extract output_schema from bind response (allows dynamic return types)
				auto os_col = response_batch->GetColumnByName("output_schema");
				if (os_col) {
					auto os_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(os_col);
					if (os_binary && !os_binary->IsNull(0)) {
						auto os_view = os_binary->GetView(0);
						auto os_buf = std::make_shared<arrow::Buffer>(
						    reinterpret_cast<const uint8_t *>(os_view.data()), os_view.size());
						auto reader = std::make_shared<arrow::io::BufferReader>(os_buf);
						arrow::ipc::DictionaryMemo dict_memo;
						auto schema_result = arrow::ipc::ReadSchema(reader.get(), &dict_memo);
						if (schema_result.ok() && schema_result.ValueUnsafe()->num_fields() > 0) {
							bind_data->resolved_output_schema = schema_result.ValueUnsafe();
							// Update the bound function's return type to match
							ArrowSchemaWrapper c_schema;
							ArrowTableSchema arrow_table;
							vector<LogicalType> out_types;
							vector<string> out_names;
							ArrowSchemaToDuckDBTypes(context, bind_data->resolved_output_schema,
							                         c_schema, arrow_table, out_types, out_names);
							if (!out_types.empty()) {
								function.return_type = out_types[0];
							}
						}
					}
				}
			}
		}
	}

	// Verify execution_id was extracted — without it, all queries share FunctionStorage state
	if (bind_data->exec_state->execution_id.empty()) {
		throw IOException("VGI aggregate_bind did not return execution_id for '%s'", bind_data->function_name);
	}

	return bind_data;
}

} // namespace vgi
} // namespace duckdb
