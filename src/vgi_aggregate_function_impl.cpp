// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_aggregate_function_impl.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_batch_validation.hpp"
#include "vgi_aggregate_window_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_global_functions.hpp" // ResolveVgiGlobalBinding (system.main registrations)
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_transport.hpp"
#include "vgi_unary_rpc.hpp"
#include "vgi_worker_pool.hpp"
#include "generated/vgi_request_builders.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "nanoarrow/nanoarrow.h"

namespace duckdb {

using vgi::ExtractVgiSettings;

namespace {

// A builder for the batch of int64 columns `schema`, each with room for `count` rows.
void ReserveInt64Columns(vgi::VgiBatchBuilder &builder, idx_t count) {
	for (int c = 0; c < builder.num_columns(); c++) {
		if (ArrowArrayReserve(builder.column(c), static_cast<int64_t>(count)) != NANOARROW_OK) {
			throw IOException("Arrow error in VGI aggregate: out of memory reserving %llu rows",
			                  static_cast<unsigned long long>(count));
		}
	}
}

const vgi::VgiSchemaPtr &GroupIdSchema() {
	static const auto schema = vgi::MakeSchema({vgi::type::Int64().Named("group_id")});
	return schema;
}

const vgi::VgiSchemaPtr &UpdateGroupIdSchema() {
	static const auto schema = vgi::MakeSchema({vgi::type::Int64().Named("__vgi_group_id")});
	return schema;
}

// A one-column int64 batch (`schema`) of the group ids of `states` (through `sdata`'s selection).
vgi::VgiBatchPtr BuildGroupIdBatch(const vgi::VgiSchemaPtr &schema, const UnifiedVectorFormat &sdata,
                                   vgi::VgiAggregateState *const *states, idx_t count) {
	vgi::VgiBatchBuilder builder(schema);
	ReserveInt64Columns(builder, count);
	auto *col = builder.column(0);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sdata.sel->get_index(i);
		vgi::append::Int(col, states[idx]->group_id);
	}
	builder.SetRows(static_cast<int64_t>(count));
	return builder.Finish();
}

// The columns of `batch` as standalone arrays, each keeping the batch alive (zero-copy).
std::vector<ArrowArray> ExportBatchColumns(const vgi::VgiBatch &batch) {
	ArrowArray parent;
	batch.ExportTo(&parent);
	std::vector<ArrowArray> columns;
	columns.reserve(static_cast<size_t>(parent.n_children));
	for (int64_t i = 0; i < parent.n_children; i++) {
		columns.push_back(*parent.children[i]);
		parent.children[i]->release = nullptr;
	}
	parent.release(&parent);
	return columns;
}

// Every aggregate request carries the owning catalog schema alongside the
// function name: a name is unique only within a schema, so without it the
// worker re-resolves each unary call by bare name and can land on another
// schema's implementation after a bind that resolved correctly. Empty string
// serialises as null, which is the legitimate
// "caller names no schema" wire form — the field stays nullable.
vgi::VgiBatchPtr BuildAggregateBindRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &attach_opaque_data,
    const vgi::VgiSchemaPtr &input_schema,
    ClientContext &context, const std::map<std::string, Value> &settings,
    const std::vector<vgi::VgiSecretRequirement> &required_secrets,
    const vgi::ArrowArguments &arrow_arguments,
    const std::optional<std::vector<std::optional<std::string>>> &argument_names) {

	auto schema_bytes = vgi::SerializeSchemaToIpcBytes(input_schema);

	// Serialize arguments (const values) as IPC bytes
	std::vector<uint8_t> arguments_bytes;
	if (arrow_arguments) {
		// Already the one-row {args: struct} batch.
		arguments_bytes = vgi::SerializeToIpcBytes(arrow_arguments.batch);
	} else {
		// A zero-row {args: struct<>} batch.
		auto args_batch =
		    vgi::VgiBatch::MakeEmpty(vgi::MakeSchema({vgi::type::Struct({}).Named("args")}));
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

	vgi::generated::AggregateBindRequest request;
	request.function_name = function_name;
	request.arguments = std::move(arguments_bytes);
	request.input_schema = std::move(schema_bytes);
	request.settings = vgi::BytesOrNull(settings_bytes);
	request.secrets = vgi::BytesOrNull(secrets_ipc_bytes);
	request.attach_opaque_data = vgi::BytesOrNull(attach_opaque_data);
	request.schema_path = vgi::OptionalSingleSchemaPath(schema_name);
	request.argument_names = argument_names;
	return vgi::generated::BuildAggregateBindParams(vgi::SerializeRecord(request));
}

vgi::VgiBatchPtr BuildAggregateUpdateRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data,
    const vgi::VgiBatchPtr &input_batch) {

	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);

	vgi::generated::AggregateUpdateRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.input_batch = std::move(batch_bytes);
	request.attach_opaque_data = vgi::BytesOrNull(attach_opaque_data);
	request.schema_path = vgi::OptionalSingleSchemaPath(schema_name);
	return vgi::generated::BuildAggregateUpdateParams(vgi::SerializeRecord(request));
}

vgi::VgiBatchPtr BuildAggregateCombineRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data,
    const vgi::VgiBatchPtr &merge_batch) {

	auto batch_bytes = vgi::SerializeToIpcBytes(merge_batch);

	vgi::generated::AggregateCombineRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.merge_batch = std::move(batch_bytes);
	request.attach_opaque_data = vgi::BytesOrNull(attach_opaque_data);
	request.schema_path = vgi::OptionalSingleSchemaPath(schema_name);
	return vgi::generated::BuildAggregateCombineParams(vgi::SerializeRecord(request));
}

vgi::VgiBatchPtr BuildAggregateFinalizeRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data,
    const vgi::VgiBatchPtr &group_ids_batch,
    const vgi::VgiSchemaPtr &output_schema) {

	auto gid_bytes = vgi::SerializeToIpcBytes(group_ids_batch);
	auto schema_bytes = vgi::SerializeSchemaToIpcBytes(output_schema);

	vgi::generated::AggregateFinalizeRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.group_ids_batch = std::move(gid_bytes);
	request.output_schema = std::move(schema_bytes);
	request.attach_opaque_data = vgi::BytesOrNull(attach_opaque_data);
	request.schema_path = vgi::OptionalSingleSchemaPath(schema_name);
	return vgi::generated::BuildAggregateFinalizeParams(vgi::SerializeRecord(request));
}

vgi::VgiBatchPtr BuildAggregateDestructorRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data,
    const vgi::VgiBatchPtr &group_ids_batch) {

	auto gid_bytes = vgi::SerializeToIpcBytes(group_ids_batch);

	vgi::generated::AggregateDestructorRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.group_ids_batch = std::move(gid_bytes);
	request.attach_opaque_data = vgi::BytesOrNull(attach_opaque_data);
	request.schema_path = vgi::OptionalSingleSchemaPath(schema_name);
	return vgi::generated::BuildAggregateDestructorParams(vgi::SerializeRecord(request));
}

} // anonymous namespace

namespace vgi {

ErrorInfo AggregateViolation(const VgiAggregateBindData &bind_data, const std::string &method_name) {
	return std::move(ErrorInfo(error_subtype::kProtocolViolation)
	                     .Rpc(method_name)
	                     .Function(bind_data.function_name, "AGGREGATE")
	                     .Worker(bind_data.attach_params->worker_path()));
}

const std::string &AggregateWorkerPath(const VgiAggregateBindData &bind_data) {
	return bind_data.attach_params->worker_path();
}

void ThrowMissingAggregateResult(const VgiAggregateBindData &bind_data, const std::string &method_name) {
	throw IOException(AggregateViolation(bind_data, method_name), "VGI %s returned no result for '%s' [worker: %s]",
	                  method_name, bind_data.function_name, AggregateWorkerPath(bind_data));
}

// ============================================================================
// Shared RPC envelope + invoker
// ============================================================================
// Moved out of the anonymous namespace so aggregate_window_impl.cpp can share
// the same connection-pool / subprocess / HTTP transport plumbing.

AggregateRpcResult InvokeAggregateRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                      const std::string &method_name,
                                      const vgi::VgiBatchPtr &params,
                                      bool enable_logging) {
	// Validate the outgoing request batch before dispatch (catches encoder drift).
	ValidateRequestSchema(params, method_name, bind_data.attach_params->worker_path());

	UnaryRpcOptions opts {context,
	                      bind_data.attach_params->worker_path(),
	                      bind_data.attach_params->worker_debug(),
	                      bind_data.attach_params->use_pool(),
	                      bind_data.attach_params->data_version_spec(),
	                      bind_data.attach_params->implementation_version(),
	                      "rpc_aggregate",
	                      bind_data.attach_params->auth(),
	                      bind_data.attach_params->cookie_jar(),
	                      enable_logging};
	opts.iroh = bind_data.attach_params->iroh();
	// The same HTTP plumbing the catalog RPCs get. Without it every aggregate
	// RPC -- one per output row for a windowed aggregate on engines without the
	// batched window callback -- rebuilt its HTTP params through the secret
	// manager and probed HEAD /health for capabilities before the POST.
	const auto &agg_worker_path = bind_data.attach_params->worker_path();
	if (IsHttpTransport(agg_worker_path) || IsHttpiTransport(agg_worker_path)) {
		if (IsHttpTransport(agg_worker_path)) {
			opts.cached_http_params = bind_data.attach_params->GetOrInitHttpParams(context, agg_worker_path);
		}
		opts.http_client_pool = bind_data.attach_params->GetOrInitHttpClientPool();
		opts.server_caps = bind_data.attach_params->server_caps();
	}
	// Forward launcher overrides for `launch:` LOCATIONs (no-op for other transports).
	if (bind_data.attach_params->launcher_idle_timeout_seconds().has_value()) {
		opts.launcher_idle_timeout =
		    std::chrono::seconds(*bind_data.attach_params->launcher_idle_timeout_seconds());
	}
	if (bind_data.attach_params->launcher_state_dir().has_value()) {
		opts.launcher_state_dir = *bind_data.attach_params->launcher_state_dir();
	}
	auto response = InvokePooledUnaryRpc(opts, method_name, params);

	// Outer envelope unwrap + schema validation against the registry.
	// The registered schema for aggregate_* methods follows vgi-python's
	// Aggregate*Response dataclasses.
	VgiBatchPtr inner;
	const auto &worker_path = bind_data.attach_params->worker_path();
	if (response.batch && response.batch->num_rows() > 0) {
		inner = UnwrapResultEnvelope(response.batch, method_name, AggregateViolation(bind_data, method_name),
		                             /*null_result_ok=*/true, GetWorkerBatchValidation(&context));
	}
	ValidateResponseSchema(inner, method_name, worker_path);
	return {response.batch, inner};
}

// ============================================================================
// VgiAggregateBindData
// ============================================================================

unique_ptr<FunctionData> VgiAggregateBindData::Copy() const {
	auto copy = make_uniq<VgiAggregateBindData>();
	copy->attach_params = attach_params;
	copy->attach_opaque_data = attach_opaque_data;
	copy->function_name = function_name;
	copy->schema_name = schema_name;
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
	// schema_name participates: the same aggregate name can be declared in two
	// schemas of one catalog and they are different implementations, so bind
	// data from one must never satisfy an Equals() check for the other.
	if (function_name != other.function_name || schema_name != other.schema_name ||
	    attach_opaque_data != other.attach_opaque_data || settings != other.settings) {
		return false;
	}
	// Null-safe const-argument comparison: Value::operator== throws
	// "Comparison on NULL values" when either side is NULL (e.g. a NULL const
	// argument like vgi_percentile(x, NULL)). NotDistinctFrom treats NULL==NULL
	// as equal and NULL vs non-null as unequal, without throwing — so the NULL
	// const flows through to the worker, which returns its own clean validation
	// error instead of an INTERNAL error.
	if (const_values.size() != other.const_values.size()) {
		return false;
	}
	for (idx_t i = 0; i < const_values.size(); i++) {
		if (!Value::NotDistinctFrom(const_values[i], other.const_values[i])) {
			return false;
		}
	}
	return true;
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
	auto context_lock = bind_data.context.lock();
	if (!context_lock) {
		throw IOException("VGI aggregate update: ClientContext is gone");
	}
	auto &context = *context_lock;

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
	auto gid_batch = BuildGroupIdBatch(UpdateGroupIdSchema(), sdata, states, count);

	// Build actual input schema from runtime types (may differ from bind-time
	// schema when varargs are present — bind schema has fixed params only)
	vector<LogicalType> actual_input_types;
	vector<string> actual_input_names;
	for (idx_t i = 0; i < input_count; i++) {
		actual_input_types.push_back(inputs[i].GetType());
		if (i < static_cast<idx_t>(bind_data.input_schema->num_fields())) {
			actual_input_names.push_back(bind_data.input_schema->field_name(static_cast<int>(i)));
		} else {
			actual_input_names.push_back("col_" + std::to_string(i));
		}
	}
	auto actual_schema = BuildArrowSchemaFromDuckDB(context, actual_input_types, actual_input_names);

	// Convert input vectors to Arrow
	vgi::VgiBatchPtr input_batch;
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
	auto full_schema = actual_schema->AddField(0, gid_batch->schema()->field(0));

	auto columns = ExportBatchColumns(*gid_batch);
	if (input_batch) {
		for (auto &column : ExportBatchColumns(*input_batch)) {
			columns.push_back(column);
		}
	}
	auto full_batch = vgi::VgiBatch::FromColumns(full_schema, static_cast<int64_t>(count), std::move(columns));

	// Build RPC request: aggregate_update(function_name, execution_id, input_batch)
	auto request = BuildAggregateUpdateRequest(bind_data.function_name, bind_data.schema_name,
	                                            bind_data.exec_state->execution_id,
	                                            bind_data.attach_opaque_data, full_batch);

	InvokeAggregateRpc(context, bind_data, "aggregate_update", request);
}

void VgiAggregateCombine(Vector &source, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto context_lock = bind_data.context.lock();
	if (!context_lock) {
		throw IOException("VGI aggregate combine: ClientContext is gone");
	}
	auto &context = *context_lock;

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
	static const auto merge_schema = vgi::MakeSchema({
	    vgi::type::Int64().Named("source_group_id"),
	    vgi::type::Int64().Named("target_group_id"),
	});
	vgi::VgiBatchBuilder merge_builder(merge_schema);
	ReserveInt64Columns(merge_builder, count);
	auto *src_col = merge_builder.column(0);
	auto *tgt_col = merge_builder.column(1);
	for (idx_t i = 0; i < count; i++) {
		auto src_idx = sdata.sel->get_index(i);
		auto tgt_idx = cdata.sel->get_index(i);
		vgi::append::Int(src_col, src_states[src_idx]->group_id);
		vgi::append::Int(tgt_col, tgt_states[tgt_idx]->group_id);
	}
	merge_builder.SetRows(static_cast<int64_t>(count));
	auto merge_batch = merge_builder.Finish();

	auto request = BuildAggregateCombineRequest(bind_data.function_name, bind_data.schema_name,
	                                             bind_data.exec_state->execution_id,
	                                             bind_data.attach_opaque_data, merge_batch);

	InvokeAggregateRpc(context, bind_data, "aggregate_combine", request);
}

void VgiAggregateFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                          idx_t offset) {
	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto context_lock = bind_data.context.lock();
	if (!context_lock) {
		throw IOException("VGI aggregate finalize: ClientContext is gone");
	}
	auto &context = *context_lock;

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

	auto gid_batch = BuildGroupIdBatch(GroupIdSchema(), sdata, states, count);

	// Call aggregate_finalize RPC
	auto request = BuildAggregateFinalizeRequest(bind_data.function_name, bind_data.schema_name,
	                                              bind_data.exec_state->execution_id,
	                                              bind_data.attach_opaque_data, gid_batch,
	                                              bind_data.resolved_output_schema);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_finalize", request);

	auto finalize = DecodeAggregateResult<generated::AggregateFinalizeResult>(rpc_result, bind_data,
	                                                                          "aggregate_finalize");
	auto result_batch = DeserializeFromIpcBytes(finalize.result_batch, GetWorkerBatchValidation(&context));

	if (!result_batch || result_batch->num_columns() != 1) {
		throw IOException(AggregateViolation(bind_data, "aggregate_finalize"), "VGI aggregate_finalize returned invalid result for '%s'", bind_data.function_name);
	}
	if (static_cast<idx_t>(result_batch->num_rows()) != count) {
		throw IOException(AggregateViolation(bind_data, "aggregate_finalize")
		                      .ExpectedActual(std::to_string(count) + " rows",
		                                      std::to_string(result_batch->num_rows()) + " rows"),
		                  "VGI aggregate_finalize returned %d rows but expected %d for '%s'",
		                  result_batch->num_rows(), count, bind_data.function_name);
	}

	// Convert Arrow result to DuckDB Vector
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	ArrowSchemaToDuckDBTypes(context, result_batch->schema(), c_schema, arrow_table, output_types, output_names);

	// Type-confusion guard: the worker's finalize result is converted (via a type
	// map built from its own schema) into a result Vector of the declared return
	// type, so a result whose type disagrees with what the worker declared at
	// bind is a misread. No projection on this path. See vgi_batch_validation.hpp.
	ValidateProjectedWireBatch(&context, *result_batch, bind_data.resolved_output_schema, {},
	                           bind_data.attach_params->worker_path(), bind_data.function_name);

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

		// Lock the weak_ptr<ClientContext>. If the originating session has
		// gone away (destructor firing on a task-scheduler thread during
		// shutdown after the session tore down), we have no context to
		// dispatch RPCs on — surrender silently. Worker-side state will
		// be reclaimed by execution_id timeout / pool eviction.
		auto context_lock = bind_data.context.lock();
		if (!context_lock) {
			return;
		}
		auto &context = *context_lock;

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

		vgi::VgiBatchBuilder gid_builder(GroupIdSchema());
		vgi::append::Int(gid_builder.column(0), 0);
		gid_builder.FinishRow();
		auto gid_batch = gid_builder.Finish();

		auto request = BuildAggregateDestructorRequest(bind_data.function_name, bind_data.schema_name,
		                                                bind_data.exec_state->execution_id,
		                                                bind_data.attach_opaque_data, gid_batch);
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
	auto &registered_info = function.function_info->Cast<VgiAggregateFunctionInfo>();

	// A function published into system.main outlives DETACH (DuckDB has no way
	// to unregister one), so its connection state is resolved from the live
	// catalog here rather than trusted from registration time: a re-ATTACH
	// refreshes it transparently, and a DETACHed catalog throws a clear error.
	shared_ptr<VgiAggregateFunctionInfo> live_info;
	if (registered_info.global) {
		auto binding = ResolveVgiGlobalBinding(context, registered_info.catalog_name, function.name);
		live_info = make_shared_ptr<VgiAggregateFunctionInfo>(registered_info);
		live_info->attach_params = binding.attach_params;
		live_info->attach_opaque_data = binding.attach_opaque_data;
		live_info->catalog = binding.catalog;
		live_info->setting_names = binding.setting_names;
	}
	const auto &func_info = live_info ? *live_info : registered_info;

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
	idx_t const_parameter_count = func_info.positional_is_const.size() - expected_non_const;
	idx_t logical_argument_count =
	    const_args_already_erased ? arguments.size() + const_parameter_count : arguments.size();
	std::vector<std::optional<std::string>> resolved_argument_names;
	resolved_argument_names.reserve(logical_argument_count);
	for (idx_t i = 0; i < logical_argument_count; i++) {
		if (i < func_info.positional_names.size()) {
			resolved_argument_names.emplace_back(func_info.positional_names[i]);
		} else {
			resolved_argument_names.emplace_back(std::nullopt);
		}
	}

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
	auto request = BuildAggregateBindRequest(func_info.function_name, func_info.schema_name,
	                                          func_info.attach_opaque_data, input_schema,
	                                          context, settings, func_info.required_secrets,
	                                          arrow_arguments, resolved_argument_names);

	auto bind_data = make_uniq<VgiAggregateBindData>();
	bind_data->attach_params = func_info.attach_params;
	bind_data->attach_opaque_data = func_info.attach_opaque_data;
	bind_data->function_name = func_info.function_name;
	bind_data->schema_name = func_info.schema_name;
	bind_data->settings = settings;
	bind_data->required_secrets = func_info.required_secrets;
	bind_data->resolved_output_schema = func_info.output_schema;
	bind_data->input_schema = input_schema;
	bind_data->catalog = func_info.catalog;
	// shared_from_this on ClientContext is safe — DuckDB owns it via shared_ptr.
	bind_data->context = context.shared_from_this();
	bind_data->const_values = const_values;

	// Call aggregate_bind to get execution_id
	auto rpc_result = InvokeAggregateRpc(context, *bind_data, "aggregate_bind", request);

	auto bind_result = DecodeAggregateResult<generated::AggregateBindResult>(rpc_result, *bind_data, "aggregate_bind");
	bind_data->exec_state->execution_id = std::move(bind_result.execution_id);
	// A non-empty output_schema overrides the declared return type (dynamic return types).
	if (!bind_result.output_schema.empty()) {
		auto schema = DeserializeSchema(bind_result.output_schema);
		if (schema && schema->num_fields() > 0) {
			bind_data->resolved_output_schema = schema;
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> out_types;
			vector<string> out_names;
			ArrowSchemaToDuckDBTypes(context, bind_data->resolved_output_schema, c_schema, arrow_table, out_types,
			                         out_names);
			if (!out_types.empty()) {
				function.return_type = out_types[0];
			}
		}
	}

	// Verify execution_id was extracted — without it, all queries share FunctionStorage state
	if (bind_data->exec_state->execution_id.empty()) {
		throw IOException(AggregateViolation(*bind_data, "aggregate_bind").Set(error_key::kField, "execution_id"),
		                  "VGI aggregate_bind did not return execution_id for '%s'", bind_data->function_name);
	}

	return bind_data;
}

} // namespace vgi
} // namespace duckdb
