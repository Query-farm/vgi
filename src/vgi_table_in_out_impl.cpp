// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_in_out_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_client_timing.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"
// VgiSerializeFilters lives in vgi_table_function_impl.hpp — public utility
// (declared at line 528 of that header). C3b reuses it on the streaming
// TableInOut bind path; same serialization as the pure-table path so a
// single filter format flows to all worker shapes.
#include "vgi_table_function_impl.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Bind Data for Table-In-Out Functions
// ============================================================================

VgiTableInOutBindData::VgiTableInOutBindData() = default;

VgiTableInOutBindData::~VgiTableInOutBindData() = default;

unique_ptr<FunctionData> VgiTableInOutBindData::Copy() const {
	auto copy = make_uniq<VgiTableInOutBindData>();
	copy->attach_params = attach_params;
	copy->attach_opaque_data = attach_opaque_data;
	copy->transaction_opaque_data = transaction_opaque_data;
	copy->function_name = function_name;
	copy->settings = settings;
	copy->arguments = arguments;
	copy->output_schema = output_schema;
	copy->input_schema = input_schema;
	copy->bind_result = bind_result;
	copy->max_processes = max_processes;
	copy->cardinality_estimate = cardinality_estimate;
	copy->table_buffering = table_buffering;
	copy->source_order_dependent = source_order_dependent;
	copy->sink_order_dependent = sink_order_dependent;
	copy->requires_input_batch_index = requires_input_batch_index;
	return copy;
}

bool VgiTableInOutBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<VgiTableInOutBindData>();
	return worker_path() == other.worker_path() && function_name == other.function_name && attach_opaque_data == other.attach_opaque_data;
}

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

VgiTableInOutGlobalState::VgiTableInOutGlobalState(DatabaseInstance *db_p) : db(db_p) {
}

VgiTableInOutGlobalState::~VgiTableInOutGlobalState() {
	if (!connection) {
		return;
	}
	// Stream reached a natural end or cancel was already sent —
	// no further action needed. connection drops via unique_ptr.
	if (stream_finished || !cancel_enabled || !db) {
		return;
	}
	auto *dispatcher = FindVgiCancelDispatcher(*db);
	if (!dispatcher) {
		return;
	}
	// Snapshot the token from the connection before moving it; HTTP
	// tracks state-tokens internally per exchange.
	auto token = connection->GetLastStateToken();
	CancelRequest req;
	req.connection = std::move(connection);
	req.state_token = !token.empty() ? std::move(token) : last_state_token;
	if (!dispatcher->Enqueue(std::move(req))) {
		// Saturation: let the unique_ptr (still in req) drop the
		// connection; cancel is best-effort.
	}
}

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

VgiTableInOutLocalState::~VgiTableInOutLocalState() = default;

// ============================================================================
// Bind Function
// ============================================================================

unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            const VgiTableInOutBindParams &params) {
	auto bind_data = make_uniq<VgiTableInOutBindData>();

	// Copy connection parameters
	bind_data->attach_params = params.attach_params;
	bind_data->attach_opaque_data = params.attach_opaque_data;
	bind_data->transaction_opaque_data = params.transaction_opaque_data;
	bind_data->function_name = params.function_name;
	bind_data->settings = params.settings;
	bind_data->required_secrets = params.required_secrets;
	bind_data->table_buffering = params.table_buffering;
	bind_data->source_order_dependent = params.source_order_dependent;
	bind_data->sink_order_dependent = params.sink_order_dependent;
	bind_data->requires_input_batch_index = params.requires_input_batch_index;
	bind_data->projection_pushdown = params.projection_pushdown;
	bind_data->filter_pushdown = params.filter_pushdown;

	// Build arguments from the regular (non-TABLE) inputs
	// input.inputs contains positional arguments, but TABLE arguments are represented as NULL
	// We skip NULL values since they represent the TABLE input (not a scalar argument)
	// Named arguments are in input.named_parameters
	vector<Value> positional_args;
	for (auto &val : input.inputs) {
		// Skip NULL values - these represent TABLE arguments
		if (val.IsNull()) {
			continue;
		}
		positional_args.push_back(val);
	}
	vector<std::pair<string, Value>> named_args;
	for (auto &[name, value] : input.named_parameters) {
		named_args.emplace_back(name, value);
	}
	bind_data->arguments = BuildArgumentsFromValues(context, positional_args, named_args);

	// Build the input schema from the table input types/names
	bind_data->input_schema = BuildArrowSchemaFromDuckDB(context, input.input_table_types, input.input_table_names);

	VGI_LOG(context, "table_in_out.bind",
	        {{"worker_path", bind_data->worker_path()},
	         {"function_name", bind_data->function_name},
	         {"input_columns", std::to_string(input.input_table_types.size())}});

	// Create the connection and perform bind
	// Try pool first (only for subprocess transport)
	std::unique_ptr<IFunctionConnection> connection;
	bool from_pool = false;
	if (bind_data->use_pool() && !IsHttpTransport(bind_data->worker_path())) {
		PoolKey pool_key {bind_data->worker_path(), bind_data->data_version_spec(),
		                  bind_data->implementation_version()};
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			from_pool = true;
			VGI_LOG(context, "table_in_out.pool_acquire",
			        {{"worker_path", bind_data->worker_path()},
			         {"function_name", bind_data->function_name},
			         {"from_pool", "true"},
			         {"pid", std::to_string(pooled->GetPid())}});
			connection = CreateFunctionConnectionFromPool(std::move(pooled), bind_data->function_name,
			                                              bind_data->arguments, bind_data->attach_opaque_data,
			                                              bind_data->transaction_opaque_data, context,
			                                              "TABLE", std::vector<uint8_t>{},
			                                              bind_data->worker_debug(), bind_data->settings,
			                                              bind_data->required_secrets);
		}
	}
	if (!connection) {
		connection = CreateFunctionConnection(bind_data->worker_path(), bind_data->function_name,
		                                      bind_data->arguments, bind_data->attach_opaque_data,
		                                      bind_data->transaction_opaque_data, context,
		                                      "TABLE", std::vector<uint8_t>{},
		                                      bind_data->worker_debug(), bind_data->settings,
		                                      bind_data->required_secrets, bind_data->attach_params);
		if (!from_pool) {
			VGI_LOG(context, "table_in_out.pool_acquire",
			        {{"worker_path", bind_data->worker_path()},
			         {"function_name", bind_data->function_name},
			         {"from_pool", "false"}});
		}
	}

	// Set the input schema for table-in-out functions
	connection->SetInputSchema(bind_data->input_schema);

	// Perform bind
	auto bind_result = connection->PerformBindRpc();

	// Store output schema (max_processes and cardinality_estimate set from init result later)
	bind_data->output_schema = bind_result.output_schema;
	bind_data->max_processes = 1;
	bind_data->cardinality_estimate = -1;

	// Retain the full BindResult so InitGlobal can call PerformInit without
	// re-running a redundant bind RPC.
	bind_data->bind_result = bind_result;

	// Convert Arrow schema to DuckDB return types (stored for ArrowToDuckDB in scan)
	ArrowSchemaToDuckDBTypes(context, bind_data->output_schema, bind_data->c_schema, bind_data->arrow_table,
	                         return_types, names);

	// Release the bind worker back to the pool. InitGlobal will re-acquire
	// (typically pool-hit, paying only a cheap bind RPC) and then call
	// PerformInit(phase=INPUT). Holding the worker through init would force
	// ancillary planner RPCs to spawn fresh workers.
	if (bind_data->use_pool()) {
		auto bind_worker_pid = connection->GetSubprocessPid().value_or(-1);
		auto bind_conn_id = connection->GetConnIdHex();
		if (auto pooled = connection->ReleaseForPooling()) {
			auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
			PoolReleaseLogFields lf;
			lf.conn_id = bind_conn_id;
			lf.worker_path = bind_data->worker_path();
			lf.worker_pid = bind_worker_pid;
			lf.phase = "bind";
			LogWorkerPoolRelease(context, lf, rr.pooled, rr.skip_reason, rr.pool_size, rr.total_pool_size);
		}
		connection.reset();
	}

	VGI_LOG(context, "table_in_out.bind_complete",
	        {{"worker_path", bind_data->worker_path()},
	         {"function_name", bind_data->function_name},
	         {"output_columns", std::to_string(return_types.size())}});

	return bind_data;
}

// ============================================================================
// Init Functions
// ============================================================================

unique_ptr<GlobalTableFunctionState> VgiTableInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableInOutBindData>();
	auto global_state = make_uniq<VgiTableInOutGlobalState>(context.db.get());
	{
		Value v;
		if (context.TryGetCurrentSetting("vgi_cancel_enabled", v)) {
			global_state->cancel_enabled = v.GetValue<bool>();
		}
	}

	// Acquire a connection without firing a redundant bind RPC. The
	// planner-phase BindResult is already cached on bind_data; PerformInit's
	// payload carries it inline. AcquireConnectionForInit pool-hits when
	// possible (subprocess) or constructs a fresh HTTP connection, and wires
	// SetInputSchema for the typed-input-stream protocol.
	FunctionConnectionParams acquire_params;
	acquire_params.attach_params = bind_data.attach_params;
	acquire_params.attach_opaque_data = bind_data.attach_opaque_data;
	acquire_params.function_name = bind_data.function_name;
	acquire_params.arguments = bind_data.arguments;
	acquire_params.transaction_opaque_data = bind_data.transaction_opaque_data;
	acquire_params.settings = bind_data.settings;
	acquire_params.required_secrets = bind_data.required_secrets;
	acquire_params.phase = "init_global";
	acquire_params.function_type = "TABLE";
	acquire_params.input_schema = bind_data.input_schema;
	auto acquired = AcquireConnectionForInit(context, acquire_params);
	auto connection = std::move(acquired.connection);
	global_state->bind_result = bind_data.bind_result;

	// Capture projection + filter pushdown from the TableFunctionInitInput.
	// Mirrors the streaming pure-table path at
	// ``vgi_table_function_impl.cpp:1023-1090``. The capability flags
	// (advertised in ``vgi_table_function_set.cpp``) gate whether DuckDB
	// populated ``input.column_ids`` / ``input.filters`` with non-trivial
	// pushdown state; without them, column_ids is a single virtual column
	// placeholder and filters is null.
	if (bind_data.projection_pushdown && !input.column_ids.empty()) {
		// Save the DuckDB-side column_ids list. Threaded onto LocalState
		// at InitLocal so ``ProduceOutputFromBatch`` can drive
		// ``ArrowToDuckDB`` with arrow_scan_is_projected=true and
		// the right per-column-type lookups.
		global_state->column_ids.reserve(input.column_ids.size());
		for (auto col_id : input.column_ids) {
			global_state->column_ids.push_back(col_id);
		}
		global_state->projection_ids.reserve(input.column_ids.size());
		for (auto col_id : input.column_ids) {
			global_state->projection_ids.push_back(static_cast<int32_t>(col_id));
		}
	}
	// Note: DuckDB's planner discards ``LogicalGet.table_filters`` for the
	// InOut path (see plan_get.cpp:37-83), so ``input.filters`` is always
	// null here even when ``bind_data.filter_pushdown`` is True. The
	// streaming TableInOut path therefore only supports projection
	// pushdown end-to-end; filters fall back to a separate FILTER node
	// above the operator. The serialization below is guarded on
	// ``input.filters != nullptr`` so the branch is a no-op today but
	// ready if upstream DuckDB grows InOut filter-pushdown support.
	if (bind_data.filter_pushdown && bind_data.output_schema && input.filters) {
		// VgiSerializeFilters indexes filter column refs through
		// ``column_ids`` → ``column_names``. The streaming pure-table
		// path passes ``bind_data.all_column_names``; the InOut path's
		// equivalent is the worker's full output schema.
		vector<string> output_names;
		output_names.reserve(bind_data.output_schema->num_fields());
		for (int i = 0; i < bind_data.output_schema->num_fields(); ++i) {
			output_names.push_back(bind_data.output_schema->field(i)->name());
		}
		try {
			auto serialized = vgi::VgiSerializeFilters(
			    context, input.column_ids, input.filters,
			    output_names, bind_data.worker_path());
			global_state->static_filter_bytes = std::move(serialized.filter_bytes);
			global_state->join_keys_buffers = std::move(serialized.join_keys_buffers);
			if (global_state->static_filter_bytes) {
				VGI_LOG(context, "table_in_out.filters_serialized",
				        {{"function_name", bind_data.function_name},
				         {"filter_bytes_size",
				          std::to_string(global_state->static_filter_bytes->size())}});
			}
		} catch (const InvalidInputException &e) {
			// Unsupported filter — skip pushdown; DuckDB filters locally.
			VGI_LOG(context, "table_in_out.filter_pushdown_skipped",
			        {{"function_name", bind_data.function_name}, {"reason", e.what()}});
		}
	}

	// Perform init with phase=INPUT for table-in-out functions. Init is the
	// first RPC after acquire, so stale-pool detection lives here: a pooled
	// subprocess that died while idle surfaces as IOException, and we retry
	// once with a forced-fresh connection. HTTP transport never enters the
	// retry branch (from_pool is always false there).
	InitResult init_result;
	try {
		init_result = connection->PerformInit(global_state->bind_result,
		                                       global_state->projection_ids,
		                                       global_state->static_filter_bytes,
		                                       global_state->join_keys_buffers, "INPUT");
	} catch (const IOException &e) {
		if (!acquired.from_pool) {
			throw;
		}
		VGI_LOG(context, "worker_pool.stale",
		        {{"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"phase", "init_global"},
		         {"error", e.what()}});
		acquired = AcquireConnectionForInit(context, acquire_params, /*force_fresh=*/true);
		connection = std::move(acquired.connection);
		init_result = connection->PerformInit(global_state->bind_result,
		                                       global_state->projection_ids,
		                                       global_state->static_filter_bytes,
		                                       global_state->join_keys_buffers, "INPUT");
	}
	global_state->global_execution_id = std::move(init_result.execution_id);

	// Open the input writer for Stream 5
	connection->OpenInputWriter();

	// Store the connection
	global_state->connection = std::move(connection);

	VGI_LOG(context, "table_in_out.init_global",
	        {{"worker_path", bind_data.worker_path()}, {"function_name", bind_data.function_name}});

	return global_state;
}

unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state_p) {
	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableInOutLocalState>(std::move(current_chunk), context.client);
	// Thread projection state captured at InitGlobal onto the local scan
	// state so ``ProduceOutputFromBatch`` can drive ``ArrowToDuckDB`` with
	// ``arrow_scan_is_projected=true`` and the right per-column-type
	// lookups. ``ArrowScanLocalState::column_ids`` (inherited) is what
	// ``ArrowToDuckDB`` reads to remap output positions to worker-original
	// column indices. Empty list means "no projection" — positional read.
	if (global_state_p != nullptr) {
		auto &gstate = global_state_p->Cast<VgiTableInOutGlobalState>();
		if (!gstate.column_ids.empty()) {
			local_state->column_ids = gstate.column_ids;
		}
	}
	return local_state;
}

// ============================================================================
// Arrow Batch to DuckDB DataChunk Conversion Helpers
// ============================================================================

//! Load an Arrow C++ RecordBatch into the ArrowScanLocalState for consumption.
//! Must call Reset() to clear stale owned_data references before loading a new batch.
static void LoadBatchIntoScanState(VgiTableInOutLocalState &local_state,
                                   const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(batch, *chunk);
	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();
}

//! Check whether the local state has remaining rows to produce from the current batch.
static bool HasRemainingBatchData(const VgiTableInOutLocalState &local_state) {
	return local_state.chunk && local_state.chunk->arrow_array.release &&
	       local_state.chunk_offset < static_cast<idx_t>(local_state.chunk->arrow_array.length);
}

//! Produce one DataChunk (up to STANDARD_VECTOR_SIZE rows) from the current batch,
//! advancing chunk_offset. Returns the number of rows produced.
//!
//! The destination ``output`` chunk may have MORE columns than the worker's
//! output (e.g., under LATERAL, DuckDB's PhysicalTableInOutFunction appends
//! correlated/projected-input columns to the chunk and expects the table
//! in-out function to leave them alone). ``ArrowTableFunction::ArrowToDuckDB``
//! iterates all of ``output.ColumnCount()`` columns, which would walk off the
//! end of the Arrow batch's child array. To handle that, we build a temporary
//! ``DataChunk`` that references only the first ``n_children`` vectors of
//! ``output`` and convert into that. Writes land in the shared underlying
//! vector buffers; the trailing correlated columns stay untouched.
static idx_t ProduceOutputFromBatch(VgiTableInOutLocalState &local_state, const ArrowTableSchema &arrow_table,
                                    DataChunk &output, bool projection_pushdown = false) {
	idx_t remaining = static_cast<idx_t>(local_state.chunk->arrow_array.length) - local_state.chunk_offset;
	idx_t output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
	output.SetCardinality(output_size);

	idx_t fn_columns = static_cast<idx_t>(local_state.chunk->arrow_array.n_children);
	D_ASSERT(fn_columns <= output.ColumnCount());
	// Under projection_pushdown, the worker emitted a narrow Arrow batch
	// (positional columns matching the projection_ids order). The arrow
	// type map ``arrow_table.GetColumns()`` is keyed by *worker-original*
	// column index (built from the full output schema at bind), so reading
	// it requires the remap-via-column_ids that
	// ``arrow_scan_is_projected=true`` enables (see
	// ``duckdb/src/function/table/arrow_conversion.cpp`` —
	// ``ArrowToDuckDB`` reads ``col_idx = column_ids[idx]`` for the type
	// map and ``arrow_array_idx = idx`` for the data).
	if (fn_columns == output.ColumnCount()) {
		ArrowTableFunction::ArrowToDuckDB(local_state, arrow_table.GetColumns(), output, projection_pushdown);
	} else {
		// LATERAL / projected-input path: DuckDB's PhysicalTableInOutFunction
		// appended correlated outer columns at ``[fn_columns, ColumnCount)``
		// and pre-filled them before calling us. ``ArrowToDuckDB`` iterates
		// ``output.ColumnCount()`` and during zero-copy conversion *rebinds*
		// each destination Vector's buffer (see
		// ``duckdb/src/function/table/arrow_conversion.cpp``), so a
		// ``Reference``-based view over the leading columns of ``output``
		// would leave ``output.data[c]`` unchanged — the rebind would land on
		// the view's Vector object and never propagate back.
		//
		// Instead, convert into a freshly-allocated scratch chunk and then
		// ``output.data[c].Reference(scratch.data[c])``. Vector buffer
		// refcounting keeps the Arrow data alive after ``scratch`` goes out
		// of scope, and the trailing LATERAL-projected columns are
		// untouched. With projection_pushdown the scratch's types come
		// from ``output.data[c]`` which are the post-projection slot
		// types — already correct.
		DataChunk scratch;
		vector<LogicalType> scratch_types;
		scratch_types.reserve(fn_columns);
		for (idx_t c = 0; c < fn_columns; c++) {
			scratch_types.push_back(output.data[c].GetType());
		}
		scratch.Initialize(BufferAllocator::Get(local_state.context), scratch_types, output_size);
		scratch.SetCardinality(output_size);
		ArrowTableFunction::ArrowToDuckDB(local_state, arrow_table.GetColumns(), scratch, projection_pushdown);
		for (idx_t c = 0; c < fn_columns; c++) {
			output.data[c].Reference(scratch.data[c]);
		}
	}

	local_state.chunk_offset += output.size();
	return output.size();
}

// ============================================================================
// In-Out Function (Main Processing)
// ============================================================================

OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableInOutBindData>();
	auto &global_state = data.global_state->Cast<VgiTableInOutGlobalState>();
	auto &local_state = data.local_state->Cast<VgiTableInOutLocalState>();
	auto &client_context = context.client;

	// Loud-failure guard: a table_buffering function should never reach the
	// streaming in_out_function callback. The optimizer extension rewrites
	// LogicalGet → LogicalVgiTableBufferingFunction at optimize_function
	// time, so reaching here means the rewriter was disabled or buggy.
	// Catch this loudly rather than silently regressing to the broken
	// per-pipeline-finalize semantics that the buffered path is meant to fix.
	if (bind_data.table_buffering) {
		// `vgi_table_buffering=false` deliberately routes us here so users can
		// roll back to the streaming path if the rewriter has a bug. Throw an
		// `InvalidInputException` (recoverable; doesn't kill the session)
		// instead of `InternalException`.
		throw InvalidInputException(
		    "Function '%s' is a TableBufferingFunction subclass but the "
		    "vgi_table_buffering rewriter did not fire (likely SET vgi_table_buffering=false). "
		    "Re-enable with `SET vgi_table_buffering=true` or migrate the worker to a "
		    "streaming TableInOutGenerator shape.", bind_data.function_name);
	}

	if (!global_state.connection) {
		throw InternalException("VgiTableInOutFunction: connection is null");
	}

	// Continue producing rows from a batch that exceeded STANDARD_VECTOR_SIZE
	if (HasRemainingBatchData(local_state)) {
		idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
		VGI_LOG(client_context, "table_in_out.read_output",
		        {{"conn", global_state.connection->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"output_rows", std::to_string(rows_copied)}});
		return HasRemainingBatchData(local_state) ? OperatorResultType::HAVE_MORE_OUTPUT
		                                          : OperatorResultType::NEED_MORE_INPUT;
	}

	// Convert input DataChunk to Arrow RecordBatch
	const bool timing = ClientTiming::Enabled();
	std::shared_ptr<arrow::RecordBatch> input_batch;
	if (timing) {
		ScopedNs _t(ClientTiming::Instance().convert_in_ns);
		input_batch = DataChunkToArrow(client_context, input, bind_data.input_schema);
	} else {
		input_batch = DataChunkToArrow(client_context, input, bind_data.input_schema);
	}

	VGI_LOG(client_context, "table_in_out.write_input",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"input_rows", std::to_string(input_batch->num_rows())}});

	// Write the input batch to the worker
	if (timing) {
		ScopedNs _t(ClientTiming::Instance().write_ns);
		global_state.connection->WriteInputBatch(input_batch);
	} else {
		global_state.connection->WriteInputBatch(input_batch);
	}

	// Read output batch (1:1 lockstep in exchange mode)
	std::shared_ptr<arrow::RecordBatch> output_batch;
	if (timing) {
		ScopedNs _t(ClientTiming::Instance().read_ns);
		output_batch = global_state.connection->ReadDataBatch();
	} else {
		output_batch = global_state.connection->ReadDataBatch();
	}

	if (!output_batch) {
		// EOS - stream exhausted
		output.SetCardinality(0);
		global_state.stream_finished = true;
		return OperatorResultType::FINISHED;
	}

	// Empty batch (0 rows) - worker consumed input but has no output yet
	if (output_batch->num_rows() == 0) {
		output.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Load batch into scan state and produce output
	idx_t rows_copied;
	if (timing) {
		ScopedNs _t(ClientTiming::Instance().convert_out_ns);
		LoadBatchIntoScanState(local_state, output_batch);
		rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
		ClientTiming::Instance().batches.fetch_add(1, std::memory_order_relaxed);
	} else {
		LoadBatchIntoScanState(local_state, output_batch);
		rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
	}

	VGI_LOG(client_context, "table_in_out.read_output",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	return HasRemainingBatchData(local_state) ? OperatorResultType::HAVE_MORE_OUTPUT
	                                          : OperatorResultType::NEED_MORE_INPUT;
}

// ============================================================================
// Finalize Function
// ============================================================================

OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableInOutBindData>();
	auto &global_state = data.global_state->Cast<VgiTableInOutGlobalState>();
	auto &local_state = data.local_state->Cast<VgiTableInOutLocalState>();
	auto &client_context = context.client;

	// See the matching guard in VgiTableInOutFunction. A table_buffering
	// function reaching the streaming finalize callback would re-introduce
	// the per-pipeline-finalize bug — fail loudly.
	if (bind_data.table_buffering) {
		throw InvalidInputException(
		    "Function '%s' is a TableBufferingFunction subclass but the "
		    "vgi_table_buffering rewriter did not fire (likely SET vgi_table_buffering=false). "
		    "Re-enable with `SET vgi_table_buffering=true` or migrate the worker to a "
		    "streaming TableInOutGenerator shape.", bind_data.function_name);
	}

	if (!global_state.connection) {
		global_state.stream_finished = true;
		return OperatorFinalizeResultType::FINISHED;
	}

	// Perform finalize init (only once)
	// This closes current data streams and opens new init with phase=FINALIZE
	// The worker enters producer mode (tick-based) to emit any finalize output
	if (global_state.connection->IsTableInOut() && !global_state.connection->IsFinished() && !global_state.finalize_sent) {
		global_state.connection->PerformFinalizeInit(global_state.bind_result);
		global_state.finalize_sent = true;

		VGI_LOG(client_context, "table_in_out.finalize",
		        {{"conn", global_state.connection->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name}});
	}

	// Continue producing rows from a batch that exceeded STANDARD_VECTOR_SIZE
	if (HasRemainingBatchData(local_state)) {
		idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
		VGI_LOG(client_context, "table_in_out.finalize_output",
		        {{"conn", global_state.connection->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"output_rows", std::to_string(rows_copied)}});
		// Always return HAVE_MORE_OUTPUT — next call will either continue this batch
		// or read the next one from the worker
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	// Read next output batch from worker
	VGI_LOG(client_context, "table_in_out.finalize_reading",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name}});
	auto output_batch = global_state.connection->ReadDataBatch();

	if (!output_batch || output_batch->num_rows() == 0) {
		// No more output - clean up
		if (bind_data.use_pool() && global_state.connection) {
			auto release_conn_id = global_state.connection->GetConnIdHex();
			if (auto pooled = global_state.connection->ReleaseForPooling()) {
				auto released_pid = pooled->GetPid();
				auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
				PoolReleaseLogFields lf;
				lf.conn_id = release_conn_id;
				lf.worker_path = bind_data.worker_path();
				lf.function_name = bind_data.function_name;
				lf.worker_pid = released_pid;
				lf.event_name = "table_in_out.pool_release";
				LogWorkerPoolRelease(client_context, lf, rr.pooled, rr.skip_reason, rr.pool_size,
				                     rr.total_pool_size);
			}
		}
		global_state.stream_finished = true;
		return OperatorFinalizeResultType::FINISHED;
	}

	// Load batch into scan state and produce output
	LoadBatchIntoScanState(local_state, output_batch);
	idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);

	VGI_LOG(client_context, "table_in_out.finalize_output",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	// Check if worker is finished and we've exhausted the current batch
	if (!HasRemainingBatchData(local_state) && global_state.connection->IsFinished()) {
		if (bind_data.use_pool()) {
			auto release_conn_id = global_state.connection->GetConnIdHex();
			if (auto pooled = global_state.connection->ReleaseForPooling()) {
				auto released_pid = pooled->GetPid();
				auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
				PoolReleaseLogFields lf;
				lf.conn_id = release_conn_id;
				lf.worker_path = bind_data.worker_path();
				lf.function_name = bind_data.function_name;
				lf.worker_pid = released_pid;
				lf.event_name = "table_in_out.pool_release";
				LogWorkerPoolRelease(client_context, lf, rr.pooled, rr.skip_reason, rr.pool_size,
				                     rr.total_pool_size);
			}
		}
		global_state.stream_finished = true;
		return OperatorFinalizeResultType::FINISHED;
	}

	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
