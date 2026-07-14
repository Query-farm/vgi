// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_in_out_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_client_timing.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_exchange_cache_key.hpp" // exchange-mode result cache (M1)
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
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

#include <atomic>
#include <random>

namespace duckdb {
namespace vgi {

// Mint a stable, process-unique id for one streaming table-in-out substream.
// Layout: 8-byte process salt (random, once per process) || 8-byte monotonic
// counter, little-endian. The salt makes ids unique ACROSS DuckDB processes
// that share one HTTP worker fleet + shared storage (so two processes' substream
// #1 never collide); the counter makes them unique WITHIN a process (across
// substreams and across queries reusing the same attach). Per-substream worker
// state is additionally attach-sharded worker-side, so uniqueness only has to
// hold within a shared-storage namespace — this comfortably does.
std::vector<uint8_t> MintSubstreamId() {
	static const uint64_t salt = [] {
		std::random_device rd;
		return (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
	}();
	static std::atomic<uint64_t> counter {0};
	uint64_t n = counter.fetch_add(1, std::memory_order_relaxed);
	std::vector<uint8_t> id(16);
	for (int i = 0; i < 8; ++i) {
		id[i] = static_cast<uint8_t>((salt >> (8 * i)) & 0xFF);
		id[8 + i] = static_cast<uint8_t>((n >> (8 * i)) & 0xFF);
	}
	return id;
}

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
	copy->parallel_safe = parallel_safe;
	copy->has_finalize = has_finalize;
	copy->input_from_args = input_from_args;
	copy->single_row_scan = single_row_scan;
	copy->declared_input_types = declared_input_types;
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
	// Phase A: a streaming table-in-out function is a per-substream map, so it
	// parallelizes by default — one worker per substream (per PipelineExecutor).
	// This holds EVEN with a finalize callback: with per-substream workers each
	// FinalExecute runs on the substream's OWN worker over its OWN state, so the
	// per-substream finalize is correct (the DuckDB #18222 corruption was a
	// *shared* worker seeing N FinalExecutes; that is exactly what per-substream
	// fan-out removes). A global cross-substream combine is NOT a streaming
	// table-in-out — it is a TableBufferingFunction. `has_finalize` (derived from
	// the actual registered callback, the single source of truth) tells Execute
	// to leave the connection open at input-EOS so the per-substream FINALIZE can
	// reuse it. `max_workers=1` (A3) is the serial opt-out for a not-yet-migrated
	// worker.
	bind_data->has_finalize = static_cast<bool>(input.table_function.in_out_function_final);
	// A3 serial opt-out: Meta.max_workers=1 forces the single-shared-worker path
	// (MaxThreads()=1). 0/unset or >1 keeps the parallel-by-default per-substream
	// fan-out. (A hard N-worker cap for max_workers>1 is not modeled here — the
	// per-substream model spawns one worker per substream; only the ==1 serial
	// case is honored, matching the pure-table path's use of the same field.)
	bind_data->parallel_safe = (params.max_workers != 1);

	bind_data->input_from_args = params.input_from_args;

	// Build arguments from the regular (non-TABLE) inputs.
	// input.inputs contains positional arguments, but TABLE arguments are represented as NULL.
	// We skip NULL values since they represent the TABLE input (not a scalar argument).
	// Named arguments are in input.named_parameters.
	//
	// BLENDED (input_from_args): the positional inputs ARE the per-row input
	// columns (delivered by DuckDB's synthesized input chunk), NOT bind arguments —
	// skip ALL of them. Only named args survive as bind-time scalars.
	vector<Value> positional_args;
	if (!params.input_from_args) {
		for (auto &val : input.inputs) {
			// Skip NULL values - these represent TABLE arguments
			if (val.IsNull()) {
				continue;
			}
			positional_args.push_back(val);
		}
	}
	vector<std::pair<string, Value>> named_args;
	for (auto &[name, value] : input.named_parameters) {
		named_args.emplace_back(name, value);
	}
	bind_data->arguments = BuildArgumentsFromValues(context, positional_args, named_args);

	// Build the input schema.
	//
	// BLENDED: use the DECLARED positional arg names (the worker reads columns by
	// those names) with the input-provided types. IGNORE input.input_table_names —
	// it is empty in the literal shape (f(52,13) -> ["",""]) and would otherwise
	// name the worker's input columns after the referenced columns in the column
	// shape. For a pure-VARARGS blended function the declared names don't cover the
	// N runtime columns, so fall back to generated col0..colN-1.
	if (params.input_from_args) {
		vector<string> in_names;
		vector<LogicalType> in_types;
		const idx_t n_cols = input.input_table_types.size();
		const bool fixed_names_match = !params.has_varargs && params.positional_input_names.size() == n_cols;
		for (idx_t i = 0; i < n_cols; i++) {
			// Names: declared arg names for fixed arity, else generated col0..colN-1.
			in_names.push_back(fixed_names_match ? params.positional_input_names[i] : ("col" + std::to_string(i)));
			// Types: the DECLARED arg types, NOT input.input_table_types — so the
			// worker always receives its declared arg types (a literal delivers the
			// constant's natural type, e.g. DECIMAL for 52.0 / INTEGER for 52, while
			// a column already casts to the signature). Fixed args use their declared
			// type; varargs columns use the vararg element type; fall back to the
			// incoming type only if neither is available (shouldn't happen).
			if (i < params.positional_input_types.size()) {
				in_types.push_back(params.positional_input_types[i]);
			} else if (params.has_varargs && params.varargs_input_type.id() != LogicalTypeId::INVALID) {
				in_types.push_back(params.varargs_input_type);
			} else {
				in_types.push_back(input.input_table_types[i]);
			}
		}
		bind_data->declared_input_types = in_types;
		bind_data->input_schema = BuildArrowSchemaFromDuckDB(context, in_types, in_names);
		// Childless call shape (literal f(52,13) or a pure-varargs childless call)
		// -> PhysicalTableScan, which needs the write-once scan-mode in Execute.
		// Robust signal: all synthesized input names are empty (the column/LATERAL
		// shape gives non-empty names and a streaming child). Do NOT key on
		// !input.inputs.empty() (false-negatives the zero-positional childless call).
		bool all_names_empty = true;
		for (auto &nm : input.input_table_names) {
			if (!nm.empty()) {
				all_names_empty = false;
				break;
			}
		}
		bind_data->single_row_scan = all_names_empty;
	} else {
		bind_data->input_schema = BuildArrowSchemaFromDuckDB(context, input.input_table_types, input.input_table_names);
	}

	VGI_LOG(context, "table_in_out.bind",
	        {{"worker_path", bind_data->worker_path()},
	         {"function_name", bind_data->function_name},
	         {"input_from_args", params.input_from_args ? "true" : "false"},
	         {"single_row_scan", bind_data->single_row_scan ? "true" : "false"},
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
	// Phase A: MaxThreads() reads this. Derived at bind from the finalize callback.
	global_state->parallel_safe = bind_data.parallel_safe;
	{
		Value v;
		if (context.TryGetCurrentSetting("vgi_cancel_enabled", v)) {
			global_state->cancel_enabled = v.GetValue<bool>();
		}
	}

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

	// Phase A: a parallel (no-finalize) function fans out one worker PER SUBSTREAM
	// — each VgiTableInOutLocalState lazily acquires + inits its own pooled worker
	// in VgiTableInOutFunction (no shared global connection, no exchange mutex),
	// so skip the shared-connection acquire here. A finalize function keeps the
	// single shared connection (serial path below).
	if (!global_state->parallel_safe) {
		// Acquire a connection without firing a redundant bind RPC. The
		// planner-phase BindResult is already cached on bind_data; PerformInit's
		// payload carries it inline. AcquireConnectionForInit pool-hits when
		// possible (subprocess) or constructs a fresh HTTP connection.
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

		// Perform init with phase=INPUT. Init is the first RPC after acquire, so
		// stale-pool detection lives here: a pooled subprocess that died while idle
		// surfaces as IOException, and we retry once with a forced-fresh connection.
		InitResult init_result;
		try {
			init_result = connection->PerformInit(global_state->bind_result, global_state->projection_ids,
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
			init_result = connection->PerformInit(global_state->bind_result, global_state->projection_ids,
			                                      global_state->static_filter_bytes,
			                                      global_state->join_keys_buffers, "INPUT");
		}
		global_state->global_execution_id = std::move(init_result.execution_id);
		connection->OpenInputWriter();
		global_state->connection = std::move(connection);
	}

	VGI_LOG(context, "table_in_out.init_global",
	        {{"worker_path", bind_data.worker_path()}, {"function_name", bind_data.function_name}});

	// Exchange-mode result cache (M1): eligible only for a parallel, no-finalize
	// streaming map (output depends solely on the current input batch — a pure
	// per-batch function; the worker asserts this by advertising vgi.cache.*). The
	// serial/finalize path (shared worker, cross-batch state) and the literal
	// scan-mode path are excluded. Build the STATIC key once; the per-batch input
	// hash is folded in at each exchange.
	if (bind_data.parallel_safe && !bind_data.has_finalize && !bind_data.single_row_scan) {
		const char *reason = nullptr;
		int64_t catalog_version = 0; // out param; already folded into cache_key
		if (BuildExchangeCacheKeyStatic(context, bind_data, global_state->projection_ids,
		                                global_state->cache_key, global_state->cache_catalog_name,
		                                catalog_version, reason)) {
			global_state->cache_eligible = true;
			Value ttl_v;
			if (context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", ttl_v)) {
				global_state->cache_default_ttl_seconds = static_cast<int64_t>(ttl_v.GetValue<uint64_t>());
			}
		} else if (reason) {
			VGI_LOG(context, "result_cache.ineligible",
			        {{"function", bind_data.function_name}, {"reason", reason}});
		}
	}

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
	// Phase A: mint this substream's stable id (one InitLocal call == one
	// substream == one PipelineExecutor; the spike proved Execute↔FinalExecute
	// run on the SAME local state, so a single mint here is stable across the
	// substream's whole life). See MintSubstreamId for the encoding / uniqueness
	// rationale, and VgiTableInOutLocalState::substream_id for the contract.
	local_state->substream_id = MintSubstreamId();
	return local_state;
}

// ============================================================================
// Arrow Batch to DuckDB DataChunk Conversion Helpers
// ============================================================================

//! Load an Arrow C++ RecordBatch into the ArrowScanLocalState for consumption.
//! Must call Reset() to clear stale owned_data references before loading a new batch.
void LoadBatchIntoScanState(VgiTableInOutLocalState &local_state,
                            const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(batch, *chunk);
	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();
}

//! Check whether the local state has remaining rows to produce from the current batch.
bool HasRemainingBatchData(const VgiTableInOutLocalState &local_state) {
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
idx_t ProduceOutputFromBatch(VgiTableInOutLocalState &local_state, const ArrowTableSchema &arrow_table,
                             DataChunk &output, bool projection_pushdown) {
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
// Phase A: per-substream worker helpers (parallel, no-finalize path)
// ============================================================================

// Acquire + init a fresh worker for ONE substream (one VgiTableInOutLocalState).
// Mirrors the serial InitGlobal acquire, reading the already-captured
// bind_result / projection / filter state off the global state.
static std::unique_ptr<IFunctionConnection>
AcquireSubstreamConnection(ClientContext &context, const VgiTableInOutBindData &bind_data,
                           VgiTableInOutGlobalState &gstate, const std::vector<uint8_t> &substream_id) {
	FunctionConnectionParams p;
	p.attach_params = bind_data.attach_params;
	p.attach_opaque_data = bind_data.attach_opaque_data;
	p.function_name = bind_data.function_name;
	p.arguments = bind_data.arguments;
	p.transaction_opaque_data = bind_data.transaction_opaque_data;
	p.settings = bind_data.settings;
	p.required_secrets = bind_data.required_secrets;
	p.phase = "init_substream";
	p.function_type = "TABLE";
	p.input_schema = bind_data.input_schema;
	auto acquired = AcquireConnectionForInit(context, p);
	auto conn = std::move(acquired.connection);
	// Stamp the per-substream id BEFORE PerformInit so it rides the INPUT init
	// request (and, later, this same connection's FINALIZE init) on the wire.
	conn->SetSubstreamId(substream_id);
	try {
		conn->PerformInit(gstate.bind_result, gstate.projection_ids, gstate.static_filter_bytes,
		                  gstate.join_keys_buffers, "INPUT");
	} catch (const IOException &) {
		if (!acquired.from_pool) {
			throw;
		}
		acquired = AcquireConnectionForInit(context, p, /*force_fresh=*/true);
		conn = std::move(acquired.connection);
		conn->SetSubstreamId(substream_id);
		conn->PerformInit(gstate.bind_result, gstate.projection_ids, gstate.static_filter_bytes,
		                  gstate.join_keys_buffers, "INPUT");
	}
	conn->OpenInputWriter();
	return conn;
}

// Acquire + INPUT-init a worker for a blended function from bind_data alone (no
// VgiTableInOutGlobalState). Used by the batched-lateral operator, which has no
// InitGlobal — bind_data.bind_result is populated at bind. `projection_ids`
// (worker-original column indices) is threaded when the function supports
// projection pushdown so it emits only the referenced columns; the operator sets
// the scan state's column_ids to match for the projected read. No filter pushdown
// (DuckDB's InOut path discards table_filters). Mirrors AcquireSubstreamConnection.
std::unique_ptr<IFunctionConnection>
AcquireBlendedInputConnection(ClientContext &context, const VgiTableInOutBindData &bind_data,
                              const std::vector<uint8_t> &substream_id,
                              const std::vector<int32_t> &projection_ids) {
	FunctionConnectionParams p;
	p.attach_params = bind_data.attach_params;
	p.attach_opaque_data = bind_data.attach_opaque_data;
	p.function_name = bind_data.function_name;
	p.arguments = bind_data.arguments;
	p.transaction_opaque_data = bind_data.transaction_opaque_data;
	p.settings = bind_data.settings;
	p.required_secrets = bind_data.required_secrets;
	p.phase = "init_lateral_batch";
	p.function_type = "TABLE";
	p.input_schema = bind_data.input_schema;
	auto acquired = AcquireConnectionForInit(context, p);
	auto conn = std::move(acquired.connection);
	conn->SetSubstreamId(substream_id);
	try {
		conn->PerformInit(bind_data.bind_result, projection_ids, nullptr, {}, "INPUT");
	} catch (const IOException &) {
		if (!acquired.from_pool) {
			throw;
		}
		acquired = AcquireConnectionForInit(context, p, /*force_fresh=*/true);
		conn = std::move(acquired.connection);
		conn->SetSubstreamId(substream_id);
		conn->PerformInit(bind_data.bind_result, projection_ids, nullptr, {}, "INPUT");
	}
	conn->OpenInputWriter();
	return conn;
}

// Return a substream's worker to the pool at clean end-of-stream. On early
// termination the local-state destructor drops the connection (unique_ptr),
// which is safe (a mid-stream worker is never pooled).
void ReleaseSubstreamConnection(std::unique_ptr<IFunctionConnection> &conn,
                                const VgiTableInOutBindData &bind_data, ClientContext &context) {
	if (conn && bind_data.use_pool()) {
		auto release_conn_id = conn->GetConnIdHex();
		if (auto pooled = conn->ReleaseForPooling()) {
			auto released_pid = pooled->GetPid();
			auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
			PoolReleaseLogFields lf;
			lf.conn_id = release_conn_id;
			lf.worker_path = bind_data.worker_path();
			lf.function_name = bind_data.function_name;
			lf.worker_pid = released_pid;
			lf.event_name = "table_in_out.pool_release";
			LogWorkerPoolRelease(context, lf, rr.pooled, rr.skip_reason, rr.pool_size, rr.total_pool_size);
		}
	}
	conn.reset();
}

// Convert an input DataChunk to the worker-input Arrow batch. For a BLENDED
// function, first cast the columns to their DECLARED types (bind_data
// .declared_input_types) when the incoming types differ — so the worker always
// receives its declared arg types regardless of call shape. This matters for the
// LITERAL shape, which delivers each constant's natural type (DECIMAL for 52.0,
// INTEGER for 52) rather than the DOUBLE signature; the column/LATERAL shape has
// already been cast to the signature by DuckDB, so this is a no-op there. A no-op
// for classic (non-blended) table-in-out (empty declared_input_types).
std::shared_ptr<arrow::RecordBatch>
ConvertInputToArrow(ClientContext &context, DataChunk &input, const VgiTableInOutBindData &bind_data) {
	const auto &declared = bind_data.declared_input_types;
	bool needs_cast = false;
	if (!declared.empty() && declared.size() == input.ColumnCount()) {
		for (idx_t c = 0; c < input.ColumnCount(); c++) {
			if (input.data[c].GetType() != declared[c]) {
				needs_cast = true;
				break;
			}
		}
	}
	if (!needs_cast) {
		return DataChunkToArrow(context, input, bind_data.input_schema);
	}
	vector<LogicalType> dtypes(declared.begin(), declared.end());
	DataChunk casted;
	casted.Initialize(Allocator::Get(context), dtypes);
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		VectorOperations::Cast(context, input.data[c], casted.data[c], input.size());
	}
	casted.SetCardinality(input.size());
	return DataChunkToArrow(context, casted, bind_data.input_schema);
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

	// Phase A: select the exchange connection. A parallel (no-finalize) function
	// uses THIS substream's own worker (per-substream fan-out) — acquired lazily
	// here on first use, and NOT shared, so no exchange mutex. A finalize function
	// uses the single shared global connection under exchange_mutex (serial path).
	const bool parallel = global_state.parallel_safe;
	if (parallel) {
		if (!local_state.connection) {
			local_state.connection =
			    AcquireSubstreamConnection(client_context, bind_data, global_state, local_state.substream_id);
		}
	} else if (!global_state.connection) {
		throw InternalException("VgiTableInOutFunction: connection is null");
	}
	IFunctionConnection &conn = parallel ? *local_state.connection : *global_state.connection;

	// ------------------------------------------------------------------------
	// Phase B — blended LITERAL scan-mode (single_row_scan).
	// ------------------------------------------------------------------------
	// DuckDB drives the childless call shape (f(52,13) / pure-varargs childless)
	// through PhysicalTableScan, which acts as a SOURCE: it re-invokes this
	// callback with the SAME cardinality-1 input chunk and decides flow SOLELY on
	// `chunk.size()` (it DISCARDS our returned OperatorResultType). So we must
	// return a 0-row chunk ONLY at true end-of-stream, and never mid-stream.
	if (bind_data.single_row_scan) {
		// 1) Drain a large (1->N) output batch that overflowed STANDARD_VECTOR_SIZE.
		if (HasRemainingBatchData(local_state)) {
			idx_t rows_copied =
			    ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
			VGI_LOG(client_context, "table_in_out.scan_output",
			        {{"conn", conn.GetConnIdHex()}, {"function_name", bind_data.function_name},
			         {"output_rows", std::to_string(rows_copied)}});
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		// 2) First call: write the single synthesized input row ONCE, then close the
		//    input writer so the worker can reach EOS (no deadlock).
		if (!local_state.input_submitted) {
			auto input_batch = ConvertInputToArrow(client_context, input, bind_data);
			VGI_LOG(client_context, "table_in_out.scan_write_input",
			        {{"conn", conn.GetConnIdHex()}, {"function_name", bind_data.function_name},
			         {"input_rows", std::to_string(input_batch->num_rows())}});
			conn.WriteInputBatch(input_batch);
			conn.CloseInputWriter();
			local_state.input_submitted = true;
		}
		// 3) Drain to EOS INSIDE this call: skip empty-but-not-EOS batches (worker
		//    heartbeat / 1->0). Return a >0-row chunk on the first non-empty batch;
		//    return a 0-row chunk (SetCardinality(0)) only on nullptr (true EOS).
		while (true) {
			std::shared_ptr<arrow::RecordBatch> output_batch;
			try {
				output_batch = conn.ReadDataBatch();
			} catch (const IOException &) {
				// Worker may exit right after input EOS (e.g. empty 1->0). Treat as
				// clean end-of-stream, mirroring the scalar path's post-close read.
				output_batch = nullptr;
			}
			if (!output_batch) {
				output.SetCardinality(0);
				// Cancel-not-pool: the "no-finalize in-out worker returns to a clean
				// accept-loop after input-EOS" transition is unproven for scan-mode,
				// so drop the connection instead of pooling it.
				if (parallel) {
					local_state.connection.reset();
				} else {
					global_state.stream_finished = true;
				}
				return OperatorResultType::FINISHED;
			}
			if (output_batch->num_rows() == 0) {
				continue; // empty-but-not-EOS: keep reading, never return 0 rows mid-stream
			}
			LoadBatchIntoScanState(local_state, output_batch);
			idx_t rows_copied =
			    ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
			VGI_LOG(client_context, "table_in_out.scan_output",
			        {{"conn", conn.GetConnIdHex()}, {"function_name", bind_data.function_name},
			         {"output_rows", std::to_string(rows_copied)}});
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	// Continue producing rows from a batch that exceeded STANDARD_VECTOR_SIZE
	if (HasRemainingBatchData(local_state)) {
		idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
		VGI_LOG(client_context, "table_in_out.read_output",
		        {{"conn", conn.GetConnIdHex()},
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
		input_batch = ConvertInputToArrow(client_context, input, bind_data);
	} else {
		input_batch = ConvertInputToArrow(client_context, input, bind_data);
	}

	// Exchange-mode result cache (M1). Key this input batch (ordered — the output is
	// positionally aligned to the input) on the static key + input-batch hash. A HIT
	// replays the cached output batch and SKIPS the worker exchange (no write_input
	// log = proof of hit); a MISS runs the exchange and, once the worker's cc is
	// latched + cacheable, stores the output. The lookup is unconditional (another
	// substream may have already stored this input's output).
	std::shared_ptr<arrow::RecordBatch> output_batch;
	bool cache_hit = false;
	VgiResultCacheKey batch_key;
	if (global_state.cache_eligible) {
		batch_key = global_state.cache_key;
		batch_key.input_hash = HashInputBatchOrdered(input_batch);
		auto entry = VgiResultCache::Instance().Lookup(batch_key, std::chrono::steady_clock::now());
		if (entry && !entry->streams.empty() && !entry->streams[0].batches.empty()) {
			output_batch = DeserializeCachedRecordBatch(entry->streams[0].batches[0]);
			cache_hit = true;
			VGI_LOG(client_context, "result_cache.hit",
			        {{"function", bind_data.function_name},
			         {"key_hash", batch_key.HexDigest()},
			         {"tier", "memory"}});
		}
	}

	if (!cache_hit) {
		// The 1:1 write→read exchange. SERIAL (finalize) path: the ONE shared
		// `connection` is fed by multiple source sub-pipelines under UNION ALL whose
		// PipelineExecutors may run concurrently, so hold `exchange_mutex` across the
		// whole write→read (else their calls interleave on the single IPC stream and
		// desync the worker's schema-first reader). PARALLEL path: this substream owns
		// its connection exclusively, so no lock is needed.
		std::unique_lock<std::mutex> exchange_guard;
		if (!parallel) {
			exchange_guard = std::unique_lock<std::mutex>(global_state.exchange_mutex);
		}

		VGI_LOG(client_context, "table_in_out.write_input",
		        {{"conn", conn.GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"input_rows", std::to_string(input_batch->num_rows())}});

		// Write the input batch to the worker
		if (timing) {
			ScopedNs _t(ClientTiming::Instance().write_ns);
			conn.WriteInputBatch(input_batch);
		} else {
			conn.WriteInputBatch(input_batch);
		}

		// Read output batch (1:1 lockstep in exchange mode)
		if (timing) {
			ScopedNs _t(ClientTiming::Instance().read_ns);
			output_batch = conn.ReadDataBatch();
		} else {
			output_batch = conn.ReadDataBatch();
		}

		// Latch the worker's cache-control advertisement off the first exchange
		// output, then (if cacheable) memoize this input batch's output. Skip on the
		// terminal EOS batch (nullptr) — there is nothing to cache and cc rides data.
		if (global_state.cache_eligible && output_batch) {
			if (!local_state.cache_cc_latched) {
				local_state.cache_cc = conn.GetLastCacheControl();
				local_state.cache_cc_latched = true;
			}
			if (local_state.cache_cc.Cacheable()) {
				auto sr = StoreExchangeMemoEntry(batch_key, local_state.cache_cc,
				                                 global_state.cache_catalog_name,
				                                 global_state.cache_default_ttl_seconds, {output_batch});
				if (sr.stored) {
					VGI_LOG(client_context, "result_cache.store",
					        {{"function", bind_data.function_name},
					         {"key_hash", batch_key.HexDigest()},
					         {"tier", "memory"},
					         {"rows", std::to_string(sr.rows)},
					         {"bytes", std::to_string(sr.bytes)}});
				} else if (sr.reason) {
					VGI_LOG(client_context, "result_cache.store_skipped",
					        {{"function", bind_data.function_name}, {"reason", sr.reason}});
				}
			}
		}
	}

	if (!output_batch) {
		// EOS - stream exhausted
		output.SetCardinality(0);
		if (parallel) {
			// A no-finalize map is done here — return its substream worker to the
			// pool. A finalize function must KEEP its substream connection open so
			// the per-substream VgiTableInOutFinalize can drive the FINALIZE phase
			// on the same worker (it releases the connection at finalize-EOS).
			if (!bind_data.has_finalize) {
				ReleaseSubstreamConnection(local_state.connection, bind_data, client_context);
			}
		} else {
			global_state.stream_finished = true;
		}
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
	        {{"conn", conn.GetConnIdHex()},
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

	// Phase A: select the connection this substream finalizes on. PARALLEL path:
	// each substream drives the FINALIZE phase on its OWN worker (the one that saw
	// this substream's input), so its per-substream finalize reads its own state.
	// SERIAL path: the single shared global connection. A parallel substream that
	// received no input never acquired a connection (null) — nothing to finalize.
	const bool parallel = global_state.parallel_safe;
	IFunctionConnection *conn = parallel ? local_state.connection.get() : global_state.connection.get();
	bool &finalize_sent = parallel ? local_state.finalize_sent : global_state.finalize_sent;

	if (!conn) {
		global_state.stream_finished = true;
		return OperatorFinalizeResultType::FINISHED;
	}

	// Release helper: return `conn` to the pool. PARALLEL path resets the local
	// unique_ptr after release (a mid-stream/finished substream worker is never
	// double-released); SERIAL path leaves the global connection in place (its
	// destructor handles teardown) and only marks the stream finished.
	auto release_and_finish = [&]() {
		if (bind_data.use_pool()) {
			auto release_conn_id = conn->GetConnIdHex();
			if (auto pooled = conn->ReleaseForPooling()) {
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
		if (parallel) {
			local_state.connection.reset();
		} else {
			global_state.stream_finished = true;
		}
	};

	// Perform finalize init (only once per substream)
	// This closes current data streams and opens new init with phase=FINALIZE
	// The worker enters producer mode (tick-based) to emit any finalize output
	if (conn->IsTableInOut() && !conn->IsFinished() && !finalize_sent) {
		conn->PerformFinalizeInit(global_state.bind_result);
		finalize_sent = true;

		VGI_LOG(client_context, "table_in_out.finalize",
		        {{"conn", conn->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name}});
	}

	// Continue producing rows from a batch that exceeded STANDARD_VECTOR_SIZE
	if (HasRemainingBatchData(local_state)) {
		idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);
		VGI_LOG(client_context, "table_in_out.finalize_output",
		        {{"conn", conn->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"output_rows", std::to_string(rows_copied)}});
		// Always return HAVE_MORE_OUTPUT — next call will either continue this batch
		// or read the next one from the worker
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	// Read next output batch from worker
	VGI_LOG(client_context, "table_in_out.finalize_reading",
	        {{"conn", conn->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name}});
	auto output_batch = conn->ReadDataBatch();

	if (!output_batch || output_batch->num_rows() == 0) {
		// No more output - clean up
		release_and_finish();
		return OperatorFinalizeResultType::FINISHED;
	}

	// Load batch into scan state and produce output
	LoadBatchIntoScanState(local_state, output_batch);
	idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output, bind_data.projection_pushdown);

	VGI_LOG(client_context, "table_in_out.finalize_output",
	        {{"conn", conn->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	// Check if worker is finished and we've exhausted the current batch
	if (!HasRemainingBatchData(local_state) && conn->IsFinished()) {
		release_and_finish();
		return OperatorFinalizeResultType::FINISHED;
	}

	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
