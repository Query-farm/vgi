#include "vgi_buffered_table_function_impl.hpp"

#include <arrow/c/bridge.h>

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiBufferedTableFunction
// ============================================================================

LogicalVgiBufferedTableFunction::LogicalVgiBufferedTableFunction(idx_t table_index_p,
                                                                  vector<LogicalType> return_types_p,
                                                                  vector<string> return_names_p,
                                                                  unique_ptr<FunctionData> bind_data_p)
    : table_index(table_index_p),
      return_types(std::move(return_types_p)),
      return_names(std::move(return_names_p)),
      bind_data(std::move(bind_data_p)) {
}

vector<ColumnBinding> LogicalVgiBufferedTableFunction::GetColumnBindings() {
	vector<ColumnBinding> bindings;
	bindings.reserve(return_types.size());
	for (idx_t i = 0; i < return_types.size(); i++) {
		bindings.emplace_back(table_index, i);
	}
	return bindings;
}

vector<idx_t> LogicalVgiBufferedTableFunction::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalVgiBufferedTableFunction::GetName() const {
	return "VGI_BUFFERED_TABLE";
}

string LogicalVgiBufferedTableFunction::GetExtensionName() const {
	return "vgi_buffered_table_function";
}

void LogicalVgiBufferedTableFunction::ResolveTypes() {
	// Output column types are exactly the function's declared return_types.
	types = return_types;
}

void LogicalVgiBufferedTableFunction::Serialize(Serializer & /*serializer*/) const {
	throw NotImplementedException(
	    "LogicalVgiBufferedTableFunction serialization is not supported "
	    "(plan caching across processes is out of scope for v1)");
}

PhysicalOperator &LogicalVgiBufferedTableFunction::CreatePlan(ClientContext &context,
                                                                PhysicalPlanGenerator &planner) {
	// Plan the child (the table-input subquery) first so its data is
	// pipelined into our Sink.
	D_ASSERT(children.size() == 1);
	estimated_cardinality = EstimateCardinality(context);

	auto &child_plan = planner.CreatePlan(*children[0]);

	// Cast bind_data to read the source_order_dependent flag for the
	// PhysicalOperator's ParallelSource/SourceOrder advertisement.
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	const bool source_order_dependent = bd.source_order_dependent;

	auto types_copy = return_types;
	auto names_copy = return_names;
	auto &op = planner.Make<PhysicalVgiBufferedTableFunction>(std::move(types_copy), std::move(names_copy),
	                                                            std::move(bind_data), source_order_dependent,
	                                                            estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

// ============================================================================
// PhysicalVgiBufferedTableFunction
// ============================================================================

PhysicalVgiBufferedTableFunction::PhysicalVgiBufferedTableFunction(PhysicalPlan &physical_plan,
                                                                    vector<LogicalType> return_types_p,
                                                                    vector<string> return_names_p,
                                                                    unique_ptr<FunctionData> bind_data_p,
                                                                    bool source_order_dependent_p,
                                                                    idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(return_types_p),
                       estimated_cardinality),
      bind_data(std::move(bind_data_p)),
      return_names(std::move(return_names_p)),
      source_order_dependent(source_order_dependent_p) {
}

string PhysicalVgiBufferedTableFunction::GetName() const {
	return "VGI_BUFFERED_TABLE";
}

InsertionOrderPreservingMap<string> PhysicalVgiBufferedTableFunction::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	result["Name"] = bd.function_name;
	result["Source"] = source_order_dependent ? "ordered" : "parallel";
	return result;
}

// ============================================================================
// Global / Local state
// ============================================================================

namespace {

class VgiBufferedTableFunctionGlobalSinkState : public GlobalSinkState {
public:
	explicit VgiBufferedTableFunctionGlobalSinkState(DatabaseInstance *db_p) : db(db_p) {
	}
	~VgiBufferedTableFunctionGlobalSinkState() override;

	// DatabaseInstance is captured at construction so the destructor can
	// route any still-live connections through the global cancel-dispatcher
	// (same pattern the streaming InOut path uses on its error path).
	DatabaseInstance *db = nullptr;

	// Init coordination. The first Sink thread to arrive runs PerformInit on
	// its own per-thread worker (no separate coordinator); peer threads wait
	// here for execution_id to be published, then run their own secondary
	// inits in parallel. `init_started` is the "someone is running init"
	// latch; `init_done` flips to true once execution_id is publishable;
	// `init_failed` lets waiters bail when the init runner threw.
	std::mutex init_mutex;
	std::condition_variable init_cv;
	bool init_started = false;
	bool init_failed = false;
	std::atomic<bool> init_done {false};
	std::vector<uint8_t> execution_id;
	std::atomic<int64_t> state_id_counter {0};

	// Per-thread workers handed off from Combine. Available for the source
	// phase to grab via GetData. Also: state_ids[] runs parallel to workers[]
	// and is what we ship to the combine RPC.
	std::mutex workers_mutex;
	std::vector<std::unique_ptr<IFunctionConnection>> workers;
	std::vector<int64_t> state_ids;

	// Tracks every per-thread sink worker that is currently in-flight (between
	// `lstate.connection = std::move(...)` and the Combine handoff). On the
	// error path we want to cancel-dispatch every in-flight peer so its
	// blocking RPC unblocks promptly rather than waiting for the worker's own
	// idle path. Stored as raw pointers (the unique_ptr is owned by the
	// LocalSinkState); LocalSinkState's destructor calls UntrackInFlight to
	// remove itself before dropping the connection.
	std::mutex in_flight_mutex;
	std::vector<IFunctionConnection *> in_flight_workers;

	// Populated by Sink::Finalize after combine returns. Drained by the
	// source phase under finalize_queue_mutex.
	std::mutex finalize_queue_mutex;
	std::deque<int64_t> finalize_queue;

	// Function name & attach_id snapshot, captured from bind_data. Used by
	// the per-RPC wrappers (RpcBufferedTable*) so they don't need to chase
	// pointers back through the operator.
	std::string function_name;
	std::vector<uint8_t> attach_opaque_data;

	// True once Sink::Finalize has fired (mirrors aggregate gstates).
	std::atomic<bool> finalized {false};

	void TrackInFlight(IFunctionConnection *conn) {
		std::lock_guard<std::mutex> lk(in_flight_mutex);
		in_flight_workers.push_back(conn);
	}
	void UntrackInFlight(IFunctionConnection *conn) {
		std::lock_guard<std::mutex> lk(in_flight_mutex);
		auto it = std::find(in_flight_workers.begin(), in_flight_workers.end(), conn);
		if (it != in_flight_workers.end()) {
			in_flight_workers.erase(it);
		}
	}
};

VgiBufferedTableFunctionGlobalSinkState::~VgiBufferedTableFunctionGlobalSinkState() {
	// Two cases:
	//
	// 1) Happy path — finalize ran cleanly. By the time we reach here every
	//    worker has returned to its idle accept loop on its own thread (the
	//    source loop releases-for-pooling on FINISHED). `workers` and
	//    `coordinator` are empty.
	//
	// 2) Error path — some Sink, Combine, Finalize, or Source RPC threw. Any
	//    connection still alive may be parked inside a blocking syscall on a
	//    worker thread we no longer control. Dispatch a cancel through the
	//    global VgiCancelDispatcher (same machinery the streaming InOut path
	//    uses at vgi_table_in_out_impl.cpp:58-81) so the worker unblocks and
	//    is reclaimable rather than forced-killed.
	auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
	auto reclaim_or_cancel = [&](std::unique_ptr<IFunctionConnection> conn) {
		if (!conn) {
			return;
		}
		if (dispatcher) {
			auto token = conn->GetLastStateToken();
			CancelRequest req;
			req.connection = std::move(conn);
			req.state_token = std::move(token);
			(void)dispatcher->Enqueue(std::move(req));
		}
		// `conn` (if not moved into req) drops here — best-effort.
	};
	for (auto &w : workers) {
		reclaim_or_cancel(std::move(w));
	}
	workers.clear();
	state_ids.clear();

	// Any in-flight LocalSinkState connections handle their own cancel-dispatch
	// via their destructor's cached gstate pointer + UntrackInFlight.
}

class VgiBufferedTableFunctionLocalSinkState : public LocalSinkState {
public:
	// Per-thread worker connection. Acquired lazily on the thread's first
	// Sink call (which assigns state_id from the global atomic). On the error
	// path the destructor routes the connection through the cancel-dispatcher
	// so the worker unblocks instead of being abandoned mid-RPC.
	std::unique_ptr<IFunctionConnection> connection;
	int64_t state_id = -1;

	// Pointers cached at Sink-time so the destructor (which runs on this
	// thread, possibly after another thread's exception has torn down the
	// pipeline) can dispatch a cancel without re-walking the operator.
	VgiBufferedTableFunctionGlobalSinkState *gstate_ptr = nullptr;
	DatabaseInstance *db = nullptr;

	~VgiBufferedTableFunctionLocalSinkState() override {
		if (!connection) {
			return;
		}
		// Tell gstate we're no longer in-flight; if Combine already moved us
		// to workers[], this is a no-op.
		if (gstate_ptr) {
			gstate_ptr->UntrackInFlight(connection.get());
		}
		// Cancel-dispatch the connection so a blocked process call (e.g. a
		// worker thread parked in select() waiting for the next request) gets
		// woken up promptly.
		auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
		if (!dispatcher) {
			return;
		}
		auto token = connection->GetLastStateToken();
		CancelRequest req;
		req.connection = std::move(connection);
		req.state_token = std::move(token);
		(void)dispatcher->Enqueue(std::move(req));
	}
};

class VgiBufferedTableFunctionGlobalSourceState : public GlobalSourceState {
public:
	// Cap reported back to DuckDB's scheduler. Populated by GetGlobalSourceState
	// from the worker count handed to source — there is no point scheduling
	// more drainer threads than we have workers. When source_order_dependent
	// is true the operator also declares ParallelSource()=false, so DuckDB
	// clamps to 1 regardless.
	idx_t worker_count = 0;

	idx_t MaxThreads() override {
		return worker_count > 0 ? worker_count : 1;
	}
};

class VgiBufferedTableFunctionLocalSourceState : public LocalSourceState {
public:
	// One worker per source thread, popped from gstate.workers on first
	// GetData. While has_more is true we keep calling RpcBufferedTableFinalize
	// on the same finalize_state_id; when has_more flips to false we pop the
	// next id from the queue.
	std::unique_ptr<IFunctionConnection> worker;
	int64_t current_finalize_state_id = -1;
	bool has_more = false;
	// Arrow → DuckDB scan plumbing. Rebuilt per emitted batch (the batch
	// carries its own schema; we don't need to precompute one).
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
};

// Helper: build FunctionConnectionParams from bind_data + optional
// global_execution_id (empty = primary, non-empty = secondary).
FunctionConnectionParams BuildAcquireParams(const VgiTableInOutBindData &bd,
                                             const std::vector<uint8_t> &global_execution_id) {
	FunctionConnectionParams params;
	params.attach_params = bd.attach_params;
	params.attach_opaque_data = bd.attach_opaque_data;
	params.transaction_opaque_data = bd.transaction_opaque_data;
	params.function_name = bd.function_name;
	params.settings = bd.settings;
	params.required_secrets = bd.required_secrets;
	params.global_execution_id = global_execution_id;
	params.function_type = "TABLE";
	params.input_schema = bd.input_schema;
	return params;
}

} // namespace

// ============================================================================
// Sink
// ============================================================================

unique_ptr<GlobalSinkState>
PhysicalVgiBufferedTableFunction::GetGlobalSinkState(ClientContext &context) const {
	auto *db = &DatabaseInstance::GetDatabase(context);
	auto gstate = make_uniq<VgiBufferedTableFunctionGlobalSinkState>(db);
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	gstate->function_name = bd.function_name;
	gstate->attach_opaque_data = bd.attach_opaque_data;
	return std::move(gstate);
}

unique_ptr<LocalSinkState>
PhysicalVgiBufferedTableFunction::GetLocalSinkState(ExecutionContext & /*context*/) const {
	return make_uniq<VgiBufferedTableFunctionLocalSinkState>();
}

SinkResultType PhysicalVgiBufferedTableFunction::Sink(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiBufferedTableFunctionGlobalSinkState>();
	auto &lstate = input.local_state.Cast<VgiBufferedTableFunctionLocalSinkState>();
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();

	// Strict invariant: Sink must not see input after Sink::Finalize ran on
	// the same GlobalSinkState. DuckDB's MetaPipeline contract already
	// enforces this in every context we've found; this assertion exists as
	// belt-and-suspenders so a future planner change that violates it
	// surfaces loudly instead of silently producing wrong results.
	if (gstate.finalized.load()) {
		throw InternalException(
		    "PhysicalVgiBufferedTableFunction::Sink called after Finalize returned READY — "
		    "the strict once-per-GlobalSinkState invariant has been violated.");
	}

	// Init protocol — symmetric, no coordinator. The first Sink thread to
	// arrive runs PerformInit on its OWN per-thread worker; that init mints
	// the execution_id. Other threads wait on a CV until execution_id is
	// publishable, then run their own per-thread secondary inits in parallel.
	//
	// The init RPC opens a Stream on the wire; for BUFFERED_TABLE we don't
	// use it (all subsequent traffic is unary RPCs). In exchange-mode (which
	// is selected because we pass a non-null input_schema at bind), the
	// worker is blocked waiting for input. Open the input writer and close
	// it (sends EOS) so the worker's exchange loop completes, then drain
	// ReadDataBatch to EOS. After that stdin/stdout are free for the unary
	// buffered_table_* RPCs.
	auto drain_init_stream = [](IFunctionConnection &conn) {
		conn.OpenInputWriter();
		conn.CloseInputWriter();
		auto batch = conn.ReadDataBatch();
		while (batch) {
			batch = conn.ReadDataBatch();
		}
	};

	if (!lstate.connection) {
		bool i_am_runner = false;
		std::vector<uint8_t> exec_id;

		{
			std::unique_lock<std::mutex> lk(gstate.init_mutex);
			if (gstate.init_done.load(std::memory_order_relaxed)) {
				// Init already published — secondary acquire path.
				exec_id = gstate.execution_id;
			} else if (!gstate.init_started) {
				// First arrival — I will run init.
				gstate.init_started = true;
				i_am_runner = true;
			} else {
				// Init is in flight on another thread — wait it out.
				gstate.init_cv.wait(lk, [&] {
					return gstate.init_done.load(std::memory_order_relaxed) || gstate.init_failed;
				});
				if (gstate.init_failed) {
					throw IOException("buffered_table init failed on peer thread");
				}
				exec_id = gstate.execution_id;
			}
		}

		if (i_am_runner) {
			// Acquire MY worker (no global_execution_id; we mint one) and run
			// init on it. This worker then becomes my per-thread worker —
			// state_id is assigned exactly as a secondary's would be. No
			// dedicated coordinator process exists.
			std::vector<uint8_t> minted_exec_id;
			try {
				auto params = BuildAcquireParams(bd, /*global_execution_id=*/{});
				auto acquired = AcquireConnectionForInit(context.client, params);
				lstate.connection = std::move(acquired.connection);
				auto init_result = lstate.connection->PerformInit(bd.bind_result, /*projection_ids=*/{},
				                                                    /*pushdown_filters=*/nullptr,
				                                                    /*join_keys=*/{},
				                                                    /*phase=*/"BUFFERED_TABLE");
				minted_exec_id = std::move(init_result.execution_id);
				drain_init_stream(*lstate.connection);
				lstate.state_id = gstate.state_id_counter.fetch_add(1);
				VGI_LOG(context.client, "buffered_table.init",
				        {{"conn", lstate.connection->GetConnIdHex()},
				         {"function_name", bd.function_name},
				         {"role", "init_runner"},
				         {"state_id", std::to_string(lstate.state_id)}});
			} catch (...) {
				{
					std::lock_guard<std::mutex> lk(gstate.init_mutex);
					gstate.init_failed = true;
				}
				gstate.init_cv.notify_all();
				throw;
			}
			{
				std::lock_guard<std::mutex> lk(gstate.init_mutex);
				gstate.execution_id = std::move(minted_exec_id);
				gstate.init_done.store(true, std::memory_order_release);
			}
			gstate.init_cv.notify_all();
		} else {
			// Secondary acquire — runs in parallel with peer secondaries.
			auto params = BuildAcquireParams(bd, exec_id);
			auto acquired = AcquireConnectionForInit(context.client, params);
			lstate.connection = std::move(acquired.connection);
			lstate.connection->PerformInit(bd.bind_result, /*projection_ids=*/{},
			                                /*pushdown_filters=*/nullptr,
			                                /*join_keys=*/{},
			                                /*phase=*/"BUFFERED_TABLE");
			drain_init_stream(*lstate.connection);
			lstate.state_id = gstate.state_id_counter.fetch_add(1);
			VGI_LOG(context.client, "buffered_table.init",
			        {{"conn", lstate.connection->GetConnIdHex()},
			         {"function_name", bd.function_name},
			         {"role", "secondary"},
			         {"state_id", std::to_string(lstate.state_id)}});
		}

		// Cache pointers so lstate's destructor can cancel-dispatch on error,
		// and register ourselves as in-flight so the gstate teardown path
		// knows about us. Combine will remove us when it hands the connection
		// over to gstate.workers[].
		lstate.gstate_ptr = &gstate;
		lstate.db = gstate.db;
		gstate.TrackInFlight(lstate.connection.get());
	}

	// Convert DuckDB chunk → Arrow batch and ship to worker.
	auto input_batch = DataChunkToArrow(context.client, chunk, bd.input_schema);
	VGI_LOG(context.client, "buffered_table.process",
	        {{"conn", lstate.connection->GetConnIdHex()},
	         {"function_name", bd.function_name},
	         {"state_id", std::to_string(lstate.state_id)},
	         {"input_rows", std::to_string(input_batch->num_rows())}});
	lstate.connection->RpcBufferedTableProcess(gstate.function_name, gstate.execution_id, lstate.state_id,
	                                            input_batch);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalVgiBufferedTableFunction::Combine(ExecutionContext & /*context*/,
                                           OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<VgiBufferedTableFunctionGlobalSinkState>();
	auto &lstate = input.local_state.Cast<VgiBufferedTableFunctionLocalSinkState>();

	// Thread didn't receive any input → nothing to hand off.
	if (!lstate.connection) {
		return SinkCombineResultType::FINISHED;
	}

	// Untrack first so the lstate dtor doesn't try to cancel-dispatch the
	// connection after Combine has moved it into gstate.workers[].
	gstate.UntrackInFlight(lstate.connection.get());
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(lstate.connection));
		gstate.state_ids.push_back(lstate.state_id);
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalVgiBufferedTableFunction::Finalize(Pipeline & /*pipeline*/, Event & /*event*/,
                                                              ClientContext &context,
                                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<VgiBufferedTableFunctionGlobalSinkState>();

	// Snapshot state_ids and pop one worker for the combine RPC, all under
	// workers_mutex. No special "coordinator" — combine reads peer state
	// through shared BoundStorage in the worker library, so any worker that
	// finished init can serve. We pop one to avoid running combine and
	// source-drain concurrently on the same connection.
	std::vector<int64_t> state_ids_snapshot;
	std::unique_ptr<IFunctionConnection> combine_worker;
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		if (gstate.workers.empty()) {
			// No Sink thread ever ran process() — no input arrived. Skip
			// combine entirely.
			gstate.finalized.store(true);
			return SinkFinalizeType::READY;
		}
		state_ids_snapshot = gstate.state_ids;
		combine_worker = std::move(gstate.workers.back());
		gstate.workers.pop_back();
	}

	// Run combine outside the lock — it's a network round-trip. Cleanup of a
	// failed combine_worker is handled by its unique_ptr: it drops here and
	// the gstate destructor will cancel-dispatch any workers still in
	// gstate.workers[]. The combine_worker connection itself, if it throws,
	// is destroyed at this scope's exit which releases its own pid/fd.
	VGI_LOG(context, "buffered_table.combine",
	        {{"conn", combine_worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"num_state_ids", std::to_string(state_ids_snapshot.size())}});
	auto finalize_state_ids =
	    combine_worker->RpcBufferedTableCombine(gstate.function_name, gstate.execution_id,
	                                              state_ids_snapshot);
	VGI_LOG(context, "buffered_table.combine_result",
	        {{"conn", combine_worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"num_finalize_state_ids", std::to_string(finalize_state_ids.size())}});

	// Combine succeeded — push the worker back into the pool so source threads
	// can use it like any other worker.
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(combine_worker));
	}
	{
		std::lock_guard<std::mutex> lk(gstate.finalize_queue_mutex);
		for (auto id : finalize_state_ids) {
			gstate.finalize_queue.push_back(id);
		}
	}
	gstate.finalized.store(true);
	return SinkFinalizeType::READY;
}

// ============================================================================
// Source
// ============================================================================

unique_ptr<GlobalSourceState>
PhysicalVgiBufferedTableFunction::GetGlobalSourceState(ClientContext & /*context*/) const {
	auto gss = make_uniq<VgiBufferedTableFunctionGlobalSourceState>();
	// sink_state is populated by the time GetGlobalSourceState runs (the
	// pipeline executor materializes the sink before scheduling the source).
	// Read the worker count *now* so MaxThreads can return a sensible cap and
	// DuckDB doesn't oversubscribe drainer threads.
	if (sink_state) {
		auto &gstate = sink_state->Cast<VgiBufferedTableFunctionGlobalSinkState>();
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gss->worker_count = gstate.workers.size();
	}
	return std::move(gss);
}

unique_ptr<LocalSourceState>
PhysicalVgiBufferedTableFunction::GetLocalSourceState(ExecutionContext & /*context*/,
                                                      GlobalSourceState & /*gstate*/) const {
	return make_uniq<VgiBufferedTableFunctionLocalSourceState>();
}

SourceResultType PhysicalVgiBufferedTableFunction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                    OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<VgiBufferedTableFunctionLocalSourceState>();
	// gstate is the *Sink* global state under DuckDB's sink-to-source
	// transition — operators that are both Sink and Source share the same
	// state pointer via sink_state. The source-side input.global_state is a
	// thin delegate (GetGlobalSourceState above).
	auto &gstate = sink_state->Cast<VgiBufferedTableFunctionGlobalSinkState>();

	// Acquire a worker on first GetData call from this thread.
	if (!lstate.worker) {
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		if (gstate.workers.empty()) {
			chunk.SetCardinality(0);
			return SourceResultType::FINISHED;
		}
		lstate.worker = std::move(gstate.workers.back());
		gstate.workers.pop_back();
	}

	// If we don't have an in-flight finalize_state_id, pull the next one.
	if (!lstate.has_more) {
		std::lock_guard<std::mutex> lk(gstate.finalize_queue_mutex);
		if (gstate.finalize_queue.empty()) {
			// Done — release this thread's worker back to the pool.
			auto pooled = lstate.worker->ReleaseForPooling();
			lstate.worker.reset();
			if (pooled) {
				VgiWorkerPool::Instance().Release(std::move(pooled));
			}
			chunk.SetCardinality(0);
			return SourceResultType::FINISHED;
		}
		lstate.current_finalize_state_id = gstate.finalize_queue.front();
		gstate.finalize_queue.pop_front();
	}

	VGI_LOG(context.client, "buffered_table.finalize",
	        {{"conn", lstate.worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"finalize_state_id", std::to_string(lstate.current_finalize_state_id)}});
	auto resp = lstate.worker->RpcBufferedTableFinalize(gstate.function_name, gstate.execution_id,
	                                                     lstate.current_finalize_state_id);
	lstate.has_more = resp.has_more;

	// Convert the Arrow batch → DuckDB DataChunk.
	if (!resp.batch || resp.batch->num_rows() == 0) {
		chunk.SetCardinality(0);
		// Returning HAVE_MORE_OUTPUT keeps DuckDB pumping until we report
		// FINISHED on a future call. With has_more=false we'll pull the next
		// finalize_state_id; with has_more=true the worker is just emitting
		// progressively-empty batches before non-empty ones — keep going.
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// Use the helper pair: ExportSchema + ExportRecordBatch + ArrowToDuckDB
	// is the same plumbing the streaming InOut path uses
	// (vgi_table_in_out_impl.cpp:306-345).
	ExportSchema(resp.batch->schema(), lstate.c_schema);
	ArrowTableFunction::PopulateArrowTableSchema(context.client, lstate.arrow_table,
	                                              lstate.c_schema.arrow_schema);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(resp.batch, *chunk_wrapper);

	auto scan_state =
	    make_uniq<ArrowScanLocalState>(std::move(chunk_wrapper), context.client);
	scan_state->chunk_offset = 0;

	idx_t rows = std::min<idx_t>(resp.batch->num_rows(), STANDARD_VECTOR_SIZE);
	chunk.SetCardinality(rows);
	ArrowTableFunction::ArrowToDuckDB(*scan_state, lstate.arrow_table.GetColumns(), chunk, false);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
