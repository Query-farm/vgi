#include "vgi_buffered_table_function_impl.hpp"

#include <arrow/c/bridge.h>

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

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
	VgiBufferedTableFunctionGlobalSinkState() = default;
	~VgiBufferedTableFunctionGlobalSinkState() override;

	// The coordinator worker (set on the first Sink call). Used to send the
	// combine RPC, then handed off into the workers list to also serve as a
	// source-phase worker. Held under coordinator_mutex during init only.
	std::mutex coordinator_mutex;
	std::unique_ptr<IFunctionConnection> coordinator;
	std::vector<uint8_t> execution_id;
	std::atomic<int64_t> state_id_counter {0};

	// Per-thread workers handed off from Combine. Available for the source
	// phase to grab via GetData. Also: state_ids[] runs parallel to workers[]
	// and is what we ship to the combine RPC.
	std::mutex workers_mutex;
	std::vector<std::unique_ptr<IFunctionConnection>> workers;
	std::vector<int64_t> state_ids;

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
};

VgiBufferedTableFunctionGlobalSinkState::~VgiBufferedTableFunctionGlobalSinkState() {
	// Best-effort cancel-dispatch on early teardown. Mirrors the streaming
	// in-out path at vgi_table_in_out_impl.cpp:56-79, but iterates every
	// connection we still own (coordinator + workers[]). Any per-thread
	// LocalSinkState that errored mid-flight has already dropped its
	// connection via its own destructor, so we don't need to chase those.
	// Cancel is best-effort — the dispatcher may saturate; that's OK.
	auto release_all = [&](std::unique_ptr<IFunctionConnection> conn) {
		if (!conn) {
			return;
		}
		// If finalize ran cleanly, the connections were left behind for the
		// source phase to pool-release at end. Anything still here at this
		// destructor is uncompleted state — return to pool best-effort.
		auto pooled = conn->ReleaseForPooling();
		if (pooled) {
			VgiWorkerPool::Instance().Release(std::move(pooled));
		}
	};
	if (coordinator) {
		release_all(std::move(coordinator));
	}
	for (auto &w : workers) {
		release_all(std::move(w));
	}
	workers.clear();
}

class VgiBufferedTableFunctionLocalSinkState : public LocalSinkState {
public:
	// Per-thread worker connection. Acquired lazily on the thread's first
	// Sink call (which assigns state_id from the global atomic).
	std::unique_ptr<IFunctionConnection> connection;
	int64_t state_id = -1;
};

class VgiBufferedTableFunctionGlobalSourceState : public GlobalSourceState {
public:
	idx_t MaxThreads() override {
		// One drainer per worker, capped by partition keys. We can't access
		// the global sink state from here (different lifecycle), so report
		// "many" and let DuckDB's scheduler do the right thing under
		// ParallelSource. When source_order_dependent is true we declared
		// ParallelSource()=false in the op, so DuckDB clamps to 1 anyway.
		return std::numeric_limits<idx_t>::max();
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
PhysicalVgiBufferedTableFunction::GetGlobalSinkState(ClientContext & /*context*/) const {
	auto gstate = make_uniq<VgiBufferedTableFunctionGlobalSinkState>();
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

	// First Sink call across the whole query acquires the coordinator and
	// runs init with phase=BUFFERED_TABLE. Other threads block on the
	// mutex briefly, then see gstate.execution_id populated and proceed.
	//
	// The init RPC opens a Stream on the wire; for BUFFERED_TABLE we don't
	// use that stream (all subsequent traffic is unary RPCs). The worker's
	// stream-state is an empty TableInOutFinalizeState, but in exchange-
	// mode (which is selected because we pass a non-null input_schema at
	// bind), the worker is blocked waiting for input. Open the input writer
	// and close it (sends EOS) so the worker's exchange loop completes,
	// then drain ReadDataBatch to EOS. After that stdin/stdout are free for
	// the unary buffered_table_* RPCs.
	auto drain_init_stream = [](IFunctionConnection &conn) {
		conn.OpenInputWriter();
		conn.CloseInputWriter();
		auto batch = conn.ReadDataBatch();
		while (batch) {
			batch = conn.ReadDataBatch();
		}
	};

	if (!gstate.execution_id.empty()) {
		// Fast path — coordinator already exists.
	} else {
		std::lock_guard<std::mutex> lk(gstate.coordinator_mutex);
		if (gstate.execution_id.empty()) {
			auto params = BuildAcquireParams(bd, /*global_execution_id=*/{});
			auto acquired = AcquireConnectionForInit(context.client, params);
			gstate.coordinator = std::move(acquired.connection);
			auto init_result = gstate.coordinator->PerformInit(bd.bind_result, /*projection_ids=*/{},
			                                                    /*pushdown_filters=*/nullptr,
			                                                    /*join_keys=*/{},
			                                                    /*phase=*/"BUFFERED_TABLE");
			gstate.execution_id = std::move(init_result.execution_id);
			drain_init_stream(*gstate.coordinator);
		}
	}

	// Per-thread acquisition: secondary init, reusing the coordinator's
	// execution_id so the worker library treats the per-thread worker as
	// "join this exec" (mirrors the parallel-aggregate is_secondary path).
	if (!lstate.connection) {
		auto params = BuildAcquireParams(bd, /*global_execution_id=*/gstate.execution_id);
		auto acquired = AcquireConnectionForInit(context.client, params);
		lstate.connection = std::move(acquired.connection);
		lstate.connection->PerformInit(bd.bind_result, /*projection_ids=*/{},
		                                /*pushdown_filters=*/nullptr,
		                                /*join_keys=*/{},
		                                /*phase=*/"BUFFERED_TABLE");
		drain_init_stream(*lstate.connection);
		lstate.state_id = gstate.state_id_counter.fetch_add(1);
	}

	// Convert DuckDB chunk → Arrow batch and ship to worker.
	auto input_batch = DataChunkToArrow(context.client, chunk, bd.input_schema);
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

	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(lstate.connection));
		gstate.state_ids.push_back(lstate.state_id);
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalVgiBufferedTableFunction::Finalize(Pipeline & /*pipeline*/, Event & /*event*/,
                                                              ClientContext & /*context*/,
                                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<VgiBufferedTableFunctionGlobalSinkState>();

	// No input ever arrived. Tear down the coordinator (if we even had one)
	// and skip the combine RPC.
	if (gstate.workers.empty()) {
		if (gstate.coordinator) {
			auto pooled = gstate.coordinator->ReleaseForPooling();
			gstate.coordinator.reset();
			if (pooled) {
				VgiWorkerPool::Instance().Release(std::move(pooled));
			}
		}
		gstate.finalized.store(true);
		return SinkFinalizeType::READY;
	}

	// Send combine to the coordinator. The worker may coordinate with peer
	// workers (shared BoundStorage etc.) and returns the partition keys
	// for the source phase.
	auto finalize_state_ids =
	    gstate.coordinator->RpcBufferedTableCombine(gstate.function_name, gstate.execution_id,
	                                                  gstate.state_ids);
	{
		std::lock_guard<std::mutex> lk(gstate.finalize_queue_mutex);
		for (auto id : finalize_state_ids) {
			gstate.finalize_queue.push_back(id);
		}
	}

	// The coordinator now joins the source-phase worker pool. (Any worker
	// can serve any finalize_state_id — the worker library handles peer
	// coordination internally; from C++'s point of view they're equivalent.)
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(gstate.coordinator));
		gstate.state_ids.push_back(-1); // placeholder; coordinator wasn't a sink state_id
	}
	gstate.finalized.store(true);
	return SinkFinalizeType::READY;
}

// ============================================================================
// Source
// ============================================================================

unique_ptr<GlobalSourceState>
PhysicalVgiBufferedTableFunction::GetGlobalSourceState(ClientContext & /*context*/) const {
	return make_uniq<VgiBufferedTableFunctionGlobalSourceState>();
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
