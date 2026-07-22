// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_buffering_impl.hpp"

#include <arrow/c/bridge.h>
#include <arrow/util/byte_size.h> // TotalBufferSize — bound the whole-input RAM capture

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_cache_control.hpp"        // VgiCacheControl (M3)
#include "vgi_cached_replay_connection.hpp" // CachedReplayConnection (M3 hit replay)
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_exchange_cache_key.hpp"   // exchange-mode result cache (M3)
#include "vgi_logging.hpp"
#include "vgi_result_cache.hpp"         // VgiResultCache singleton (M3)
#include "vgi_unary_rpc.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiTableBufferingFunction
// ============================================================================

LogicalVgiTableBufferingFunction::LogicalVgiTableBufferingFunction(idx_t table_index_p,
                                                                  vector<LogicalType> return_types_p,
                                                                  vector<string> return_names_p,
                                                                  unique_ptr<FunctionData> bind_data_p)
    : table_index(table_index_p),
      return_types(std::move(return_types_p)),
      return_names(std::move(return_names_p)),
      bind_data(std::move(bind_data_p)) {
}

vector<ColumnBinding> LogicalVgiTableBufferingFunction::GetColumnBindings() {
	vector<ColumnBinding> bindings;
	bindings.reserve(return_types.size());
	for (idx_t i = 0; i < return_types.size(); i++) {
		bindings.emplace_back(table_index, i);
	}
	return bindings;
}

vector<idx_t> LogicalVgiTableBufferingFunction::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalVgiTableBufferingFunction::GetName() const {
	return "VGI_TABLE_BUFFERING";
}

string LogicalVgiTableBufferingFunction::GetExtensionName() const {
	return "vgi_table_buffering_function";
}

void LogicalVgiTableBufferingFunction::ResolveTypes() {
	// Output column types are exactly the function's declared return_types.
	types = return_types;
}

void LogicalVgiTableBufferingFunction::Serialize(Serializer & /*serializer*/) const {
	throw NotImplementedException(
	    "LogicalVgiTableBufferingFunction serialization is not supported "
	    "(plan caching across processes is out of scope for v1)");
}

PhysicalOperator &LogicalVgiTableBufferingFunction::CreatePlan(ClientContext &context,
                                                                PhysicalPlanGenerator &planner) {
	// Plan the child (the table-input subquery) first so its data is
	// pipelined into our Sink.
	D_ASSERT(children.size() == 1);
	estimated_cardinality = EstimateCardinality(context);

	auto &child_plan = planner.CreatePlan(*children[0]);

	// Cast bind_data to read the ordering flags for the PhysicalOperator's
	// ParallelSource/SourceOrder + ParallelSink/RequiredPartitionInfo
	// advertisement.
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	const bool source_order_dependent = bd.source_order_dependent;
	const bool sink_order_dependent = bd.sink_order_dependent;
	const bool requires_input_batch_index = bd.requires_input_batch_index;

	auto types_copy = return_types;
	auto names_copy = return_names;
	auto &op_base = planner.Make<PhysicalVgiTableBufferingFunction>(
	    std::move(types_copy), std::move(names_copy), std::move(bind_data), source_order_dependent,
	    sink_order_dependent, requires_input_batch_index, estimated_cardinality);
	auto &op = op_base.Cast<PhysicalVgiTableBufferingFunction>();
	// Thread pushdown state captured by the rewriter onto the physical op
	// (per-query plan data, not per-bind data). All Sink-side and Source-side
	// PerformInit calls read these to populate InitRequest's projection_ids
	// and pushdown_filters fields.
	op.projection_pushdown = projection_pushdown;
	op.projection_ids = std::move(projection_ids);
	op.column_ids = std::move(column_ids);
	op.pushdown_filters = std::move(pushdown_filters);
	op.join_keys_buffers = std::move(join_keys_buffers);
	op.all_column_types = std::move(all_column_types);
	op.all_column_names = std::move(all_column_names);
	op.explain_summary = std::move(explain_summary);
	op.children.push_back(child_plan);
	return op_base;
}

// ============================================================================
// PhysicalVgiTableBufferingFunction
// ============================================================================

PhysicalVgiTableBufferingFunction::PhysicalVgiTableBufferingFunction(PhysicalPlan &physical_plan,
                                                                    vector<LogicalType> return_types_p,
                                                                    vector<string> return_names_p,
                                                                    unique_ptr<FunctionData> bind_data_p,
                                                                    bool source_order_dependent_p,
                                                                    bool sink_order_dependent_p,
                                                                    bool requires_input_batch_index_p,
                                                                    idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(return_types_p),
                       estimated_cardinality),
      bind_data(std::move(bind_data_p)),
      return_names(std::move(return_names_p)),
      source_order_dependent(source_order_dependent_p),
      sink_order_dependent(sink_order_dependent_p),
      requires_input_batch_index(requires_input_batch_index_p) {
}

string PhysicalVgiTableBufferingFunction::GetName() const {
	return "VGI_TABLE_BUFFERING";
}

InsertionOrderPreservingMap<string> PhysicalVgiTableBufferingFunction::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	result["Name"] = bd.function_name;
	result["Source"] = source_order_dependent ? "ordered" : "parallel";
	// Surface pushed-down projections/filters so users can verify pushdown
	// via EXPLAIN. The string was pre-built at rewriter time from
	// TableFilter::ToString (the C++ side has no symmetric deserializer for
	// the serialized filter IPC bytes).
	if (!explain_summary.empty()) {
		result["Pushdown"] = explain_summary;
	}
	return result;
}

// ============================================================================
// Global / Local state
// ============================================================================

namespace {

class VgiTableBufferingGlobalSinkState : public GlobalSinkState {
public:
	explicit VgiTableBufferingGlobalSinkState(DatabaseInstance *db_p) : db(db_p) {
	}
	~VgiTableBufferingGlobalSinkState() override;

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

	// Per-thread workers handed off from Combine. Available for the source
	// phase to grab via GetData. Also: state_ids[] runs parallel to workers[]
	// and is what we ship to the combine RPC.
	std::mutex workers_mutex;
	std::vector<std::unique_ptr<IFunctionConnection>> workers;
	std::vector<std::vector<uint8_t>> state_ids;

	// Populated by Sink::Finalize after combine returns. Drained by the
	// source phase under finalize_queue_mutex.
	std::mutex finalize_queue_mutex;
	std::deque<std::vector<uint8_t>> finalize_queue;

	// Function name & attach_id snapshot, captured from bind_data. Used by
	// the per-RPC wrappers (RpcTableBuffering*) so they don't need to
	// chase pointers back through the operator.
	std::string function_name;
	std::vector<uint8_t> attach_opaque_data;

	// Captured at GetGlobalSinkState time so the destructor can fire a
	// best-effort table_buffering_destructor RPC after the source phase
	// drains. context_weak goes null if the originating session ends
	// before teardown completes; attach_params keeps the connection
	// metadata alive (worker_path, auth, pool flags) regardless.
	weak_ptr<ClientContext> context_weak;
	std::shared_ptr<VgiAttachParameters> attach_params;

	// True once Sink::Finalize has fired (mirrors aggregate gstates).
	std::atomic<bool> finalized {false};

	// ---- Exchange-mode result cache (M3, whole-input memoization) ----
	// Built once at GetGlobalSinkState. Scoped to !sink_order_dependent (result is a
	// function of the input MULTISET, so a commutative additive fold of per-row hashes
	// keys it — order/thread independent). A HIT skips the combine RPC + the Source
	// finalize-drain and replays the cached result; ingestion (Sink process() RPCs)
	// still runs (the key is only known after all input is folded).
	bool cache_eligible = false;
	VgiResultCacheKey cache_key;                 // static; input_hash set at Finalize
	std::string cache_catalog_name;
	int64_t cache_default_ttl_seconds = 0;
	// Additive input digest, merged from per-thread LocalSinkState partials at Combine.
	std::atomic<uint64_t> digest_lo {0};
	std::atomic<uint64_t> digest_hi {0};
	std::atomic<uint64_t> digest_rows {0};
	// Serve (hit): the cached whole-input result + a single-claim guard (only one
	// Source thread replays the single entry; the rest are FINISHED immediately).
	bool serving_from_cache = false;
	std::shared_ptr<const VgiResultCacheEntry> serving;
	std::atomic<bool> serving_claimed {false};
	// Capture (miss): all Source batches accumulate here under `capture_mu`; committed
	// in this destructor iff every finalize state reached EOS (never-partial).
	std::mutex capture_mu;
	bool capturing = false;
	VgiCacheControl capture_cc;
	bool capture_cc_latched = false;
	std::vector<std::shared_ptr<arrow::RecordBatch>> capture_batches;
	size_t total_finalize_states = 0;
	std::atomic<size_t> finalize_eos_count {0};
	// Set if the worker's finalize output is not cacheable (no vgi.cache.* advertised),
	// so capture stops early and the destructor never stores a partial result.
	std::atomic<bool> capture_aborted {false};
	// [S6/S9] Bound the whole-input RAM capture. Unlike the producer path, buffered
	// capture accumulates the entire result in RAM (capture_batches) before the dtor
	// commits — so it must be bounded DURING capture or a multi-GB result blows memory.
	// A capture crossing max_entry_bytes or the process-global in-flight budget aborts
	// to uncached (keeps streaming to DuckDB). All three guarded by capture_mu; the
	// reserved inflight budget is released in the dtor.
	int64_t cache_max_entry_bytes = 67108864;
	int64_t capture_bytes = 0;
	int64_t reserved_inflight_bytes = 0;
};

VgiTableBufferingGlobalSinkState::~VgiTableBufferingGlobalSinkState() {
	// `workers` holds the per-thread Sink workers (handed off by Combine) plus
	// the combine worker (pushed back by Finalize). The Source phase acquires
	// *fresh* workers and never drains this vector — those Sink/combine workers
	// were init'd with phase=TABLE_BUFFERING and would reject a second
	// PerformInit — so on the HAPPY path `workers` is NOT empty here; it holds
	// idle, finished, reusable connections owned by this gstate (no thread is
	// using them).  Return each to the pool first (the canonical happy-path
	// release the Source phase uses on FINISHED).
	//
	// Only if a worker can't be pooled (HTTP transport, pooling disabled, or a
	// dead connection) do we fall back to cancel-dispatch through the global
	// VgiCancelDispatcher (same machinery the streaming InOut path uses) so any
	// worker still parked in a blocking syscall unblocks and is reclaimable
	// rather than forced-killed on the error path.
	auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
	auto reclaim_or_cancel = [&](std::unique_ptr<IFunctionConnection> conn) {
		if (!conn) {
			return;
		}
		// Try pooling first — restores the pooling win the TABLE_BUFFERING
		// design is built around.  Release() internally skips dead workers.
		if (auto pooled = conn->ReleaseForPooling()) {
			(void)VgiWorkerPool::Instance().Release(std::move(pooled));
			return;
		}
		// Not poolable (HTTP / pooling off / dead) — cancel-dispatch so a
		// possibly-blocked worker thread is woken rather than left parked.
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

	// In-flight LocalSinkState connections (still owned by a thread's lstate)
	// handle their own cancel-dispatch in ~VgiTableBufferingLocalSinkState.

	// Best-effort table_buffering_destructor RPC. Wipes the worker's
	// FunctionStorage rows for this execution_id (worker-defined
	// namespaces under the b"buf*" prefix) and pops any in-process iter
	// caches. Delivery is best-effort:
	//   * ClientContext gone (session ended early): skip; cleanup_old_entries
	//     is the FunctionStorage backstop.
	//   * execution_id never published (init failed before completing):
	//     nothing to clean; skip.
	//   * RPC throws: swallow; we're in a destructor.
	if (!execution_id.empty() && attach_params) {
		auto context_lock = context_weak.lock();
		if (context_lock) {
			try {
				auto rpc_params =
				    vgi::BuildTableBufferingDestructorInner(function_name, execution_id, attach_opaque_data);
				vgi::UnaryRpcOptions opts {*context_lock,
				                            attach_params->worker_path(),
				                            attach_params->worker_debug(),
				                            attach_params->use_pool(),
				                            attach_params->data_version_spec(),
				                            attach_params->implementation_version(),
				                            "rpc_table_buffering_destructor",
				                            attach_params->auth(),
				                            attach_params->cookie_jar(),
				                            /*enable_logging=*/false};
				if (attach_params->launcher_idle_timeout_seconds().has_value()) {
					opts.launcher_idle_timeout =
					    std::chrono::seconds(*attach_params->launcher_idle_timeout_seconds());
				}
				if (attach_params->launcher_state_dir().has_value()) {
					opts.launcher_state_dir = *attach_params->launcher_state_dir();
				}
				(void)vgi::InvokePooledUnaryRpc(opts, "table_buffering_destructor", rpc_params);
			} catch (...) {
				// Swallow — destructor must not throw. The worker side's
				// cleanup_old_entries (1-day default) is the long-term
				// FunctionStorage GC backstop for missed RPCs.
			}
		}
	}

	// Exchange-mode result cache (M3): commit the captured whole-input result iff
	// EVERY finalize state reached EOS (never-partial — a mid-drain error or early
	// LIMIT teardown leaves finalize_eos_count < total_finalize_states, so nothing is
	// stored). Runs after the Source phase (source shares this sink_state), so all
	// capture_batches are present. allow_disk=true — one large result, like a producer
	// entry; Insert rejects it (uncached) if it exceeds the memory cap with disk off.
	if (capturing && !capture_aborted.load() && total_finalize_states > 0 &&
	    finalize_eos_count.load(std::memory_order_acquire) == total_finalize_states &&
	    capture_cc.Cacheable()) {
		try {
			auto sr = StoreExchangeMemoEntry(cache_key, capture_cc, cache_catalog_name,
			                                 cache_default_ttl_seconds, capture_batches, /*allow_disk=*/true);
			if (sr.stored) {
				VgiResultCache::Instance().RecordExchangeStore();
			}
		} catch (...) {
			// Destructor must not throw; a failed cache store is non-fatal.
		}
	}
	// [S6] Release the in-flight capture budget this gstate reserved (0 if it never
	// captured or aborted early). Held from first captured batch until here — the
	// transient window that bounds concurrent buffered-capture RAM.
	VgiResultCache::Instance().ReleaseInflightCapture(reserved_inflight_bytes);
}

class VgiTableBufferingLocalSinkState : public LocalSinkState {
public:
	// Per-thread worker connection. Acquired lazily on the thread's first
	// Sink call (which assigns state_id from the global atomic). On the error
	// path the destructor routes the connection through the cancel-dispatcher
	// so the worker unblocks instead of being abandoned mid-RPC.
	std::unique_ptr<IFunctionConnection> connection;
	// Worker-chosen opaque state_id, returned from the first
	// table_buffering_process RPC on this thread. Empty until then.
	std::vector<uint8_t> state_id;

	// Cached at Sink-time so the destructor (which runs on this thread,
	// possibly after another thread's exception has torn down the pipeline) can
	// dispatch a cancel without re-walking the operator.
	DatabaseInstance *db = nullptr;

	// M3 result cache: this thread's partial additive input digest, folded per Sink
	// chunk and merged into the gstate atomics at Combine (associative/commutative).
	uint64_t digest_lo = 0;
	uint64_t digest_hi = 0;
	uint64_t digest_rows = 0;

	~VgiTableBufferingLocalSinkState() override {
		// If Combine already moved our connection into gstate.workers[], `connection`
		// is null and there's nothing to do — that worker is now the gstate's to
		// pool/cancel. We only reach the cancel-dispatch below when this lstate is
		// torn down still owning a connection (an error unwound the pipeline before
		// Combine ran), which is the sole mechanism that unblocks a peer worker
		// parked in a blocking process() RPC.
		if (!connection) {
			return;
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

class VgiTableBufferingGlobalSourceState : public GlobalSourceState {
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

class VgiTableBufferingLocalSourceState : public LocalSourceState {
public:
	// Per-thread fresh worker. Source acquires from the pool on first
	// GetData and per finalize_state_id transition (each new state_id
	// opens a new TABLE_BUFFERING_FINALIZE init stream that we drain via
	// ReadDataBatch). We don't reuse the per-thread workers from the
	// Sink phase because their init_done_ guard would reject a second
	// PerformInit; cleaner to acquire fresh.
	std::unique_ptr<IFunctionConnection> worker;
	std::vector<uint8_t> current_finalize_state_id;
	// True once PerformInit(TABLE_BUFFERING_FINALIZE) has fired on
	// `worker` for `current_finalize_state_id`. Subsequent GetData
	// calls just keep calling ReadDataBatch on the same worker until
	// EOS (null batch).
	bool stream_open = false;
	// Arrow → DuckDB scan plumbing. Rebuilt per emitted batch (the batch
	// carries its own schema; we don't need to precompute one).
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	// The currently-draining emitted batch. Persisted across GetData calls so a
	// finalize() batch larger than STANDARD_VECTOR_SIZE is emitted over several
	// vectors (advancing chunk_offset) rather than truncated to the first 2048
	// rows. Null between batches / before the first one.
	std::unique_ptr<ArrowScanLocalState> scan_state;

	// Captured at GetData-time alongside the worker acquisition so the
	// destructor can route an unreleased connection through the global
	// cancel-dispatcher. Mirrors LocalSinkState's `db` field. nullptr
	// before the first GetData call, or if the cancel dispatcher isn't
	// installed (in which case the destructor falls back to dropping
	// the unique_ptr, same as the gstate destructor's last-resort path).
	DatabaseInstance *db = nullptr;

	~VgiTableBufferingLocalSourceState() override {
		if (!worker) {
			return;
		}
		// EOS path goes through ReleaseForPooling in GetDataInternal and
		// leaves `worker` empty before we reach here. So if we're here
		// holding a worker, something else short-circuited:
		//   1) PerformInit(TABLE_BUFFERING_FINALIZE) threw (worker died,
		//      user's initial_finalize_state raised, EPIPE);
		//   2) ReadDataBatch threw mid-stream;
		//   3) DuckDB tore down the source pipeline early (LIMIT, user
		//      break, parent exception) without reaching EOS.
		// (3) is the on_cancel path — we want to deliver a CANCEL_KEY
		// batch to the worker so it can run cls.on_cancel(...) before
		// the pipe closes. (1)/(2) want the same dispatch so a worker
		// parked in a blocking syscall unblocks promptly.
		auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
		if (!dispatcher) {
			return;
		}
		auto token = worker->GetLastStateToken();
		CancelRequest req;
		req.connection = std::move(worker);
		req.state_token = std::move(token);
		(void)dispatcher->Enqueue(std::move(req));
	}
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

// Emit up to STANDARD_VECTOR_SIZE rows from the scan_state's current batch into
// `chunk`, advancing chunk_offset. Mirrors ProduceOutputFromBatch in
// vgi_table_in_out_impl.cpp; the Source phase never has appended LATERAL
// columns, so the scratch-chunk path isn't needed here.
void EmitBufferedPage(ArrowScanLocalState &scan_state, const ArrowTableSchema &arrow_table, DataChunk &chunk,
                      bool projection_pushdown) {
	idx_t length = static_cast<idx_t>(scan_state.chunk->arrow_array.length);
	idx_t remaining = length - scan_state.chunk_offset;
	idx_t rows = std::min<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	chunk.SetCardinality(rows);
	// column_ids is intentionally left empty so ArrowToDuckDB reads positionally
	// (children[idx]) — the worker emits a narrow batch already aligned to the
	// projected output schema. ArrowToDuckDB reads `rows` from chunk_offset; we
	// advance chunk_offset ourselves afterwards.
	ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), chunk, projection_pushdown);
	scan_state.chunk_offset += rows;
}

} // namespace

// ============================================================================
// Sink
// ============================================================================

unique_ptr<GlobalSinkState>
PhysicalVgiTableBufferingFunction::GetGlobalSinkState(ClientContext &context) const {
	auto *db = &DatabaseInstance::GetDatabase(context);
	auto gstate = make_uniq<VgiTableBufferingGlobalSinkState>(db);
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	gstate->function_name = bd.function_name;
	gstate->attach_opaque_data = bd.attach_opaque_data;
	// Captured for the best-effort destructor RPC fired from
	// ~VgiTableBufferingGlobalSinkState. context.shared_from_this()
	// gives us a non-owning observer; if the session ends before teardown
	// runs the destructor swallows and relies on cleanup_old_entries.
	gstate->context_weak = context.shared_from_this();
	gstate->attach_params = bd.attach_params;

	// Exchange-mode result cache (M3): eligible only for an UNORDERED sink (result is a
	// function of the input multiset — the commutative additive digest keys it). An
	// order-dependent sink is excluded (its result depends on input order, which the
	// commutative fold can't capture).
	if (!bd.sink_order_dependent) {
		std::vector<int32_t> proj_key;
		if (bd.projection_pushdown) {
			proj_key = projection_ids; // the wire projection the worker was init'd with
		}
		const char *reason = nullptr;
		int64_t catalog_version = 0;
		if (BuildExchangeCacheKeyStatic(context, bd, proj_key, gstate->cache_key,
		                                gstate->cache_catalog_name, catalog_version, reason, "buffered")) {
			gstate->cache_eligible = true;
			Value ttl_v;
			if (context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", ttl_v)) {
				gstate->cache_default_ttl_seconds = static_cast<int64_t>(ttl_v.GetValue<uint64_t>());
			}
			Value mev;
			if (context.TryGetCurrentSetting("vgi_result_cache_max_entry_bytes", mev) && !mev.IsNull()) {
				gstate->cache_max_entry_bytes = static_cast<int64_t>(mev.GetValue<uint64_t>());
			}
		} else if (reason) {
			VGI_LOG(context, "result_cache.ineligible",
			        {{"function", bd.function_name}, {"reason", reason}});
		}
	}
	return std::move(gstate);
}

unique_ptr<LocalSinkState>
PhysicalVgiTableBufferingFunction::GetLocalSinkState(ExecutionContext & /*context*/) const {
	return make_uniq<VgiTableBufferingLocalSinkState>();
}

SinkResultType PhysicalVgiTableBufferingFunction::Sink(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiTableBufferingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<VgiTableBufferingLocalSinkState>();
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();

	// Strict invariant: Sink must not see input after Sink::Finalize ran on
	// the same GlobalSinkState. DuckDB's MetaPipeline contract already
	// enforces this in every context we've found; this assertion exists as
	// belt-and-suspenders so a future planner change that violates it
	// surfaces loudly instead of silently producing wrong results.
	if (gstate.finalized.load()) {
		throw InternalException(
		    "PhysicalVgiTableBufferingFunction::Sink called after Finalize returned READY — "
		    "the strict once-per-GlobalSinkState invariant has been violated.");
	}

	// M3 result cache: fold this chunk into the per-thread additive input digest
	// (merged into the gstate at Combine). Order/thread independent — the buffered
	// result is a function of the input multiset (eligibility requires !sink_order_dependent).
	if (gstate.cache_eligible) {
		AccumulateInputDigest(chunk, lstate.digest_lo, lstate.digest_hi, lstate.digest_rows);
	}

	// Init protocol — symmetric, no coordinator. The first Sink thread to
	// arrive runs PerformInit on its OWN per-thread worker; that init mints
	// the execution_id. Other threads wait on a CV until execution_id is
	// publishable, then run their own per-thread secondary inits in parallel.
	//
	// The init RPC opens a Stream on the wire; for TABLE_BUFFERING we don't
	// use it (all subsequent traffic is unary RPCs). In exchange-mode (which
	// is selected because we pass a non-null input_schema at bind), the
	// worker is blocked waiting for input. Open the input writer and close
	// it (sends EOS) so the worker's exchange loop completes, then drain
	// ReadDataBatch to EOS. After that stdin/stdout are free for the unary
	// table_buffering_* RPCs.
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
			// All reads of init_done / init_started / init_failed below
			// happen under init_mutex, so the mutex provides synchronizes-
			// with. The release/acquire on init_done is therefore redundant
			// — kept as documentation that init_done's intent is a one-way
			// latch, not because it's load-bearing here. If a future caller
			// reads init_done OUTSIDE the lock they'd also need atomic
			// access to init_started/init_failed (currently plain bools).
			if (gstate.init_done.load(std::memory_order_acquire)) {
				// Init already published — secondary acquire path.
				exec_id = gstate.execution_id;
			} else if (!gstate.init_started) {
				// First arrival — I will run init.
				gstate.init_started = true;
				i_am_runner = true;
			} else {
				// Init is in flight on another thread — wait it out, polling
				// the query's interrupted flag every 250ms so a Ctrl-C while
				// the init runner is blocked (slow worker spawn, OAuth
				// refresh, etc.) doesn't leave peers hung indefinitely.
				while (!gstate.init_done.load(std::memory_order_acquire) && !gstate.init_failed) {
					if (context.client.interrupted) {
						throw IOException("table_buffering init wait interrupted (query cancelled)");
					}
					gstate.init_cv.wait_for(lk, std::chrono::milliseconds(250));
				}
				if (gstate.init_failed) {
					throw IOException("table_buffering init failed on peer thread");
				}
				exec_id = gstate.execution_id;
			}
		}

		// Cache the gstate/db pointers immediately so the lstate destructor
		// can cancel-dispatch if anything in the init flow below throws
		// AFTER the connection is acquired but BEFORE the success path
		// publishes execution_id. Without this, an exception in PerformInit
		// (or drain_init_stream) would leave the connection owned by lstate
		// but its destructor sees `db == nullptr` and falls back to a raw
		// drop, which closes stdin without giving the worker a chance to
		// shut down cleanly.
		lstate.db = gstate.db;

		if (i_am_runner) {
			// Acquire MY worker (no global_execution_id; we mint one) and run
			// init on it. This worker then becomes my per-thread worker —
			// state_id is assigned exactly as a secondary's would be. No
			// dedicated coordinator process exists.
			//
			// TODO: AcquireConnectionForInit can block in non-interruptible
			// steps (posix_spawn, worker-pool mutex contention) before
			// reaching the first fd-based read. Runner cancellation latency
			// is bounded by the longest such step; peer threads waiting on
			// init_cv poll interrupted every 250ms, but the runner itself
			// only checks interrupted at fd-read boundaries. Acceptable
			// because the steps are short in practice; flagged for future
			// work if a slow worker spawn becomes a real problem.
			std::vector<uint8_t> minted_exec_id;
			try {
				auto params = BuildAcquireParams(bd, /*global_execution_id=*/{});
				auto acquired = AcquireConnectionForInit(context.client, params);
				lstate.connection = std::move(acquired.connection);
				auto init_result = lstate.connection->PerformInit(bd.bind_result, projection_ids,
				                                                    pushdown_filters,
				                                                    join_keys_buffers,
				                                                    /*phase=*/"TABLE_BUFFERING");
				minted_exec_id = std::move(init_result.execution_id);
				drain_init_stream(*lstate.connection);
				// state_id is now assigned by the worker (returned on the
				// RpcTableBufferingProcess response in Sink()).
				if (VgiInfoLogActive(context.client)) {
					// state_id is not yet known at init time (worker returns
					// it from the first process() RPC); log it post-Sink.
					VGI_LOG(context.client, "table_buffering.init",
					        {{"conn", lstate.connection->GetConnIdHex()},
					         {"function_name", bd.function_name},
					         {"role", "init_runner"}});
				}
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
			lstate.connection->PerformInit(bd.bind_result, projection_ids,
			                                pushdown_filters,
			                                join_keys_buffers,
			                                /*phase=*/"TABLE_BUFFERING");
			drain_init_stream(*lstate.connection);
			// state_id assigned by worker on first Sink RPC.
			if (VgiInfoLogActive(context.client)) {
				VGI_LOG(context.client, "table_buffering.init",
				        {{"conn", lstate.connection->GetConnIdHex()},
				         {"function_name", bd.function_name},
				         {"role", "secondary"}});
			}
		}
	}

	// Convert DuckDB chunk → Arrow batch and ship to worker.
	auto input_batch = DataChunkToArrow(context.client, chunk, bd.input_schema);

	// batch_index plumbing: when the operator declared
	// RequiredPartitionInfo()=BatchIndex(), DuckDB threads a globally-
	// unique monotonic batch_index into lstate.partition_info. If the
	// source can't supply it (e.g. range() — single-thread TABLE_SCAN
	// without batch_index support) the optional_idx is INVALID and we
	// fail loudly: workers that opted in to ordering depend on having
	// batch_index per call; silently passing nullopt would result in
	// quiet wrong answers. Conservative bind-time alternative would
	// require knowing the source plan — easier to fail at first chunk.
	std::optional<int64_t> batch_index;
	if (requires_input_batch_index) {
		// partition_info is on the LocalSinkState BASE, not our subclass.
		const auto &bi = input.local_state.partition_info.batch_index;
		if (!bi.IsValid()) {
			throw IOException(
			    "VGI buffered table function '%s' declared "
			    "Meta.requires_input_batch_index=True but the source for this "
			    "query does not support batch_index. Wrap input in a TEMP "
			    "TABLE / parquet scan, or remove the metadata flag.",
			    bd.function_name);
		}
		batch_index = static_cast<int64_t>(bi.GetIndex());
	}

	// Worker chooses the state_id and returns it on the response — opaque
	// bytes the worker can encode any way it wants (8-byte packed pid via
	// thread_state_id(), UUID4, hash digest, etc.). The C++ side just
	// shuttles them between phases.
	lstate.state_id = lstate.connection->RpcTableBufferingProcess(
	    gstate.function_name, gstate.execution_id, input_batch, batch_index);
	if (VgiInfoLogActive(context.client)) {
		// Render state_id as hex — opaque bytes have no canonical decimal form.
		auto to_hex = [](const std::vector<uint8_t> &b) {
			std::string out;
			out.reserve(b.size() * 2);
			static const char *digits = "0123456789abcdef";
			for (auto c : b) {
				out.push_back(digits[c >> 4]);
				out.push_back(digits[c & 0xF]);
			}
			return out;
		};
		vector<std::pair<string, string>> info {
		    {"conn", lstate.connection->GetConnIdHex()},
		    {"function_name", bd.function_name},
		    {"state_id", to_hex(lstate.state_id)},
		    {"input_rows", std::to_string(input_batch->num_rows())},
		};
		if (batch_index.has_value()) {
			info.emplace_back("batch_index", std::to_string(*batch_index));
		}
		VGI_LOG(context.client, "table_buffering.process", info);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkNextBatchType
PhysicalVgiTableBufferingFunction::NextBatch(ExecutionContext & /*context*/,
                                              OperatorSinkNextBatchInput & /*input*/) const {
	// Required to be implementable for sinks that declare
	// RequiredPartitionInfo()=BatchIndex(). DuckDB invokes this when the
	// per-thread batch_index advances. We don't need to flush anything —
	// workers receive batch_index per process() call and own their own
	// ordering logic. PhysicalBatchInsert does the same (no-op).
	return SinkNextBatchType::READY;
}

SinkCombineResultType
PhysicalVgiTableBufferingFunction::Combine(ExecutionContext & /*context*/,
                                           OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<VgiTableBufferingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<VgiTableBufferingLocalSinkState>();

	// M3 result cache: merge this thread's partial additive input digest into the
	// gstate (associative + commutative → order/thread independent). Done before the
	// no-input early return (a thread with no input contributes 0, harmlessly).
	if (gstate.cache_eligible) {
		gstate.digest_lo.fetch_add(lstate.digest_lo, std::memory_order_relaxed);
		gstate.digest_hi.fetch_add(lstate.digest_hi, std::memory_order_relaxed);
		gstate.digest_rows.fetch_add(lstate.digest_rows, std::memory_order_relaxed);
	}

	// Thread didn't receive any input → nothing to hand off.
	if (!lstate.connection) {
		return SinkCombineResultType::FINISHED;
	}

	// Move the connection into gstate.workers[]; this nulls lstate.connection, so
	// ~VgiTableBufferingLocalSinkState sees no connection and won't cancel-dispatch
	// it (the worker is now the gstate's to pool/cancel at teardown).
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(lstate.connection));
		gstate.state_ids.push_back(lstate.state_id);
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalVgiTableBufferingFunction::Finalize(Pipeline & /*pipeline*/, Event & /*event*/,
                                                              ClientContext &context,
                                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<VgiTableBufferingGlobalSinkState>();

	// M3 result cache: the input digest is now complete (all Combine calls merged).
	// Fold it into the key and probe the cache. A HIT skips the combine RPC + the
	// Source finalize-drain — the Source phase replays the cached result instead.
	if (gstate.cache_eligible) {
		gstate.cache_key.input_hash =
		    FinalizeInputDigest(gstate.digest_lo.load(), gstate.digest_hi.load(), gstate.digest_rows.load());
		auto entry = VgiResultCache::Instance().Lookup(gstate.cache_key, std::chrono::steady_clock::now());
		if (entry) {
			VgiResultCache::Instance().RecordExchangeHit(entry->total_bytes);
			VGI_LOG(context, "result_cache.hit",
			        {{"function", gstate.function_name}, {"key_hash", gstate.cache_key.HexDigest()}, {"tier", "memory"}});
			gstate.serving_from_cache = true;
			gstate.serving = entry;
			gstate.finalized.store(true);
			return SinkFinalizeType::READY; // Source replays; no combine, no worker drain
		}
		// Not a hit — the combine below runs (a fresh whole-input miss).
		VgiResultCache::Instance().RecordExchangeMiss();
	}

	// Snapshot state_ids and pop one worker for the combine RPC, all under
	// workers_mutex. No special "coordinator" — combine reads peer state
	// through shared BoundStorage in the worker library, so any worker that
	// finished init can serve. We pop one to avoid running combine and
	// source-drain concurrently on the same connection.
	std::vector<std::vector<uint8_t>> state_ids_snapshot;
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

	// Run combine outside the lock — it's a network round-trip.
	VGI_LOG(context, "table_buffering.combine",
	        {{"conn", combine_worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"num_state_ids", std::to_string(state_ids_snapshot.size())}});
	// state_ids flow through opaque-bytes — no packing/unpacking, the
	// worker chose the encoding when it returned them from process().
	std::vector<std::vector<uint8_t>> finalize_state_ids;
	try {
		finalize_state_ids =
		    combine_worker->RpcTableBufferingCombine(gstate.function_name, gstate.execution_id,
		                                              state_ids_snapshot);
	} catch (...) {
		// Combine failed. Three things to do before rethrow:
		//   (a) Set finalized=true so the gstate destructor's "happy path"
		//       skip is honored — the post-finalize Sink guard at the top
		//       of Sink() is the only consumer of this flag, and it's
		//       harmless to flip it here since the pipeline is unwinding.
		//   (b) Cancel-dispatch combine_worker through the global dispatcher.
		//       Without this the unique_ptr just drops, leaving the worker
		//       thread parked in a blocking syscall until its own idle
		//       reclaim path kicks in (subprocess: pool timeout; HTTP:
		//       keep-alive expiry). Mirrors the gstate destructor's
		//       reclaim_or_cancel logic and the LocalSinkState destructor.
		//   (c) Pop the state_id we held aside for this worker so the
		//       transient workers/state_ids size imbalance doesn't leak
		//       past this throw. state_ids is dead after Finalize anyway,
		//       but keeping the invariant tight makes the destructor
		//       reasoning easier.
		gstate.finalized.store(true);
		{
			std::lock_guard<std::mutex> lk(gstate.workers_mutex);
			if (!gstate.state_ids.empty()) {
				gstate.state_ids.pop_back();
			}
		}
		auto *dispatcher = gstate.db ? FindVgiCancelDispatcher(*gstate.db) : nullptr;
		if (dispatcher) {
			auto token = combine_worker->GetLastStateToken();
			CancelRequest req;
			req.connection = std::move(combine_worker);
			req.state_token = std::move(token);
			(void)dispatcher->Enqueue(std::move(req));
		}
		// combine_worker (if dispatcher was null) drops here — last resort,
		// same as the gstate-destructor fallback.
		throw;
	}
	VGI_LOG(context, "table_buffering.combine_result",
	        {{"conn", combine_worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"num_finalize_state_ids", std::to_string(finalize_state_ids.size())}});

	// Combine succeeded — push the worker back into the pool so source threads
	// can use it like any other worker. Note: state_ids is NOT updated here.
	// After Finalize runs, state_ids is no longer read; the source-phase
	// routing structure is finalize_queue (populated below). So the transient
	// invariant `state_ids.size() != workers.size()` between this push and
	// the gstate destructor is harmless. Don't "fix" this by re-pushing a
	// state_id — there's no sink state_id to attribute to the combine worker.
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(combine_worker));
	}
	{
		std::lock_guard<std::mutex> lk(gstate.finalize_queue_mutex);
		for (auto &id : finalize_state_ids) {
			gstate.finalize_queue.push_back(std::move(id));
		}
		// M3 result cache (MISS): arm capture. The Source phase accumulates every
		// drained batch; the gstate destructor commits iff all finalize states reach
		// EOS (never-partial). total_finalize_states is the drain target.
		if (gstate.cache_eligible) {
			gstate.capturing = true;
			gstate.total_finalize_states = gstate.finalize_queue.size();
		}
	}
	gstate.finalized.store(true);
	return SinkFinalizeType::READY;
}

// ============================================================================
// Source
// ============================================================================

// Post-execution EXPLAIN ANALYZE line: whole-input (M3) cache hit vs miss. The
// decision was made at Sink::Finalize and lives on the sink gstate; buffered is
// whole-input keyed, so a query is a single hit or miss (binary, not a rate).
InsertionOrderPreservingMap<string>
PhysicalVgiTableBufferingFunction::ExtraSourceParams(GlobalSourceState & /*gstate*/,
                                                     LocalSourceState & /*lstate*/) const {
	InsertionOrderPreservingMap<string> result;
	if (!sink_state) {
		return result;
	}
	auto &sink = sink_state->Cast<VgiTableBufferingGlobalSinkState>();
	if (!sink.cache_eligible) {
		return result; // caching not active for this scan → no Cache line
	}
	result["Cache"] = sink.serving_from_cache ? "hit (memory)" : "miss";
	return result;
}

unique_ptr<GlobalSourceState>
PhysicalVgiTableBufferingFunction::GetGlobalSourceState(ClientContext & /*context*/) const {
	auto gss = make_uniq<VgiTableBufferingGlobalSourceState>();
	// sink_state is populated by the time GetGlobalSourceState runs (the
	// pipeline executor materializes the sink before scheduling the source).
	// Read the worker count *now* so MaxThreads can return a sensible cap and
	// DuckDB doesn't oversubscribe drainer threads.
	if (sink_state) {
		auto &gstate = sink_state->Cast<VgiTableBufferingGlobalSinkState>();
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gss->worker_count = gstate.workers.size();
	}
	return std::move(gss);
}

unique_ptr<LocalSourceState>
PhysicalVgiTableBufferingFunction::GetLocalSourceState(ExecutionContext & /*context*/,
                                                      GlobalSourceState & /*gstate*/) const {
	return make_uniq<VgiTableBufferingLocalSourceState>();
}

SourceResultType PhysicalVgiTableBufferingFunction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                    OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<VgiTableBufferingLocalSourceState>();
	// gstate is the *Sink* global state under DuckDB's sink-to-source
	// transition — operators that are both Sink and Source share the same
	// state pointer via sink_state.
	auto &gstate = sink_state->Cast<VgiTableBufferingGlobalSinkState>();
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();

	// Drain any rows still pending from the current emitted batch before
	// reading a new one. finalize() may emit a batch larger than
	// STANDARD_VECTOR_SIZE; we split it across GetData calls via the persisted
	// scan_state.chunk_offset rather than truncating to a single vector.
	if (lstate.scan_state &&
	    lstate.scan_state->chunk_offset < static_cast<idx_t>(lstate.scan_state->chunk->arrow_array.length)) {
		EmitBufferedPage(*lstate.scan_state, lstate.arrow_table, chunk, projection_pushdown);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	// Current batch (if any) is fully drained; drop it before fetching the next.
	lstate.scan_state.reset();

	// M3 result cache HIT: replay the single cached whole-input result. Exactly one
	// Source thread claims it (CachedReplayConnection); the rest are FINISHED. Skips
	// the finalize_queue + worker acquire + PerformInit entirely.
	if (gstate.serving_from_cache && !lstate.stream_open) {
		if (gstate.serving_claimed.exchange(true)) {
			chunk.SetCardinality(0);
			return SourceResultType::FINISHED; // another thread is replaying the result
		}
		lstate.worker = make_uniq<CachedReplayConnection>(gstate.serving);
		lstate.stream_open = true;
	}

	// Open a fresh worker + stream for the next finalize_state_id when
	// the previous stream has hit EOS (or this is the first call).
	if (!lstate.stream_open) {
		// Pop the next finalize_state_id; if none, we're FINISHED.
		{
			std::lock_guard<std::mutex> lk(gstate.finalize_queue_mutex);
			if (gstate.finalize_queue.empty()) {
				lstate.worker.reset();
				chunk.SetCardinality(0);
				return SourceResultType::FINISHED;
			}
			lstate.current_finalize_state_id = std::move(gstate.finalize_queue.front());
			gstate.finalize_queue.pop_front();
		}
		// Acquire a fresh worker (or reuse the prior one — the unary path
		// reused workers from gstate.workers, but those were init'd via
		// phase=TABLE_BUFFERING and would reject a second PerformInit. The
		// pool keeps subprocess workers warm so this is cheap.).
		auto acquire_params = BuildAcquireParams(bd, gstate.execution_id);
		auto acquired = AcquireConnectionForInit(context.client, acquire_params);
		lstate.worker = std::move(acquired.connection);
		// Capture the DatabaseInstance pointer alongside the worker so the
		// LocalSourceState destructor can cancel-dispatch the worker if a
		// later step (PerformInit, ReadDataBatch) throws. Without this the
		// unique_ptr would just drop and leave the worker thread parked.
		lstate.db = &DatabaseInstance::GetDatabase(context.client);
		if (VgiInfoLogActive(context.client)) {
			auto to_hex = [](const std::vector<uint8_t> &b) {
				std::string out;
				out.reserve(b.size() * 2);
				static const char *digits = "0123456789abcdef";
				for (auto c : b) {
					out.push_back(digits[c >> 4]);
					out.push_back(digits[c & 0xF]);
				}
				return out;
			};
			VGI_LOG(context.client, "table_buffering.finalize_init",
			        {{"conn", lstate.worker->GetConnIdHex()},
			         {"function_name", gstate.function_name},
			         {"finalize_state_id", to_hex(lstate.current_finalize_state_id)}});
		}
		// The Source-side init thread must receive the same projection_ids /
		// pushdown_filters as the Sink-side init thread so the worker's
		// finalize() emits batches narrowed to the projected output_schema.
		// Without this, the worker would emit full-width batches but the
		// post-projection return_types this operator advertises is narrow —
		// ArrowToDuckDB would read out-of-bounds.
		lstate.worker->PerformInit(bd.bind_result, projection_ids,
		                           pushdown_filters,
		                           join_keys_buffers,
		                           /*phase=*/"TABLE_BUFFERING_FINALIZE",
		                           /*order_by=*/std::nullopt,
		                           /*table_sample=*/std::nullopt,
		                           /*init_opaque_data=*/{},
		                           /*finalize_state_id=*/lstate.current_finalize_state_id);
		lstate.stream_open = true;
	}

	auto batch = lstate.worker->ReadDataBatch();
	if (!batch) {
		// EOS — this finalize_state_id is exhausted. Release worker back
		// to the pool; next GetData call will open the next stream.
		// M3 (MISS capture): count this finalize state as fully drained — the gstate
		// destructor commits only when finalize_eos_count == total_finalize_states.
		if (gstate.capturing && !gstate.serving_from_cache) {
			gstate.finalize_eos_count.fetch_add(1, std::memory_order_acq_rel);
		}
		auto pooled = lstate.worker->ReleaseForPooling();
		lstate.worker.reset();
		lstate.stream_open = false;
		if (pooled) {
			VgiWorkerPool::Instance().Release(std::move(pooled));
		}
		chunk.SetCardinality(0);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// M3 (MISS capture): accumulate the whole-input result. Latch the worker's cache
	// advertisement off the first batch; if not cacheable, abort capture (nothing is
	// stored). Capturing 0-row batches would bloat the entry with no rows — skip them
	// (they're valid "no output yet" ticks, not result data).
	if (gstate.capturing && !gstate.serving_from_cache && !gstate.capture_aborted.load() &&
	    batch->num_rows() > 0) {
		std::lock_guard<std::mutex> lk(gstate.capture_mu);
		if (!gstate.capture_cc_latched) {
			gstate.capture_cc = lstate.worker->GetLastCacheControl();
			gstate.capture_cc_latched = true;
			if (!gstate.capture_cc.Cacheable()) {
				gstate.capture_aborted.store(true);
			}
		}
		if (!gstate.capture_aborted.load()) {
			// [S6/S9] Bound the RAM capture. A result crossing the per-entry cap or the
			// process-global in-flight budget aborts to uncached — drop what we've
			// accumulated (freeing RAM now) and release any reserved budget; the dtor
			// then won't commit. The query keeps streaming to DuckDB unaffected.
			int64_t sz = arrow::util::TotalBufferSize(*batch);
			if (gstate.capture_bytes + sz > gstate.cache_max_entry_bytes ||
			    !VgiResultCache::Instance().TryReserveInflightCapture(sz)) {
				gstate.capture_aborted.store(true);
				VgiResultCache::Instance().NoteCaptureAbort();
				VgiResultCache::Instance().ReleaseInflightCapture(gstate.reserved_inflight_bytes);
				gstate.reserved_inflight_bytes = 0;
				gstate.capture_bytes = 0;
				gstate.capture_batches.clear();
			} else {
				gstate.capture_bytes += sz;
				gstate.reserved_inflight_bytes += sz;
				gstate.capture_batches.push_back(batch);
			}
		}
	}

	if (batch->num_rows() == 0) {
		chunk.SetCardinality(0);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// Convert the Arrow batch → DuckDB DataChunk via the same helper pair
	// the streaming InOut path uses (vgi_table_in_out_impl.cpp:306-345).
	ExportSchema(batch->schema(), lstate.c_schema);
	// Reset before repopulating: PopulateArrowTableSchema *appends* columns via
	// ArrowTableSchema::AddColumn, which D_ASSERTs that a column index isn't
	// added twice. A finalize() stream that returns multiple batches reaches
	// here once per batch on the same lstate.arrow_table, so without this reset
	// the second batch re-adds index 0 and aborts debug builds (in release the
	// assert is compiled out and the duplicate emplace is a silent no-op). The
	// schema is rebuilt per batch, as documented on the member.
	lstate.arrow_table = ArrowTableSchema();
	ArrowTableFunction::PopulateArrowTableSchema(context.client, lstate.arrow_table,
	                                              lstate.c_schema.arrow_schema);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(batch, *chunk_wrapper);

	// Persist the scan_state on the local state so a batch wider than
	// STANDARD_VECTOR_SIZE is drained over multiple GetData calls (see the
	// pending-drain check at the top of this function). ``scan_state.column_ids``
	// is left empty: when projection_pushdown is on the worker emits a narrow
	// Arrow batch positionally aligned with chunk.ColumnCount(), and
	// `arrow_scan_is_projected=true` makes ArrowToDuckDB read children[idx]
	// directly (our ``lstate.arrow_table`` is built per-batch from that narrow
	// schema, so its column-type map keys are 0..n-1 positional).
	lstate.scan_state = make_uniq<ArrowScanLocalState>(std::move(chunk_wrapper), context.client);
	lstate.scan_state->chunk_offset = 0;

	EmitBufferedPage(*lstate.scan_state, lstate.arrow_table, chunk, projection_pushdown);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
