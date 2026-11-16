// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_settings_defaults.hpp"
#include "vgi_table_buffering_impl.hpp"
#include "vgi_batch_validation.hpp"

#include <exception>

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_cache_control.hpp"
#include "vgi_cached_replay_connection.hpp" // CachedReplayConnection (cache-hit replay)
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_exchange_cache_key.hpp"   // exchange-mode result cache
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_result_cache.hpp"
#include "vgi_unary_rpc.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

// LogicalVgiTableBufferingFunction

LogicalVgiTableBufferingFunction::LogicalVgiTableBufferingFunction(TableIndex table_index_p,
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
		bindings.emplace_back(table_index, ProjectionIndex(i));
	}
	return bindings;
}

vector<TableIndex> LogicalVgiTableBufferingFunction::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalVgiTableBufferingFunction::GetName() const {
	return "VGI_TABLE_BUFFERING";
}

string LogicalVgiTableBufferingFunction::GetExtensionName() const {
	return "vgi_table_buffering_function";
}

void LogicalVgiTableBufferingFunction::ResolveTypes() {
	types = return_types;
}

void LogicalVgiTableBufferingFunction::Serialize(Serializer & /*serializer*/) const {
	throw NotImplementedException(
	    "LogicalVgiTableBufferingFunction serialization is not supported "
	    "(plan caching across processes is out of scope for v1)");
}

// The source of the pipeline feeding our Sink: the first operator down the
// children[0] chain that is itself a source, as PhysicalOperator::BuildPipelines
// picks it.
static optional_ptr<const PhysicalOperator> FindPipelineSource(const PhysicalOperator &op) {
	const PhysicalOperator *current = &op;
	while (true) {
		if (current->IsSource()) {
			return current;
		}
		if (current->children.empty()) {
			return nullptr;
		}
		// children[0] is the pipeline's own input; a second child (a join build
		// side, say) belongs to a different pipeline.
		current = &current->children[0].get();
	}
}

// Whether this plan's source can supply a batch index. Checked at plan time
// because pipeline scheduling throws an InternalException for a BatchIndex sink
// over a source that cannot, before any of our code runs.
static bool SourceCanSupplyBatchIndex(const PhysicalOperator &child_plan) {
	auto source = FindPipelineSource(child_plan);
	if (!source) {
		return false;
	}
	return source->SupportsPartitioning(OperatorPartitionInfo::BatchIndex());
}

PhysicalOperator &LogicalVgiTableBufferingFunction::CreatePlan(ClientContext &context,
                                                                PhysicalPlanGenerator &planner) {
	// The child (table-input subquery) feeds our Sink.
	D_ASSERT(children.size() == 1);
	estimated_cardinality = EstimateCardinality(context);

	auto &child_plan = planner.CreatePlan(*children[0]);

	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	const bool source_order_dependent = bd.source_order_dependent;
	bool sink_order_dependent = bd.sink_order_dependent;
	bool requires_input_batch_index = bd.requires_input_batch_index;
	// A batch index is a request for input order. When the source cannot supply
	// one (range(), VALUES), serialize the sink instead and number its batches.
	bool synthesize_batch_index = false;
	if (requires_input_batch_index && !SourceCanSupplyBatchIndex(child_plan)) {
		requires_input_batch_index = false;
		sink_order_dependent = true;
		synthesize_batch_index = true;
	}

	auto types_copy = return_types;
	auto names_copy = return_names;
	auto &op_base = planner.Make<PhysicalVgiTableBufferingFunction>(
	    std::move(types_copy), std::move(names_copy), std::move(bind_data), source_order_dependent,
	    sink_order_dependent, requires_input_batch_index, synthesize_batch_index,
	    estimated_cardinality);
	auto &op = op_base.Cast<PhysicalVgiTableBufferingFunction>();
	// Pushdown state is per-query plan data, not bind data.
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

// PhysicalVgiTableBufferingFunction

PhysicalVgiTableBufferingFunction::PhysicalVgiTableBufferingFunction(PhysicalPlan &physical_plan,
                                                                    vector<LogicalType> return_types_p,
                                                                    vector<string> return_names_p,
                                                                    unique_ptr<FunctionData> bind_data_p,
                                                                    bool source_order_dependent_p,
                                                                    bool sink_order_dependent_p,
                                                                    bool requires_input_batch_index_p,
                                                                    bool synthesize_batch_index_p,
                                                                    idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(return_types_p),
                       estimated_cardinality),
      bind_data(std::move(bind_data_p)),
      return_names(std::move(return_names_p)),
      source_order_dependent(source_order_dependent_p),
      sink_order_dependent(sink_order_dependent_p),
      requires_input_batch_index(requires_input_batch_index_p),
      synthesize_batch_index(synthesize_batch_index_p) {
}

string PhysicalVgiTableBufferingFunction::GetName() const {
	return "VGI_TABLE_BUFFERING";
}

InsertionOrderPreservingMap<string> PhysicalVgiTableBufferingFunction::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	result["Name"] = bd.function_name;
	result["Source"] = source_order_dependent ? "ordered" : "parallel";
	// See LogicalVgiTableBufferingFunction::explain_summary.
	if (!explain_summary.empty()) {
		result["Pushdown"] = explain_summary;
	}
	return result;
}

// Global / local state

namespace {

class VgiTableBufferingGlobalSinkState : public GlobalSinkState {
public:
	explicit VgiTableBufferingGlobalSinkState(DatabaseInstance *db_p) : db(db_p) {
	}
	~VgiTableBufferingGlobalSinkState() override;

	// For the destructor's cancel-dispatch and result-cache access.
	DatabaseInstance *db = nullptr;

	// Init coordination (see Sink). init_started: a thread is running the
	// primary init; init_done: execution_id is published; init_failed /
	// init_error: the runner threw, and what. All accessed under init_mutex.
	std::mutex init_mutex;
	std::condition_variable init_cv;
	bool init_started = false;
	bool init_failed = false;
	std::exception_ptr init_error;
	std::atomic<bool> init_done {false};
	std::vector<uint8_t> execution_id;

	// Sink workers handed off by Combine, plus the combine worker after
	// Finalize; the Source phase only counts them (it uses fresh workers).
	// state_ids parallels workers until Finalize and is sent to combine.
	std::mutex workers_mutex;
	std::vector<std::unique_ptr<IFunctionConnection>> workers;
	std::vector<std::vector<uint8_t>> state_ids;

	// synthesize_batch_index numbering; the sink is single-threaded then, so this
	// is source order (atomic anyway).
	std::atomic<int64_t> next_synthetic_batch_index {0};

	// Filled by Finalize after combine; drained by the Source phase.
	std::mutex finalize_queue_mutex;
	std::deque<std::vector<uint8_t>> finalize_queue;

	// Snapshots from bind_data for the RPCs. The destructor's
	// table_buffering_destructor goes out as a pooled unary call, so its
	// request is the only carrier of schema_name (a bare name could resolve to
	// a same-named function in another schema).
	std::string function_name;
	std::string schema_name;
	std::vector<uint8_t> attach_opaque_data;

	// For the destructor's best-effort table_buffering_destructor RPC.
	// context_weak expires if the session ends first.
	weak_ptr<ClientContext> context_weak;
	std::shared_ptr<VgiAttachParameters> attach_params;

	// Set once Finalize has run; Sink asserts on it.
	std::atomic<bool> finalized {false};

	// Exchange-mode result cache (whole-input memoization). Only for
	// !sink_order_dependent: the result is a function of the input multiset, keyed
	// by a commutative additive fold of per-row hashes. A hit skips combine and the
	// finalize drain; the process() RPCs still run, since the key is known only
	// after all input is folded.
	bool cache_eligible = false;
	VgiResultCacheKey cache_key;                 // static; input_hash set at Finalize
	std::string cache_catalog_name;
	int64_t cache_default_ttl_seconds = 0;
	// Additive input digest, merged from per-thread LocalSinkState partials at Combine.
	std::atomic<uint64_t> digest_lo {0};
	std::atomic<uint64_t> digest_hi {0};
	std::atomic<uint64_t> digest_rows {0};
	// Hit: the cached result; serving_claimed lets exactly one Source thread
	// replay it.
	bool serving_from_cache = false;
	std::shared_ptr<const VgiResultCacheEntry> serving;
	std::atomic<bool> serving_claimed {false};
	// Miss: Source batches accumulate under capture_mu; the destructor commits
	// only if every finalize state reached EOS (never a partial result).
	std::mutex capture_mu;
	bool capturing = false;
	VgiCacheControl capture_cc;
	bool capture_cc_latched = false;
	std::vector<VgiBatchPtr> capture_batches;
	size_t total_finalize_states = 0;
	std::atomic<size_t> finalize_eos_count {0};
	// Finalize output not cacheable, or capture over budget: stop capturing.
	std::atomic<bool> capture_aborted {false};
	// The whole result is held in RAM until the destructor commits, so capture is
	// bounded as it goes: crossing max_entry_bytes or the global in-flight budget
	// aborts it (output still streams to DuckDB). Guarded by capture_mu; the
	// reserved in-flight bytes are released in the destructor.
	int64_t cache_max_entry_bytes = defaults::RESULT_CACHE_MAX_ENTRY_BYTES;
	int64_t capture_bytes = 0;
	int64_t reserved_inflight_bytes = 0;
};

VgiTableBufferingGlobalSinkState::~VgiTableBufferingGlobalSinkState() {
	// Even on the happy path `workers` holds the idle Sink and combine workers
	// (the Source phase never uses them). Pool each; one that cannot be pooled
	// (HTTP, pooling off, dead) is cancel-dispatched so a worker parked in a
	// blocking call is woken rather than left parked.
	auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
	auto reclaim_or_cancel = [&](std::unique_ptr<IFunctionConnection> conn) {
		if (!conn) {
			return;
		}
		// Release() skips dead workers.
		if (auto pooled = conn->ReleaseForPooling()) {
			(void)VgiWorkerPool::Instance().Release(std::move(pooled));
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

	// In-flight LocalSinkState connections (still owned by a thread's lstate)
	// handle their own cancel-dispatch in ~VgiTableBufferingLocalSinkState.

	// Best-effort table_buffering_destructor RPC: the worker clears its
	// storage for this execution_id. Skipped if init never published an
	// execution_id or the session is gone (the worker's storage expiry,
	// cleanup_old_entries in the Python SDK, is then the backstop).
	if (!execution_id.empty() && attach_params) {
		auto context_lock = context_weak.lock();
		if (context_lock) {
			try {
				auto rpc_params = vgi::BuildTableBufferingDestructorInner(function_name, schema_name, execution_id,
				                                                             attach_opaque_data);
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
				opts.iroh = attach_params->iroh();
				if (attach_params->launcher_idle_timeout_seconds().has_value()) {
					opts.launcher_idle_timeout =
					    std::chrono::seconds(*attach_params->launcher_idle_timeout_seconds());
				}
				if (attach_params->launcher_state_dir().has_value()) {
					opts.launcher_state_dir = *attach_params->launcher_state_dir();
				}
				(void)vgi::InvokePooledUnaryRpc(opts, "table_buffering_destructor", rpc_params);
			} catch (...) {
				// Destructors must not throw.
			}
		}
	}

	// Commit the captured result only if every finalize state reached EOS (a
	// mid-drain error or early LIMIT teardown stores nothing). This runs after the
	// Source phase, which shares this state. allow_disk: one large result, like a
	// producer entry.
	if (capturing && !capture_aborted.load() && total_finalize_states > 0 &&
	    finalize_eos_count.load(std::memory_order_acquire) == total_finalize_states &&
	    capture_cc.Cacheable()) {
		try {
			auto sr = StoreExchangeMemoEntry(GetResultCache(*db), cache_key, capture_cc, cache_catalog_name,
			                                 cache_default_ttl_seconds, capture_batches, /*allow_disk=*/true);
			if (sr.stored) {
				GetResultCache(*db).RecordExchangeStore();
			}
		} catch (...) {
			// Destructor must not throw; a failed cache store is non-fatal.
		}
	}
	// Release the in-flight capture budget (0 if never captured or aborted).
	GetResultCache(*db).ReleaseInflightCapture(reserved_inflight_bytes);
}

class VgiTableBufferingLocalSinkState : public LocalSinkState {
public:
	// Per-thread worker, acquired on the thread's first Sink call.
	std::unique_ptr<IFunctionConnection> connection;
	// Worker-chosen opaque state_id, returned by each process() RPC.
	std::vector<uint8_t> state_id;

	// Set at Sink time for the destructor's cancel-dispatch.
	DatabaseInstance *db = nullptr;

	// This thread's partial input digest, merged into the gstate at Combine.
	uint64_t digest_lo = 0;
	uint64_t digest_hi = 0;
	uint64_t digest_rows = 0;

	~VgiTableBufferingLocalSinkState() override {
		// Still owning a connection means an error unwound the pipeline before
		// Combine moved it to the gstate. Cancel-dispatch it: that is the only
		// thing that unblocks a worker parked in a blocking process() RPC.
		if (!connection) {
			return;
		}
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
	// MaxThreads cap: the number of Sink-phase workers (see GetGlobalSourceState).
	idx_t worker_count = 0;

	idx_t MaxThreads() override {
		return worker_count > 0 ? worker_count : 1;
	}
};

class VgiTableBufferingLocalSourceState : public LocalSourceState {
public:
	// A fresh worker per finalize_state_id (Sink workers would reject a second
	// PerformInit), draining one TABLE_BUFFERING_FINALIZE stream.
	std::unique_ptr<IFunctionConnection> worker;
	std::vector<uint8_t> current_finalize_state_id;
	// The stream for current_finalize_state_id is open (or a cache replay).
	bool stream_open = false;
	// Arrow → DuckDB plumbing, rebuilt per batch from the batch's own schema.
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	// The batch being emitted, kept across GetData calls so one larger than
	// STANDARD_VECTOR_SIZE is emitted over several chunks, not truncated.
	std::unique_ptr<ArrowScanLocalState> scan_state;

	// Set with the worker, for the destructor's cancel-dispatch.
	DatabaseInstance *db = nullptr;

	~VgiTableBufferingLocalSourceState() override {
		if (!worker) {
			return;
		}
		// At EOS GetDataInternal pools the worker, so holding one here means
		// PerformInit or ReadDataBatch threw, or DuckDB tore the source down
		// early (LIMIT, interrupt). Cancel-dispatch: the worker gets its
		// on_cancel and a blocked call unblocks.
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

// global_execution_id: empty = primary init, non-empty = secondary.
FunctionConnectionParams BuildAcquireParams(const VgiTableInOutBindData &bd,
                                             const std::vector<uint8_t> &global_execution_id) {
	FunctionConnectionParams params;
	params.attach_params = bd.attach_params;
	params.attach_opaque_data = bd.attach_opaque_data;
	params.transaction_opaque_data = bd.transaction_opaque_data;
	params.function_name = bd.function_name;
	params.schema_name = bd.schema_name;
	params.settings = bd.settings;
	params.required_secrets = bd.required_secrets;
	params.global_execution_id = global_execution_id;
	params.function_type = "TABLE";
	params.input_schema = bd.input_schema;
	return params;
}

// A TABLE_BUFFERING init opens an exchange stream that is never used (all
// later traffic is unary). Close its input (EOS) and drain its output so the
// connection is free for the table_buffering_* RPCs.
void DrainBufferingInitStream(IFunctionConnection &conn) {
	conn.OpenInputWriter();
	conn.CloseInputWriter();
	auto batch = conn.ReadDataBatch();
	while (batch) {
		batch = conn.ReadDataBatch();
	}
}

// Emit up to STANDARD_VECTOR_SIZE rows of the current batch, advancing
// chunk_offset. Like ProduceOutputFromBatch (vgi_table_in_out_impl.cpp), minus
// LATERAL columns.
void EmitBufferedPage(ArrowScanLocalState &scan_state, const ArrowTableSchema &arrow_table, DataChunk &chunk,
                      bool projection_pushdown) {
	idx_t length = static_cast<idx_t>(scan_state.chunk->arrow_array.length);
	idx_t remaining = length - scan_state.chunk_offset;
	idx_t rows = std::min<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	chunk.SetCardinality(rows);
	// column_ids stays empty: ArrowToDuckDB reads positionally, since the worker
	// emits batches aligned to the projected output.
	ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), chunk, projection_pushdown);
	scan_state.chunk_offset += rows;
}

} // namespace

// Sink

unique_ptr<GlobalSinkState>
PhysicalVgiTableBufferingFunction::GetGlobalSinkState(ClientContext &context) const {
	auto *db = &DatabaseInstance::GetDatabase(context);
	auto gstate = make_uniq<VgiTableBufferingGlobalSinkState>(db);
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	gstate->function_name = bd.function_name;
	gstate->schema_name = bd.schema_name;
	gstate->attach_opaque_data = bd.attach_opaque_data;
	gstate->context_weak = context.shared_from_this();
	gstate->attach_params = bd.attach_params;

	// Result cache: unordered sinks only (see cache_eligible).
	if (!bd.sink_order_dependent) {
		std::vector<int32_t> proj_key;
		if (bd.projection_pushdown) {
			proj_key = projection_ids; // the projection the worker is init'd with
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

	// DuckDB never sends input after Finalize; fail loudly if that changes
	// rather than produce wrong results.
	if (gstate.finalized.load()) {
		throw InternalException(
		    "PhysicalVgiTableBufferingFunction::Sink called after Finalize returned READY — "
		    "the strict once-per-GlobalSinkState invariant has been violated.");
	}

	if (gstate.cache_eligible) {
		AccumulateInputDigest(chunk, lstate.digest_lo, lstate.digest_hi, lstate.digest_rows);
	}

	// Init: the first thread's primary init mints execution_id; the others wait
	// for it, then run secondary inits in parallel (see the class comment).
	if (!lstate.connection) {
		bool i_am_runner = false;
		std::vector<uint8_t> exec_id;

		{
			std::unique_lock<std::mutex> lk(gstate.init_mutex);
			// All of these are read under init_mutex; the acquire on init_done is
			// redundant. Reading them outside the lock would need init_started /
			// init_failed to become atomics too.
			if (gstate.init_done.load(std::memory_order_acquire)) {
				exec_id = gstate.execution_id;
			} else if (!gstate.init_started) {
				gstate.init_started = true;
				i_am_runner = true;
			} else {
				// Wait for the runner, polling for interrupt every 250 ms so a
				// cancel is not stuck behind a slow spawn or OAuth refresh.
				while (!gstate.init_done.load(std::memory_order_acquire) && !gstate.init_failed) {
					if (context.client.IsInterrupted()) {
						throw InterruptException();
					}
					gstate.init_cv.wait_for(lk, std::chrono::milliseconds(250));
				}
				if (gstate.init_failed) {
					if (gstate.init_error) {
						std::rethrow_exception(gstate.init_error);
					}
					throw IOException("table_buffering init failed on peer thread");
				}
				exec_id = gstate.execution_id;
			}
		}

		// Before acquiring: if PerformInit or the drain throws, the lstate
		// destructor needs db to cancel-dispatch rather than just drop the
		// connection.
		lstate.db = gstate.db;

		if (i_am_runner) {
			// Primary init on this thread's own worker, which then serves this
			// thread like any secondary. Known limitation: the runner only checks
			// for interrupt at fd reads, so cancellation waits out
			// non-interruptible acquire steps (posix_spawn, pool contention).
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
				DrainBufferingInitStream(*lstate.connection);
				// state_id comes from the first process() RPC, so it is logged there.
				if (VgiInfoLogActive(context.client)) {
					VGI_LOG(context.client, "table_buffering.init",
					        {{"conn", lstate.connection->GetConnIdHex()},
					         {"function_name", bd.function_name},
					         {"role", "init_runner"}});
				}
			} catch (...) {
				{
					std::lock_guard<std::mutex> lk(gstate.init_mutex);
					gstate.init_failed = true;
					gstate.init_error = std::current_exception();
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
			auto params = BuildAcquireParams(bd, exec_id);
			auto acquired = AcquireConnectionForInit(context.client, params);
			lstate.connection = std::move(acquired.connection);
			lstate.connection->PerformInit(bd.bind_result, projection_ids,
			                                pushdown_filters,
			                                join_keys_buffers,
			                                /*phase=*/"TABLE_BUFFERING");
			DrainBufferingInitStream(*lstate.connection);
			if (VgiInfoLogActive(context.client)) {
				VGI_LOG(context.client, "table_buffering.init",
				        {{"conn", lstate.connection->GetConnIdHex()},
				         {"function_name", bd.function_name},
				         {"role", "secondary"}});
			}
		}
	}

	auto input_batch = DataChunkToArrow(context.client, chunk, bd.input_schema);

	// batch_index, by the route CreatePlan chose: DuckDB's partition_info
	// (RequiredPartitionInfo()=BatchIndex()), or our own numbering on a
	// serialized sink. The throw is a backstop for a source that claims support
	// but supplies nothing: sending nullopt would give quietly wrong answers.
	std::optional<int64_t> batch_index;
	if (synthesize_batch_index) {
		batch_index = gstate.next_synthetic_batch_index.fetch_add(1);
	} else if (requires_input_batch_index) {
		// partition_info is on the LocalSinkState base class.
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

	// The worker-chosen state_id is opaque bytes, passed through unchanged.
	lstate.state_id = lstate.connection->RpcTableBufferingProcess(
	    gstate.function_name, gstate.execution_id, input_batch, batch_index);
	if (VgiInfoLogActive(context.client)) {
		// state_id is a worker-issued value the client stores and returns, so it
		// is logged only as a short digest (OpaqueDigest), never raw.
		vector<std::pair<string, string>> info {
		    {"conn", lstate.connection->GetConnIdHex()},
		    {"function_name", bd.function_name},
		    {"state_id_sha256", OpaqueDigest(lstate.state_id)},
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
	return SinkNextBatchType::READY;
}

SinkCombineResultType
PhysicalVgiTableBufferingFunction::Combine(ExecutionContext & /*context*/,
                                           OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<VgiTableBufferingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<VgiTableBufferingLocalSinkState>();

	// Merge the thread's digest (commutative, so order-independent).
	if (gstate.cache_eligible) {
		gstate.digest_lo.fetch_add(lstate.digest_lo, std::memory_order_relaxed);
		gstate.digest_hi.fetch_add(lstate.digest_hi, std::memory_order_relaxed);
		gstate.digest_rows.fetch_add(lstate.digest_rows, std::memory_order_relaxed);
	}

	// No input on this thread: nothing to hand off.
	if (!lstate.connection) {
		return SinkCombineResultType::FINISHED;
	}

	// The gstate now owns the worker (the lstate destructor sees null).
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

	// The input digest is complete: probe the result cache. On a hit the Source
	// phase replays the cached result; no combine, no finalize drain.
	if (gstate.cache_eligible) {
		gstate.cache_key.input_hash =
		    FinalizeInputDigest(gstate.digest_lo.load(), gstate.digest_hi.load(), gstate.digest_rows.load());
		const char *hit_tier = "memory";
		auto entry = GetResultCache(*gstate.db).Lookup(gstate.cache_key, std::chrono::steady_clock::now(), &hit_tier);
		if (entry) {
			GetResultCache(*gstate.db).RecordExchangeHit(entry->total_bytes);
			VGI_LOG(context, "result_cache.hit",
			        {{"function", gstate.function_name}, {"key_hash", gstate.cache_key.HexDigest()}, {"tier", hit_tier}});
			gstate.serving_from_cache = true;
			gstate.serving = entry;
			gstate.finalized.store(true);
			return SinkFinalizeType::READY;
		}
		GetResultCache(*gstate.db).RecordExchangeMiss();
	}

	// Any worker can run combine (state is shared through BoundStorage); pop one
	// so nothing else uses its connection meanwhile.
	std::vector<std::vector<uint8_t>> state_ids_snapshot;
	std::unique_ptr<IFunctionConnection> combine_worker;
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		if (!gstate.workers.empty()) {
			state_ids_snapshot = gstate.state_ids;
			combine_worker = std::move(gstate.workers.back());
			gstate.workers.pop_back();
		}
	}
	if (!combine_worker) {
		// Runtime-empty input: no Sink thread got rows. Still run init, combine
		// with no state_ids, and the finalize drain, so whole-input reductions
		// answer for empty input (a count emits 0), as the Python client and
		// COPY TO (VgiCopyToFinalize) do. It must be a primary TABLE_BUFFERING
		// init: that mints execution_id and is where worker SDKs persist the
		// bind state combine and finalize read. Finalize runs after every
		// Combine, so no handshake with Sink threads is needed.
		auto &bd = bind_data->Cast<VgiTableInOutBindData>();
		try {
			auto params = BuildAcquireParams(bd, /*global_execution_id=*/{});
			auto acquired = AcquireConnectionForInit(context, params);
			combine_worker = std::move(acquired.connection);
			auto init_result = combine_worker->PerformInit(bd.bind_result, projection_ids, pushdown_filters,
			                                               join_keys_buffers, /*phase=*/"TABLE_BUFFERING");
			DrainBufferingInitStream(*combine_worker);
			{
				std::lock_guard<std::mutex> lk(gstate.init_mutex);
				gstate.execution_id = std::move(init_result.execution_id);
				gstate.init_done.store(true, std::memory_order_release);
			}
			VGI_LOG(context, "table_buffering.init",
			        {{"conn", combine_worker->GetConnIdHex()},
			         {"function_name", bd.function_name},
			         {"role", "empty_input"}});
		} catch (...) {
			gstate.finalized.store(true);
			auto *dispatcher = gstate.db ? FindVgiCancelDispatcher(*gstate.db) : nullptr;
			if (combine_worker && dispatcher) {
				auto token = combine_worker->GetLastStateToken();
				CancelRequest req;
				req.connection = std::move(combine_worker);
				req.state_token = std::move(token);
				(void)dispatcher->Enqueue(std::move(req));
			}
			throw;
		}
	}

	// Outside the lock: it is a round trip.
	VGI_LOG(context, "table_buffering.combine",
	        {{"conn", combine_worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"num_state_ids", std::to_string(state_ids_snapshot.size())}});
	std::vector<std::vector<uint8_t>> finalize_state_ids;
	try {
		finalize_state_ids =
		    combine_worker->RpcTableBufferingCombine(gstate.function_name, gstate.execution_id,
		                                              state_ids_snapshot);
	} catch (...) {
		// Cancel-dispatch the popped worker (otherwise it stays parked in a
		// blocking call), and pop one state_id so workers/state_ids stay the
		// same size.
		gstate.finalized.store(true);
		{
			// (On the empty-input path state_ids is empty, so nothing pops.)
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
		// Without a dispatcher, combine_worker just drops.
		throw;
	}
	VGI_LOG(context, "table_buffering.combine_result",
	        {{"conn", combine_worker->GetConnIdHex()},
	         {"function_name", gstate.function_name},
	         {"num_finalize_state_ids", std::to_string(finalize_state_ids.size())}});

	// Return the worker to gstate.workers. state_ids is not read after this, so
	// it is left unchanged (it may now be shorter than workers).
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		gstate.workers.push_back(std::move(combine_worker));
	}
	{
		std::lock_guard<std::mutex> lk(gstate.finalize_queue_mutex);
		for (auto &id : finalize_state_ids) {
			gstate.finalize_queue.push_back(std::move(id));
		}
		// Cache miss: arm capture; total_finalize_states is the EOS target.
		if (gstate.cache_eligible) {
			gstate.capturing = true;
			gstate.total_finalize_states = gstate.finalize_queue.size();
		}
	}
	gstate.finalized.store(true);
	return SinkFinalizeType::READY;
}

// Source

unique_ptr<GlobalSourceState>
PhysicalVgiTableBufferingFunction::GetGlobalSourceState(ClientContext & /*context*/) const {
	auto gss = make_uniq<VgiTableBufferingGlobalSourceState>();
	// The sink has finished by now; its worker count caps MaxThreads.
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
	// The Sink global state, shared with the Source phase via sink_state.
	auto &gstate = sink_state->Cast<VgiTableBufferingGlobalSinkState>();
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	if (gstate.cache_eligible && QueryProfiler::Get(context.client).IsEnabled()) {
		context.thread.profiler.GetOperatorMetrics(*this).AddExtraInfo(
		    "Cache", gstate.serving_from_cache ? "hit (memory)" : "miss");
	}

	// Finish emitting the current batch first (see scan_state).
	if (lstate.scan_state &&
	    lstate.scan_state->chunk_offset < static_cast<idx_t>(lstate.scan_state->chunk->arrow_array.length)) {
		EmitBufferedPage(*lstate.scan_state, lstate.arrow_table, chunk, projection_pushdown);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	lstate.scan_state.reset();

	// Cache hit: one Source thread replays the entry; the rest finish.
	if (gstate.serving_from_cache && !lstate.stream_open) {
		if (gstate.serving_claimed.exchange(true)) {
			chunk.SetCardinality(0);
			return SourceResultType::FINISHED;
		}
		lstate.worker = make_uniq<CachedReplayConnection>(gstate.serving);
		lstate.stream_open = true;
	}

	// Open a fresh worker and stream for the next finalize_state_id.
	if (!lstate.stream_open) {
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
		// Fresh, not from gstate.workers (see LocalSourceState::worker); the
		// pool keeps subprocess workers warm.
		auto acquire_params = BuildAcquireParams(bd, gstate.execution_id);
		auto acquired = AcquireConnectionForInit(context.client, acquire_params);
		lstate.worker = std::move(acquired.connection);
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
		// Same projection/filters as the Sink inits: finalize() must emit the
		// projected (narrow) schema this operator advertises, or ArrowToDuckDB
		// reads out of bounds.
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
		// EOS for this finalize_state_id: count it for capture and pool the worker.
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

	// Cache miss: capture the result. The first non-empty batch's cache
	// advertisement decides cacheability; 0-row batches are not captured.
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
			// Over budget (see capture_bytes): abort and free the capture now.
			int64_t sz = batch->TotalBufferSize();
			if (gstate.capture_bytes + sz > gstate.cache_max_entry_bytes ||
			    !GetResultCache(*gstate.db).TryReserveInflightCapture(sz)) {
				gstate.capture_aborted.store(true);
				GetResultCache(*gstate.db).NoteCaptureAbort();
				GetResultCache(*gstate.db).ReleaseInflightCapture(gstate.reserved_inflight_bytes);
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

	// Convert the Arrow batch → DuckDB DataChunk. Unlike the streaming InOut
	// path, which builds its type map once at bind, this one is per batch.
	ExportSchema(batch->schema(), lstate.c_schema);
	// Reset first: PopulateArrowTableSchema appends via AddColumn, which
	// D_ASSERTs on a repeated column index.
	lstate.arrow_table = ArrowTableSchema();
	ArrowTableFunction::PopulateArrowTableSchema(context.client, lstate.arrow_table,
	                                              lstate.c_schema.arrow_schema);

	// Type-confusion guard: the type map comes from the batch's own schema but
	// fills vectors of the declared return type. Skipped for cache replay. See
	// vgi_batch_validation.hpp.
	if (!lstate.worker->IsCachedReplay()) {
		vgi::ValidateProjectedWireBatch(&context.client, *batch, bd.output_schema, projection_ids, bd.worker_path(),
		                                bd.function_name);
	}

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(batch, *chunk_wrapper);

	// Kept on the lstate so a long batch is emitted over several calls; see
	// EmitBufferedPage for why column_ids stays empty.
	lstate.scan_state = make_uniq<ArrowScanLocalState>(std::move(chunk_wrapper), context.client);
	lstate.scan_state->chunk_offset = 0;

	EmitBufferedPage(*lstate.scan_state, lstate.arrow_table, chunk, projection_pushdown);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
