// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include "vgi_settings_defaults.hpp"
#include <atomic>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <utility>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_global_functions.hpp" // VgiFunctionRegistrationTarget
#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_result_cache.hpp" // result-cache key/entry/capture types
#include "vgi_worker_pool.hpp"

namespace duckdb {

class DatabaseInstance;

namespace vgi {

//! Invocation metadata attached to each VGI TableFunction's function_info;
//! read at bind via input.info->Cast<VgiTableFunctionInfo>().
class VgiTableFunctionInfo final : public TableFunctionInfo {
public:
	VgiTableFunctionInfo(VgiFunctionRegistrationTarget target, VgiFunctionInfo function_info)
	    : target_(std::move(target)), function_info_(std::move(function_info)) {
	}

	~VgiTableFunctionInfo() override = default;

	//! Where this function was registered + the connection state captured then.
	//! For a globally-published function (`target().IsGlobal()`) the live state
	//! is re-resolved at bind — see ResolveVgiFunctionBinding.
	const VgiFunctionRegistrationTarget &target() const {
		return target_;
	}

	//! Attach alias of the owning catalog.
	const std::string &catalog_name() const {
		return target_.catalog_name;
	}

	const std::shared_ptr<VgiAttachParameters> &attach_params() const {
		return target_.attach_params;
	}

	const std::string &worker_path() const {
		return target_.attach_params->worker_path();
	}

	const std::vector<uint8_t> &attach_opaque_data() const {
		return target_.attach_opaque_data;
	}

	bool worker_debug() const {
		return target_.attach_params->worker_debug();
	}

	bool use_pool() const {
		return target_.attach_params->use_pool();
	}

	const VgiFunctionInfo &function_info() const {
		return function_info_;
	}

	const std::vector<std::string> &setting_names() const {
		return target_.setting_names;
	}

private:
	VgiFunctionRegistrationTarget target_;
	VgiFunctionInfo function_info_;
};

class VgiQueryPreparationLease;

//! Bind data shared by VGI table-function calls and catalog-table scans.

struct VgiTableFunctionBindData : public TableFunctionData {
	// Explicit remote SQL owns a transaction-scoped preparation and must rebind
	// on every DuckDB prepared-statement execution, including parameter changes.
	bool explicit_query = false;
	// Accepted pure projection proposal, retained through copy/serialization.
	std::vector<uint8_t> projection_request_bytes;
	std::vector<uint8_t> projection_response_bytes;
	std::shared_ptr<VgiQueryPreparationLease> query_preparation;
	bool SupportStatementCache() const override;

	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data;

	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }

	std::string function_name;
	// Catalog schema that owns `function_name`. The same name may be registered
	// in several schemas of one catalog, so the worker resolves the pair rather
	// than the bare name. Empty for non-catalog call sites.
	std::string schema_name;

	ArrowArguments arguments;

	// Settings forwarded to the worker.
	std::map<std::string, Value> settings;

	// Secrets declared in function metadata.
	std::vector<VgiSecretRequirement> required_secrets;
	// True for metadata-declared secrets OR secrets the worker requested during
	// its two-phase bind. The result cache keys such a result on
	// bind_result.secret_scope.
	bool secret_dependent = false;

	// Output schema from bind, as the C ABI wrapper and DuckDB's Arrow schema.
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;

	int32_t max_processes = 1;
	// True iff the worker declared FIXED_ORDER (and not supports_batch_index).
	// DuckDB does NOT serialize such a source by itself —
	// ``PhysicalTableScan::ParallelSource()`` returns true for any scan — so
	// this is mirrored onto ``VgiTableFunctionGlobalState::fixed_order`` and
	// ``MaxThreads()`` clamps to 1.
	bool fixed_order = false;

	// True iff the worker declared ``Meta.supports_batch_index = True``. Then
	// ``VgiGetPartitionData`` is installed and bind leaves ``fixed_order``
	// false (vgi_table_function_set.cpp): the source stays parallel and ordered
	// sinks reassemble by batch_index. Every data batch MUST carry
	// ``vgi_batch_index`` metadata; ``InstallBatch`` parses it into
	// ``VgiTableFunctionLocalState::current_batch_index`` and enforces
	// per-stream monotonicity (DuckDB's release-build checks are
	// global-uniqueness-only).
	bool supports_batch_index = false;

	// True iff the worker declared ``Meta.supports_splits = True`` AND the
	// ``vgi_split_scans`` setting is on. Both are folded in at BIND, so this single
	// flag is the gate for the whole split path — including the invariants that
	// must hold before it runs (revalidation refusal, the capture accounting, the
	// MaxThreads clamp).
	bool supports_splits = false;
	// The worker's declared split-token lifetime. Absent means UNBOUNDED, not
	// "expires immediately": TTL checks fire only on a declared value.
	std::optional<int64_t> split_token_ttl_seconds;

	// Worker-declared partition shape (Meta.partition_kind). When not
	// ``NotPartitioned``, vgi_table_function_set.cpp installs
	// ``get_partition_info`` so the planner can pick
	// PhysicalPartitionedAggregate for matching GROUP BY queries.
	VgiPartitionKind partition_kind = VgiPartitionKind::NotPartitioned;

	// Bind-output-schema indices of fields marked ``vgi.partition_column ==
	// "true"``, resolved once at bind. Non-empty iff partition_kind !=
	// NotPartitioned (enforced at bind with a BinderException).
	std::vector<idx_t> partition_column_indices;

	mutable int64_t cardinality_estimate = -1;
	// Max cardinality for NodeStatistics; -1 = unknown. From the inlined
	// ``TableInfo`` (catalog tables) or ``table_function_cardinality`` (RPC).
	mutable int64_t cardinality_max = -1;

	bool projection_pushdown = false;

	// VGI v2 expression-filter capabilities advertised by the worker.
	std::vector<std::string> filter_semantic_profiles;
	std::vector<VgiFilterFunctionCapability> additional_filter_functions;

	vector<string> all_column_names;
	// Parallel to all_column_names; used by the statistics callback.
	vector<LogicalType> all_column_types;

	// Catalog table being scanned; null for a table-function call.
	optional_ptr<TableCatalogEntry> table_entry;

	// Worker-output-schema index of the is_row_id column (-1 = none) and its type.
	int rowid_worker_col_index = -1;
	LogicalType rowid_type = LogicalType::INVALID;
	// Worker field name of the rowid column, captured BEFORE the rowid is erased
	// from all_column_names: a filter on COLUMN_IDENTIFIER_ROW_ID must carry this
	// name for the worker to match it. Empty when there is no rowid.
	std::string rowid_column_name;

	// Retained for init and lazy cardinality/statistics RPCs. Init carries
	// bind_request_bytes, output_schema_bytes and opaque_data inline, so no
	// second bind goes over the wire.
	BindResult bind_result;

	mutable bool cardinality_fetched = false;

	// Lazy per-column cache of the table_function_statistics RPC (catalog tables
	// reach it only when VgiTableEntry::GetStatistics declines). `statistics_mutex`
	// guards both fields: the callback may run on several optimizer threads.
	mutable std::mutex statistics_mutex;
	mutable bool statistics_fetched = false;
	mutable std::unordered_map<std::string, unique_ptr<BaseStatistics>> statistics_cache;

	// Set by the set_scan_order callback during optimization.
	mutable std::optional<OrderByHint> order_by_hint;

	// TABLESAMPLE SYSTEM hint, read from input.sample_options in InitGlobal
	// (which receives const bind_data).
	mutable std::optional<TableSampleHint> table_sample_hint;

	// AT (...) time-travel clause of a catalog-table scan (empty otherwise), so
	// the vgi_table_scan (de)serialize callbacks can rebuild an identical bind
	// after a logical-plan deep copy (e.g. the WindowSelfJoin optimizer).
	std::string at_unit;
	std::string at_value;

	//! COPY ... FROM context, set only by VgiCopyFromBind. Carried in the bind
	//! request (and so in init's bind_request_bytes) for the worker's
	//! CopyFromFunction.
	std::optional<CopyFromBindContext> copy_from;

	//! Deep copy, e.g. for the late-materialization optimizer's CreateLHSGet.
	//! Shares the immutable Arrow conversion state and rebuilds the C-ABI schema
	//! from bind_result.output_schema. The statistics cache is NOT copied; the
	//! clone re-fetches lazily.
	unique_ptr<FunctionData> Copy() const override;
};

// A captured DynamicFilter for tick-based pushdown.
struct VgiDynamicFilterInfo {
	shared_ptr<DynamicFilterData> filter_data;
	//! Column index in the worker's unprojected output schema
	idx_t column_index;
	string column_name;
	ExpressionType comparison_type;
	//! Whether this filter is wrapped in ConjunctionOr with IsNull (NULLS_FIRST)
	bool nulls_first = false;
	//! Stable v2 predicate identity and monotonic revision for tick deltas.
	//! The revision advances only when the value (or active state) changes.
	string predicate_id;
	uint64_t revision = 0;
	bool active = false;
	//! The value emitted under `revision` while active, to detect a change.
	ExpressionType emitted_comparison = ExpressionType::INVALID;
	Value emitted_constant;
};

// Result-cache capture for a cache-eligible scan that missed. Each local state
// captures its own substream, so the append path takes no lock (`mu` guards only
// `streams` and the cache-control latch). Committed in the gstate destructor iff
// the complete result was drained (never-partial invariant).
struct VgiResultCaptureCtx {
	VgiResultCache *cache = nullptr; // owning database outlives query capture
	VgiResultCacheKey key;
	std::string catalog_name;
	//! Current transaction id; folded into the key at commit iff the worker
	//! advertised scope=transaction (so the entry is only reused within this txn).
	std::string transaction_id;
	int64_t max_entry_bytes = 0;
	int64_t max_bytes = 0;
	uint64_t default_ttl_seconds = 0;
	bool catalog_version_frozen = false; // allows never-expires (at-pinned / frozen)

	std::atomic<int64_t> total_bytes {0};
	// Bytes reserved against the database-wide in-flight capture budget
	// (VgiResultCache::TryReserveInflightCapture); released at gstate teardown.
	std::atomic<int64_t> reserved_inflight_bytes {0};
	std::atomic<int> launched {0}; // ++ in InitLocal
	std::atomic<int> eos {0};      // ++ when a local state runs out of work

	//! Split-path never-partial accounting. ``eos == launched`` does not prove
	//! completeness: a reader destroyed mid-split consumed a claim without
	//! reading it. Drained splits must also equal the planned total.
	bool split_path = false;
	idx_t total_splits = 0;
	std::atomic<idx_t> completed_splits {0}; // ++ at a split's true EOS
	// Commit ONLY when `eos == launched` and nothing aborted. A mid-stream error
	// throws out of ReadDataBatch before that thread reaches EOS, so it leaves
	// `eos < launched` without any extra flag.
	std::atomic<bool> aborted {false};

	std::mutex mu; // guards `streams`, `cc_seen`, `cc`
	std::vector<std::unique_ptr<CachedStream>> streams;
	bool cc_seen = false;      // a vgi.cache.* advertisement has been latched
	VgiCacheControl cc;        // the latched advertisement (from the first advertising batch)

	// Threshold spill-to-disk: capture buffers in RAM up to max_entry_bytes; past
	// that, with the disk tier on, it SPILLS to a streaming disk blob instead of
	// aborting, so RAM stays at ~max_entry_bytes. The writer is created under `mu`
	// at the first crossing. Once `spilling` is set, InstallBatch appends to
	// `disk_writer` and each thread drains its own RAM substream on its next
	// batch; the gstate dtor drains substreams that finished before the spill. A
	// spilled entry is disk-only.
	std::string disk_dir;
	uint64_t disk_max = 0;
	std::string disk_compression = "zstd"; // resolved lazily by the writer at first spill
	uint64_t disk_compression_level = 1;
	std::atomic<bool> spilling {false};
	std::shared_ptr<VgiCaptureDiskWriter> disk_writer; // created under `mu` at first spill
	bool streaming() const {
		return spilling.load(std::memory_order_acquire);
	}

	// --- Per-partition split (SINGLE_VALUE_PARTITIONS) ----------------------
	// Armed at InitGlobal when partition_kind == SingleValuePartitions, the
	// vgi_result_cache_partition_scope setting is on, and the filter shape is
	// splittable; the split at commit additionally needs `cc.partition_scope`.
	// Additive to the whole-scan entry: one entry per distinct partition-value
	// tuple, keyed by (static + `residual_filter_bytes`) with input_hash = "p:" +
	// sha256(CanonicalPartitionTupleKey(...)). See docs/result_cache.md.
	bool partition_split_requested = false;
	std::vector<idx_t> partition_column_indices;   // output-schema indices, declared order
	std::vector<LogicalType> partition_types;      // matching declared partition types
	std::vector<std::string> partition_names;      // matching declared partition column names
	std::string residual_filter_bytes;             // filter_bytes with the partition predicate stripped
	uint64_t partition_max = defaults::RESULT_CACHE_PARTITION_MAX_ENUMERATED;                  // cap on distinct partitions per split

	// Register a per-local-state substream; increments `launched`.
	CachedStream *NewStream() {
		std::lock_guard<std::mutex> lk(mu);
		streams.push_back(make_uniq<CachedStream>());
		launched.fetch_add(1, std::memory_order_relaxed);
		return streams.back().get();
	}
};

struct VgiTableFunctionGlobalState : public GlobalTableFunctionState {
	// --- Result-cache serve / capture -------------------------------------
	// Cache hit: serving_entry is replayed via CachedReplayConnection on one
	// thread (MaxThreads() == 1). serve_claimed stops a second local state, if
	// DuckDB ever creates one, from duplicating rows.
	bool serving_from_cache = false;
	std::shared_ptr<const VgiResultCacheEntry> serving_entry;
	std::atomic<bool> serve_claimed {false};
	// Opt-in gates passed. Drives EXPLAIN ANALYZE's "Cache: miss" label; an
	// ineligible scan gets no Cache line.
	bool cache_eligible = false;
	// Capture (cache miss + eligible). Null when the scan isn't cacheable.
	std::shared_ptr<VgiResultCaptureCtx> capture;

	// --- Conditional revalidation ----------------------------------------
	// A stale-but-revalidatable entry: init sent if_none_match /
	// if_modified_since from its etag/last_modified. A 0-row
	// vgi.cache.not_modified reply makes GetNextBatch slide the entry's TTL and
	// replay it (MaxThreads() == 1); fresh data is recorded by `capture` and
	// replaces the stale entry at gstate destruction.
	bool revalidating = false;
	std::shared_ptr<const VgiResultCacheEntry> revalidation_entry;
	std::string revalidate_if_none_match;   // stored etag ("" = none)
	std::string revalidate_if_modified_since; // stored last_modified ("" = none)

	//! True iff this scan takes the split path (worker capability AND the
	//! ``vgi_split_scans`` setting, folded together at bind).
	bool supports_splits = false;

	//! Splits returned by ``plan()``, in the order the worker emitted them.
	//! Empty is legal and means "no work" — not an error.
	std::vector<VgiScanSplit> splits;

	//! SINGLE_VALUE_PARTITIONS data from protocol-3.1 split key_value batches.
	//! Populated only when the plan grouping exactly matches the bind-time
	//! partition columns and every split value validates.
	bool split_partition_data_enabled = false;
	std::vector<duckdb::vector<ColumnPartitionData>> split_partition_data;

	//! Next unclaimed split, claimed with ``fetch_add(1)``. Greedy claiming
	//! self-balances, and ascending claims are the ONLY thing keeping
	//! ``batch_index`` (derived from split position) monotone per reader — do not
	//! replace with work-stealing or a shuffled assignment.
	std::atomic<idx_t> next_split {0};

	//! Splits claimed AND fully drained. ``next_split`` alone over-reports (a
	//! reader destroyed mid-split consumed a claim), which would let a partial
	//! result be cached as whole.
	std::atomic<idx_t> completed_splits {0};
	std::atomic<bool> revalidation_served {false};

	// Commits a completed capture to the result cache (never-partial gate).
	~VgiTableFunctionGlobalState() override;

	// --- Async init plumbing ----------------------------------------------
	// DuckDB runs independent pipelines' `init_global` serially on the main
	// thread (Executor::ScheduleEventsInternal), so a synchronous init RPC would
	// block every other pipeline's init. The RPC runs on a background thread and
	// is awaited (EnsureInitApplied) before anything touches the connection or
	// execution id, turning N serial RTTs into one concurrent batch.
	using InitFuture = std::future<std::pair<InitResult, std::unique_ptr<IFunctionConnection>>>;
	// Guarded by `init_apply_mutex`; only the thread that flips `init_applied`
	// consumes the future. `mutable` so the const MaxThreads() can drive
	// EnsureInitApplied().
	mutable InitFuture pending_init;
	mutable std::mutex init_apply_mutex;
	mutable std::atomic<bool> init_applied {false};

	// Block until the deferred init result is folded into this gstate.
	// Idempotent and thread-safe. Afterwards `global_execution_id`,
	// `max_processes` and `primary_connection` are populated.
	void EnsureInitApplied() const;

	mutable std::vector<uint8_t> global_execution_id;

	// The primary init's opaque data, forwarded to every secondary init as
	// `init_opaque_data`: the worker's secondary-init path skips `on_init` and
	// echoes these bytes, so without them any function that carries state
	// through init opaque data breaks once a scan runs more than one worker.
	mutable std::vector<uint8_t> init_opaque_data;

	// Captured at InitGlobal for the post-execution metrics RPC
	// (VgiTableFunctionGetMetrics). Valid until the query tears down, since the
	// gstate dies with the query.
	ClientContext *client_context_for_explain = nullptr;

	// `mutable` so EnsureInitApplied() can populate it from MaxThreads().
	mutable idx_t max_processes = 1;

	// Read concurrently by the progress callback.
	std::atomic<idx_t> rows_read {0};

	// Wire RecordBatch shape across all scan threads (``rows_read`` counts
	// DuckDB output chunks instead), surfaced by ``VgiTableFunctionGetMetrics``.
	// Must live on the gstate: DuckDB calls get_metrics once per source thread
	// (OperatorProfiler::FinishSource) into one metrics map and later keys
	// override earlier ones, so a per-thread counter would report only the last
	// thread. Relaxed atomics: pure counters, read after the pipeline quiesces.
	std::atomic<idx_t> batches_received {0};
	std::atomic<idx_t> batch_rows_total {0};
	std::atomic<idx_t> batch_bytes_total {0};
	std::atomic<idx_t> batch_rows_max {0};
	//! Starts at max so the first CAS wins; check ``batches_received`` before
	//! reading it.
	std::atomic<idx_t> batch_rows_min {NumericLimits<idx_t>::Maximum()};

	//! Fold one worker-emitted batch into the statistics above.
	void RecordBatchStats(idx_t rows, idx_t bytes) {
		batches_received.fetch_add(1, std::memory_order_relaxed);
		batch_rows_total.fetch_add(rows, std::memory_order_relaxed);
		batch_bytes_total.fetch_add(bytes, std::memory_order_relaxed);
		auto prev_max = batch_rows_max.load(std::memory_order_relaxed);
		while (rows > prev_max &&
		       !batch_rows_max.compare_exchange_weak(prev_max, rows, std::memory_order_relaxed)) {
		}
		auto prev_min = batch_rows_min.load(std::memory_order_relaxed);
		while (rows < prev_min &&
		       !batch_rows_min.compare_exchange_weak(prev_min, rows, std::memory_order_relaxed)) {
		}
	}

	// Synthetic batch index for PartitionColumns functions WITHOUT
	// supports_batch_index. PipelineExecutor::NextBatch refreshes the sink's
	// partition_data only when the source batch_index *changes*, so it must move
	// per batch. It MUST be global, not per-local-state: a late-registering
	// thread inherits a nonzero minimum from Pipeline::RegisterNewBatchIndex, and
	// a per-thread counter restarting at 0 can equal it, so NextBatch sees no
	// change and reads an empty partition_data. Per-batch granularity is harmless:
	// the sink buckets by partition value (GetOrCreatePartition).
	std::atomic<idx_t> synthetic_batch_index {0};

	// Primary connection, handed off under the mutex to the first InitLocal.
	// `mutable` so EnsureInitApplied() can install it from MaxThreads().
	mutable std::mutex connection_mutex;
	mutable std::unique_ptr<IFunctionConnection> primary_connection;

	vector<VgiDynamicFilterInfo> dynamic_filters;

	// Serialized once, reused on every tick.
	VgiBufferPtr static_filter_bytes;

	// Updated by the scan, read by the connection for tick metadata.
	shared_ptr<TickFilterState> tick_filter_state;

	// Init pushdown data captured by InitGlobal so secondary workers get the
	// same projection / filters / hints as the primary; otherwise they would emit
	// unprojected batches that the projected-layout conversion misreads.
	std::vector<int32_t> projection_ids;
	std::vector<VgiBufferPtr> join_keys_buffers;
	std::optional<OrderByHint> order_by_hint;
	std::optional<TableSampleHint> table_sample_hint;

	// Mirrored from ``VgiTableFunctionBindData::fixed_order``; clamps MaxThreads().
	bool fixed_order = false;

	idx_t MaxThreads() const override {
		// Called from Pipeline::ScheduleParallel BEFORE init_local. With async
		// init, max_processes is a provisional 1 until the future resolves, and
		// ScheduleParallel would then single-thread the scan — hence the wait.
		// Sibling pipelines' init RPCs were all started by
		// Executor::ScheduleEventsInternal's ResetSource(true) loop, so the
		// first wait absorbs the longest RTT (once per scheduling wave).
		//
		// Cache serve replays one flattened stream; there is no init future.
		if (serving_from_cache) {
			return 1;
		}
		// The not_modified swap to CachedReplayConnection must be race-free.
		if (revalidating) {
			return 1;
		}
		EnsureInitApplied();
		// The worker may still advertise max_workers > 1.
		if (fixed_order) {
			return 1;
		}
		// Split path: at most one reader per split and the worker cap. This must
		// stay AFTER the clamps above so it can only narrow. Zero splits is legal
		// (fully pruned), so floor at one reader, which finds no claims.
		if (supports_splits) {
			return MaxValue<idx_t>(1, MinValue<idx_t>(splits.size(), max_processes));
		}
		return max_processes;
	}
};

//! Async prefetch state of a VGI table-function scan.
enum class PrefetchState : uint8_t {
	IDLE,      //! No prefetch in flight, no prefetched data
	IN_FLIGHT, //! Prefetch task is running (scan returned BLOCKED)
	READY,     //! Prefetch completed successfully, batch available
	ERROR      //! Prefetch completed with an error
};

//! Connection + prefetch result, shared by the local state and each
//! VgiPrefetchTask. Shared because on cancellation ``Executor::CancelTasks``
//! destroys the local state BEFORE draining pending tasks; the task's
//! shared_ptr keeps the connection alive, and ``cancelled`` makes it skip I/O.
struct VgiPrefetchSlot {
	std::unique_ptr<IFunctionConnection> connection;
	// HTTP-only: latest stream-state token, so the cancel dispatcher addresses
	// the right server-side session.
	std::vector<uint8_t> last_state_token;
	std::atomic<PrefetchState> state {PrefetchState::IDLE};
	VgiBatchPtr batch;            // valid when state == READY
	std::exception_ptr exception; // valid when state == ERROR
	// Set by the local-state destructor: a task must issue no new RPCs.
	std::atomic<bool> cancelled {false};
};

//! Prefetches the next batch on a DuckDB worker thread while the scan is
//! BLOCKED; see VgiPrefetchSlot for the ownership rule.
class VgiPrefetchTask : public AsyncTask {
public:
	// The context outlives the task: it runs as an executor task, and the query's
	// executor waits for those before the query ends.
	VgiPrefetchTask(ClientContext &context, std::shared_ptr<VgiPrefetchSlot> slot)
	    : context_(context), slot_(std::move(slot)) {
	}
	void Execute() override;

private:
	ClientContext &context_;
	std::shared_ptr<VgiPrefetchSlot> slot_;
};

struct VgiTableFunctionLocalState : public ArrowScanLocalState {
	VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx,
	                           std::shared_ptr<VgiAttachParameters> attach_params);

	~VgiTableFunctionLocalState() noexcept;

	IFunctionConnection *connection() {
		return prefetch_slot_->connection.get();
	}
	const IFunctionConnection *connection() const {
		return prefetch_slot_->connection.get();
	}

	//! Re-acquire this reader's connection, bypassing the pool. Split path only:
	//! InitLocal acquires without initializing, so a pooled worker that died
	//! while idle surfaces only at the FIRST split's init, past the usual
	//! stale-pool retry. Cleared after the first claim: a later failure is real,
	//! and a fresh connection cannot resume splits already drained.
	std::function<std::unique_ptr<IFunctionConnection>(ClientContext &)> reacquire_fresh_connection;

	//! True while this reader holds a claimed, not-yet-drained split; only a
	//! finished split increments ``completed_splits``.
	bool has_split = false;

	//! Most recently claimed split (strictly ascending per reader).
	idx_t last_split_index = 0;

	bool done = false;
	bool first_scan_call_ = true;

	// batch_index of the most recent data batch, returned by
	// ``VgiGetPartitionData``. INVALID until the first data batch — safe because
	// ``PipelineExecutor::NextBatch`` calls ``GetPartitionData`` only for a
	// non-empty chunk or an ADVANCED fetch, which table scans never report.
	//
	// CRITICAL: written ONLY by ``InstallBatch`` on the pipeline thread, NEVER
	// in ``VgiPrefetchTask::Execute`` / ``ReadDataBatch`` (scheduler threads),
	// which would race ``VgiGetPartitionData``.
	idx_t current_batch_index = DConstants::INVALID_INDEX;

	// Per-partition-column (min, max) of the most recent data batch (from
	// ``vgi_partition_values#b64`` metadata), in
	// ``VgiTableFunctionBindData::partition_column_indices`` order, returned as
	// ``OperatorPartitionData::partition_data`` for partition-aware sinks. Same
	// thread-safety contract as ``current_batch_index``.
	duckdb::vector<ColumnPartitionData> current_partition_data;
	//! True when current_partition_data came from this split's protocol-3.1
	//! key_value rather than legacy per-data-batch metadata.
	bool partition_data_from_split = false;

	// Captured at InitLocal for the destructor, which has no ClientContext.
	bool cancel_enabled = true;

	std::shared_ptr<VgiPrefetchSlot> prefetch_slot_;

	// This local state's capture substream (null when not capturing), borrowed
	// from the gstate's capture ctx.
	CachedStream *capture_stream = nullptr;
	bool capture_checked_first_batch = false;
	// Set once this local state drained its RAM substream into the spill writer.
	bool capture_spilled = false;

	// For the destructor to find the cancel dispatcher (outlives this state).
	DatabaseInstance &db_;

private:
	ClientContext &context_;
	std::shared_ptr<VgiAttachParameters> attach_params_;
};

//! Bind with the worker (or from an inlined catalog bind_result) and populate
//! bind_data, return_types and names. bind_data must already carry
//! attach_params, function_name, arguments and attach_opaque_data.
void PerformVgiTableFunctionBind(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                 vector<LogicalType> &return_types, vector<Identifier> &names);

struct SerializedFilters {
	VgiBufferPtr filter_bytes;                    //! filter RecordBatch IPC (nullptr when no filters)
	std::vector<VgiBufferPtr> join_keys_buffers;  //! one single-column IPC batch per IN-filter value list
};

enum class VgiFilterColumnIndexDomain : uint8_t {
	//! DuckDB filters are keyed by projection positions, mapped through column_ids.
	PROJECTED,
	//! Synthetic filters explicitly keyed by the original bind-schema column index.
	BIND_SCHEMA,
};

//! Serialize a TableFilterSet for the worker. Throws InvalidInputException if a
//! required filter cannot be encoded exactly. The filter RecordBatch has:
//!   - Column 0: non-null filter_spec (string) containing a v2 snapshot document
//!   - Columns 1..N: canonical value_N/type_N typed payload fields
//! Encoding, version, and evaluation context are stored on the batch schema.
//!
//! An IN list over 4 KiB (up to vgi_join_keys_max_bytes, else an error) goes
//! out-of-line as a single-column batch in join_keys_buffers; smaller lists
//! are inline literals.
//!
//! ``rowid_column_name`` and ``rowid_worker_col_index`` identify the field that
//! DuckDB hides from ``column_names``. They restore worker-schema indexes for
//! rowid and for ordinary fields shifted by the erased field.
//! ``exclude_filter_keys``: projected filter-map entries to SKIP, used by the
//! per-partition result cache to serialize the RESIDUAL (non-partition) filter.
SerializedFilters VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                      optional_ptr<TableFilterSet> filters,
                                      const vector<string> &column_names, const string &worker_path,
                                      const string &rowid_column_name = "",
                                      int64_t rowid_worker_col_index = -1,
                                      const std::set<idx_t> *exclude_filter_keys = nullptr,
                                      VgiFilterColumnIndexDomain index_domain = VgiFilterColumnIndexDomain::PROJECTED,
                                      const std::vector<VgiFilterFunctionCapability> &additional_functions = {});

//! One revisioned runtime-filter mutation carried in a v2 delta batch.
//! A remove has a null filter; an upsert points at a filter that remains alive
//! for the duration of VgiSerializeDynamicFilterDelta.
struct VgiDynamicFilterDeltaUpdate {
	string predicate_id;
	uint64_t revision;
	idx_t column_index;
	string column_name;
	const TableFilter *filter;
};

//! Serialize the advisory Top-N mutations sent in tick metadata.
VgiBufferPtr VgiSerializeDynamicFilterDelta(ClientContext &context, const string &worker_path,
                                                              const vector<VgiDynamicFilterDeltaUpdate> &updates);

//! True if ``filter`` contains a runtime-valued internal filter function (Top-N
//! DynamicFilter, bloom filter, prefix range). Walks for *static* information
//! should then skip the whole OptionalFilter subtree: the value is unknown at
//! init, and a partial walk yields a stricter view than the filter constrains.
bool VgiContainsDynamicFilter(const TableFilter &filter);

//! Expression pushdown callback: checks if the expression tree only uses functions the worker supports
bool VgiPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr);

//! Expression pushdown callback for buffered table-in/out functions, whose
//! LogicalGet owns VgiTableInOutBindData rather than VgiTableFunctionBindData.
bool VgiTableInOutPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr);

//! Starts the primary connection's init RPC asynchronously (see EnsureInitApplied).
unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context, TableFunctionInitInput &input);

//! Claims the primary connection or creates a secondary one.
unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p);

void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output);

unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p);

double VgiTableFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
                                const GlobalTableFunctionState *global_state_p);

InsertionOrderPreservingMap<string> VgiTableFunctionToString(TableFunctionToStringInput &input);

//! get_metrics callback: post-execution diagnostics for EXPLAIN ANALYZE, called
//! once per scan thread. Adds intrinsic keys, then (best-effort) a unary RPC with
//! the global execution_id lets the worker's dynamic_to_string hook add its own;
//! an RPC failure is logged and leaves only the intrinsic keys.
void VgiTableFunctionGetMetrics(TableFunctionGetMetricsInput &input);

BindInfo VgiTableScanGetBindInfo(TableFunctionGetBindInfoInput &input);
optional_ptr<TableCatalogEntry> VgiTableScanGetTableEntry(optional_ptr<const FunctionData> bind_data_p);

virtual_column_map_t VgiTableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p);

vector<column_t> VgiTableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p);

//! Captures the ORDER BY + LIMIT hint from the RowGroupPruner optimizer.
void VgiSetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data_p);

//! get_partition_data callback: the batch_index AND per-column (min, max)
//! ``ColumnPartitionData`` of this local state's most recent data batch. Sinks
//! read whichever their ``RequiredPartitionInfo()`` asks for. Installed only
//! when the function opts in to either feature.
OperatorPartitionData VgiGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input);

//! get_partition_info callback: the declared ``TablePartitionInfo`` over
//! ``input.partition_ids``, or ``NOT_PARTITIONED`` if any requested column is
//! outside the declared partition set. Registered only for partitioned
//! functions. DuckDB's planner consumes only ``SINGLE_VALUE_PARTITIONS``
//! (``CanUsePartitionedAggregate``); other kinds fall back to HASH_GROUP_BY.
TablePartitionInfo VgiGetPartitionInfo(ClientContext &context, TableFunctionPartitionInput &input);

//! Column statistics: VgiTableEntry's first for catalog-table scans, else the
//! lazily cached table_function_statistics RPC.
unique_ptr<BaseStatistics> VgiTableFunctionStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                       column_t column_index);

} // namespace vgi
} // namespace duckdb
