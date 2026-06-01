// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <utility>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

class DatabaseInstance;

namespace vgi {

// ============================================================================
// VgiTableFunctionInfo - Stores metadata needed to invoke a VGI table function
// ============================================================================

//! Information stored with each VGI table function to enable invocation.
//! This is attached to the TableFunction via the function_info member and
//! accessed in the bind function via input.info->Cast<VgiTableFunctionInfo>().
class VgiTableFunctionInfo final : public TableFunctionInfo {
public:
	VgiTableFunctionInfo(Catalog &catalog, std::shared_ptr<VgiAttachParameters> attach_params,
	                     std::vector<uint8_t> attach_opaque_data, VgiFunctionInfo function_info,
	                     std::vector<std::string> setting_names)
	    : catalog_(catalog), attach_params_(std::move(attach_params)), attach_opaque_data_(std::move(attach_opaque_data)),
	      function_info_(std::move(function_info)), setting_names_(std::move(setting_names)) {
	}

	~VgiTableFunctionInfo() override = default;

	//! Catalog this function belongs to
	Catalog &catalog() const {
		return catalog_;
	}

	//! Attach parameters for this catalog
	const std::shared_ptr<VgiAttachParameters> &attach_params() const {
		return attach_params_;
	}

	//! Path to the VGI worker executable
	const std::string &worker_path() const {
		return attach_params_->worker_path();
	}

	//! Attach ID for the catalog connection
	const std::vector<uint8_t> &attach_opaque_data() const {
		return attach_opaque_data_;
	}

	//! Whether to enable worker debug output
	bool worker_debug() const {
		return attach_params_->worker_debug();
	}

	//! Whether pooling is enabled for this function's workers
	bool use_pool() const {
		return attach_params_->use_pool();
	}

	//! Full function metadata from the worker
	const VgiFunctionInfo &function_info() const {
		return function_info_;
	}

	//! Names of settings registered by this catalog
	const std::vector<std::string> &setting_names() const {
		return setting_names_;
	}

private:
	Catalog &catalog_;
	std::shared_ptr<VgiAttachParameters> attach_params_;
	std::vector<uint8_t> attach_opaque_data_;
	VgiFunctionInfo function_info_;
	std::vector<std::string> setting_names_;
};

// ============================================================================
// VgiTableFunctionBindData - Shared bind data for VGI table functions
// ============================================================================

//! Bind data for VGI table functions. Used by both the direct vgi_table_function()
//! and catalog-based VGI table functions.
struct VgiTableFunctionBindData : public TableFunctionData {
	// Worker identification
	std::shared_ptr<VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }

	// Function identification
	std::string function_name;

	// Arguments for creating worker connections
	ArrowArguments arguments;

	// Settings to pass to the worker (e.g., DuckDB pragmas)
	std::map<std::string, Value> settings;

	// Required secrets for this function (from function metadata)
	std::vector<VgiSecretRequirement> required_secrets;

	// Schema information (discovered from OutputSpec during bind)
	// Arrow C ABI schema wrapper for DuckDB conversion
	ArrowSchemaWrapper c_schema;
	// DuckDB's Arrow table schema for type conversion
	ArrowTableSchema arrow_table;

	// Execution hints (defaults at bind, updated after init)
	int32_t max_processes = 1;
	// True iff the worker declared this function as FIXED_ORDER. DuckDB's
	// ``Pipeline::IsOrderDependent()`` controls *operator caching* but does
	// NOT force a single-threaded source — ``PhysicalTableScan::ParallelSource()``
	// unconditionally returns true, so without this flag a FIXED_ORDER scan
	// would still fan out to ``max_processes`` workers (and produce
	// non-deterministic emit order despite the "fixed" promise). The flag
	// is mirrored onto ``VgiTableFunctionGlobalState::fixed_order`` at
	// init-global time; ``MaxThreads()`` clamps to 1 when set.
	bool fixed_order = false;

	// True iff the worker declared ``Meta.supports_batch_index = True``.
	// When set, the function registration at vgi_table_function_set.cpp
	// installs ``VgiGetPartitionData`` AND skips the ``fixed_order ->
	// MaxThreads=1`` clamp above (the source stays parallel and the sink
	// reassembles via batch_index). Each emitted Arrow data batch MUST
	// carry ``vgi_batch_index`` in KeyValueMetadata; ``InstallBatch``
	// parses it on the consumer thread and stashes it on
	// ``VgiTableFunctionLocalState::current_batch_index`` for
	// ``VgiGetPartitionData`` to return. Per-stream monotonicity is
	// enforced in ``InstallBatch`` (DuckDB's release-build checks are
	// global-uniqueness-only).
	bool supports_batch_index = false;

	// Partition shape declared by the worker over its annotated bind
	// schema fields (Meta.partition_kind on the Python side). When non-
	// ``NotPartitioned``, vgi_table_function_set.cpp installs
	// ``table_func.get_partition_info`` returning the matching
	// ``TablePartitionInfo`` value so the planner can pick
	// PhysicalPartitionedAggregate for matching GROUP BY queries.
	VgiPartitionKind partition_kind = VgiPartitionKind::NotPartitioned;

	// Base column indices into the bind output schema for fields that
	// carry the ``vgi.partition_column == "true"`` metadata marker.
	// Resolved ONCE at bind by walking ``bind_result.output_schema``;
	// stored here so ``VgiGetPartitionInfo`` does an O(P) membership
	// check rather than re-walking the schema per planner call.
	// Empty when ``partition_kind == NotPartitioned``; non-empty
	// otherwise (registration-time check enforces this invariant via
	// BinderException).
	std::vector<idx_t> partition_column_indices;

	mutable int64_t cardinality_estimate = -1;
	// Optional max-cardinality, surfaced into DuckDB's NodeStatistics.
	// -1 = unknown. Populated either from the inlined ``TableInfo``
	// (catalog table path) or from ``table_function_cardinality`` (RPC path).
	mutable int64_t cardinality_max = -1;

	// Whether this function supports projection pushdown (from FunctionInfo)
	bool projection_pushdown = false;

	// Expression filter function names the worker supports (e.g., ["&&", "st_intersects_extent"])
	std::vector<std::string> supported_expression_filters;

	vector<string> all_column_names;
	// Parallel to all_column_names — populated during bind for the stats callback so it
	// can ask InvokeTableFunctionStatistics for typed per-column stats without re-walking
	// the Arrow schema.
	vector<LogicalType> all_column_types;

	// Table entry reference (for get_bind_info callback; null for direct vgi_table_function)
	optional_ptr<TableCatalogEntry> table_entry;

	// Row ID column: index in worker's output schema marked is_row_id (-1 = none)
	int rowid_worker_col_index = -1;
	// DuckDB type for the row_id column (INVALID when rowid_worker_col_index == -1)
	LogicalType rowid_type = LogicalType::INVALID;
	// Worker-schema field name of the rowid column (e.g. "row_id"). Captured at
	// bind BEFORE the rowid is erased from all_column_names, so it survives for
	// filter serialization — a filter on COLUMN_IDENTIFIER_ROW_ID must be named
	// with this so the worker matches it (late-materialization rowid pushdown).
	// Empty when rowid_worker_col_index < 0.
	std::string rowid_column_name;

	// Bind output retained for init phase and lazy cardinality / statistics
	// RPCs. The init payload (BuildInitRequest) carries bind_request_bytes,
	// output_schema_bytes, and opaque_data inline, so InitGlobal /
	// init_local_secondary never need a second on-wire bind.
	BindResult bind_result;

	// Lazy cardinality fetching flag (mutable for const callback access)
	mutable bool cardinality_fetched = false;

	// Lazy per-column stats cache for the direct table-function path. The catalog-table
	// path delegates to VgiTableEntry::GetStatistics instead and never touches these.
	// `statistics_mutex` guards both `statistics_fetched` and `statistics_cache`, since
	// DuckDB may call the stats callback from multiple optimizer threads concurrently.
	mutable std::mutex statistics_mutex;
	mutable bool statistics_fetched = false;
	mutable std::unordered_map<std::string, unique_ptr<BaseStatistics>> statistics_cache;

	// Order pushdown hint from DuckDB optimizer (set by set_scan_order callback).
	// Mutable because set_scan_order is called during optimization (after bind, before execution).
	mutable std::optional<OrderByHint> order_by_hint;

	// TABLESAMPLE SYSTEM hint from DuckDB optimizer (read from input.sample_options in InitGlobal).
	// Mutable because InitGlobal receives const bind_data.
	mutable std::optional<TableSampleHint> table_sample_hint;

	// AT (...) time-travel clause this scan was bound under (empty for the
	// common no-AT case). Captured on the catalog-scan path so the
	// vgi_table_scan (de)serialize callbacks can rebuild an identical bind
	// after a logical-plan deep copy (e.g. the WindowSelfJoin optimizer
	// duplicating a `COUNT(*) OVER (PARTITION BY ...)` scan). Only the
	// catalog-scan path sets these; the direct vgi_table_function() path
	// leaves them empty.
	std::string at_unit;
	std::string at_value;

	//! Deep-copy for DuckDB's late-materialization optimizer, which clones the
	//! LogicalGet (and hence this bind data) via CreateLHSGet to build the
	//! narrow ordering-scan LHS. Shares the immutable Arrow conversion state
	//! (ArrowType is self-contained, shared via shared_ptr) and rebuilds the
	//! C-ABI schema export from bind_result.output_schema (context-free). The
	//! per-bind statistics cache + its mutex are intentionally NOT copied — the
	//! clone re-fetches lazily.
	unique_ptr<FunctionData> Copy() const override;
};

// ============================================================================
// VgiTableFunctionGlobalState - Shared global state for progress tracking
// ============================================================================

// Info about a captured DynamicFilter for tick-based pushdown
struct VgiDynamicFilterInfo {
	//! The shared dynamic filter data (holds the mutable ConstantFilter value)
	shared_ptr<DynamicFilterData> filter_data;
	//! Column index in the projected column list
	idx_t column_index;
	//! Column name for serialization
	string column_name;
	//! The comparison type (from the ConstantFilter inside DynamicFilterData)
	ExpressionType comparison_type;
	//! Whether this filter is wrapped in ConjunctionOr with IsNull (NULLS_FIRST)
	bool nulls_first = false;
};

struct VgiTableFunctionGlobalState : public GlobalTableFunctionState {
	// --- Async init plumbing ----------------------------------------------
	// DuckDB schedules independent pipelines' `init_global` serially on the
	// main thread (Executor::ScheduleEventsInternal), so a synchronous HTTP
	// init RPC blocks every other pipeline's init too. We move the RPC onto
	// a background thread, return this gstate immediately, and wait on the
	// future from the worker-thread init_local path before any consumer
	// touches the connection or execution id. For a fan-out of N independent
	// metadata reads (typical of a Ducklake bind-time scan plan) this
	// collapses N × RTT serial latency into a single concurrent batch.
	using InitFuture = std::future<std::pair<InitResult, std::unique_ptr<IFunctionConnection>>>;
	// Guarded by `init_apply_mutex`; a single thread (the one that flips
	// `init_applied` from false to true) calls `.get()` and consumes the
	// future — every other waiter sees `init_applied == true` and skips.
	// `mutable` so MaxThreads() (declared `const` by DuckDB) can drive
	// EnsureInitApplied() — lazy init of fields populated by the async
	// init RPC.
	mutable InitFuture pending_init;
	mutable std::mutex init_apply_mutex;
	mutable std::atomic<bool> init_applied {false};

	// Block until the deferred init RPC has completed and its result has
	// been folded into this gstate. Idempotent + thread-safe; safe to call
	// from any number of init_local / scan threads. After the first call
	// returns, `global_execution_id`, `max_processes`, and
	// `primary_connection` are populated and ready to use.
	void EnsureInitApplied() const;

	// Global execution identifier for multi-worker coordination
	mutable std::vector<uint8_t> global_execution_id;

	// Opaque data returned by the worker's primary init RPC
	// (`GlobalInitResponse.opaque_data`). Forwarded to every secondary init
	// as `init_opaque_data` so the worker's secondary-init branch — which
	// echoes `init_opaque_data` straight into the response and skips
	// `on_init` — sees the same bytes the primary's `on_init` produced.
	// Without this, secondaries always send `None` and any function that
	// round-trips state through init opaque data (e.g. `tx_cached_value`
	// shipping a cached value bind→init→process) breaks the moment a
	// parallel scan launches more than one worker.
	mutable std::vector<uint8_t> init_opaque_data;

	// Captured at InitGlobal so the post-execution dynamic_to_string callback
	// can issue an RPC. The DuckDB callback signature does not pass a
	// ClientContext, but the gstate is owned by the same pipeline that owns
	// the ClientContext, so this stays valid until the query tears down.
	ClientContext *client_context_for_explain = nullptr;

	// Maximum number of worker processes (from OutputSpec).
	// `mutable` so EnsureInitApplied() can populate it from MaxThreads().
	mutable idx_t max_processes = 1;

	// Progress tracking (atomic for thread safety with progress callback)
	std::atomic<idx_t> rows_read {0};

	// Synthetic batch-index source for PartitionColumns-mode functions that do
	// NOT advertise supports_batch_index. DuckDB's PipelineExecutor::NextBatch
	// refreshes the sink's partition_data only when the source's batch_index
	// *changes*, so these functions need a value that moves per batch to drive
	// PartitionedAggregate. It MUST be globally monotonic across scan threads,
	// not per-local-state: DuckDB initializes each thread's sink batch_index
	// from the global batch-index pool (Pipeline::RegisterNewBatchIndex returns
	// the current minimum), so a late-registering thread starts at >0. A
	// per-thread counter that restarts at 0 then collides with that initialized
	// value (synthetic 0 -> base+0+1 == the global-min the thread inherited),
	// NextBatch sees "no change", and the never-installed partition_data is
	// dereferenced empty -> "index 0 within vector of size 0". A single global
	// counter guarantees every batch's index strictly exceeds any thread's
	// inherited minimum, so the first chunk always refreshes. Same-valued
	// partitions still merge correctly downstream — the sink buckets by
	// partition *value* (GetOrCreatePartition), so per-batch granularity is
	// harmless. fetch_add(relaxed) is enough; we only need uniqueness +
	// monotonicity, not ordering against other memory.
	std::atomic<idx_t> synthetic_batch_index {0};

	// Primary connection (moved from bind_data during InitGlobal)
	// Protected by mutex for thread-safe handoff to first InitLocal caller.
	// `mutable` so EnsureInitApplied() can install it from MaxThreads().
	mutable std::mutex connection_mutex;
	mutable std::unique_ptr<IFunctionConnection> primary_connection;

	// Dynamic filter info captured at init time (for tick-based pushdown)
	vector<VgiDynamicFilterInfo> dynamic_filters;

	// Cached static filter bytes (serialized once, reused on every tick)
	std::shared_ptr<arrow::Buffer> static_filter_bytes;

	// Shared tick filter state (updated by scan, read by connection for tick metadata)
	shared_ptr<TickFilterState> tick_filter_state;

	// Init-phase pushdown data captured by InitGlobal so secondary workers
	// see the same projection / filters / hints as the primary worker. Without
	// these, secondary workers emit the full unprojected schema while the
	// primary emits projected batches, and the per-batch ArrowToDuckDB read
	// (which assumes projected layout when projection_pushdown=true) reads
	// the wrong column position.
	std::vector<int32_t> projection_ids;
	std::vector<std::shared_ptr<arrow::Buffer>> join_keys_buffers;
	std::optional<OrderByHint> order_by_hint;
	std::optional<TableSampleHint> table_sample_hint;

	// Mirrored from ``VgiTableFunctionBindData::fixed_order`` at init-global
	// time. When set, ``MaxThreads()`` clamps to 1 so DuckDB's pipeline
	// scheduler runs the source on a single thread. See the bind-data
	// comment for why ``Pipeline::IsOrderDependent()`` alone is insufficient.
	bool fixed_order = false;

	idx_t MaxThreads() const override {
		// Called from Pipeline::ScheduleParallel on the main scheduling
		// thread, BEFORE init_local. With async init_global, max_processes
		// is the provisional 1 until the future resolves — without this
		// wait, ScheduleParallel sees max_threads<=1 and falls back to a
		// sequential task, silently single-threading parallel scans on
		// workers that advertise max_workers > 1.
		//
		// Wall cost: bounded by max(RTT) across the in-flight init batch.
		// Every sibling pipeline's init RPC was kicked off in
		// Executor::SchedulePipeline's eager ResetSource(true) loop before
		// any Schedule() runs, so the first MaxThreads() wait absorbs the
		// longest RPC and subsequent ones see resolved futures.
		//
		// This holds within ONE ScheduleEventsInternal invocation. Queries
		// scheduled in waves (CTE materialization, join build vs. probe)
		// pay one wait per wave. Most queries are one wave.
		EnsureInitApplied();
		// FIXED_ORDER functions must serialize the source — see field
		// comment above. The worker may still advertise max_workers > 1
		// (it's allowed to *support* multi-worker; the planner is the one
		// promising ordered emission to downstream operators).
		if (fixed_order) {
			return 1;
		}
		return max_processes;
	}
};

// ============================================================================
// Async Prefetch State
// ============================================================================

//! State machine for async I/O prefetch of VGI table function batches.
enum class PrefetchState : uint8_t {
	IDLE,      //! No prefetch in flight, no prefetched data
	IN_FLIGHT, //! Prefetch task is running (scan returned BLOCKED)
	READY,     //! Prefetch completed successfully, batch available
	ERROR      //! Prefetch completed with an error
};

// ============================================================================
// VgiPrefetchSlot - Shared ownership of connection + prefetch state
// ============================================================================

//! Holds the pieces that a VgiPrefetchTask needs (connection + prefetch
//! result slots) as an independently-owned shared object. The local state
//! holds a shared_ptr to the slot; each scheduled VgiPrefetchTask takes its
//! own shared_ptr copy.
//!
//! Why shared ownership: under query cancellation DuckDB's
//! ``Executor::CancelTasks`` clears pipelines (destroying the
//! ``VgiTableFunctionLocalState``) BEFORE draining the scheduler's pending
//! task queue. If the task held a raw reference to the local state, its
//! ``connection->ReadDataBatch()`` call would dereference freed memory.
//! With the connection living inside this slot, the task's shared_ptr keeps
//! it alive until the task finishes; the ``cancelled`` flag lets the task
//! short-circuit instead of doing real I/O when the local state is gone.
struct VgiPrefetchSlot {
	// Connection to the VGI worker — owned here so slot lifetime governs it.
	std::unique_ptr<IFunctionConnection> connection;
	// Most recent stream-state token seen on HTTP exchanges. Used by
	// the cancel dispatcher to address the correct server-side session
	// when max_workers > 1. Empty on subprocess transport.
	std::vector<uint8_t> last_state_token;
	// Prefetch state machine (see ``PrefetchState`` above).
	std::atomic<PrefetchState> state {PrefetchState::IDLE};
	// The batch produced by the prefetch (valid when state == READY).
	std::shared_ptr<arrow::RecordBatch> batch;
	// Any exception thrown by the prefetch (valid when state == ERROR).
	std::exception_ptr exception;
	// Set by the local-state destructor. A task that sees this flag must not
	// issue new RPCs on ``connection``; the data is no longer needed.
	std::atomic<bool> cancelled {false};
};

// ============================================================================
// VgiPrefetchTask - AsyncTask that prefetches the next batch from a VGI worker
// ============================================================================

//! Runs on a DuckDB worker thread while the scan is BLOCKED.
//! Holds a shared_ptr to ``VgiPrefetchSlot`` so it can safely complete (or
//! bail out) even if the owning ``VgiTableFunctionLocalState`` was destroyed
//! by query cancellation before the scheduler got to us.
class VgiPrefetchTask : public AsyncTask {
public:
	explicit VgiPrefetchTask(std::shared_ptr<VgiPrefetchSlot> slot) : slot_(std::move(slot)) {
	}
	void Execute() override;

private:
	std::shared_ptr<VgiPrefetchSlot> slot_;
};

// ============================================================================
// VgiTableFunctionLocalState - Extends DuckDB's ArrowScanLocalState
// ============================================================================

struct VgiTableFunctionLocalState : public ArrowScanLocalState {
	VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx,
	                           std::shared_ptr<VgiAttachParameters> attach_params);

	~VgiTableFunctionLocalState() noexcept;

	//! Shortcut accessor: the worker connection lives inside the slot.
	IFunctionConnection *connection() {
		return prefetch_slot_->connection.get();
	}
	const IFunctionConnection *connection() const {
		return prefetch_slot_->connection.get();
	}

	// Completion tracking
	bool done = false;
	bool first_scan_call_ = true;

	// batch_index emitted by the most recent data batch from the worker
	// (read out of the Arrow record-batch KeyValueMetadata in InstallBatch,
	// see vgi_table_function_impl.cpp). Threaded into ``VgiGetPartitionData``
	// so DuckDB's ordered sinks can reassemble parallel output. INVALID
	// until the first data batch arrives — safe because ``GetPartitionData``
	// is only called when ``source_chunk.size() > 0`` (see
	// duckdb/src/parallel/pipeline_executor.cpp:130-149), which means a
	// data batch already landed via ``InstallBatch``.
	//
	// CRITICAL: this field is written ONLY by ``InstallBatch`` on the
	// consumer (pipeline-executor) thread. NEVER written inside
	// ``VgiPrefetchTask::Execute`` / ``ReadDataBatch`` — those run on
	// scheduler worker threads and a write there would race with
	// ``VgiGetPartitionData`` on the pipeline thread. Per-batch
	// monotonicity is also checked in ``InstallBatch`` (DuckDB's
	// ``BatchedDataCollection::Append`` assertion is debug-only).
	idx_t current_batch_index = DConstants::INVALID_INDEX;

	// Per-partition-column (min, max) ``duckdb::Value`` pairs decoded
	// from the most recent data batch's ``vgi_partition_values#b64``
	// metadata, in the order declared by
	// ``VgiTableFunctionBindData::partition_column_indices``.
	// Threaded into ``VgiGetPartitionData``'s
	// ``OperatorPartitionData::partition_data`` so partition-aware
	// sinks (today only ``PhysicalPartitionedAggregate``) can route
	// chunks by min/max value.
	//
	// Same thread-safety contract as ``current_batch_index``: written
	// ONLY by ``InstallBatch`` on the consumer thread. Empty until the
	// first data batch decodes; cleared at scan setup. Used only when
	// ``bind_data.partition_kind != NotPartitioned``.
	// (Uses ``duckdb::vector`` to match
	// ``OperatorPartitionData::partition_data``'s type alias.)
	duckdb::vector<ColumnPartitionData> current_partition_data;

	// Captured at init-local time; read by the destructor (which has no
	// ClientContext available). Setting changes mid-query do not
	// affect in-flight streams.
	bool cancel_enabled = true;

	// Shared prefetch state (see VgiPrefetchSlot docstring).
	std::shared_ptr<VgiPrefetchSlot> prefetch_slot_;

	// Accessed by the destructor to locate the per-DatabaseInstance
	// cancel dispatcher. Captured at construction (the DatabaseInstance
	// outlives the local state).
	DatabaseInstance &db_;

private:
	ClientContext &context_;
	std::shared_ptr<VgiAttachParameters> attach_params_;
};

// ============================================================================
// Shared Table Function Implementation
// ============================================================================

//! Perform the bind handshake with the worker and populate bind_data.
//! The bind_data must have worker_path, function_name, arguments, and attach_opaque_data already set.
//! This function creates a temporary connection, performs bind, and stores the results.
//! Returns the output schema from the worker.
void PerformVgiTableFunctionBind(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                 vector<LogicalType> &return_types, vector<string> &names);

//! Serialize filters to Arrow IPC bytes for worker.
//! Returns nullptr if filters is empty/null.
//! Throws InvalidInputException if filters contain unsupported types (e.g., DynamicFilter, BloomFilter).
//! Result of filter serialization — contains the filter batch and optional join keys batch.
struct SerializedFilters {
	std::shared_ptr<arrow::Buffer> filter_bytes;                    //! Arrow IPC bytes of the filter RecordBatch (or nullptr)
	std::vector<std::shared_ptr<arrow::Buffer>> join_keys_buffers;  //! Arrow IPC bytes per join key column (one single-column batch each)
};

//! Serialize a TableFilterSet into Arrow IPC bytes for the VGI worker.
//! The filter RecordBatch has:
//!   - Column 0: filter_spec (string) - JSON-encoded filter structure
//!   - Columns 1..N: Values referenced by filters, with exact Arrow types
//! Version is stored in Arrow schema metadata on filter_spec field: {"vgi_filter_version": "1"}
//!
//! If InFilter values are present and within size limits, each IN filter's values are
//! serialized as a separate single-column Arrow IPC RecordBatch in join_keys_buffers.
//!
//! ``rowid_column_name`` (default empty = none) is the worker-schema field name
//! of the table's rowid column. A filter on the rowid virtual column arrives with
//! ``column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID`` (UINT64_MAX); we name it
//! with this so the worker — which matches pushed/join-key columns by name — can
//! apply it (e.g. the rowid IN-list / min-max range pushed by DuckDB's
//! late-materialization semi-join). It is passed explicitly (not looked up in
//! ``column_names``) because ``column_names`` has the rowid column erased.
SerializedFilters VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                      optional_ptr<TableFilterSet> filters,
                                      const vector<string> &column_names, const string &worker_path,
                                      const string &rowid_column_name = "");

//! Returns true if any descendant of ``filter`` is a DynamicFilter (Top-N
//! tick-time bound). Consumers that walk TableFilter trees for *static*
//! information (presence-of-filter checks, serialization of constants, etc.)
//! should skip the entire OptionalFilter subtree when this returns true:
//! the DynamicFilter has no value at init time, and any partial walk yields
//! a stricter view than the OptionalFilter actually constrains.
bool VgiContainsDynamicFilter(const TableFilter &filter);

//! Expression pushdown callback: checks if the expression tree only uses functions the worker supports
bool VgiPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr);

//! Init global function - creates primary connection and performs init handshake
unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context, TableFunctionInitInput &input);

//! Init local function - creates local state, claims primary connection or creates secondary
unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p);

//! Scan function - reads data from worker and converts to DuckDB format
void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output);

//! Cardinality function - returns row count estimate from bind
unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p);

//! Progress function - returns scan progress as percentage
double VgiTableFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
                                const GlobalTableFunctionState *global_state_p);

//! ToString function - returns info for EXPLAIN output
InsertionOrderPreservingMap<string> VgiTableFunctionToString(TableFunctionToStringInput &input);

//! DynamicToString function - returns post-execution diagnostics for EXPLAIN ANALYZE Extra Info.
//! Fired once per parallel scan thread at end-of-stream. Issues a unary RPC to the worker pool
//! with the global execution_id; the worker hands it to the user's dynamic_to_string hook so
//! the function can return diagnostics it persisted during process(). Always also surfaces
//! intrinsic keys (Worker, Function, Rows Read, Threads). Best-effort: any RPC failure is
//! logged and degrades to just the intrinsic keys.
InsertionOrderPreservingMap<string> VgiTableFunctionDynamicToString(TableFunctionDynamicToStringInput &input);

//! Get bind info callback for returning table entry reference
BindInfo VgiTableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p);

//! Virtual column callback for row_id support on scan functions
virtual_column_map_t VgiTableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p);

//! Row ID column callback for row_id support on scan functions
vector<column_t> VgiTableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p);

//! set_scan_order callback - captures ORDER BY + LIMIT hint from RowGroupPruner optimizer
void VgiSetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data_p);

//! get_partition_data callback — returns the batch_index AND the
//! per-column ``(min, max)`` ``ColumnPartitionData`` of the most recent
//! data batch on this local source state. Both halves coexist; sinks
//! consume whichever their ``RequiredPartitionInfo()`` requests:
//!
//!   * ``BatchIndex()`` sinks (BatchCollector, BatchInsert,
//!     BatchCopyToFile, Limit) read ``batch_index``, ignore
//!     ``partition_data``.
//!   * ``PartitionColumns()`` sinks (PhysicalPartitionedAggregate)
//!     read ``partition_data``, ignore ``batch_index``.
//!
//! Installed conditionally in vgi_table_function_set.cpp when the
//! function opts in to either feature.
OperatorPartitionData VgiGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input);

//! get_partition_info callback — reports the function's declared
//! ``TablePartitionInfo`` over ``input.partition_ids`` (the column
//! indices the planner is asking about). Returns ``NOT_PARTITIONED``
//! when any requested column is NOT in the function's declared
//! partition set; otherwise returns the mapped ``VgiPartitionKind``.
//! Registered ONLY for functions whose worker declared
//! ``Meta.partition_kind != NOT_PARTITIONED`` (see
//! vgi_table_function_set.cpp). Today DuckDB's planner consumes only
//! ``SINGLE_VALUE_PARTITIONS`` (at plan_aggregate.cpp:109); the other
//! kinds are reported faithfully but fall back to ``HASH_GROUP_BY``.
TablePartitionInfo VgiGetPartitionInfo(ClientContext &context, TableFunctionPartitionInput &input);

//! Statistics callback - returns column statistics from VgiTableEntry (for catalog scans)
//! or nullptr (for direct vgi_table_function calls)
unique_ptr<BaseStatistics> VgiTableFunctionStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                       column_t column_index);

} // namespace vgi
} // namespace duckdb
