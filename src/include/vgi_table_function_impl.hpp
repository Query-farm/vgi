#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <optional>

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
	                     std::vector<uint8_t> attach_id, VgiFunctionInfo function_info,
	                     std::vector<std::string> setting_names)
	    : catalog_(catalog), attach_params_(std::move(attach_params)), attach_id_(std::move(attach_id)),
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
	const std::vector<uint8_t> &attach_id() const {
		return attach_id_;
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
	std::vector<uint8_t> attach_id_;
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
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> transaction_id;

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
	mutable int64_t cardinality_estimate = -1;

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

	// Cached bind request for lazy cardinality RPC (populated during bind)
	std::vector<uint8_t> bind_request_bytes;
	std::vector<uint8_t> bind_opaque_data;

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
	// Global execution identifier for multi-worker coordination
	std::vector<uint8_t> global_execution_id;

	// Captured at InitGlobal so the post-execution dynamic_to_string callback
	// can issue an RPC. The DuckDB callback signature does not pass a
	// ClientContext, but the gstate is owned by the same pipeline that owns
	// the ClientContext, so this stays valid until the query tears down.
	ClientContext *client_context_for_explain = nullptr;

	// Maximum number of worker processes (from OutputSpec)
	idx_t max_processes = 1;

	// Progress tracking (atomic for thread safety with progress callback)
	std::atomic<idx_t> rows_read {0};

	// Primary connection (moved from bind_data during InitGlobal)
	// Protected by mutex for thread-safe handoff to first InitLocal caller.
	std::mutex connection_mutex;
	std::unique_ptr<IFunctionConnection> primary_connection;

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

	idx_t MaxThreads() const override {
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
//! The bind_data must have worker_path, function_name, arguments, and attach_id already set.
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
SerializedFilters VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                      optional_ptr<TableFilterSet> filters,
                                      const vector<string> &column_names, const string &worker_path);

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

//! Statistics callback - returns column statistics from VgiTableEntry (for catalog scans)
//! or nullptr (for direct vgi_table_function calls)
unique_ptr<BaseStatistics> VgiTableFunctionStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                       column_t column_index);

} // namespace vgi
} // namespace duckdb
