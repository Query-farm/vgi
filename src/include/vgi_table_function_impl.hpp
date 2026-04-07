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
namespace vgi {

// ============================================================================
// VgiTableFunctionInfo - Stores metadata needed to invoke a VGI table function
// ============================================================================

//! Information stored with each VGI table function to enable invocation.
//! This is attached to the TableFunction via the function_info member and
//! accessed in the bind function via input.info->Cast<VgiTableFunctionInfo>().
class VgiTableFunctionInfo final : public TableFunctionInfo {
public:
	VgiTableFunctionInfo(Catalog &catalog, std::string worker_path, std::vector<uint8_t> attach_id,
	                     bool worker_debug, bool use_pool, VgiFunctionInfo function_info,
	                     std::vector<std::string> setting_names)
	    : catalog_(catalog), worker_path_(std::move(worker_path)), attach_id_(std::move(attach_id)),
	      worker_debug_(worker_debug), use_pool_(use_pool), function_info_(std::move(function_info)),
	      setting_names_(std::move(setting_names)) {
	}

	~VgiTableFunctionInfo() override = default;

	//! Catalog this function belongs to
	Catalog &catalog() const {
		return catalog_;
	}

	//! Path to the VGI worker executable
	const std::string &worker_path() const {
		return worker_path_;
	}

	//! Attach ID for the catalog connection
	const std::vector<uint8_t> &attach_id() const {
		return attach_id_;
	}

	//! Whether to enable worker debug output
	bool worker_debug() const {
		return worker_debug_;
	}

	//! Whether pooling is enabled for this function's workers
	bool use_pool() const {
		return use_pool_;
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
	std::string worker_path_;
	std::vector<uint8_t> attach_id_;
	bool worker_debug_;
	bool use_pool_;
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
	std::string worker_path;
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> transaction_id;
	bool worker_debug = false;
	bool use_pool = false;

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

	// Connection from bind phase, persisted for reuse in InitGlobal.
	// Mutable to allow InitGlobal to move it out (bind_data is const after bind).
	mutable std::unique_ptr<IFunctionConnection> bind_connection;

	// Order pushdown hint from DuckDB optimizer (set by set_scan_order callback).
	// Mutable because set_scan_order is called during optimization (after bind, before execution).
	mutable std::optional<OrderByHint> order_by_hint;
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

// Forward declaration
struct VgiTableFunctionLocalState;

// ============================================================================
// VgiPrefetchTask - AsyncTask that prefetches the next batch from a VGI worker
// ============================================================================

//! Runs on a DuckDB worker thread while the scan is BLOCKED.
//! Safe to access local_state_ because DuckDB guarantees the scan won't be
//! called concurrently while the task is in flight.
class VgiPrefetchTask : public AsyncTask {
public:
	explicit VgiPrefetchTask(VgiTableFunctionLocalState &local_state) : local_state_(local_state) {
	}
	void Execute() override;

private:
	VgiTableFunctionLocalState &local_state_;
};

// ============================================================================
// VgiTableFunctionLocalState - Extends DuckDB's ArrowScanLocalState
// ============================================================================

struct VgiTableFunctionLocalState : public ArrowScanLocalState {
	VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx, bool use_pool,
	                           const std::string &worker_path)
	    : ArrowScanLocalState(std::move(current_chunk), ctx), context_(ctx), use_pool_(use_pool),
	      worker_path_(worker_path) {
	}

	~VgiTableFunctionLocalState() {
		D_ASSERT(prefetch_state_.load() != PrefetchState::IN_FLIGHT);
		// Return connection to pool if applicable
		if (use_pool_ && connection && connection->CanBePooled()) {
			auto worker_pid = connection->GetPid();
			auto pooled = connection->ReleaseForPooling();
			if (pooled) {
				VgiWorkerPool::Instance().Release(std::move(pooled));
				VGI_LOG(context_, "worker_pool.release",
				        {{"worker_path", worker_path_},
				         {"worker_pid", std::to_string(worker_pid)},
				         {"use_pool", "true"}});
			}
		}
	}

	// Connection to worker (owns the subprocess or HTTP state)
	std::unique_ptr<IFunctionConnection> connection;

	// Completion tracking
	bool done = false;

	// --- Async prefetch state ---
	std::atomic<PrefetchState> prefetch_state_ {PrefetchState::IDLE};
	std::shared_ptr<arrow::RecordBatch> prefetch_batch_;
	std::exception_ptr prefetch_exception_;
	bool first_scan_call_ = true;

private:
	ClientContext &context_;
	bool use_pool_;
	std::string worker_path_;
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
