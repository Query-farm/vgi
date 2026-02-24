#pragma once

#include <atomic>
#include <map>
#include <mutex>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// VgiTableFunctionBindData - Shared bind data for VGI table functions
// ============================================================================

//! Bind data for VGI table functions. Used by both the direct vgi_table_function()
//! and catalog-based VGI table functions.
struct VgiTableFunctionBindData : public TableFunctionData {
	// Worker identification
	std::string worker_path;
	std::vector<uint8_t> attach_id;
	bool worker_debug = false;
	size_t max_pool_size = 0; // 0 = pool disabled

	// Function identification
	std::string function_name;

	// Arguments for creating worker connections
	ArrowArguments arguments;

	// Settings to pass to the worker (e.g., DuckDB pragmas)
	std::map<std::string, std::string> settings;

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

	vector<string> all_column_names;

	// Cached bind request for lazy cardinality RPC (populated during bind)
	std::vector<uint8_t> bind_request_bytes;
	std::vector<uint8_t> bind_opaque_data;

	// Lazy cardinality fetching flag (mutable for const callback access)
	mutable bool cardinality_fetched = false;

	// Connection from bind phase, persisted for reuse in InitGlobal.
	// Mutable to allow InitGlobal to move it out (bind_data is const after bind).
	mutable std::unique_ptr<FunctionConnection> bind_connection;
};

// ============================================================================
// VgiTableFunctionGlobalState - Shared global state for progress tracking
// ============================================================================

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
	std::unique_ptr<FunctionConnection> primary_connection;

	idx_t MaxThreads() const override {
		return max_processes;
	}
};

// ============================================================================
// VgiTableFunctionLocalState - Extends DuckDB's ArrowScanLocalState
// ============================================================================

struct VgiTableFunctionLocalState : public ArrowScanLocalState {
	VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx, size_t max_pool_size,
	                           const std::string &worker_path)
	    : ArrowScanLocalState(std::move(current_chunk), ctx), context_(ctx), max_pool_size_(max_pool_size),
	      worker_path_(worker_path) {
	}

	~VgiTableFunctionLocalState() {
		// Return connection to pool if applicable
		if (max_pool_size_ > 0 && connection && connection->CanBePooled()) {
			auto worker_pid = connection->GetPid();
			auto pooled = connection->ReleaseForPooling();
			if (pooled) {
				VgiWorkerPool::Instance().Release(std::move(pooled), max_pool_size_);
				VGI_LOG(context_, "worker_pool.release",
				        {{"worker_path", worker_path_},
				         {"worker_pid", std::to_string(worker_pid)},
				         {"max_pool_size", std::to_string(max_pool_size_)}});
			}
		}
	}

	// Connection to worker (owns the subprocess)
	std::unique_ptr<FunctionConnection> connection;

	// Completion tracking
	bool done = false;

private:
	ClientContext &context_;
	size_t max_pool_size_;
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
//! The returned RecordBatch has:
//!   - Column 0: filter_spec (string) - JSON-encoded filter structure
//!   - Columns 1..N: Values referenced by filters, with exact Arrow types
//! Version is stored in Arrow schema metadata on filter_spec field: {"vgi_filter_version": "1"}
std::shared_ptr<arrow::Buffer> VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                                   optional_ptr<TableFilterSet> filters,
                                                   const vector<string> &column_names, const string &worker_path);

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

} // namespace vgi
} // namespace duckdb
