#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <unordered_set>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"

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

	// Execution hints from OutputSpec
	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;

	// Invocation identifier returned by the worker (for correlation in subsequent streams)
	std::vector<uint8_t> invocation_id;
	std::string invocation_id_hex; // Cached hex representation for logging

	// Features that the worker has activated for this invocation
	std::unordered_set<std::string> active_features;
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
	std::atomic<idx_t> rows_read{0};

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
	explicit VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}

	// Connection to worker (owns the subprocess)
	std::unique_ptr<FunctionConnection> connection;

	// Completion tracking
	bool done = false;
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
