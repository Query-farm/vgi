#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"

namespace duckdb {
namespace vgi {

// Forward declaration
class FunctionConnection;

// ============================================================================
// Bind Data for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutBindData : public TableFunctionData {
	VgiTableInOutBindData();
	~VgiTableInOutBindData() override;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

	// Connection parameters
	std::string worker_path;
	std::vector<uint8_t> attach_id;
	bool worker_debug = false;
	bool use_pool = true;
	std::string function_name;
	std::map<std::string, std::string> settings;

	// Arguments (excluding TABLE input)
	ArrowArguments arguments;

	// Output schema from bind handshake
	std::shared_ptr<arrow::Schema> output_schema;

	// Input schema (built from table input types/names)
	std::shared_ptr<arrow::Schema> input_schema;

	// Worker capabilities
	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;

	// Connection created during bind, moved to global state during init
	mutable std::unique_ptr<FunctionConnection> bind_connection;
};

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutGlobalState : public GlobalTableFunctionState {
	VgiTableInOutGlobalState();
	~VgiTableInOutGlobalState() override;

	// Primary connection for this execution
	std::unique_ptr<FunctionConnection> connection;

	// Global execution ID for multi-worker coordination
	std::vector<uint8_t> global_execution_id;

	// Pending output batch (when worker produces more output than fits in one chunk)
	std::shared_ptr<arrow::RecordBatch> pending_output;

	// Whether finalize signal has been sent to the worker
	bool finalize_sent = false;

	idx_t MaxThreads() const override {
		return 1; // Table-in-out functions are single-threaded for now
	}
};

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutLocalState : public LocalTableFunctionState {
	VgiTableInOutLocalState();
	~VgiTableInOutLocalState() override;
};

// ============================================================================
// Parameters for Bind Function
// ============================================================================

struct VgiTableInOutBindParams {
	std::string worker_path;
	std::string function_name;
	std::vector<uint8_t> attach_id;
	bool worker_debug = false;
	bool use_pool = true;
	std::map<std::string, std::string> settings;
};

// ============================================================================
// Function Implementations
// ============================================================================

// Bind function for table-in-out functions
// Receives table input schema via input.input_table_types/input_table_names
unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            const VgiTableInOutBindParams &params);

// Init functions
unique_ptr<GlobalTableFunctionState> VgiTableInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state);

// In-out function - processes input chunks and produces output chunks
OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output);

// Finalize function - called when all input has been processed
OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace vgi
} // namespace duckdb
