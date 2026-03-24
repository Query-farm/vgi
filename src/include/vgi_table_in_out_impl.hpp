#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {
namespace vgi {

// Forward declaration
class IFunctionConnection;

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
	std::vector<uint8_t> transaction_id;
	bool worker_debug = false;
	bool use_pool = false;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Arguments (excluding TABLE input)
	ArrowArguments arguments;

	// Output schema from bind handshake
	std::shared_ptr<arrow::Schema> output_schema;

	// Arrow C ABI schema and table schema for ArrowToDuckDB conversion
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;

	// Input schema (built from table input types/names)
	std::shared_ptr<arrow::Schema> input_schema;

	// Worker capabilities
	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;

	// Connection created during bind, moved to global state during init
	mutable std::unique_ptr<IFunctionConnection> bind_connection;
};

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutGlobalState : public GlobalTableFunctionState {
	VgiTableInOutGlobalState();
	~VgiTableInOutGlobalState() override;

	// Primary connection for this execution
	std::unique_ptr<IFunctionConnection> connection;

	// Global execution ID for multi-worker coordination
	std::vector<uint8_t> global_execution_id;

	// Whether finalize signal has been sent to the worker
	bool finalize_sent = false;

	idx_t MaxThreads() const override {
		return 1; // Table-in-out functions are single-threaded for now
	}
};

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutLocalState : public ArrowScanLocalState {
	VgiTableInOutLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}
	~VgiTableInOutLocalState() override;
};

// ============================================================================
// Parameters for Bind Function
// ============================================================================

struct VgiTableInOutBindParams {
	std::string worker_path;
	std::string function_name;
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> transaction_id;
	bool worker_debug = false;
	bool use_pool = false;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;
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
