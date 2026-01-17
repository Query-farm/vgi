#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>

#include <arrow/api.h>

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

namespace duckdb {

// Forward declaration
namespace vgi {
class FunctionConnection;
}

// ============================================================================
// VgiScalarFunctionInfo - Attached to each scalar function during registration
// ============================================================================

struct VgiScalarFunctionInfo : public ScalarFunctionInfo {
	VgiScalarFunctionInfo() = default;
	~VgiScalarFunctionInfo() override = default;

	// Worker connection parameters
	std::string worker_path;
	std::vector<uint8_t> attach_id;
	std::string function_name;
	bool worker_debug = false;
	bool use_pool = true;
	std::map<std::string, std::string> settings;

	// Schema info from catalog registration
	std::shared_ptr<arrow::Schema> output_schema;  // Single "result" column
};

// ============================================================================
// VgiScalarFunctionLocalState - Per-thread execution state
// ============================================================================

struct VgiScalarFunctionLocalState : public FunctionLocalState {
	VgiScalarFunctionLocalState();
	~VgiScalarFunctionLocalState() override;

	// Active connection to worker (created lazily on first execute)
	std::unique_ptr<vgi::FunctionConnection> connection;

	// Whether connection has been initialized
	bool initialized = false;

	// Cached input schema (built from DataChunk column types on first call)
	std::shared_ptr<arrow::Schema> input_schema;
};

// ============================================================================
// Function Declarations
// ============================================================================

namespace vgi {

// Initialize local state for scalar function execution
unique_ptr<FunctionLocalState> VgiScalarFunctionInitLocalState(ExpressionState &state,
                                                                const BoundFunctionExpression &expr,
                                                                FunctionData *bind_data);

// Main scalar function execution handler
void VgiScalarFunctionExecute(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace vgi
} // namespace duckdb
