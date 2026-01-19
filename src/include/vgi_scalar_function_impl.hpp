#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>

#include <arrow/api.h>

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

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

	// Whether this function has a dynamic return type (vgi:any) that needs bind-time resolution
	bool has_dynamic_return_type = false;

	// Const parameter support: which positional params are constants
	std::vector<bool> positional_is_const;
	std::vector<std::string> positional_names;
};

// ============================================================================
// VgiScalarFunctionBindData - Bind data for scalar functions with dynamic types
// ============================================================================

struct VgiScalarFunctionBindData : public FunctionData {
	VgiScalarFunctionBindData() = default;
	~VgiScalarFunctionBindData() override = default;

	// Copy of function info for execution
	std::string worker_path;
	std::vector<uint8_t> attach_id;
	std::string function_name;
	bool worker_debug = false;
	bool use_pool = true;
	std::map<std::string, std::string> settings;

	// Actual output schema resolved during bind (with concrete types)
	std::shared_ptr<arrow::Schema> resolved_output_schema;

	// Input schema built from argument types during bind (after const params erased)
	std::shared_ptr<arrow::Schema> input_schema;

	// Extracted constant values (from const parameters erased at bind time)
	vector<Value> const_values;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<VgiScalarFunctionBindData>();
		copy->worker_path = worker_path;
		copy->attach_id = attach_id;
		copy->function_name = function_name;
		copy->worker_debug = worker_debug;
		copy->use_pool = use_pool;
		copy->settings = settings;
		copy->resolved_output_schema = resolved_output_schema;
		copy->input_schema = input_schema;
		copy->const_values = const_values;
		return copy;
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<VgiScalarFunctionBindData>();
		return worker_path == other.worker_path && attach_id == other.attach_id &&
		       function_name == other.function_name && worker_debug == other.worker_debug &&
		       use_pool == other.use_pool;
	}
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

// Bind function for scalar functions with dynamic return types
// Connects to the worker to determine actual return type based on argument types
unique_ptr<FunctionData> VgiScalarFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments);

} // namespace vgi
} // namespace duckdb
