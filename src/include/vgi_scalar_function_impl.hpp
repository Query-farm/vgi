// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>

#include <arrow/api.h>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/main/client_properties.hpp" // ClientProperties

#include "vgi_catalog_metadata.hpp"
#include "vgi_result_cache.hpp" // VgiResultCacheKey (per-value memo)

namespace duckdb {

// Forward declaration
namespace vgi {
class IFunctionConnection;
}

// ============================================================================
// VgiScalarFunctionInfo - Attached to each scalar function during registration
// ============================================================================

struct VgiScalarFunctionInfo : public ScalarFunctionInfo {
	VgiScalarFunctionInfo() = default;
	~VgiScalarFunctionInfo() override = default;

	// Worker connection parameters
	std::shared_ptr<vgi::VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::vector<uint8_t> attach_opaque_data;
	optional_ptr<Catalog> catalog;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<std::string> setting_names;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

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
	std::shared_ptr<vgi::VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::vector<uint8_t> attach_opaque_data;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

	// Actual output schema resolved during bind (with concrete types)
	std::shared_ptr<arrow::Schema> resolved_output_schema;

	// Input schema built from argument types during bind (after const params erased)
	std::shared_ptr<arrow::Schema> input_schema;

	// DuckDB-side input types corresponding to `input_schema`, captured at
	// bind time. Used at execute time to cast incoming DataChunk columns back
	// to the bind-promised types before serializing to Arrow — DuckDB's
	// optimizer may elide the Cast it inserted at bind time (e.g. skipping a
	// DECIMAL(3,2)→DOUBLE cast in the physical plan), so args.data[i] can
	// arrive with a narrower type than the worker was told to expect.
	vector<LogicalType> input_duckdb_types;

	// Extracted constant values (from const parameters erased at bind time)
	vector<Value> const_values;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<VgiScalarFunctionBindData>();
		copy->attach_params = attach_params;
		copy->attach_opaque_data = attach_opaque_data;
		copy->function_name = function_name;
		copy->settings = settings;
		copy->required_secrets = required_secrets;
		copy->resolved_output_schema = resolved_output_schema;
		copy->input_schema = input_schema;
		copy->input_duckdb_types = input_duckdb_types;
		copy->const_values = const_values;
		return copy;
	}

	// Intentionally compares only identity fields (worker_path, attach_opaque_data, function_name, etc.)
	// for DuckDB plan cache matching. Derived fields like schemas and constants are excluded
	// because they are deterministic given the identity fields and argument types.
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<VgiScalarFunctionBindData>();
		return worker_path() == other.worker_path() && attach_opaque_data == other.attach_opaque_data &&
		       function_name == other.function_name && worker_debug() == other.worker_debug() &&
		       use_pool() == other.use_pool();
	}
};

// ============================================================================
// VgiScalarFunctionLocalState - Per-thread execution state
// ============================================================================

struct VgiScalarFunctionLocalState : public FunctionLocalState {
	VgiScalarFunctionLocalState();
	~VgiScalarFunctionLocalState() override;

	// Active connection to worker (created lazily on first execute)
	std::unique_ptr<vgi::IFunctionConnection> connection;

	// Whether connection has been initialized
	bool initialized = false;

	// Cached input schema (built from DataChunk column types on first call)
	std::shared_ptr<arrow::Schema> input_schema;

	// Pool setting captured during initialization (for use in destructor)
	bool use_pool = false;

	// Cached output schema parse — populated on the first batch and reused
	// for every subsequent batch on this thread. The output schema of a
	// scalar function is fixed at bind time, so re-running
	// ArrowSchemaToDuckDBTypes per batch (which does ExportSchema +
	// PopulateArrowTableSchema + GetDuckDBTypesFromArrowTable) is wasted
	// work. Caching these four fields in local state turns the per-batch
	// "schema" phase from O(parse) into a member access.
	ArrowSchemaWrapper output_c_schema;
	ArrowTableSchema output_arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	bool output_schema_cached = false;

	// Cached input-side conversion state — populated on the first batch and
	// reused on every subsequent batch by DataChunkToArrowCached. Skips the
	// per-batch types/names push-back, ClientProperties copy, and
	// ArrowTypeExtensionData::GetExtensionTypes() lookup that
	// DataChunkToArrow otherwise does internally.
	vector<LogicalType> input_arrow_types;
	vector<string> input_arrow_names;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> input_extension_types;
	std::optional<ClientProperties> input_client_props;
	bool input_convert_cached = false;

	// Per-value memoization: the static cache key (identity + worker + fn + const args +
	// settings + versions) built once on the first batch; per-value keys derive from it by
	// setting input_hash per distinct input tuple. cache_eligible gates the whole tier.
	bool cache_key_built = false;
	bool cache_eligible = false;
	vgi::VgiResultCacheKey cache_static_key;
	std::string cache_catalog_name;
	int64_t cache_default_ttl_seconds = 0;
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
