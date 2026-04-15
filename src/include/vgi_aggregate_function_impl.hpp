#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/function/aggregate_function.hpp"

#include "vgi_catalog_api.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// VgiAggregateState - trivial C++ state stored in DuckDB's hash table
// ============================================================================

struct VgiAggregateState {
	int64_t group_id = -1;
};

// ============================================================================
// VgiAggregateFunctionInfo - attached to each aggregate function during registration
// ============================================================================

struct VgiAggregateFunctionInfo : public AggregateFunctionInfo {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_id;
	optional_ptr<Catalog> catalog;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<std::string> setting_names;
	std::vector<VgiSecretRequirement> required_secrets;
	std::shared_ptr<arrow::Schema> output_schema;

	// Const parameter support (same pattern as scalar functions)
	std::vector<bool> positional_is_const;
	std::vector<std::string> positional_names;

	const std::string &worker_path() const {
		return attach_params->worker_path();
	}
	bool worker_debug() const {
		return attach_params->worker_debug();
	}
	bool use_pool() const {
		return attach_params->use_pool();
	}
};

// ============================================================================
// VgiAggregateBindData - per-query bind data
// ============================================================================

struct VgiAggregateBindData : public FunctionData {
	// Identity fields
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_id;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<VgiSecretRequirement> required_secrets;
	std::shared_ptr<arrow::Schema> resolved_output_schema;
	std::shared_ptr<arrow::Schema> input_schema;
	optional_ptr<Catalog> catalog;
	ClientContext *context = nullptr;
	vector<Value> const_values;

	// Shared execution state across bind_data copies (DuckDB may copy during planning)
	struct ExecState {
		std::atomic<int64_t> group_id_counter{0};
		std::atomic<int64_t> destroy_counter{0};
		std::vector<uint8_t> execution_id;
	};
	std::shared_ptr<ExecState> exec_state = std::make_shared<ExecState>();

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

// ============================================================================
// Aggregate function callbacks
// ============================================================================

idx_t VgiAggregateStateSize(const AggregateFunction &function);
void VgiAggregateInitialize(const AggregateFunction &function, data_ptr_t state);
void VgiAggregateUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state,
                        idx_t count);
void VgiAggregateCombine(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count);
void VgiAggregateFinalize(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                          idx_t offset);
void VgiAggregateDestroy(Vector &state, AggregateInputData &aggr_input_data, idx_t count);

unique_ptr<FunctionData> VgiAggregateFunctionBind(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments);

} // namespace vgi
} // namespace duckdb
