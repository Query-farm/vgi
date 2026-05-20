// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
// State tag - discriminates which code path owns the l_state buffer
// ============================================================================
// DuckDB allocates one l_state buffer per aggregate state, whose size is
// determined by VgiAggregateStateSize(). The same buffer can hold either a
// VgiAggregateState (for update/combine/finalize) or a
// VgiAggregateWindowLocalState (for the window callback). The tag lets the
// shared destructor pick the right cleanup flow.
enum class VgiAggregateStateTag : uint8_t {
	UNSET = 0,
	AGGREGATE = 1,
	WINDOW = 2,
};

// ============================================================================
// VgiAggregateState - trivial C++ state stored in DuckDB's hash table
// ============================================================================

struct VgiAggregateState {
	// Tag MUST be the first field so it can be read via a
	// reinterpret_cast<uint8_t *> regardless of which variant owns the buffer.
	VgiAggregateStateTag tag = VgiAggregateStateTag::UNSET;
	int64_t group_id = -1;
};

// ============================================================================
// VgiAggregateFunctionInfo - attached to each aggregate function during registration
// ============================================================================

struct VgiAggregateFunctionInfo : public AggregateFunctionInfo {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	optional_ptr<Catalog> catalog;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<std::string> setting_names;
	std::vector<VgiSecretRequirement> required_secrets;
	std::shared_ptr<arrow::Schema> output_schema;

	// Const parameter support (same pattern as scalar functions)
	std::vector<bool> positional_is_const;
	std::vector<std::string> positional_names;

	// True if the worker implements the window() callback — enables
	// DuckDB's WindowCustomAggregator path.
	bool supports_window = false;

	// True if the worker opts into the streaming-partitioned protocol —
	// the optimizer rule swaps LogicalWindow for a streaming operator that
	// pipes input chunks straight to the worker (no DuckDB-side partition
	// materialisation).
	bool streaming_partitioned = false;

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
	std::vector<uint8_t> attach_opaque_data;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<VgiSecretRequirement> required_secrets;
	std::shared_ptr<arrow::Schema> resolved_output_schema;
	std::shared_ptr<arrow::Schema> input_schema;
	optional_ptr<Catalog> catalog;
	// Weak reference to the ClientContext that produced this bind_data.
	// Used by the destructor RPC path: late-firing destructors on a
	// task-scheduler thread may run after the originating session has
	// gone, which would dangle a raw pointer. Lock at use-site; if the
	// context is gone, skip the RPC. Uses duckdb::weak_ptr so it pairs
	// with shared_from_this() on ClientContext (which is built around
	// duckdb::enable_shared_from_this).
	weak_ptr<ClientContext> context;
	vector<Value> const_values;

	// Shared execution state across bind_data copies (DuckDB may copy during planning)
	struct ExecState {
		std::atomic<int64_t> group_id_counter{0};
		std::atomic<int64_t> destroy_counter{0};
		// Incremented in VgiAggregateWindowInit to assign a unique partition_id
		// per OVER partition. Used as the storage cache key on the worker.
		std::atomic<int64_t> partition_id_counter{0};
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

// ============================================================================
// Internal RPC helpers (exposed so aggregate_window_impl can share the same
// connection-pool plumbing as the standard aggregate callbacks).
// ============================================================================

struct AggregateRpcResult {
	// Outer vgi_rpc unary envelope: {result: Binary}. Legacy callers still use
	// this and manually unwrap.
	std::shared_ptr<arrow::RecordBatch> response_batch;
	// Inner response batch, already unwrapped from the envelope and validated
	// against the method's registered schema. May be null if the envelope's
	// result column is null or the batch is empty. New code should prefer this.
	std::shared_ptr<arrow::RecordBatch> inner_batch;
};

// Wrap a request batch in the standard vgi_rpc envelope: {request: binary}.
std::shared_ptr<arrow::RecordBatch> WrapAsRpcParams(const std::shared_ptr<arrow::RecordBatch> &request_batch);

// Dispatch an aggregate-flavored unary RPC. Handles subprocess or HTTP
// transport, pool reuse, stale-pool retry — same path as the standard
// aggregate_{bind,update,combine,finalize,destructor} callbacks.
//
// enable_logging: set to false for RPCs dispatched from aggregate destructors
// that run on task-scheduler threads during pipeline teardown. The RPC path
// will then skip VGI_LOG calls and StderrDrainer::DrainToLog — both of which
// reach into ClientContext-backed logging that is not safe off the main
// thread during teardown.
AggregateRpcResult InvokeAggregateRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                      const std::string &method_name,
                                      const std::shared_ptr<arrow::RecordBatch> &params,
                                      bool enable_logging = true);

} // namespace vgi
} // namespace duckdb
