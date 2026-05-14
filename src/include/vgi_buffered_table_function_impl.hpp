#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "vgi_function_connection.hpp"
#include "vgi_table_in_out_impl.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiBufferedTableFunction
// ============================================================================
// Replaces a LogicalGet of a VGI table-in-out function when the function's
// metadata declares Meta.buffered_table = True. The OptimizerExtension
// performs the rewrite at optimize_function time (post-built-in passes);
// any LATERAL decorrelation has already happened by then.
//
// Carries:
//   - table_index: the LogicalGet's table_index, threaded through so
//     downstream column references continue to resolve.
//   - return_types / return_names: the function's output schema.
//   - bind_data: the original VgiTableInOutBindData, owned by us. The
//     PhysicalVgiBufferedTableFunction takes a reference to this for its
//     execution-time RPC machinery.

class LogicalVgiBufferedTableFunction : public LogicalExtensionOperator {
public:
	LogicalVgiBufferedTableFunction(idx_t table_index_p, vector<LogicalType> return_types_p,
	                                vector<string> return_names_p,
	                                unique_ptr<FunctionData> bind_data_p);

	// Threaded through for ColumnBinding resolution.
	idx_t table_index;

	// Returned by the underlying function — preserved on the rewrite so
	// downstream operators see the same shape they did against the
	// original LogicalGet.
	vector<LogicalType> return_types;
	vector<string> return_names;

	// The VgiTableInOutBindData from the original bind. CreatePlan reads
	// fields (worker_path, function_name, attach_params, input_schema,
	// bind_result, buffered_table, source_order_dependent) to construct
	// the PhysicalVgiBufferedTableFunction. Stored as the base FunctionData
	// type to play nice with DuckDB's planner conventions; cast to
	// VgiTableInOutBindData at use sites.
	unique_ptr<FunctionData> bind_data;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;
	string GetExtensionName() const override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	// v1: plan caching across processes is out of scope.
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

// ============================================================================
// PhysicalVgiBufferedTableFunction
// ============================================================================
// Sink+Source operator: ingest all input across all sub-pipelines into the
// worker, run a single cross-pipeline combine, then emit output via parallel
// (or single-threaded — see source_order_dependent) source.
//
// Lifecycle (per the design plan, see /Users/rusty/.claude/plans/yes-lets-make-a-elegant-sparrow.md):
//
//   Sink(chunk)                   — parallel per DuckDB thread; first Sink
//                                   acquires the coordinator connection and
//                                   performs init with phase=BUFFERED_TABLE;
//                                   per-thread workers are spawned with
//                                   global_execution_id = gstate.execution_id
//                                   (the secondary-init path).
//   Combine(local sink state)     — moves the per-thread worker into the
//                                   shared gstate.workers/state_ids lists.
//   Finalize()                    — single-threaded, exactly once per
//                                   GlobalSinkState. Calls
//                                   RpcBufferedTableCombine on the
//                                   coordinator; populates finalize_queue.
//   GetData(chunk)                — parallel (or single-threaded when
//                                   source_order_dependent). Pulls next
//                                   finalize_state_id from the queue, calls
//                                   RpcBufferedTableFinalize repeatedly while
//                                   has_more, then moves to the next id.

class PhysicalVgiBufferedTableFunction : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalVgiBufferedTableFunction(PhysicalPlan &physical_plan, vector<LogicalType> return_types_p,
	                                  vector<string> return_names_p,
	                                  unique_ptr<FunctionData> bind_data_p, bool source_order_dependent_p,
	                                  idx_t estimated_cardinality);

	// Owned bind data. The physical operator is the sole long-lived holder
	// during execution; the LogicalVgiBufferedTableFunction transfers
	// ownership at CreatePlan time.
	unique_ptr<FunctionData> bind_data;
	vector<string> return_names;
	bool source_order_dependent;

public:
	// ========== Sink Interface ==========
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}

	// ========== Source Interface ==========
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                  GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                  OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return !source_order_dependent;
	}
	OrderPreservationType SourceOrder() const override {
		return source_order_dependent ? OrderPreservationType::FIXED_ORDER : OrderPreservationType::NO_ORDER;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace vgi
} // namespace duckdb
