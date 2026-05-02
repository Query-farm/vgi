#pragma once

#include <memory>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_aggregate_streaming_impl.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiStreamingWindow
// ============================================================================
// Inserted by the optimizer rule in place of a LogicalWindow when every
// window expression is a VGI aggregate with streaming_partitioned=true and
// the frame is cumulative. Mirrors LogicalWindow's column-binding shape so
// downstream operators see the same column references — the rewrite is
// transparent.

class LogicalVgiStreamingWindow : public LogicalExtensionOperator {
public:
	explicit LogicalVgiStreamingWindow(idx_t window_index)
	    : window_index(window_index) {
	}

	idx_t window_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;
	string GetExtensionName() const override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	// Serialization left as an explicit no-op for v1 — plan caching across
	// processes isn't a v1 requirement.
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

// ============================================================================
// PhysicalVgiStreamingWindow
// ============================================================================
// Pipeline operator (Execute / FinalExecute, not Sink+Source). Receives
// input chunks from upstream; lazily opens an aggregate_streaming session
// on the first chunk; ships each chunk to the worker via streaming_chunk
// and appends the worker's result column(s) to the passed-through input;
// closes the session on FinalExecute.
//
// v1 constraints:
//   - One window expression per node. (Multi-expression LogicalWindow nodes
//     are rejected by the optimizer rule for v1.)
//   - Single-threaded (ParallelOperator() = false). Future multi-thread
//     support routes by partition-key hash to disjoint worker connections.
//   - Streaming session state lives on the worker for the duration of the
//     query; not spillable.

class PhysicalVgiStreamingWindow : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalVgiStreamingWindow(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                            vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality);

	//! The window expressions (each is a BoundWindowExpression). The
	//! operator's output schema is [child types..., expr return types...]
	//! exactly mirroring PhysicalWindow's shape.
	vector<unique_ptr<Expression>> select_list;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk,
	                                         GlobalOperatorState &gstate, OperatorState &state) const override;

	bool RequiresFinalExecute() const override {
		return true;
	}

	bool ParallelOperator() const override {
		return false; // v1: single-threaded; cross-partition state lives on one worker.
	}

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::FIXED_ORDER;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace vgi
} // namespace duckdb
