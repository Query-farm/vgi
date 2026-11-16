// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_aggregate_streaming_impl.hpp"

namespace duckdb {
namespace vgi {

// Replaces a LogicalWindow (VgiStreamingWindowOptimizer) when every window
// expression is a streaming_partitioned VGI aggregate over a cumulative frame.
// Keeps LogicalWindow's column bindings so the rewrite is invisible downstream.

class LogicalVgiStreamingWindow : public LogicalExtensionOperator {
public:
	explicit LogicalVgiStreamingWindow(TableIndex window_index)
	    : window_index(window_index) {
	}

	TableIndex window_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;
	string GetExtensionName() const override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	// Throws NotImplementedException: the operator is not serializable.
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

// Pipeline operator (Execute / FinalExecute). Opens one aggregate_streaming
// session per window expression on the first chunk, ships each chunk via
// streaming_chunk, appends the worker's result columns to the passed-through
// input, and closes the sessions in FinalExecute. Single-threaded because a
// session's cross-partition state lives on one worker; that state is not spillable.

class PhysicalVgiStreamingWindow : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalVgiStreamingWindow(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                            vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality);

	//! BoundWindowExpressions. Output schema is [child types..., expr return types...],
	//! as for PhysicalWindow.
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
		return false; // cross-partition state lives on one worker session.
	}

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::FIXED_ORDER;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace vgi
} // namespace duckdb
