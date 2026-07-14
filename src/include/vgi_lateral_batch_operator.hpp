// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "vgi_table_in_out_impl.hpp"

namespace duckdb {

class DBConfig;

namespace vgi {

// ============================================================================
// LogicalVgiLateralBatch
// ============================================================================
// Installed by VgiLateralBatchRewriter in place of a correlated (decorrelated,
// projected_input) LogicalGet of a blended VGI table-in-out function. Replicates
// the LogicalGet's column-binding / type shape exactly ([worker output cols under
// table_index | the child's projected/outer cols]) so the enclosing DELIM_JOIN and
// parent operators are unaffected — the rewrite is transparent. CreatePlan builds
// the batched PhysicalVgiLateralBatch instead of the row-by-row
// PhysicalTableInOutFunction.

class LogicalVgiLateralBatch : public LogicalExtensionOperator {
public:
	LogicalVgiLateralBatch(idx_t table_index, vector<column_t> projected_input,
	                       vector<LogicalType> worker_output_types, vector<ColumnBinding> worker_bindings,
	                       unique_ptr<FunctionData> bind_data, vector<column_t> worker_column_ids,
	                       bool projection_pushdown)
	    : table_index(table_index), projected_input(std::move(projected_input)),
	      worker_output_types(std::move(worker_output_types)), worker_bindings(std::move(worker_bindings)),
	      bind_data(std::move(bind_data)), worker_column_ids(std::move(worker_column_ids)),
	      projection_pushdown(projection_pushdown) {
	}

	idx_t table_index;
	//! Trailing child-chunk column indices of the correlated/outer columns.
	vector<column_t> projected_input;
	//! Worker output column types (the leading base_idx columns).
	vector<LogicalType> worker_output_types;
	//! Worker-original column indices of the leading base_idx output columns (from
	//! LogicalGet::GetColumnIds()). When the function supports projection pushdown,
	//! DuckDB's UNUSED_COLUMNS pass narrows these to the referenced worker columns;
	//! they are threaded to the worker as the wire projection so it emits exactly
	//! this narrow set, and drive the projected ArrowToDuckDB remap.
	vector<column_t> worker_column_ids;
	//! Echo of VgiTableInOutBindData::projection_pushdown — gates whether the wire
	//! projection is sent (sending it to a non-projecting worker mis-projects).
	bool projection_pushdown;
	//! Worker output column bindings (ColumnBinding(table_index, i)); snapshot at
	//! rewrite so we don't depend on the child for these.
	vector<ColumnBinding> worker_bindings;
	//! The VgiTableInOutBindData (owns worker connection info); outlives the LogicalGet.
	unique_ptr<FunctionData> bind_data;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;
	string GetExtensionName() const override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

// ============================================================================
// PhysicalVgiLateralBatch
// ============================================================================
// Pipeline Operator (Execute, no Sink/Source). Replaces the row-by-row
// PhysicalTableInOutFunction for a correlated blended VGI table-in-out call:
// ships the WHOLE input chunk's worker-input columns to the worker in ONE
// exchange, reads one output batch (+ per-output-row provenance), stamps the
// correlated columns onto each output row by the provenance, and drains a 1->N
// output larger than STANDARD_VECTOR_SIZE across HAVE_MORE_OUTPUT calls.
//
// Spike (v1): 1->1 identity provenance only (assumes output rows == input rows,
// stamps by row index), ParallelOperator()=false. Provenance + 1->N + parallel
// follow.

class PhysicalVgiLateralBatch : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalVgiLateralBatch(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                        unique_ptr<FunctionData> bind_data, vector<column_t> projected_input, idx_t input_length,
	                        idx_t base_idx, vector<column_t> worker_column_ids, bool projection_pushdown,
	                        idx_t estimated_cardinality);

	//! Owns the VgiTableInOutBindData for the life of the plan.
	unique_ptr<FunctionData> bind_data;
	//! Trailing child-chunk column indices of the correlated columns.
	vector<column_t> projected_input;
	//! Number of worker-input columns (leading columns of the child chunk).
	idx_t input_length;
	//! First output column index of the projected/outer columns.
	idx_t base_idx;
	//! Worker-original column indices of the leading base_idx output columns. When
	//! projection_pushdown is set, threaded to the worker as the wire projection so
	//! it emits exactly this narrow set, and used to drive the projected
	//! ArrowToDuckDB remap (arrow_scan_is_projected=true).
	vector<column_t> worker_column_ids;
	//! Whether the function supports projection pushdown (gates threading the wire
	//! projection + the projected ArrowToDuckDB read).
	bool projection_pushdown;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		// Each pipeline-executor thread owns its OperatorState -> its own substream_id
		// -> its own worker (Phase A per-substream fan-out). No shared connection.
		return true;
	}

	OrderPreservationType OperatorOrder() const override {
		// LATERAL without an outer ORDER BY is unordered, and after decorrelation the
		// correlated columns are value-joined, so cross-morsel reordering is sound.
		return OrderPreservationType::NO_ORDER;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

// OptimizerExtension registration (defined in vgi_lateral_batch_operator.cpp).
void RegisterVgiLateralBatchRewriter(DBConfig &config);

} // namespace vgi
} // namespace duckdb
