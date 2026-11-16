// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "vgi_table_in_out_impl.hpp"

namespace duckdb {

struct DBConfig;

namespace vgi {

// Installed by VgiLateralBatchRewriter in place of a correlated (decorrelated,
// projected_input) LogicalGet of a blended VGI table-in-out function. Keeps the
// LogicalGet's exact binding/type shape ([worker output cols under table_index |
// the child's projected/outer cols]) so the enclosing DELIM_JOIN and parents are
// unaffected. CreatePlan builds PhysicalVgiLateralBatch instead of the row-by-row
// PhysicalTableInOutFunction.

class LogicalVgiLateralBatch : public LogicalExtensionOperator {
public:
	LogicalVgiLateralBatch(unique_ptr<LogicalGet> source, vector<LogicalType> worker_output_types,
	                       vector<ColumnBinding> worker_bindings, vector<column_t> worker_column_ids,
	                       bool projection_pushdown);

	TableIndex table_index;
	//! Trailing child-chunk column indices of the correlated/outer columns.
	vector<column_t> projected_input;
	//! Worker output column types (the leading base_idx columns).
	vector<LogicalType> worker_output_types;
	//! Worker-original column indices of the leading base_idx output columns (from
	//! LogicalGet::GetColumnIds(), narrowed by DuckDB's UNUSED_COLUMNS pass). With
	//! projection pushdown they are sent as the wire projection and drive the
	//! projected ArrowToDuckDB remap; otherwise they are the identity.
	vector<column_t> worker_column_ids;
	//! Echo of VgiTableInOutBindData::projection_pushdown — gates whether the wire
	//! projection is sent (sending it to a non-projecting worker mis-projects).
	bool projection_pushdown;
	//! Worker output column bindings (ColumnBinding(table_index, i)), snapshotted at
	//! rewrite so they don't depend on the child.
	vector<ColumnBinding> worker_bindings;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;
	string GetExtensionName() const override;
	LogicalPlanSQLExportResult ToSQL(LogicalPlanSQLExportContext &context,
	                                 const LogicalPlanVerificationPath &path) override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;

private:
	//! Retains the complete invocation and bind data; the live child belongs to this operator.
	unique_ptr<LogicalGet> sql_source;
};

// Pipeline operator (Execute only) replacing the row-by-row
// PhysicalTableInOutFunction for a correlated blended VGI table-in-out call: ships
// each input chunk's worker-input columns to the worker in one exchange per call
// (split by input_rows_per_call), reads the output with its per-output-row
// provenance (vgi_rpc.parent_row; identity when absent), stamps the correlated
// columns onto each output row by that provenance, and drains output larger than
// STANDARD_VECTOR_SIZE across HAVE_MORE_OUTPUT calls.

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
	//! See LogicalVgiLateralBatch::worker_column_ids.
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
		// Each thread's OperatorState has its own substream_id and worker; no
		// connection is shared.
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

void RegisterVgiLateralBatchRewriter(DBConfig &config);

} // namespace vgi
} // namespace duckdb
