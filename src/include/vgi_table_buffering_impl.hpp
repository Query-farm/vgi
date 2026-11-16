// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>


#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "vgi_function_connection.hpp"
#include "vgi_table_in_out_impl.hpp"

namespace duckdb {
namespace vgi {

// Replaces the LogicalGet of a buffered (Meta.table_buffering) VGI table-in-out
// function. The OptimizerExtension rewrites after the built-in passes, so LATERAL
// decorrelation has already happened.

class LogicalVgiTableBufferingFunction : public LogicalExtensionOperator {
public:
	LogicalVgiTableBufferingFunction(TableIndex table_index_p, vector<LogicalType> return_types_p,
	                                vector<string> return_names_p,
	                                unique_ptr<FunctionData> bind_data_p);

	// The LogicalGet's, so downstream column references still resolve.
	TableIndex table_index;

	// Post-projection types, as LogicalGet::ResolveTypes() gives for the original
	// Get; downstream column refs resolve to these. The full worker schema is
	// all_column_types.
	vector<LogicalType> return_types;
	vector<string> return_names;

	// The original VgiTableInOutBindData; ownership moves to the physical
	// operator at CreatePlan.
	unique_ptr<FunctionData> bind_data;

	// Pushdown, captured by the rewriter from the LogicalGet.
	// Worker declared projection_pushdown and DuckDB pushed a projection; when
	// false, projection_ids is empty and return_types is the full worker schema.
	bool projection_pushdown = false;
	// Worker-schema column indices requested (empty = no projection).
	std::vector<int32_t> projection_ids;
	// LogicalGet::GetColumnIds() (projected output -> worker index). Table-filter
	// keys are already bind-schema indexes and do not index this.
	std::vector<int32_t> column_ids;
	// VgiSerializeFilters output; null if no filters.
	VgiBufferPtr pushdown_filters;
	// One single-column IPC batch per IN filter.
	std::vector<VgiBufferPtr> join_keys_buffers;
	// Full worker-output schema (LogicalGet::returned_types / names), for filter
	// serialization (column index -> name).
	vector<LogicalType> all_column_types;
	vector<string> all_column_names;
	// Projections + filters for EXPLAIN, from TableFilter::ToString at rewrite
	// time: the serialized filter bytes cannot be turned back into text.
	std::string explain_summary;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;
	string GetExtensionName() const override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	// Throws: plan serialization is not supported.
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

// Sink+Source operator for buffered table functions: process() all input on
// per-thread workers, one combine, then drain each finalize state.
//
//   Sink     — parallel. The first thread runs a primary TABLE_BUFFERING init on
//              its own worker (the worker mints execution_id); peers wait on
//              init_cv, then run secondary inits with that id. Each chunk is one
//              process() RPC. No coordinator: workers share state through
//              BoundStorage keyed by execution_id, so any of them can combine.
//   Combine  — moves the thread's worker and state_id into the gstate.
//   Finalize — once per GlobalSinkState. Probes the result cache, then pops one
//              worker (or, for empty input, inits one) and runs combine, filling
//              finalize_queue. On throw that worker is cancel-dispatched.
//   GetData  — parallel unless source_order_dependent. Per finalize_state_id, a
//              fresh worker (Sink workers reject a second PerformInit) opens a
//              TABLE_BUFFERING_FINALIZE producer stream and drains it to EOS.

class PhysicalVgiTableBufferingFunction : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalVgiTableBufferingFunction(PhysicalPlan &physical_plan, vector<LogicalType> return_types_p,
	                                  vector<string> return_names_p,
	                                  unique_ptr<FunctionData> bind_data_p, bool source_order_dependent_p,
	                                  bool sink_order_dependent_p, bool requires_input_batch_index_p,
	                                  bool synthesize_batch_index_p, idx_t estimated_cardinality);

	// Owned (moved from the logical operator at CreatePlan).
	unique_ptr<FunctionData> bind_data;
	vector<string> return_names;
	bool source_order_dependent;
	// Sink ordering (mutually exclusive as declared by the worker):
	// sink_order_dependent → single-threaded sink; requires_input_batch_index →
	// RequiredPartitionInfo()=BatchIndex().
	bool sink_order_dependent;
	bool requires_input_batch_index;
	// The worker asked for a batch index but the pipeline source cannot supply
	// one (range(), VALUES, ...): the sink runs single-threaded, so batches arrive
	// in source order, and numbers them itself. See CreatePlan.
	bool synthesize_batch_index;

	// Pushdown, moved from LogicalVgiTableBufferingFunction (see there); used by
	// every Sink and Source PerformInit.
	bool projection_pushdown = false;
	std::vector<int32_t> projection_ids;
	std::vector<int32_t> column_ids;
	VgiBufferPtr pushdown_filters;
	std::vector<VgiBufferPtr> join_keys_buffers;
	vector<LogicalType> all_column_types;
	vector<string> all_column_names;
	std::string explain_summary;

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	// No-op: workers get batch_index on every process() call.
	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		// Synthesized numbering is only meaningful on a single thread: with a
		// parallel sink, "the order batches arrived" is not the source order.
		return !sink_order_dependent && !synthesize_batch_index;
	}
	bool SinkOrderDependent() const override {
		// Tells the planner input order matters, so it does not reorder the
		// input pipeline; a synthesized batch index depends on it too.
		return sink_order_dependent || synthesize_batch_index;
	}
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return requires_input_batch_index
		    ? OperatorPartitionInfo::BatchIndex()
		    : OperatorPartitionInfo::NoPartitionInfo();
	}

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
