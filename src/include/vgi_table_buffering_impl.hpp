#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <arrow/buffer.h>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "vgi_function_connection.hpp"
#include "vgi_table_in_out_impl.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiTableBufferingFunction
// ============================================================================
// Replaces a LogicalGet of a VGI table-in-out function when the function's
// metadata declares Meta.table_buffering = True. The OptimizerExtension
// performs the rewrite at optimize_function time (post-built-in passes);
// any LATERAL decorrelation has already happened by then.
//
// Carries:
//   - table_index: the LogicalGet's table_index, threaded through so
//     downstream column references continue to resolve.
//   - return_types / return_names: the function's output schema.
//   - bind_data: the original VgiTableInOutBindData, owned by us. The
//     PhysicalVgiTableBufferingFunction takes a reference to this for its
//     execution-time RPC machinery.

class LogicalVgiTableBufferingFunction : public LogicalExtensionOperator {
public:
	LogicalVgiTableBufferingFunction(idx_t table_index_p, vector<LogicalType> return_types_p,
	                                vector<string> return_names_p,
	                                unique_ptr<FunctionData> bind_data_p);

	// Threaded through for ColumnBinding resolution.
	idx_t table_index;

	// The post-projection types that upstream operators bind against. Matches
	// what ``LogicalGet::ResolveTypes()`` produces for the original Get under
	// projection pushdown: narrowed via column_ids/projection_ids when those
	// are non-empty, otherwise the full worker schema. Downstream column refs
	// resolve to this. NOT the full worker schema — that lives in
	// all_column_types below.
	vector<LogicalType> return_types;
	// Output names parallel to return_types (post-projection).
	vector<string> return_names;

	// The VgiTableInOutBindData from the original bind. CreatePlan reads
	// fields (worker_path, function_name, attach_params, input_schema,
	// bind_result, table_buffering, source_order_dependent) to construct
	// the PhysicalVgiTableBufferingFunction. Stored as the base FunctionData
	// type to play nice with DuckDB's planner conventions; cast to
	// VgiTableInOutBindData at use sites.
	unique_ptr<FunctionData> bind_data;

	// ============================================================================
	// Pushdown — captured by the rewriter from the LogicalGet at optimize time
	// ============================================================================
	// True if the worker declared projection_pushdown=True in Meta and DuckDB
	// pushed a projection. When false, projection_ids is empty and return_types
	// equals the full worker schema.
	bool projection_pushdown = false;
	// Worker-schema column indices DuckDB requested. Empty when no projection.
	// Mirrors the streaming pure-table path's int32_t projection_ids vector.
	std::vector<int32_t> projection_ids;
	// DuckDB-side column index list — what's referenced by table_filters' col
	// indices. Same as ``LogicalGet::GetColumnIds()`` at rewriter time, mapped
	// to int32_t for transit. Needed by the Source-side ArrowScan to do the
	// projected→worker-schema index lookup in ArrowToDuckDB.
	std::vector<int32_t> column_ids;
	// Serialized table_filters (Arrow IPC bytes with json filter_spec + value
	// columns) — null if no filters. Reuses VgiSerializeFilters from the
	// streaming pure-table path.
	std::shared_ptr<arrow::Buffer> pushdown_filters;
	// Per-IN-filter join-keys batches (one single-column Arrow IPC RecordBatch
	// each). Empty when no IN filters. Mirrors the streaming convention.
	std::vector<std::shared_ptr<arrow::Buffer>> join_keys_buffers;
	// Full worker-output schema (size matches the worker's declared output —
	// what the worker would emit absent projection). Preserved verbatim from
	// ``LogicalGet::returned_types`` / ``names`` at rewriter time. Used by
	// VgiSerializeFilters (filter column-index → original-name lookup) and by
	// the Source-side ArrowScan to populate its arrow_column_map_t with the
	// full type signature.
	vector<LogicalType> all_column_types;
	vector<string> all_column_names;
	// Pre-built human-readable description of projections + filters for
	// EXPLAIN output. Built from ``TableFilter::ToString`` at rewriter time;
	// the C++ side has no symmetric deserializer for the serialized filter
	// IPC bytes, so we capture the pretty string when we still have the raw
	// TableFilter objects in scope.
	std::string explain_summary;

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
// PhysicalVgiTableBufferingFunction
// ============================================================================
// Sink+Source operator: ingest all input across all sub-pipelines into the
// worker, run a single cross-pipeline combine, then emit output via parallel
// (or single-threaded — see source_order_dependent) source.
//
// Lifecycle:
//
//   Sink(chunk)                   — parallel per DuckDB thread. The first
//                                   thread to arrive becomes the *init
//                                   runner*: it acquires its own per-thread
//                                   worker, runs PerformInit with phase=
//                                   TABLE_BUFFERING and no global_execution_id
//                                   (the worker mints one), publishes the
//                                   resulting execution_id on the gstate
//                                   under init_mutex/init_cv. Peer Sink
//                                   threads block on the condvar until init
//                                   publishes (or fails), then acquire their
//                                   own workers with the published
//                                   global_execution_id (secondary init —
//                                   no cold work) and run process() per
//                                   chunk. There is no dedicated coordinator
//                                   role; storage is shared via BoundStorage
//                                   keyed by execution_id, so any worker that
//                                   finished init can serve any combine.
//   Combine(local sink state)     — moves the per-thread worker into the
//                                   shared gstate.workers/state_ids lists
//                                   under workers_mutex.
//   Finalize()                    — single-threaded, exactly once per
//                                   GlobalSinkState (DuckDB serializes this
//                                   even under UNION ALL). Pops *one* worker
//                                   from gstate.workers — any worker, since
//                                   they're interchangeable — calls
//                                   RpcTableBufferingCombine on it, pushes
//                                   it back, populates finalize_queue. On
//                                   throw the popped worker is cancel-
//                                   dispatched (see Sink::Finalize comments).
//   GetData(chunk)                — parallel (or single-threaded when
//                                   source_order_dependent). Pulls next
//                                   finalize_state_id from the queue,
//                                   acquires a fresh worker (Sink-phase
//                                   workers can't reuse init), opens a
//                                   producer-mode stream via
//                                   PerformInit(phase=TABLE_BUFFERING_FINALIZE)
//                                   and drains batches until EOS, then
//                                   moves to the next id.

class PhysicalVgiTableBufferingFunction : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalVgiTableBufferingFunction(PhysicalPlan &physical_plan, vector<LogicalType> return_types_p,
	                                  vector<string> return_names_p,
	                                  unique_ptr<FunctionData> bind_data_p, bool source_order_dependent_p,
	                                  bool sink_order_dependent_p, bool requires_input_batch_index_p,
	                                  idx_t estimated_cardinality);

	// Owned bind data. The physical operator is the sole long-lived holder
	// during execution; the LogicalVgiTableBufferingFunction transfers
	// ownership at CreatePlan time.
	unique_ptr<FunctionData> bind_data;
	vector<string> return_names;
	bool source_order_dependent;
	// Sink-side ordering knobs (mutually exclusive — see metadata.py
	// validation). sink_order_dependent → ParallelSink=false (single thread).
	// requires_input_batch_index → RequiredPartitionInfo()=BatchIndex().
	bool sink_order_dependent;
	bool requires_input_batch_index;

	// Pushdown data — populated by CreatePlan from the Logical op. Mirrors
	// the field set on LogicalVgiTableBufferingFunction; see those docstrings.
	// Read by Sink/Source PerformInit calls and by the Source-side ArrowScan.
	bool projection_pushdown = false;
	std::vector<int32_t> projection_ids;
	std::vector<int32_t> column_ids;
	std::shared_ptr<arrow::Buffer> pushdown_filters;
	std::vector<std::shared_ptr<arrow::Buffer>> join_keys_buffers;
	vector<LogicalType> all_column_types;
	vector<string> all_column_names;
	std::string explain_summary;

public:
	// ========== Sink Interface ==========
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	// NextBatch is invoked by DuckDB when batch_index advances. We don't
	// need to flush anything — workers receive batch_index per process()
	// call and handle ordering themselves. Required when
	// RequiredPartitionInfo()=BatchIndex(); no-op otherwise.
	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return !sink_order_dependent;
	}
	bool SinkOrderDependent() const override {
		// Distinct from ParallelSink: SinkOrderDependent affects the
		// optimizer's right-of-join placement etc., and we want the planner
		// to know that order matters when sink_order_dependent is set so
		// it doesn't reorder the input pipeline arbitrarily.
		return sink_order_dependent;
	}
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return requires_input_batch_index
		    ? OperatorPartitionInfo::BatchIndex()
		    : OperatorPartitionInfo::NoPartitionInfo();
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
