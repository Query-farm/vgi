// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_lateral_batch_operator.hpp"

#include <cstring>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiLateralBatch
// ============================================================================

vector<ColumnBinding> LogicalVgiLateralBatch::GetColumnBindings() {
	// Worker output columns under table_index (snapshot), then the child's
	// projected/outer column bindings — byte-for-byte LogicalGet's shape.
	auto result = worker_bindings;
	auto child_bindings = children[0]->GetColumnBindings();
	for (auto entry : projected_input) {
		result.emplace_back(child_bindings[entry]);
	}
	return result;
}

vector<idx_t> LogicalVgiLateralBatch::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalVgiLateralBatch::GetName() const {
	return "VGI_LATERAL_BATCH";
}

string LogicalVgiLateralBatch::GetExtensionName() const {
	return "vgi_lateral_batch";
}

void LogicalVgiLateralBatch::ResolveTypes() {
	types = worker_output_types;
	for (auto entry : projected_input) {
		types.push_back(children[0]->types[entry]);
	}
}

void LogicalVgiLateralBatch::Serialize(Serializer & /*serializer*/) const {
	throw NotImplementedException("LogicalVgiLateralBatch serialization is not supported "
	                              "(plan caching across processes is out of scope for v1)");
}

PhysicalOperator &LogicalVgiLateralBatch::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	estimated_cardinality = EstimateCardinality(context);
	auto &child_plan = planner.CreatePlan(*children[0]);
	// input_length = worker-input width = child columns minus the correlated cols.
	const idx_t input_length = children[0]->types.size() - projected_input.size();
	const idx_t base_idx = worker_output_types.size();
	auto out_types = types; // [worker | projected]
	auto &op = planner.Make<PhysicalVgiLateralBatch>(std::move(out_types), std::move(bind_data),
	                                                  std::move(projected_input), input_length, base_idx,
	                                                  estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

// ============================================================================
// Operator state
// ============================================================================

namespace {

struct VgiLateralBatchGlobalState : public GlobalOperatorState {};

struct VgiLateralBatchOperatorState : public OperatorState {
	VgiLateralBatchOperatorState(ClientContext &context, const std::vector<uint8_t> &substream_id)
	    : substream_id(substream_id),
	      scan(make_uniq<ArrowArrayWrapper>(), context) {
	}
	~VgiLateralBatchOperatorState() override {
		// Mid-stream worker (no finalize map) is never pooled — just drop it.
		connection.reset();
	}

	std::unique_ptr<IFunctionConnection> connection; // this thread's own worker
	std::vector<uint8_t> substream_id;
	VgiTableInOutLocalState scan; // drain buffer for the worker output batch
	// Per-output-row parent input-row index for the batch currently in `scan`.
	// Held across HAVE_MORE_OUTPUT slices (the input chunk is re-passed unchanged
	// by DuckDB while draining, so stamping stays correct). Identity [0..n) when the
	// worker emitted no provenance (1->1 maps). Cleared when the batch is exhausted.
	std::vector<int32_t> parent_index;
};

// Decode the worker's per-output-row provenance. `raw` is the base64-decoded raw
// little-endian int32[] from the batch's ``vgi_rpc.parent_row`` metadata (already
// b64-decoded by the connection). Empty = no provenance: assume an identity 1->1
// map (valid only when output rows == input rows, else a loud error). Validates
// length and that every parent index is in [0, input_rows).
std::vector<int32_t> DecodeParentRow(const std::string &raw, idx_t output_rows, idx_t input_rows,
                                     const VgiTableInOutBindData &bd) {
	std::vector<int32_t> result;
	if (raw.empty()) {
		if (output_rows != input_rows) {
			throw IOException(
			    "vgi_batch_lateral: worker '%s' function '%s' returned %llu output rows for %llu input rows "
			    "without vgi_rpc.parent_row provenance; a fan-out (1->N) or filtering (1->0) blended LATERAL map "
			    "must emit per-output-row parent indices",
			    bd.worker_path(), bd.function_name, (unsigned long long)output_rows, (unsigned long long)input_rows);
		}
		result.resize(output_rows);
		for (idx_t i = 0; i < output_rows; i++) {
			result[i] = static_cast<int32_t>(i);
		}
		return result;
	}
	if (raw.size() != output_rows * sizeof(int32_t)) {
		throw IOException("vgi_batch_lateral: vgi_rpc.parent_row is %llu bytes for %llu output rows "
		                  "(expected %llu); worker '%s' function '%s'",
		                  (unsigned long long)raw.size(), (unsigned long long)output_rows,
		                  (unsigned long long)(output_rows * sizeof(int32_t)), bd.worker_path(), bd.function_name);
	}
	result.resize(output_rows);
	std::memcpy(result.data(), raw.data(), raw.size()); // LE int32[] — DuckDB targets are little-endian
	for (idx_t i = 0; i < output_rows; i++) {
		if (result[i] < 0 || static_cast<idx_t>(result[i]) >= input_rows) {
			throw IOException("vgi_batch_lateral: parent_row[%llu] = %d is out of range [0, %llu); "
			                  "worker '%s' function '%s'",
			                  (unsigned long long)i, result[i], (unsigned long long)input_rows, bd.worker_path(),
			                  bd.function_name);
		}
	}
	return result;
}

// Stamp the projected/correlated columns onto the produced output slice by gathering
// each input correlated column at the parent-row index of every output row. `start`
// is the offset into `parent_index` for the first row of this slice (== the scan's
// chunk_offset before this ProduceOutputFromBatch call).
void StampProjected(DataChunk &chunk, DataChunk &input, const std::vector<int32_t> &parent_index,
                    const std::vector<column_t> &projected_input, idx_t base_idx, idx_t start, idx_t produced) {
	if (produced == 0) {
		return;
	}
	SelectionVector sel(produced);
	for (idx_t r = 0; r < produced; r++) {
		sel.set_index(r, static_cast<idx_t>(parent_index[start + r]));
	}
	for (idx_t k = 0; k < projected_input.size(); k++) {
		// target[r] = input_col[sel[r]] — a flat gather that severs the input
		// dependency for the emitted chunk (like the streaming-window op's copy).
		VectorOperations::Copy(input.data[projected_input[k]], chunk.data[base_idx + k], sel, produced, 0, 0);
	}
}

} // anonymous namespace

// ============================================================================
// PhysicalVgiLateralBatch
// ============================================================================

PhysicalVgiLateralBatch::PhysicalVgiLateralBatch(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                 unique_ptr<FunctionData> bind_data_p, vector<column_t> projected_input_p,
                                                 idx_t input_length_p, idx_t base_idx_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      bind_data(std::move(bind_data_p)), projected_input(std::move(projected_input_p)), input_length(input_length_p),
      base_idx(base_idx_p) {
}

unique_ptr<GlobalOperatorState> PhysicalVgiLateralBatch::GetGlobalOperatorState(ClientContext & /*context*/) const {
	return make_uniq<VgiLateralBatchGlobalState>();
}

unique_ptr<OperatorState> PhysicalVgiLateralBatch::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<VgiLateralBatchOperatorState>(context.client, MintSubstreamId());
}

OperatorResultType PhysicalVgiLateralBatch::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState & /*gstate*/, OperatorState &state_p) const {
	auto &state = state_p.Cast<VgiLateralBatchOperatorState>();
	auto &client_context = context.client;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();

	// Lazy per-substream worker acquire (INPUT phase, input writer open).
	if (!state.connection) {
		state.connection = AcquireBlendedInputConnection(client_context, bd, state.substream_id);
	}

	// (A) Drain the remaining slices of the output batch already loaded in `scan`.
	// DuckDB re-passes the SAME `input` chunk unchanged across HAVE_MORE_OUTPUT, so
	// the held parent_index still refers to the right correlated rows.
	if (HasRemainingBatchData(state.scan)) {
		idx_t start = state.scan.chunk_offset; // rows already produced from this batch
		idx_t produced = ProduceOutputFromBatch(state.scan, bd.arrow_table, chunk);
		StampProjected(chunk, input, state.parent_index, projected_input, base_idx, start, produced);
		chunk.SetCardinality(produced);
		return HasRemainingBatchData(state.scan) ? OperatorResultType::HAVE_MORE_OUTPUT
		                                         : OperatorResultType::NEED_MORE_INPUT;
	}

	if (input.size() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// (B) Fresh input chunk: split off the worker-input columns [0, input_length)
	// as a zero-copy view.
	DataChunk worker_input;
	vector<LogicalType> in_types;
	in_types.reserve(input_length);
	for (idx_t c = 0; c < input_length; c++) {
		in_types.push_back(input.data[c].GetType());
	}
	worker_input.InitializeEmpty(in_types);
	for (idx_t c = 0; c < input_length; c++) {
		worker_input.data[c].Reference(input.data[c]);
	}
	worker_input.SetCardinality(input.size());

	// ONE batched exchange for the whole input chunk.
	auto input_batch = ConvertInputToArrow(client_context, worker_input, bd);
	VGI_LOG(client_context, "table_in_out.write_input",
	        {{"conn", state.connection->GetConnIdHex()},
	         {"worker_path", bd.worker_path()},
	         {"function_name", bd.function_name},
	         {"input_rows", std::to_string(input_batch->num_rows())}});
	state.connection->WriteInputBatch(input_batch);
	auto output_batch = state.connection->ReadDataBatch();

	if (!output_batch) {
		// Unexpected EOS on a live map stream — drop (never pool a mid-stream worker).
		chunk.SetCardinality(0);
		state.connection.reset();
		return OperatorResultType::FINISHED;
	}
	if (output_batch->num_rows() == 0) {
		// 1->0 for the WHOLE chunk: every input row filtered out. (A per-row 1->0 with
		// some surviving rows carries provenance and is handled by the stamp below.)
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Decode provenance (identity when absent + rows align). Held for the drain.
	state.parent_index = DecodeParentRow(state.connection->GetLastParentRowBytes(),
	                                     static_cast<idx_t>(output_batch->num_rows()), input.size(), bd);

	LoadBatchIntoScanState(state.scan, output_batch);
	idx_t start = state.scan.chunk_offset; // 0 for a freshly loaded batch
	idx_t produced = ProduceOutputFromBatch(state.scan, bd.arrow_table, chunk); // fills [0, base_idx)
	StampProjected(chunk, input, state.parent_index, projected_input, base_idx, start, produced);
	chunk.SetCardinality(produced);
	return HasRemainingBatchData(state.scan) ? OperatorResultType::HAVE_MORE_OUTPUT
	                                         : OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalVgiLateralBatch::GetName() const {
	return "VGI_LATERAL_BATCH";
}

InsertionOrderPreservingMap<string> PhysicalVgiLateralBatch::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	result["Function"] = bd.function_name;
	result["Projected"] = std::to_string(projected_input.size());
	return result;
}

// ============================================================================
// OptimizerExtension — VgiLateralBatchRewriter
// ============================================================================

namespace {

bool IsBatchableLateralGet(LogicalGet &get) {
	if (!get.bind_data) {
		return false;
	}
	auto *bd = dynamic_cast<VgiTableInOutBindData *>(get.bind_data.get());
	return bd && bd->input_from_args && !bd->has_finalize && !bd->table_buffering &&
	       !get.projected_input.empty() && get.children.size() == 1;
}

void RewriteOne(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		RewriteOne(child);
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	if (!IsBatchableLateralGet(get)) {
		return;
	}
	const idx_t base_idx = get.types.size() - get.projected_input.size();
	auto all_bindings = get.GetColumnBindings();
	vector<ColumnBinding> worker_bindings(all_bindings.begin(), all_bindings.begin() + base_idx);
	vector<LogicalType> worker_output_types(get.types.begin(), get.types.begin() + base_idx);

	auto new_op = make_uniq<LogicalVgiLateralBatch>(get.table_index, std::move(get.projected_input),
	                                                std::move(worker_output_types), std::move(worker_bindings),
	                                                std::move(get.bind_data));
	new_op->children.push_back(std::move(get.children[0]));
	new_op->ResolveOperatorTypes();
	op = std::move(new_op);
}

class VgiLateralBatchRewriter : public OptimizerExtension {
public:
	VgiLateralBatchRewriter() {
		optimize_function = Optimize;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Value enabled;
		if (input.context.TryGetCurrentSetting("vgi_batch_lateral", enabled) && !enabled.GetValue<bool>()) {
			return; // rollback: leave the row-by-row PhysicalTableInOutFunction path
		}
		RewriteOne(plan);
	}
};

} // anonymous namespace

void RegisterVgiLateralBatchRewriter(DBConfig &config) {
	OptimizerExtension::Register(config, VgiLateralBatchRewriter());
}

} // namespace vgi
} // namespace duckdb
