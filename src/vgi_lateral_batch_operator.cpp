// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_lateral_batch_operator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
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
};

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

	if (input.size() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Split off the worker-input columns [0, input_length) as a zero-copy view.
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
		// 1->0: every input row filtered out this chunk.
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// SPIKE: 1->1 identity provenance only. Assert row-alignment so the direct
	// Reference stamp below is correct; multi-slice drain + real provenance follow.
	if (static_cast<idx_t>(output_batch->num_rows()) != input.size()) {
		throw NotImplementedException(
		    "vgi_batch_lateral spike supports only 1->1 maps (got %lld output rows for %llu input rows); "
		    "provenance-based 1->N is not yet implemented",
		    (long long)output_batch->num_rows(), (unsigned long long)input.size());
	}

	LoadBatchIntoScanState(state.scan, output_batch);
	idx_t produced = ProduceOutputFromBatch(state.scan, bd.arrow_table, chunk); // fills [0, base_idx)
	if (HasRemainingBatchData(state.scan)) {
		throw NotImplementedException("vgi_batch_lateral spike: output batch > STANDARD_VECTOR_SIZE "
		                              "(multi-slice drain not yet implemented)");
	}

	// Identity stamp: output row r <- input row r, so each projected/outer column
	// is a direct reference to the input's correlated column (aligned 1:1).
	for (idx_t k = 0; k < projected_input.size(); k++) {
		chunk.data[base_idx + k].Reference(input.data[projected_input[k]]);
	}
	chunk.SetCardinality(produced);
	return OperatorResultType::NEED_MORE_INPUT;
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
