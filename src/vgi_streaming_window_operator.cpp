// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_streaming_window_operator.hpp"

#include <arrow/c/bridge.h>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_aggregate_streaming_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_attach_parameters.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiStreamingWindow
// ============================================================================

vector<ColumnBinding> LogicalVgiStreamingWindow::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t i = 0; i < expressions.size(); i++) {
		child_bindings.emplace_back(window_index, i);
	}
	return child_bindings;
}

vector<idx_t> LogicalVgiStreamingWindow::GetTableIndex() const {
	return vector<idx_t> {window_index};
}

string LogicalVgiStreamingWindow::GetName() const {
	return "VGI_STREAMING_WINDOW";
}

string LogicalVgiStreamingWindow::GetExtensionName() const {
	return "vgi_streaming_window";
}

void LogicalVgiStreamingWindow::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

void LogicalVgiStreamingWindow::Serialize(Serializer & /*serializer*/) const {
	throw NotImplementedException(
	    "LogicalVgiStreamingWindow serialization is not supported "
	    "(plan caching across processes is out of scope for v1)");
}

PhysicalOperator &LogicalVgiStreamingWindow::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	estimated_cardinality = EstimateCardinality(context);

	// Plan the child first so its output rows are pipelined into us.
	auto &child_plan = planner.CreatePlan(*children[0]);

	// Output types: child types followed by one type per window expression.
	vector<LogicalType> out_types;
	out_types.reserve(types.size());
	out_types.insert(out_types.end(), types.begin(), types.end());

	auto &op = planner.Make<PhysicalVgiStreamingWindow>(std::move(out_types), std::move(expressions),
	                                                     estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

// ============================================================================
// Operator state
// ============================================================================

namespace {

// One streaming session per window expression in the operator. Created
// lazily on the first Execute call once we know the input schema.
//
// Cleanup paths:
//   - Success path: FinalExecute() runs aggregate_streaming_close
//     synchronously through the active ClientContext and clears
//     sessions_opened.
//   - Cancel / error path: FinalExecute() never runs, only the destructor
//     does. The destructor cannot reach a ClientContext (may run on a
//     task-scheduler thread mid-teardown), so it hands each open session
//     to VgiCancelDispatcher which dispatches aggregate_streaming_close
//     off-thread on its bot Connection. Without this, every cancelled /
//     errored streaming-window query would leak per-session state on a
//     pooled subprocess worker.
struct VgiStreamingWindowOperatorState : public OperatorState {
	bool sessions_opened = false;
	vector<VgiStreamingSession> sessions;        // parallel to select_list
	vector<const VgiAggregateBindData *> bind_datas; // pointer aliases; lifetime owned by Expressions
	// Keep-alive of the per-session attach parameters for the destructor.
	// Parallel to sessions: attach_params_per_expr[i] backs sessions[i].
	// Captured at OpenSessions time because by destructor time the
	// owning bind_data may already be gone.
	vector<std::shared_ptr<VgiAttachParameters>> attach_params_per_expr;
	// Per-expression Arrow input schema:
	//   [partition keys..., order keys..., value cols-for-this-expr...]
	// All expressions in the same operator share the same partition/order
	// keys (DuckDB's planner separates by partition equivalence into different
	// LogicalWindow nodes), but value-col types differ per expression.
	vector<std::shared_ptr<arrow::Schema>> chunk_input_schema_per_expr;
	idx_t partition_key_count = 0;
	idx_t order_key_count = 0;
	// DatabaseInstance pointer captured at OpenSessions time so the
	// destructor can locate the cancel dispatcher without a ClientContext.
	// Plain pointer is safe: DatabaseInstance outlives all queries running
	// against it, and OperatorState is destroyed before DatabaseInstance.
	DatabaseInstance *db = nullptr;

	~VgiStreamingWindowOperatorState() override {
		// FinalExecute clears sessions_opened on the success path; if it's
		// still true here we're tearing down via a cancel / error path.
		if (!sessions_opened || !db) {
			return;
		}
		auto *dispatcher = vgi::FindVgiCancelDispatcher(*db);
		if (!dispatcher) {
			return;
		}
		for (idx_t i = 0; i < sessions.size(); i++) {
			vgi::StreamingCloseRequest req;
			req.attach_params = attach_params_per_expr[i];
			req.function_name = std::move(sessions[i].function_name);
			req.execution_id = std::move(sessions[i].execution_id);
			req.attach_opaque_data = std::move(sessions[i].attach_opaque_data);
			// Saturation / shutdown returns false; nothing actionable here.
			(void)dispatcher->EnqueueStreamingClose(std::move(req));
		}
		sessions_opened = false;
	}
};

struct VgiStreamingWindowGlobalState : public GlobalOperatorState {};

} // anonymous namespace

unique_ptr<GlobalOperatorState>
PhysicalVgiStreamingWindow::GetGlobalOperatorState(ClientContext & /*context*/) const {
	return make_uniq<VgiStreamingWindowGlobalState>();
}

unique_ptr<OperatorState> PhysicalVgiStreamingWindow::GetOperatorState(ExecutionContext & /*context*/) const {
	return make_uniq<VgiStreamingWindowOperatorState>();
}

// ============================================================================
// PhysicalVgiStreamingWindow
// ============================================================================

PhysicalVgiStreamingWindow::PhysicalVgiStreamingWindow(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                         vector<unique_ptr<Expression>> select_list_p,
                                                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list_p)) {
}

// Open one streaming session per window expression. Called on the first
// Execute call so we know the input chunk's schema.
//
// Partial-failure handling: if VgiAggregateStreamingOpen throws on the Nth
// expression, the [0..N-1] sessions are already open on the worker. The
// catch block closes them synchronously (best-effort, logging suppressed)
// and rethrows; the worker's per-session state never leaks here.
static void OpenSessions(ClientContext &context, const PhysicalVgiStreamingWindow &op,
                          DataChunk &first_input, VgiStreamingWindowOperatorState &state) {
	state.sessions.reserve(op.select_list.size());
	state.bind_datas.reserve(op.select_list.size());
	state.attach_params_per_expr.reserve(op.select_list.size());
	state.chunk_input_schema_per_expr.reserve(op.select_list.size());
	// Capture DatabaseInstance for the destructor's dispatcher lookup.
	state.db = context.db.get();

	try {
		for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); expr_idx++) {
			auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
			if (!wexpr.bind_info) {
				throw IOException("VGI streaming window: window expression has no bind_info");
			}
			auto &bind_data = wexpr.bind_info->Cast<VgiAggregateBindData>();

			const idx_t pkc = wexpr.partitions.size();
			const idx_t okc = wexpr.orders.size();
			// Planner groups expressions sharing partition/order keys into
			// the same LogicalWindow node, so all expressions in one operator
			// see the same (pkc, okc). Assert defensively.
			if (expr_idx == 0) {
				state.partition_key_count = pkc;
				state.order_key_count = okc;
			} else if (state.partition_key_count != pkc || state.order_key_count != okc) {
				throw InternalException(
				    "VGI streaming window: planner produced expressions with mismatched "
				    "partition/order key counts in the same node "
				    "(expr 0: pkc=%llu okc=%llu, expr %llu: pkc=%llu okc=%llu)",
				    (unsigned long long)state.partition_key_count,
				    (unsigned long long)state.order_key_count,
				    (unsigned long long)expr_idx, (unsigned long long)pkc,
				    (unsigned long long)okc);
			}

			// Build input_schema describing the columns shipped per chunk for
			// THIS expression: partition keys, order keys, this expr's value cols.
			vector<LogicalType> col_types;
			vector<string> col_names;
			col_types.reserve(pkc + okc + wexpr.children.size());
			col_names.reserve(pkc + okc + wexpr.children.size());

			for (idx_t i = 0; i < pkc; i++) {
				col_types.push_back(wexpr.partitions[i]->return_type);
				col_names.push_back(StringUtil::Format("__pk_%llu", static_cast<unsigned long long>(i)));
			}
			for (idx_t i = 0; i < okc; i++) {
				col_types.push_back(wexpr.orders[i].expression->return_type);
				col_names.push_back(StringUtil::Format("__ok_%llu", static_cast<unsigned long long>(i)));
			}
			for (idx_t i = 0; i < wexpr.children.size(); i++) {
				col_types.push_back(wexpr.children[i]->return_type);
				col_names.push_back(StringUtil::Format("__val_%llu", static_cast<unsigned long long>(i)));
			}

			auto input_schema = BuildArrowSchemaFromDuckDB(context, col_types, col_names);

			// Open BEFORE pushing the bookkeeping vectors so that on throw
			// the size of state.sessions == size of state.bind_datas ==
			// size of state.chunk_input_schema_per_expr, and the catch
			// block can iterate state.sessions to close exactly what's open.
			auto session = VgiAggregateStreamingOpen(context, bind_data, input_schema,
			                                          static_cast<int64_t>(pkc),
			                                          static_cast<int64_t>(okc));
			state.bind_datas.push_back(&bind_data);
			state.attach_params_per_expr.push_back(bind_data.attach_params);
			state.chunk_input_schema_per_expr.push_back(std::move(input_schema));
			state.sessions.push_back(std::move(session));
		}
	} catch (...) {
		// Close anything we already opened. Best-effort; logging disabled
		// because we may be unwinding through a logging-aware path. Each
		// individual close is wrapped so one failure doesn't strand others.
		for (idx_t i = 0; i < state.sessions.size(); i++) {
			try {
				VgiAggregateStreamingClose(context, *state.bind_datas[i],
				                            state.sessions[i],
				                            /*enable_logging=*/false);
			} catch (...) {
				// swallow; rethrow original below
			}
		}
		state.sessions.clear();
		state.bind_datas.clear();
		state.attach_params_per_expr.clear();
		state.chunk_input_schema_per_expr.clear();
		throw;
	}

	state.sessions_opened = true;
}

// Project a chunk into the streaming-input layout: partition cols, order
// cols, value cols (in that order). Each expression gets its own
// ExpressionExecutor so the per-expression result Vectors land in the
// caller-provided ``projected`` DataChunk.
static void ProjectInputInto(ClientContext &context, const BoundWindowExpression &wexpr,
                              DataChunk &input, DataChunk &projected) {
	idx_t out_idx = 0;
	auto eval = [&](const Expression &expr) {
		ExpressionExecutor exec(context, expr);
		exec.ExecuteExpression(input, projected.data[out_idx++]);
	};
	for (auto &p : wexpr.partitions) {
		eval(*p);
	}
	for (auto &o : wexpr.orders) {
		eval(*o.expression);
	}
	for (auto &c : wexpr.children) {
		eval(*c);
	}
	projected.SetCardinality(input.size());
}

OperatorResultType PhysicalVgiStreamingWindow::Execute(ExecutionContext &context, DataChunk &input,
                                                        DataChunk &chunk, GlobalOperatorState & /*gstate*/,
                                                        OperatorState &state_p) const {
	auto &state = state_p.Cast<VgiStreamingWindowOperatorState>();
	auto &client_context = context.client;

	if (input.size() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	if (!state.sessions_opened) {
		OpenSessions(client_context, *this, input, state);
	}

	const idx_t input_width = input.ColumnCount();
	chunk.SetCardinality(input.size());

	// 1. Pass through input columns unchanged (zero-copy reference).
	for (idx_t i = 0; i < input_width; i++) {
		chunk.data[i].Reference(input.data[i]);
	}

	// 2. For each window expression, project + ship + materialise the result
	//    column. Ships one streaming_chunk RPC per upstream DataChunk; if the
	//    per-RPC overhead becomes a bottleneck, the right place to batch is
	//    *upstream* of this operator (larger DataChunk size on the source) or
	//    *downstream of the worker* (worker-side pickle/IO amortisation).
	//    Buffering here was prototyped and reverted — the gain on
	//    portfolio_agg-shaped workloads was overshadowed by per-row Python
	//    work in streaming_chunk dominating wall time.
	for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		auto &wexpr = select_list[expr_idx]->Cast<BoundWindowExpression>();
		auto &session = state.sessions[expr_idx];
		auto &bind_data = *state.bind_datas[expr_idx];

		vector<LogicalType> col_types;
		col_types.reserve(wexpr.partitions.size() + wexpr.orders.size() + wexpr.children.size());
		for (auto &p : wexpr.partitions) {
			col_types.push_back(p->return_type);
		}
		for (auto &o : wexpr.orders) {
			col_types.push_back(o.expression->return_type);
		}
		for (auto &c : wexpr.children) {
			col_types.push_back(c->return_type);
		}

		DataChunk projected;
		projected.Initialize(client_context, col_types);
		projected.SetCardinality(input.size());
		ProjectInputInto(client_context, wexpr, input, projected);

		auto arrow_batch = DataChunkToArrow(client_context, projected,
		                                     state.chunk_input_schema_per_expr[expr_idx]);
		auto result_batch = VgiAggregateStreamingChunk(client_context, bind_data, session, arrow_batch);

		// Convert the one-column result batch back into chunk's
		// (input_width + expr_idx) column.
		ArrowSchemaWrapper c_schema;
		ArrowTableSchema arrow_table;
		vector<LogicalType> out_types;
		vector<string> out_names;
		ArrowSchemaToDuckDBTypes(client_context, result_batch->schema(), c_schema, arrow_table, out_types, out_names);

		auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
		ExportRecordBatch(result_batch, *chunk_wrapper);
		ArrowScanLocalState scan_state(std::move(chunk_wrapper), client_context);

		DataChunk result_chunk;
		result_chunk.Initialize(client_context, {wexpr.return_type});
		result_chunk.SetCardinality(input.size());
		ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), result_chunk, false);

		// Physical copy (mirrors aggregate_window_impl.cpp). Avoids any
		// shared-lifetime question between scan_state's arrow buffers and
		// the outgoing ``chunk`` once this iteration goes out of scope.
		VectorOperations::Copy(result_chunk.data[0], chunk.data[input_width + expr_idx],
		                       input.size(), 0, 0);
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType PhysicalVgiStreamingWindow::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                     GlobalOperatorState & /*gstate*/,
                                                                     OperatorState &state_p) const {
	auto &state = state_p.Cast<VgiStreamingWindowOperatorState>();

	if (state.sessions_opened) {
		for (idx_t i = 0; i < state.sessions.size(); i++) {
			try {
				VgiAggregateStreamingClose(context.client, *state.bind_datas[i], state.sessions[i]);
			} catch (const std::exception &e) {
				VGI_STDERR_DEBUG("[VGI] streaming_close raised: %s\n", e.what());
				// Swallow; we're tearing down.
			}
		}
		state.sessions_opened = false;
	}

	chunk.SetCardinality(0);
	return OperatorFinalizeResultType::FINISHED;
}

string PhysicalVgiStreamingWindow::GetName() const {
	return "VGI_STREAMING_WINDOW";
}

InsertionOrderPreservingMap<string> PhysicalVgiStreamingWindow::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string exprs;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			exprs += "\n";
		}
		exprs += select_list[i]->ToString();
	}
	result["__expressions__"] = exprs;
	return result;
}

} // namespace vgi
} // namespace duckdb
