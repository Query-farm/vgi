// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"

#include "vgi_platform.hpp"

#include <cerrno>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#if VGI_POSIX_TRANSPORT
#include <signal.h>
#endif

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "query_farm_telemetry.hpp"
#include "vgi_telemetry.hpp"
#include "vgi_multi_scan_rewriter.hpp"
#ifdef __EMSCRIPTEN__
#include "vgi_wasm_async_pool.hpp"
#endif
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_copy_from_impl.hpp"
#include "vgi_copy_to_impl.hpp"
#include "vgi_exception.hpp"
#include "vgi_protocol_constants.hpp"
#include "vgi_container_runtime.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_cookie_jar.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "vgi_aggregate_function_impl.hpp"
#include "vgi_oauth.hpp"
#include "vgi_profiling.hpp"
#include "vgi_secret_storage.hpp"
#include "vgi_lateral_batch_operator.hpp"
#include "vgi_streaming_window_operator.hpp"
#include "vgi_table_buffering_impl.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#ifndef __EMSCRIPTEN__
#include "vgi_subprocess.hpp"
#endif
#include "vgi_table_function.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"
#include "vgi_table_statistics_function.hpp"
#include "vgi_table_branches_function.hpp"
#include "vgi_function_arguments_function.hpp"
#include "vgi_clear_cache.hpp"
#include "vgi_result_cache.hpp"
#include "vgi_companion_catalogs.hpp"
#include "vgi_github_functions.hpp"
#include "vgi_worker_pool_functions.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

// Forward declarations — functions defined below after VgiStorageExtension class
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input);
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options);
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog);

// ============================================================================
// VgiJoinOptimizer — auto-raise InFilter threshold for queries with VGI scans
// ============================================================================
// When DuckDB joins a local table against a VGI remote table, the hash join
// can push an InFilter containing the build-side's distinct key values to the
// probe-side scan. DuckDB's default threshold (dynamic_or_filter_threshold=50)
// is too low for remote scans where network savings justify sending more keys.
// This optimizer raises the threshold to vgi_join_keys_threshold when a VGI scan
// is detected in the plan.

class VgiJoinOptimizer : public OptimizerExtension {
public:
	VgiJoinOptimizer() {
		pre_optimize_function = Optimize;
	}

private:
	static bool IsVgiScan(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			return get.function.function == vgi::VgiTableFunctionScan;
		}
		return false;
	}

	static bool SubtreeContainsVgiScan(LogicalOperator &op) {
		if (IsVgiScan(op)) {
			return true;
		}
		for (auto &child : op.children) {
			if (SubtreeContainsVgiScan(*child)) {
				return true;
			}
		}
		return false;
	}

	//! Check if the plan has a comparison join where any child subtree contains a VGI scan.
	//! Only raises the threshold when there's an actual join involving VGI — a plain
	//! SELECT * FROM vgi_table doesn't trigger it.
	static bool HasJoinWithVgiScan(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			for (auto &child : op.children) {
				if (SubtreeContainsVgiScan(*child)) {
					return true;
				}
			}
		}
		for (auto &child : op.children) {
			if (HasJoinWithVgiScan(*child)) {
				return true;
			}
		}
		return false;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		if (!HasJoinWithVgiScan(*plan)) {
			return;
		}

		Value threshold_val;
		if (!input.context.TryGetCurrentSetting("vgi_join_keys_threshold", threshold_val) || threshold_val.IsNull()) {
			return;
		}
		auto vgi_threshold = threshold_val.GetValue<idx_t>();
		if (vgi_threshold == 0) {
			return; // disabled
		}

		// Only raise, never lower a user-set threshold
		auto current = Settings::Get<DynamicOrFilterThresholdSetting>(input.context);
		if (current < vgi_threshold) {
			auto &client_config = ClientConfig::GetConfig(input.context);
			client_config.user_settings.SetUserSetting(DynamicOrFilterThresholdSetting::SettingIndex,
			                                           Value::UBIGINT(vgi_threshold));
		}
	}
};

// ============================================================================
// VgiStreamingWindowOptimizer — rewrite eligible LogicalWindow → streaming op
// ============================================================================
// Walks the plan post-optimize. For each LogicalWindow whose every window
// expression is a VGI aggregate with streaming_partitioned=true and a
// cumulative frame (UNBOUNDED PRECEDING -> CURRENT ROW), replace with our
// custom LogicalVgiStreamingWindow. Otherwise leave the LogicalWindow alone
// (it falls through to PhysicalWindow as today).
//
// All-or-nothing in v1: if any expression is ineligible, the whole node
// stays a regular LogicalWindow. Mixed-eligibility splitting is a v2 task.

class VgiStreamingWindowOptimizer : public OptimizerExtension {
public:
	VgiStreamingWindowOptimizer() {
		optimize_function = Optimize;
	}

private:
	static bool IsCumulativeFrame(const BoundWindowExpression &wexpr) {
		if (wexpr.start != WindowBoundary::UNBOUNDED_PRECEDING) {
			return false;
		}
		switch (wexpr.end) {
		case WindowBoundary::CURRENT_ROW_RANGE:
		case WindowBoundary::CURRENT_ROW_ROWS:
		case WindowBoundary::CURRENT_ROW_GROUPS:
			return true;
		default:
			return false;
		}
	}

	static bool IsStreamingEligible(const Expression &expr) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_WINDOW) {
			return false;
		}
		auto &wexpr = expr.Cast<BoundWindowExpression>();
		if (!wexpr.aggregate) {
			return false;
		}
		auto info = wexpr.aggregate->function_info.get();
		if (!info) {
			return false;
		}
		auto vgi_info = dynamic_cast<vgi::VgiAggregateFunctionInfo *>(info);
		if (!vgi_info || !vgi_info->streaming_partitioned) {
			return false;
		}
		// Streaming-open currently ships an empty Arguments envelope; any
		// const positional would be silently dropped on the wire. Punt
		// these to PhysicalWindow until the const-arg pass-through is
		// wired (see comment in BuildStreamingOpenRequest).
		for (bool is_const : vgi_info->positional_is_const) {
			if (is_const) {
				return false;
			}
		}
		if (!IsCumulativeFrame(wexpr)) {
			return false;
		}
		// EXCLUDE clauses produce multiple subframes per row — punt for v1.
		if (wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
			return false;
		}
		// DISTINCT, FILTER, ORDER BY in args — punt for v1, fall through to
		// PhysicalWindow which handles them.
		if (wexpr.distinct || wexpr.filter_expr || !wexpr.arg_orders.empty()) {
			return false;
		}
		return true;
	}

	static void Rewrite(unique_ptr<LogicalOperator> &op) {
		// Recurse first so children are rewritten before we look at this node.
		for (auto &child : op->children) {
			Rewrite(child);
		}
		if (op->type != LogicalOperatorType::LOGICAL_WINDOW) {
			return;
		}
		auto &window = op->Cast<LogicalWindow>();
		if (window.expressions.empty()) {
			return;
		}
		for (auto &expr : window.expressions) {
			if (!IsStreamingEligible(*expr)) {
				return; // leave the LogicalWindow alone
			}
		}

		// All expressions eligible — swap in the streaming operator.
		auto streaming_op = make_uniq<vgi::LogicalVgiStreamingWindow>(window.window_index);
		streaming_op->expressions = std::move(window.expressions);
		streaming_op->children = std::move(window.children);

		// Restore the sort PhysicalWindow would have done internally.  Unlike
		// PhysicalWindow, the VGI streaming operator has no internal sort: it
		// ships chunks to the worker in arrival order and the worker computes the
		// running aggregate over rows in exactly that order.  Without an explicit
		// LogicalOrder on (PARTITION BY, ORDER BY), an OVER (... ORDER BY y) over
		// an unsorted input silently produces wrong cumulative values.  Insert a
		// binding-transparent LogicalOrder (empty projection_map passes the
		// child's bindings/types through unchanged, so streaming_op's bound
		// expressions keep resolving and the plan survives VerifyPlan).  All
		// expressions in one node share equivalent partitions/orders (enforced by
		// eligibility + the streaming operator's OpenSessions asserts), so
		// expressions[0] is representative.
		auto &wexpr = streaming_op->expressions[0]->Cast<BoundWindowExpression>();
		if (!wexpr.partitions.empty() || !wexpr.orders.empty()) {
			vector<BoundOrderByNode> sort_orders;
			// Group partitions first (any total order over the key suffices to make
			// each partition contiguous); then the within-partition window order,
			// copied verbatim so ASC/DESC + NULLS placement match the frame.
			for (auto &p : wexpr.partitions) {
				sort_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, p->Copy());
			}
			for (auto &o : wexpr.orders) {
				sort_orders.push_back(o.Copy());
			}
			auto order = make_uniq<LogicalOrder>(std::move(sort_orders));
			order->children.push_back(std::move(streaming_op->children[0]));
			order->ResolveOperatorTypes();
			streaming_op->children[0] = std::move(order);
		}

		streaming_op->ResolveOperatorTypes();

		VGI_STDERR_DEBUG("[VGI] LogicalWindow rewritten -> LogicalVgiStreamingWindow "
		                 "(window_index=%llu, %zu expression(s))\n",
		                 static_cast<unsigned long long>(window.window_index),
		                 streaming_op->expressions.size());

		op = std::move(streaming_op);
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_streaming_window", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		Rewrite(plan);
	}
};

// ============================================================================
// VgiTableBufferingRewriter — rewrite LogicalGet → LogicalVgiTableBufferingFunction
// ============================================================================
// Walks the plan post-built-in-optimizers. For each LogicalGet whose bind_data
// is a VgiTableInOutBindData with table_buffering=true, replaces it with a
// LogicalVgiTableBufferingFunction carrying the same return types/names and
// the bind_data ownership. The LATERAL decorrelation pass has already run by
// this point, so any correlated subqueries are already DELIM_JOINs and our
// LogicalGet appears as a plain (non-projected_input) node.

class VgiTableBufferingRewriter : public OptimizerExtension {
public:
	VgiTableBufferingRewriter() {
		optimize_function = Optimize;
	}

private:
	static bool IsBufferedTableInOutGet(LogicalGet &get) {
		if (!get.bind_data) {
			return false;
		}
		auto *bd = dynamic_cast<vgi::VgiTableInOutBindData *>(get.bind_data.get());
		return bd && bd->table_buffering;
	}

	static void Rewrite(unique_ptr<LogicalOperator> &op, ClientContext &context) {
		// Recurse first so leaf-level rewrites finish before we look at this node.
		for (auto &child : op->children) {
			Rewrite(child, context);
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto &get = op->Cast<LogicalGet>();
		if (!IsBufferedTableInOutGet(get)) {
			return;
		}
		// The LogicalGet's child carries the table-input subquery for an
		// in-out function. Move it into our new extension op.
		D_ASSERT(get.children.size() <= 1);

		// LogicalGet::ResolveTypes() (duckdb/src/planner/operator/logical_get.cpp:162-177)
		// has already narrowed `get.types` to match column_ids/projection_ids
		// under projection pushdown. Pass the narrowed types as our op's
		// return_types so upstream LogicalProjection/LogicalFilter bindings
		// resolve correctly. The full worker schema lives separately in
		// all_column_types/all_column_names for filter serialization and
		// for telling the worker what the projected output_schema should be.
		auto post_proj_types = get.types;
		auto post_proj_names = vector<string>();
		const auto &all_col_ids = get.GetColumnIds();
		if (!get.projection_ids.empty()) {
			post_proj_names.reserve(get.projection_ids.size());
			for (auto proj_id : get.projection_ids) {
				const auto &idx = all_col_ids[proj_id];
				post_proj_names.push_back(idx.IsVirtualColumn() ? string("__virtual__")
				                                                : get.names[idx.GetPrimaryIndex()]);
			}
		} else if (!all_col_ids.empty()) {
			post_proj_names.reserve(all_col_ids.size());
			for (const auto &idx : all_col_ids) {
				post_proj_names.push_back(idx.IsVirtualColumn() ? string("__virtual__")
				                                                : get.names[idx.GetPrimaryIndex()]);
			}
		} else {
			post_proj_names = get.names;
		}
		D_ASSERT(post_proj_types.size() == post_proj_names.size());

		auto rewritten = make_uniq<vgi::LogicalVgiTableBufferingFunction>(
		    get.table_index, std::move(post_proj_types), std::move(post_proj_names),
		    std::move(get.bind_data));
		rewritten->children = std::move(get.children);

		// Capture pushdown state. The streaming pure-table path captures the
		// same data at InitGlobal time via TableFunctionInitInput; the buffered
		// path goes through a custom PhysicalOperator with no InitInput, so the
		// rewriter is the analogous capture point.
		rewritten->all_column_types = get.returned_types;
		rewritten->all_column_names = get.names;

		// Read pushdown capabilities from bind_data (echoed from Meta at
		// catalog-load time). The catalog set's HasTableInput branch already
		// gated `table_func.projection_pushdown=true` on this same flag, so
		// `column_ids` only contains real column indices (rather than a
		// virtual GetAnyColumn placeholder) when this is true.
		const auto *bd_for_caps =
		    static_cast<const vgi::VgiTableInOutBindData *>(rewritten->bind_data.get());
		const bool worker_supports_projection =
		    bd_for_caps && bd_for_caps->projection_pushdown;

		// projection_ids: worker-schema indices of the columns the parent op
		// actually wants. Match the streaming pure-table convention
		// (vgi_table_function_impl.cpp:1023-1042): when the function declares
		// projection_pushdown=true, use ``get.GetColumnIds()`` as the
		// projection (NOT ``get.projection_ids`` — that's a secondary
		// projection on top of column_ids; rare in practice). The streaming
		// path also handles get.projection_ids for completeness.
		if (worker_supports_projection && !all_col_ids.empty()) {
			rewritten->projection_pushdown = true;
			if (get.projection_ids.empty()) {
				rewritten->projection_ids.reserve(all_col_ids.size());
				for (const auto &idx : all_col_ids) {
					rewritten->projection_ids.push_back(static_cast<int32_t>(
					    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                          : idx.GetPrimaryIndex()));
				}
			} else {
				rewritten->projection_ids.reserve(get.projection_ids.size());
				for (auto proj_id : get.projection_ids) {
					const auto &idx = all_col_ids[proj_id];
					rewritten->projection_ids.push_back(static_cast<int32_t>(
					    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                          : idx.GetPrimaryIndex()));
				}
			}
		}
		// column_ids: what TableFilter col indices reference. Empty when no
		// filters were pushed. VgiSerializeFilters indexes into
		// all_column_names through this list.
		rewritten->column_ids.reserve(all_col_ids.size());
		for (const auto &idx : all_col_ids) {
			rewritten->column_ids.push_back(static_cast<int32_t>(
			    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID : idx.GetPrimaryIndex()));
		}

		// Serialize filters via the shared utility (declared in
		// vgi_table_function_impl.hpp). Catches and swallows
		// InvalidInputException — same convention as the streaming path:
		// unsupported filter types skip pushdown and DuckDB will filter
		// locally above us.
		if (!get.table_filters.filters.empty()) {
			try {
				auto bd = rewritten->bind_data.get();
				auto bd_typed = static_cast<vgi::VgiTableInOutBindData *>(bd);
				auto worker_path = bd_typed && bd_typed->attach_params
				                       ? bd_typed->attach_params->worker_path()
				                       : std::string{};
				// VgiSerializeFilters takes column_ids as the DuckDB-side post-
				// projection list — what filter col_idx indexes into. Use the
				// raw column_t form via GetColumnIds() which mirrors what the
				// streaming path's InitGlobal uses (input.column_ids).
				vector<column_t> col_ids_raw;
				col_ids_raw.reserve(all_col_ids.size());
				for (const auto &idx : all_col_ids) {
					col_ids_raw.push_back(idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                                            : idx.GetPrimaryIndex());
				}
				// NOTE: this is the only site that produces COLUMN_IDENTIFIER_ROW_ID
				// without passing a rowid_column_name to VgiSerializeFilters, so a
				// rowid filter here would serialize with the literal sentinel name.
				// This is currently unreachable: multi-branch tables rewrite to a
				// UNION ALL of branch functions (not a single rowid scan), and
				// late_materialization is gated to the catalog-table scan path. If
				// rowid pushdown is ever added here, thread the rowid field name
				// through (see VgiTableFunctionBindData::rowid_column_name).
				auto serialized = vgi::VgiSerializeFilters(
				    context, col_ids_raw, &get.table_filters,
				    rewritten->all_column_names, worker_path);
				rewritten->pushdown_filters = std::move(serialized.filter_bytes);
				rewritten->join_keys_buffers = std::move(serialized.join_keys_buffers);
			} catch (const InvalidInputException &) {
				// Unsupported filter — leave pushdown_filters null; DuckDB
				// will filter locally above the operator.
			}
		}

		// Pre-build the EXPLAIN summary while we still have the raw
		// TableFilter objects in scope (the wire-serialized form is opaque
		// to the C++ side). Reused later by PhysicalVgiTableBufferingFunction
		// ::ParamsToString.
		if (rewritten->projection_pushdown || !get.table_filters.filters.empty()) {
			std::string summary;
			if (rewritten->projection_pushdown) {
				summary += "Projections: [";
				for (size_t i = 0; i < rewritten->projection_ids.size(); ++i) {
					if (i > 0) {
						summary += ", ";
					}
					auto col_idx = rewritten->projection_ids[i];
					if (col_idx >= 0 && static_cast<size_t>(col_idx) < rewritten->all_column_names.size()) {
						summary += rewritten->all_column_names[col_idx];
					} else {
						summary += "<unknown>";
					}
				}
				summary += "]";
			}
			if (!get.table_filters.filters.empty()) {
				if (!summary.empty()) {
					summary += " | ";
				}
				summary += "Filters: ";
				bool first = true;
				// table_filters.filters keys are indices into column_ids (the
				// post-projection list), not direct worker-schema indices.
				// Resolve through column_ids → worker-schema → all_column_names.
				for (auto &kv : get.table_filters.filters) {
					if (!first) {
						summary += " AND ";
					}
					first = false;
					std::string name = "<unknown>";
					if (kv.first < all_col_ids.size()) {
						const auto &idx = all_col_ids[kv.first];
						if (!idx.IsVirtualColumn() &&
						    idx.GetPrimaryIndex() < rewritten->all_column_names.size()) {
							name = rewritten->all_column_names[idx.GetPrimaryIndex()];
						}
					}
					summary += kv.second->ToString(name);
				}
			}
			rewritten->explain_summary = std::move(summary);
		}

		rewritten->ResolveOperatorTypes();

		VGI_STDERR_DEBUG(
		    "[VGI] LogicalGet rewritten -> LogicalVgiTableBufferingFunction "
		    "(table_index=%llu, %zu output column(s), projection_pushdown=%d, "
		    "filter_bytes=%s)\n",
		    static_cast<unsigned long long>(get.table_index), rewritten->return_types.size(),
		    rewritten->projection_pushdown ? 1 : 0,
		    rewritten->pushdown_filters ? "yes" : "no");

		op = std::move(rewritten);
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		// Gated on a session setting so an emergency-rollback path exists if
		// the rewriter regresses against a real workload. When disabled, the
		// loud-failure asserts in VgiTableInOutFunction/VgiTableInOutFinalize
		// will catch any table_buffering function call so users see a clear
		// error rather than the pre-fix UNION-ALL corruption.
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_table_buffering", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		Rewrite(plan, input.context);
	}
};

// ============================================================================
// VgiRequiredFiltersOptimizer — enforce Table.required_filters
// ============================================================================
// Walks the plan post-FilterPushdown. For each LogicalGet whose underlying
// table is a VgiTableEntry with a non-empty `required_filters`,
// collects the dotted-path column references touched by any filter expression
// in scope (the LogicalGet's table_filters plus any parent LogicalFilter
// expressions referencing this LogicalGet's table_index). Throws
// BinderException listing any required path that is not satisfied (prefix-
// inclusive: a present `bbox` satisfies all of `bbox.xmin` / `.xmax` / etc.).
//
// Phase: optimize_function (post-optimize), so DuckDB's FilterPushdown has
// already settled filters into LogicalGet::table_filters.
//
// Cost: O(plan_size) for tables that have no requirements (empty list →
// fast return after the cast); a small TableFilter tree walk + expression
// walk per LogicalGet that does have requirements.
class VgiRequiredFiltersOptimizer : public OptimizerExtension {
public:
	VgiRequiredFiltersOptimizer() {
		optimize_function = Optimize;
	}

private:
	// Append a dotted path to the present set.
	static void EmitPath(unordered_set<std::string> &out, const std::string &top,
	                     const std::string &suffix) {
		if (suffix.empty()) {
			out.insert(top);
		} else {
			out.insert(top + "." + suffix);
		}
	}

	// Recursive walk over a TableFilter sub-tree, emitting dotted paths for
	// every column reference. `top_col` is the LogicalGet column name; `suffix`
	// grows as we descend into StructFilters. Mirrors the iteration shape of
	// VgiSerializeFilters' JSON walker in vgi_table_function_impl.cpp:572+ but
	// emits strings instead of building JSON.
	static void WalkTableFilter(const TableFilter &filter, const std::string &top_col,
	                            const std::string &suffix, unordered_set<std::string> &out) {
		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON:
		case TableFilterType::IS_NULL:
		case TableFilterType::IS_NOT_NULL:
		case TableFilterType::IN_FILTER:
		case TableFilterType::EXPRESSION_FILTER:
			EmitPath(out, top_col, suffix);
			return;

		case TableFilterType::CONJUNCTION_AND: {
			auto &conj = filter.Cast<ConjunctionAndFilter>();
			for (auto &child : conj.child_filters) {
				WalkTableFilter(*child, top_col, suffix, out);
			}
			return;
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &conj = filter.Cast<ConjunctionOrFilter>();
			for (auto &child : conj.child_filters) {
				WalkTableFilter(*child, top_col, suffix, out);
			}
			return;
		}
		case TableFilterType::STRUCT_EXTRACT: {
			auto &sf = filter.Cast<StructFilter>();
			std::string deeper = suffix.empty() ? sf.child_name : (suffix + "." + sf.child_name);
			if (sf.child_filter) {
				WalkTableFilter(*sf.child_filter, top_col, deeper, out);
			} else {
				EmitPath(out, top_col, deeper);
			}
			return;
		}
		case TableFilterType::OPTIONAL_FILTER: {
			auto &opt = filter.Cast<OptionalFilter>();
			// A null child_filter is a placeholder/sentinel — typically a Top-N
			// OptionalFilter still waiting for a DynamicFilter value that never
			// materialised. It is NOT a constraint, so we emit nothing. Mirrors
			// the serializer at vgi_table_function_impl.cpp:699-703 which skips
			// the OptionalFilter entirely in this case (returns false).
			if (!opt.child_filter) {
				return;
			}
			// Skip the entire OptionalFilter when its subtree contains a
			// DynamicFilter — mirrors the serializer at
			// vgi_table_function_impl.cpp:714-716. The static portion of a
			// Top-N OptionalFilter (e.g. OR(IsNull, DynamicFilter)) is
			// machinery, not user intent: emitting presence here would let a
			// Top-N artifact spuriously satisfy a required_filters
			// requirement the user never wrote in their WHERE clause.
			if (vgi::VgiContainsDynamicFilter(*opt.child_filter)) {
				return;
			}
			WalkTableFilter(*opt.child_filter, top_col, suffix, out);
			return;
		}
		case TableFilterType::DYNAMIC_FILTER:
		case TableFilterType::BLOOM_FILTER: {
			// Values aren't known at optimize time, but the column reference
			// is — count it as presence so that join-derived filters satisfy
			// requirements as the user intends.
			EmitPath(out, top_col, suffix);
			return;
		}
		default:
			// Unknown future filter type — treat as presence so we don't
			// over-reject the user.
			EmitPath(out, top_col, suffix);
			return;
		}
	}

	// Resolve a BoundColumnRefExpression's binding.column_index (which is
	// post-projection — an index into LogicalGet::column_ids) to the original
	// column index in LogicalGet::names. Returns -1 for virtual columns or
	// out-of-range bindings.
	static int64_t ResolveOrigIndex(idx_t binding_col_idx,
	                                const vector<ColumnIndex> &col_ids,
	                                idx_t names_size) {
		if (binding_col_idx >= col_ids.size()) {
			return -1;
		}
		const auto &ci = col_ids[binding_col_idx];
		if (ci.IsVirtualColumn()) {
			return -1;
		}
		auto orig = ci.GetPrimaryIndex();
		if (orig >= names_size) {
			return -1;
		}
		return static_cast<int64_t>(orig);
	}

	// Walk an Expression for column refs whose binding resolves to the given
	// LogicalGet. Emit top-level column names (or `col.subfield` for the
	// `struct_extract(col, 'subfield')` shape that DuckDB uses to lower
	// `col.sub` in expressions). `col_ids` and `names` come from the
	// LogicalGet — `binding.column_index` is post-projection and must be
	// resolved through `col_ids` before indexing `names`.
	static void WalkExpression(const Expression &expr, idx_t target_table_index,
	                           const vector<ColumnIndex> &col_ids,
	                           const vector<std::string> &names,
	                           unordered_set<std::string> &out) {
		// Detect `struct_extract(<col_ref>, '<subfield>')`. DuckDB exposes this
		// either as a BoundFunctionExpression with function name "struct_extract"
		// (children = [col_ref, constant subfield index]) or as
		// BoundExtractFunction wrapper. We match by name on
		// BoundFunctionExpression and read the second child's constant value.
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &fn = expr.Cast<BoundFunctionExpression>();
			if (fn.function.name == "struct_extract" && fn.children.size() >= 2) {
				auto &col_arg = *fn.children[0];
				auto &sub_arg = *fn.children[1];
				if (col_arg.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    sub_arg.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &cref = col_arg.Cast<BoundColumnRefExpression>();
					if (cref.binding.table_index == target_table_index) {
						auto orig = ResolveOrigIndex(cref.binding.column_index, col_ids, names.size());
						if (orig >= 0) {
							auto &const_expr = sub_arg.Cast<BoundConstantExpression>();
							if (!const_expr.value.IsNull() &&
							    const_expr.value.type().id() == LogicalTypeId::VARCHAR) {
								out.insert(names[orig] + "." +
								           const_expr.value.GetValue<std::string>());
								// Don't descend into children — we already
								// captured the dotted path.
								return;
							}
						}
					}
				}
			}
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &cref = expr.Cast<BoundColumnRefExpression>();
			if (cref.binding.table_index == target_table_index) {
				auto orig = ResolveOrigIndex(cref.binding.column_index, col_ids, names.size());
				if (orig >= 0) {
					out.insert(names[orig]);
				}
			}
		}
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
			WalkExpression(child, target_table_index, col_ids, names, out);
		});
	}

	// Prefix-satisfaction: a required path is satisfied if itself OR any
	// shorter dotted prefix of it is in `present`. So a present `"bbox"`
	// satisfies all of `"bbox.xmin"`, `"bbox.xmax"`, etc. — the parent struct
	// filter is at least as constraining as the four-corner conjunction.
	static bool IsSatisfied(const std::string &required,
	                        const unordered_set<std::string> &present) {
		if (present.count(required)) {
			return true;
		}
		auto dot = required.rfind('.');
		while (dot != std::string::npos) {
			if (present.count(required.substr(0, dot))) {
				return true;
			}
			if (dot == 0) {
				break;
			}
			dot = required.rfind('.', dot - 1);
		}
		return false;
	}

	static std::string JoinPaths(const std::vector<std::string> &v) {
		std::string out;
		for (size_t i = 0; i < v.size(); ++i) {
			if (i) {
				out += ", ";
			}
			out += v[i];
		}
		return out;
	}

	// Render one OR-group for an error message: a singleton group as the bare
	// path (`ticker`); a multi-member group as `one of (ticker, cik)`.
	static std::string RenderGroup(const std::vector<std::string> &group) {
		if (group.size() == 1) {
			return group[0];
		}
		return "one of (" + JoinPaths(group) + ")";
	}

	// Join rendered groups with ", " for the required / missing lists.
	static std::string JoinGroups(const std::vector<std::vector<std::string>> &groups) {
		std::string out;
		for (size_t i = 0; i < groups.size(); ++i) {
			if (i) {
				out += ", ";
			}
			out += RenderGroup(groups[i]);
		}
		return out;
	}

	// Resolve "what VgiTableEntry owns this LogicalGet, if any". Two paths:
	//   (1) VGI-side scans (vgi_table_scan): GetTable() returns the entry.
	//   (2) Native delegation (marker bind_data): GetTable() returns nullptr,
	//       but bind_data is a VgiNativeDelegationMarkerBindData carrying the
	//       entry. The rewriter below will replace the marker after the check.
	static optional_ptr<VgiTableEntry> ResolveEntry(const LogicalGet &get) {
		if (auto tbl = get.GetTable()) {
			if (auto *vgi = dynamic_cast<VgiTableEntry *>(tbl.get())) {
				return vgi;
			}
		}
		if (get.bind_data) {
			if (auto *mark =
			        dynamic_cast<VgiNativeDelegationMarkerBindData *>(get.bind_data.get())) {
				return &mark->table.get();
			}
		}
		return nullptr;
	}

	// Run the requirement check against the LogicalGet's filters + parent
	// LogicalFilter expressions. Throws BinderException with the formatted
	// message on miss; returns silently on satisfy.
	static void EnforceRequirements(
	    const LogicalGet &get, VgiTableEntry &vgi,
	    const std::vector<unique_ptr<Expression>> &parent_exprs) {
		const auto &required = vgi.GetTableInfo().required_filters;
		if (required.empty()) {
			return;
		}

		unordered_set<std::string> present;

		// Source 1: filters pushed into the LogicalGet itself.
		// At post-optimize time a TableFilterSet is keyed by the *original*
		// table-schema column index (the ColumnIndex primary index) — see
		// TableFilterSet::PushFilter (duckdb/src/planner/table_filter.cpp),
		// which stores col_idx.GetPrimaryIndex() as the key, and
		// remove_unused_columns.cpp which reads it back as a primary index.
		// (The remap to a column_ids *position* happens later, at physical
		// planning in CreateTableFilterSet.) So the key indexes directly into
		// `get.names` (the full schema-ordered column list); do NOT route it
		// through column_ids — for native multi-file/Hive scans column_ids is
		// a non-identity permutation and the indirection lands on the wrong
		// column.
		const auto &col_ids = get.GetColumnIds();
		for (auto &kv : get.table_filters.filters) {
			if (kv.first >= get.names.size()) {
				// Virtual column (rowid etc.) — never a required path.
				continue;
			}
			WalkTableFilter(*kv.second, get.names[kv.first], "", present);
		}

		// Source 2: dynamic_filters (Top-N / runtime). The public API doesn't
		// expose per-column iteration, so we can't introspect *which* columns
		// have dynamic filters from here. Acceptable for v1: the typical
		// Overture bbox case is satisfied by static table_filters or by parent
		// LogicalFilter walks; if a worker has a real need to count dynamic
		// presence per-column, that's a follow-up. We do conservatively note
		// that filters exist by emitting nothing — leaving the strict check.

		// Source 3: parent LogicalFilter expressions in scope that reference
		// this LogicalGet's table_index. Catches range-join LogicalFilters
		// above the scan and other non-pushed-down predicates. Walk() has
		// already rewritten these through any intervening LogicalProjection,
		// so their column refs bind to this get's table_index.
		for (auto &expr_ptr : parent_exprs) {
			WalkExpression(*expr_ptr, get.table_index, col_ids, get.names, present);
		}

		// Compute unsatisfied groups: a group is satisfied when any one of its
		// member paths has a filter present; every group must be satisfied.
		std::vector<std::vector<std::string>> missing;
		for (const auto &group : required) {
			bool satisfied = false;
			for (const auto &member : group) {
				if (IsSatisfied(member, present)) {
					satisfied = true;
					break;
				}
			}
			if (!satisfied) {
				missing.push_back(group);
			}
		}
		if (missing.empty()) {
			return;
		}

		throw BinderException(
		    "Table '%s.%s.%s' requires WHERE filters on: %s. Missing: %s. "
		    "Add predicates targeting those columns (or a filter on a parent struct) "
		    "to avoid scanning the entire table.",
		    vgi.ParentCatalog().GetName(), vgi.ParentSchema().name, vgi.name,
		    JoinGroups(required), JoinGroups(missing));
	}

	// If the LogicalGet wraps a VgiNativeDelegationMarkerBindData, transfer
	// the pre-bound native TableFunction / bind_data / return shape stashed by
	// GetScanFunctionImpl into the LogicalGet in place. The LogicalGet was
	// constructed with the catalog's declared Table.columns; we now replace
	// returned_types/names with the native function's actual bind output so
	// downstream passes (physical planner, executor) see the true scan shape.
	static void RewriteMarkerToNative(ClientContext &context, LogicalGet &get) {
		auto *mark = dynamic_cast<VgiNativeDelegationMarkerBindData *>(get.bind_data.get());
		if (!mark) {
			return;
		}
		// Move out of the marker. The marker bind_data is replaced by the
		// native bind_data below, so this is the marker's last access.
		auto native_tf = std::move(mark->native_tf);
		auto native_bind = std::move(mark->native_bind);
		auto native_return_types = std::move(mark->native_return_types);
		auto native_return_names = std::move(mark->native_return_names);
		auto native_virtual_columns = std::move(mark->native_virtual_columns);
		auto function_name_log = mark->scan_result.function_name;
		auto table_name_log = mark->table.get().name;

		get.function = std::move(native_tf);
		get.bind_data = std::move(native_bind);
		get.returned_types = std::move(native_return_types);
		get.names = std::move(native_return_names);
		get.virtual_columns = std::move(native_virtual_columns);

		VGI_LOG(context, "vgi.scan_function.native_delegation_rewritten",
		        {{"table", table_name_log}, {"function", function_name_log}});
	}

	// Substitute every column ref that binds to `proj`'s output with a copy of
	// the projection's defining expression, so a filter accumulated *above* the
	// projection ends up referencing whatever the projection reads (ultimately
	// the LogicalGet). Mirrors DuckDB's own ReplaceProjectionBindings
	// (duckdb/src/optimizer/pushdown/pushdown_projection.cpp).
	static void RewriteThroughProjection(unique_ptr<Expression> &expr,
	                                     const LogicalProjection &proj) {
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    expr, [&](BoundColumnRefExpression &cref, unique_ptr<Expression> &slot) {
			    if (cref.binding.table_index == proj.table_index &&
			        cref.binding.column_index < proj.expressions.size()) {
				    slot = proj.expressions[cref.binding.column_index]->Copy();
			    }
		    });
	}

	static void Walk(ClientContext &context, LogicalOperator &op,
	                 std::vector<unique_ptr<Expression>> &parent_exprs) {
		// LogicalFilter: accumulate owned copies of its expressions before
		// descending — so any LogicalGet beneath us sees them. Pop on exit.
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &lf = op.Cast<LogicalFilter>();
			size_t pushed = 0;
			for (auto &expr : lf.expressions) {
				parent_exprs.emplace_back(expr->Copy());
				++pushed;
			}
			for (auto &child : op.children) {
				Walk(context, *child, parent_exprs);
			}
			for (size_t i = 0; i < pushed; ++i) {
				parent_exprs.pop_back();
			}
			return;
		}

		// LogicalProjection: rewrite the accumulated filter expressions through
		// the projection so their bindings reference the projection's input
		// (the get, or a deeper projection). Restore the originals on exit so
		// sibling subtrees are unaffected. Without this, a filter sitting above
		// an interposed projection (the common shape DuckDB pushdown produces)
		// binds to the projection's table_index and never matches the get.
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = op.Cast<LogicalProjection>();
			std::vector<unique_ptr<Expression>> saved = std::move(parent_exprs);
			parent_exprs.clear();
			for (auto &expr : saved) {
				auto rewritten = expr->Copy();
				RewriteThroughProjection(rewritten, proj);
				parent_exprs.emplace_back(std::move(rewritten));
			}
			for (auto &child : op.children) {
				Walk(context, *child, parent_exprs);
			}
			parent_exprs = std::move(saved);
			return;
		}

		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			if (auto vgi = ResolveEntry(get)) {
				// Run the requirement check FIRST — if it fails, we throw,
				// the plan is discarded, and the user sees a clean
				// BinderException without ever binding read_parquet.
				EnforceRequirements(get, *vgi, parent_exprs);
				// Then rewrite if this was a marker; no-op for VGI-side scans.
				RewriteMarkerToNative(context, get);
			}
		}

		for (auto &child : op.children) {
			Walk(context, *child, parent_exprs);
		}
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		std::vector<unique_ptr<Expression>> parent_exprs;
		Walk(input.context, *plan, parent_exprs);
	}
};

// Storage extension for VGI — also holds per-secret-type redact keys
class VgiStorageExtension : public StorageExtension {
public:
	explicit VgiStorageExtension(DatabaseInstance &db) : dispatcher_(db) {
		attach = VgiCatalogAttach;
		create_transaction_manager = CreateVgiTransactionManager;
	}

	vgi::VgiCancelDispatcher &GetCancelDispatcher() {
		return dispatcher_;
	}

	// Convenience: locate the VGI extension on a DatabaseInstance and
	// return its dispatcher. nullptr if the extension isn't registered.
	static vgi::VgiCancelDispatcher *FindCancelDispatcher(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto ext = StorageExtension::Find(config, "vgi");
		if (!ext) {
			return nullptr;
		}
		return &static_cast<VgiStorageExtension &>(*ext).dispatcher_;
	}

	// Thread-safe registry of redact keys per secret type name.
	// Populated during ATTACH after successful secret type registration.
	void SetRedactKeys(const std::string &secret_type_name, case_insensitive_set_t keys) {
		std::lock_guard<std::mutex> lock(redact_mutex_);
		redact_keys_[secret_type_name] = std::move(keys);
	}

	case_insensitive_set_t GetRedactKeys(const std::string &secret_type_name) const {
		std::lock_guard<std::mutex> lock(redact_mutex_);
		auto it = redact_keys_.find(secret_type_name);
		if (it != redact_keys_.end()) {
			return it->second;
		}
		return {};
	}

	// Locate the VGI storage extension on a DatabaseInstance. nullptr if absent.
	static VgiStorageExtension *Find(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto ext = StorageExtension::Find(config, "vgi");
		if (!ext) {
			return nullptr;
		}
		return static_cast<VgiStorageExtension *>(ext.get());
	}

	// Registry of Orchard remote secret providers, keyed by attached catalog
	// name. The SecretManager owns each VgiRemoteSecretStorage (no unload API);
	// these are non-owning pointers kept for diagnostics and best-effort detach.
	// Valid for the DB's lifetime (the storage objects never go away).
	void RegisterSecretProvider(const std::string &catalog_name, vgi::VgiRemoteSecretStorage *storage) {
		std::lock_guard<std::mutex> lock(secret_mutex_);
		secret_providers_[catalog_name] = storage;
	}

	vgi::VgiRemoteSecretStorage *FindSecretProvider(const std::string &catalog_name) {
		std::lock_guard<std::mutex> lock(secret_mutex_);
		auto it = secret_providers_.find(catalog_name);
		return it != secret_providers_.end() ? it->second : nullptr;
	}

	std::vector<std::pair<std::string, vgi::VgiRemoteSecretStorage *>> AllSecretProviders() {
		std::lock_guard<std::mutex> lock(secret_mutex_);
		std::vector<std::pair<std::string, vgi::VgiRemoteSecretStorage *>> out;
		out.reserve(secret_providers_.size());
		for (auto &kv : secret_providers_) {
			out.emplace_back(kv.first, kv.second);
		}
		return out;
	}

	// Auto-allocate a unique tie_break_offset for each secret provider, starting
	// at 100 (built-ins occupy 10/20, so a remote catch-all loses to local
	// secrets). Step 10 leaves room and never collides within a DB.
	int64_t NextSecretTieBreakOffset() {
		std::lock_guard<std::mutex> lock(secret_mutex_);
		int64_t off = next_secret_offset_;
		next_secret_offset_ += 10;
		return off;
	}

	// ---- COPY ... FROM format registry --------------------------------------
	// Tracks the VGI-registered COPY formats per DB so vgi_copy_formats() can
	// enumerate them and so ATTACH can detect collisions. The system-catalog
	// CopyFunction entry persists past DETACH, so entries here also persist
	// (matching the documented "DETACH can't unregister" limitation).
	//
	// Concurrency: callers performing the registry-check + system-catalog
	// create + record sequence hold CopyFormatMutex() across all three steps to
	// close the TOCTOU between concurrent ATTACHes (see VgiCatalogAttach).
	struct CopyFormatEntry {
		std::string catalog_name;   // owning catalog attach alias (may later be detached)
		std::string registered_name; // scoped name registered in the system catalog (what users type)
		vgi::VgiCopyFromFormatInfo info; // info.format_name is the worker's bare advertised name
	};

	std::mutex &CopyFormatMutex() {
		return copy_format_mutex_;
	}

	// Caller must hold CopyFormatMutex(). key = lowercase format name.
	const CopyFormatEntry *FindCopyFormatLocked(const std::string &format_key) const {
		auto it = copy_formats_.find(format_key);
		return it != copy_formats_.end() ? &it->second : nullptr;
	}

	// Caller must hold CopyFormatMutex().
	void InsertCopyFormatLocked(const std::string &format_key, CopyFormatEntry entry) {
		copy_formats_[format_key] = std::move(entry);
	}

	std::vector<CopyFormatEntry> AllCopyFormats() const {
		std::lock_guard<std::mutex> lock(copy_format_mutex_);
		std::vector<CopyFormatEntry> out;
		out.reserve(copy_formats_.size());
		for (auto &kv : copy_formats_) {
			out.push_back(kv.second);
		}
		return out;
	}

	// --- Companion catalog registry (lakehouse federation) ---------------
	// Tracks catalogs the VGI layer attached on behalf of a catalog_attach
	// `attach_catalogs` manifest, so we never clobber a non-companion catalog
	// and can refcount-share a companion across VGI catalogs.
	enum class CompanionOutcome { ATTACHED, SHARED, CONFLICT };

	// Resolve + (if needed) attach one companion, serialized against other
	// companion ops. Returns:
	//   SHARED   — an existing companion with the same target; refcount bumped,
	//              no attach performed.
	//   ATTACHED — freshly attached (attach_fn ran and did not throw).
	//   CONFLICT — the alias is held by a different-target companion or by a
	//              non-companion catalog; `conflict_target_out` describes the
	//              holder. Nothing is attached; never clobbers.
	// attach_fn performs the real DatabaseManager::AttachDatabase and throws on
	// failure (registry stays unchanged in that case — it propagates out).
	CompanionOutcome AcquireCompanion(const std::string &alias, const std::string &target,
	                                  const std::string &db_type, bool hidden, DatabaseManager &db_manager,
	                                  const std::function<void()> &attach_fn, std::string &conflict_target_out) {
		std::lock_guard<std::mutex> lock(companion_mutex_);
		auto it = companion_catalogs_.find(alias);
		if (it != companion_catalogs_.end()) {
			if (it->second.target == target) {
				// Share — but only if the companion is still actually attached.
				// If a user detached it out-of-band, the registry is stale; drop
				// the stale record and fall through to a fresh attach so branches
				// can still resolve it.
				if (db_manager.GetDatabase(alias)) {
					it->second.refcount++;
					return CompanionOutcome::SHARED;
				}
				companion_catalogs_.erase(it);
			} else {
				conflict_target_out = it->second.target;
				return CompanionOutcome::CONFLICT;
			}
		}
		if (db_manager.GetDatabase(alias)) {
			conflict_target_out = "(existing non-companion catalog)";
			return CompanionOutcome::CONFLICT;
		}
		attach_fn(); // throws → registry unchanged
		companion_catalogs_[alias] = CompanionRecord {target, 1, db_type, hidden};
		return CompanionOutcome::ATTACHED;
	}

	// One companion catalog for the vgi_companion_catalogs() diagnostic.
	struct CompanionSnapshot {
		std::string alias;
		std::string target;
		std::string db_type;
		bool hidden = false;
		int64_t refcount = 0;
	};

	// Snapshot the companion registry (for vgi_companion_catalogs()).
	std::vector<CompanionSnapshot> AllCompanions() const {
		std::lock_guard<std::mutex> lock(companion_mutex_);
		std::vector<CompanionSnapshot> out;
		out.reserve(companion_catalogs_.size());
		for (const auto &kv : companion_catalogs_) {
			out.push_back(CompanionSnapshot {kv.first, kv.second.target, kv.second.db_type, kv.second.hidden,
			                                 kv.second.refcount});
		}
		return out;
	}

	// Decrement refcounts for the aliases a detaching VGI catalog referenced;
	// return those that hit zero (the caller detaches them from DuckDB).
	std::vector<std::string> ReleaseCompanions(const std::vector<std::string> &aliases) {
		std::lock_guard<std::mutex> lock(companion_mutex_);
		std::vector<std::string> to_detach;
		for (const auto &alias : aliases) {
			auto it = companion_catalogs_.find(alias);
			if (it == companion_catalogs_.end()) {
				continue;
			}
			if (--it->second.refcount <= 0) {
				to_detach.push_back(alias);
				companion_catalogs_.erase(it);
			}
		}
		return to_detach;
	}

private:
	mutable std::mutex redact_mutex_;
	std::unordered_map<std::string, case_insensitive_set_t> redact_keys_;
	vgi::VgiCancelDispatcher dispatcher_;
	mutable std::mutex secret_mutex_;
	std::map<std::string, vgi::VgiRemoteSecretStorage *> secret_providers_;
	int64_t next_secret_offset_ = 100;
	mutable std::mutex copy_format_mutex_;
	std::map<std::string, CopyFormatEntry> copy_formats_;

	struct CompanionRecord {
		std::string target; // canonical target key (raw manifest target string)
		int64_t refcount = 0;
		std::string db_type; // resolved/advertised db_type (for the vgi_companion_catalogs() diagnostic)
		bool hidden = false; // attached hidden (invisible to duckdb_databases())
	};
	mutable std::mutex companion_mutex_;
	std::map<std::string, CompanionRecord> companion_catalogs_;
};

namespace vgi {

// Resolve a companion's secret_ref into ATTACH options for metadata connections
// that need credentials at attach time (e.g. a postgres DSN). Data-file creds
// (S3/GCS/HTTP) don't need this — they resolve through the Orchard catch-all
// secret storage at query time.
//
// SECURITY: secret_ref is a WORKER-CHOSEN local secret name, and the companion
// target host is also worker-chosen — so blindly injecting the secret would let
// a malicious worker exfiltrate a user's credentials to an arbitrary host. This
// is therefore FAIL-CLOSED: injection happens only when the user explicitly
// opts in per-ATTACH (`attach_companion_secrets true`). Without opt-in a
// non-empty secret_ref is skipped (logged); the companion falls back to the
// Orchard catch-all / ambient credentials, or fails on its own — never exfil.
static void InjectCompanionSecret(ClientContext &context, const std::string &secret_ref, bool allowed,
                                  AttachOptions &options) {
	if (secret_ref.empty()) {
		return;
	}
	if (!allowed) {
		VGI_LOG(context, "vgi.companion_secret.skipped",
		        {{"secret_ref", secret_ref},
		         {"reason", "attach_companion_secrets not enabled (opt-in required to inject a worker-named secret)"}});
		return;
	}
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto entry = secret_manager.GetSecretByName(transaction, secret_ref);
	if (!entry || !entry->secret) {
		throw BinderException("VGI companion secret_ref '%s' did not resolve to a secret", secret_ref);
	}
	const auto *kv = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
	if (!kv) {
		throw BinderException("VGI companion secret_ref '%s' is not a key-value secret", secret_ref);
	}
	// Don't overwrite an option the worker set explicitly.
	for (const auto &e : kv->secret_map) {
		if (options.options.find(e.first) == options.options.end()) {
			options.options[e.first] = e.second;
		}
	}
}

void ReleaseCompanionCatalogs(ClientContext &context, const std::vector<std::string> &aliases) {
	if (aliases.empty()) {
		return;
	}
	auto *vgi_ext = VgiStorageExtension::Find(*context.db);
	if (!vgi_ext) {
		return;
	}
	auto to_detach = vgi_ext->ReleaseCompanions(aliases);
	auto &db_manager = DatabaseManager::Get(context);
	for (const auto &alias : to_detach) {
		// Best-effort: DetachDatabase throws on the default-DB guard
		// (`USE companion; DETACH vgi_catalog`) — a rare edge case that must not
		// abort the parent detach or leak the exception out of OnDetach.
		try {
			db_manager.DetachDatabase(context, alias, OnEntryNotFound::RETURN_NULL);
			VGI_LOG(context, "vgi.companion_detach", {{"alias", alias}});
		} catch (const std::exception &e) {
			VGI_LOG(context, "vgi.companion_detach.failed", {{"alias", alias}, {"error", e.what()}});
		}
	}
}

} // namespace vgi

// Create function for VGI secrets — builds a KeyValueSecret from config options.
// All named parameters from CREATE SECRET are copied into the secret's key-value map.
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Look up redact keys from VgiStorageExtension (per-DatabaseInstance, not global)
	auto &config = DBConfig::GetConfig(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	if (vgi_ext) {
		auto &vgi_storage = static_cast<VgiStorageExtension &>(*vgi_ext);
		secret->redact_keys = vgi_storage.GetRedactKeys(input.type);
	}

	// Copy all user-provided options into the secret map
	for (const auto &entry : input.options) {
		secret->secret_map[entry.first] = entry.second;
	}

	return secret;
}

namespace {

// Container-transport configuration extracted from a struct-valued LOCATION.
struct ParsedContainerOptions {
	string image;                       // the oci:// / docker:// address (struct's image/location/path key)
	string runtime_override;            // "" ⇒ auto-detect
	string connection;                  // "" ⇒ auto; else http|tcp|unix|stdio (shared vs per-process)
	std::vector<string> volumes;        // "host:/container/path"
	std::vector<string> env;            // "KEY=VALUE"
	std::vector<string> extra_args;     // verbatim docker-run argv tokens
};

// Collect a struct field that is either a VARCHAR (split on `delim`) or a
// LIST(VARCHAR) (elements used verbatim). Empty/NULL entries are dropped.
static std::vector<string> CollectScalarOrList(const Value &v, char delim) {
	std::vector<string> out;
	if (v.IsNull()) {
		return out;
	}
	if (v.type().id() == LogicalTypeId::LIST) {
		for (auto &child : ListValue::GetChildren(v)) {
			if (child.IsNull()) {
				continue;
			}
			string s = child.ToString();
			StringUtil::Trim(s);
			if (!s.empty()) {
				out.push_back(s);
			}
		}
	} else {
		for (auto &part : StringUtil::Split(v.ToString(), delim)) {
			string s = part;
			StringUtil::Trim(s);
			if (!s.empty()) {
				out.push_back(s);
			}
		}
	}
	return out;
}

// Parse a struct-valued LOCATION into its address (image) + container options.
// The address comes from an `image` (or `location`/`path`) key; remaining keys
// are runtime / transport / volumes / env / extra_args. Throws on unknown keys
// or a missing address.
static ParsedContainerOptions ParseContainerLocationStruct(const Value &loc) {
	ParsedContainerOptions out;
	auto &child_types = StructType::GetChildTypes(loc.type());
	auto &children = StructValue::GetChildren(loc);
	for (idx_t i = 0; i < children.size(); i++) {
		auto key = StringUtil::Lower(child_types[i].first);
		const Value &cv = children[i];
		if (key == "image" || key == "location" || key == "path") {
			out.image = cv.IsNull() ? "" : cv.ToString();
		} else if (key == "runtime" || key == "container_runtime") {
			out.runtime_override = cv.IsNull() ? "" : cv.ToString();
		} else if (key == "connection") {
			if (!cv.IsNull()) {
				out.connection = cv.ToString();
			}
		} else if (key == "volumes" || key == "volume") {
			auto vs = CollectScalarOrList(cv, ',');
			out.volumes.insert(out.volumes.end(), vs.begin(), vs.end());
		} else if (key == "env") {
			auto es = CollectScalarOrList(cv, ',');
			out.env.insert(out.env.end(), es.begin(), es.end());
		} else if (key == "extra_args" || key == "args") {
			// LIST → verbatim argv; VARCHAR → shell-tokenized.
			if (!cv.IsNull() && cv.type().id() == LogicalTypeId::LIST) {
				auto as = CollectScalarOrList(cv, ' ');
				out.extra_args.insert(out.extra_args.end(), as.begin(), as.end());
			} else if (!cv.IsNull()) {
				auto toks = vgi::launcher::ParseLaunchArgv(cv.ToString());
				out.extra_args.insert(out.extra_args.end(), toks.begin(), toks.end());
			}
		} else {
			throw BinderException(
			    "VGI struct LOCATION: unknown key '%s' (expected image, runtime, connection, "
			    "volumes, env, or extra_args)",
			    child_types[i].first);
		}
	}
	if (out.image.empty()) {
		throw BinderException(
		    "VGI struct LOCATION must include an 'image' (or 'location'/'path') key with the worker address");
	}
	return out;
}

} // namespace

// ATTACH handler for VGI catalogs
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	string worker_path;
	string catalog_name = info.path; // The first argument to ATTACH is the catalog name
	bool worker_debug = false;
	bool use_pool = true;
	bool cache_enabled = true; // ATTACH `cache` option (result cache opt-out)
	int64_t pool_max_override = -1;      // -1 = not set
	int64_t pool_timeout_override = -1;  // -1 = not set
	string oauth_refresh_token;
	string bearer_token;
	string data_version_spec;
	string implementation_version;
	// Per-LOCATION launcher overrides — only valid with ``launch:`` LOCATIONs;
	// rejected at parse time for any other transport.  -1 sentinel for "unset"
	// on the integer; std::nullopt-equivalent on the string is empty.
	int64_t launcher_idle_timeout_seconds = -1;
	string launcher_state_dir;
	// A struct-valued LOCATION carries the worker address plus all transport
	// options in one parameter (container transport). Captured here, parsed after
	// the option loops. A plain VARCHAR LOCATION sets worker_path directly.
	Value location_struct;
	bool location_is_struct = false;
	// Opt out of Orchard's remote secret provider for this catalog. Default on:
	// an HTTP catalog whose attach response advertises a secret-service URL gets
	// a VgiRemoteSecretStorage auto-registered (reusing this catalog's identity).
	bool secrets_enabled = true;
	// Opt out of provisioning companion catalogs advertised via the
	// catalog_attach `attach_catalogs` manifest (lakehouse federation). Default
	// on: attach is largely lazy and guarded by a scheme allowlist + a
	// never-clobber conflict policy. Set `attach_companions false` to disable.
	bool attach_companions_enabled = true;
	// Opt IN to injecting a worker-named secret (`secret_ref`) into a companion's
	// ATTACH options. Default OFF: a worker chooses both the secret name and the
	// target host, so auto-injecting would let a malicious worker exfiltrate the
	// user's credentials to an arbitrary host. Data-file creds still resolve via
	// the Orchard catch-all without this.
	bool attach_companion_secrets = false;
	// Unknown-to-the-extension options are forwarded to the worker as
	// attach-time options after validation against the catalog's declared
	// AttachOptionSpec list (fetched via InvokeCatalogs when present).
	std::map<std::string, Value> attach_options;
	// Every non-secret ATTACH option (name → string value), folded into the
	// result-cache key so differing options (which may route to different data /
	// locations) never share a cache entry. Excludes the secret tokens and
	// LOCATION (already keyed via worker_path). std::map keeps it canonically
	// sorted for a stable serialization.
	std::map<std::string, std::string> key_options;

	// Per-option handler, shared between the connection-string query (below) and
	// the explicit (TYPE vgi, ...) options clause so both honour the same names.
	auto apply_option = [&](const string &lower_name, const Value &value) {
		// Record for the cache key — every option except the secret tokens and
		// LOCATION/PATH (LOCATION is already the key's worker_path; the tokens
		// must never enter the key or the on-disk digest).
		if (lower_name != "type" && lower_name != "location" && lower_name != "path" &&
		    lower_name != "bearer_token" && lower_name != "oauth_refresh_token" &&
		    value.type().id() != LogicalTypeId::STRUCT) {
			key_options[lower_name] = value.ToString();
		}
		if (lower_name == "type") {
			return;
		} else if (lower_name == "location" || lower_name == "path") {
			// LOCATION is dynamically typed: a STRUCT bundles the worker address
			// plus transport options (container transport); a VARCHAR is the plain
			// address. The struct is parsed after the option loops.
			if (value.type().id() == LogicalTypeId::STRUCT) {
				location_struct = value;
				location_is_struct = true;
			} else {
				worker_path = value.ToString();
			}
		} else if (lower_name == "worker_debug") {
			// DefaultCastAs (not GetValue<bool>) so VARCHAR values from the
			// connection-string query ("true"/"false") cast semantically rather
			// than as a raw int8; typed BOOLEAN values from the clause are a no-op.
			worker_debug = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "pool") {
			use_pool = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "cache") {
			cache_enabled = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "secrets") {
			secrets_enabled = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "attach_companions") {
			attach_companions_enabled = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "attach_companion_secrets") {
			attach_companion_secrets = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "pool_max") {
			pool_max_override = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		} else if (lower_name == "pool_timeout") {
			pool_timeout_override = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		} else if (lower_name == "oauth_refresh_token") {
			oauth_refresh_token = value.ToString();
		} else if (lower_name == "bearer_token") {
			bearer_token = value.ToString();
		} else if (lower_name == "data_version_spec") {
			data_version_spec = value.ToString();
		} else if (lower_name == "implementation_version") {
			implementation_version = value.ToString();
		} else if (lower_name == "launcher_idle_timeout") {
			launcher_idle_timeout_seconds = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
			if (launcher_idle_timeout_seconds < 0) {
				throw BinderException(
				    "launcher_idle_timeout must be >= 0 (got %lld); use 0 for no timeout",
				    static_cast<long long>(launcher_idle_timeout_seconds));
			}
		} else if (lower_name == "launcher_state_dir") {
			launcher_state_dir = value.ToString();
			if (launcher_state_dir.empty()) {
				throw BinderException("launcher_state_dir, if set, must not be empty");
			}
		} else {
			// Collect for validation + forwarding to the worker.
			attach_options.emplace(lower_name, value);
		}
	};

	// Connection-string form (mirrors the airport extension): the worker location
	// and ATTACH options may be encoded in the ATTACH path as
	//   '<catalog>?location=<worker>&<opt>=<val>&...'
	// so a catalog is reachable without an explicit options clause — e.g. from the
	// DuckDB/haybarn CLI:  haybarn-cli "vgi:example?location=https://host/vgi/".
	// The base (before '?') stays the catalog name; the query string supplies
	// options, applied first so an explicit (TYPE vgi, ...) clause still overrides.
	bool secret_in_path = false;
	bool had_query = false;
	{
		auto qpos = catalog_name.find('?');
		if (qpos != string::npos) {
			had_query = true;
			string query = catalog_name.substr(qpos + 1);
			catalog_name = catalog_name.substr(0, qpos);
			size_t start = 0;
			while (start <= query.size()) {
				auto amp = query.find('&', start);
				string token =
				    query.substr(start, amp == string::npos ? string::npos : amp - start);
				if (!token.empty()) {
					auto eq = token.find('=');
					string key = (eq == string::npos) ? token : token.substr(0, eq);
					string val = (eq == string::npos) ? string() : token.substr(eq + 1);
					auto lower_key = StringUtil::Lower(key);
					if (lower_key == "oauth_refresh_token" || lower_key == "bearer_token") {
						secret_in_path = true;
					}
					apply_option(lower_key, Value(val));
				}
				if (amp == string::npos) {
					break;
				}
				start = amp + 1;
			}
			// A token embedded in the ATTACH path is just as introspection-visible
			// as one in the options clause — strip the whole query from info.path.
			// Recommend CREATE SECRET / the options clause for non-ephemeral tokens.
			if (secret_in_path) {
				info.path = catalog_name + "?<redacted>";
			}
		}
	}

	// Explicit (TYPE vgi, ...) options clause. Applied after the query string so it
	// wins on conflicts; tokens are wiped from the parsed AttachInfo so they can't
	// be echoed by DuckDB introspection (catalog logging, prepared-statement dumps,
	// telemetry). The CatalogAuth holds the live token from here on.
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		apply_option(lower_name, entry.second);
		if (lower_name == "oauth_refresh_token" || lower_name == "bearer_token") {
			entry.second = Value("<redacted>");
		}
	}

	// Resolve a struct-valued LOCATION into the worker address (worker_path) plus
	// the container options carried alongside it. Done before the bare-form
	// fallback / empty-LOCATION check below so worker_path is set.
	ParsedContainerOptions container_opts;
	if (location_is_struct) {
		container_opts = ParseContainerLocationStruct(location_struct);
		worker_path = container_opts.image;
	}

	// Validate mutual exclusivity of auth options
	if (!bearer_token.empty() && !oauth_refresh_token.empty()) {
		throw BinderException("Cannot specify both bearer_token and oauth_refresh_token");
	}

	// Bare connection-string form: when no LOCATION was given and the path
	// carried no '?query' (which would mark the base as a catalog name,
	// airport-style), the whole ATTACH path IS the worker location — e.g. the
	// haybarn CLI form `vgi:uvx vgi-easter`, or `ATTACH 'uvx vgi-easter'
	// (TYPE vgi)`. The remote catalog name is discovered from the worker below.
	bool discover_catalog = false;
	if (worker_path.empty() && !had_query && !catalog_name.empty()) {
		worker_path = catalog_name;
		catalog_name.clear();
		discover_catalog = true;
	}

	if (worker_path.empty()) {
		throw BinderException("VGI ATTACH requires LOCATION option specifying the worker path");
	}

	// Telemetry capture — the event fires only on the success path, near the
	// return. Snapshot the raw (pre-rewrite) location + the user's options here,
	// before the container/HTTP rewrites and the attach RPC. Best-effort; nothing
	// below may throw on telemetry's account.
	vgi::VgiAttachInfo tele;
	tele.raw_location = worker_path;
	tele.opt_pool = use_pool;
	tele.opt_secrets = secrets_enabled;
	tele.opt_cache = cache_enabled;
	tele.opt_attach_companions = attach_companions_enabled;
	tele.opt_attach_companion_secrets = attach_companion_secrets;
	if (pool_max_override >= 0) {
		tele.has_pool_max = true;
		tele.opt_pool_max = pool_max_override;
	}
	if (pool_timeout_override >= 0) {
		tele.has_pool_timeout = true;
		tele.opt_pool_timeout = pool_timeout_override;
	}
	const auto tele_start = std::chrono::steady_clock::now();

	// Launcher overrides are only meaningful for ``launch:`` LOCATIONs.
	// ``unix://`` connects to an externally-managed worker (we don't spawn,
	// don't manage state); HTTP and bare-subprocess transports don't go
	// through the launcher at all.  Reject loudly so users catch typos
	// or wrong-transport mistakes before they hit the worker.
	if ((launcher_idle_timeout_seconds >= 0 || !launcher_state_dir.empty()) &&
	    !vgi::IsLaunchLocation(worker_path)) {
		throw BinderException(
		    "launcher_idle_timeout / launcher_state_dir are only valid for `launch:` "
		    "LOCATIONs (got LOCATION=%s)",
		    worker_path);
	}

	// A struct-valued LOCATION carries container options, so it is only meaningful
	// for a container (oci:// / docker://) address. Catch a struct LOCATION whose
	// address isn't a container image early.
	if (location_is_struct && !vgi::IsContainerLocation(worker_path)) {
		throw BinderException(
		    "VGI struct LOCATION is only supported for oci:// / docker:// container images "
		    "(got image=%s)",
		    worker_path);
	}

	if (vgi::IsContainerLocation(worker_path)) {
#if !VGI_SUBPROCESS_TRANSPORT
		throw BinderException(
		    "VGI container (oci:// / docker://) LOCATIONs require a child-process transport "
		    "not available in this build; use http:// instead");
#else
		// Validate an explicit `connection` up front — before any docker call
		// (runtime detect / image inspect) — so a bad value fails fast and
		// daemon-free. `stdio` ⇒ private per-process; http/tcp ⇒ shared; unix is
		// rejected (AF_UNIX over docker bind mounts is unreliable).
		string conn = StringUtil::Lower(container_opts.connection);
		bool shared = false;
		vgi::ContainerConnMode shared_mode = vgi::ContainerConnMode::HTTP;
		if (!conn.empty() && conn != "stdio") {
			shared = true;
			shared_mode = vgi::ParseContainerConnMode(conn);
			if (shared_mode == vgi::ContainerConnMode::UNIX) {
				throw BinderException(
				    "VGI shared container connection 'unix' is not supported (AF_UNIX over docker bind "
				    "mounts is unreliable); use 'tcp' or 'http'");
			}
		}

		vgi::ContainerSpec spec;
		spec.transport = "stdio";
		spec.image = vgi::StripContainerScheme(worker_path);
		spec.runtime = vgi::DetectContainerRuntime(container_opts.runtime_override);

		// Auto-mount the volumes the image declares via its OCI label, then layer
		// user-supplied volume entries on top (user wins on a duplicate mount
		// point).  Inspection pulls the image first if it isn't present locally.
		spec.volumes = vgi::InspectImageVolumes(spec.runtime, spec.image);
		auto upsert_volume = [&](const string &host, const string &path) {
			for (auto &v : spec.volumes) {
				if (v.path == path) {
					v.host = host;
					return;
				}
			}
			spec.volumes.push_back(vgi::ContainerVolume {host, path});
		};
		for (auto &e : container_opts.volumes) {
			auto colon = e.find(':');
			if (colon == string::npos) {
				throw BinderException(
				    "VGI LOCATION volume entry '%s' must be of the form 'name:/container/path'", e);
			}
			upsert_volume(e.substr(0, colon), e.substr(colon + 1));
		}
		spec.env = std::move(container_opts.env);
		spec.extra_args = std::move(container_opts.extra_args);

		// Auto-select the connection from the image's advertised transports when
		// none was given (prefer tcp — native protocol + idle self-shutdown — over
		// http); a label-less image stays private per-process (back-compat).
		if (conn.empty()) {
			auto modes = vgi::InspectImageTransports(spec.runtime, spec.image);
			bool has_tcp = false, has_http = false;
			for (auto m : modes) {
				has_tcp = has_tcp || (m == vgi::ContainerConnMode::TCP);
				has_http = has_http || (m == vgi::ContainerConnMode::HTTP);
			}
			if (has_tcp) {
				shared = true;
				shared_mode = vgi::ContainerConnMode::TCP;
			} else if (has_http) {
				shared = true;
				shared_mode = vgi::ContainerConnMode::HTTP;
			}
		}

		// Telemetry: container facts (before worker_path is rewritten below).
		tele.is_container = true;
		tele.container_runtime = spec.runtime.kind;
		tele.container_connection = shared ? (shared_mode == vgi::ContainerConnMode::TCP ? "tcp" : "http") : "stdio";
		tele.container_shared = shared;

		if (shared) {
			// Transparent shared container: register the resolved spec/mode under an
			// internal container-shared: token; the dispatch resolves the live
			// endpoint at connection time via the daemon-introspection coordinator.
			string shared_loc = "container-shared:" + worker_path + "#" + vgi::ContainerSpecHash(spec);
			vgi::RegisterSharedContainer(shared_loc, spec, shared_mode);
			static std::once_flag reap_shared_once;
			std::call_once(reap_shared_once, [&]() { vgi::ReapDeadSharedContainers(spec.runtime); });
			// http mode talks to the container over the existing HTTP transport;
			// tcp speaks the native protocol over a socket (no httpfs needed).
			if (shared_mode == vgi::ContainerConnMode::HTTP) {
				ExtensionHelper::TryAutoLoadExtension(db.GetDatabase(), "httpfs");
				if (!db.GetDatabase().ExtensionIsLoaded("httpfs")) {
					throw BinderException("VGI shared http container requires the httpfs extension. "
					                      "Install it with: INSTALL httpfs; LOAD httpfs;");
				}
			}
			use_pool = false;
			worker_path = shared_loc;
		} else {
			// Private per-process container (the shipped default for label-less
			// images / connection:'stdio'). Resolve the run command + a
			// pool-disambiguation suffix and register it for the spawn sites.
			string command_template = vgi::BuildContainerRunCommandTemplate(spec);
			string suffixed = worker_path + "#" + vgi::ContainerSpecHash(spec);
			vgi::RegisterContainerLaunch(suffixed, spec.runtime.binary, command_template);
			static std::once_flag reap_once;
			std::call_once(reap_once, [&]() { vgi::ReapOrphanContainers(spec.runtime); });
			worker_path = suffixed;
		}
#endif
	}

#ifdef __EMSCRIPTEN__
	// WASM: HTTP (duckdb-wasm XHR layer) and the browser `worker:` SAB transport
	// (a Web Worker over a SharedArrayBuffer duplex ring) are supported; no
	// subprocess/fork transports exist here.
	if (!vgi::IsHttpTransport(worker_path) && !vgi::IsWebWorkerTransport(worker_path)) {
		throw BinderException("VGI in WASM only supports HTTP ('http[s]://') or "
		                      "browser worker ('worker:') transports.");
	}
	use_pool = false;
	// HTTP in WASM goes through duckdb-wasm's XHR layer, not httpfs; worker: pools
	// via SAB slots, not the subprocess pool.
#else
	// HTTP transport: require httpfs for POST support, disable subprocess pooling
	if (vgi::IsHttpTransport(worker_path)) {
		try {
			ExtensionHelper::TryAutoLoadExtension(db.GetDatabase(), "httpfs");
		} catch (...) {
			// ignore auto-load errors, check below
		}
		if (!db.GetDatabase().ExtensionIsLoaded("httpfs")) {
			throw BinderException("VGI HTTP transport requires the httpfs extension. "
			                      "Install it with: INSTALL httpfs; LOAD httpfs;");
		}
		use_pool = false; // HTTP is stateless, no subprocess pooling
	}
#endif

	// Create per-catalog auth state
	std::shared_ptr<vgi::CatalogAuth> auth;
	if (!bearer_token.empty()) {
		if (!vgi::IsHttpTransport(worker_path)) {
			throw BinderException("bearer_token is only valid for HTTP transport "
			                      "(LOCATION must be an HTTP/HTTPS URL)");
		}
		auth = std::make_shared<vgi::BearerTokenCatalogAuth>(bearer_token);
	} else {
		auto oauth_auth = std::make_shared<vgi::OAuthCatalogAuth>();
		if (!oauth_refresh_token.empty()) {
			if (!vgi::IsHttpTransport(worker_path)) {
				throw BinderException("oauth_refresh_token is only valid for HTTP transport "
				                      "(LOCATION must be an HTTP/HTTPS URL)");
			}
			oauth_auth->SeedRefreshToken(oauth_refresh_token);
		}
		auth = std::move(oauth_auth);
	}

	// Telemetry: auth mode. OAuth/bearer only apply to HTTP transport; every other
	// transport is unauthenticated from the extension's perspective.
	if (!bearer_token.empty()) {
		tele.auth_mode = "bearer";
	} else if (!oauth_refresh_token.empty()) {
		tele.auth_mode = "oauth_refresh_token";
	} else if (vgi::IsHttpTransport(worker_path)) {
		tele.auth_mode = "oauth";
	} else {
		tele.auth_mode = "none";
	}

	// HTTP cookie jar — carries proxy-issued Set-Cookie / Cookie headers for
	// sticky version-aware routing. Subprocess transport doesn't use this.
	std::shared_ptr<vgi::SessionCookieJar> cookie_jar;
	if (vgi::IsHttpTransport(worker_path)) {
		cookie_jar = std::make_shared<vgi::SessionCookieJar>();
	}

	// Lifted from the InvokeCatalogAttach call below — these optionals must
	// also flow into InvokeCatalogs so the discovery RPC primes the launcher
	// cache with the user's overrides, not [defaults]. See InvokeCatalogs's
	// docstring and InvokeCatalogAttach's analogous fix.
	std::optional<int64_t> launcher_idle_for_attach;
	std::optional<std::string> launcher_state_dir_for_attach;
	if (launcher_idle_timeout_seconds >= 0) {
		launcher_idle_for_attach = launcher_idle_timeout_seconds;
	}
	if (!launcher_state_dir.empty()) {
		launcher_state_dir_for_attach = launcher_state_dir;
	}

	// Fetch the worker's catalog list once when we need it — either to discover
	// the catalog name for the bare connection-string form, or to validate
	// attach-time options against the catalog's declared AttachOptionSpec list.
	// Catalogs without options on the explicit form pay no overhead (no RPC).
	if (discover_catalog || !attach_options.empty()) {
		auto catalogs = vgi::InvokeCatalogs(worker_path, context, worker_debug, use_pool, auth,
		                                    launcher_idle_for_attach, launcher_state_dir_for_attach);

		// Bare form: resolve the (omitted) catalog name from the worker. A
		// single-catalog worker resolves unambiguously; >1 forces the user to
		// name one via the '<catalog>?location=<worker>' form.
		if (discover_catalog) {
			if (catalogs.empty()) {
				throw BinderException("VGI worker at '%s' exposes no catalogs", worker_path);
			}
			if (catalogs.size() > 1) {
				std::string names;
				for (const auto &c : catalogs) {
					if (!names.empty()) {
						names += ", ";
					}
					names += c.name;
				}
				throw BinderException(
				    "VGI worker at '%s' exposes %llu catalogs (%s); name one with "
				    "ATTACH '<catalog>?location=%s' (TYPE vgi) or "
				    "ATTACH '<catalog>' (TYPE vgi, LOCATION '%s')",
				    worker_path, static_cast<unsigned long long>(catalogs.size()), names, worker_path,
				    worker_path);
			}
			catalog_name = catalogs[0].name;

			// When DuckDB derived the database alias from the path (the CLI
			// default-DB open `vgi:uvx vgi-easter`, or `ATTACH 'uvx vgi-easter'
			// (TYPE vgi)` with no AS clause), the alias is the ugly path
			// basename. Replace it with the discovered catalog name so the DB
			// surfaces as `easter`, not `uvx vgi-easter`. Registration reads
			// GetName() *after* attach (DatabaseManager::FinalizeAttach), and
			// derives the default-database pointer from it too, so SetName here
			// is honored consistently. A user-chosen `AS alias` (name differs
			// from the path basename) is left untouched.
			auto &fs = FileSystem::GetFileSystem(context);
			if (name == AttachedDatabase::ExtractDatabaseName(info.path, fs)) {
				db.SetName(catalog_name);
			}
		}

		// Validate attach-time options against the resolved catalog's declared
		// AttachOptionSpec list (skipped on the bare form when no options given).
		if (!attach_options.empty()) {
			const vgi::VgiCatalogInfo *matching_info = nullptr;
			for (const auto &info_entry : catalogs) {
				if (info_entry.name == catalog_name) {
					matching_info = &info_entry;
					break;
				}
			}
			if (!matching_info) {
				throw BinderException("Worker at '%s' exposes no catalog named '%s'", worker_path, catalog_name);
			}

			auto build_accepted_list = [&]() {
				std::string joined;
				for (const auto &spec : matching_info->attach_option_specs) {
					if (!joined.empty()) {
						joined += ", ";
					}
					joined += spec.name;
				}
				return joined.empty() ? std::string("(none)") : joined;
			};

			std::map<std::string, Value> validated_options;
			for (const auto &opt : attach_options) {
				const vgi::VgiAttachOptionSpec *matching_spec = nullptr;
				for (const auto &spec : matching_info->attach_option_specs) {
					if (StringUtil::Lower(spec.name) == opt.first) {
						matching_spec = &spec;
						break;
					}
				}
				if (!matching_spec) {
					throw BinderException("Unknown ATTACH option '%s' for catalog '%s'. Accepted options: %s",
					                      opt.first, catalog_name, build_accepted_list());
				}

				Value casted;
				std::string cast_error;
				if (!opt.second.DefaultTryCastAs(matching_spec->type, casted, &cast_error)) {
					throw BinderException("Cannot cast ATTACH option '%s' to declared type %s: %s",
					                      matching_spec->name, matching_spec->type.ToString(), cast_error);
				}
				validated_options.emplace(matching_spec->name, std::move(casted));
			}
			attach_options = std::move(validated_options);
		}
	}

	// Call catalog_attach via RPC. The worker validates data_version_spec and
	// implementation_version and throws with a human-readable message on
	// unsatisfiable requests; that surfaces as the ATTACH failure. The
	// launcher_*_for_attach optionals were already built above (alongside the
	// InvokeCatalogs path) so the override flows into both RPCs identically.
	auto attach_result = vgi::InvokeCatalogAttach(worker_path, catalog_name, context, worker_debug, use_pool, auth,
	                                              data_version_spec, implementation_version, cookie_jar,
	                                              attach_options, launcher_idle_for_attach,
	                                              launcher_state_dir_for_attach);

	// Register extension options for settings exposed by this catalog
	// Check for type conflicts with existing settings
	auto &config = DBConfig::GetConfig(db.GetDatabase());

	for (const auto &setting : attach_result.settings) {
		if (config.HasExtensionOption(setting.name)) {
			// Setting already exists - verify types match
			ExtensionOption existing_option;
			if (!config.TryGetExtensionOption(setting.name, existing_option)) {
				throw BinderException("Failed to retrieve existing VGI setting '%s'", setting.name);
			}
			if (existing_option.type != setting.type) {
				throw BinderException("VGI setting '%s' already exists with type %s, but catalog '%s' requires type %s",
				                      setting.name, existing_option.type.ToString(), catalog_name,
				                      setting.type.ToString());
			}
			// Types match - setting is already registered, no need to add again
		} else {
			// New setting - register it
			config.AddExtensionOption(setting.name, setting.description, setting.type, setting.default_value);
		}
	}

	// Register secret types exposed by this catalog
	auto &secret_manager = SecretManager::Get(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	for (const auto &st : attach_result.secret_types) {
		// Build redact keys set from parameter metadata
		case_insensitive_set_t redact_set;
		for (const auto &param : st.parameters) {
			if (param.redact) {
				redact_set.insert(param.name);
			}
		}

		// Register the secret type with DuckDB
		SecretType secret_type;
		secret_type.name = st.name;
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		secret_type.extension = "vgi";
		try {
			secret_manager.RegisterSecretType(secret_type);
		} catch (const InternalException &) {
			// Already registered by another VGI catalog — skip
			// Don't overwrite redact keys from the first (winning) registration
			redact_set = {};
		}

		// Store redact keys on VgiStorageExtension (only for newly registered types)
		if (vgi_ext && !redact_set.empty()) {
			static_cast<VgiStorageExtension &>(*vgi_ext).SetRedactKeys(st.name, std::move(redact_set));
		}

		// Register "config" provider create function with named parameters
		CreateSecretFunction create_func;
		create_func.secret_type = st.name;
		create_func.provider = "config";
		create_func.function = VgiCreateSecret;
		for (const auto &param : st.parameters) {
			create_func.named_parameters[param.name] = param.type;
		}
		secret_manager.RegisterSecretFunction(create_func, OnCreateConflict::IGNORE_ON_CONFLICT);
	}

	// Build per-path pool config and register with pool
	vgi::PoolSettings pool_settings;
	if (use_pool) {
		// Default per-path limit from vgi_worker_pool_max setting
		Value max_val;
		if (context.TryGetCurrentSetting("vgi_worker_pool_max", max_val)) {
			pool_settings.max_pool_size = static_cast<size_t>(max_val.GetValue<int64_t>());
		}
		Value idle_val;
		if (context.TryGetCurrentSetting("vgi_worker_pool_idle_limit_seconds", idle_val)) {
			pool_settings.idle_timeout_seconds = static_cast<size_t>(idle_val.GetValue<int64_t>());
		}
		// ATTACH options override the defaults
		if (pool_max_override >= 0) {
			pool_settings.max_pool_size = static_cast<size_t>(pool_max_override);
		}
		if (pool_timeout_override >= 0) {
			pool_settings.idle_timeout_seconds = static_cast<size_t>(pool_timeout_override);
		}
	} else {
		pool_settings.max_pool_size = 0; // disabled
	}
	vgi::VgiWorkerPool::Instance().ConfigurePath(worker_path, pool_settings);

	// Set database-level comment and tags from worker metadata
	if (!attach_result.comment.empty()) {
		db.comment = Value(attach_result.comment);
	}
	for (auto &[key, val] : attach_result.tags) {
		db.tags[key] = val;
	}

	// Surface resolved versions via duckdb_databases().tags so users can verify
	// what the worker actually picked. Namespaced with vgi_ to avoid clashing
	// with any worker-declared tags.
	if (!attach_result.resolved_data_version.empty()) {
		db.tags["vgi_resolved_data_version"] = attach_result.resolved_data_version;
	}
	if (!attach_result.resolved_implementation_version.empty()) {
		db.tags["vgi_resolved_implementation_version"] = attach_result.resolved_implementation_version;
	}

	// Create attach parameters. Resolved versions are what the worker picked;
	// they're used by the pool key so catalogs attached at different versions
	// never share a subprocess worker.  Launcher overrides flow through to
	// the cache layer where they're pinned to the worker's lifetime.
	vgi::VgiAttachParametersConfig attach_cfg;
	attach_cfg.worker_path = worker_path;
	attach_cfg.catalog_name = catalog_name;
	attach_cfg.worker_debug = worker_debug;
	attach_cfg.use_pool = use_pool;
	attach_cfg.cache = cache_enabled;
	attach_cfg.auth = auth;
	attach_cfg.data_version_spec = attach_result.resolved_data_version;
	attach_cfg.implementation_version = attach_result.resolved_implementation_version;
	attach_cfg.cookie_jar = cookie_jar;
	if (launcher_idle_timeout_seconds >= 0) {
		attach_cfg.launcher_idle_timeout_seconds = launcher_idle_timeout_seconds;
	}
	if (!launcher_state_dir.empty()) {
		attach_cfg.launcher_state_dir = launcher_state_dir;
	}
	// Canonical (sorted) serialization of the non-secret ATTACH options for the
	// result-cache key. std::map iteration is already sorted.
	{
		std::string canonical;
		for (auto &kv : key_options) {
			canonical += kv.first;
			canonical += '=';
			canonical += kv.second;
			canonical += ';';
		}
		attach_cfg.attach_options_canonical = std::move(canonical);
	}
	auto attach_params = std::make_shared<vgi::VgiAttachParameters>(std::move(attach_cfg));

	// Prime the HTTPParams cache while we're outside any VGI catalog
	// transaction. HTTPFSUtil::InitializeParameters pulls settings/secrets
	// through KeyValueSecretReader, which takes the MetaTransaction mutex —
	// doing it lazily from inside VgiTransaction::Start would self-deadlock
	// (the surrounding MetaTransaction::GetTransaction call already holds that
	// mutex). Doing it here, during ATTACH handling and before any VGI
	// transaction exists, avoids the reentrancy. TODO(#22258): remove once
	// https://github.com/duckdb/duckdb/issues/22258 is fixed.
	if (vgi::IsHttpTransport(worker_path)) {
		attach_params->GetOrInitHttpParams(context, worker_path);
	}

	auto attach_result_ptr = std::make_shared<vgi::CatalogAttachResult>(std::move(attach_result));

	// Snapshot eager-load thresholds at attach time. The runtime form is a
	// fixed struct of int64s; we never re-parse the map on the hot path.
	VgiObjectCounts eager_thresholds;
	{
		Value threshold_val;
		std::map<std::string, int64_t> threshold_map;
		if (context.TryGetCurrentSetting("vgi_eager_load_threshold", threshold_val) && !threshold_val.IsNull()) {
			auto &child_values = MapValue::GetChildren(threshold_val);
			for (auto &kv : child_values) {
				auto &kv_struct = StructValue::GetChildren(kv);
				if (kv_struct.size() != 2 || kv_struct[0].IsNull() || kv_struct[1].IsNull()) {
					continue;
				}
				threshold_map[kv_struct[0].GetValue<std::string>()] = kv_struct[1].GetValue<int64_t>();
			}
		}
		// Default for missing keys is 1000 (per setting docs); ObjectCountsFromMap fills in.
		eager_thresholds = ObjectCountsFromMap(threshold_map, 1000);
	}

	// Auto-register Orchard's remote secret provider when the catalog advertises
	// a secret-service URL via tags (and the user didn't opt out with
	// `secrets false`). Reuses THIS catalog's auth (OAuth/bearer) so a single
	// login authorizes both worker RPCs and secret fetches. HTTP-only — there's
	// no secret endpoint to call on subprocess/launch/unix transports.
	if (secrets_enabled && vgi::IsHttpTransport(worker_path)) {
		auto turl = attach_result_ptr->tags.find("vgi_secret_service_url");
		if (turl != attach_result_ptr->tags.end() && !turl->second.empty()) {
			const std::string secret_endpoint = turl->second;
			// A remote credential broker must be reached over TLS. Require https://
			// — allow cleartext http only for loopback (local testing).
			auto lower_secret_url = StringUtil::Lower(secret_endpoint);
			const bool is_https = StringUtil::StartsWith(lower_secret_url, "https://");
			const bool is_loopback_http =
			    StringUtil::StartsWith(lower_secret_url, "http://127.0.0.1") ||
			    StringUtil::StartsWith(lower_secret_url, "http://localhost") ||
			    StringUtil::StartsWith(lower_secret_url, "http://[::1]");
			if (!is_https && !is_loopback_http) {
				throw BinderException(
				    "vgi_secret_service_url advertised by catalog '%s' must be an https:// URL — a remote "
				    "secret broker must not be reached over cleartext HTTP (only http://localhost is allowed "
				    "for testing). Got: %s",
				    catalog_name, secret_endpoint);
			}
			// Default cache TTL frozen per-provider at attach time.
			int64_t default_ttl = 300;
			Value ttl_val;
			if (context.TryGetCurrentSetting("vgi_secret_default_ttl_seconds", ttl_val) && !ttl_val.IsNull()) {
				default_ttl = ttl_val.GetValue<int64_t>();
				if (default_ttl < 0) {
					default_ttl = 0;
				}
			}

			auto *vgi_ext = VgiStorageExtension::Find(*context.db);
			if (vgi_ext) {
				const int64_t offset = vgi_ext->NextSecretTieBreakOffset();
				// Unique storage name per registration (offset suffix) so a
				// re-ATTACH after detach doesn't collide with the inert prior
				// storage still owned by the SecretManager.
				const std::string storage_name =
				    "vgi_secret_" + catalog_name + "_" + std::to_string(offset);
				auto storage = make_uniq<vgi::VgiRemoteSecretStorage>(
				    *context.db, storage_name, secret_endpoint, auth, std::chrono::seconds(default_ttl), offset);
				auto *storage_ptr = storage.get();
				try {
					SecretManager::Get(context).LoadSecretStorage(std::move(storage));
				} catch (const std::exception &e) {
					throw BinderException(
					    "Failed to register VGI remote secret provider for catalog '%s' (endpoint %s): %s",
					    catalog_name, secret_endpoint, e.what());
				}
				vgi_ext->RegisterSecretProvider(catalog_name, storage_ptr);
			}
		}
	}

	// Auto-register custom COPY ... FROM formats advertised by this catalog.
	// Each format becomes a DuckDB CopyFunction in the system catalog. DuckDB's
	// COPY format namespace is global and keyed by the exact string the user
	// types, so we SCOPE the registered name by the attach alias (`name`, which
	// is unique per DB): `<alias>.<format>`. That makes collisions between
	// attaches impossible by construction — two attaches of the same worker get
	// distinct format names — so no opt-out flag is needed. Users write
	// `COPY t FROM 'p' (FORMAT '<alias>.<format>')`. Old workers that don't
	// implement the discovery RPC degrade to "no formats". See docs/copy_from.md.
	{
		std::vector<vgi::VgiCopyFromFormatInfo> formats;
		try {
			vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result_ptr->attach_opaque_data, {}};
			formats = vgi::InvokeCatalogCopyFromFormats(rpc_ctx, context);
		} catch (const vgi::VgiRpcException &e) {
			if (e.GetErrorKind() != vgi::error_kind::kMethodNotImplemented) {
				throw;
			}
			// Old worker: advertises no COPY formats. Continue the ATTACH.
		}

		if (!formats.empty()) {
			auto *vgi_ext = VgiStorageExtension::Find(*context.db);
			auto &system_catalog = Catalog::GetSystemCatalog(*context.db);
			auto sys_txn = CatalogTransaction::GetSystemTransaction(*context.db);
			const std::string alias_lower = StringUtil::Lower(name);

			// Collect setting names registered by this catalog so COPY binds can
			// forward catalog-scoped settings to the worker handler.
			std::vector<std::string> setting_names;
			setting_names.reserve(attach_result_ptr->settings.size());
			for (const auto &setting : attach_result_ptr->settings) {
				setting_names.push_back(setting.name);
			}

			for (auto &fmt : formats) {
				// Scoped, collision-free registered name (lowercased to match the
				// binder's case-insensitive format lookup).
				const std::string registered_name = alias_lower + "." + StringUtil::Lower(fmt.format_name);

				// Registry-first under the registry lock. With alias scoping a hit
				// means a re-ATTACH of the same alias (the prior system-catalog
				// entry persists past DETACH) — idempotent, skip re-registering.
				std::lock_guard<std::mutex> lock(vgi_ext->CopyFormatMutex());
				if (vgi_ext->FindCopyFormatLocked(registered_name)) {
					continue;
				}

				// Defensive: refuse if something already owns the scoped name in
				// the system catalog (shouldn't happen for alias-scoped names).
				CatalogEntryRetriever retriever {context};
				auto clash = system_catalog.GetEntry(retriever, DEFAULT_SCHEMA,
				                                     {CatalogType::COPY_FUNCTION_ENTRY, registered_name},
				                                     OnEntryNotFound::RETURN_NULL);
				if (clash) {
					throw BinderException(
					    "Cannot register COPY format '%s' for catalog '%s': a COPY format with that name "
					    "already exists.",
					    registered_name, name);
				}

				// Build the CopyFunction, wiring each direction the worker
				// advertises. A format may be "from", "to", or "both"; the FROM
				// and TO sides are independent (copy_from_function.function_info
				// vs cf.function_info), so one CopyFunction can carry both.
				const std::string dir = StringUtil::Lower(fmt.direction);
				const bool wants_from = (dir != "to"); // "from", "both", or unset → reader
				const bool wants_to = (dir == "to" || dir == "both");

				CopyFunction cf(registered_name);
				if (wants_from) {
					// Per-format reader context rides on the copy_from_function's
					// TableFunctionInfo carrier (self-contained, outlives DETACH).
					cf.copy_from_bind = vgi::VgiCopyFromBind;
					cf.copy_from_function = vgi::MakeVgiCopyFromTableFunction();
					cf.copy_from_function.function_info = make_shared_ptr<vgi::VgiCopyFromFunctionInfo>(
					    name, attach_params, attach_result_ptr->attach_opaque_data, fmt.format_name, fmt.handler,
					    fmt.options_schema, setting_names);
				}
				if (wants_to) {
					// Per-format writer context rides on cf.function_info, which
					// copy_to_bind receives directly (no table-function indirection).
					cf.function_info = make_shared_ptr<vgi::VgiCopyToFunctionInfo>(
					    name, attach_params, attach_result_ptr->attach_opaque_data, fmt.format_name, fmt.handler,
					    fmt.options_schema, setting_names);
					vgi::InstallVgiCopyToCallbacks(cf, fmt.ordered);
				}
				cf.extension = registered_name;

				CreateCopyFunctionInfo cf_info(std::move(cf));
				cf_info.comment = fmt.comment.has_value() ? Value(*fmt.comment) : Value();
				for (auto &tag : fmt.tags) {
					cf_info.tags[tag.first] = tag.second;
				}
				system_catalog.CreateCopyFunction(sys_txn, cf_info);

				VgiStorageExtension::CopyFormatEntry entry;
				entry.catalog_name = name;
				entry.registered_name = registered_name;
				entry.info = fmt;
				vgi_ext->InsertCopyFormatLocked(registered_name, std::move(entry));
			}
		}
	}

	// Capture the companion manifest before attach_result_ptr is moved into the
	// catalog below.
	std::vector<vgi::VgiAttachCatalogInfo> companion_manifest;
	if (attach_companions_enabled && attach_result_ptr) {
		companion_manifest = attach_result_ptr->attach_catalogs;
	}

	// Telemetry: catalog facts from the attach result, captured before the result
	// is moved into the catalog below. companion_count reflects what the worker
	// advertised (independent of the attach_companions opt-out); db_types only —
	// never companion targets/DSNs.
	tele.catalog_name = catalog_name;
	tele.has_catalog_version = true;
	tele.catalog_version = attach_result_ptr->catalog_version;
	tele.resolved_impl_version = attach_result_ptr->resolved_implementation_version;
	tele.resolved_data_version = attach_result_ptr->resolved_data_version;
	{
		auto turl = attach_result_ptr->tags.find("vgi_secret_service_url");
		if (turl != attach_result_ptr->tags.end() && !turl->second.empty()) {
			tele.secret_service_url = vgi::VgiScrubLocation(turl->second);
		}
	}
	tele.companion_count = static_cast<int64_t>(attach_result_ptr->attach_catalogs.size());
	for (auto &c : attach_result_ptr->attach_catalogs) {
		tele.companion_types.push_back(c.db_type);
	}

	auto vgi_catalog = make_uniq<VgiCatalog>(db, name, options.access_mode, std::move(attach_params),
	                                          std::move(attach_result_ptr), eager_thresholds);

	// Provision companion catalogs advertised via the catalog_attach
	// `attach_catalogs` manifest (lakehouse federation). Done eagerly here,
	// inside the storage attach callback: reentrancy-safe because DuckDB's
	// databases_lock is NOT held across CreateAttachedDatabase (which invoked
	// us) — a companion registers before this parent (FinalizeAttach).
	//
	// Trust: opt-out (`attach_companions false`) + a scheme allowlist +
	// AcquireCompanion's never-clobber conflict policy (it will not replace a
	// catalog the VGI layer did not itself create). The secret provider was
	// registered above, so a companion's credential lookups resolve through the
	// Orchard catch-all automatically (see docs/companion_catalogs.md).
	if (attach_companions_enabled && !companion_manifest.empty()) {
		auto *vgi_ext = VgiStorageExtension::Find(*context.db);
		if (vgi_ext) {
			static const std::set<std::string> kAllowedCompanionSchemes = {
			    "ducklake", "iceberg", "postgres", "mysql", "duckdb", "sqlite"};
			auto &db_manager = DatabaseManager::Get(context);
			auto &cfg = DBConfig::GetConfig(context);
			std::vector<std::string> referenced;
			// Partial-failure safety: a mid-loop throw (scheme reject, required
			// conflict/failure) must not leak the companions already attached in
			// this ATTACH — release them (registry refcount + detach) before the
			// exception unwinds out of VgiCatalogAttach (whose catalog is never
			// returned, so OnDetach would never run).
			try {
			for (const auto &c : companion_manifest) {
				// Scheme allowlist (defense-in-depth): the scheme is the explicit
				// db_type or the target's prefix. Reject bare local-file paths
				// and unknown schemes.
				std::string scheme = StringUtil::Lower(c.db_type);
				if (scheme.empty()) {
					auto colon = c.target.find(':');
					if (colon != std::string::npos) {
						scheme = StringUtil::Lower(c.target.substr(0, colon));
					}
				}
				if (scheme.empty() || kAllowedCompanionSchemes.find(scheme) == kAllowedCompanionSchemes.end()) {
					throw BinderException(
					    "VGI catalog '%s' advertised companion '%s' with target '%s' whose scheme '%s' is not "
					    "permitted (allowed: ducklake, iceberg, postgres, mysql, duckdb, sqlite).",
					    catalog_name, c.alias, c.target, scheme);
				}

				auto handle_failure = [&](const std::string &reason) {
					if (c.required) {
						throw BinderException(
						    "VGI catalog '%s' could not attach required companion '%s' (target '%s'): %s",
						    catalog_name, c.alias, c.target, reason);
					}
					VGI_LOG(context, "vgi.companion_attach.skipped",
					        {{"alias", c.alias}, {"target", c.target}, {"reason", reason}});
				};

				std::string conflict_target;
				VgiStorageExtension::CompanionOutcome outcome;
				try {
					outcome = vgi_ext->AcquireCompanion(
					    c.alias, c.target, scheme, c.hidden, db_manager,
					    [&]() {
						    AttachInfo cinfo;
						    cinfo.name = c.alias;
						    cinfo.path = c.target;
						    cinfo.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT; // never clobber
						    AttachOptions coptions(cfg.options);
						    coptions.db_type = c.db_type;
						    if (coptions.db_type.empty()) {
							    DBPathAndType::ExtractExtensionPrefix(cinfo.path, coptions.db_type);
						    }
						    // Companions attach at their natural (config/worker-specified)
						    // access mode — writable federation (postgres, a writable
						    // DuckLake, a local duckdb/sqlite file) is supported. Note
						    // the tradeoff: a bare `sqlite`/`duckdb` target that doesn't
						    // exist is CREATED, so a malicious worker can make the client
						    // create empty db files at writable paths (low impact — no
						    // data write on attach, existing non-db files error). Only
						    // attach workers you trust; the scheme allowlist +
						    // never-clobber policy remain the primary guards.
						    if (c.hidden) {
							    coptions.visibility = AttachVisibility::HIDDEN;
						    }
						    for (const auto &kv : c.options) {
							    coptions.options[kv.first] = Value(kv.second);
						    }
						    // Orchard: pre-resolve secret_ref into ATTACH options for
						    // metadata connections that need creds at attach time.
						    // Fail-closed — only when the user opted in (see the flag).
						    vgi::InjectCompanionSecret(context, c.secret_ref, attach_companion_secrets, coptions);
						    db_manager.AttachDatabase(context, cinfo, coptions);
					    },
					    conflict_target);
				} catch (const std::exception &e) {
					handle_failure(e.what());
					continue;
				}
				if (outcome == VgiStorageExtension::CompanionOutcome::CONFLICT) {
					handle_failure("alias already in use by " + conflict_target + " (not replaced)");
					continue;
				}
				referenced.push_back(c.alias);
				VGI_LOG(context, "vgi.companion_attach",
				        {{"alias", c.alias},
				         {"target", c.target},
				         {"shared", outcome == VgiStorageExtension::CompanionOutcome::SHARED ? "true" : "false"}});
			}
			} catch (...) {
				vgi::ReleaseCompanionCatalogs(context, referenced);
				throw;
			}
			vgi_catalog->SetCompanionCatalogs(std::move(referenced));
		}
	}

	// Success path only — fire the attach telemetry event (async, fire-and-forget,
	// honors QUERY_FARM_TELEMETRY_OPT_OUT; never throws).
	tele.extension_version = VGI_EXTENSION_VERSION;
	if (auth) {
		tele.auth_interactive = auth->WasInteractive();
		vgi::OAuthTokenSet token_copy;
		if (auth->GetTokenSetCopy(token_copy)) {
			tele.oauth_issuer = token_copy.identity.issuer;
		}
	}
	tele.attach_duration_ms = static_cast<int64_t>(
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - tele_start).count());
	vgi::VgiSendAttachEvent(context, tele);

	return vgi_catalog;
}

// Create transaction manager for VGI catalog
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	return make_uniq<VgiTransactionManager>(db, vgi_catalog);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_logout() table function
//===--------------------------------------------------------------------===//

struct OAuthLogoutBindData : public TableFunctionData {
	std::string target_catalog; // empty = all catalogs
	bool logout_all = false;
};

struct OAuthLogoutState : public GlobalTableFunctionState {
	struct LogoutRow {
		std::string catalog_name;
		std::string status;
	};
	std::vector<LogoutRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> OAuthLogoutBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<OAuthLogoutBindData>();
	if (input.inputs.size() > 1) {
		throw BinderException("vgi_oauth_logout expects 0 or 1 arguments (catalog name), got %d", input.inputs.size());
	}
	if (!input.inputs.empty()) {
		bind_data->target_catalog = input.inputs[0].GetValue<string>();
	} else {
		bind_data->logout_all = true;
	}
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> OAuthLogoutInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthLogoutState>();
	auto &bind_data = input.bind_data->Cast<OAuthLogoutBindData>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto catalog_name = catalog.GetName();
		if (!bind_data.logout_all && catalog_name != bind_data.target_catalog) {
			continue;
		}

		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (params && params->auth()) {
			params->auth()->ClearTokens();
			state->rows.push_back({catalog_name, "logged_out"});
		}
	}

	return std::move(state);
}

static void OAuthLogoutFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthLogoutState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.status));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_tokens() table function
//===--------------------------------------------------------------------===//

struct OAuthTokensState : public GlobalTableFunctionState {
	struct TokenRow {
		std::string catalog_name;
		std::string origin;
		std::string status;
		bool has_expires;
		int64_t expires_in_seconds;
		bool has_refresh_token;
	};
	std::vector<TokenRow> rows;
	idx_t offset = 0;
};

static LogicalType OAuthStatusEnumType() {
	Vector values(LogicalType::VARCHAR, 3);
	values.SetValue(0, Value("active"));
	values.SetValue(1, Value("expired"));
	values.SetValue(2, Value("none"));
	return LogicalType::ENUM("vgi_oauth_status", values, 3);
}

static unique_ptr<FunctionData> OAuthTokensBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(OAuthStatusEnumType());
	names.push_back("status");
	return_types.push_back(LogicalType::INTERVAL);
	names.push_back("expires_in");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("has_refresh_token");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> OAuthTokensInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthTokensState>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (!params) {
			continue;
		}

		OAuthTokensState::TokenRow row;
		row.catalog_name = catalog.GetName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

		auto &auth = params->auth();
		if (!auth) {
			row.status = "none";
			row.has_expires = false;
			row.expires_in_seconds = 0;
			row.has_refresh_token = false;
		} else {
			auto bearer = auth->GetToken();
			if (!bearer.empty()) {
				row.status = "active";
			} else {
				row.status = "expired";
			}

			vgi::CatalogAuth::TokenInfo token_info;
			if (auth->GetTokenInfo(token_info)) {
				row.has_refresh_token = token_info.has_refresh_token;
				row.has_expires = token_info.has_expires;
				row.expires_in_seconds = token_info.expires_in_seconds;
			} else {
				row.has_refresh_token = false;
				row.has_expires = false;
				row.expires_in_seconds = 0;
			}
		}

		state->rows.push_back(std::move(row));
	}

	return std::move(state);
}

static void OAuthTokensFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthTokensState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.origin));
		output.SetValue(2, count, Value(row.status));
		if (row.status == "active" && row.has_expires) {
			output.SetValue(3, count, Value::INTERVAL(Interval::FromMicro(row.expires_in_seconds * Interval::MICROS_PER_SEC)));
		} else {
			output.SetValue(3, count, Value());
		}
		output.SetValue(4, count, Value::BOOLEAN(row.has_refresh_token));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_identity() table function
//===--------------------------------------------------------------------===//
//
// Emits one row per attached VGI catalog showing the OIDC identity parsed
// from the OAuth id_token (if any). Catalogs without OAuth state appear
// with authenticated=false and NULL identity fields.

struct OAuthIdentityState : public GlobalTableFunctionState {
	struct IdentityRow {
		std::string catalog_name;
		std::string origin;
		bool authenticated = false;
		bool has_identity = false;
		std::string sub;
		std::string email;
		std::string name;
		std::string issuer;
		std::string claims_json;  // empty when no id_token was parsed
	};
	std::vector<IdentityRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> OAuthIdentityBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("authenticated");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("sub");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("email");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("issuer");
	// Raw decoded JWT payload — provider-specific claims (Entra preferred_username/oid/tid,
	// group/role arrays, custom attributes) reachable via JSON path expressions.
	return_types.push_back(LogicalType::JSON());
	names.push_back("claims");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> OAuthIdentityInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthIdentityState>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (!params) {
			continue;
		}

		OAuthIdentityState::IdentityRow row;
		row.catalog_name = catalog.GetName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

		// Get identity from per-catalog auth state
		auto &auth = params->auth();
		if (auth) {
			vgi::OAuthTokenSet token_copy;
			if (auth->GetTokenSetCopy(token_copy)) {
				row.authenticated = token_copy.IsValid();
				if (token_copy.identity.present) {
					row.has_identity = true;
					row.sub = token_copy.identity.sub;
					row.email = token_copy.identity.email;
					row.name = token_copy.identity.name;
					row.issuer = token_copy.identity.issuer;
					row.claims_json = token_copy.identity.claims_json;
				}
			}
		}

		state->rows.push_back(std::move(row));
	}

	return std::move(state);
}

static void OAuthIdentityFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthIdentityState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.origin));
		output.SetValue(2, count, Value::BOOLEAN(row.authenticated));
		output.SetValue(3, count, row.has_identity && !row.sub.empty() ? Value(row.sub) : Value());
		output.SetValue(4, count, row.has_identity && !row.email.empty() ? Value(row.email) : Value());
		output.SetValue(5, count, row.has_identity && !row.name.empty() ? Value(row.name) : Value());
		output.SetValue(6, count, row.has_identity && !row.issuer.empty() ? Value(row.issuer) : Value());
		// The claims column is typed JSON — DuckDB stores JSON as VARCHAR with an alias,
		// so a plain Value(string) lands correctly and returns as JSON on read.
		output.SetValue(7, count,
		                row.has_identity && !row.claims_json.empty() ? Value(row.claims_json) : Value());
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

// ── Orchard remote secret provider diagnostics ──────────────────────────────
// These live here (not a separate .cpp) because the per-DB provider registry is
// a member of the file-local VgiStorageExtension.

struct VgiSecretProvidersData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiSecretProvidersBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names = {"catalog_name", "endpoint", "tie_break_offset", "active", "cached_secrets", "ttl_seconds"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
	                LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT};
	return make_uniq<VgiSecretProvidersData>();
}

static void VgiSecretProvidersScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiSecretProvidersData>();
	if (data.finished) {
		return;
	}
	data.finished = true;

	auto *ext = VgiStorageExtension::Find(*context.db);
	if (!ext) {
		output.SetCardinality(0);
		return;
	}
	auto providers = ext->AllSecretProviders();
	idx_t row = 0;
	for (auto &kv : providers) {
		auto *storage = kv.second;
		output.SetValue(0, row, Value(kv.first));
		output.SetValue(1, row, Value(storage->Endpoint()));
		output.SetValue(2, row, Value::BIGINT(storage->TieBreakOffset()));
		output.SetValue(3, row, Value::BOOLEAN(storage->Active()));
		output.SetValue(4, row, Value::BIGINT(static_cast<int64_t>(storage->CachedSecretCount())));
		output.SetValue(5, row, Value::BIGINT(storage->DefaultTtlSeconds()));
		row++;
	}
	output.SetCardinality(row);
}

// vgi_companion_catalogs(): one row per companion catalog attached by the VGI layer
// (lakehouse federation). Surfaces hidden companions that are invisible to
// duckdb_databases().
struct VgiCompanionsData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiCompanionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names = {"catalog_name", "target", "db_type", "hidden", "refcount"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN,
	                LogicalType::BIGINT};
	return make_uniq<VgiCompanionsData>();
}

static void VgiCompanionsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiCompanionsData>();
	if (data.finished) {
		return;
	}
	data.finished = true;

	auto *ext = VgiStorageExtension::Find(*context.db);
	if (!ext) {
		output.SetCardinality(0);
		return;
	}
	auto companions = ext->AllCompanions();
	idx_t row = 0;
	for (const auto &c : companions) {
		output.SetValue(0, row, Value(c.alias));
		output.SetValue(1, row, Value(c.target));
		output.SetValue(2, row, c.db_type.empty() ? Value(LogicalType::VARCHAR) : Value(c.db_type));
		output.SetValue(3, row, Value::BOOLEAN(c.hidden));
		output.SetValue(4, row, Value::BIGINT(c.refcount));
		row++;
	}
	output.SetCardinality(row);
}

struct VgiSecretFlushData : public TableFunctionData {
	bool finished = false;
	string catalog_filter; // empty = all providers
};

static unique_ptr<FunctionData> VgiSecretFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names = {"flushed"};
	return_types = {LogicalType::BIGINT};
	auto data = make_uniq<VgiSecretFlushData>();
	auto it = input.named_parameters.find("catalog");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		data->catalog_filter = it->second.ToString();
	}
	return std::move(data);
}

static void VgiSecretFlushScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiSecretFlushData>();
	if (data.finished) {
		return;
	}
	data.finished = true;

	int64_t flushed = 0;
	auto *ext = VgiStorageExtension::Find(*context.db);
	if (ext) {
		for (auto &kv : ext->AllSecretProviders()) {
			if (!data.catalog_filter.empty() && kv.first != data.catalog_filter) {
				continue;
			}
			flushed += static_cast<int64_t>(kv.second->FlushCache());
		}
	}
	output.SetValue(0, 0, Value::BIGINT(flushed));
	output.SetCardinality(1);
}

// ============================================================================
// vgi_copy_formats() — diagnostic: one row per (catalog, format, direction,
// option) for every VGI-registered COPY ... FROM format. Modeled on
// vgi_function_arguments(); reads the per-DB registry on VgiStorageExtension.
// ============================================================================

struct VgiCopyFormatRow {
	string catalog_name;
	string format_name;
	string direction;
	bool ordered = false;
	string description;
	bool has_comment = false;
	string comment;
	Value tags; // MAP(VARCHAR, VARCHAR)
	string handler;
	bool has_option = false;
	string option_name;
	string option_type;
	bool has_option_desc = false;
	string option_desc;
};

struct VgiCopyFormatsData : public TableFunctionData {
	vector<VgiCopyFormatRow> rows;
	idx_t offset = 0;
};

static Value TagsToMapValue(const std::map<std::string, std::string> &tags) {
	vector<Value> keys;
	vector<Value> values;
	keys.reserve(tags.size());
	values.reserve(tags.size());
	for (auto &kv : tags) {
		keys.emplace_back(Value(kv.first));
		values.emplace_back(Value(kv.second));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

static unique_ptr<FunctionData> VgiCopyFormatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names = {"catalog_name", "format_name",  "direction",   "ordered",     "format_description", "format_comment",
	         "format_tags",  "handler",      "option_name", "option_type", "option_description"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::BOOLEAN,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR};

	auto data = make_uniq<VgiCopyFormatsData>();
	auto *ext = VgiStorageExtension::Find(*context.db);
	if (!ext) {
		return std::move(data);
	}

	for (auto &entry : ext->AllCopyFormats()) {
		const auto &info = entry.info;
		Value tags_value = TagsToMapValue(info.tags);

		auto base_row = [&]() {
			VgiCopyFormatRow r;
			r.catalog_name = entry.catalog_name;
			// The registered (alias-scoped) name is what users type in FORMAT.
			r.format_name = entry.registered_name;
			r.direction = info.direction;
			r.ordered = info.ordered;
			r.description = info.description;
			r.has_comment = info.comment.has_value();
			r.comment = info.comment.value_or("");
			r.tags = tags_value;
			r.handler = info.handler;
			return r;
		};

		if (info.options_schema && info.options_schema->num_fields() > 0) {
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> types;
			vector<string> opt_names;
			vgi::ArrowSchemaToDuckDBTypes(context, info.options_schema, c_schema, arrow_table, types, opt_names);
			for (idx_t i = 0; i < opt_names.size(); i++) {
				auto field = info.options_schema->field(static_cast<int>(i));
				auto row = base_row();
				row.has_option = true;
				row.option_name = opt_names[i];
				row.option_type = (i < types.size() ? types[i] : LogicalType::ANY).ToString();
				if (field->HasMetadata()) {
					auto doc = field->metadata()->Get(vgi::VGI_DOC_METADATA_KEY);
					if (doc.ok() && !doc.ValueUnsafe().empty()) {
						row.has_option_desc = true;
						row.option_desc = doc.ValueUnsafe();
					}
				}
				data->rows.push_back(std::move(row));
			}
		} else {
			// Format with no declared options: still emit one row.
			data->rows.push_back(base_row());
		}
	}
	return std::move(data);
}

static void VgiCopyFormatsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiCopyFormatsData>();
	idx_t count = 0;
	while (data.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &r = data.rows[data.offset];
		output.SetValue(0, count, Value(r.catalog_name));
		output.SetValue(1, count, Value(r.format_name));
		output.SetValue(2, count, Value(r.direction));
		output.SetValue(3, count, Value::BOOLEAN(r.ordered));
		output.SetValue(4, count, Value(r.description));
		output.SetValue(5, count, r.has_comment ? Value(r.comment) : Value(LogicalType::VARCHAR));
		output.SetValue(6, count, r.tags);
		output.SetValue(7, count, Value(r.handler));
		output.SetValue(8, count, r.has_option ? Value(r.option_name) : Value(LogicalType::VARCHAR));
		output.SetValue(9, count, r.has_option ? Value(r.option_type) : Value(LogicalType::VARCHAR));
		output.SetValue(10, count, r.has_option_desc ? Value(r.option_desc) : Value(LogicalType::VARCHAR));
		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
#if defined(__EMSCRIPTEN__) && VGI_ASYNC_INIT_ENABLED
	// Pre-spawn a bounded pool of background workers at extension load. Required
	// under MAIN_MODULE=1 + pthreads: pthread_create after side-modules are
	// dlopen'd is unreliable (emsdk #19425/#19199/#13303), so we spawn once at
	// load time and keep the workers parked in cv_.wait() until tasks arrive.
	// Pool size must fit within PTHREAD_POOL_SIZE alongside DuckDB's worker
	// threads — see duckdb-wasm/lib/CMakeLists.txt.
	//
	// Only spawned when VGI_ASYNC_INIT_ENABLED == 1 (off by default on WASM).
	// With async init disabled, the pool would sit idle forever — skip it.
	duckdb::vgi::VgiWasmAsyncPool::Instance().EnsureStarted(3);
#endif
#if VGI_POSIX_TRANSPORT
	// Ignore SIGPIPE - we handle broken pipes via EPIPE error from write()
	// This prevents the process from being killed when a worker dies unexpectedly.
	// Windows has no SIGPIPE; the (future) Win32 subprocess backend maps
	// ERROR_BROKEN_PIPE to the same EPIPE-equivalent status instead.
	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	if (sigaction(SIGPIPE, &sa, nullptr) == -1) {
		VGI_STDERR_DEBUG("[VGI] extension.load sigpipe_ignore_failed errno=%d\n", errno);
	}
#endif // VGI_POSIX_TRANSPORT

	// Register profiling atexit handler if VGI_PROFILE is enabled
	vgi::VgiProfileRegisterAtExit();

	// Register VGI log type
	auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
	log_manager.RegisterLogType(make_uniq<VgiLogType>());

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);

	// Register VGI storage extension
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "vgi", make_shared_ptr<VgiStorageExtension>(loader.GetDatabaseInstance()));

	// -------------------------------------------------------------------------
	// Register native GeoArrow Arrow extension types
	// -------------------------------------------------------------------------
	// Enable round-tripping GEOMETRY through Arrow IPC as native struct/list
	// layouts instead of opaque WKB blobs.  Each entry maps a GeoArrow
	// extension name to a DuckDB struct type and a GeometryStorageType used
	// by the spatial extension's Geometry::ToVectorizedFormat /
	// FromVectorizedFormat for conversion.
	//
	// ArrowToDuck (worker → client): struct vector → GEOMETRY via cast
	// DuckToArrow (client → worker): GEOMETRY → struct vector via ToVectorizedFormat
	{
		auto xy = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});

		struct NativeGeoArrow {
			static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
				auto &cast_set = CastFunctionSet::Get(context);
				GetCastFunctionInput cast_input(context);
				auto info = cast_set.GetCastFunction(source.GetType(), result.GetType(), cast_input);
				CastParameters params(info.cast_data.get(), false, nullptr, nullptr);
				info.function(source, result, count, params);
			}
		};

		// DuckToArrow closures per geometry storage type
		// Each lambda captures the storage type for ToVectorizedFormat
#define VGI_GEOARROW_DUCK_TO_ARROW(STORAGE_TYPE)                                                                       \
	[](ClientContext &, Vector &source, Vector &result, idx_t count) {                                                 \
		Geometry::ToVectorizedFormat(source, result, count, STORAGE_TYPE);                                              \
	}

		struct GeoArrowEntry {
			const char *name;
			LogicalType arrow_type;
			cast_duck_arrow_t duck_to_arrow;
		};

		GeoArrowEntry entries[] = {
		    {"geoarrow.point", xy,
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::POINT_XY)},
		    {"geoarrow.linestring", LogicalType::LIST(xy),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::LINESTRING_XY)},
		    {"geoarrow.polygon", LogicalType::LIST(LogicalType::LIST(xy)),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::POLYGON_XY)},
		    {"geoarrow.multipoint", LogicalType::LIST(xy),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTIPOINT_XY)},
		    {"geoarrow.multilinestring", LogicalType::LIST(LogicalType::LIST(xy)),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTILINESTRING_XY)},
		    {"geoarrow.multipolygon", LogicalType::LIST(LogicalType::LIST(LogicalType::LIST(xy))),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTIPOLYGON_XY)},
		};

#undef VGI_GEOARROW_DUCK_TO_ARROW

		for (auto &entry : entries) {
			try {
				config.RegisterArrowExtension(
				    {entry.name, nullptr, nullptr,
				     make_shared_ptr<ArrowTypeExtensionData>(LogicalType::GEOMETRY(), entry.arrow_type,
				                                             NativeGeoArrow::ArrowToDuck, entry.duck_to_arrow)});
			} catch (...) {
				// Extension may already be registered (e.g., by spatial extension in a future version)
			}
		}
	}

	// Register HTTP timeout setting
	config.AddExtensionOption("vgi_http_timeout_seconds",
	                          "Timeout in seconds for VGI HTTP requests (catalog, init, and exchange operations)",
	                          LogicalType::BIGINT, Value::BIGINT(300));

	config.AddExtensionOption(
	    "vgi_secret_default_ttl_seconds",
	    "Default cache TTL (seconds) for credentials fetched from an Orchard remote secret provider. "
	    "Capped per-credential by the credential's own expiry. Read at ATTACH and frozen per-provider.",
	    LogicalType::BIGINT, Value::BIGINT(300));

	// Register OAuth settings
	config.AddExtensionOption("vgi_oauth_timeout_seconds",
	                          "Window in seconds for a human to complete interactive OAuth (device-code or "
	                          "browser/PKCE) authentication. Further capped by the provider's token expires_in.",
	                          LogicalType::BIGINT, Value::BIGINT(120));
	config.AddExtensionOption("vgi_oauth_enabled",
	                          "Enable interactive OAuth (PKCE or device-code) authentication on HTTP 401. "
	                          "Set false to fail fast instead of prompting.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_oauth_flow",
	                          "OAuth flow type: auto (default), device_code, or pkce",
	                          LogicalType::VARCHAR, Value("auto"));
	config.AddExtensionOption("vgi_oauth_prompt",
	                          "OAuth prompt behavior: none (default), login, select_account, or consent",
	                          LogicalType::VARCHAR, Value("none"));

	// Register async prefetch setting. Default is off: async prefetch returns
	// SourceResultType::BLOCKED from table-scan sources, which DuckDB's
	// PositionalTableScanner operator does not handle (it throws
	// NotImplementedException). Other join forms (CROSS / NATURAL / ASOF /
	// LATERAL / INNER / HASH) route through PipelineExecutor and suspend
	// correctly on BLOCKED, so users running queries without POSITIONAL JOIN
	// can opt in via ``SET vgi_async_prefetch=true;`` to reclaim the
	// subprocess-RPC latency hiding.
	config.AddExtensionOption("vgi_async_prefetch",
	                          "Enable async I/O prefetch for VGI table function scans (default off: "
	                          "DuckDB's POSITIONAL JOIN operator does not handle BLOCKED sources)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("vgi_cancel_enabled",
	                          "Notify VGI workers (on both subprocess and HTTP transports) when a stream is torn "
	                          "down early so their on_cancel hook can release resources. Set to false to disable: "
	                          "destructors skip the cancel dispatch entirely, on_cancel is never invoked, and "
	                          "workers learn the stream is gone only via normal stream-close / HTTP TTL.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	// Register join key pushdown settings + optimizer
	config.AddExtensionOption("vgi_join_keys_threshold",
	                          "When a join has a VGI scan on one side, raise DuckDB's "
	                          "dynamic_or_filter_threshold to this value so the build side's distinct join "
	                          "keys are pushed to the worker as an IN filter. This is a threshold, not a "
	                          "cap on keys sent: if the distinct count exceeds it, NO keys are pushed (the "
	                          "filter is not built). Raise-only — never lowers a user-set threshold. "
	                          "0 = disabled. See also vgi_join_keys_max_bytes for the byte-size cap.",
	                          LogicalType::UBIGINT, Value::UBIGINT(100000));
	config.AddExtensionOption("vgi_join_keys_max_bytes",
	                          "Max estimated byte size for join keys batch (skip pushdown if exceeded)",
	                          LogicalType::UBIGINT, Value::UBIGINT(67108864)); // 64MB

	// =========================================================================
	// Multi-scan rewriter MUST register BEFORE VgiJoinOptimizer. Both are
	// pre_optimize_function; they fire in registration order. The rewriter
	// replaces the marker LogicalGet with a LogicalSetOperation(UNION_ALL,
	// [LogicalGet(vgi_fn), ...]) — only after that swap do the inner
	// LogicalGets carry `function == VgiTableFunctionScan`, which is what
	// VgiJoinOptimizer's IsVgiScan() detects to raise the InFilter
	// threshold. If JoinOptimizer ran first, it would walk the unrewritten
	// plan, see only markers (function == MultiBranchMarkerExecute), and
	// miss the join+VGI heuristic.
	//
	// Phase-split rationale (PRE-pushdown rewrite vs. POST-pushdown rewrite
	// for buffered_table) is documented at the buffered_table registration
	// site below. The multi-scan rewriter is intentionally pre-pushdown so
	// that DuckDB's standard filter-pushdown distributes parent filters
	// into each arm after we've produced LogicalSetOperation. =========================================
	config.AddExtensionOption(
	    "vgi_multi_branch_scans",
	    "Rewrite VGI multi-branch table scans into LogicalSetOperation(UNION_ALL, ...) "
	    "via the optimizer extension. Set to false to disable the rewrite — multi-branch "
	    "table scans will then throw at execution time (the marker placeholder's loud-fail). "
	    "Emergency-rollback knob; not generally useful.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));
	vgi::RegisterVgiMultiScanRewriter(config);

	OptimizerExtension::Register(config, VgiJoinOptimizer());

	// Streaming-window optimizer rule: rewrite eligible LogicalWindow ->
	// LogicalVgiStreamingWindow. Gate on a session setting so benchmarks /
	// fallback paths can disable it without recompiling.
	config.AddExtensionOption("vgi_streaming_window",
	                          "Route eligible OVER (...) queries against VGI aggregates with "
	                          "streaming_partitioned=true through the custom streaming operator. "
	                          "Set to false to fall back to PhysicalWindow / WindowCustomAggregator.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	OptimizerExtension::Register(config, VgiStreamingWindowOptimizer());

	// Table sink+source function rewriter — replace LogicalGet of any
	// TableBufferingFunction subclass with our Sink+Source physical op.
	// Gated on a session setting so a regression has an emergency rollback
	// path. When disabled, the loud-failure asserts in VgiTableInOutFunction /
	// VgiTableInOutFinalize will surface a clear error rather than the
	// pre-fix UNION-ALL corruption.
	config.AddExtensionOption("vgi_table_buffering",
	                          "Rewrite calls to TableBufferingFunction subclasses through "
	                          "the Sink+Source PhysicalVgiTableBufferingFunction operator. "
	                          "Set to false to disable the rewrite — table_buffering queries "
	                          "will then throw a clear InternalException instead of running.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	OptimizerExtension::Register(config, VgiTableBufferingRewriter());

	// Batched correlated LATERAL for blended VGI table-in-out functions. Replaces
	// DuckDB's row-by-row PhysicalTableInOutFunction (one worker exchange per input
	// row) with PhysicalVgiLateralBatch (one exchange per input chunk), recovering
	// the input->output row mapping via worker-emitted provenance. optimize_function
	// (post-decorrelation, opaque extension node). See docs / the operator file.
	config.AddExtensionOption("vgi_batch_lateral",
	                          "Batch correlated LATERAL calls to blended VGI table functions through a single "
	                          "worker exchange per input chunk instead of DuckDB's row-by-row driver. "
	                          "Set to false to fall back to the row-by-row PhysicalTableInOutFunction.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	vgi::RegisterVgiLateralBatchRewriter(config);

	// Enforce Table.required_filters at bind/optimize time.
	// Post-optimize so DuckDB's FilterPushdown has settled filters into
	// LogicalGet::table_filters. Zero overhead for tables that don't opt in
	// (the optimizer does an O(plan_size) walk; tables with an empty
	// required-paths list return immediately after the VgiTableEntry cast).
	OptimizerExtension::Register(config, VgiRequiredFiltersOptimizer());

	// VgiMultiScanRewriter is registered BEFORE VgiJoinOptimizer above —
	// see the comment block at that site for ordering rationale. It's
	// pre_optimize_function (rewrites into standard DuckDB operators that
	// benefit from filter pushdown). VgiStreamingWindowOptimizer and
	// VgiTableBufferingRewriter above are optimize_function (post-pushdown)
	// because their LogicalExtensionOperator outputs are opaque to pushdown.

	// Register worker pool settings
	config.AddExtensionOption("vgi_worker_pool_idle_limit_seconds",
	                          "Maximum idle time in seconds before pooled workers are removed", LogicalType::BIGINT,
	                          Value::BIGINT(5));
	config.AddExtensionOption("vgi_worker_pool_max", "Default per-path pool limit for VGI workers (0 = disabled)",
	                          LogicalType::BIGINT, Value::BIGINT(256));

	// Result cache (milestone 1: in-memory tier). Caches the complete result of
	// a worker-advertised-cacheable table-function call and serves identical
	// future calls from memory. Double-gated: only results advertising
	// vgi.cache.* are stored, and a catalog opts out with the `cache` ATTACH option.
	config.AddExtensionOption("vgi_result_cache",
	                          "Enable the VGI table-function result cache (master switch; still "
	                          "double-gated by worker advertisement + per-catalog `cache` option)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_result_cache_max_bytes",
	                          "Global byte cap for the in-memory result cache (LRU eviction above it)",
	                          LogicalType::UBIGINT, Value::UBIGINT(268435456));
	config.AddExtensionOption("vgi_result_cache_max_entry_bytes",
	                          "Per-entry byte cap; capture aborts (streams uncached) above it",
	                          LogicalType::UBIGINT, Value::UBIGINT(67108864));
	config.AddExtensionOption("vgi_result_cache_max_entries",
	                          "Entry-count cap for the in-memory result cache (LRU eviction above it; "
	                          "0 = unlimited). Bounds unbounded small-entry accumulation under the byte cap",
	                          LogicalType::UBIGINT, Value::UBIGINT(131072));
	config.AddExtensionOption("vgi_result_cache_max_inflight_bytes",
	                          "Process-global budget for in-flight capture buffers; a capture that would "
	                          "exceed it streams uncached (bounds concurrent-capture RAM; 0 = unbounded)",
	                          LogicalType::UBIGINT, Value::UBIGINT(268435456));
	config.AddExtensionOption("vgi_result_cache_disk_reap_interval_seconds",
	                          "How often the on-disk result-cache tier is reaped (reading every ref is "
	                          "O(total refs) I/O, so this runs on a coarser cadence than the 1s memory reap)",
	                          LogicalType::UBIGINT, Value::UBIGINT(60));
	config.AddExtensionOption("vgi_result_cache_default_ttl_seconds",
	                          "Default cache TTL when a worker advertises cacheability without a ttl "
	                          "(0 = require a worker-supplied ttl/expires)",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));
	config.AddExtensionOption("vgi_result_cache_revalidate_min_bytes",
	                          "Minimum stored-payload size before a stale revalidatable entry is "
	                          "conditionally revalidated (below it, refetch instead of a conditional request)",
	                          LogicalType::UBIGINT, Value::UBIGINT(262144));
	config.AddExtensionOption("vgi_result_cache_dir",
	                          "Directory for the on-disk result-cache tier (content-addressed store; "
	                          "cross-process + cross-restart). Empty = disk tier off (memory only)",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption("vgi_result_cache_disk_max_bytes",
	                          "Byte cap for the on-disk result-cache tier (0 = disk tier off)",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));
	config.AddExtensionOption("vgi_result_cache_disk_compression",
	                          "Compression codec for the on-disk result-cache tier (Arrow built-in IPC "
	                          "compression, applied per batch so seek is preserved; memory tier is never "
	                          "compressed): 'zstd' (default), 'lz4', or 'none'. Default-on when the disk "
	                          "tier is enabled",
	                          LogicalType::VARCHAR, Value("zstd"));
	config.AddExtensionOption("vgi_result_cache_disk_compression_level",
	                          "zstd compression level for the on-disk result-cache tier (ignored for "
	                          "lz4/none). Keep it low — the default 1 is Pareto-optimal (near-zstd-3 ratio "
	                          "at ~half the CPU)",
	                          LogicalType::UBIGINT, Value::UBIGINT(1));
	config.AddExtensionOption("vgi_result_cache_exchange_disk_max_refs",
	                          "File-count cap for EXCHANGE-mode disk entries (streaming table-in-out / "
	                          "correlated LATERAL / buffered). Every exchange memo may persist to disk "
	                          "regardless of payload size (so a small-but-expensive result still warms the "
	                          "cross-process cache); the reaper LRU-evicts oldest exchange refs above this "
	                          "count so per-input-chunk fan-out can't spray unbounded files. Scoped to "
	                          "exchange refs so a memo flood never evicts a large producer entry (0 = "
	                          "unbounded; default 100000). Loose store only; the packed backend below "
	                          "bounds file count structurally",
	                          LogicalType::UBIGINT, Value::UBIGINT(100000));
	config.AddExtensionOption("vgi_result_cache_pack",
	                          "Route SMALL on-disk result-cache entries into append-only per-process pack "
	                          "files + a rebuildable index (git-style loose-vs-packed split) instead of a "
	                          "loose object+ref file pair each, so thousands of tiny per-input-chunk exchange "
	                          "memos cost a few files. Large entries stay loose. Default ON (the disk tier "
	                          "itself is opt-in, so this only bites once a cache dir is configured)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_result_cache_pack_max_entry_bytes",
	                          "Route threshold for the packed disk backend: on-disk entries below this size "
	                          "are packed, at/above are stored as loose objects (default 262144 = 256 KB)",
	                          LogicalType::UBIGINT, Value::UBIGINT(262144));
	config.AddExtensionOption("vgi_result_cache_pack_target_bytes",
	                          "Roll to a fresh pack file once the current one exceeds this size (bounds one "
	                          "compaction unit; default 67108864 = 64 MB)",
	                          LogicalType::UBIGINT, Value::UBIGINT(67108864));
	config.AddExtensionOption("vgi_result_cache_pack_compaction_dead_pct",
	                          "Compact an owned pack file when this percent of its bytes is dead "
	                          "(expired/evicted); rewrites live records to a fresh pack and drops the old "
	                          "(git gc). 0..100, default 50",
	                          LogicalType::UBIGINT, Value::UBIGINT(50));
	config.AddExtensionOption("vgi_exchange_input_dedup",
	                          "Before an exchange-mode MAP call (scalar / streaming table-in-out / batched "
	                          "correlated LATERAL), ship only the DISTINCT worker-input tuples in each chunk "
	                          "to the worker and scatter the results back — turning e.g. 2048 evals over a "
	                          "low-cardinality column into the distinct count. Compute-only (no cache); sound "
	                          "under the same per-row-purity the cacheability opt-in asserts. Default ON",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption(
	    "vgi_result_cache_partition_scope",
	    "Per-PARTITION result caching for SINGLE_VALUE_PARTITIONS table functions that advertise "
	    "vgi.cache.partition_scope. When on, a cacheable partitioned scan ALSO caches its result split "
	    "by partition value (one entry per distinct partition-value tuple), so a later =/IN-filtered "
	    "scan on the partition column(s) serves the requested partitions from cache without the worker. "
	    "Additive — the whole-scan entry is still stored/served. Default ON",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption(
	    "vgi_result_cache_partition_max_enumerated",
	    "Cap on the number of distinct partitions handled per scan by the per-partition cache: bounds "
	    "both the enumerated =/IN cross-product at serve time and the distinct-partition count at "
	    "capture (split) time. Over the cap, the scan falls back to the whole-scan cache / worker. "
	    "Default 1024",
	    LogicalType::UBIGINT, Value::UBIGINT(1024));
	config.AddExtensionOption("vgi_result_cache_per_value",
	                          "CEILING (not an enabler) over per-VALUE memoization for exchange-mode maps: "
	                          "after input dedup, memoize the worker output keyed on the individual input "
	                          "tuple, so a fully-warm distinct set serves without the worker (cross-chunk / "
	                          "cross-query / cross-restart value reuse the per-chunk cache misses). The tier "
	                          "is OFF unless the worker advertises `vgi.cache.per_value` on its output batch, "
	                          "because a per-value serve's fixed cost (key probe + decode + assembly) only "
	                          "pays back when one worker call is dearer than that — for a cheap map it is a "
	                          "large net loss. Setting this false vetoes the tier even for a worker that asks "
	                          "for it; setting it true does NOT enable it. Default true (no veto)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_result_cache_per_value_max_stores_per_chunk",
	                          "Cap on how many NEW per-value memo entries a single input chunk may store "
	                          "(0 = unlimited). Bounds entry-count amplification on a high-cardinality input "
	                          "(one distinct value → one tiny entry): a chunk memoizes at most this many new "
	                          "values, the rest are recomputed next time. A cap on STORES, not lookups, so it "
	                          "never breaks store-then-hit for a low-cardinality workload (K new < cap → all "
	                          "stored). Default 256",
	                          LogicalType::UBIGINT, Value::UBIGINT(256));
	config.AddExtensionOption("vgi_result_cache_per_value_disk_max_bytes",
	                          "Cap on the on-disk size (bytes) of the per-value memo SQLite store when "
	                          "vgi_result_cache_dir is set. Over the cap, the backend reaps expired rows and "
	                          "evicts least-recently-used entries (LRU) to stay under it — enforced on store "
	                          "(a cache miss), off the hot path. 0 = unlimited. Default 1 GiB",
	                          LogicalType::UBIGINT, Value::UBIGINT(1073741824ULL));
	config.AddExtensionOption("vgi_exchange_per_batch_min_distinct_ratio",
	                          "DEPRECATED / NO-OP. Formerly suppressed the coarse per-chunk (M2) exchange-cache "
	                          "entry when the chunk's distinct ratio (K/N) fell below this, on the theory that "
	                          "the per-value tier already covered an identical-chunk replay. It does not: an M2 "
	                          "serve is ONE decode per chunk while the per-value reassembly of the same rows is "
	                          "K decodes plus a K-way concat, so suppressing M2 made the warm path ~14x slower "
	                          "rather than cheaper. The coarse entry is now always stored when eligible and this "
	                          "setting is ignored. Still accepted so existing scripts do not error",
	                          LogicalType::DOUBLE, Value::DOUBLE(0.5));

	// Cache directory for worker binaries downloaded via github:// / github-auto://
	// LOCATIONs. Empty (default) → ${XDG_CACHE_HOME:-~/.cache}/vgi/releases. Must be
	// on an exec-capable filesystem (not a noexec runtime/tmp mount).
	config.AddExtensionOption("vgi_github_cache_dir",
	                          "Cache directory for worker binaries downloaded from GitHub releases "
	                          "(github:// / github-auto:// LOCATIONs); empty = ${XDG_CACHE_HOME:-~/.cache}/vgi/releases",
	                          LogicalType::VARCHAR, Value(""));

	// Eager-load thresholds. Per-kind: a schema's estimated_object_count[kind]
	// is compared against the corresponding value here; below or equal triggers
	// a single bulk LoadEntries() instead of per-name single-entry RPCs. Read
	// once at ATTACH time and snapshotted onto the VgiCatalog — mid-session SET
	// changes do NOT affect already-attached catalogs.
	{
		vector<Value> threshold_keys;
		vector<Value> threshold_values;
		for (const char *kind : {"table", "view", "index", "scalar_function", "aggregate_function",
		                         "table_function", "macro"}) {
			threshold_keys.emplace_back(kind);
			threshold_values.emplace_back(Value::BIGINT(1000));
		}
		auto default_threshold = Value::MAP(LogicalType::VARCHAR, LogicalType::BIGINT,
		                                    std::move(threshold_keys), std::move(threshold_values));
		config.AddExtensionOption(
		    "vgi_eager_load_threshold",
		    "Per-object-kind threshold (keyed by VgiCatalogSet::CacheKindName: table, view, index, "
		    "scalar_function, aggregate_function, table_function, macro). When a schema's "
		    "estimated_object_count[kind] is <= the threshold, the first GetEntry() triggers a single "
		    "bulk LoadEntries() instead of N per-name RPCs. Read at ATTACH; mid-session SET requires "
		    "re-ATTACH to take effect.",
		    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT), std::move(default_threshold));
	}

	// When a worker reports estimated_object_count[kind] == 0, the client
	// treats it as a hard guarantee and skips both the bulk
	// catalog_schema_contents_* RPC and the per-name single-entry RPCs for
	// that kind (table/view/index). Set this to false to disable the bypass —
	// every elided RPC will fire instead. Used by support engineers to rule
	// the bypass in/out without restarting the worker; read per-call so it
	// takes effect immediately (no re-attach needed).
	config.AddExtensionOption(
	    "vgi_trust_empty_kinds",
	    "Trust worker assertions that estimated_object_count[kind] == 0 means the kind is empty "
	    "(skip catalog_schema_contents_* RPC). Set to false to force every RPC to fire even when "
	    "the worker reports zero — debug escape hatch for diagnosing worker bugs.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));

	// Set default pool settings for paths without explicit per-path config
	// (e.g., direct vgi_table_function() calls that don't go through ATTACH)
	vgi::VgiWorkerPool::Instance().SetDefaultSettings({256, 5});

	// Configure the result-cache global byte caps (process-global; match the
	// AddExtensionOption defaults above). SET on these settings tightens the
	// per-query capture guard (read from context at scan time); the global LRU
	// cap here governs eviction.
	vgi::VgiResultCache::Instance().Configure(vgi::VgiResultCache::Settings{});

	// Register VGI table functions
	RegisterVgiCatalogsFunction(loader);
	RegisterVgiTableFunction(loader);

	// Register the synthetic catalog-scan function under its name. VgiTableEntry
	// builds an unnamed-in-catalog "vgi_table_scan" TableFunction per scan, but
	// DuckDB's LogicalOperator::Copy() round-trips a LogicalGet through
	// serialize/deserialize (e.g. the WindowSelfJoin optimizer duplicating a
	// `COUNT(*) OVER (PARTITION BY ...)` scan), and FunctionSerializer resolves
	// the function *by name* from the catalog on the way back. Without a
	// registered entry that lookup throws "Table Function with name
	// vgi_table_scan does not exist!". This entry exists solely to carry the
	// deserialize callback; it has no bind, so a direct call still fails at bind
	// time. VgiTableScanDeserialize reassigns `function` to a fully-configured
	// scan, so the flags/callbacks set here are only placeholders.
	{
		TableFunction scan_func("vgi_table_scan", {}, vgi::VgiTableFunctionScan, nullptr,
		                        vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
		scan_func.serialize = VgiTableEntry::VgiTableScanSerialize;
		scan_func.deserialize = VgiTableEntry::VgiTableScanDeserialize;
		scan_func.projection_pushdown = true;
		scan_func.filter_pushdown = true;
		CreateTableFunctionInfo scan_info(scan_func);
		scan_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Internal: the physical scan operator for tables in an attached VGI catalog. DuckDB binds it "
		    "automatically when you query a VGI table (SELECT ... FROM mycatalog.schema.table); it exists in "
		    "the function list only so serialized query plans can resolve it by name. It takes no arguments "
		    "and cannot be called directly — use a normal table reference instead.",
		    {}, {}, {}));
		loader.RegisterFunction(std::move(scan_info));
	}

	// Register the native-delegation marker function under its name. Same
	// motivation as vgi_table_scan above: built-in optimizer passes that run
	// BEFORE VgiRequiredFiltersOptimizer (e.g. WindowSelfJoin,
	// CommonSubplanOptimizer) may call LogicalOperator::Copy() on a LogicalGet
	// while it still wraps the marker. FunctionSerializer::DeserializeBase
	// resolves table functions by name from the system catalog on the way
	// back, so without a registered entry that lookup throws "Table Function
	// with name vgi_native_delegation_marker does not exist!" — opaque error.
	//
	// We give the registered entry a `deserialize` that throws a CLEAR
	// SerializationException. Today's built-in callers (WindowSelfJoin,
	// CommonSubplanOptimizer) wrap their Copy() in try/catch and bail
	// gracefully (perf regression, not wrong result). Any future caller that
	// doesn't try/catch gets a message pointing directly at the real bug:
	// VgiRequiredFiltersOptimizer didn't fire when it should have.
	{
		TableFunction marker_func = MakeNativeDelegationMarkerFunction();
		marker_func.deserialize = [](Deserializer &, TableFunction &) -> unique_ptr<FunctionData> {
			throw SerializationException(
			    "VGI native-delegation marker function 'vgi_native_delegation_marker' "
			    "is being deserialized — VgiRequiredFiltersOptimizer must run BEFORE "
			    "any pass that calls LogicalOperator::Copy() on the LogicalGet wrapping "
			    "it. This usually means the optimizer extension was not registered or "
			    "an earlier built-in pass moved the LogicalGet across an operator that "
			    "the rewriter's tree walk doesn't descend into. File a bug — this is "
			    "an unreachable state in normal use.");
		};
		loader.RegisterFunction(marker_func);
	}

	// Register worker pool diagnostic functions
	vgi::RegisterVgiWorkerPoolFunction(loader);
	vgi::RegisterVgiWorkerPoolStatsFunction(loader);
	vgi::RegisterVgiWorkerPoolFlushFunction(loader);
	vgi::RegisterVgiGithubCacheFunction(loader);
	vgi::RegisterVgiGithubCacheFlushFunction(loader);

	// Register table statistics diagnostic function
	vgi::RegisterVgiTableStatisticsFunction(loader);

	// Register cache management function
	vgi::RegisterVgiClearCacheFunction(loader);

	// Register result-cache diagnostics
	vgi::RegisterVgiResultCacheFunction(loader);
	vgi::RegisterVgiResultCacheFlushFunction(loader);
	vgi::RegisterVgiResultCacheReapFunction(loader);
	vgi::RegisterVgiResultCacheStatsFunction(loader);

	// Register Orchard remote secret provider diagnostics
	{
		TableFunction copy_formats_func("vgi_copy_formats", {}, VgiCopyFormatsScan, VgiCopyFormatsBind);
		CreateTableFunctionInfo copy_formats_info(copy_formats_func);
		copy_formats_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "List the custom COPY ... FROM formats registered by attached VGI catalogs, one row per (catalog, "
		    "format, direction, option). Format names are scoped by the attach alias ('<alias>.<format>') — the "
		    "'format_name' column is the exact string to type in FORMAT. Options surface name/type/description "
		    "from the handler's argument metadata.",
		    {}, {}, {"SELECT * FROM vgi_copy_formats();"}));
		loader.RegisterFunction(std::move(copy_formats_info));

		TableFunction providers_func("vgi_secret_providers", {}, VgiSecretProvidersScan, VgiSecretProvidersBind);
		CreateTableFunctionInfo providers_info(providers_func);
		providers_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "List the Orchard remote secret providers auto-registered by attached VGI catalogs, one row per "
		    "provider with its endpoint, tie-break offset, active flag, cached-secret count, and cache TTL. "
		    "A provider appears here when an attached VGI catalog advertises a secret-service URL.",
		    {}, {}, {"SELECT * FROM vgi_secret_providers();"}));
		loader.RegisterFunction(std::move(providers_info));

		TableFunction companions_func("vgi_companion_catalogs", {}, VgiCompanionsScan, VgiCompanionsBind);
		CreateTableFunctionInfo companions_info(companions_func);
		companions_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "List the companion catalogs attached by VGI catalogs (lakehouse federation), one row per companion "
		    "with its catalog_name (alias), target, db_type, hidden flag, and refcount (how many attached VGI "
		    "catalogs share it). Surfaces `hidden` companions that are invisible to duckdb_databases().",
		    {}, {}, {"SELECT * FROM vgi_companion_catalogs();"}));
		loader.RegisterFunction(std::move(companions_info));

		TableFunction flush_func("vgi_secret_provider_flush", {}, VgiSecretFlushScan, VgiSecretFlushBind);
		flush_func.named_parameters["catalog"] = LogicalType::VARCHAR;
		CreateTableFunctionInfo flush_info(flush_func);
		flush_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Clear the TTL cache of one Orchard remote secret provider, or all providers when 'catalog' is "
		    "omitted. Returns the count of positive (found) secrets dropped.",
		    {"catalog"}, {LogicalType::VARCHAR},
		    {"SELECT * FROM vgi_secret_provider_flush();",
		     "SELECT * FROM vgi_secret_provider_flush(catalog := 'mycatalog');"}));
		loader.RegisterFunction(std::move(flush_info));
	}

	// Register multi-branch diagnostic function — one row per branch per
	// VGI table, across every attached VGI catalog. See vgi_table_branches_function.cpp.
	vgi::RegisterVgiTableBranchesFunction(loader);

	// Register per-argument diagnostic function — one row per (catalog, schema,
	// function, argument) with named/positional/const/varargs/type + vgi_doc.
	vgi::RegisterVgiFunctionArgumentsFunction(loader);

	// Register OAuth diagnostic/management functions
	{
		// vgi_oauth_logout([origin])
		TableFunction logout_func("vgi_oauth_logout", {}, OAuthLogoutFunction, OAuthLogoutBind, OAuthLogoutInit);
		logout_func.varargs = LogicalType::VARCHAR;
		CreateTableFunctionInfo logout_info(logout_func);
		logout_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Forget cached OAuth tokens. With no arguments, clears every stored token; pass one or more OAuth "
		    "origins to clear only those. Returns one row per cleared origin.",
		    {"origin"}, {LogicalType::VARCHAR},
		    {"SELECT * FROM vgi_oauth_logout();",
		     "SELECT * FROM vgi_oauth_logout('https://auth.example.com');"}));
		loader.RegisterFunction(std::move(logout_info));

		// vgi_oauth_tokens()
		TableFunction tokens_func("vgi_oauth_tokens", {}, OAuthTokensFunction, OAuthTokensBind, OAuthTokensInit);
		CreateTableFunctionInfo tokens_info(tokens_func);
		tokens_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Show the OAuth tokens the extension has cached, one row per origin, with each token's expiry and "
		    "refresh state. Token values themselves are never exposed. Pair with vgi_oauth_logout() to clear "
		    "stale or unwanted tokens.",
		    {}, {}, {"SELECT * FROM vgi_oauth_tokens();"}));
		loader.RegisterFunction(std::move(tokens_info));

		// vgi_oauth_identity() — surfaces OIDC identity claims per attached VGI catalog
		TableFunction identity_func("vgi_oauth_identity", {}, OAuthIdentityFunction, OAuthIdentityBind,
		                            OAuthIdentityInit);
		CreateTableFunctionInfo identity_info(identity_func);
		identity_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Report the OIDC identity for each attached VGI catalog (catalog_name, origin, authenticated, sub, "
		    "email, name, issuer, and the full decoded id_token claims as JSON). Reach provider-specific fields "
		    "via the claims JSON, e.g. claims->>'$.hd' for Google Workspace.",
		    {}, {}, {"SELECT * FROM vgi_oauth_identity();"}));
		loader.RegisterFunction(std::move(identity_info));
	}
}

void VgiExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string VgiExtension::Name() {
	return "vgi";
}

std::string VgiExtension::Version() const {
	return VGI_EXTENSION_VERSION;
}

namespace vgi {
VgiCancelDispatcher *FindVgiCancelDispatcher(DatabaseInstance &db) {
	return ::duckdb::VgiStorageExtension::FindCancelDispatcher(db);
}
} // namespace vgi

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(vgi, loader) {
	duckdb::LoadInternal(loader);
}
}
