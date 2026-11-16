// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#define DUCKDB_EXTENSION_MAIN

#include "vgi_engine_compat.hpp"
#include "vgi_arrow.hpp"
#include "vgi_settings_defaults.hpp"
#include "vgi_extension.hpp"
#include "vgi_settings.hpp"
#include "vgi_memo_arena.hpp"

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
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/planner/extension_callback.hpp"
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
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/parser/keyword_helper.hpp"
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
#include "vgi_attach_credentials.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_engine_features.hpp"
#include "vgi_protocols.hpp"
#include "vgi_query_function.hpp"
#include "vgi_projection_pushdown.hpp"
#include "vgi_spatial_extent_pushdown.hpp"
#include "vgi_attach_tickets.hpp"
#include "vgi_reflection.hpp"
#include "vgi_copy_from_impl.hpp"
#include "vgi_copy_to_impl.hpp"
#include "vgi_exception.hpp"
#include "vgi_protocol_constants.hpp"
#include "vgi_container_runtime.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_cookie_jar.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "vgi_aggregate_function_impl.hpp"
#include "vgi_global_functions.hpp"
#include "vgi_iroh_config.hpp"
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
#include "vgi_table_function_impl.hpp"
#include "vgi_transport.hpp"
#include "vgi_location_policy.hpp"
#include "vgi_batch_validation.hpp"
#include "vgi_worker_pool.hpp"
#include "vgi_table_statistics_function.hpp"
#include "vgi_table_branches_function.hpp"
#include "vgi_function_arguments_function.hpp"
#include "vgi_secret_types_function.hpp"
#include "vgi_catalog_contents_function.hpp"
#include "vgi_clear_cache.hpp"
#include "vgi_result_cache.hpp"
#include "vgi_companion_catalogs.hpp"
#include "vgi_github_functions.hpp"
#include "vgi_github.hpp"
#include "vgi_database_worker.hpp"
#include "vgi_worker_package_functions.hpp"
#include "vgi_worker_pool_functions.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"

#define VGI_EXTENSION_VERSION "2026072901"

namespace duckdb {

static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input);
static unique_ptr<BaseSecret> IrohCreateSecret(ClientContext &context, CreateSecretInput &input);
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options);
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog);

// Post-optimize: replace a LogicalWindow with LogicalVgiStreamingWindow when
// EVERY expression is a streaming_partitioned VGI aggregate over a cumulative
// frame (UNBOUNDED PRECEDING -> CURRENT ROW) with identical PARTITION BY /
// ORDER BY keys; otherwise leave it for PhysicalWindow. Mixed nodes are not split.

class VgiStreamingWindowOptimizer : public OptimizerExtension {
public:
	VgiStreamingWindowOptimizer() {
		optimize_function = Optimize;
	}

private:
	static bool IsCumulativeFrame(const BoundWindowExpression &wexpr) {
		if (wexpr.WindowStart() != WindowBoundary::UNBOUNDED_PRECEDING) {
			return false;
		}
		switch (wexpr.WindowEnd()) {
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
		if (!wexpr.AggregateFunction()) {
			return false;
		}
		auto info = wexpr.AggregateFunction()->GetFunctionInfo().get();
		if (!info) {
			return false;
		}
		auto vgi_info = dynamic_cast<vgi::VgiAggregateFunctionInfo *>(info);
		if (!vgi_info || !vgi_info->streaming_partitioned) {
			return false;
		}
		// Streaming-open ships an empty Arguments envelope, so a const positional
		// would be silently dropped (see BuildStreamingOpenRequest).
		for (bool is_const : vgi_info->positional_is_const) {
			if (is_const) {
				return false;
			}
		}
		if (!IsCumulativeFrame(wexpr)) {
			return false;
		}
		// EXCLUDE produces multiple subframes per row.
		if (wexpr.WindowExclude() != WindowExcludeMode::NO_OTHER) {
			return false;
		}
		// DISTINCT, FILTER and argument ORDER BY are left to PhysicalWindow.
		if (wexpr.Distinct() || wexpr.Filter() || !wexpr.ArgOrders().empty()) {
			return false;
		}
		return true;
	}

	// The operator is fed in expression 0's sort order and projects each
	// expression's own keys, so every expression must use the same keys in the
	// same positions; otherwise other expressions see out-of-order rows.
	static bool HasSameWindowKeys(const BoundWindowExpression &lhs, const BoundWindowExpression &rhs) {
		if (lhs.Partitions().size() != rhs.Partitions().size() || lhs.OrderBy().size() != rhs.OrderBy().size()) {
			return false;
		}
		for (idx_t i = 0; i < lhs.Partitions().size(); i++) {
			if (!lhs.Partitions()[i]->Equals(*rhs.Partitions()[i])) {
				return false;
			}
		}
		for (idx_t i = 0; i < lhs.OrderBy().size(); i++) {
			if (!lhs.OrderBy()[i].Equals(rhs.OrderBy()[i])) {
				return false;
			}
		}
		return true;
	}

	static void Rewrite(unique_ptr<LogicalOperator> &op) {
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
		auto &first = window.expressions[0]->Cast<BoundWindowExpression>();
		for (idx_t i = 1; i < window.expressions.size(); i++) {
			if (!HasSameWindowKeys(first, window.expressions[i]->Cast<BoundWindowExpression>())) {
				return; // mixed keys: PhysicalWindow sorts per key set
			}
		}

		auto streaming_op = make_uniq<vgi::LogicalVgiStreamingWindow>(window.window_index);
		streaming_op->expressions = std::move(window.expressions);
		streaming_op->children = std::move(window.children);

		// The streaming operator does not sort (the worker aggregates in arrival
		// order), so add the sort PhysicalWindow would have done, or ORDER BY
		// windows over unsorted input give wrong running values. An empty
		// projection_map keeps the child's bindings, so streaming_op's bound
		// expressions still resolve. The key check above makes [0] representative.
		auto &wexpr = streaming_op->expressions[0]->Cast<BoundWindowExpression>();
		if (!wexpr.Partitions().empty() || !wexpr.OrderBy().empty()) {
			vector<BoundOrderByNode> sort_orders;
			// Partition keys first (any order makes partitions contiguous), then
			// the window ORDER BY verbatim so ASC/DESC and NULLS match the frame.
			for (auto &p : wexpr.Partitions()) {
				sort_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, p->Copy());
			}
			for (auto &o : wexpr.OrderBy()) {
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
		                 static_cast<unsigned long long>(window.window_index.index),
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

// Post-optimize: replace each LogicalGet whose bind_data is a table_buffering
// VgiTableInOutBindData with a LogicalVgiTableBufferingFunction that takes over
// its bind_data. Decorrelation has already run, so the get is a plain node.

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
		// The child, if any, is the table-input subquery; it moves to the new op.
		D_ASSERT(get.children.size() <= 1);

		// LogicalGet::ResolveTypes() already narrowed `get.types` to the
		// projection; use them as return_types so parent bindings resolve. The
		// full worker schema goes in all_column_types/all_column_names.
		auto post_proj_types = get.types;
		auto post_proj_names = vector<string>();
		const auto &all_col_ids = get.GetColumnIds();
		if (!get.projection_ids.empty()) {
			post_proj_names.reserve(get.projection_ids.size());
			for (auto proj_id : get.projection_ids) {
				const auto &idx = all_col_ids[proj_id.GetIndex()];
				post_proj_names.push_back(idx.IsVirtualColumn() ? string("__virtual__")
				                                                : get.names[idx.GetPrimaryIndex()].GetIdentifierName());
			}
		} else if (!all_col_ids.empty()) {
			post_proj_names.reserve(all_col_ids.size());
			for (const auto &idx : all_col_ids) {
				post_proj_names.push_back(idx.IsVirtualColumn() ? string("__virtual__")
				                                                : get.names[idx.GetPrimaryIndex()].GetIdentifierName());
			}
		} else {
			post_proj_names = IdentifiersToStrings(get.names);
		}
		D_ASSERT(post_proj_types.size() == post_proj_names.size());

		auto rewritten = make_uniq<vgi::LogicalVgiTableBufferingFunction>(
		    get.table_index, std::move(post_proj_types), std::move(post_proj_names),
		    std::move(get.bind_data));
		rewritten->children = std::move(get.children);

		// Capture pushdown state here: the buffered path's custom operator gets
		// no TableFunctionInitInput (the pure-table path captures it at InitGlobal).
		rewritten->all_column_types = get.returned_types;
		rewritten->all_column_names = IdentifiersToStrings(get.names);

		// The catalog set enabled table_func.projection_pushdown on this same
		// flag, so column_ids holds real column indices only when it is true.
		const auto *bd_for_caps =
		    static_cast<const vgi::VgiTableInOutBindData *>(rewritten->bind_data.get());
		const bool worker_supports_projection =
		    bd_for_caps && bd_for_caps->projection_pushdown;

		// projection_ids: worker-schema indices of the columns the parent op
		// actually wants. Match the pure-table convention
		// (``VgiTableFunctionInitGlobal``): when the function declares
		// projection_pushdown=true, project ``get.GetColumnIds()``. When
		// ``get.projection_ids`` is non-empty (a secondary projection on top
		// of column_ids; rare in practice), apply it on top below.
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
					const auto &idx = all_col_ids[proj_id.GetIndex()];
					rewritten->projection_ids.push_back(static_cast<int32_t>(
					    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                          : idx.GetPrimaryIndex()));
				}
			}
		}
		// column_ids is the projected output-to-worker mapping used later by
		// ArrowToDuckDB. DuckDB 2.0 also keys table_filters by positions
		// in this vector, after projection pruning.
		rewritten->column_ids.reserve(all_col_ids.size());
		for (const auto &idx : all_col_ids) {
			rewritten->column_ids.push_back(static_cast<int32_t>(
			    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID : idx.GetPrimaryIndex()));
		}

		// FilterPushdown already delegated these predicates to the scan, so a
		// serialization failure must abort rather than drop them.
		if (get.table_filters.HasFilters()) {
			auto bd_typed = static_cast<vgi::VgiTableInOutBindData *>(rewritten->bind_data.get());
			auto worker_path = bd_typed && bd_typed->attach_params
			                       ? bd_typed->attach_params->worker_path()
			                       : std::string{};
			vector<column_t> filter_column_ids;
			for (const auto &index : all_col_ids) {
				filter_column_ids.push_back(index.GetPrimaryIndex());
			}
			auto serialized = vgi::VgiSerializeFilters(
			    context, filter_column_ids, &get.table_filters, rewritten->all_column_names, worker_path, "", -1,
			    nullptr, vgi::VgiFilterColumnIndexDomain::PROJECTED);
			rewritten->pushdown_filters = std::move(serialized.filter_bytes);
			rewritten->join_keys_buffers = std::move(serialized.join_keys_buffers);
		}

		// Build the EXPLAIN summary (PhysicalVgiTableBufferingFunction::
		// ParamsToString) while the TableFilters are in scope; the serialized
		// form is opaque.
		if (rewritten->projection_pushdown || get.table_filters.HasFilters()) {
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
			if (get.table_filters.HasFilters()) {
				if (!summary.empty()) {
					summary += " | ";
				}
				summary += "Filters: ";
				bool first = true;
				// Resolve filter projection positions back to worker columns.
				for (auto &kv : get.table_filters) {
					if (!first) {
						summary += " AND ";
					}
					first = false;
					std::string name = "<unknown>";
					if (kv.GetIndex().GetIndex() < all_col_ids.size()) {
						const auto &column = all_col_ids[kv.GetIndex().GetIndex()];
						if (!column.IsVirtualColumn() && column.GetPrimaryIndex() < rewritten->all_column_names.size()) {
							name = rewritten->all_column_names[column.GetPrimaryIndex()];
						}
					}
					summary += ExpressionFilter::GetExpressionFilter(kv.Filter(), "VGI buffered EXPLAIN").ToString(name);
				}
			}
			rewritten->explain_summary = std::move(summary);
		}

		rewritten->ResolveOperatorTypes();

		VGI_STDERR_DEBUG(
		    "[VGI] LogicalGet rewritten -> LogicalVgiTableBufferingFunction "
		    "(table_index=%llu, %zu output column(s), projection_pushdown=%d, "
		    "filter_bytes=%s)\n",
		    static_cast<unsigned long long>(get.table_index.index), rewritten->return_types.size(),
		    rewritten->projection_pushdown ? 1 : 0,
		    rewritten->pushdown_filters ? "yes" : "no");

		op = std::move(rewritten);
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		// Emergency off switch. When disabled, the guards in
		// VgiTableInOutFunction/VgiTableInOutFinalize turn any table_buffering
		// call into a clear error instead of wrong results.
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_table_buffering", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		Rewrite(plan, input.context);
	}
};

// Enforce Table.required_filters post-optimize (FilterPushdown has settled
// LogicalGet::table_filters). For each LogicalGet over a VgiTableEntry with
// requirements, collect the column paths referenced by its table_filters and by
// enclosing LogicalFilter expressions on its table_index, and throw
// BinderException for any unsatisfied group. Prefix-inclusive: a filter on
// `bbox` satisfies `bbox.xmin`. Also rewrites native-delegation markers.
class VgiRequiredFiltersOptimizer : public OptimizerExtension {
public:
	VgiRequiredFiltersOptimizer() {
		optimize_function = Optimize;
	}

private:
	using ColumnPath = std::vector<std::string>;
	using PresentPaths = std::set<ColumnPath>;
	using RequiredGroups = std::vector<std::vector<ColumnPath>>;

	// ExpressionFilter subjects are bound references. Resolve nested struct
	// extracts before traversing children so a child predicate does not count
	// as a predicate on every sibling through its parent struct.
	static bool ResolveFilterPath(const Expression &expr, const vector<ColumnPath> &roots, ColumnPath &path) {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto index = expr.Cast<BoundReferenceExpression>().Index();
			if (index >= roots.size() || roots[index].empty()) {
				return false;
			}
			path = roots[index];
			return true;
		}
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			return false;
		}
		const auto &function = expr.Cast<BoundFunctionExpression>();
		idx_t child_index;
		if (!TryGetStructExtractChildIndex(function, child_index) || function.GetChildren().empty() ||
		    !ResolveFilterPath(*function.GetChildren()[0], roots, path)) {
			return false;
		}
		const auto &children = StructType::GetChildTypes(function.GetChildren()[0]->GetReturnType());
		if (child_index >= children.size()) {
			return false;
		}
		path.push_back(children[child_index].first.GetIdentifierName());
		return true;
	}

	static void WalkFilterExpression(const Expression &expr, const vector<ColumnPath> &roots, PresentPaths &out) {
		if (auto child = ExpressionFilter::GetOptionalFilterChild(expr)) {
			// Runtime Top-N bounds are optimizer machinery, not a predicate
			// supplied by the caller. Do not let a partial optional subtree
			// satisfy the provider's required-filter contract.
			if (ExpressionFilter::ContainsInternalFunction(*child, DynamicFilterScalarFun::NAME) ||
			    ExpressionFilter::ContainsInternalFunction(*child, BloomFilterScalarFun::NAME) ||
			    ExpressionFilter::ContainsInternalFunction(*child, PrefixRangeScalarFun::NAME)) {
				return;
			}
			WalkFilterExpression(*child, roots, out);
			return;
		}
		ColumnPath path;
		if (ResolveFilterPath(expr, roots, path)) {
			out.insert(std::move(path));
			return;
		}
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
			WalkFilterExpression(child, roots, out);
		});
	}

	// Map a post-projection binding index (into column_ids) to an index into
	// LogicalGet::names; -1 for virtual or out-of-range columns.
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

	// Resolve a chain to the engine's canonical field names. Using bound
	// indexes handles nested extracts and SQL case resolution without changing
	// the raw, case-preserving wire identity. Do not also emit its parent path.
	static bool ResolveExpressionPath(const Expression &expr, TableIndex target_table_index,
	                                  const vector<ColumnIndex> &col_ids,
	                                  const vector<Identifier> &names, ColumnPath &path) {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &cref = expr.Cast<BoundColumnRefExpression>();
			if (cref.Binding().table_index != target_table_index) {
				return false;
			}
			auto orig = ResolveOrigIndex(cref.Binding().column_index.GetIndex(), col_ids, names.size());
			if (orig < 0) {
				return false;
			}
			path.push_back(names[orig].GetIdentifierName());
			return true;
		}
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			return false;
		}
		auto &fn = expr.Cast<BoundFunctionExpression>();
		if ((fn.Function().GetName() != "struct_extract" && fn.Function().GetName() != "struct_extract_at") ||
		    fn.GetChildren().empty() || !fn.BindInfo() || fn.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::STRUCT) {
			return false;
		}
		auto &bound = fn.BindInfo()->Cast<StructExtractBindData>();
		auto &children = StructType::GetChildTypes(fn.GetChildren()[0]->GetReturnType());
		if (bound.index >= children.size() ||
		    !ResolveExpressionPath(*fn.GetChildren()[0], target_table_index, col_ids, names, path)) {
			return false;
		}
		path.push_back(children[bound.index].first.GetIdentifierName());
		return true;
	}

	static void WalkExpression(const Expression &expr, TableIndex target_table_index,
	                           const vector<ColumnIndex> &col_ids,
	                           const vector<Identifier> &names, PresentPaths &out) {
		ColumnPath path;
		if (ResolveExpressionPath(expr, target_table_index, col_ids, names, path)) {
			out.insert(std::move(path));
			return;
		}
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
			WalkExpression(child, target_table_index, col_ids, names, out);
		});
	}

	static bool IsSatisfied(const ColumnPath &required, const PresentPaths &present) {
		ColumnPath prefix;
		for (const auto &component : required) {
			prefix.push_back(component);
			if (present.count(prefix)) {
				return true;
			}
		}
		return false;
	}

	static std::string RenderPath(const ColumnPath &path) {
		std::string out;
		for (const auto &part : path) {
			if (!out.empty()) {
				out += ".";
			}
			out += KeywordHelper::WriteOptionallyQuoted(part);
		}
		return out;
	}

	static std::string RenderGroup(const std::vector<ColumnPath> &group) {
		std::string out;
		for (const auto &path : group) {
			if (!out.empty()) {
				out += ", ";
			}
			out += RenderPath(path);
		}
		return group.size() == 1 ? out : "one of (" + out + ")";
	}

	static std::string JoinGroups(const RequiredGroups &groups) {
		std::string out;
		for (size_t i = 0; i < groups.size(); ++i) {
			if (i) {
				out += ", ";
			}
			out += RenderGroup(groups[i]);
		}
		return out;
	}

	// The VgiTableEntry behind this LogicalGet: GetTable() for VGI-side scans,
	// or the entry carried by a VgiNativeDelegationMarkerBindData.
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

	static void EnforceRequirements(
	    const LogicalGet &get, VgiTableEntry &vgi,
	    const std::vector<unique_ptr<Expression>> &parent_exprs) {
		const auto &required = vgi.GetTableInfo().required_filters;
		if (required.empty()) {
			return;
		}

		PresentPaths present;

		// DuckDB 2.0 keys filters by the scan projection, remapping them as
		// unused columns are removed. Resolve each position through column_ids.
		const auto &col_ids = get.GetColumnIds();
		auto root_path = [&](ProjectionIndex index) -> ColumnPath {
			auto original = ResolveOrigIndex(index.GetIndex(), col_ids, get.names.size());
			return original < 0 ? ColumnPath {} : ColumnPath {get.names[original].GetIdentifierName()};
		};
		for (const auto &entry : get.table_filters) {
			const auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "VGI required filters");
			WalkFilterExpression(*filter.expr, {root_path(entry.GetIndex())}, present);
		}
		for (const auto &item : get.table_filters.GetMultiColumnFilters()) {
			const auto &filter = ExpressionFilter::GetExpressionFilter(*item, "VGI required multi-column filters");
			vector<ColumnPath> roots;
			for (const auto &index : filter.column_indexes) {
				roots.push_back(root_path(index));
			}
			WalkFilterExpression(*filter.expr, roots, present);
		}

		// Dynamic (Top-N / runtime) filters never count; neither do the
		// optional runtime subtrees skipped by WalkFilterExpression.

		// Enclosing LogicalFilter expressions (non-pushed-down predicates such as
		// range joins), already rewritten by Walk() through any projections.
		for (auto &expr_ptr : parent_exprs) {
			WalkExpression(*expr_ptr, get.table_index, col_ids, get.names, present);
		}

		// Compute unsatisfied groups: a group is satisfied when any one of its
		// member paths has a filter present; every group must be satisfied.
		RequiredGroups missing;
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
		    vgi.ParentCatalog().GetName().GetIdentifierName(), vgi.ParentSchema().name.GetIdentifierName(),
		    vgi.name.GetIdentifierName(),
		    JoinGroups(required), JoinGroups(missing));
	}

	// Replace a VgiNativeDelegationMarkerBindData with the native function,
	// bind_data and return shape GetScanFunctionImpl pre-bound, so later passes
	// see the native scan's real shape instead of the declared Table.columns.
	static void RewriteMarkerToNative(ClientContext &context, LogicalGet &get) {
		auto *mark = dynamic_cast<VgiNativeDelegationMarkerBindData *>(get.bind_data.get());
		if (!mark) {
			return;
		}
		// Last access to the marker: get.bind_data is replaced below.
		auto native_tf = std::move(mark->native_tf);
		auto native_bind = std::move(mark->native_bind);
		auto native_return_types = std::move(mark->native_return_types);
		auto native_return_names = std::move(mark->native_return_names);
		auto native_virtual_columns = std::move(mark->native_virtual_columns);
		auto function_name_log = mark->scan_result.function_name;
		auto table_name_log = mark->table.get().name.GetIdentifierName();

		get.function = std::move(native_tf);
		get.bind_data = std::move(native_bind);
		get.returned_types = std::move(native_return_types);
		get.names = std::move(native_return_names);
		get.virtual_columns = std::move(native_virtual_columns);

		VGI_LOG(context, "vgi.scan_function.native_delegation_rewritten",
		        {{"table", table_name_log}, {"function", function_name_log}});
	}

	// Replace refs to `proj`'s outputs with the defining expressions, so a filter
	// above the projection references what it reads. Mirrors DuckDB's
	// ReplaceProjectionBindings (pushdown_projection.cpp).
	static void RewriteThroughProjection(unique_ptr<Expression> &expr,
	                                     const LogicalProjection &proj) {
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    expr, [&](BoundColumnRefExpression &cref, unique_ptr<Expression> &slot) {
			    if (cref.Binding().table_index == proj.table_index &&
			        cref.Binding().column_index.GetIndex() < proj.expressions.size()) {
				    slot = proj.expressions[cref.Binding().column_index.GetIndex()]->Copy();
			    }
		    });
	}

	static void Walk(ClientContext &context, LogicalOperator &op,
	                 std::vector<unique_ptr<Expression>> &parent_exprs) {
		// Accumulate copies of filter expressions for the gets below; pop on exit.
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

		// Rewrite accumulated expressions through the projection (a common
		// shape after pushdown), or they would never bind to the get. Restore
		// the originals for sibling subtrees.
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
				// Check before rewriting so a failure never binds the native scan.
				EnforceRequirements(get, *vgi, parent_exprs);
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

	static vgi::VgiResultCache &ResultCache(DatabaseInstance &db) {
		auto ext = Find(db);
		if (!ext) { throw InternalException("VGI extension is not loaded"); }
		return *ext->result_cache_;
	}
	static vgi::VgiMemoArenaRegistry &MemoArenas(DatabaseInstance &db) {
		auto ext = Find(db);
		if (!ext) { throw InternalException("VGI extension is not loaded"); }
		return *ext->memo_arenas_;
	}

	vgi::VgiLocationPolicy &GetLocationPolicy() {
		return location_policy_;
	}

	static vgi::VgiLocationPolicy *FindLocationPolicy(DatabaseInstance &db) {
		auto ext = StorageExtension::Find(DBConfig::GetConfig(db), "vgi");
		if (!ext) {
			return nullptr;
		}
		return &static_cast<VgiStorageExtension &>(*ext).location_policy_;
	}

	static vgi::VgiCancelDispatcher *FindCancelDispatcher(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto ext = StorageExtension::Find(config, "vgi");
		if (!ext) {
			return nullptr;
		}
		return &static_cast<VgiStorageExtension &>(*ext).dispatcher_;
	}

	// Redact keys per secret type name, recorded at ATTACH.
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

	static VgiStorageExtension *Find(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto ext = StorageExtension::Find(config, "vgi");
		if (!ext) {
			return nullptr;
		}
		return static_cast<VgiStorageExtension *>(ext.get());
	}

	// Orchard remote secret providers by catalog name. Non-owning: the
	// SecretManager owns each VgiRemoteSecretStorage for the DB's lifetime (no
	// unload API).
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

	// Unique tie_break_offset per provider: 100, 110, ... Built-in storages use
	// 0-20, so local secrets win over a remote catch-all.
	int64_t NextSecretTieBreakOffset() {
		std::lock_guard<std::mutex> lock(secret_mutex_);
		int64_t off = next_secret_offset_;
		next_secret_offset_ += 10;
		return off;
	}

	// ---- COPY format registry ----
	// For vgi_copy_formats() and ATTACH collision checks. Entries outlive
	// DETACH, as the system-catalog CopyFunction does. Hold CopyFormatMutex()
	// across check + create + record to close the TOCTOU between ATTACHes.
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

	// ---- Global (system.main) function registry ----
	// Functions published via CatalogAttachResult.global_functions, for
	// first-attach-wins and vgi_global_functions(). Entries outlive DETACH (no
	// unregister API; calls then throw, see ResolveVgiGlobalBinding). Hold
	// GlobalFunctionMutex() across check + create + record.
	struct GlobalFunctionEntry {
		std::string global_name;   // name registered into system.main (lowercased)
		std::string catalog_name;  // owning attach alias (may later be detached)
		std::string function_name; // worker-declared name (the dispatch key)
		std::string schema_name;   // worker-declared schema (the dispatch key)
		std::string function_type; // scalar | table | table_buffering | aggregate
		std::string worker_path;
	};

	std::mutex &GlobalFunctionMutex() {
		return global_function_mutex_;
	}

	// Caller must hold GlobalFunctionMutex(). key = lowercase global name.
	const GlobalFunctionEntry *FindGlobalFunctionLocked(const std::string &global_name) const {
		auto it = global_functions_.find(global_name);
		return it != global_functions_.end() ? &it->second : nullptr;
	}

	// Caller must hold GlobalFunctionMutex().
	void InsertGlobalFunctionLocked(GlobalFunctionEntry entry) {
		auto key = entry.global_name;
		global_functions_[key] = std::move(entry);
	}

	std::vector<GlobalFunctionEntry> AllGlobalFunctions() const {
		std::lock_guard<std::mutex> lock(global_function_mutex_);
		std::vector<GlobalFunctionEntry> out;
		out.reserve(global_functions_.size());
		for (auto &kv : global_functions_) {
			out.push_back(kv.second);
		}
		return out;
	}

	// ---- Companion catalog registry (lakehouse federation) ----
	// Catalogs attached for a catalog_attach `attach_catalogs` manifest, so a
	// non-companion catalog is never clobbered and companions are refcount-shared.
	enum class CompanionOutcome { ATTACHED, SHARED, CONFLICT };

	// Attach one companion if needed, serialized against other companion ops.
	//   SHARED   — same-target companion still attached; refcount bumped.
	//   ATTACHED — attach_fn ran (it throws on failure, leaving the registry
	//              unchanged).
	//   CONFLICT — alias held by another target or a non-companion catalog;
	//              `conflict_target_out` describes the holder; nothing attached.
	CompanionOutcome AcquireCompanion(const std::string &alias, const std::string &target,
	                                  const std::string &db_type, bool hidden, DatabaseManager &db_manager,
	                                  const std::function<void()> &attach_fn, std::string &conflict_target_out) {
		std::lock_guard<std::mutex> lock(companion_mutex_);
		auto it = companion_catalogs_.find(alias);
		if (it != companion_catalogs_.end()) {
			if (it->second.target == target) {
				// Share only if still attached; if it was detached out-of-band,
				// drop the stale record and re-attach.
				if (db_manager.GetDatabase(Identifier(alias))) {
					it->second.refcount++;
					return CompanionOutcome::SHARED;
				}
				companion_catalogs_.erase(it);
			} else {
				conflict_target_out = it->second.target;
				return CompanionOutcome::CONFLICT;
			}
		}
		if (db_manager.GetDatabase(Identifier(alias))) {
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
	// Effective vgi_allowed_transports allowlist (narrow-only). See vgi_location_policy.hpp.
	vgi::VgiLocationPolicy location_policy_;
	std::unique_ptr<vgi::VgiResultCache> result_cache_ = std::make_unique<vgi::VgiResultCache>();
	std::unique_ptr<vgi::VgiMemoArenaRegistry> memo_arenas_ = std::make_unique<vgi::VgiMemoArenaRegistry>();
	mutable std::mutex secret_mutex_;
	std::map<std::string, vgi::VgiRemoteSecretStorage *> secret_providers_;
	int64_t next_secret_offset_ = 100;
	mutable std::mutex copy_format_mutex_;
	std::map<std::string, CopyFormatEntry> copy_formats_;
	mutable std::mutex global_function_mutex_;
	std::map<std::string, GlobalFunctionEntry> global_functions_;

	struct CompanionRecord {
		std::string target; // canonical target key (raw manifest target string)
		int64_t refcount = 0;
		std::string db_type; // resolved/advertised db_type (for the vgi_companion_catalogs() diagnostic)
		bool hidden = false; // attached hidden (invisible to duckdb_databases())
	};
	mutable std::mutex companion_mutex_;
	std::map<std::string, CompanionRecord> companion_catalogs_;
};

// ConnectionManager invokes these callbacks under its lock. The dispatcher
// uses an unregistered ClientContext so waiting for it cannot reenter that lock.
class VgiConnectionLifecycle : public ExtensionCallback {
public:
	explicit VgiConnectionLifecycle(shared_ptr<VgiStorageExtension> storage) : storage_(std::move(storage)) {
	}
	void OnConnectionOpened(ClientContext &) override {
		storage_->GetCancelDispatcher().Resume();
	}
	void OnConnectionClosed(ClientContext &context) override {
		// Called before this connection is removed from the count.
		if (ConnectionManager::Get(*context.db).GetConnectionCount() == 1) {
			storage_->GetCancelDispatcher().Suspend();
		}
	}
private:
	shared_ptr<VgiStorageExtension> storage_;
};

namespace vgi {

// Copy a companion's secret_ref into its ATTACH options, for metadata
// connections that need credentials at attach time (e.g. a postgres DSN).
// Data-file creds resolve through the Orchard catch-all at query time instead.
//
// SECURITY: the worker chooses both the secret name and the target host, so
// injecting blindly would let a malicious worker exfiltrate credentials.
// Fail-closed: only with `attach_companion_secrets true`; otherwise the ref is
// skipped (logged) and the companion uses ambient credentials or fails.
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
		if (options.options.find(e.first.GetIdentifierName()) == options.options.end()) {
			options.options[e.first.GetIdentifierName()] = e.second;
		}
	}
}

// ---- Global (system.main) function publishing ----

// True when anything owns `name` in system.main's function namespace. Functions
// and macros share one per-schema CatalogSet, so a lookup can surface another
// type as a type-mismatch error, which also means "taken".
static bool SystemFunctionNameTaken(ClientContext &context, Catalog &system_catalog, const std::string &name) {
	static const CatalogType kFunctionTypes[] = {CatalogType::SCALAR_FUNCTION_ENTRY,
	                                             CatalogType::AGGREGATE_FUNCTION_ENTRY,
	                                             CatalogType::TABLE_FUNCTION_ENTRY, CatalogType::MACRO_ENTRY,
	                                             CatalogType::TABLE_MACRO_ENTRY, CatalogType::PRAGMA_FUNCTION_ENTRY};
	for (auto type : kFunctionTypes) {
		try {
			CatalogEntryRetriever retriever {context};
			if (system_catalog.GetEntry(retriever, DEFAULT_SCHEMA, {type, QualifiedName(Identifier(name))}, OnEntryNotFound::RETURN_NULL)) {
				return true;
			}
		} catch (const std::exception &) {
			// A type mismatch on an existing entry — the name is owned.
			return true;
		}
	}
	return false;
}

// Function kind for conflict checks; Table and TableBuffering are both "table".
static std::string GlobalFunctionKindName(vgi::VgiFunctionType type) {
	switch (type) {
	case vgi::VgiFunctionType::Scalar:
		return "scalar";
	case vgi::VgiFunctionType::Aggregate:
		return "aggregate";
	case vgi::VgiFunctionType::Table:
	case vgi::VgiFunctionType::TableBuffering:
		return "table";
	}
	return "table";
}

// Publish `attach_result.global_functions` into system.main under
// `<global_function_prefix>_<name>`. Best-effort: every failure path logs and
// continues so a global-name collision can never fail the ATTACH.
static void RegisterVgiGlobalFunctions(ClientContext &context, const std::string &catalog_alias,
                                       const std::shared_ptr<vgi::VgiAttachParameters> &attach_params,
                                       const vgi::CatalogAttachResult &attach_result) {
	auto *vgi_ext = VgiStorageExtension::Find(*context.db);
	if (!vgi_ext) {
		return;
	}
	auto &system_catalog = Catalog::GetSystemCatalog(*context.db);
	auto sys_txn = CatalogTransaction::GetSystemTransaction(*context.db);

	std::vector<std::string> setting_names;
	setting_names.reserve(attach_result.settings.size());
	for (const auto &setting : attach_result.settings) {
		setting_names.push_back(setting.name);
	}

	// No Catalog&: the entry outlives DETACH and re-resolves the catalog by
	// `catalog_name` at every bind.
	vgi::VgiFunctionRegistrationTarget target;
	target.catalog = nullptr;
	target.catalog_name = catalog_alias;
	target.attach_params = attach_params;
	target.attach_opaque_data = attach_result.attach_opaque_data;
	target.setting_names = setting_names;
	// Empty: each function dispatches on the schema the worker declared it in.
	target.schema_name = std::string();

	// Group overloads by global name; std::map keeps registration order stable
	// (overload order is user-visible in dispatch).
	struct GlobalGroup {
		vgi::VgiFunctionType kind = vgi::VgiFunctionType::Scalar;
		std::vector<vgi::VgiFunctionInfo> overloads;
	};
	std::map<std::string, GlobalGroup> groups;
	for (const auto &func_info : attach_result.global_functions) {
		// A global function exists to be called from SQL; a table_only one
		// would be published only to be hidden again.
		if (func_info.visibility == vgi::VgiFunctionVisibility::TableOnly) {
			continue;
		}
		const auto global_name = vgi::VgiGlobalFunctionName(attach_result.global_function_prefix, func_info.name);
		auto it = groups.find(global_name);
		if (it == groups.end()) {
			GlobalGroup group;
			group.kind = func_info.function_type;
			group.overloads.push_back(func_info);
			groups.emplace(global_name, std::move(group));
			continue;
		}
		// One namespace per schema: keep the first kind seen.
		if (GlobalFunctionKindName(it->second.kind) != GlobalFunctionKindName(func_info.function_type)) {
			VGI_LOG(context, "global_function.register",
			        {{"catalog", catalog_alias},
			         {"global_name", global_name},
			         {"function", func_info.name},
			         {"outcome", "skipped_kind_conflict"}});
			continue;
		}
		it->second.overloads.push_back(func_info);
	}

	for (auto &group_pair : groups) {
		const auto &global_name = group_pair.first;
		auto &group = group_pair.second;
		const auto kind = GlobalFunctionKindName(group.kind);

		std::lock_guard<std::mutex> lock(vgi_ext->GlobalFunctionMutex());
		if (auto *existing = vgi_ext->FindGlobalFunctionLocked(global_name)) {
			// Same alias (re-ATTACH): reuse it; binds resolve the live catalog.
			// Different alias: first attach wins.
			const bool same_owner = StringUtil::CIEquals(existing->catalog_name, catalog_alias);
			VGI_LOG(context, "global_function.register",
			        {{"catalog", catalog_alias},
			         {"global_name", global_name},
			         {"owner", existing->catalog_name},
			         {"outcome", same_owner ? "reused" : "skipped_name_taken"}});
			continue;
		}

		if (SystemFunctionNameTaken(context, system_catalog, global_name)) {
			VGI_LOG(context, "global_function.register",
			        {{"catalog", catalog_alias},
			         {"global_name", global_name},
			         {"outcome", "skipped_name_taken"}});
			continue;
		}

		try {
			vector<FunctionDescription> descriptions;
			switch (group.kind) {
			case vgi::VgiFunctionType::Scalar: {
				auto func_set =
				    vgi::BuildVgiScalarFunctionSet(context, global_name, group.overloads, target, descriptions);
				CreateScalarFunctionInfo info(func_set);
				info.SetSchema(DEFAULT_SCHEMA);
				info.internal = true;
				info.descriptions = std::move(descriptions);
				system_catalog.CreateFunction(sys_txn, info);
				break;
			}
			case vgi::VgiFunctionType::Aggregate: {
				auto func_set =
				    vgi::BuildVgiAggregateFunctionSet(context, global_name, group.overloads, target, descriptions);
				CreateAggregateFunctionInfo info(func_set);
				info.SetSchema(DEFAULT_SCHEMA);
				info.internal = true;
				info.descriptions = std::move(descriptions);
				system_catalog.CreateFunction(sys_txn, info);
				break;
			}
			default: {
				auto func_set =
				    vgi::BuildVgiTableFunctionSet(context, global_name, group.overloads, target, descriptions);
				CreateTableFunctionInfo info(func_set);
				info.SetSchema(DEFAULT_SCHEMA);
				info.internal = true;
				info.descriptions = std::move(descriptions);
				system_catalog.CreateFunction(sys_txn, info);
				break;
			}
			}

			VgiStorageExtension::GlobalFunctionEntry entry;
			entry.global_name = global_name;
			entry.catalog_name = catalog_alias;
			entry.function_name = group.overloads.front().name;
			entry.schema_name = group.overloads.front().schema_name;
			entry.function_type = vgi::VgiFunctionTypeToString(group.kind);
			entry.worker_path = attach_params ? attach_params->worker_path() : std::string();
			vgi_ext->InsertGlobalFunctionLocked(std::move(entry));

			VGI_LOG(context, "global_function.register",
			        {{"catalog", catalog_alias},
			         {"global_name", global_name},
			         {"function", group.overloads.front().name},
			         {"kind", kind},
			         {"outcome", "registered"}});
		} catch (const std::exception &e) {
			// Advisory: never fail the ATTACH; the schema-qualified name works.
			VGI_LOG(context, "global_function.register",
			        {{"catalog", catalog_alias},
			         {"global_name", global_name},
			         {"outcome", "error"},
			         {"error_message", e.what()}});
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
		// Best-effort: DetachDatabase throws when the companion is the default
		// DB (`USE companion; DETACH vgi_catalog`); must not escape OnDetach.
		try {
			db_manager.DetachDatabase(context, Identifier(alias), OnEntryNotFound::RETURN_NULL);
			VGI_LOG(context, "vgi.companion_detach", {{"alias", alias}});
		} catch (const std::exception &e) {
			VGI_LOG(context, "vgi.companion_detach.failed", {{"alias", alias}, {"error", e.what()}});
		}
	}
}

} // namespace vgi

// KeyValueSecret from every CREATE SECRET option, with the redact keys recorded
// for this secret type at ATTACH.
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	auto &config = DBConfig::GetConfig(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	if (vgi_ext) {
		auto &vgi_storage = static_cast<VgiStorageExtension &>(*vgi_ext);
		for (const auto &key : vgi_storage.GetRedactKeys(input.type.GetIdentifierName())) {
			secret->redact_keys.insert(Identifier(key));
		}
	}

	for (const auto &entry : input.options) {
		secret->secret_map[Identifier(entry.first)] = entry.second;
	}

	return secret;
}

// Iroh transport identities are extension-owned rather than advertised by a
// worker. SECRET_KEY is always redacted, including from duckdb_secrets().
static unique_ptr<BaseSecret> IrohCreateSecret(ClientContext &, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	secret->redact_keys.insert("secret_key");
	for (const auto &entry : input.options) {
		secret->secret_map[Identifier(entry.first)] = entry.second;
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

// Parse a struct-valued LOCATION: address from `image` (or `location`/`path`),
// plus runtime / connection / volumes / env / extra_args. Throws on unknown keys
// or a missing address.
static ParsedContainerOptions ParseContainerLocationStruct(const Value &loc) {
	ParsedContainerOptions out;
	auto &child_types = StructType::GetChildTypes(loc.type());
	auto &children = StructValue::GetChildren(loc);
	for (idx_t i = 0; i < children.size(); i++) {
		auto key = StringUtil::Lower(child_types[i].first.GetIdentifierName());
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
	string oauth_profile;
	string oauth_cache_mode;
	string bearer_token;
	// ATTACH `attach_ticket`: a vgi.attach_tickets.v1 ticket from
	// vgi_export_session(). A credential: redacted, never logged, and keyed only
	// as a salted hash. Sent as the sole catalog option, vgi_attach_ticket.
	string attach_ticket;
	bool attach_ticket_given = false;
	string tcp_proxy;
	vgi::IrohOptions iroh_options; // iroh_* options (shared parser with vgi_catalogs)
	string data_version_spec;
	string implementation_version;
	// ``launch:``-only overrides; -1 / empty = unset.
	int64_t launcher_idle_timeout_seconds = -1;
	string launcher_state_dir;
	// A struct-valued LOCATION (container transport) is parsed after the option
	// loops; a VARCHAR LOCATION sets worker_path directly.
	Value location_struct;
	bool location_is_struct = false;
	// `secrets false` opts out of auto-registering a VgiRemoteSecretStorage when
	// the attach response advertises a secret-service URL.
	bool secrets_enabled = true;
	// `attach_companions false` opts out of the `attach_catalogs` manifest
	// (guarded by a scheme allowlist and never-clobber policy when on).
	bool attach_companions_enabled = true;
	// Opt-in: see InjectCompanionSecret for why this defaults off.
	bool attach_companion_secrets = false;
	// `global_functions false` opts out of publishing into system.main.
	bool global_functions_enabled = true;
	// Options the extension does not know, forwarded to the worker after
	// validation against the catalog's AttachOptionSpec list.
	std::map<std::string, Value> attach_options;
	// Non-secret ATTACH options for the result-cache key (attaches with
	// different options never share entries). Excludes secret tokens and
	// LOCATION; std::map gives a canonical order.
	std::map<std::string, std::string> key_options;
	// Cache-key entries for worker-declared options, held back until the spec is
	// known: a credential (spec `secret`) enters the key only as a salted HMAC,
	// never in plain text.
	std::map<std::string, std::string> deferred_key_options;
	// Option names given in the ATTACH path's '?query', so a credential there
	// can be redacted from info.path once the spec says it is one.
	std::set<std::string> path_query_keys;

	// Shared by the connection-string query and the (TYPE vgi, ...) clause.
	auto apply_option = [&](const string &lower_name, const Value &value) {
		// Secret tokens must never enter the cache key or on-disk digest.
		// Worker-declared options are deferred until their spec says whether
		// they are secret.
		const bool keyable = lower_name != "type" && lower_name != "location" && lower_name != "path" &&
		                     lower_name != "bearer_token" && lower_name != "oauth_refresh_token" &&
		                     lower_name != "iroh_secret_key" && lower_name != vgi::ATTACH_TICKET_OPTION &&
		                     value.type().id() != LogicalTypeId::STRUCT;
		bool worker_option = false;
		if (lower_name == "type") {
			return;
		} else if (lower_name == "location" || lower_name == "path") {
			if (value.type().id() == LogicalTypeId::STRUCT) {
				location_struct = value;
				location_is_struct = true;
			} else {
				worker_path = value.ToString();
			}
		} else if (lower_name == "worker_debug") {
			// DefaultCastAs, not GetValue<bool>: query-string values are VARCHAR.
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
		} else if (lower_name == "global_functions") {
			global_functions_enabled = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "pool_max") {
			pool_max_override = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		} else if (lower_name == "pool_timeout") {
			pool_timeout_override = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		} else if (lower_name == "oauth_refresh_token") {
			oauth_refresh_token = value.ToString();
		} else if (lower_name == "oauth_profile") {
			oauth_profile = value.ToString();
			if (oauth_profile.empty()) {
				throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "oauth_profile"), "oauth_profile must not be empty");
			}
		} else if (lower_name == "oauth_cache") {
			oauth_cache_mode = StringUtil::Lower(value.ToString());
		} else if (lower_name == "bearer_token") {
			bearer_token = value.ToString();
		} else if (lower_name == vgi::ATTACH_TICKET_OPTION) {
			if (value.IsNull()) {
				throw BinderException("attach_ticket must not be NULL");
			}
			attach_ticket = value.ToString();
			attach_ticket_given = true;
		} else if (lower_name == "tcp_proxy") {
			tcp_proxy = value.ToString();
			if (tcp_proxy.empty()) {
				throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "tcp_proxy"), "tcp_proxy, if set, must not be empty");
			}
		} else if (vgi::ApplyIrohOption(lower_name, value, iroh_options)) {
		} else if (lower_name == "data_version_spec") {
			data_version_spec = value.ToString();
		} else if (lower_name == "implementation_version") {
			implementation_version = value.ToString();
		} else if (lower_name == "launcher_idle_timeout") {
			launcher_idle_timeout_seconds = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
			if (launcher_idle_timeout_seconds < 0) {
				throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "launcher_idle_timeout"),
				                      StringUtil::Format("launcher_idle_timeout must be >= 0 (got %lld); use 0 for "
				                                         "no timeout",
				                                         static_cast<long long>(launcher_idle_timeout_seconds)));
			}
		} else if (lower_name == "launcher_state_dir") {
			launcher_state_dir = value.ToString();
			if (launcher_state_dir.empty()) {
				throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "launcher_state_dir"), "launcher_state_dir, if set, must not be empty");
			}
		} else {
			// Assignment, not emplace: the options clause is applied after the
			// path's '?query' and must win.
			attach_options[lower_name] = value;
			worker_option = true;
		}
		if (keyable) {
			if (worker_option) {
				deferred_key_options[lower_name] = value.ToString();
			} else {
				key_options[lower_name] = value.ToString();
			}
		}
	};

	// Connection-string form (as in the airport extension):
	//   '<catalog>?location=<worker>&<opt>=<val>&...'
	// e.g. haybarn-cli "vgi:example?location=https://host/vgi/". Query options are
	// applied first so an explicit (TYPE vgi, ...) clause overrides them.
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
					path_query_keys.insert(lower_key);
					if (lower_key == "oauth_refresh_token" || lower_key == "bearer_token" ||
					    lower_key == "iroh_secret_key" || lower_key == vgi::ATTACH_TICKET_OPTION) {
						secret_in_path = true;
					}
					apply_option(lower_key, Value(val));
				}
				if (amp == string::npos) {
					break;
				}
				start = amp + 1;
			}
			// A token in the path is as introspection-visible as one in the
			// clause: strip the whole query from info.path.
			if (secret_in_path) {
				info.path = catalog_name + "?<redacted>";
			}
		}
	}

	// Explicit options clause, applied after the query string. Tokens are
	// redacted so DuckDB introspection can't echo them; CatalogAuth holds the
	// live value. Redact both info.options and options.options: the latter is
	// what duckdb_databases().options reads.
	auto redact_option = [&](const std::string &lower_target) {
		for (auto &entry : info.options) {
			if (StringUtil::Lower(entry.first) == lower_target) {
				entry.second = Value("<redacted>");
			}
		}
		for (auto &entry : options.options) {
			if (StringUtil::Lower(entry.first) == lower_target) {
				entry.second = Value("<redacted>");
			}
		}
	};
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		apply_option(lower_name, entry.second);
	}
	for (const char *token_option :
	     {"oauth_refresh_token", "bearer_token", "iroh_secret_key", vgi::ATTACH_TICKET_OPTION}) {
		redact_option(token_option);
	}

	// Before the bare-form fallback / empty-LOCATION check, so worker_path is set.
	ParsedContainerOptions container_opts;
	if (location_is_struct) {
		container_opts = ParseContainerLocationStruct(location_struct);
		worker_path = container_opts.image;
	}

	if (!bearer_token.empty() && !oauth_refresh_token.empty()) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "bearer_token"), "Cannot specify both bearer_token and oauth_refresh_token");
	}
	// An attach ticket IS the catalog's options (and version specs); the worker
	// refuses anything beside it, so reject that before any I/O. Extension
	// options (LOCATION, auth, pool, ...) remain allowed.
	if (attach_ticket_given) {
		if (attach_ticket.empty()) {
			throw BinderException("attach_ticket must not be empty");
		}
		if (!attach_options.empty()) {
			std::string extra;
			for (const auto &opt : attach_options) {
				extra += (extra.empty() ? "" : ", ") + opt.first;
			}
			throw BinderException("attach_ticket cannot be combined with catalog options (%s): the ticket carries "
			                      "the options the catalog was originally attached with",
			                      extra);
		}
		if (!data_version_spec.empty() || !implementation_version.empty()) {
			throw BinderException("attach_ticket cannot be combined with data_version_spec or "
			                      "implementation_version: the ticket carries the original version specs");
		}
	}

	if (oauth_profile.empty()) {
		oauth_profile = name;
	}
	if (oauth_cache_mode.empty()) {
		Value cache_value;
		oauth_cache_mode = context.TryGetCurrentSetting("vgi_oauth_cache", cache_value)
		                       ? StringUtil::Lower(cache_value.ToString())
		                       : "auto";
	}
	if (oauth_cache_mode != "auto" && oauth_cache_mode != "persistent" &&
	    oauth_cache_mode != "memory" && oauth_cache_mode != "none") {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "oauth_cache").Set(vgi::error_key::kActual, oauth_cache_mode),
		                      StringUtil::Format("oauth_cache must be auto, persistent, memory, or none (got '%s')",
		                                         oauth_cache_mode));
	}

	// Bare form: with no LOCATION and no '?query', the whole ATTACH path is the
	// worker location (`vgi:uvx vgi-easter`, `ATTACH 'uvx vgi-easter' (TYPE vgi)`)
	// and the catalog name is discovered from the worker below.
	bool discover_catalog = false;
	if (worker_path.empty() && !had_query && !catalog_name.empty()) {
		worker_path = catalog_name;
		catalog_name.clear();
		discover_catalog = true;
	}

	if (worker_path.empty()) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "location"), "VGI ATTACH requires LOCATION option specifying the worker path");
	}
	// What the user typed as LOCATION, for vgi_export_session() (worker_path may
	// be rewritten below, e.g. database:// into an internal artifact token).
	const std::string user_location = location_is_struct ? location_struct.ToString() : worker_path;
	// Policy gate on the user's LOCATION, before ANY I/O on it (container
	// inspection, database:// resolution, Iroh config, discovery RPC) and before
	// the rewrites below.
	vgi::CheckLocationPolicy(context, worker_path, vgi::LocationEntryPoint::ATTACH);

	if (!tcp_proxy.empty() && !vgi::IsTcpTransport(worker_path)) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "tcp_proxy"), "tcp_proxy is only valid for tcp:// LOCATIONs");
	}

	const bool is_iroh_location = vgi::IsIrohTransport(worker_path) || vgi::IsHttpiTransport(worker_path);
	if (is_iroh_location) {
		use_pool = false; // Iroh owns its endpoint/QUIC connection pool.
	}
	auto iroh_config = vgi::BuildIrohClientConfigForLocation(context, worker_path, std::move(iroh_options), "ATTACH");

	// Telemetry (emitted only on success): snapshot the pre-rewrite location and
	// options. Nothing may throw on telemetry's account.
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

	// Launcher overrides only mean something for ``launch:`` (``unix://`` is an
	// externally managed worker); reject them elsewhere.
	if ((launcher_idle_timeout_seconds >= 0 || !launcher_state_dir.empty()) &&
	    !vgi::IsLaunchLocation(worker_path)) {
		throw BinderException(
		    vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, launcher_state_dir.empty() ? "launcher_idle_timeout" : "launcher_state_dir"),
		    StringUtil::Format("launcher_idle_timeout / launcher_state_dir are only valid for `launch:` "
		                       "LOCATIONs (got LOCATION=%s)",
		                       worker_path));
	}

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
		// Validate `connection` before any runtime call. stdio = private
		// per-process; http/tcp = shared; unix is rejected.
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

		// Image-declared volumes, overridden by user entries on the same mount
		// point. Inspection may pull the image.
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

		// No `connection`: prefer an advertised tcp (idle self-shutdown) over
		// http; a label-less image stays private per-process.
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

		tele.is_container = true;
		tele.container_runtime = spec.runtime.kind;
		tele.container_connection = shared ? (shared_mode == vgi::ContainerConnMode::TCP ? "tcp" : "http") : "stdio";
		tele.container_shared = shared;

		if (shared) {
			// Register under an internal container-shared: token; the dispatch
			// resolves the live endpoint at connection time (EnsureSharedContainer).
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
			// Private per-process container: register the run command under a
			// pool-disambiguating "#<hash>" suffix for the spawn sites.
			string command_template = vgi::BuildContainerRunCommandTemplate(spec);
			string suffixed = worker_path + "#" + vgi::ContainerSpecHash(spec);
			vgi::RegisterContainerLaunch(suffixed, spec.runtime.binary, command_template);
			static std::once_flag reap_once;
			std::call_once(reap_once, [&]() { vgi::ReapOrphanContainers(spec.runtime); });
			worker_path = suffixed;
		}
#endif
	}

	// database:// is a package resolver, not a transport: snapshot the registry
	// row now; RPCs run the immutable artifact as an ordinary subprocess.
	std::shared_ptr<void> worker_artifact_anchor;
	if (vgi::IsDatabaseLocation(worker_path)) {
		auto resolved = vgi::ResolveDatabaseWorker(worker_path, context);
		worker_path = std::move(resolved.token);
		worker_artifact_anchor = std::move(resolved.lifetime_anchor);
	}

	// Pin a github-auto:// @latest LOCATION to one tag for the catalog's lifetime;
	// worker_path then records what actually ran. vgi_export_session keeps the
	// user's @latest LOCATION.
	worker_path = vgi::PinGithubLatestLocation(worker_path, context);

#ifdef __EMSCRIPTEN__
	// WASM: HTTP and browser SAB targets only. `worker:` names a legacy worker
	// script; `iroh://` an EndpointId the page maps to its Iroh adapter Worker.
	if (!vgi::IsHttpTransport(worker_path) && !vgi::IsWebWorkerTransport(worker_path) &&
	    !vgi::IsIrohTransport(worker_path) && !vgi::IsHttpiTransport(worker_path)) {
		throw BinderException("VGI in WASM only supports HTTP ('http[s]://') or "
		                      "browser SAB ('worker:', 'iroh://', or 'httpi://') transports.");
	}
	use_pool = false;
#else
	if (vgi::IsHttpTransport(worker_path)) {
		try {
			ExtensionHelper::TryAutoLoadExtension(db.GetDatabase(), "httpfs");
		} catch (...) {
		}
		if (!db.GetDatabase().ExtensionIsLoaded("httpfs")) {
			throw BinderException("VGI HTTP transport requires the httpfs extension. "
			                      "Install it with: INSTALL httpfs; LOAD httpfs;");
		}
		use_pool = false;
	}
#endif

	std::shared_ptr<vgi::CatalogAuth> auth;
	if (!bearer_token.empty()) {
		if (!vgi::IsHttpTransport(worker_path) && !vgi::IsHttpiTransport(worker_path)) {
			throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "bearer_token"),
			                      "bearer_token is only valid for HTTP transport "
			                      "(LOCATION must be an HTTP/HTTPS or httpi:// URL)");
		}
		auth = std::make_shared<vgi::BearerTokenCatalogAuth>(bearer_token);
	} else {
		auto oauth_auth = std::make_shared<vgi::OAuthCatalogAuth>(oauth_profile, oauth_cache_mode);
		if (!oauth_refresh_token.empty()) {
			if (!vgi::IsHttpTransport(worker_path) && !vgi::IsHttpiTransport(worker_path)) {
				throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, "oauth_refresh_token"),
				                      "oauth_refresh_token is only valid for HTTP transport "
				                      "(LOCATION must be an HTTP/HTTPS or httpi:// URL)");
			}
			oauth_auth->SeedRefreshToken(oauth_refresh_token);
		}
		auth = std::move(oauth_auth);
	}

	if (!bearer_token.empty()) {
		tele.auth_mode = "bearer";
	} else if (!oauth_refresh_token.empty()) {
		tele.auth_mode = "oauth_refresh_token";
	} else if (vgi::IsHttpTransport(worker_path) || vgi::IsHttpiTransport(worker_path)) {
		tele.auth_mode = "oauth";
	} else {
		tele.auth_mode = "none";
	}

	// Carries proxy-issued cookies for sticky version-aware routing.
	std::shared_ptr<vgi::SessionCookieJar> cookie_jar;
	if (vgi::IsHttpTransport(worker_path) || vgi::IsHttpiTransport(worker_path)) {
		cookie_jar = std::make_shared<vgi::SessionCookieJar>();
	}

	// Also passed to InvokeCatalogs, so the discovery RPC primes the launcher
	// cache with the user's overrides rather than defaults.
	std::optional<int64_t> launcher_idle_for_attach;
	std::optional<std::string> launcher_state_dir_for_attach;
	if (launcher_idle_timeout_seconds >= 0) {
		launcher_idle_for_attach = launcher_idle_timeout_seconds;
	}
	if (!launcher_state_dir.empty()) {
		launcher_state_dir_for_attach = launcher_state_dir;
	}

	// Before the first pooled RPC below, so ATTACH's own discovery / attach
	// workers are already sized by this ATTACH's settings.
	vgi::PoolSettings pool_settings;
	if (use_pool) {
		pool_settings = vgi::PoolSettingsFromSession(context);
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

	// The catalog list is fetched only to discover the name (bare form) or to
	// validate attach options against the AttachOptionSpec list.
	if (discover_catalog || !attach_options.empty()) {
		auto catalogs = vgi::InvokeCatalogs(worker_path, context, worker_debug, use_pool, auth,
		                                    launcher_idle_for_attach, launcher_state_dir_for_attach,
		                                    worker_artifact_anchor, tcp_proxy, iroh_config);

		// Bare form: only a single-catalog worker resolves unambiguously.
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

			// An alias DuckDB derived from the path (no AS clause) is the path
			// basename; rename it to the discovered catalog (`easter`, not
			// `uvx vgi-easter`). DatabaseManager::FinalizeAttach reads GetName()
			// after attach, so SetName here sticks. A user AS alias is kept.
			auto &fs = FileSystem::GetFileSystem(context);
			if (name == AttachedDatabase::ExtractDatabaseName(info.path, fs)) {
				db.SetName(Identifier(catalog_name));
			}
		}

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

			auto find_spec = [&](const std::string &lower_name) -> const vgi::VgiAttachOptionSpec * {
				for (const auto &spec : matching_info->attach_option_specs) {
					if (StringUtil::Lower(spec.name) == lower_name) {
						return &spec;
					}
				}
				return nullptr;
			};

			// The salt is read (or created) only when a credential needs hashing.
			std::string key_salt;
			auto hashed_key = [&](const std::string &lower_name, const Value &casted) {
				if (key_salt.empty()) {
					std::string cache_dir;
#ifndef __EMSCRIPTEN__
					Value dir_value;
					if (context.TryGetCurrentSetting("vgi_result_cache_dir", dir_value) && !dir_value.IsNull()) {
						cache_dir = dir_value.ToString();
					}
#endif
					key_salt = vgi::AttachOptionKeySalt(cache_dir);
				}
				return vgi::HashedAttachOptionKeyValue(key_salt, lower_name, casted.ToString());
			};

			std::map<std::string, Value> validated_options;
			std::set<std::string> secret_option_names; // explicit options the spec marks secret
			for (const auto &opt : attach_options) {
				const auto *matching_spec = find_spec(opt.first);
				if (!matching_spec) {
					throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, opt.first).Set(vgi::error_key::kCatalog, catalog_name),
					                      StringUtil::Format("Unknown ATTACH option '%s' for catalog '%s'. "
					                                         "Accepted options: %s",
					                                         opt.first, catalog_name, build_accepted_list()));
				}

				std::string cast_error;
				auto cast_result = opt.second.DefaultTryCastAs(matching_spec->type, &cast_error);
				if (!cast_result) {
					if (matching_spec->secret) {
						// The cast error quotes the value; a credential's must not surface.
						throw BinderException(
						    vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, matching_spec->name)
						        .Set(vgi::error_key::kCatalog, catalog_name)
						        .Set(vgi::error_key::kExpected, matching_spec->type.ToString()),
						    StringUtil::Format("Cannot cast ATTACH option '%s' to declared type %s",
						                       matching_spec->name, matching_spec->type.ToString()));
					}
					throw BinderException(
					    vgi::ErrorInfo(vgi::error_subtype::kInvalidAttachOption).Set(vgi::error_key::kAttachOption, matching_spec->name)
					        .Set(vgi::error_key::kCatalog, catalog_name)
					        .Set(vgi::error_key::kExpected, matching_spec->type.ToString()),
					    StringUtil::Format("Cannot cast ATTACH option '%s' to declared type %s: %s",
					                       matching_spec->name, matching_spec->type.ToString(), cast_error));
				}
				auto casted = std::move(*cast_result);
				if (matching_spec->secret) {
					secret_option_names.insert(opt.first);
					key_options[opt.first] = hashed_key(opt.first, casted);
				} else {
					auto deferred = deferred_key_options.find(opt.first);
					if (deferred != deferred_key_options.end()) {
						key_options[opt.first] = deferred->second;
					}
				}
				validated_options.emplace(matching_spec->name, std::move(casted));
			}

			attach_options = std::move(validated_options);

			// Redact explicit options the spec marks secret, from the options
			// clause and from the ATTACH path's '?query' alike.
			if (!secret_option_names.empty()) {
				for (const auto &name_to_redact : secret_option_names) {
					redact_option(name_to_redact);
				}
				for (const auto &qk : path_query_keys) {
					if (secret_option_names.count(qk)) {
						info.path = catalog_name + "?<redacted>";
						break;
					}
				}
			}
		}
	}

	// For vgi_export_session(): validated options (secrets included, memory
	// only) and the version specs as typed.
	auto retained_attach = std::make_shared<vgi::VgiRetainedAttach>();
	retained_attach->location = user_location;
	retained_attach->catalog_name = catalog_name;
	retained_attach->options = attach_options;
	retained_attach->data_version_spec = data_version_spec;
	retained_attach->implementation_version = implementation_version;

	if (attach_ticket_given) {
		// Only a worker hosting vgi.attach_tickets.v1 may receive the ticket;
		// any other would reject it or hand it to catalog code. Ask reflection.
		vgi::VgiAttachParametersConfig probe_cfg;
		probe_cfg.worker_path = worker_path;
		probe_cfg.worker_debug = worker_debug;
		probe_cfg.use_pool = use_pool;
		probe_cfg.auth = auth;
		probe_cfg.cookie_jar = cookie_jar;
		probe_cfg.iroh = iroh_config;
		probe_cfg.launcher_idle_timeout_seconds = launcher_idle_for_attach;
		probe_cfg.launcher_state_dir = launcher_state_dir_for_attach;
		probe_cfg.worker_artifact_anchor = worker_artifact_anchor;
		probe_cfg.tcp_proxy = tcp_proxy;
		auto probe_params = std::make_shared<vgi::VgiAttachParameters>(std::move(probe_cfg));
		if (!vgi::HostsProtocol(context, probe_params, vgi::ATTACH_TICKETS_PROTOCOL_NAME)) {
			throw BinderException("The VGI worker at '%s' does not host %s, so it cannot redeem attach_ticket. "
			                      "Attach tickets need an HTTP worker with a configured signing key and grant keys.",
			                      worker_path, vgi::ATTACH_TICKETS_PROTOCOL_NAME);
		}
		attach_options.clear();
		attach_options.emplace(vgi::ATTACH_TICKET_WIRE_OPTION, Value(attach_ticket));
		retained_attach->options.clear();
		retained_attach->via_ticket = true;
		// Different tickets may restore different options: key the result cache
		// on the ticket, as a salted hash only.
		std::string cache_dir;
#ifndef __EMSCRIPTEN__
		Value dir_value;
		if (context.TryGetCurrentSetting("vgi_result_cache_dir", dir_value) && !dir_value.IsNull()) {
			cache_dir = dir_value.ToString();
		}
#endif
		key_options[vgi::ATTACH_TICKET_OPTION] = vgi::HashedAttachOptionKeyValue(
		    vgi::AttachOptionKeySalt(cache_dir), vgi::ATTACH_TICKET_OPTION, attach_ticket);
	}

	// The worker validates the version specs; its error is the ATTACH failure.
	vgi::ServerCapabilities attach_capabilities;
	auto invoke_attach = [&]() {
		return vgi::InvokeCatalogAttach(worker_path, catalog_name, context, worker_debug, use_pool, auth,
		                                data_version_spec, implementation_version, cookie_jar, attach_options,
		                                launcher_idle_for_attach, launcher_state_dir_for_attach, worker_artifact_anchor,
		                                tcp_proxy, iroh_config, &attach_capabilities);
	};
	vgi::CatalogAttachResult attach_result;
	if (!attach_ticket_given) {
		attach_result = invoke_attach();
	} else {
		// Report a refused ticket by error kind only (same for every SDK), never
		// with the worker's wording, which could quote the ticket.
		try {
			attach_result = invoke_attach();
		} catch (const vgi::VgiRpcException &e) {
			const auto &kind = e.GetErrorKind();
			if (kind == "attach_ticket_invalid") {
				throw InvalidInputException(
				    "attach_ticket was refused (attach_ticket_invalid): the worker could not open it for this "
				    "caller. A ticket opens only on the worker that sealed it, unmodified, for the principal it was "
				    "sealed for: pair it with that principal's grant (bearer_token).");
			}
			if (kind == "attach_ticket_expired") {
				throw InvalidInputException("attach_ticket was refused (attach_ticket_expired): the ticket has "
				                            "expired. Export the session again with vgi_export_session().");
			}
			throw;
		}
	}

	// Register the catalog's settings; an existing one must have the same type.
	auto &config = DBConfig::GetConfig(db.GetDatabase());

	for (const auto &setting : attach_result.settings) {
		if (config.HasExtensionOption(Identifier(setting.name))) {
			ExtensionOption existing_option;
			if (!config.TryGetExtensionOption(Identifier(setting.name), existing_option)) {
				throw BinderException("Failed to retrieve existing VGI setting '%s'", setting.name);
			}
			if (existing_option.type != setting.type) {
				throw BinderException(
				    vgi::ErrorInfo()
				        .Set(vgi::error_key::kSetting, setting.name)
				        .Set(vgi::error_key::kCatalog, catalog_name)
				        .ExpectedActual(setting.type.ToString(), existing_option.type.ToString()),
				    StringUtil::Format(
				        "VGI setting '%s' already exists with type %s, but catalog '%s' requires type %s",
				        setting.name, existing_option.type.ToString(), catalog_name, setting.type.ToString()));
			}
		} else {
			config.AddExtensionOption(Identifier(setting.name), setting.description, setting.type, setting.default_value);
		}
	}

	auto &secret_manager = SecretManager::Get(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	for (const auto &st : attach_result.secret_types) {
		// The extension owns `iroh`. A worker advertising it is refused, so it
		// can never shadow its redaction or provider.
		if (vgi::IsReservedSecretTypeName(st.name)) {
			VGI_LOG(context, "attach.secret_type_refused",
			        {{"catalog", catalog_name}, {"secret_type", st.name}, {"reason", "reserved by the vgi extension"}});
			continue;
		}
		case_insensitive_set_t redact_set;
		for (const auto &param : st.parameters) {
			if (param.redact) {
				redact_set.insert(param.name);
			}
		}

		SecretType secret_type;
		secret_type.name = Identifier(st.name);
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		secret_type.extension = "vgi";
		try {
			secret_manager.RegisterSecretType(secret_type);
		} catch (const InternalException &) {
			// Registered by another catalog: keep the first registration's keys.
			redact_set = {};
			VGI_LOG(context, "attach.secret_type_exists",
			        {{"catalog", catalog_name}, {"secret_type", st.name}, {"outcome", "kept first registration"}});
		}

		if (vgi_ext && !redact_set.empty()) {
			static_cast<VgiStorageExtension &>(*vgi_ext).SetRedactKeys(st.name, std::move(redact_set));
		}

		CreateSecretFunction create_func;
		create_func.secret_type = st.name;
		create_func.provider = "config";
		create_func.function = VgiCreateSecret;
		for (const auto &param : st.parameters) {
			create_func.named_parameters[Identifier(param.name)] = param.type;
		}
		secret_manager.RegisterSecretFunction(create_func, OnCreateConflict::IGNORE_ON_CONFLICT);
	}

	if (!attach_result.comment.empty()) {
		db.comment = Value(attach_result.comment);
	}
	for (auto &[key, val] : attach_result.tags) {
		db.tags[key] = val;
	}

	// Resolved versions in duckdb_databases().tags (vgi_ prefix avoids clashing
	// with worker tags).
	if (!attach_result.resolved_data_version.empty()) {
		db.tags["vgi_resolved_data_version"] = attach_result.resolved_data_version;
	}
	if (!attach_result.resolved_implementation_version.empty()) {
		db.tags["vgi_resolved_implementation_version"] = attach_result.resolved_implementation_version;
	}

	// The resolved versions go into the pool key, so catalogs at different
	// versions never share a worker.
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
	attach_cfg.iroh = iroh_config;
	attach_cfg.worker_artifact_anchor = worker_artifact_anchor;
	attach_cfg.tcp_proxy = tcp_proxy;
	if (launcher_idle_timeout_seconds >= 0) {
		attach_cfg.launcher_idle_timeout_seconds = launcher_idle_timeout_seconds;
	}
	if (!launcher_state_dir.empty()) {
		attach_cfg.launcher_state_dir = launcher_state_dir;
	}
	attach_cfg.attach_options_canonical = vgi::CanonicalAttachOptions(key_options);
	attach_cfg.retained_attach = std::move(retained_attach);
	auto attach_params = std::make_shared<vgi::VgiAttachParameters>(std::move(attach_cfg));
	// Reuse the capabilities the attach RPC negotiated, so no probe is needed.
	attach_params->StoreServerCapabilities(attach_capabilities);

	// Prime the HTTPParams cache outside any VGI transaction: see
	// VgiAttachParameters::GetOrInitHttpParams (duckdb#22258).
	if (vgi::IsHttpTransport(worker_path)) {
		attach_params->GetOrInitHttpParams(context, worker_path);
	}

	auto attach_result_ptr = std::make_shared<vgi::CatalogAttachResult>(std::move(attach_result));

	// Snapshot eager-load thresholds once; the hot path never re-parses the map.
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
		eager_thresholds = ObjectCountsFromMap(threshold_map, vgi::defaults::EAGER_LOAD_THRESHOLD);
	}

	// Auto-register Orchard's remote secret provider when the catalog's tags
	// advertise vgi_secret_service_url (HTTP only). It reuses this catalog's
	// auth, so one login covers worker RPCs and secret fetches.
	if (secrets_enabled && vgi::IsHttpTransport(worker_path)) {
		auto turl = attach_result_ptr->tags.find("vgi_secret_service_url");
		if (turl != attach_result_ptr->tags.end() && !turl->second.empty()) {
			const std::string secret_endpoint = turl->second;
			// A credential broker must use TLS; cleartext http only for loopback.
			auto lower_secret_url = StringUtil::Lower(secret_endpoint);
			const bool is_https = StringUtil::StartsWith(lower_secret_url, "https://");
			const bool is_loopback_http = vgi::IsLoopbackHttpUrl(secret_endpoint);
			if (!is_https && !is_loopback_http) {
				throw BinderException(
				    "vgi_secret_service_url advertised by catalog '%s' must be an https:// URL — a remote "
				    "secret broker must not be reached over cleartext HTTP (only http://localhost is allowed "
				    "for testing). Got: %s",
				    catalog_name, secret_endpoint);
			}
			int64_t default_ttl = vgi::defaults::SECRET_DEFAULT_TTL_SECONDS;
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
				// Offset suffix: a re-ATTACH must not collide with the inert prior
				// storage the SecretManager still owns.
				const std::string storage_name =
				    "vgi_secret_" + catalog_name + "_" + std::to_string(offset);
				auto storage = make_uniq<vgi::VgiRemoteSecretStorage>(
				    *context.db, storage_name, secret_endpoint, auth, std::chrono::seconds(default_ttl), offset);
				auto *storage_ptr = storage.get();
				try {
					SecretManager::Get(context).LoadSecretStorage(std::move(storage));
				} catch (const std::exception &e) {
					throw BinderException(
					    vgi::ErrorInfo()
					        .Set(vgi::error_key::kCatalog, catalog_name)
					        .Url(secret_endpoint)
					        .Merge(vgi::ExtraInfoOf(e)),
					    StringUtil::Format(
					        "Failed to register VGI remote secret provider for catalog '%s' (endpoint %s): %s",
					        catalog_name, secret_endpoint, vgi::RawMessageOf(e)));
				}
				vgi_ext->RegisterSecretProvider(catalog_name, storage_ptr);
			}
		}
	}

	// Register the catalog's COPY ... FROM formats as system-catalog
	// CopyFunctions. The format namespace is global, so names are scoped by the
	// (per-DB unique) attach alias: `<alias>.<format>`, which makes collisions
	// between attaches impossible. Workers without the RPC have no formats.
	// See docs/copy_from.md.
	{
		std::vector<vgi::VgiCopyFromFormatInfo> formats;
		try {
			vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result_ptr->attach_opaque_data, {}};
			formats = vgi::InvokeCatalogCopyFromFormats(rpc_ctx, context);
		} catch (const vgi::VgiRpcException &e) {
			if (e.GetErrorKind() != vgi::error_kind::kMethodNotImplemented) {
				throw;
			}
		}

		if (!formats.empty()) {
			auto *vgi_ext = VgiStorageExtension::Find(*context.db);
			auto &system_catalog = Catalog::GetSystemCatalog(*context.db);
			auto sys_txn = CatalogTransaction::GetSystemTransaction(*context.db);
			const std::string alias_lower = StringUtil::Lower(name);

			// COPY binds forward these catalog-scoped settings to the worker.
			std::vector<std::string> setting_names;
			setting_names.reserve(attach_result_ptr->settings.size());
			for (const auto &setting : attach_result_ptr->settings) {
				setting_names.push_back(setting.name);
			}

			for (auto &fmt : formats) {
				// Lowercased to match the binder's case-insensitive lookup.
				const std::string registered_name = alias_lower + "." + StringUtil::Lower(fmt.format_name);

				// A registry hit is a re-ATTACH of the same alias (the system-catalog
				// entry outlives DETACH): skip.
				std::lock_guard<std::mutex> lock(vgi_ext->CopyFormatMutex());
				if (vgi_ext->FindCopyFormatLocked(registered_name)) {
					continue;
				}

				// Defensive: should not happen for alias-scoped names.
				CatalogEntryRetriever retriever {context};
				auto clash = system_catalog.GetEntry(retriever, DEFAULT_SCHEMA,
				                                     {CatalogType::COPY_FUNCTION_ENTRY, QualifiedName(Identifier(registered_name))},
				                                     OnEntryNotFound::RETURN_NULL);
				if (clash) {
					throw BinderException(
					    "Cannot register COPY format '%s' for catalog '%s': a COPY format with that name "
					    "already exists.",
					    registered_name, name);
				}

				// "from", "to" or "both": the two sides use independent info slots
				// (copy_from_function.function_info vs cf.function_info).
				const std::string dir = StringUtil::Lower(fmt.direction);
				const bool wants_from = (dir != "to"); // "from", "both", or unset → reader
				const bool wants_to = (dir == "to" || dir == "both");

				CopyFunction cf {Identifier(registered_name)};
				if (wants_from) {
					// Self-contained reader context; outlives DETACH.
					cf.copy_from_bind = vgi::VgiCopyFromBind;
					cf.copy_from_function = vgi::MakeVgiCopyFromTableFunction();
					cf.copy_from_function.function_info = make_shared_ptr<vgi::VgiCopyFromFunctionInfo>(
					    name, attach_params, attach_result_ptr->attach_opaque_data, fmt.format_name, fmt.handler,
					    fmt.options_schema, setting_names);
				}
				if (wants_to) {
					// copy_to_bind receives cf.function_info directly.
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

	// Publish global functions into system.main (best-effort; see
	// RegisterVgiGlobalFunctions and docs/global_functions.md).
	if (global_functions_enabled && attach_result_ptr && !attach_result_ptr->global_functions.empty()) {
		RegisterVgiGlobalFunctions(context, name, attach_params, *attach_result_ptr);
	}

	// attach_result_ptr is moved into the catalog below.
	std::vector<vgi::VgiAttachCatalogInfo> companion_manifest;
	if (attach_companions_enabled && attach_result_ptr) {
		companion_manifest = attach_result_ptr->attach_catalogs;
	}

	// Telemetry: companion_count is what the worker advertised (ignores the
	// opt-out); db_types only, never targets/DSNs.
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

	// Provision the `attach_catalogs` companions here. Reentrant attach is safe:
	// DuckDB does not hold databases_lock across CreateAttachedDatabase (our
	// caller), so a companion registers before this parent's FinalizeAttach.
	// Guards: scheme allowlist + AcquireCompanion's never-clobber policy. The
	// secret provider registered above serves companion credential lookups.
	// See docs/companion_catalogs.md.
	if (attach_companions_enabled && !companion_manifest.empty()) {
		auto *vgi_ext = VgiStorageExtension::Find(*context.db);
		if (vgi_ext) {
			static const std::set<std::string> kAllowedCompanionSchemes = {
			    "ducklake", "iceberg", "postgres", "mysql", "duckdb", "sqlite"};
			auto &db_manager = DatabaseManager::Get(context);
			auto &cfg = DBConfig::GetConfig(context);
			std::vector<std::string> referenced;
			// A mid-loop throw must release the companions already attached: the
			// catalog is never returned, so OnDetach would never run.
			try {
			for (const auto &c : companion_manifest) {
				// Scheme = db_type or the target's prefix; bare paths and unknown
				// schemes are rejected.
				std::string scheme = StringUtil::Lower(c.db_type);
				if (scheme.empty()) {
					auto colon = c.target.find(':');
					if (colon != std::string::npos) {
						scheme = StringUtil::Lower(c.target.substr(0, colon));
					}
				}
				// The target is commonly a DSN (`postgres:... password=...`), so it is
				// only ever reported through RedactCredentials.
				const std::string redacted_target = vgi::RedactCredentials(c.target);
				if (scheme.empty() || kAllowedCompanionSchemes.find(scheme) == kAllowedCompanionSchemes.end()) {
					throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kCompanionAttachFailed)
					                          .Set(vgi::error_key::kCatalog, catalog_name)
					                          .Set(vgi::error_key::kCompanion, c.alias)
					                          .Set(vgi::error_key::kActual, scheme),
					                      StringUtil::Format(
					                          "VGI catalog '%s' advertised companion '%s' with target '%s' whose "
					                          "scheme '%s' is not permitted (allowed: ducklake, iceberg, postgres, "
					                          "mysql, duckdb, sqlite).",
					                          catalog_name, c.alias, redacted_target, scheme));
				}

				auto handle_failure = [&](const std::string &raw_reason,
				                          const vgi::ErrorInfo &inner_info = vgi::ErrorInfo()) {
					// The attach error text can echo the DSN back; redact it too.
					const std::string reason = vgi::RedactCredentials(raw_reason);
					if (c.required) {
						vgi::ErrorInfo info(vgi::error_subtype::kCompanionAttachFailed);
						info.Set(vgi::error_key::kCatalog, catalog_name).Set(vgi::error_key::kCompanion, c.alias);
						// Inner fields (e.g. from the companion extension) fill what
						// we lack, but never a raw path/url that may hold the DSN.
						for (const auto &kv : inner_info) {
							if (kv.first == vgi::error_key::kErrorSubtype) {
								continue;
							}
							info.emplace(kv.first, vgi::RedactCredentials(kv.second));
						}
						info.erase("stack_trace_pointers");
						throw BinderException(
						    info, StringUtil::Format(
						              "VGI catalog '%s' could not attach required companion '%s' (target '%s'): %s",
						              catalog_name, c.alias, redacted_target, reason));
					}
					VGI_LOG(context, "vgi.companion_attach.skipped",
					        {{"alias", c.alias}, {"target", redacted_target}, {"reason", reason}});
				};

				std::string conflict_target;
				VgiStorageExtension::CompanionOutcome outcome;
				try {
					outcome = vgi_ext->AcquireCompanion(
					    c.alias, c.target, scheme, c.hidden, db_manager,
					    [&]() {
						    AttachInfo cinfo;
						    cinfo.name = Identifier(c.alias);
						    cinfo.path = c.target;
						    cinfo.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT; // never clobber
						    AttachOptions coptions(cfg.options);
						    coptions.db_type = c.db_type;
						    if (coptions.db_type.empty()) {
							    DBPathAndType::ExtractExtensionPrefix(cinfo.path, coptions.db_type);
						    }
						    // Natural access mode (writable federation is supported).
						    // Known tradeoff: a missing `sqlite`/`duckdb` target is
						    // CREATED, so a malicious worker can create empty db files
						    // at writable paths.
						    if (c.hidden) {
							    coptions.visibility = AttachVisibility::HIDDEN;
						    }
						    for (const auto &kv : c.options) {
							    coptions.options[kv.first] = Value(kv.second);
						    }
						    vgi::InjectCompanionSecret(context, c.secret_ref, attach_companion_secrets, coptions);
						    db_manager.AttachDatabase(context, cinfo, coptions);
					    },
					    conflict_target);
				} catch (const std::exception &e) {
					handle_failure(vgi::RawMessageOf(e), vgi::ExtraInfoOf(e));
					continue;
				}
				if (outcome == VgiStorageExtension::CompanionOutcome::CONFLICT) {
					handle_failure("alias already in use by " + conflict_target + " (not replaced)");
					continue;
				}
				referenced.push_back(c.alias);
				VGI_LOG(context, "vgi.companion_attach",
				        {{"alias", c.alias},
				         {"target", redacted_target},
				         {"shared", outcome == VgiStorageExtension::CompanionOutcome::SHARED ? "true" : "false"}});
			}
			} catch (...) {
				vgi::ReleaseCompanionCatalogs(context, referenced);
				throw;
			}
			vgi_catalog->SetCompanionCatalogs(std::move(referenced));
		}
	}

	// Success path only. Async, honours QUERY_FARM_TELEMETRY_OPT_OUT, never throws.
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
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<OAuthLogoutBindData>();
	auto it = input.named_parameters.find("catalog");
	if (it != input.named_parameters.end()) {
		// A NULL from a variable must not silently widen to "log out everything".
		if (it->second.IsNull()) {
			throw BinderException("vgi_oauth_logout: catalog must not be NULL; omit it to log out every catalog");
		}
		bind_data->target_catalog = it->second.ToString();
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

	bool matched = false;
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto catalog_name = catalog.GetName().GetIdentifierName();
		if (!bind_data.logout_all && catalog_name != bind_data.target_catalog) {
			continue;
		}
		matched = true;

		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (params && params->auth()) {
			params->auth()->ClearTokens();
			state->rows.push_back({catalog_name, "logged_out"});
		}
	}
	if (!bind_data.logout_all && !matched) {
		throw InvalidInputException("vgi_oauth_logout: no attached VGI catalog named '%s'", bind_data.target_catalog);
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
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
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
		row.catalog_name = catalog.GetName().GetIdentifierName();
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
// vgi_oauth_identity(): one row per attached VGI catalog with the OIDC identity
// from its id_token; authenticated=false and NULL fields without OAuth state.
//===--------------------------------------------------------------------===//

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
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
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
	// Raw decoded JWT payload, for provider-specific claims via JSON paths.
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
		row.catalog_name = catalog.GetName().GetIdentifierName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

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
		// JSON is a VARCHAR alias, so a plain string Value is correct.
		output.SetValue(7, count,
		                row.has_identity && !row.claims_json.empty() ? Value(row.claims_json) : Value());
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

// ---- Orchard remote secret provider diagnostics ----
// Here, not in their own .cpp, because they read the file-local
// VgiStorageExtension.

struct VgiSecretProvidersData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiSecretProvidersBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
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

// vgi_companion_catalogs(): one row per companion catalog, including hidden
// ones duckdb_databases() does not show.
struct VgiCompanionsData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiCompanionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
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
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
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

// vgi_copy_formats(): one row per (catalog, format, direction, option) of every
// VGI-registered COPY format.

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
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
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
				auto field_metadata = info.options_schema->field_metadata(static_cast<int>(i));
				auto row = base_row();
				row.has_option = true;
				row.option_name = opt_names[i];
				row.option_type = (i < types.size() ? types[i] : LogicalType::ANY).ToString();
				auto doc = field_metadata.Get(vgi::VGI_DOC_METADATA_KEY);
				if (doc && !doc->empty()) {
					row.has_option_desc = true;
					row.option_desc = *doc;
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

// vgi_global_functions(): one row per function published into system.main.
// `live` = the owning catalog is still attached (registrations outlive DETACH).

struct VgiGlobalFunctionRow {
	string global_name;
	string catalog_name;
	string function_name;
	string schema_name;
	string function_type;
	string worker_path;
	bool live = false;
};

struct VgiGlobalFunctionsData : public TableFunctionData {
	vector<VgiGlobalFunctionRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> VgiGlobalFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"global_name", "catalog_name", "function_name", "schema_name", "function_type", "worker_path", "live"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};

	auto data = make_uniq<VgiGlobalFunctionsData>();
	auto *ext = VgiStorageExtension::Find(*context.db);
	if (!ext) {
		return std::move(data);
	}
	auto &db_manager = DatabaseManager::Get(context);
	for (auto &entry : ext->AllGlobalFunctions()) {
		VgiGlobalFunctionRow row;
		row.global_name = entry.global_name;
		row.catalog_name = entry.catalog_name;
		row.function_name = entry.function_name;
		row.schema_name = entry.schema_name;
		row.function_type = entry.function_type;
		row.worker_path = entry.worker_path;
		auto db = db_manager.GetDatabase(context, Identifier(entry.catalog_name));
		row.live = db && dynamic_cast<VgiCatalog *>(&db->GetCatalog()) != nullptr;
		data->rows.push_back(std::move(row));
	}
	return std::move(data);
}

static void VgiGlobalFunctionsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiGlobalFunctionsData>();
	idx_t count = 0;
	while (data.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &r = data.rows[data.offset];
		output.SetValue(0, count, Value(r.global_name));
		output.SetValue(1, count, Value(r.catalog_name));
		output.SetValue(2, count, Value(r.function_name));
		output.SetValue(3, count, Value(r.schema_name));
		output.SetValue(4, count, Value(r.function_type));
		output.SetValue(5, count, Value(r.worker_path));
		output.SetValue(6, count, Value::BOOLEAN(r.live));
		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	vgi::CheckArrowRuntime();
#if defined(__EMSCRIPTEN__) && VGI_ASYNC_INIT_ENABLED
	// Spawn the async-init pool at load: pthread_create after side-module
	// dlopen is unreliable (see VGI_ASYNC_INIT_ENABLED). Its size must fit in
	// PTHREAD_POOL_SIZE alongside DuckDB's threads (duckdb-wasm/lib/CMakeLists.txt).
	duckdb::vgi::VgiWasmAsyncPool::Instance().EnsureStarted(3);
#endif
#if VGI_POSIX_TRANSPORT
	// A dead worker must surface as EPIPE from write(), not kill the process.
	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	if (sigaction(SIGPIPE, &sa, nullptr) == -1) {
		VGI_STDERR_DEBUG("[VGI] extension.load sigpipe_ignore_failed errno=%d\n", errno);
	}
#endif // VGI_POSIX_TRANSPORT

	vgi::VgiProfileRegisterAtExit();

	auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
	log_manager.RegisterLogType(make_uniq<VgiLogType>());

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	auto vgi_storage = make_shared_ptr<VgiStorageExtension>(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "vgi", vgi_storage);
	ExtensionCallback::Register(config, make_shared_ptr<VgiConnectionLifecycle>(vgi_storage));
#ifdef __EMSCRIPTEN__
	// Start the cancel dispatcher thread now, on the main runtime thread: lazy
	// creation from an interrupted scan's destructor on a pthread deadlocks
	// (see VgiCancelDispatcher::EnsureWorkerStarted). It uses one
	// PTHREAD_POOL_SIZE slot. On failure LOAD still succeeds; cancels are
	// dropped and server-side streams expire by TTL.
	try {
		vgi_storage->GetCancelDispatcher().StartWorker();
	} catch (const std::exception &e) {
		VGI_STDERR_DEBUG("[VGI] cancel_dispatcher.start_failed what=%s\n", e.what());
	}
#endif
	// LOCATION transport allowlist. Registered right after the storage extension
	// that owns its state, and before anything can ATTACH.
	vgi::RegisterLocationPolicySetting(config, vgi_storage->GetLocationPolicy());

	// The extension-owned Iroh identity secret type, available right after LOAD.
	SecretType iroh_secret_type;
	iroh_secret_type.name = "iroh";
	iroh_secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	iroh_secret_type.default_provider = "config";
	iroh_secret_type.extension = "vgi";
	loader.RegisterSecretType(std::move(iroh_secret_type));
	CreateSecretFunction iroh_secret_function;
	iroh_secret_function.secret_type = "iroh";
	iroh_secret_function.provider = "config";
	iroh_secret_function.function = IrohCreateSecret;
	iroh_secret_function.named_parameters["secret_key"] = LogicalType::VARCHAR;
	loader.RegisterFunction(std::move(iroh_secret_function));

	// Native GeoArrow extension types: GEOMETRY crosses Arrow IPC as struct/list
	// layouts instead of WKB. Worker → client casts the struct to GEOMETRY;
	// client → worker uses Geometry::ToVectorizedFormat with each entry's
	// GeometryStorageType.
	{
		auto xy = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});

		struct NativeGeoArrow {
			static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
				auto &cast_set = CastFunctionSet::Get(context);
				GetCastFunctionInput cast_input(context);
				auto info = cast_set.GetCastFunction(source.GetType(), result.GetType(), cast_input);
				CastParameters params(info.GetCastData(), false, nullptr, nullptr);
				info.Cast(source, result, count, params);
			}
		};

#define VGI_GEOARROW_DUCK_TO_ARROW(STORAGE_TYPE)                                                                       \
	[](ClientContext &, const Vector &source, Vector &result, idx_t count) {                                                 \
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
				// Already registered (e.g. by another extension).
			}
		}
	}

	// Built-in settings and their validation/defaults/docs share src/vgi_settings.json.
	vgi::RegisterVgiSettings(config);
	vgi::RegisterVgiMultiScanRewriter(config);
	vgi::RegisterVgiProjectionOptimizer(config);
	OptimizerExtension::Register(config, VgiStreamingWindowOptimizer());
	OptimizerExtension::Register(config, VgiTableBufferingRewriter());
	vgi::RegisterVgiLateralBatchRewriter(config);
	OptimizerExtension::Register(config, VgiRequiredFiltersOptimizer());
	vgi::RegisterVgiSpatialExtentOptimizer(config);

	RegisterVgiCatalogsFunction(loader);
	RegisterVgiProtocolsFunction(loader);
	RegisterVgiEngineFeaturesFunction(loader);
	vgi::RegisterVgiQueryFunction(loader);
	vgi::RegisterVgiExportSessionFunction(loader);

	// Catalog entry for the synthetic "vgi_table_scan": LogicalOperator::Copy()
	// (e.g. WindowSelfJoin) round-trips a LogicalGet through serialization, and
	// FunctionSerializer resolves the function by name. The entry only carries
	// the deserialize callback (no bind, so direct calls fail);
	// VgiTableScanDeserialize swaps in a fully configured scan.
	{
		TableFunction scan_func("vgi_table_scan", {}, vgi::VgiTableFunctionScan, nullptr,
		                        vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
		scan_func.serialize = VgiTableEntry::VgiTableScanSerialize;
		scan_func.deserialize = VgiTableEntry::VgiTableScanDeserialize;
		scan_func.projection_pushdown = true;
		scan_func.filter_pushdown = true;
		VgiDisableRuntimeFilterPushdown(scan_func);
		CreateTableFunctionInfo scan_info(scan_func);
		scan_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Internal: the physical scan operator for tables in an attached VGI catalog. DuckDB binds it "
		    "automatically when you query a VGI table (SELECT ... FROM mycatalog.schema.table); it exists in "
		    "the function list only so serialized query plans can resolve it by name. It takes no arguments "
		    "and cannot be called directly — use a normal table reference instead.",
		    {}, {}, {}));
		loader.RegisterFunction(std::move(scan_info));
	}

	// Same for the native-delegation marker: passes before
	// VgiRequiredFiltersOptimizer (WindowSelfJoin, CommonSubplanOptimizer) may
	// Copy() a LogicalGet still wrapping it. Its deserialize throws a clear
	// error; those callers catch it and skip their rewrite.
	{
		TableFunction marker_func = MakeNativeDelegationMarkerFunction();
		marker_func.deserialize = [](Deserializer &, BoundTableFunction &) -> unique_ptr<FunctionData> {
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

	vgi::RegisterVgiWorkerPoolFunction(loader);
	vgi::RegisterVgiWorkerPoolStatsFunction(loader);
	vgi::RegisterVgiWorkerPoolFlushFunction(loader);
	vgi::RegisterVgiGithubCacheFunction(loader);
	vgi::RegisterVgiGithubCacheFlushFunction(loader);
	vgi::RegisterVgiGithubLatestFunction(loader);
	vgi::RegisterVgiGithubResolveFunction(loader);
	vgi::RegisterVgiWorkerPackageMacro(loader);
	vgi::RegisterVgiWorkerCacheFunctions(loader);

	vgi::RegisterVgiTableStatisticsFunction(loader);

	vgi::RegisterVgiClearCacheFunction(loader);
	vgi::RegisterVgiCatalogContentsFunction(loader);

	vgi::RegisterVgiResultCacheFunction(loader);
	vgi::RegisterVgiResultCacheFlushFunction(loader);
	vgi::RegisterVgiResultCacheReapFunction(loader);
	vgi::RegisterVgiResultCacheStatsFunction(loader);

	// Registry diagnostics: COPY formats, global functions, secret providers,
	// companions.
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

		TableFunction global_functions_func("vgi_global_functions", {}, VgiGlobalFunctionsScan,
		                                    VgiGlobalFunctionsBind);
		CreateTableFunctionInfo global_functions_info(global_functions_func);
		global_functions_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "List the functions attached VGI catalogs published into DuckDB's global function namespace "
		    "(system.main), one row per globally-visible name. 'global_name' is the name to call unqualified; "
		    "'catalog_name' is the attach alias that owns it (first attach wins); 'live' is false once that "
		    "catalog is DETACHed — the registration persists for the life of the process, but calling it then "
		    "throws until the catalog is re-ATTACHed.",
		    {}, {}, {"SELECT * FROM vgi_global_functions();"}));
		loader.RegisterFunction(std::move(global_functions_info));

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
		vgi::AddVgiFunctionOption(flush_func, "catalog", LogicalType::VARCHAR);
		CreateTableFunctionInfo flush_info(flush_func);
		flush_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Clear the TTL cache of one Orchard remote secret provider, or all providers when 'catalog' is "
		    "omitted. Returns the count of positive (found) secrets dropped.",
		    {"catalog"}, {LogicalType::VARCHAR},
		    {"SELECT * FROM vgi_secret_provider_flush();",
		     "SELECT * FROM vgi_secret_provider_flush(catalog := 'mycatalog');"}));
		loader.RegisterFunction(std::move(flush_info));
	}

	vgi::RegisterVgiTableBranchesFunction(loader);

	vgi::RegisterVgiFunctionArgumentsFunction(loader);
	vgi::RegisterVgiSecretTypesFunction(loader);

	{
		// Per catalog, not per origin: each attached catalog owns its CatalogAuth.
		TableFunction logout_func("vgi_oauth_logout", {}, OAuthLogoutFunction, OAuthLogoutBind, OAuthLogoutInit);
		vgi::AddVgiFunctionOption(logout_func, "catalog", LogicalType::VARCHAR);
		CreateTableFunctionInfo logout_info(logout_func);
		logout_info.descriptions.push_back(vgi::MakeFunctionDescription(
		    "Forget the OAuth tokens of attached VGI catalogs, including any refresh token saved in the "
		    "persistent credential store. With no arguments, logs out every attached VGI catalog; pass "
		    "catalog := 'alias' to log out only that one (an unknown alias is an error). Returns one row "
		    "(catalog_name, status) per catalog logged out.",
		    {"catalog"}, {LogicalType::VARCHAR},
		    {"SELECT * FROM vgi_oauth_logout();",
		     "SELECT * FROM vgi_oauth_logout(catalog := 'mycatalog');"}));
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
VgiResultCache &GetResultCache(DatabaseInstance &db) {
	return ::duckdb::VgiStorageExtension::ResultCache(db);
}
VgiResultCache &GetResultCache(ClientContext &context) {
	return GetResultCache(*context.db);
}
VgiMemoArenaRegistry &GetMemoArenaRegistry(ClientContext &context) {
	return ::duckdb::VgiStorageExtension::MemoArenas(*context.db);
}
VgiLocationPolicy *FindVgiLocationPolicy(DatabaseInstance &db) {
	return ::duckdb::VgiStorageExtension::FindLocationPolicy(db);
}
} // namespace vgi

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(vgi, loader) {
	duckdb::LoadInternal(loader);
}
}
