// =============================================================================
// VgiMultiScanRewriter implementation.
//
// Phase C of the multi-branch design. Detects LogicalGet nodes whose bind
// data is a VgiMultiBranchMarkerBindData (placed there by
// VgiTableEntry::GetScanFunctionImpl when branches_.size() > 1) and rewrites
// them into LogicalSetOperation(UNION_ALL, [LogicalProjection(
// LogicalFilter(branch_filter, LogicalGet(branch_fn))), ...]).
//
// Design constraints honoured here:
//   - Pre-pushdown phase: DuckDB's filter pushdown runs AFTER this rewriter
//     and distributes parent filters into each arm naturally.
//   - Column reconciliation by NAME with NULL-fill for missing columns
//     (matches SQL UNION ALL BY NAME semantics — validated by U1 spike).
//   - column_ids on each arm mirror the marker's narrowed column_ids
//     (S2 spike finding — silent ColumnBindingResolver failure otherwise).
//   - ColumnBindingReplacer pass on the root plan to rewrite parent
//     expressions referencing the old marker bindings.
//
// Known limitations:
//   - branch_filter binding supports only the subset
//     "col OP const [AND col OP const ...]" — no function calls, no
//     subqueries, no nested expressions. Richer filters would require
//     full ExpressionBinder integration.
// =============================================================================

#include "vgi_multi_scan_rewriter.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

// ---------------------------------------------------------------------------
// Marker placeholder function (never executed; rewriter must replace it)
// ---------------------------------------------------------------------------

static void MultiBranchMarkerExecute(ClientContext &, TableFunctionInput &, DataChunk &) {
	throw InternalException(
	    "VgiMultiScanRewriter did not fire — multi-branch placeholder reached execution. "
	    "Check that the vgi_multi_branch_scans setting is true and that the optimizer "
	    "extension is registered. This is a bug — please report it.");
}

TableFunction MakeMultiBranchMarkerFunction() {
	TableFunction fn("vgi_multi_branch_marker", {}, MultiBranchMarkerExecute);
	// No bind callback — bind_data is supplied externally by GetScanFunctionImpl.
	// No init_global / init_local — the marker should never be executed.
	return fn;
}

// ---------------------------------------------------------------------------
// Minimal branch_filter binder (scope: col OP const [AND ...])
// ---------------------------------------------------------------------------

// Map column name → ColumnBinding for the arm. Built from the arm's return_names
// and the arm's GetColumnBindings(). The binder uses this to resolve
// column references in the branch_filter expression.
struct ArmColumnLookup {
	const std::vector<std::string> &names;
	const std::vector<LogicalType> &types;
	const std::vector<ColumnBinding> &bindings;
};

// Forward declarations.
static unique_ptr<Expression> BindBranchFilterExpr(ClientContext &context, const ParsedExpression &expr,
                                                    const ArmColumnLookup &cols);

static unique_ptr<Expression> BindBranchFilterColumnRef(const ColumnRefExpression &cref,
                                                          const ArmColumnLookup &cols) {
	if (cref.IsQualified()) {
		throw BinderException(
		    "VGI branch_filter does not support qualified column references "
		    "(got '%s'). Use unqualified column names referring to the branch's output schema.",
		    cref.GetName());
	}
	const auto &name = cref.GetColumnName();
	for (size_t i = 0; i < cols.names.size(); i++) {
		if (cols.names[i] == name) {
			return make_uniq<BoundColumnRefExpression>(cols.types[i], cols.bindings[i]);
		}
	}
	throw BinderException(
	    "VGI branch_filter references unknown column '%s'. Branch must expose this column.",
	    name);
}

static unique_ptr<Expression> BindBranchFilterConstant(const ConstantExpression &cexpr) {
	return make_uniq<BoundConstantExpression>(cexpr.value);
}

static unique_ptr<Expression> BindBranchFilterComparison(ClientContext &context, const ComparisonExpression &cmp,
                                                          const ArmColumnLookup &cols) {
	auto left = BindBranchFilterExpr(context, *cmp.left, cols);
	auto right = BindBranchFilterExpr(context, *cmp.right, cols);
	// Implicit cast: if one side is a constant whose type differs from the
	// other, cast the constant to the other side's type. Handles
	// `ts >= TIMESTAMP '...'` cleanly without invoking the full type
	// resolver. Mismatched non-constant types throw — arbitrary type
	// coercion between non-constants is not supported.
	if (left->return_type != right->return_type) {
		if (left->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			left = BoundCastExpression::AddCastToType(context, std::move(left), right->return_type);
		} else if (right->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			right = BoundCastExpression::AddCastToType(context, std::move(right), left->return_type);
		} else {
			throw BinderException(
			    "VGI branch_filter comparison between mismatched types %s and %s is not supported "
			    "(no automatic cast between two non-constant operands).",
			    left->return_type.ToString(), right->return_type.ToString());
		}
	}
	return make_uniq<BoundComparisonExpression>(cmp.GetExpressionType(), std::move(left), std::move(right));
}

static unique_ptr<Expression> BindBranchFilterConjunction(ClientContext &context, const ConjunctionExpression &conj,
                                                            const ArmColumnLookup &cols) {
	if (conj.children.empty()) {
		throw BinderException("VGI branch_filter: empty conjunction is invalid.");
	}
	auto result = make_uniq<BoundConjunctionExpression>(conj.GetExpressionType());
	for (const auto &child : conj.children) {
		result->children.push_back(BindBranchFilterExpr(context, *child, cols));
	}
	return result;
}

static unique_ptr<Expression> BindBranchFilterExpr(ClientContext &context, const ParsedExpression &expr,
                                                    const ArmColumnLookup &cols) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
		return BindBranchFilterColumnRef(expr.Cast<ColumnRefExpression>(), cols);
	case ExpressionClass::CONSTANT:
		return BindBranchFilterConstant(expr.Cast<ConstantExpression>());
	case ExpressionClass::COMPARISON:
		return BindBranchFilterComparison(context, expr.Cast<ComparisonExpression>(), cols);
	case ExpressionClass::CONJUNCTION:
		return BindBranchFilterConjunction(context, expr.Cast<ConjunctionExpression>(), cols);
	default:
		throw BinderException(
		    "VGI branch_filter only supports column references, constants, comparisons "
		    "(=, <, >, <=, >=, <>), and AND/OR conjunctions. Expression class %s is not supported. "
		    "Restructure your filter to use only the supported expression forms.",
		    static_cast<int>(expr.GetExpressionClass()));
	}
}

// ---------------------------------------------------------------------------
// Per-arm LogicalGet construction (mirrors the S2+S3 spike pattern)
// ---------------------------------------------------------------------------

// Build a LogicalGet for a single branch. Resolves function_name through the
// catalog, invokes its bind callback with the branch's args, propagates
// column_ids from the marker so the arm's output column count matches.
struct ArmBindResult {
	unique_ptr<LogicalGet> get;
	std::vector<std::string> raw_names;   // column names as the branch returned them
	std::vector<LogicalType> raw_types;   // ditto
	std::vector<ColumnBinding> bindings;  // matching ColumnBinding list
};

static ArmBindResult BindBranchArm(const VgiScanBranch &branch, ClientContext &context, Binder &binder,
                                    const std::string &table_catalog_name,
                                    const std::string &table_schema_name, const std::string &default_schema,
                                    const std::vector<ColumnIndex> &marker_column_ids) {
	// Look up the branch's function. Try the table's catalog/schema first,
	// fall back to the catalog's default schema, then the system catalog
	// (read_parquet, iceberg_scan, etc.).
	EntryLookupInfo lookup(CatalogType::TABLE_FUNCTION_ENTRY, branch.function_name);
	optional_ptr<CatalogEntry> entry = Catalog::GetEntry(context, table_catalog_name, table_schema_name, lookup,
	                                                      OnEntryNotFound::RETURN_NULL);
	if (!entry && !default_schema.empty() && default_schema != table_schema_name) {
		entry = Catalog::GetEntry(context, table_catalog_name, default_schema, lookup,
		                          OnEntryNotFound::RETURN_NULL);
	}
	if (!entry) {
		entry = Catalog::GetEntry(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
	}
	if (!entry) {
		throw BinderException("VGI branch references unknown function '%s'. "
		                       "Check that any required extensions are loaded.",
		                       branch.function_name);
	}
	auto &fn_entry = entry->Cast<TableFunctionCatalogEntry>();

	// Pick an overload by argument types. Same shape as the throwaway spike.
	// Note: GetFunctionByArguments expects duckdb::vector (not std::vector).
	vector<LogicalType> arg_types;
	arg_types.reserve(branch.positional_arguments.size());
	for (const auto &v : branch.positional_arguments) {
		arg_types.push_back(v.type());
	}
	TableFunction tf = fn_entry.functions.GetFunctionByArguments(context, arg_types);

	// Build the bind input. The Binder + TableFunctionRef are required by
	// the TableFunctionBindInput constructor; we pass the optimizer's binder
	// and a default-constructed ref.
	vector<Value> parameters(branch.positional_arguments.begin(), branch.positional_arguments.end());
	named_parameter_map_t named_parameters;
	for (auto &kv : branch.named_arguments) {
		named_parameters.emplace(kv.first, kv.second);
	}
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
	                                  tf.function_info.get(), &binder, tf, ref);

	vector<LogicalType> return_types;
	vector<string> return_names;
	auto bind_data = tf.bind(context, bind_input, return_types, return_names);

	virtual_column_map_t virtual_columns;
	if (tf.get_virtual_columns) {
		virtual_columns = tf.get_virtual_columns(context, bind_data.get());
	}

	auto bind_index = binder.GenerateTableIndex();
	auto get = make_uniq<LogicalGet>(bind_index, tf, std::move(bind_data), return_types, return_names,
	                                 std::move(virtual_columns));
	get->parameters = std::move(parameters);
	get->named_parameters = std::move(named_parameters);
	get->input_table_types = std::move(input_table_types);
	get->input_table_names = std::move(input_table_names);

	// Each arm produces ALL of its natural return columns. We deliberately
	// do NOT mirror the marker's narrowed column_ids onto the arm because:
	//
	//   1. Marker column_ids may contain virtual rowid placeholders (when
	//      DuckDB optimised `count(*)` to "I just need any column"). Those
	//      don't exist on every arm — e.g., sequence() and read_parquet
	//      both have rowid but with different types, and the union ends
	//      up with mismatched arm types (BIGINT vs BOOLEAN seen in
	//      multi_branch_heterogeneous.test).
	//
	//   2. The per-arm LogicalProjection (BuildArmProjection) explicitly
	//      maps each arm's raw output to the canonical column list, so the
	//      union's column shape is always the canonical shape regardless
	//      of which columns the underlying arm "needs". DuckDB's filter +
	//      projection pushdown handles per-arm narrowing AFTER the rewrite.
	//
	// Populate column_ids with the full set of bound return columns so
	// ResolveOperatorTypes() doesn't fall back to GetAnyColumn().
	{
		vector<ColumnIndex> all_cols;
		all_cols.reserve(return_types.size());
		for (idx_t i = 0; i < return_types.size(); i++) {
			all_cols.emplace_back(i);
		}
		get->SetColumnIds(std::move(all_cols));
	}
	(void)marker_column_ids;
	get->ResolveOperatorTypes();

	ArmBindResult result;
	result.raw_names = std::move(return_names);
	result.raw_types = std::move(return_types);
	result.bindings = get->GetColumnBindings();
	result.get = std::move(get);
	return result;
}

// ---------------------------------------------------------------------------
// Per-arm projection (column reconciliation by NAME with NULL-fill + cast)
// ---------------------------------------------------------------------------

// Build a LogicalProjection above the arm's LogicalGet (and the optional
// branch_filter LogicalFilter) that maps raw_names → wanted_canonical_names.
//
// `wanted_canonical_names` is a SUBSET of the table's full canonical column
// list, in the order the parent expects. When the marker's column_ids has
// been narrowed by the binder (e.g. `count(*)` only needs one column), the
// projection produces just those columns, so the union's column count
// matches the parent's bindings.
//
// Match by NAME; emit NULL::canonical_type for missing canonicals; reject
// extra (non-canonical) columns with a loud error.
static unique_ptr<LogicalProjection> BuildArmProjection(
    ClientContext &context, unique_ptr<LogicalOperator> arm_child, Binder &binder, const ArmBindResult &arm,
    const std::vector<std::string> &wanted_canonical_names,
    const std::vector<LogicalType> &wanted_canonical_types,
    const std::vector<std::string> &all_canonical_names) {

	vector<unique_ptr<Expression>> proj_exprs;
	proj_exprs.reserve(wanted_canonical_names.size());

	// Build a name→index lookup for the branch's raw output.
	case_insensitive_map_t<size_t> raw_lookup;
	for (size_t i = 0; i < arm.raw_names.size(); i++) {
		raw_lookup.emplace(arm.raw_names[i], i);
	}

	for (size_t i = 0; i < wanted_canonical_names.size(); i++) {
		const auto &canonical_name = wanted_canonical_names[i];
		const auto &canonical_type = wanted_canonical_types[i];
		auto it = raw_lookup.find(canonical_name);
		if (it != raw_lookup.end()) {
			size_t raw_idx = it->second;
			auto colref = make_uniq<BoundColumnRefExpression>(arm.raw_types[raw_idx], arm.bindings[raw_idx]);
			unique_ptr<Expression> expr = std::move(colref);
			if (arm.raw_types[raw_idx] != canonical_type) {
				// Same-named column with different type — cast it. Let DuckDB's
				// cast machinery throw with its own error if the cast isn't
				// defined.
				expr = BoundCastExpression::AddCastToType(context, std::move(expr), canonical_type);
			}
			proj_exprs.push_back(std::move(expr));
		} else {
			// Branch doesn't return this canonical column — NULL-fill.
			proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value(canonical_type)));
		}
	}

	// Reject branch columns that AREN'T in the FULL canonical list. Catches
	// "worker added a column nobody knows about" at rewrite time. (Note: we
	// reject against the full canonical list, not the narrowed wanted-list,
	// because a column that's canonical-but-not-needed is fine — it just
	// won't surface in the union.)
	for (size_t i = 0; i < arm.raw_names.size(); i++) {
		bool found = false;
		for (const auto &cname : all_canonical_names) {
			if (StringUtil::CIEquals(cname, arm.raw_names[i])) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw BinderException(
			    "VGI branch returned column '%s' which is not in the table's declared schema. "
			    "Branches may return a SUBSET of canonical columns (missing → NULL) or all canonicals "
			    "in any order, but extras are not allowed.",
			    arm.raw_names[i]);
		}
	}

	auto proj_idx = binder.GenerateTableIndex();
	auto proj = make_uniq<LogicalProjection>(proj_idx, std::move(proj_exprs));
	proj->children.push_back(std::move(arm_child));
	proj->ResolveOperatorTypes();
	return proj;
}

// ---------------------------------------------------------------------------
// Rewriter
// ---------------------------------------------------------------------------

static bool IsMultiBranchMarker(const LogicalGet &get) {
	if (!get.bind_data) {
		return false;
	}
	return dynamic_cast<const VgiMultiBranchMarkerBindData *>(get.bind_data.get()) != nullptr;
}

static void RewriteOne(unique_ptr<LogicalOperator> &op, ClientContext &context, Binder &binder,
                        LogicalOperator &root) {
	// Recurse children first (bottom-up). Mirrors the spike implementation.
	for (auto &child : op->children) {
		RewriteOne(child, context, binder, root);
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	if (!IsMultiBranchMarker(get)) {
		return;
	}

	auto &marker = get.bind_data->Cast<VgiMultiBranchMarkerBindData>();

	// Auto-load required extensions before binding any branch.
	for (const auto &ext : marker.required_extensions) {
		ExtensionHelper::TryAutoLoadExtension(context, ext);
	}

	// Capture old column bindings (parent operators reference these).
	auto old_bindings = get.GetColumnBindings();
	const auto &marker_column_ids = get.GetColumnIds();

	// The marker's column_ids (positional in the canonical schema) tells
	// the rewriter which canonical columns the parent operators actually
	// reference. The union's output schema is the narrowed subset, not the
	// full canonical schema — keeps binding-count consistent with the
	// marker's GetColumnBindings() output.
	std::vector<std::string> wanted_names;
	std::vector<LogicalType> wanted_types;
	if (marker_column_ids.empty()) {
		// No narrowing — emit the full canonical schema. Matches what
		// GetColumnBindings() returns when column_ids is empty (one
		// binding via GetAnyColumn for the first canonical column).
		wanted_names.push_back(marker.canonical_column_names[0]);
		wanted_types.push_back(marker.canonical_column_types[0]);
	} else {
		for (const auto &idx : marker_column_ids) {
			if (idx.IsVirtualColumn()) {
				// Virtual columns (rowid) aren't supported on multi-branch
				// unions because each arm may have different rowid semantics.
				// Fall back to the first canonical column — gives the parent
				// a real column to count over, which is what `count(*)`
				// actually needs.
				wanted_names.push_back(marker.canonical_column_names[0]);
				wanted_types.push_back(marker.canonical_column_types[0]);
			} else {
				idx_t col = idx.GetPrimaryIndex();
				if (col >= marker.canonical_column_names.size()) {
					throw InternalException(
					    "VGI multi-scan rewriter: marker column_ids index %d out of range (canonical has %d)",
					    static_cast<int>(col),
					    static_cast<int>(marker.canonical_column_names.size()));
				}
				wanted_names.push_back(marker.canonical_column_names[col]);
				wanted_types.push_back(marker.canonical_column_types[col]);
			}
		}
	}

	// Build each arm.
	vector<unique_ptr<LogicalOperator>> arms;
	arms.reserve(marker.branches.size());
	for (const auto &branch : marker.branches) {
		auto arm = BindBranchArm(branch, context, binder, marker.table_catalog_name, marker.table_schema_name,
		                          marker.default_schema, marker_column_ids);

		unique_ptr<LogicalOperator> arm_top = std::move(arm.get);

		// Optional branch_filter LogicalFilter (uses the minimal binder above).
		if (branch.parsed_branch_filter) {
			ArmColumnLookup cols{arm.raw_names, arm.raw_types, arm.bindings};
			auto bound = BindBranchFilterExpr(context, *branch.parsed_branch_filter, cols);
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions.push_back(std::move(bound));
			filter->children.push_back(std::move(arm_top));
			filter->ResolveOperatorTypes();
			arm_top = std::move(filter);
		}

		// Column reconciliation: each arm contributes wanted-shaped rows.
		auto projection = BuildArmProjection(context, std::move(arm_top), binder, arm,
		                                      wanted_names, wanted_types,
		                                      marker.canonical_column_names);
		arms.push_back(std::move(projection));
	}

	// Assemble the union. column_count = narrowed wanted column count.
	const idx_t column_count = wanted_names.size();
	auto setop_idx = binder.GenerateTableIndex();
	auto union_op = make_uniq<LogicalSetOperation>(setop_idx, column_count, std::move(arms),
	                                                LogicalOperatorType::LOGICAL_UNION,
	                                                /*setop_all=*/true,
	                                                /*allow_out_of_order=*/true);
	union_op->ResolveOperatorTypes();
	auto new_bindings = union_op->GetColumnBindings();
	if (new_bindings.size() != old_bindings.size()) {
		throw InternalException("VgiMultiScanRewriter: binding count mismatch (old=%d, new=%d) for table %s",
		                         static_cast<int>(old_bindings.size()), static_cast<int>(new_bindings.size()),
		                         marker.table_schema_name);
	}

	auto *union_ptr = union_op.get();
	op = std::move(union_op);

	// Rewrite parent expressions referencing the old marker bindings. Stop
	// descent at the union itself so we don't trash the arms' bindings.
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings.reserve(old_bindings.size());
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
	}
	replacer.stop_operator = union_ptr;
	replacer.VisitOperator(root);

	VGI_LOG(context, "vgi.multi_scan_rewriter.fired",
	    {{"branches", std::to_string(marker.branches.size())},
	     {"schema", marker.table_schema_name}});
}

class VgiMultiScanRewriter : public OptimizerExtension {
public:
	VgiMultiScanRewriter() {
		// Pre-pushdown phase: rewriting INTO standard DuckDB operators that
		// benefit from filter pushdown. Different from VgiTableBufferingRewriter
		// which is post-pushdown (its LogicalExtensionOperator is opaque to
		// pushdown). See the phase-split comment in vgi_extension.cpp.
		pre_optimize_function = Optimize;
	}

private:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		// Gate on the emergency-rollback session setting.
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_multi_branch_scans", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		if (!plan) {
			return;
		}
		RewriteOne(plan, input.context, input.optimizer.binder, *plan);
	}
};

void RegisterVgiMultiScanRewriter(DBConfig &config) {
	OptimizerExtension::Register(config, VgiMultiScanRewriter());
}

} // namespace vgi
} // namespace duckdb
