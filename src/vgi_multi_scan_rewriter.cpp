// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VgiMultiScanRewriter (see the header). Arms reconcile columns by NAME with
// NULL-fill (UNION ALL BY NAME semantics); a ColumnBindingReplacer pass rewrites
// parent expressions that referenced the marker's bindings.
//
// Known limitation: branch_filter binding supports only
// "col OP const [AND col OP const ...]" — no function calls, subqueries or
// nested expressions.

#include "vgi_multi_scan_rewriter.hpp"
#include "storage/vgi_table_function_resolver.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/settings.hpp"
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
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_table_function_impl.hpp"

#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

// ---------------------------------------------------------------------------
// Marker placeholder function (never executed; rewriter must replace it)
// ---------------------------------------------------------------------------

// Bind refuses when the rewriter cannot run (RequireVgiOptimizerExtensions), so
// this is a backstop. It must stay an ordinary error: an InternalException
// invalidates the whole database, and this is reachable from user settings.
static void MultiBranchMarkerExecute(ClientContext &, TableFunctionInput &, DataChunk &) {
	throw InvalidInputException(
	    "VgiMultiScanRewriter did not fire — multi-branch placeholder reached execution. "
	    "Check that the vgi_multi_branch_scans setting is true, that the optimizer is enabled, "
	    "and that 'extension' is not in disabled_optimizers.");
}

void RequireVgiOptimizerExtensions(ClientContext &context, const std::string &what) {
	if (!Settings::Get<EnableOptimizerSetting>(context)) {
		throw BinderException("%s needs DuckDB's optimizer, which is disabled. Re-enable it with "
		                      "PRAGMA enable_optimizer.",
		                      what);
	}
	if (Optimizer::OptimizerDisabled(context, OptimizerType::EXTENSION)) {
		throw BinderException("%s needs DuckDB's optimizer extensions, which are disabled ('extension' is in "
		                      "disabled_optimizers). Remove it from disabled_optimizers.",
		                      what);
	}
}

TableFunction MakeMultiBranchMarkerFunction() {
	TableFunction fn("vgi_multi_branch_marker", {}, MultiBranchMarkerExecute);
	return fn;
}

// ---------------------------------------------------------------------------
// Minimal branch_filter binder (scope: col OP const [AND ...])
// ---------------------------------------------------------------------------

// Column name → the arm's ColumnBinding, for resolving branch_filter references.
struct ArmColumnLookup {
	const std::vector<std::string> &names;
	const std::vector<LogicalType> &types;
	const std::vector<ColumnBinding> &bindings;
};

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
	return make_uniq<BoundConstantExpression>(cexpr.GetLiteral().ToValue());
}

static unique_ptr<Expression> BindBranchFilterComparison(ClientContext &context, const ComparisonExpression &cmp,
                                                          const ArmColumnLookup &cols) {
	auto left = BindBranchFilterExpr(context, cmp.Left(), cols);
	auto right = BindBranchFilterExpr(context, cmp.Right(), cols);
	// Implicit cast: if one side is a constant whose type differs from the
	// other, cast the constant to the other side's type. Handles
	// `ts >= TIMESTAMP '...'` cleanly without invoking the full type
	// resolver. Mismatched non-constant types throw — arbitrary type
	// coercion between non-constants is not supported.
	if (left->GetReturnType() != right->GetReturnType()) {
		if (left->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			left = BoundCastExpression::AddCastToType(context, std::move(left), right->GetReturnType());
		} else if (right->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			right = BoundCastExpression::AddCastToType(context, std::move(right), left->GetReturnType());
		} else {
			throw BinderException(
			    "VGI branch_filter comparison between mismatched types %s and %s is not supported "
			    "(no automatic cast between two non-constant operands).",
			    left->GetReturnType().ToString(), right->GetReturnType().ToString());
		}
	}
	return BoundComparisonExpression::Create(cmp.GetExpressionType(), std::move(left), std::move(right));
}

static unique_ptr<Expression> BindBranchFilterConjunction(ClientContext &context, const ConjunctionExpression &conj,
                                                            const ArmColumnLookup &cols) {
	if (conj.GetChildren().empty()) {
		throw BinderException("VGI branch_filter: empty conjunction is invalid.");
	}
	auto result = make_uniq<BoundConjunctionExpression>(conj.GetExpressionType());
	for (const auto &child : conj.GetChildren()) {
		result->GetChildrenMutable().push_back(BindBranchFilterExpr(context, *child, cols));
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
	case ExpressionClass::CAST: {
		// Typed literals — DATE '2026-10-03', TIMESTAMP '...', 42::BIGINT — parse
		// as a cast of a constant. Fold them to a typed constant; a cast of a
		// column is outside this minimal binder.
		const auto &cast = expr.Cast<CastExpression>();
		auto child = BindBranchFilterExpr(context, cast.Child(), cols);
		if (child->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			throw BinderException("VGI branch_filter only supports casts of constants (typed literals such as "
			                      "DATE '2026-01-01'), not of columns.");
		}
		const auto target = TransformStringToLogicalType(cast.TargetType().ToString(), context);
		const auto &value = child->Cast<BoundConstantExpression>().GetValue();
		Value folded;
		if (cast.IsTryCast()) {
			auto attempt = value.TryCastAs(context, target);
			folded = attempt ? *attempt : Value(target);
		} else {
			folded = value.CastAs(context, target);
		}
		return make_uniq<BoundConstantExpression>(std::move(folded));
	}
	default:
		throw BinderException(
		    "VGI branch_filter only supports column references, constants, comparisons "
		    "(=, <, >, <=, >=, <>), and AND/OR conjunctions. Expression class %d is not supported. "
		    "Restructure your filter to use only the supported expression forms.",
		    static_cast<int>(expr.GetExpressionClass()));
	}
}

// ---------------------------------------------------------------------------
// Per-arm LogicalGet construction
// ---------------------------------------------------------------------------
struct ArmBindResult {
	unique_ptr<LogicalGet> get;
	std::vector<std::string> raw_names;   // column names as the branch returned them
	std::vector<LogicalType> raw_types;   // as the branch returned them
	std::vector<ColumnBinding> bindings;  // matching ColumnBinding list
};

//! Map a declared format name to the DuckDB table function that reads it:
//! ``read_<format>`` except for the listed exceptions. An unknown format falls
//! through to the convention, so the ordinary "unknown function" path reports
//! it (with its extension-load hint).
static std::string VgiFormatReaderFunction(const std::string &format_name) {
	static const std::map<std::string, std::string> kExceptions = {
	    {"iceberg", "iceberg_scan"},
	    {"delta", "delta_scan"},
	    {"arrow", "read_arrow"},
	    {"ipc", "read_arrow"},
	};
	auto it = kExceptions.find(format_name);
	if (it != kExceptions.end()) {
		return it->second;
	}
	return "read_" + format_name;
}

// A catalog-table branch scans an attached-catalog BASE TABLE
// (source_catalog.source_schema.source_table), bound like DuckDB's own
// base-table binding (Binder::Bind(BaseTableRef), TABLE_ENTRY case). The
// companion catalog was attached at VGI-attach time, so this is a pure lookup.
static ArmBindResult BindCatalogTableArm(const VgiScanBranch &branch, ClientContext &context, Binder &binder) {
	// The companion catalog must already be attached. If it isn't (e.g.
	// `attach_companions false`, or an optional companion that failed/conflicted),
	// fail with a clear message rather than a bare "catalog not found".
	if (!Catalog::GetCatalogEntry(context, Identifier(branch.source_catalog))) {
		throw BinderException(
		    "VGI multi-branch catalog-table branch references companion catalog '%s' (table '%s.%s') which is not "
		    "attached. Companion catalogs are attached at VGI ATTACH time — was `attach_companions false` set, or did "
		    "an optional companion fail to attach?",
		    branch.source_catalog, branch.source_schema, branch.source_table);
	}

	// Resolve + scan through the 3-arg GetScanFunction that carries the
	// EntryLookupInfo. Catalog-managed sources like DuckLake REQUIRE it — the
	// 2-arg overload throws "called without entry lookup info" (it needs the
	// lookup to bind the snapshot/file-list). No AT clause = current snapshot.
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(branch.source_table)));
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, Identifier(branch.source_catalog),
	                                                    Identifier(branch.source_schema), Identifier(branch.source_table));

	unique_ptr<FunctionData> bind_data;
	auto scan_function = table.GetScanFunction(context, bind_data, table_lookup);

	vector<LogicalType> return_types;
	vector<Identifier> return_names;
	for (auto &col : table.GetColumns().Logical()) {
		return_types.push_back(col.Type());
		return_names.push_back(col.Name());
	}

	virtual_column_map_t virtual_columns;
	if (scan_function.get_virtual_columns) {
		virtual_columns = scan_function.get_virtual_columns(context, bind_data.get());
	} else {
		virtual_columns = table.GetVirtualColumns();
	}

	auto bind_index = binder.GenerateTableIndex();
	auto get = make_uniq<LogicalGet>(bind_index, BoundTableFunction(std::move(scan_function)), std::move(bind_data), return_types, return_names,
	                                 std::move(virtual_columns));

	// Full column set (same rationale as the function path — avoid the
	// GetAnyColumn() fallback in ResolveOperatorTypes).
	vector<ColumnIndex> all_cols;
	all_cols.reserve(return_types.size());
	for (idx_t i = 0; i < return_types.size(); i++) {
		all_cols.emplace_back(i);
	}
	get->SetColumnIds(std::move(all_cols));
	get->ResolveOperatorTypes();

	ArmBindResult result;
	result.raw_names = IdentifiersToStrings(return_names);
	result.raw_types = std::move(return_types);
	result.bindings = get->GetColumnBindings();
	result.get = std::move(get);
	return result;
}

static ArmBindResult BindBranchArm(const VgiScanBranch &branch, ClientContext &context, Binder &binder,
                                    const std::string &table_catalog_name,
                                    const std::string &table_schema_name, const std::string &table_name,
                                    const std::string &default_schema,
                                    const std::vector<ColumnIndex> &marker_column_ids) {
	if (branch.IsCatalogTable()) {
		(void)marker_column_ids;
		return BindCatalogTableArm(branch, context, binder);
	}

	// A FORMAT branch (a format plus locations) resolves the format to its reader
	// and takes the ordinary function path, inheriting overload selection,
	// extension auto-load and pushdown discovery. Only the three fields that path
	// reads are overridden: VgiScanBranch is not copy-assignable (it owns
	// parsed_branch_filter), and the filter belongs to the original branch.
	std::string eff_function_name = branch.function_name;
	vector<Value> eff_positional(branch.positional_arguments.begin(), branch.positional_arguments.end());
	std::map<std::string, Value> eff_named = branch.named_arguments;
	if (branch.IsFormatBranch()) {
		eff_function_name = VgiFormatReaderFunction(branch.format_name);
		// Locations become the reader's single positional argument: a LIST for a
		// multi-file read, a plain VARCHAR when there is exactly one, because some
		// readers (iceberg_scan) accept only a scalar path.
		eff_positional.clear();
		if (branch.format_locations.size() == 1) {
			eff_positional.push_back(Value(branch.format_locations[0]));
		} else {
			vector<Value> locs;
			locs.reserve(branch.format_locations.size());
			for (const auto &loc : branch.format_locations) {
				locs.push_back(Value(loc));
			}
			eff_positional.push_back(Value::LIST(LogicalType::VARCHAR, std::move(locs)));
		}
		eff_named = branch.format_options;
	}
	// Resolve through the shared resolver (vgi_table_function_resolver.hpp) used
	// by the single-branch scan and the write path, not Catalog::GetEntry, which
	// never returns table_only functions. A format branch's reader skips the
	// hidden sets.
	auto &table_catalog = Catalog::GetCatalog(context, Identifier(table_catalog_name));
	vgi::VgiTableFunctionRequest request;
	request.catalog = &table_catalog;
	request.table_schema = table_schema_name;
	request.table_name = table_name;
	request.role = "scan";
	request.function_name = eff_function_name;
	request.worker_schema = branch.IsFunctionBranch() ? branch.schema_name : std::string();
	request.default_schema = default_schema;
	request.allow_system_catalog = true;
	// A format branch names a DuckDB reader, never a worker function.
	request.format_reader = branch.IsFormatBranch();
	auto resolved = vgi::ResolveVgiTableFunction(context, request);
	auto &fn_entry = *resolved.entry;

	// Bind both positional and named arguments through DuckDB's signature
	// resolver so casts, overload selection and keyword defaults agree with SQL.
	vector<unique_ptr<Expression>> positional_arguments;
	for (const auto &value : eff_positional) {
		positional_arguments.push_back(make_uniq<BoundConstantExpression>(value));
	}
	vector<pair<Identifier, unique_ptr<Expression>>> named_arguments;
	for (const auto &item : eff_named) {
		named_arguments.emplace_back(Identifier(item.first), make_uniq<BoundConstantExpression>(item.second));
	}
	vector<Value> parameters;
	named_argument_map_t named_parameters;
	FunctionBinder function_binder(context);
	ErrorData error;
	auto overload = function_binder.BindFunction(fn_entry.name, fn_entry.functions, positional_arguments,
	                                               named_arguments, parameters, named_parameters, error);
	if (!overload.IsValid()) {
		error.Throw();
	}
	BoundTableFunction tf(fn_entry.functions.GetFunctionByOffset(overload.GetIndex()));
	tf.GetSignature().FillNamedDefaults(context, named_parameters);
	tf.SetCallArguments(parameters, named_parameters);
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
	                                  tf.function_info.get(), &binder, tf, ref);

	vector<LogicalType> return_types;
	vector<Identifier> return_names;
	if (!tf.bind) {
		throw NotImplementedException("VGI branch scans require a table-function bind callback");
	}
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

	// Each arm produces ALL of its return columns rather than mirroring the
	// marker's narrowed column_ids: those may hold virtual rowid placeholders
	// (count(*)) whose types differ between arms, and BuildArmProjection maps
	// each arm to the wanted columns anyway; pushdown narrows arms afterwards.
	// The full set also keeps ResolveOperatorTypes() off GetAnyColumn().
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
	result.raw_names = IdentifiersToStrings(return_names);
	result.raw_types = std::move(return_types);
	result.bindings = get->GetColumnBindings();
	result.get = std::move(get);
	return result;
}

// ---------------------------------------------------------------------------
// Per-arm projection (column reconciliation by NAME with NULL-fill + cast)
// ---------------------------------------------------------------------------

// A LogicalProjection above the arm (and its optional branch_filter) mapping
// raw_names to `wanted_canonical_names`: the subset of canonical columns the
// parent references (the marker's narrowed column_ids), in its order, so the
// union's column count matches the parent's bindings. Matches by NAME, casts
// on type mismatch, NULL-fills missing columns and rejects non-canonical ones.
static unique_ptr<LogicalProjection> BuildArmProjection(
    ClientContext &context, unique_ptr<LogicalOperator> arm_child, Binder &binder, const ArmBindResult &arm,
    const std::vector<std::string> &wanted_canonical_names,
    const std::vector<LogicalType> &wanted_canonical_types,
    const std::vector<std::string> &all_canonical_names) {

	vector<unique_ptr<Expression>> proj_exprs;
	proj_exprs.reserve(wanted_canonical_names.size());

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
				// DuckDB's cast machinery reports an undefined cast.
				expr = BoundCastExpression::AddCastToType(context, std::move(expr), canonical_type);
			}
			proj_exprs.push_back(std::move(expr));
		} else {
			proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value(canonical_type)));
		}
	}

	// Reject against the FULL canonical list, not the wanted subset: a canonical
	// but unreferenced column is fine.
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

// ---------------------------------------------------------------------------
// Protocol 3.2 scan planning: predicate collection, filtered fetch, pruning
// (docs/multi_branch.md)
// ---------------------------------------------------------------------------

using ConjunctList = vector<const Expression *>;

static void SplitConjuncts(const Expression &expr, ConjunctList &out) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (const auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			SplitConjuncts(*child, out);
		}
		return;
	}
	out.push_back(&expr);
}

// The WHERE conjuncts that apply to every row reaching each child of `op`.
// Filters add their conjuncts; inner joins and cross products pass them to
// both sides; outer, semi, anti and mark joins only to the preserved side;
// every other operator (projection, aggregate, window, limit, set operation,
// delim join, ...) stops them. Collection is for pruning only, so stopping
// early merely forgoes pruning.
static vector<ConjunctList> ChildConjuncts(const LogicalOperator &op, const ConjunctList &applicable) {
	vector<ConjunctList> out(op.children.size());
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER: {
		ConjunctList extended = applicable;
		for (const auto &expr : op.Cast<LogicalFilter>().expressions) {
			SplitConjuncts(*expr, extended);
		}
		for (auto &child : out) {
			child = extended;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		for (auto &child : out) {
			child = applicable;
		}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		const auto join_type = op.Cast<LogicalJoin>().join_type;
		if (join_type == JoinType::INNER) {
			for (auto &child : out) {
				child = applicable;
			}
		} else if ((join_type == JoinType::LEFT || join_type == JoinType::SEMI || join_type == JoinType::ANTI ||
		            join_type == JoinType::MARK || join_type == JoinType::SINGLE) &&
		           !out.empty()) {
			out[0] = applicable;
		} else if ((join_type == JoinType::RIGHT || join_type == JoinType::RIGHT_SEMI ||
		            join_type == JoinType::RIGHT_ANTI) &&
		           out.size() > 1) {
			out[1] = applicable;
		}
		break;
	}
	default:
		break;
	}
	return out;
}

// A predicate the planner may send to the worker and prune with: it references
// only this scan's columns (at least one), and it is deterministic and constant
// at planning time (no prepared-statement parameter, subquery or volatile call).
static bool IsPlanningPredicate(const Expression &expr, TableIndex table_index) {
	if (expr.HasParameter() || expr.HasSubquery() || expr.IsVolatile() || !expr.IsConsistent()) {
		return false;
	}
	bool references_scan = false;
	bool only_scan = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &col_ref) {
		if (col_ref.Binding().table_index == table_index) {
			references_scan = true;
		} else {
			only_scan = false;
		}
	});
	ExpressionIterator::VisitExpression<BoundReferenceExpression>(
	    expr, [&](const BoundReferenceExpression &) { only_scan = false; });
	return references_scan && only_scan;
}

// A collected predicate over exactly one declared column, in the two forms the
// planner needs: `bound` keeps the scan's column bindings (for FilterCombiner),
// `filter` is the table-filter form (BoundReference #0) used for statistics
// pruning and for serializing pushdown_filters.
struct PlanningPredicate {
	idx_t declared_column;
	unique_ptr<Expression> bound;
	unique_ptr<ExpressionFilter> filter;
};

// BETWEEN is a function in DuckDB 2.0; its own filter pushdown splits it into
// two comparisons later, but this rewrite runs first. Split it here so the
// filter encoder and FilterCombiner see plain comparisons.
//
// Identify BETWEEN by its expression type, as the engine does (FilterCombiner,
// statistics propagation), never by bind data alone: LIKE, ILIKE, regexp_matches
// and many other functions carry bind data too. Treating one as a BETWEEN reads
// a third argument a two-argument function lacks (an INTERNAL "index 2 within
// vector of size 2" error), and for a three-argument one such as LIKE ... ESCAPE
// or regexp_matches(s, p, options) reads its bind data as BETWEEN's (undefined
// behaviour). scan_planning_predicates*.test covers both shapes.
static bool IsSplittableBetween(const BoundFunctionExpression &func) {
	return func.GetExpressionType() == ExpressionType::COMPARE_BETWEEN && func.GetChildren().size() == 3 &&
	       BoundBetweenExpression::HasValidBindData(func);
}

static vector<unique_ptr<Expression>> NormalizeConjunct(const Expression &conjunct) {
	vector<unique_ptr<Expression>> out;
	if (conjunct.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		const auto &func = conjunct.Cast<BoundFunctionExpression>();
		if (IsSplittableBetween(func)) {
			out.push_back(BoundComparisonExpression::Create(BoundBetweenExpression::LowerComparisonType(func),
			                                                BoundBetweenExpression::Input(func).Copy(),
			                                                BoundBetweenExpression::LowerBound(func).Copy()));
			out.push_back(BoundComparisonExpression::Create(BoundBetweenExpression::UpperComparisonType(func),
			                                                BoundBetweenExpression::Input(func).Copy(),
			                                                BoundBetweenExpression::UpperBound(func).Copy()));
			return out;
		}
	}
	out.push_back(conjunct.Copy());
	return out;
}

// Fold constant subexpressions to literals. The pre-optimizer runs before
// DuckDB's expression rewriter, so DATE '2026-10-02' is still a cast of a
// string here; the filter encoder and statistics checks need a constant.
static void FoldConstants(ClientContext &context, unique_ptr<Expression> &expr) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return;
	}
	if (expr->IsFoldable()) {
		Value value;
		if (ExpressionExecutor::TryEvaluateScalar(context, *expr, value)) {
			auto folded = make_uniq<BoundConstantExpression>(std::move(value));
			folded->SetAlias(expr->GetAlias());
			expr = std::move(folded);
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) { FoldConstants(context, child); });
}

static vector<PlanningPredicate> CollectPlanningPredicates(ClientContext &context, const LogicalGet &get,
                                                           const ConjunctList &applicable) {
	vector<PlanningPredicate> out;
	const auto &column_ids = get.GetColumnIds();
	vector<unique_ptr<Expression>> normalized;
	for (const auto *conjunct : applicable) {
		if (!IsPlanningPredicate(*conjunct, get.table_index)) {
			continue;
		}
		for (auto &part : NormalizeConjunct(*conjunct)) {
			FoldConstants(context, part);
			normalized.push_back(std::move(part));
		}
	}
	for (const auto &owned : normalized) {
		const Expression *conjunct = owned.get();
		optional_idx binding_column;
		bool single_column = true;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *conjunct, [&](const BoundColumnRefExpression &col_ref) {
			    const idx_t col = col_ref.Binding().column_index.GetIndex();
			    if (binding_column.IsValid() && binding_column.GetIndex() != col) {
				    single_column = false;
			    }
			    binding_column = optional_idx(col);
		    });
		if (!single_column || !binding_column.IsValid() || binding_column.GetIndex() >= column_ids.size() ||
		    column_ids[binding_column.GetIndex()].IsVirtualColumn()) {
			continue;
		}
		auto filter_expr = conjunct->Copy();
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    filter_expr, [&](BoundColumnRefExpression &col_ref, unique_ptr<Expression> &child) {
			    child = make_uniq<BoundReferenceExpression>(col_ref.GetAlias(), col_ref.GetReturnType(), 0ULL);
		    });
		PlanningPredicate predicate;
		predicate.declared_column = column_ids[binding_column.GetIndex()].GetPrimaryIndex();
		predicate.bound = conjunct->Copy();
		predicate.filter = make_uniq<ExpressionFilter>(std::move(filter_expr));
		out.push_back(std::move(predicate));
	}
	return out;
}

// Serialize the predicates as a filter v2 snapshot over the declared columns.
// Each predicate is first serialized alone: one the filter encoder can't
// express, or one that would need an out-of-band join-key payload (this RPC
// has no join_keys channel), is left out. Every predicate is advisory, so
// leaving one out only forgoes pruning.
static std::optional<std::vector<uint8_t>> SerializePlanningPredicates(ClientContext &context,
                                                                       const vector<PlanningPredicate> &predicates,
                                                                       const std::vector<std::string> &std_column_names,
                                                                       const string &table_label) {
	const vector<string> column_names(std_column_names.begin(), std_column_names.end());
	auto make_set = [](const PlanningPredicate &p, TableFilterSet &set) {
		set.PushFilter(ProjectionIndex(p.declared_column), p.filter->Copy());
	};
	auto serialize = [&](TableFilterSet &set) {
		return VgiSerializeFilters(context, {}, set, column_names, table_label, "", -1, nullptr,
		                           VgiFilterColumnIndexDomain::BIND_SCHEMA);
	};
	TableFilterSet kept;
	idx_t kept_count = 0;
	for (const auto &predicate : predicates) {
		try {
			TableFilterSet single;
			make_set(predicate, single);
			auto trial = serialize(single);
			if (!trial.filter_bytes || !trial.join_keys_buffers.empty()) {
				continue;
			}
		} catch (const std::exception &) {
			continue;
		}
		make_set(predicate, kept);
		kept_count++;
	}
	if (kept_count == 0) {
		return std::nullopt;
	}
	auto serialized = serialize(kept);
	if (!serialized.filter_bytes || !serialized.join_keys_buffers.empty()) {
		return std::nullopt;
	}
	const auto *data = serialized.filter_bytes->data();
	return std::vector<uint8_t>(data, data + serialized.filter_bytes->size());
}

// Whether `branch` provably holds no row satisfying `predicates`: its column
// statistics make a predicate always false (or false-or-null), or its
// branch_filter contradicts them. Anything that can't be evaluated keeps the
// branch.
static bool BranchIsPruned(ClientContext &context, const VgiScanBranch &branch,
                           const vector<PlanningPredicate> &predicates, const VgiMultiBranchMarkerBindData &marker,
                           const ArmColumnLookup &scan_columns, const string &table_label) {
	if (predicates.empty()) {
		return false;
	}
	if (!branch.column_statistics.empty()) {
		try {
			auto stats = ParseColumnStatisticsBatch(DeserializeFromIpcBytes(branch.column_statistics),
			                                        marker.canonical_column_types, marker.canonical_column_names,
			                                        table_label, "table", table_label, context);
			for (const auto &predicate : predicates) {
				const auto &name = marker.canonical_column_names[predicate.declared_column];
				auto entry = stats.find(name);
				if (entry == stats.end() || !entry->second) {
					continue;
				}
				const auto result = predicate.filter->CheckStatistics(context, *entry->second);
				if (result == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
				    result == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
					return true;
				}
			}
		} catch (const std::exception &) {
			// Unreadable statistics never prune.
		}
	}
	if (branch.parsed_branch_filter) {
		try {
			auto bound_filter = BindBranchFilterExpr(context, *branch.parsed_branch_filter, scan_columns);
			FilterCombiner combiner(context);
			if (combiner.AddFilter(std::move(bound_filter)) == FilterResult::UNSATISFIABLE) {
				return true;
			}
			for (const auto &predicate : predicates) {
				if (combiner.AddFilter(predicate.bound->Copy()) == FilterResult::UNSATISFIABLE) {
					return true;
				}
			}
		} catch (const std::exception &) {
			// A branch_filter over columns this scan doesn't project, or a shape
			// the minimal binder rejects, never prunes here; the arm binds it later.
		}
	}
	return false;
}

static void RewriteOne(unique_ptr<LogicalOperator> &op, ClientContext &context, Binder &binder,
                        LogicalOperator &root, const ConjunctList &applicable) {
	// Bottom-up; each child receives the WHERE conjuncts that apply to it.
	{
		auto child_conjuncts = ChildConjuncts(*op, applicable);
		for (idx_t i = 0; i < op->children.size(); i++) {
			RewriteOne(op->children[i], context, binder, root, child_conjuncts[i]);
		}
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	if (!IsMultiBranchMarker(get)) {
		return;
	}

	auto &marker = get.bind_data->Cast<VgiMultiBranchMarkerBindData>();
	const string table_label = marker.table_schema_name;

	// Protocol 3.2: collect this scan's planning predicates, fetch deferred
	// branches with them, and prune branches the predicates exclude.
	auto predicates = CollectPlanningPredicates(context, get, applicable);
	if (marker.deferred) {
		auto filters = SerializePlanningPredicates(context, predicates, marker.canonical_column_names, table_label);
		auto fetched = marker.fetch_branches(context, filters);
		marker.branches = std::move(fetched.branches);
		marker.required_extensions = std::move(fetched.required_extensions);
		marker.deferred = false;
		VGI_LOG(context, "vgi.scan_planning.fetched",
		        {{"schema", table_label},
		         {"predicates_collected", std::to_string(predicates.size())},
		         {"predicates_sent", filters ? "true" : "false"},
		         {"branches", std::to_string(marker.branches.size())}});
	}
	{
		auto scan_bindings = get.GetColumnBindings();
		std::vector<std::string> scan_names;
		std::vector<LogicalType> scan_types;
		for (const auto &idx : get.GetColumnIds()) {
			const idx_t col = idx.IsVirtualColumn() ? 0 : idx.GetPrimaryIndex();
			scan_names.push_back(col < marker.canonical_column_names.size() ? marker.canonical_column_names[col] : "");
			scan_types.push_back(col < marker.canonical_column_types.size() ? marker.canonical_column_types[col]
			                                                                : LogicalType::SQLNULL);
		}
		ArmColumnLookup scan_columns {scan_names, scan_types, scan_bindings};
		const auto before = marker.branches.size();
		std::vector<VgiScanBranch> kept;
		kept.reserve(before);
		for (auto &branch : marker.branches) {
			if (!BranchIsPruned(context, branch, predicates, marker, scan_columns, table_label)) {
				kept.push_back(std::move(branch));
			}
		}
		marker.branches = std::move(kept);
		if (marker.branches.size() != before) {
			VGI_LOG(context, "vgi.scan_planning.pruned",
			        {{"schema", table_label},
			         {"before", std::to_string(before)},
			         {"after", std::to_string(marker.branches.size())}});
		}
	}
	if (marker.has_at_clause &&
	    (marker.branches.size() > 1 ||
	     (marker.branches.size() == 1 && (!marker.branches[0].branch_filter.empty() ||
	                                      marker.branches[0].IsCatalogTable() || marker.branches[0].IsFormatBranch())))) {
		throw BinderException(ErrorInfo(error_subtype::kUnsupported).Set(error_key::kOperation, "AT"),
		                      StringUtil::Format("AT (...) clauses are not supported on multi-branch VGI tables "
		                                         "[schema: %s, branches: %d]",
		                                         table_label, static_cast<int>(marker.branches.size())));
	}
	if (marker.branches.empty()) {
		// Every branch was pruned, or a filtered table's worker returned none:
		// the scan produces no rows. LogicalEmptyResult keeps the scan's types
		// and column bindings, so parent operators need no rebinding.
		op = make_uniq<LogicalEmptyResult>(std::move(op));
		VGI_LOG(context, "vgi.scan_planning.empty", {{"schema", table_label}});
		return;
	}

	for (const auto &ext : marker.required_extensions) {
		ExtensionHelper::TryAutoLoadExtension(context, ext);
	}

	// Parent operators reference these.
	auto old_bindings = get.GetColumnBindings();
	const auto &marker_column_ids = get.GetColumnIds();

	// The union outputs only the canonical columns the marker's column_ids
	// reference, so its binding count matches the marker's GetColumnBindings().
	std::vector<std::string> wanted_names;
	std::vector<LogicalType> wanted_types;
	if (marker_column_ids.empty()) {
		// Empty column_ids: GetColumnBindings() returns one binding (GetAnyColumn),
		// so emit just the first canonical column.
		wanted_names.push_back(marker.canonical_column_names[0]);
		wanted_types.push_back(marker.canonical_column_types[0]);
	} else {
		for (const auto &idx : marker_column_ids) {
			if (idx.IsVirtualColumn()) {
				// Arms may disagree on rowid semantics, so a virtual column becomes the
				// first canonical column — a real column for count(*) to count.
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

	vector<unique_ptr<LogicalOperator>> arms;
	arms.reserve(marker.branches.size());
	for (const auto &branch : marker.branches) {
		auto arm = BindBranchArm(branch, context, binder, marker.table_catalog_name, marker.table_schema_name,
		                          marker.table_name, marker.default_schema, marker_column_ids);

		unique_ptr<LogicalOperator> arm_top = std::move(arm.get);

		if (branch.parsed_branch_filter) {
			ArmColumnLookup cols{arm.raw_names, arm.raw_types, arm.bindings};
			auto bound = BindBranchFilterExpr(context, *branch.parsed_branch_filter, cols);
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions.push_back(std::move(bound));
			filter->children.push_back(std::move(arm_top));
			filter->ResolveOperatorTypes();
			arm_top = std::move(filter);
		}

		auto projection = BuildArmProjection(context, std::move(arm_top), binder, arm,
		                                      wanted_names, wanted_types,
		                                      marker.canonical_column_names);
		arms.push_back(std::move(projection));
	}

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

	// Snapshot what the post-move log needs: `op` still owns the LogicalGet
	// whose bind_data is `marker`, and the std::move below frees it — reading
	// `marker.*` afterward is a use-after-free.
	const auto branch_count = marker.branches.size();
	const string marker_schema_name = marker.table_schema_name;

	auto *union_ptr = union_op.get();
	op = std::move(union_op);

	// Rewrite parent references to the marker bindings; stop at the union so the
	// arms' bindings are untouched.
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings.reserve(old_bindings.size());
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
	}
	replacer.stop_operator = union_ptr;
	replacer.VisitOperator(root);

	VGI_LOG(context, "vgi.multi_scan_rewriter.fired",
	    {{"branches", std::to_string(branch_count)},
	     {"schema", marker_schema_name}});
}

class VgiMultiScanRewriter : public OptimizerExtension {
public:
	VgiMultiScanRewriter() {
		// Pre-pushdown (see the header); VgiTableBufferingRewriter is post-pushdown
		// because its LogicalExtensionOperator is opaque to pushdown.
		pre_optimize_function = Optimize;
	}

private:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_multi_branch_scans", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		if (!plan) {
			return;
		}
		RewriteOne(plan, input.context, input.optimizer.binder, *plan, ConjunctList());
	}
};

void RegisterVgiMultiScanRewriter(DBConfig &config) {
	OptimizerExtension::Register(config, VgiMultiScanRewriter());
}

} // namespace vgi
} // namespace duckdb
