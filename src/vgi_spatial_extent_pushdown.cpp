// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Derived bounding-box filters for exact spatial predicates.
//
// DuckDB offers `ST_Intersects(geom, <constant>)` to a scan as a filter, but a
// worker can only take the bounding-box test `geom && <constant>`
// (`duckdb.spatial/intersects_extent@1`), so VGI declines the exact predicate
// and the worker reads every row. Every predicate below implies that the two
// bounding boxes intersect, so `geom && <constant>` is a sound pre-filter for
// it:
//
//   ST_Intersects, ST_Contains, ST_Within, ST_Covers, ST_CoveredBy,
//   ST_Touches, ST_Overlaps, ST_Crosses, ST_Equals, ST_ContainsProperly
//
// (DuckDB's `&&` is inclusive: boxes that only share an edge or corner
// intersect, which ST_Touches needs.) Negated forms (ST_Disjoint, NOT ...) are
// never derived from: they keep rows whose boxes do NOT intersect.
//
// After DuckDB's filter pushdown, this optimizer finds such a predicate in a
// LogicalFilter directly above a VGI scan whose worker declared
// intersects_extent, and pushes `geom && <constant>` into the scan as an
// OPTIONAL filter, which VGI serializes as an advisory predicate. The exact
// predicate stays in the LogicalFilter, so the result is unchanged: the worker
// may use the box to read less, and DuckDB still applies the exact test.
//
// Only a constant with a real box is used: one with at least one vertex and
// finite X and Y ranges. An empty constant has no box, so `geom && EMPTY` is
// false for every row, yet spatial's ST_Equals(EMPTY, EMPTY) is true. A
// constant with a NaN coordinate is empty to spatial (GEOS) but has an
// unbounded axis to DuckDB's extent code; such constants, and infinite ones, are
// skipped too. Every listed predicate is false for an empty constant, so the
// gate loses nothing.
//
// The derived filter is always a plain `col && <constant>` over the bare
// column, the only form VGI's filter encoding carries: the constant is
// relabelled to a CRS-typed column's type (a CRS cast is metadata only), a
// constant with a different CRS derives nothing, and a bind that would put a
// cast on the column is dropped. (DuckDB 2.0 binds GEOMETRY('EPSG:4326')
// columns without such a cast today; these are guards.)
//
// Phase: optimize_function (post-optimize), after FilterPushdown has settled
// which predicates the scan took.

#include "vgi_spatial_extent_pushdown.hpp"
#include "vgi_logging.hpp"
#include "vgi_table_function_impl.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <set>

namespace duckdb {
namespace vgi {
namespace {

//! Exact predicates that imply `bbox(a) && bbox(b)`, in either argument order.
const std::set<string> &ExtentImplyingPredicates() {
	static const std::set<string> names {"st_intersects", "st_contains",        "st_within",   "st_covers",
	                                     "st_coveredby",  "st_touches",         "st_overlaps", "st_crosses",
	                                     "st_equals",     "st_containsproperly"};
	return names;
}

bool WorkerTakesIntersectsExtent(const VgiTableFunctionBindData &data) {
	if (std::find(data.filter_semantic_profiles.begin(), data.filter_semantic_profiles.end(),
	              "vgi.duckdb.standard.v1") == data.filter_semantic_profiles.end()) {
		return false;
	}
	for (const auto &capability : data.additional_filter_functions) {
		if (capability.namespace_name == "duckdb.spatial" && capability.name == "intersects_extent" &&
		    capability.version == 1) {
			return true;
		}
	}
	return false;
}

bool IsGeometry(const LogicalType &type) {
	return type.id() == LogicalTypeId::GEOMETRY;
}

//! Whether `constant` (a non-NULL GEOMETRY) has a usable bounding box: at
//! least one vertex and finite X and Y ranges. See the file comment.
bool HasFiniteExtent(const Value &constant) {
	try {
		auto &wkb = StringValue::Get(constant);
		auto extent = GeometryExtent::Empty();
		auto vertices = Geometry::GetExtent(string_t(wkb.data(), UnsafeNumericCast<uint32_t>(wkb.size())), extent);
		return vertices > 0 && extent.HasXY();
	} catch (std::exception &) {
		return false;
	}
}

//! The GEOMETRY column ref under `expr`: the expression itself, or a
//! GEOMETRY-to-GEOMETRY cast of it (a CRS relabel). nullptr otherwise.
const BoundColumnRefExpression *GeometryColumnRef(const Expression &expr) {
	const Expression *inner = &expr;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (!BoundCastExpression::IsCast(function) || BoundCastExpression::IsTryCast(function) ||
		    !IsGeometry(BoundCastExpression::SourceType(function)) || !IsGeometry(function.GetReturnType())) {
			return nullptr;
		}
		inner = &BoundCastExpression::Child(function);
	}
	if (inner->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF || !IsGeometry(inner->GetReturnType())) {
		return nullptr;
	}
	return &inner->Cast<BoundColumnRefExpression>();
}

//! `constant` as a value of `column_type`: unchanged when the types match,
//! relabelled when the constant has no CRS. Fails for a different CRS.
bool ConstantForColumn(const Value &constant, const LogicalType &column_type, Value &result) {
	if (constant.type() == column_type) {
		result = constant;
		return true;
	}
	if (GeoType::HasCRS(constant.type())) {
		return false;
	}
	auto cast = constant.DefaultTryCastAs(column_type);
	if (!cast) {
		return false;
	}
	result = std::move(*cast);
	return true;
}

bool ContainsEqual(const Expression &expr, const Expression &candidate) {
	if (expr.Equals(candidate)) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { found = found || ContainsEqual(child, candidate); });
	return found;
}

class SpatialExtentOptimizer : public OptimizerExtension {
public:
	SpatialExtentOptimizer() {
		optimize_function = Optimize;
	}

private:
	//! `geom && <constant>` for an exact spatial predicate over a column of
	//! `get` and a constant geometry; nullptr when the predicate does not fit.
	static unique_ptr<Expression> DeriveExtentFilter(ClientContext &context, const LogicalGet &get,
	                                                 const Expression &expr, ProjectionIndex &column) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			return nullptr;
		}
		auto &function = expr.Cast<BoundFunctionExpression>();
		auto name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
		if (!ExtentImplyingPredicates().count(name) || function.GetChildren().size() != 2 ||
		    function.GetReturnType().id() != LogicalTypeId::BOOLEAN ||
		    function.Function().GetCatalogName() != "system" || function.Function().GetSchemaName() != "main") {
			return nullptr;
		}
		auto &children = function.GetChildren();
		for (idx_t side = 0; side < 2; side++) {
			auto *ref = GeometryColumnRef(*children[side]);
			auto &other = *children[1 - side];
			if (!ref || !IsGeometry(other.GetReturnType()) || !other.IsFoldable()) {
				continue;
			}
			if (ref->Binding().table_index != get.table_index) {
				continue;
			}
			Value evaluated;
			if (!ExpressionExecutor::TryEvaluateScalar(context, other, evaluated) || evaluated.IsNull()) {
				// A NULL constant makes the predicate NULL: the exact filter drops every
				// row on its own, and there is no box to derive.
				return nullptr;
			}
			auto &column_type = ref->GetReturnType();
			Value constant;
			if (!ConstantForColumn(evaluated, column_type, constant) || !HasFiniteExtent(constant)) {
				return nullptr;
			}
			vector<unique_ptr<Expression>> args;
			args.push_back(make_uniq<BoundReferenceExpression>(column_type, 0ULL));
			args.push_back(make_uniq<BoundConstantExpression>(constant));
			FunctionBinder binder(context);
			ErrorData error;
			auto derived = binder.BindScalarFunction(Identifier(DEFAULT_SCHEMA), Identifier("st_intersects_extent"),
			                                         std::move(args), error);
			// The column side must reach the worker as the bare column: VGI's filter
			// encoding cannot carry a cast on it.
			if (!derived || derived->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION ||
			    derived->Cast<BoundFunctionExpression>().GetChildren()[0]->GetExpressionClass() !=
			        ExpressionClass::BOUND_REF) {
				return nullptr;
			}
			column = ref->Binding().column_index;
			return derived;
		}
		return nullptr;
	}

	static void Visit(ClientContext &context, LogicalOperator &op) {
		for (auto &child : op.children) {
			Visit(context, *child);
		}
		if (op.type != LogicalOperatorType::LOGICAL_FILTER || op.children.size() != 1) {
			return;
		}
		auto *below = op.children[0].get();
		while (below->type == LogicalOperatorType::LOGICAL_FILTER && below->children.size() == 1) {
			below = below->children[0].get();
		}
		if (below->type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto &get = below->Cast<LogicalGet>();
		if (get.function.function != VgiTableFunctionScan || !get.bind_data || !get.function.filter_pushdown) {
			return;
		}
		auto &data = get.bind_data->Cast<VgiTableFunctionBindData>();
		if (!WorkerTakesIntersectsExtent(data)) {
			return;
		}
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			ProjectionIndex column;
			auto derived = DeriveExtentFilter(context, get, *expr, column);
			if (!derived) {
				continue;
			}
			auto type = derived->Cast<BoundFunctionExpression>().GetChildren()[0]->GetReturnType();
			auto optional = CreateOptionalFilterExpression(std::move(derived), type);
			// Idempotent: a second optimizer pass over the same plan pushes nothing new.
			auto existing = get.table_filters.TryGetFilterByColumnIndex(column);
			if (existing &&
			    ContainsEqual(*ExpressionFilter::GetExpressionFilter(*existing, "SpatialExtentOptimizer").expr,
			                  *optional)) {
				continue;
			}
			if (VgiInfoLogActive(context)) {
				VGI_LOG(context, "table_function.spatial_extent_derived",
				        {{"function_name", data.function_name},
				         {"predicate", expr->ToString()},
				         {"derived", optional->ToString()}});
			}
			get.table_filters.PushFilter(column, make_uniq<ExpressionFilter>(std::move(optional)));
		}
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Visit(input.context, *plan);
	}
};

} // namespace

void RegisterVgiSpatialExtentOptimizer(DBConfig &config) {
	OptimizerExtension::Register(config, SpatialExtentOptimizer());
}

} // namespace vgi
} // namespace duckdb
