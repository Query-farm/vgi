// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "vgi_table_function_impl.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"

#include "vgi_ipc.hpp"

#include "nanoarrow/nanoarrow.h"

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

unique_ptr<ExpressionFilter> InFilterValues(vector<Value> values) {
	auto type = values.empty() ? LogicalType::INTEGER : values[0].type();
	return make_uniq<ExpressionFilter>(
	    ExpressionFilter::CreateInExpression(make_uniq<BoundReferenceExpression>(type, 0), std::move(values)));
}

unique_ptr<Expression> ExtractField(unique_ptr<Expression> child, idx_t index) {
	auto type = StructType::GetChildType(child->GetReturnType(), index);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(child));
	arguments.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(index + 1)));
	BoundScalarFunction function(GetExtractAtFunction());
	function.SetReturnType(type);
	return make_uniq<BoundFunctionExpression>(std::move(function), std::move(arguments),
	                                          StructExtractAtFun::GetBindData(index));
}

VgiBatchPtr ReadBatch(const VgiBufferPtr &bytes) {
	REQUIRE(bytes);
	auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(bytes));
	auto batch = reader->ReadNext();
	REQUIRE(batch);
	return batch;
}

std::string FilterSpec(const VgiBatchPtr &batch) {
	auto strings = batch->column(0);
	REQUIRE(strings.length() == 1);
	REQUIRE(!strings.IsNull(0));
	return strings.GetString(0);
}

bool IsListField(const VgiBatchPtr &batch, int i) {
	return DescribeType(batch->schema()->field(i)).type == NANOARROW_TYPE_LIST;
}

void RequireV2Schema(const VgiBatchPtr &batch) {
	auto &schema = *batch->schema();
	REQUIRE(schema.field_name(0) == "filter_spec");
	REQUIRE_FALSE(schema.field_nullable(0));
	REQUIRE_FALSE(schema.metadata().empty());
	REQUIRE(schema.metadata().Get("vgi_filter_encoding") == std::string("vgi.filters.v2"));
	REQUIRE(schema.metadata().Get("vgi_filter_version") == std::string("2"));
	REQUIRE(schema.metadata().Get("vgi_evaluation_context") == std::string("vgi.none.v1"));
	REQUIRE(schema.field_metadata(0).empty());
}

} // namespace

TEST_CASE("filter v2 snapshot uses schema metadata and unprojected column refs", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	filters.SetFilterByColumnIndex(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(
	                                                       ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(41)));

	auto serialized = VgiSerializeFilters(*con.context, {1}, &filters, {"ignored", "actual"}, "test-worker");
	REQUIRE(serialized.filter_bytes);
	REQUIRE(serialized.join_keys_buffers.empty());
	auto batch = ReadBatch(serialized.filter_bytes);
	RequireV2Schema(batch);
	REQUIRE(batch->schema()->field_name(1) == "value_0");
	auto json = FilterSpec(batch);
	REQUIRE(json.find("\"encoding\":\"vgi.filters.v2\"") != std::string::npos);
	REQUIRE(json.find("\"kind\":\"snapshot\"") != std::string::npos);
	REQUIRE(json.find("\"mode\":\"required\"") != std::string::npos);
	REQUIRE(json.find("\"column_index\":1") != std::string::npos);
	REQUIRE(json.find("\"column_name\":\"actual\"") != std::string::npos);
}

TEST_CASE("filter v2 maps every comparison operator", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	struct ComparisonCase {
		ExpressionType type;
		const char *name;
	};
	const ComparisonCase cases[] = {
	    {ExpressionType::COMPARE_EQUAL, "eq"},
	    {ExpressionType::COMPARE_NOTEQUAL, "ne"},
	    {ExpressionType::COMPARE_LESSTHAN, "lt"},
	    {ExpressionType::COMPARE_LESSTHANOREQUALTO, "le"},
	    {ExpressionType::COMPARE_GREATERTHAN, "gt"},
	    {ExpressionType::COMPARE_GREATERTHANOREQUALTO, "ge"},
	    {ExpressionType::COMPARE_DISTINCT_FROM, "distinct_from"},
	    {ExpressionType::COMPARE_NOT_DISTINCT_FROM, "not_distinct_from"},
	};
	for (auto &entry : cases) {
		CAPTURE(entry.name);
		TableFilterSet filters;
		filters.SetFilterByColumnIndex(ProjectionIndex(0),
		                               ExpressionFilter::CreateComparisonFilter(entry.type, Value::INTEGER(1)));
		auto json =
		    FilterSpec(ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"n"}, "test-worker").filter_bytes));
		REQUIRE(json.find(string("\"op\":\"") + entry.name + "\"") != std::string::npos);
	}
}

TEST_CASE("filter v2 maps null predicates", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (bool negated : {false, true}) {
		CAPTURE(negated);
		TableFilterSet filters;
		if (negated) {
			filters.SetFilterByColumnIndex(ProjectionIndex(0),
			                               make_uniq<ExpressionFilter>(ExpressionFilter::CreateNullCheckExpression(
			                                   make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0),
			                                   ExpressionType::OPERATOR_IS_NOT_NULL)));
		} else {
			filters.SetFilterByColumnIndex(
			    ProjectionIndex(0),
			    make_uniq<ExpressionFilter>(ExpressionFilter::CreateNullCheckExpression(
			        make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0), ExpressionType::OPERATOR_IS_NULL)));
		}
		auto json =
		    FilterSpec(ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"n"}, "test-worker").filter_bytes));
		REQUIRE(json.find("\"node\":\"is_null\"") != std::string::npos);
		REQUIRE(json.find(negated ? "\"negated\":true" : "\"negated\":false") != std::string::npos);
	}
}

TEST_CASE("filter v2 preserves arbitrarily nested field_ref nodes", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	auto middle_type =
	    LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::INTEGER}, {"leaf", LogicalType::VARCHAR}});
	auto root_type = LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"middle", middle_type}});
	auto reference = make_uniq<BoundReferenceExpression>(root_type, 0);
	auto leaf = ExtractField(ExtractField(std::move(reference), 1), 2);
	filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(BoundComparisonExpression::Create(
	                                                       ExpressionType::COMPARE_EQUAL, std::move(leaf),
	                                                       make_uniq<BoundConstantExpression>(Value("x")))));

	auto serialized = VgiSerializeFilters(*con.context, {0}, &filters, {"root"}, "test-worker");
	auto json = FilterSpec(ReadBatch(serialized.filter_bytes));
	auto middle = json.find("\"field_name\":\"middle\"");
	auto leaf_pos = json.find("\"field_name\":\"leaf\"");
	REQUIRE(middle != std::string::npos);
	REQUIRE(leaf_pos != std::string::npos);
	REQUIRE(middle < leaf_pos);
}

TEST_CASE("filter v2 makes the filter-key index domain explicit", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	filters.SetFilterByColumnIndex(
	    ProjectionIndex(2), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::INTEGER(7)));
	auto direct = VgiSerializeFilters(*con.context, {}, &filters, {"a", "b", "c"}, "test-worker", "", -1, nullptr,
	                                  VgiFilterColumnIndexDomain::BIND_SCHEMA);
	auto json = FilterSpec(ReadBatch(direct.filter_bytes));
	REQUIRE(json.find("\"column_index\":2") != std::string::npos);
	REQUIRE(json.find("\"column_name\":\"c\"") != std::string::npos);
	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"a", "b", "c"}, "test-worker"),
	                  InvalidInputException);

	TableFilterSet rowid_filters;
	rowid_filters.SetFilterByColumnIndex(
	    ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::BIGINT(9)));
	auto rowid = VgiSerializeFilters(*con.context, {COLUMN_IDENTIFIER_ROW_ID}, &rowid_filters, {"a", "b"},
	                                 "test-worker", "rid", 1);
	auto rowid_json = FilterSpec(ReadBatch(rowid.filter_bytes));
	REQUIRE(rowid_json.find("\"column_index\":1") != std::string::npos);
	REQUIRE(rowid_json.find("\"column_name\":\"rid\"") != std::string::npos);

	TableFilterSet shifted_filters;
	shifted_filters.SetFilterByColumnIndex(
	    ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::INTEGER(7)));
	auto shifted = VgiSerializeFilters(*con.context, {1}, &shifted_filters, {"a", "b"}, "test-worker", "rid", 0);
	auto shifted_json = FilterSpec(ReadBatch(shifted.filter_bytes));
	REQUIRE(shifted_json.find("\"column_index\":2") != std::string::npos);
	REQUIRE(shifted_json.find("\"column_name\":\"b\"") != std::string::npos);
}

TEST_CASE("filter v2 chooses exact inline and external IN encodings", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);

	TableFilterSet small_filters;
	small_filters.SetFilterByColumnIndex(ProjectionIndex(0),
	                                     InFilterValues(vector<Value> {Value::INTEGER(1), Value::INTEGER(2)}));
	auto small = VgiSerializeFilters(*con.context, {0}, &small_filters, {"key"}, "test-worker");
	auto small_batch = ReadBatch(small.filter_bytes);
	REQUIRE(small.join_keys_buffers.empty());
	REQUIRE(small_batch->schema()->field_name(1) == "value_0");
	REQUIRE(IsListField(small_batch, 1));
	REQUIRE(FilterSpec(small_batch).find("\"kind\":\"literal\"") != std::string::npos);

	vector<Value> values;
	for (idx_t i = 0; i < 1100; i++) {
		values.push_back(Value::INTEGER(NumericCast<int32_t>(i)));
	}
	TableFilterSet large_filters;
	large_filters.SetFilterByColumnIndex(ProjectionIndex(0), InFilterValues(std::move(values)));
	auto large = VgiSerializeFilters(*con.context, {0}, &large_filters, {"key"}, "test-worker");
	REQUIRE(large.join_keys_buffers.size() == 1);
	auto large_batch = ReadBatch(large.filter_bytes);
	REQUIRE(large_batch->num_columns() == 1);
	auto json = FilterSpec(large_batch);
	REQUIRE(json.find("\"kind\":\"external\"") != std::string::npos);
	REQUIRE(json.find("\"batch_index\":0") != std::string::npos);
	REQUIRE(json.find("\"column_index\":0") != std::string::npos);
	auto keys = ReadBatch(large.join_keys_buffers[0]);
	REQUIRE(keys->num_rows() == 1100);
	REQUIRE(keys->schema()->field_name(0) == "key");
	REQUIRE(keys->schema()->metadata().Get("vgi_join_keys_version") == std::string("2"));

	TableFilterSet empty_filters;
	auto empty_in = InFilterValues({});
	empty_filters.SetFilterByColumnIndex(ProjectionIndex(0), std::move(empty_in));
	auto empty = VgiSerializeFilters(*con.context, {0}, &empty_filters, {"key"}, "test-worker");
	auto empty_batch = ReadBatch(empty.filter_bytes);
	REQUIRE(empty.join_keys_buffers.empty());
	REQUIRE(empty_batch->schema()->field_name(1) == "value_0");
	REQUIRE(FilterSpec(empty_batch).find("\"node\":\"literal\",\"value_ref\":0") != std::string::npos);
}

TEST_CASE("filter v2 safely serializes expression IN lists", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (auto expression_type : {ExpressionType::COMPARE_IN, ExpressionType::COMPARE_NOT_IN}) {
		CAPTURE(expression_type);
		auto expression = make_uniq<BoundOperatorExpression>(expression_type, LogicalType::BOOLEAN);
		expression->GetChildrenMutable().push_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, 0));
		expression->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value("green")));
		expression->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value("blue")));
		TableFilterSet filters;
		filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(expression)));

		auto batch = ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"color"}, "test-worker").filter_bytes);
		auto json = FilterSpec(batch);
		REQUIRE(json.find("\"node\":\"in\"") != std::string::npos);
		REQUIRE(
		    json.find(expression_type == ExpressionType::COMPARE_NOT_IN ? "\"negated\":true" : "\"negated\":false") !=
		    std::string::npos);
		REQUIRE(IsListField(batch, 1));
	}
}

TEST_CASE("filter v2 maps DuckDB standard functions to canonical names", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	struct StandardFunctionCase {
		const char *bound_name;
		const char *wire_name;
		LogicalType input_type;
	};
	const StandardFunctionCase cases[] = {
	    {"prefix", "starts_with", LogicalType::VARCHAR},
	    {"suffix", "ends_with", LogicalType::VARCHAR},
	    {"contains", "contains", LogicalType::VARCHAR},
	    {"list_contains", "list_contains", LogicalType::LIST(LogicalType::VARCHAR)},
	};
	for (const auto &entry : cases) {
		CAPTURE(entry.bound_name);
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundReferenceExpression>(entry.input_type, 0));
		arguments.push_back(make_uniq<BoundConstantExpression>(Value("needle")));
		ScalarFunction function(entry.bound_name, {entry.input_type, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
		                        scalar_function_t {});
		function.SetCatalogName("system");
		function.SetSchemaName("main");
		auto call = make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments), nullptr);
		TableFilterSet filters;
		filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(call)));

		auto json = FilterSpec(
		    ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"input"}, "test-worker").filter_bytes));
		REQUIRE(json.find(string("\"function\":\"") + entry.wire_name + "\"") != std::string::npos);
	}

	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, 0));
	arguments.push_back(make_uniq<BoundConstantExpression>(Value("needle")));
	ScalarFunction shadowed_function("contains", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                                 scalar_function_t {});
	shadowed_function.SetCatalogName("user_catalog");
	shadowed_function.SetSchemaName("main");
	auto call =
	    make_uniq<BoundFunctionExpression>(BoundScalarFunction(shadowed_function), std::move(arguments), nullptr);
	TableFilterSet filters;
	filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(call)));
	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"input"}, "test-worker"),
	                  InvalidInputException);

	auto require_unsupported = [&](const char *name, const LogicalType &first_type, const LogicalType &second_type) {
		vector<unique_ptr<Expression>> rejected_arguments;
		rejected_arguments.push_back(make_uniq<BoundReferenceExpression>(first_type, 0));
		rejected_arguments.push_back(make_uniq<BoundConstantExpression>(Value(second_type)));
		ScalarFunction rejected_function(name, {first_type, second_type}, LogicalType::BOOLEAN, scalar_function_t {});
		rejected_function.SetCatalogName("system");
		rejected_function.SetSchemaName("main");
		auto rejected_call = make_uniq<BoundFunctionExpression>(BoundScalarFunction(rejected_function),
		                                                        std::move(rejected_arguments), nullptr);
		TableFilterSet rejected_filters;
		rejected_filters.SetFilterByColumnIndex(ProjectionIndex(0),
		                                        make_uniq<ExpressionFilter>(std::move(rejected_call)));
		REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &rejected_filters, {"input"}, "test-worker"),
		                  InvalidInputException);
	};
	require_unsupported("list_contains", LogicalType::LIST(LogicalType::INTEGER), LogicalType::VARCHAR);
	require_unsupported("contains", LogicalType::VARCHAR_COLLATION("nocase"), LogicalType::VARCHAR);
	require_unsupported("list_contains", LogicalType::LIST(LogicalType::VARCHAR_COLLATION("nocase")),
	                    LogicalType::VARCHAR_COLLATION("nocase"));
}

TEST_CASE("required IN exceeding the transport limit fails closed", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	DBConfig::GetConfig(*con.context)
	    .AddExtensionOption("vgi_join_keys_max_bytes", "test limit", LogicalType::UBIGINT, Value::UBIGINT(0));
	REQUIRE_FALSE(con.Query("SET vgi_join_keys_max_bytes=1")->HasError());
	TableFilterSet filters;
	filters.SetFilterByColumnIndex(ProjectionIndex(0),
	                               InFilterValues(vector<Value> {Value::BIGINT(1), Value::BIGINT(2)}));
	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker"), InvalidInputException);
}

TEST_CASE("required OR serialization is all or decline", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	vector<unique_ptr<Expression>> children;
	children.push_back(
	    std::move(ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::INTEGER(1))->expr));
	auto dynamic = make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(0));
	children.push_back(CreateDynamicFilterExpression(dynamic, LogicalType::INTEGER));
	filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(BoundConjunctionExpression::Create(
	                                                       ExpressionType::CONJUNCTION_OR, std::move(children))));

	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker"), InvalidInputException);
}

TEST_CASE("optional Top-N null ordering stays atomic before its dynamic bound arrives", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (bool include_required : {false, true}) {
		CAPTURE(include_required);
		auto dynamic = make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(0));
		vector<unique_ptr<Expression>> alternatives;
		alternatives.push_back(ExpressionFilter::CreateNullCheckExpression(
		    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0), ExpressionType::OPERATOR_IS_NULL));
		alternatives.push_back(CreateDynamicFilterExpression(dynamic, LogicalType::INTEGER));
		auto optional = CreateOptionalFilterExpression(
		    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(alternatives)),
		    LogicalType::INTEGER);
		vector<unique_ptr<Expression>> conjuncts;
		conjuncts.push_back(std::move(optional));
		if (include_required) {
			conjuncts.push_back(std::move(
			    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5))
			        ->expr));
		}
		auto filter = make_uniq<ExpressionFilter>(
		    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(conjuncts)));
		REQUIRE(VgiContainsDynamicFilter(*filter));
		TableFilterSet filters;
		filters.SetFilterByColumnIndex(ProjectionIndex(0), std::move(filter));
		auto json = FilterSpec(
		    ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"score"}, "test-worker").filter_bytes));
		REQUIRE(json.find("\"node\":\"is_null\"") == std::string::npos);
		REQUIRE(json.find("\"mode\":\"advisory\"") == std::string::npos);
		if (include_required) {
			REQUIRE(json.find("\"mode\":\"required\"") != std::string::npos);
			REQUIRE(json.find("\"op\":\"gt\"") != std::string::npos);
		} else {
			REQUIRE(json.find("\"predicates\":[]") != std::string::npos);
		}
	}
}

TEST_CASE("required throwing AND remains one atomic predicate", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto reference = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0);
	auto narrowing = BoundCastExpression::AddCastToType(*con.context, std::move(reference), LogicalType::TINYINT);
	auto comparison = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, std::move(narrowing),
	                                                    make_uniq<BoundConstantExpression>(Value::TINYINT(0)));
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(comparison));
	children.push_back(
	    std::move(ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_NOTEQUAL, Value::BIGINT(0))->expr));
	TableFilterSet filters;
	filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(BoundConjunctionExpression::Create(
	                                                       ExpressionType::CONJUNCTION_AND, std::move(children))));

	auto batch = ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker").filter_bytes);
	auto json = FilterSpec(batch);
	REQUIRE(json.find("\"node\":\"and\"") != std::string::npos);
	REQUIRE(json.find("\"id\":", json.find("\"id\":") + 1) == std::string::npos);
}

TEST_CASE("filter v2 encodes advisory mode and canonical type references", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	auto reference = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0);
	auto cast = BoundCastExpression::AddCastToType(*con.context, std::move(reference), LogicalType::BIGINT);
	auto comparison = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(cast),
	                                                    make_uniq<BoundConstantExpression>(Value::BIGINT(42)));
	filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(
	                                                       std::move(comparison), LogicalType::INTEGER)));

	auto batch = ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker").filter_bytes);
	REQUIRE(batch->schema()->GetFieldIndex("type_0") >= 1);
	REQUIRE(batch->schema()->GetFieldIndex("value_0") >= 1);
	REQUIRE(batch->column(batch->schema()->GetFieldIndex("type_0")).IsNull(0));
	auto json = FilterSpec(batch);
	REQUIRE(json.find("\"type_ref\":0") != std::string::npos);
	REQUIRE(json.find("\"value_ref\":0") != std::string::npos);
	REQUIRE(json.find("\"mode\":\"advisory\",\"source\":\"other\"") != std::string::npos);
}

TEST_CASE("filter v2 gates structured extension calls on exact capabilities", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(LogicalType::GEOMETRY(), 0));
	arguments.push_back(make_uniq<BoundConstantExpression>(Value(LogicalType::GEOMETRY())));
	ScalarFunction function("&&", {LogicalType::GEOMETRY(), LogicalType::GEOMETRY()}, LogicalType::BOOLEAN,
	                        scalar_function_t {});
	auto call = make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments), nullptr, true);
	TableFilterSet filters;
	filters.SetFilterByColumnIndex(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(call)));

	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"geom"}, "test-worker"), InvalidInputException);
	std::vector<VgiFilterFunctionCapability> capabilities {{"duckdb.spatial", "intersects_extent", 1}};
	auto encoded = VgiSerializeFilters(*con.context, {0}, &filters, {"geom"}, "test-worker", "", -1, nullptr,
	                                   VgiFilterColumnIndexDomain::PROJECTED, capabilities);
	auto json = FilterSpec(ReadBatch(encoded.filter_bytes));
	REQUIRE(json.find("\"function\":{\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\","
	                  "\"version\":1}") != std::string::npos);
}

TEST_CASE("filter v2 tick delta carries revisioned upsert and remove", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto filter =
	    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::INTEGER(99));
	vector<VgiDynamicFilterDeltaUpdate> updates {{"top_n:0", 3, 0, "score", filter.get()},
	                                             {"top_n:1", 4, 1, "other", nullptr}};

	auto batch = ReadBatch(VgiSerializeDynamicFilterDelta(*con.context, "test-worker", updates));
	RequireV2Schema(batch);
	auto json = FilterSpec(batch);
	REQUIRE(json.find("\"kind\":\"delta\"") != std::string::npos);
	REQUIRE(json.find("\"operation\":\"upsert\",\"id\":\"top_n:0\",\"revision\":3") != std::string::npos);
	REQUIRE(json.find("\"mode\":\"advisory\",\"source\":\"top_n\"") != std::string::npos);
	REQUIRE(json.find("\"operation\":\"remove\",\"id\":\"top_n:1\",\"revision\":4") != std::string::npos);
}
