// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "vgi_table_function_impl.hpp"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

std::shared_ptr<arrow::RecordBatch> ReadBatch(const std::shared_ptr<arrow::Buffer> &bytes) {
	auto input = std::make_shared<arrow::io::BufferReader>(bytes);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	REQUIRE(reader_result.ok());
	auto reader = reader_result.ValueUnsafe();
	std::shared_ptr<arrow::RecordBatch> batch;
	REQUIRE(reader->ReadNext(&batch).ok());
	REQUIRE(batch);
	return batch;
}

std::string FilterSpec(const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto strings = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
	REQUIRE(strings->length() == 1);
	REQUIRE(!strings->IsNull(0));
	return strings->GetString(0);
}

void RequireV2Schema(const std::shared_ptr<arrow::RecordBatch> &batch) {
	REQUIRE(batch->schema()->field(0)->name() == "filter_spec");
	REQUIRE_FALSE(batch->schema()->field(0)->nullable());
	REQUIRE(batch->schema()->metadata());
	REQUIRE(batch->schema()->metadata()->Get("vgi_filter_encoding").ValueOrDie() == "vgi.filters.v2");
	REQUIRE(batch->schema()->metadata()->Get("vgi_filter_version").ValueOrDie() == "2");
	REQUIRE(batch->schema()->metadata()->Get("vgi_evaluation_context").ValueOrDie() == "vgi.none.v1");
	REQUIRE_FALSE(batch->schema()->field(0)->metadata());
}

} // namespace

TEST_CASE("filter v2 snapshot uses schema metadata and unprojected column refs", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	filters.filters[0] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(41));

	auto serialized = VgiSerializeFilters(*con.context, {1}, &filters, {"ignored", "actual"}, "test-worker");
	REQUIRE(serialized.filter_bytes);
	REQUIRE(serialized.join_keys_buffers.empty());
	auto batch = ReadBatch(serialized.filter_bytes);
	RequireV2Schema(batch);
	REQUIRE(batch->schema()->field(1)->name() == "value_0");
	auto json = FilterSpec(batch);
	REQUIRE(json.find("\"encoding\":\"vgi.filters.v2\"") != std::string::npos);
	REQUIRE(json.find("\"kind\":\"snapshot\"") != std::string::npos);
	REQUIRE(json.find("\"mode\":\"required\"") != std::string::npos);
	REQUIRE(json.find("\"column_index\":1") != std::string::npos);
	REQUIRE(json.find("\"column_name\":\"actual\"") != std::string::npos);
}

TEST_CASE("filter v2 preserves arbitrarily nested field_ref nodes", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	auto leaf = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value("x"));
	auto inner = make_uniq<StructFilter>(2, "leaf", std::move(leaf));
	filters.filters[0] = make_uniq<StructFilter>(1, "middle", std::move(inner));

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
	filters.filters[2] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::INTEGER(7));
	auto direct = VgiSerializeFilters(*con.context, {}, &filters, {"a", "b", "c"}, "test-worker", "", -1, nullptr,
	                                  VgiFilterColumnIndexDomain::BIND_SCHEMA);
	auto json = FilterSpec(ReadBatch(direct.filter_bytes));
	REQUIRE(json.find("\"column_index\":2") != std::string::npos);
	REQUIRE(json.find("\"column_name\":\"c\"") != std::string::npos);
	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"a", "b", "c"}, "test-worker"),
	                  InvalidInputException);

	TableFilterSet rowid_filters;
	rowid_filters.filters[0] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::BIGINT(9));
	auto rowid = VgiSerializeFilters(*con.context, {COLUMN_IDENTIFIER_ROW_ID}, &rowid_filters, {"a", "b"},
	                                 "test-worker", "rid", 1);
	auto rowid_json = FilterSpec(ReadBatch(rowid.filter_bytes));
	REQUIRE(rowid_json.find("\"column_index\":1") != std::string::npos);
	REQUIRE(rowid_json.find("\"column_name\":\"rid\"") != std::string::npos);

	TableFilterSet shifted_filters;
	shifted_filters.filters[0] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::INTEGER(7));
	auto shifted = VgiSerializeFilters(*con.context, {1}, &shifted_filters, {"a", "b"}, "test-worker", "rid", 0);
	auto shifted_json = FilterSpec(ReadBatch(shifted.filter_bytes));
	REQUIRE(shifted_json.find("\"column_index\":2") != std::string::npos);
	REQUIRE(shifted_json.find("\"column_name\":\"b\"") != std::string::npos);
}

TEST_CASE("filter v2 chooses exact inline and external IN encodings", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);

	TableFilterSet small_filters;
	small_filters.filters[0] = make_uniq<InFilter>(vector<Value> {Value::INTEGER(1), Value::INTEGER(2)});
	auto small = VgiSerializeFilters(*con.context, {0}, &small_filters, {"key"}, "test-worker");
	auto small_batch = ReadBatch(small.filter_bytes);
	REQUIRE(small.join_keys_buffers.empty());
	REQUIRE(small_batch->schema()->field(1)->name() == "value_0");
	REQUIRE(small_batch->schema()->field(1)->type()->id() == arrow::Type::LIST);
	REQUIRE(FilterSpec(small_batch).find("\"kind\":\"literal\"") != std::string::npos);

	vector<Value> values;
	for (idx_t i = 0; i < 1100; i++) {
		values.push_back(Value::INTEGER(NumericCast<int32_t>(i)));
	}
	TableFilterSet large_filters;
	large_filters.filters[0] = make_uniq<InFilter>(std::move(values));
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
	REQUIRE(keys->schema()->field(0)->name() == "key");
	REQUIRE(keys->schema()->metadata()->Get("vgi_join_keys_version").ValueOrDie() == "2");

	TableFilterSet empty_filters;
	auto empty_in = make_uniq<InFilter>(vector<Value> {Value::INTEGER(1)});
	empty_in->values.clear();
	empty_filters.filters[0] = std::move(empty_in);
	auto empty = VgiSerializeFilters(*con.context, {0}, &empty_filters, {"key"}, "test-worker");
	auto empty_batch = ReadBatch(empty.filter_bytes);
	REQUIRE(empty.join_keys_buffers.empty());
	REQUIRE(empty_batch->schema()->field(1)->name() == "value_0");
	REQUIRE(FilterSpec(empty_batch).find("\"node\":\"literal\",\"value_ref\":0") != std::string::npos);
}

TEST_CASE("required IN exceeding the transport limit fails closed", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	DBConfig::GetConfig(*con.context)
	    .AddExtensionOption("vgi_join_keys_max_bytes", "test limit", LogicalType::UBIGINT, Value::UBIGINT(0));
	REQUIRE_FALSE(con.Query("SET vgi_join_keys_max_bytes=1")->HasError());
	TableFilterSet filters;
	filters.filters[0] = make_uniq<InFilter>(vector<Value> {Value::BIGINT(1), Value::BIGINT(2)});
	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker"), InvalidInputException);
}

TEST_CASE("required OR serialization is all or decline", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableFilterSet filters;
	auto disjunction = make_uniq<ConjunctionOrFilter>();
	disjunction->child_filters.push_back(make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::INTEGER(1)));
	disjunction->child_filters.push_back(make_uniq<DynamicFilter>());
	filters.filters[0] = std::move(disjunction);

	REQUIRE_THROWS_AS(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker"), InvalidInputException);
}

TEST_CASE("required throwing AND remains one atomic predicate", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto reference = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0);
	auto narrowing = BoundCastExpression::AddCastToType(*con.context, std::move(reference), LogicalType::TINYINT);
	auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(narrowing),
	                                                       make_uniq<BoundConstantExpression>(Value::TINYINT(0)));
	auto conjunction = make_uniq<ConjunctionAndFilter>();
	conjunction->child_filters.push_back(make_uniq<ExpressionFilter>(std::move(comparison)));
	conjunction->child_filters.push_back(make_uniq<ConstantFilter>(ExpressionType::COMPARE_NOTEQUAL, Value::BIGINT(0)));
	TableFilterSet filters;
	filters.filters[0] = std::move(conjunction);

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
	auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(cast),
	                                                       make_uniq<BoundConstantExpression>(Value::BIGINT(42)));
	filters.filters[0] = make_uniq<OptionalFilter>(make_uniq<ExpressionFilter>(std::move(comparison)));

	auto batch = ReadBatch(VgiSerializeFilters(*con.context, {0}, &filters, {"key"}, "test-worker").filter_bytes);
	REQUIRE(batch->schema()->GetFieldIndex("type_0") >= 1);
	REQUIRE(batch->schema()->GetFieldIndex("value_0") >= 1);
	REQUIRE(batch->column(batch->schema()->GetFieldIndex("type_0"))->IsNull(0));
	auto json = FilterSpec(batch);
	REQUIRE(json.find("\"type_ref\":0") != std::string::npos);
	REQUIRE(json.find("\"value_ref\":0") != std::string::npos);
	REQUIRE(json.find("\"mode\":\"advisory\",\"source\":\"other\"") != std::string::npos);
}

TEST_CASE("filter v2 tick delta carries revisioned upsert and remove", "[filter-v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::INTEGER(99));
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
