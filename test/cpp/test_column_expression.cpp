#include "duckdb/parser/expression/cast_expression.hpp"
// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_rpc_types.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

using namespace duckdb;

namespace {
std::vector<uint8_t> ColumnExpressionBytes(const std::string &sql = "7", const std::string &owner = "main",
                                           bool using_expression = false) {
	return vgi::BuildNativeColumnExpression(sql, owner, {}, using_expression);
}
vgi::VgiField Field(const std::vector<uint8_t> &bytes) {
	return vgi_test::Field(
	    "a.b", vgi::type::Int32(), true,
	    vgi_test::Metadata({vgi::VGI_COLUMN_DEFAULT}, {vgi::EncodeNativeColumnDefaultMetadata(bytes)}));
}
std::vector<uint8_t> Replace(const std::vector<uint8_t> &bytes, const std::string &name, ArrowArray value) {
	auto record = vgi::DeserializeVgiQueryRecord(bytes);
	return vgi::SerializeToIpcBytes(vgi_test::ReplaceColumn(record, record->schema()->GetFieldIndex(name), value));
}
unique_ptr<ParsedExpression> ReadDefault(const vgi::VgiField &field, const std::string &owner) {
	return vgi::ReadNativeColumnDefault(vgi::MakeSchema({field})->field(0), owner);
}
} // namespace

TEST_CASE("Canonical column expressions preserve NULL and raw owner paths", "[column-expression]") {
	auto bytes = ColumnExpressionBytes("NULL", "owner.with.dot");
	auto batch = vgi::DeserializeVgiQueryRecord(bytes);
	vgi::RecordBatchSingleRow row(batch, 0, "SqlExpression", "test");
	CHECK(row["sql"].value_not_null<std::string>() == "NULL");
	CHECK(row["schema_path"].value_not_null<std::vector<std::string>>() == std::vector<std::string> {"owner.with.dot"});
	CHECK(ReadDefault(Field(bytes), "owner.with.dot")->ToString() == "NULL");
	CHECK_NOTHROW(ReadDefault(Field(bytes), "OWNER.WITH.DOT"));
	CHECK_THROWS(ReadDefault(Field(bytes), "owner"));
	CHECK_THROWS(ColumnExpressionBytes("a", "main"));
	CHECK_NOTHROW(ColumnExpressionBytes("\"a.b\" + 1", "main", true));
	CHECK_NOTHROW(ColumnExpressionBytes("NULL", "main", true));
	CHECK_NOTHROW(ColumnExpressionBytes("temperature / 10.0", "main", true));
	CHECK_THROWS(ColumnExpressionBytes("temperature / 10.0", "main"));
	CHECK_THROWS(ColumnExpressionBytes("a.b + 1", "main", true));
}

TEST_CASE("Canonical default records reject malformed envelope context and owner", "[column-expression]") {
	auto bytes = ColumnExpressionBytes();
	for (const auto &name : {"sql", "profile"}) {
		auto invalid = Replace(bytes, name, vgi::BuildStringScalar(""));
		CHECK_THROWS(ReadDefault(Field(invalid), "main"));
	}
	auto profile = Replace(bytes, "profile", vgi::BuildStringScalar("duckdb.macro.default.v1"));
	CHECK_THROWS(ReadDefault(Field(profile), "main"));
	auto owner = Replace(bytes, "schema_path", vgi::BuildStringListScalar({"main", "nested"}));
	CHECK_THROWS(ReadDefault(Field(owner), "main"));
	auto context = vgi_test::Struct(vgi::generated::QueryEvaluationContextSchema(),
	                                {vgi::BuildStringScalar("America/New_York"), vgi::BuildStringScalar("binary"),
	                                 vgi::BuildStringScalar("asc"), vgi::BuildStringScalar("nulls_last"),
	                                 vgi::BuildBoolScalar(true)});
	CHECK_THROWS(ReadDefault(Field(Replace(bytes, "evaluation_context", context)), "main"));
	bytes.push_back(0);
	CHECK_THROWS(ReadDefault(Field(bytes), "main"));
	CHECK_THROWS(ReadDefault(Field({0, 1, 2}), "main"));
	auto malformed =
	    vgi_test::Field("x", vgi::type::Int32(), true, vgi_test::Metadata({vgi::VGI_COLUMN_DEFAULT}, {"%%%"}));
	CHECK_THROWS(ReadDefault(malformed, "main"));
}

TEST_CASE("Bounded native scalar grammar rejects effects and caller binding", "[column-expression]") {
	for (const auto &sql :
	     {"random()", "nextval('s')", "(SELECT 1)", "?", "1); SELECT 2; --", "1, 2", "TRY_CAST('x' AS INTEGER)",
	      "CAST('2026-01-01' AS TIMESTAMP)", "other.abs(-1)", "abs(value := -1)"}) {
		INFO(sql);
		CHECK_THROWS(ColumnExpressionBytes(sql));
	}
	for (const auto &sql : {"7", "NULL", "'plain'", "CAST(NULL AS INTEGER)", "abs(-7)", "upper('literal')", "1 + 2"}) {
		INFO(sql);
		CHECK_NOTHROW(ReadDefault(Field(ColumnExpressionBytes(sql)), "main"));
	}
	auto parsed = ReadDefault(Field(ColumnExpressionBytes("upper('literal')")), "main");
	CHECK(parsed->Cast<FunctionExpression>().GetQualifiedName().Catalog() == "system");
	CHECK(parsed->Cast<FunctionExpression>().GetQualifiedName().Schema() == "main");
	auto cast = ReadDefault(Field(ColumnExpressionBytes("CAST(7 AS BIGINT)")), "main");
	CHECK(cast->Cast<CastExpression>().TargetType().GetCatalog() == "system");
	CHECK(cast->Cast<CastExpression>().TargetType().GetSchema() == "main");
	vgi::VgiQueryEvaluationContext context;
	context.default_order = "desc";
	CHECK_THROWS(vgi::BuildNativeColumnExpression("7", "main", context, false));
	context = {};
	context.preserve_insertion_order = false;
	CHECK_THROWS(vgi::BuildNativeColumnExpression("7", "main", context, false));
}

TEST_CASE("Table type conversion retains authoritative defaults and other metadata", "[column-expression]") {
	auto field = Field(ColumnExpressionBytes());
	auto metadata = field.metadata;
	metadata.Append("comment", "retained");
	field = field.WithMetadata(metadata);
	auto schema = vgi::MakeSchema({field});
	auto types = vgi::NativeColumnTypeSchema(schema, "main");
	CHECK(schema->field_metadata(0).FindKey(vgi::VGI_COLUMN_DEFAULT) >= 0);
	CHECK(types->field_metadata(0).FindKey(vgi::VGI_COLUMN_DEFAULT) < 0);
	CHECK(types->field_metadata(0).Get("comment").value() == "retained");
	ArrowSchemaWrapper exported;
	CHECK_NOTHROW(vgi::ExportSchema(types, exported));
	ArrowSchemaWrapper unqualified;
	CHECK_THROWS(vgi::ExportSchema(schema, unqualified));
	for (const auto &conflict : {"default", "generated_expression", "vgi.column.generated.v1",
	                             "vgi.column.generation.v1", "vgi.column.default.v1"}) {
		auto conflicting = metadata;
		conflicting.Append(conflict, "7");
		CHECK_THROWS(ReadDefault(field.WithMetadata(conflicting), "main"));
	}
	auto nested = vgi::MakeSchema({vgi_test::Field("nested", vgi::type::Struct({field}))});
	ArrowSchemaWrapper nested_exported;
	CHECK_THROWS(vgi::ExportSchema(vgi::NativeColumnTypeSchema(nested, "main"), nested_exported));
}
