// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_column_expression.hpp"
#include <cstdlib>
#include <fstream>
#include <iterator>

using namespace duckdb;
namespace {
vgi::VgiField Generated(const std::string &name, const vgi::VgiField &type, const std::string &sql,
                        const std::string &owner = "main") {
	auto record = vgi::BuildNativeGeneratedExpression(sql, owner, {});
	return vgi_test::Field(name, type, true,
	                       vgi_test::Metadata({vgi::VGI_COLUMN_GENERATED, vgi::VGI_COLUMN_GENERATION},
	                                          {vgi::EncodeNativeColumnDefaultMetadata(record), "virtual"}));
}
CreateTableInfo Import(ClientContext &context, const vgi::VgiSchemaPtr &schema) {
	vgi::VgiTableInfo info;
	info.name = "t";
	info.schema_name = "main";
	info.arrow_schema = schema;
	auto result = vgi::CreateTableInfoFromVgiTable(context, info, "main");
	REQUIRE(info.arrow_schema->Equals(*schema, true));
	return result;
}
void AddGenerated(ColumnList &columns, const std::string &name, const LogicalType &type, const std::string &sql) {
	ColumnDefinition column(Identifier(name), type);
	column.SetGeneratedExpression(std::move(Parser(ParserOptions::Builtin()).ParseExpressionList(sql)[0]));
	columns.AddColumn(std::move(column));
}
} // namespace

TEST_CASE("Canonical generated CREATE and discovery preserve physical column indexes", "[generated-column-contract]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	Connection con(db);
	con.BeginTransaction();
	ColumnList columns;
	columns.AddColumn(ColumnDefinition("n", LogicalType::BIGINT));
	AddGenerated(columns, "doubled", LogicalType::BIGINT, "n * 2");
	columns.AddColumn(ColumnDefinition("text", LogicalType::VARCHAR));
	AddGenerated(columns, "label", LogicalType::VARCHAR, "'item_' || CAST(n AS VARCHAR)");
	columns.AddColumn(ColumnDefinition("tail", LogicalType::BIGINT));
	AddGenerated(columns, "chain", LogicalType::BIGINT, "doubled + tail");
	auto wire = vgi::DuckDBColumnsToArrowSchema(*con.context, columns, "main");
	for (int index : {1, 3, 5}) {
		CHECK(wire->field_metadata(index).FindKey("generated_expression") < 0);
		CHECK(wire->field_metadata(index).Get(vgi::VGI_COLUMN_GENERATION).value() == "virtual");
		CHECK(vgi::ReadNativeColumnGenerated(wire->field(index), "main") != nullptr);
	}
	auto table = Import(*con.context, wire);
	CHECK(table.columns.LogicalColumnCount() == 6);
	CHECK(table.columns.PhysicalColumnCount() == 3);
	CHECK(table.columns.GetColumn(PhysicalIndex(0)).Name() == "n");
	CHECK(table.columns.GetColumn(PhysicalIndex(1)).Name() == "text");
	CHECK(table.columns.GetColumn(PhysicalIndex(2)).Name() == "tail");
	CHECK(table.columns.GetColumn(LogicalIndex(4)).Physical().index == 2);
	for (int index : {1, 3, 5})
		CHECK(table.columns.GetColumn(LogicalIndex(index)).Generated());
	auto plain = vgi::NativeColumnTypeSchema(wire, "main");
	for (int index : {1, 3, 5})
		CHECK(plain->field_metadata(index).FindKey(vgi::VGI_COLUMN_GENERATED) < 0);
}

TEST_CASE("Generated discovery rejects result mismatches cycles and unsupported grammar",
          "[generated-column-contract]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	Connection con(db);
	con.BeginTransaction();
	auto n = vgi_test::Field("n", vgi::type::Int64());
	CHECK_THROWS_WITH(Import(*con.context, vgi::MakeSchema({n, Generated("g", vgi::type::Utf8(), "n + 1")})),
	                  Catch::Contains("result type"));
	CHECK_THROWS(Import(*con.context, vgi::MakeSchema({n, Generated("g", vgi::type::Int64(), "missing + 1")})));
	CHECK_THROWS(Import(*con.context, vgi::MakeSchema({n, Generated("g", vgi::type::Int64(), "h + 1"),
	                                                   Generated("h", vgi::type::Int64(), "g + 1")})));
	CHECK_THROWS(Import(*con.context, vgi::MakeSchema({n, Generated("g", vgi::type::Int64(), "g + 1")})));
	CHECK_THROWS(Import(*con.context,
	                    vgi::MakeSchema({n, Generated("g", vgi::type::Int64(), "CAST('not an integer' AS BIGINT)")})));
	CHECK_THROWS(Import(*con.context, vgi::MakeSchema({vgi_test::Field("n", vgi::type::Float64()),
	                                                   Generated("g", vgi::type::Int64(), "CAST(n AS BIGINT)")})));
	CHECK_THROWS(
	    Import(*con.context,
	           vgi::MakeSchema(
	               {n, vgi_test::Field("nested", vgi::type::Struct({Generated("g", vgi::type::Int64(), "n * 2")}))})));
	for (const auto &sql : {"random()", "nextval('s')", "(SELECT 1)", "?", "n / 2", "n.field", "sum(n)",
	                        "row_number() OVER ()", "CAST(n AS DOUBLE)", "1.5", "foreign.main.abs(n)"}) {
		INFO(sql);
		CHECK_THROWS(vgi::BuildNativeGeneratedExpression(sql, "main", {}));
	}
	CHECK_NOTHROW(Import(*con.context, vgi::MakeSchema({Generated("g", vgi::type::Int64(), "n * 2"), n})));
	CHECK_NOTHROW(Import(*con.context, vgi::MakeSchema({vgi_test::Field("a.b", vgi::type::Int64()),
	                                                    Generated("g", vgi::type::Int64(), "\"a.b\" + 1")})));
}

TEST_CASE("Generated bindings retain builtin identity and reject changed session context",
          "[generated-column-contract]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	Connection con(db);
	con.BeginTransaction();
	auto schema = vgi::MakeSchema(
	    {vgi_test::Field("text", vgi::type::Utf8()), Generated("label", vgi::type::Utf8(), "upper(text)")});
	auto table = Import(*con.context, schema);
	auto &cast = table.columns.GetColumn(LogicalIndex(1)).GeneratedExpression().Cast<CastExpression>();
	auto &function = cast.Child().Cast<FunctionExpression>();
	CHECK(function.GetQualifiedName().Catalog() == "system");
	CHECK(function.GetQualifiedName().Schema() == "main");
	REQUIRE_FALSE(con.Query("SET default_order='desc'")->HasError());
	CHECK_THROWS_WITH(Import(*con.context, schema), Catch::Contains("evaluation context"));
	REQUIRE_FALSE(con.Query("SET default_order='asc'")->HasError());
	CHECK_NOTHROW(Import(*con.context, schema));
	auto record = Generated("g", vgi::type::Int64(), "n * 2");
	auto metadata = record.metadata;
	metadata.Append("vgi.column.generated.v99", "opaque");
	CHECK_THROWS(vgi::ReadNativeColumnGenerated(vgi::MakeSchema({record.WithMetadata(metadata)})->field(0), "main"));
	CHECK_THROWS(vgi::ReadNativeColumnGenerated(vgi::MakeSchema({record})->field(0), "other"));
}

TEST_CASE("Native generated metadata matches the shared schema corpus", "[generated-column-corpus]") {
	auto directory = std::getenv("VGI_GENERATED_SCHEMA_VECTORS");
	REQUIRE(directory);
	std::ifstream manifest(std::string(directory) + "/vectors.txt");
	REQUIRE(manifest.good());
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	Connection con(db);
	con.BeginTransaction();
	std::string expected, filename, name;
	size_t count = 0;
	while (manifest >> expected >> filename >> name) {
		INFO(name);
		std::ifstream input(std::string(directory) + "/" + filename, std::ios::binary);
		REQUIRE(input.good());
		std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
		auto schema = vgi::DeserializeSchema(bytes);
		if (expected == "valid")
			CHECK_NOTHROW(Import(*con.context, schema));
		else {
			REQUIRE(expected == "invalid");
			CHECK_THROWS(Import(*con.context, schema));
		}
		count++;
	}
	CHECK(count >= 23);
}
