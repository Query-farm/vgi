// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "vgi_extension.hpp"
#include "vgi_macro_contract.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_rpc_types.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"
#include <cstdlib>

using namespace duckdb;
namespace {
using Array = ArrowArray;
Array Null() {
	return vgi::BuildOptionalBinaryScalar(std::nullopt);
}
vgi::VgiBatchPtr Parameter(Array literal = Null(), Array declared = Null(), Array expression = Null(),
                           std::string name = "x") {
	return vgi::VgiBatch::FromColumns(
	    vgi::generated::MacroParameterSchema(), 1,
	    {vgi::BuildStringScalar(name), declared, literal, expression, vgi::BuildStringScalar("")});
}
vgi::VgiBatchPtr Overload(const vgi::VgiBatchPtr &parameters, std::string profile = "duckdb.macro.v1",
                          std::string owner = "main", std::string zone = "UTC") {
	auto context =
	    vgi_test::Struct(vgi::generated::QueryEvaluationContextSchema(),
	                     {vgi::BuildStringScalar(zone), vgi::BuildStringScalar("binary"), vgi::BuildStringScalar("asc"),
	                      vgi::BuildStringScalar("nulls_last"), vgi::BuildBoolScalar(true)});
	return vgi::VgiBatch::FromColumns(vgi::generated::MacroOverloadSchema(), 1,
	                                  {vgi_test::ListRows(parameters), vgi::BuildStringScalar("x"),
	                                   vgi::BuildStringScalar(profile), vgi::BuildStringListScalar({owner}),
	                                   vgi::BuildStringScalar("scalar"), context});
}
vgi::VgiBatchPtr Family(const vgi::VgiBatchPtr &overloads) {
	return vgi::VgiBatch::FromColumns(vgi::generated::MacroInfoSchema(), 1,
	                                  {vgi::BuildOptionalStringScalar(std::nullopt), vgi::BuildStringMapScalar({}),
	                                   vgi::BuildStringScalar("m"), vgi::BuildStringListScalar({"main"}),
	                                   vgi::BuildEnumArray("SCALAR", {"SCALAR", "TABLE"}),
	                                   vgi_test::ListRows(overloads)});
}
unique_ptr<QueryResult> Query(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
	return result;
}
} // namespace

TEST_CASE("Macro query qualification preserves CTE scopes and callable ownership", "[macro-contract]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	Connection con(db);
	Query(con, "ATTACH ':memory:' AS owner");
	Query(con, "CREATE SCHEMA owner.s");
	Query(con, "CREATE TABLE owner.s.base AS SELECT 7 AS value");
	Query(con, "CREATE TEMP TABLE base AS SELECT 99 AS value");
	Query(con, "CREATE MACRO owner.s.sequence(n) AS TABLE SELECT * FROM range(n)");
	Query(con, "CREATE MACRO sequence(n) AS TABLE SELECT 100 AS range");
	Query(con, "CREATE MACRO abs(n) AS 100");
	auto qualified = [&](const string &sql) {
		Parser parser {ParserOptions::Builtin()};
		parser.ParseQuery(sql);
		auto &select = parser.statements[0]->Cast<SelectStatement>();
		vgi::QualifyNativeMacroQuery(*select.node, {"sequence"}, "owner", "s");
		return Query(con, select.ToString());
	};
	auto result = qualified("WITH sequence AS (SELECT * FROM range(3)) "
	                        "SELECT * FROM sequence UNION ALL SELECT * FROM sequence(0)");
	CHECK(result->RowCount() == 3);
	CHECK(result->Collection().GetValue(0, 2).GetValue<int64_t>() == 2);
	result = qualified("WITH base AS (SELECT * FROM base) SELECT * FROM base");
	CHECK(result->Collection().GetValue(0, 0).GetValue<int32_t>() == 7);
	result = qualified("WITH q AS (SELECT 7 AS value) "
	                   "SELECT (SELECT value FROM (WITH q AS (SELECT 9 AS value) SELECT * FROM q)), "
	                   "(SELECT value FROM q), abs(-2)");
	CHECK(result->Collection().GetValue(0, 0).GetValue<int32_t>() == 9);
	CHECK(result->Collection().GetValue(1, 0).GetValue<int32_t>() == 7);
	CHECK(result->Collection().GetValue(2, 0).GetValue<int32_t>() == 2);
	{
		Parser parser {ParserOptions::Builtin()};
		parser.ParseQuery("SELECT * FROM base AT (VERSION => sequence(1))");
		auto &select = parser.statements[0]->Cast<SelectStatement>();
		vgi::QualifyNativeMacroQuery(*select.node, {"sequence"}, "owner", "s");
		auto &table = select.node->Cast<SelectNode>().from_table->Cast<BaseTableRef>();
		auto &call = table.at_clause->ExpressionMutable()->Cast<FunctionExpression>();
		CHECK(call.GetQualifiedName().Catalog() == "owner");
		CHECK(call.GetQualifiedName().Schema() == "s");
		CHECK(call.FunctionName() == "sequence");
	}
	for (const auto &sql :
	     {"SELECT * FROM main.main.base", "SELECT main.main.abs(-1)",
	      "SELECT * FROM base AT (VERSION => main.main.abs(1))", "SELECT * FROM base AT (VERSION => $1)",
	      "WITH RECURSIVE q AS (SELECT 1 UNION ALL SELECT * FROM q) SELECT * FROM q"}) {
		Parser parser {ParserOptions::Builtin()};
		parser.ParseQuery(sql);
		auto &select = parser.statements[0]->Cast<SelectStatement>();
		CHECK_THROWS(vgi::QualifyNativeMacroQuery(*select.node, {}, "owner", "s"));
	}
}

TEST_CASE("Canonical macro records reject malformed owners profiles and literal metadata", "[macro-contract]") {
	auto valid = Family(Overload(Parameter()));
	auto parsed = vgi::ParseMacroInfo(valid, "test");
	REQUIRE(parsed.overloads.size() == 1);
	CHECK(parsed.overloads[0].parameters[0].documentation == std::optional<std::string>(""));
	for (auto invalid : {Overload(Parameter(), "unknown"), Overload(Parameter(), "duckdb.macro.v1", "main.nested"),
	                     Overload(Parameter(), "duckdb.macro.v1", "main", "Europe/Paris"),
	                     Overload(Parameter(Null(), Null(), vgi::BuildBinaryScalarRequired({1})))}) {
		CHECK_THROWS(vgi::ParseMacroInfo(Family(invalid), "test"));
	}
	auto repeated = vgi::ConcatBatches({Parameter(), Parameter(Null(), Null(), Null(), "X")});
	CHECK_THROWS(vgi::ParseMacroInfo(Family(Overload(repeated)), "test"));
	auto empty = vgi::VgiBatch::MakeEmpty(vgi::generated::MacroOverloadSchema());
	CHECK_THROWS(vgi::ParseMacroInfo(Family(empty), "test"));
	for (bool annotated : {false, true}) {
		auto field = vgi_test::Field("x", vgi::type::Binary());
		if (annotated)
			field = field.WithMetadata(vgi_test::Metadata({"vgi.logical_type.v1"}, {"variant"}));
		auto batch =
		    vgi::VgiBatch::FromColumns(vgi::MakeSchema({field}), 1, {vgi::BuildBinaryScalarRequired({0, 255})});
		auto family = Family(Overload(Parameter(vgi::BuildBinaryScalarRequired(vgi::SerializeToIpcBytes(batch)))));
		if (annotated)
			CHECK_THROWS(vgi::ParseMacroInfo(family, "test"));
		else
			CHECK_NOTHROW(vgi::ParseMacroInfo(family, "test"));
	}
}

TEST_CASE("Macro listing envelopes reject trailing data and absent family records", "[macro-contract]") {
	auto family = Family(Overload(Parameter()));
	auto valid = vgi::SerializeToIpcBytes(family);
	auto trailing = valid;
	trailing.push_back(0);
	auto zero_rows = vgi::SerializeToIpcBytes(family->Slice(0, 0));
	auto check = [&](Array binary, bool accepted) {
		auto row = vgi::VgiBatch::FromColumns(vgi::MakeSchema({vgi::type::Binary()}), 1, {binary});
		auto wrapper = vgi::VgiBatch::FromColumns(
		    vgi::MakeSchema({vgi::type::List(vgi::type::Binary()).Named("items")}), 1, {vgi_test::ListColumn(row, 0)});
		if (accepted) {
			CHECK(vgi::UnwrapAndValidateItems(wrapper, "catalog_schema_contents_macros", "test").size() == 1);
		} else {
			CHECK_THROWS(vgi::UnwrapAndValidateItems(wrapper, "catalog_schema_contents_macros", "test"));
		}
	};
	check(vgi::BuildBinaryScalarRequired(valid), true);
	check(vgi::BuildBinaryScalarRequired(trailing), false);
	check(vgi::BuildBinaryScalarRequired(zero_rows), false);
	check(Null(), false);
	// The size limit is checked before attempting to copy/decode the buffer.
	CHECK_THROWS(vgi::DecodeMacroInfoRecord(nullptr, 8 * 1024 * 1024 + 1));
	auto sink = std::make_shared<vgi::VgiBufferOutputStream>();
	auto writer = vgi::VgiIpcWriter::Open(sink, family->schema());
	writer->Write(*family);
	writer->Write(*family);
	writer->Close();
	auto twice = sink->Finish();
	check(vgi::BuildBinaryScalarRequired(std::vector<uint8_t>(twice->data(), twice->data() + twice->size())), false);
}

TEST_CASE("Macro collections retain every batch and reject a malformed later family", "[macro-contract]") {
	auto family = Family(Overload(Parameter()));
	auto second =
	    vgi_test::ReplaceColumn(family, family->schema()->GetFieldIndex("name"), vgi::BuildStringScalar("m2"));
	auto stream = [&](const std::vector<vgi::VgiBatchPtr> &batches) {
		auto sink = std::make_shared<vgi::VgiBufferOutputStream>();
		auto writer = vgi::VgiIpcWriter::Open(sink, family->schema());
		for (const auto &batch : batches)
			writer->Write(*batch);
		writer->Close();
		auto buffer = sink->Finish();
		return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
	};
	auto decode = [&](const std::vector<uint8_t> &bytes) {
		vgi::generated::SchemaInfo schema;
		schema.path = {"main"};
		vgi::generated::CatalogContentsResponse response;
		response.schemas = vgi::SerializeRecord(schema);
		response.macros = bytes;
		auto contents = vgi::ParseCatalogContents(vgi::generated::ToBatch(response), "test");
		REQUIRE(contents.schemas.size() == 1);
		return vgi::DecodeContentsMacros(contents.schemas[0], "SCALAR_MACRO");
	};
	auto decoded = decode(stream({family, second}));
	REQUIRE(decoded.size() == 2);
	CHECK(decoded[0].name == "m");
	CHECK(decoded[1].name == "m2");
	CHECK(decode(stream({family->Slice(0, 0)})).empty());
	auto trailing = stream({family});
	trailing.push_back(0);
	CHECK_THROWS(decode(trailing));
	CHECK_THROWS(decode(stream({family, Family(Overload(Parameter(), "unknown"))})));
}

TEST_CASE("Native macro literal import preserves typed NULL and complex values", "[macro-contract]") {
	DuckDB db(nullptr);
	Connection con(db);
	vgi::VgiField decimal;
	decimal.format = "d:10,2";
	for (auto type : {vgi::type::Int32(), vgi::type::Date32(), decimal, vgi::type::Binary(),
	                  vgi::type::Struct({vgi_test::Field("key", vgi::type::Int64())})}) {
		auto schema = vgi::MakeSchema({vgi_test::Field("x", type)});
		auto batch = vgi::VgiBatch::FromColumns(schema, 1, {vgi_test::NullArray(type)});
		auto family =
		    Family(Overload(Parameter(vgi::BuildBinaryScalarRequired(vgi::SerializeToIpcBytes(batch)),
		                              vgi::BuildBinaryScalarRequired(vgi::SerializeSchemaToIpcBytes(schema)))));
		auto parameter = vgi::ParseMacroInfo(family, "test").overloads[0].parameters[0];
		auto value = vgi::NativeMacroDefaultValue(*con.context, parameter);
		CHECK(value.IsNull());
		CHECK(value.type() == vgi::NativeMacroParameterType(*con.context, parameter));
	}
	auto schema = vgi::MakeSchema({vgi_test::Field("x", vgi::type::Binary())});
	auto batch = vgi::VgiBatch::FromColumns(schema, 1, {vgi::BuildBinaryScalarRequired({0, 255, 65})});
	auto parameter =
	    vgi::ParseMacroInfo(
	        Family(Overload(Parameter(vgi::BuildBinaryScalarRequired(vgi::SerializeToIpcBytes(batch))))), "test")
	        .overloads[0]
	        .parameters[0];
	CHECK(vgi::NativeMacroParameterType(*con.context, parameter) == LogicalType::UNKNOWN);
	CHECK(vgi::NativeMacroDefaultValue(*con.context, parameter) == Value::BLOB_RAW(std::string("\0\xff"
	                                                                                           "A",
	                                                                                           3)));
}

TEST_CASE("Native literal and cast reconstruction ignore caller type and macro shadows", "[macro-contract]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	Connection con(db);
	Query(con, "CREATE SCHEMA shadow");
	Query(con, "CREATE TYPE shadow.\"bigint\" AS ENUM ('shadow')");
	Query(con, "SET schema='shadow'");
	Query(con, "CREATE MACRO list_value(a, b) AS 99");
	CHECK(Query(con, "SELECT list_value(1, 2)")->Collection().GetValue(0, 0) == Value(99));
	for (const auto &value : {Value::BIGINT(7), Value::LIST(LogicalType::INTEGER, {Value(1), Value(2)})}) {
		auto expression = vgi::NativeLiteralExpression(value);
		auto result = Query(con, "SELECT " + expression->ToString());
		CHECK(result->Collection().GetValue(0, 0) == value);
		CHECK(result->Collection().GetValue(0, 0).type() == value.type());
	}
	Parser parser {ParserOptions::Builtin()};
	auto cast = parser.ParseExpressionList("CAST(7 AS BIGINT)");
	vgi::QualifyNativeMacroExpression(*cast[0], {}, "memory", "main");
	CHECK(Query(con, "SELECT " + cast[0]->ToString())->Collection().GetValue(0, 0) == Value::BIGINT(7));
}

TEST_CASE("Native macro families execute with stable owner context and atomic loading", "[macro-family-native]") {
	auto worker = std::getenv("VGI_MACRO_FAMILY_WORKER");
	REQUIRE(worker);
	auto bad_worker = std::getenv("VGI_MACRO_BAD_FAMILY_WORKER");
	REQUIRE(bad_worker);
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	Query(con, "ATTACH 'options' AS m (TYPE vgi, LOCATION '" + StringUtil::Replace(worker, "'", "''") + "')");
	CHECK(Query(con, "SELECT m.main.family(7), m.main.family('seven')")->Collection().GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT m.main.family('seven')")->Collection().GetValue(0, 0) == Value("SEVEN"));
	CHECK(Query(con, "SELECT m.main.family(x := 7)")->Collection().GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT m.main.defaults(), typeof(m.main.typed_null()), m.main.typed_null() IS NULL")
	          ->Collection()
	          .GetValue(0, 0) == Value::BIGINT(43));
	CHECK(Query(con, "SELECT m.main.defaults(x := 7)")->Collection().GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT typeof(m.main.typed_null())")->Collection().GetValue(0, 0) == Value("INTEGER"));
	CHECK(Query(con, "SELECT value FROM m.main.table_family(7)")->Collection().GetValue(0, 0) == Value(7));
	CHECK(Query(con, "SELECT value FROM m.main.table_family('seven')")->Collection().GetValue(0, 0) == Value("seven"));
	Query(con, "CREATE TEMP MACRO upper(x) AS 'shadow'");
	CHECK(Query(con, "SELECT m.main.family('seven')")->Collection().GetValue(0, 0) == Value("SEVEN"));
	auto docs = Query(con, "SELECT overload_index,arg_type,arg_description FROM vgi_function_arguments() WHERE "
	                       "catalog_name='m' AND function_name='family' ORDER BY overload_index");
	REQUIRE(docs->RowCount() == 2);
	CHECK(docs->Collection().GetValue(0, 0) == Value::BIGINT(0));
	CHECK(docs->Collection().GetValue(1, 0) == Value("INTEGER"));
	CHECK(docs->Collection().GetValue(2, 0) == Value(""));
	CHECK(docs->Collection().GetValue(2, 1).IsNull());
	Query(con, "BEGIN");
	CHECK(Query(con, "SELECT m.main.family(7)")->Collection().GetValue(0, 0) == Value(8));
	Query(con, "SET default_order='desc'");
	auto changed = con.Query("SELECT m.main.family(7)");
	REQUIRE(changed->HasError());
	CHECK((changed->GetError().find("context") != std::string::npos ||
	       changed->GetError().find("settings") != std::string::npos));
	Query(con, "ROLLBACK");
	Query(con, "SET default_order='asc'");
	CHECK(Query(con, "SELECT m.main.family(7)")->Collection().GetValue(0, 0) == Value(8));
	Query(con, "ATTACH 'options' AS bad (TYPE vgi, LOCATION '" + StringUtil::Replace(bad_worker, "'", "''") + "')");
	for (int attempt = 0; attempt < 2; ++attempt) {
		auto failed = con.Query("SELECT bad.main.family(7)");
		REQUIRE(failed->HasError());
		// A valid first overload must never become callable after the invalid later member failed.
		CHECK((failed->GetError().find("syntax") != std::string::npos ||
		       failed->GetError().find("Parser") != std::string::npos));
	}
	for (const auto &mode : {std::string("WINDOW"), std::string("FOREIGN")}) {
		auto command = std::getenv(("VGI_MACRO_" + mode + "_FAMILY_WORKER").c_str());
		REQUIRE(command);
		auto alias = "bad_" + mode;
		Query(con, "ATTACH 'options' AS " + alias + " (TYPE vgi, LOCATION '" + StringUtil::Replace(command, "'", "''") +
		               "')");
		for (int attempt = 0; attempt < 2; ++attempt) {
			auto failed = con.Query("SELECT " + alias + ".main.family(7)");
			REQUIRE(failed->HasError());
			CHECK(failed->GetError().find(mode == "WINDOW" ? "window expressions" : "client catalog") !=
			      std::string::npos);
		}
	}
	// Missing-function suggestions enumerate attached catalogs. Keep the
	// deliberately unparsable providers out of this separate binding probe.
	Query(con, "DETACH bad");
	Query(con, "DETACH bad_WINDOW");
	Query(con, "DETACH bad_FOREIGN");
	auto unknown = std::getenv("VGI_MACRO_UNKNOWN_FAMILY_WORKER");
	REQUIRE(unknown);
	Query(con, "ATTACH 'options' AS late_bind (TYPE vgi, LOCATION '" + StringUtil::Replace(unknown, "'", "''") + "')");
	// Syntax/type/context validation installs the whole family. Function body
	// binding remains invocation-time; a bad selected overload is never skipped.
	CHECK(Query(con, "SELECT late_bind.main.family(7)")->Collection().GetValue(0, 0) == Value(8));
	auto missing_function = con.Query("SELECT late_bind.main.family('text')");
	REQUIRE(missing_function->HasError());
	INFO(missing_function->GetError());
	CHECK(missing_function->GetError().find("missing_function") != std::string::npos);
	CHECK(Query(con, "SELECT late_bind.main.family(7)")->Collection().GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT 11")->Collection().GetValue(0, 0) == Value(11));
}
