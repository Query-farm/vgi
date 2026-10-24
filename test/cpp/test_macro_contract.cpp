// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "vgi_extension.hpp"
#include "vgi_macro_contract.hpp"
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
using Array = std::shared_ptr<arrow::Array>;
Array Null(arrow::Type::type type = arrow::Type::BINARY) {
	return arrow::MakeArrayOfNull(type == arrow::Type::BINARY ? arrow::binary() : arrow::utf8(), 1).ValueOrDie();
}
Array Struct(const std::shared_ptr<arrow::Schema> &schema, std::vector<Array> columns) {
	return arrow::StructArray::Make(columns, schema->fields()).ValueOrDie();
}
Array List(Array values) {
	return arrow::MakeArrayFromScalar(arrow::ListScalar(values), 1).ValueOrDie();
}
Array Parameter(Array literal = Null(), Array declared = Null(), Array expression = Null(), std::string name = "x") {
	return Struct(vgi::generated::MacroParameterSchema(),
	              {vgi::BuildStringScalar(name), declared, literal, expression, vgi::BuildStringScalar("")});
}
Array Overload(Array parameters, std::string profile = "duckdb.macro.v1", std::string owner = "main",
               std::string zone = "UTC") {
	auto context =
	    Struct(vgi::generated::QueryEvaluationContextSchema(),
	           {vgi::BuildStringScalar(zone), vgi::BuildStringScalar("binary"), vgi::BuildStringScalar("asc"),
	            vgi::BuildStringScalar("nulls_last"), vgi::BuildBoolScalar(true)});
	return Struct(vgi::generated::MacroOverloadSchema(),
	              {List(parameters), vgi::BuildStringScalar("x"), vgi::BuildStringScalar(profile),
	               vgi::BuildStringListScalar({owner}), vgi::BuildStringScalar("scalar"), context});
}
std::shared_ptr<arrow::RecordBatch> Family(Array overloads) {
	auto schema = vgi::generated::MacroInfoSchema();
	return arrow::RecordBatch::Make(schema, 1,
	                                {Null(arrow::Type::STRING), vgi::BuildStringMapScalar({}),
	                                 vgi::BuildStringScalar("m"), vgi::BuildStringListScalar({"main"}),
	                                 vgi::BuildEnumArray("SCALAR", {"SCALAR", "TABLE"}), List(overloads)});
}
unique_ptr<MaterializedQueryResult> Query(Connection &con, const std::string &sql) {
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
		Parser parser;
		parser.ParseQuery(sql);
		auto &select = parser.statements[0]->Cast<SelectStatement>();
		vgi::QualifyNativeMacroQuery(*select.node, {"sequence"}, "owner", "s");
		return Query(con, select.ToString());
	};
	auto result = qualified("WITH sequence AS (SELECT * FROM range(3)) "
	                        "SELECT * FROM sequence UNION ALL SELECT * FROM sequence(0)");
	CHECK(result->RowCount() == 3);
	CHECK(result->GetValue(0, 2).GetValue<int64_t>() == 2);
	result = qualified("WITH base AS (SELECT * FROM base) SELECT * FROM base");
	CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 7);
	result = qualified("WITH q AS (SELECT 7 AS value) "
	                   "SELECT (SELECT value FROM (WITH q AS (SELECT 9 AS value) SELECT * FROM q)), "
	                   "(SELECT value FROM q), abs(-2)");
	CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 9);
	CHECK(result->GetValue(1, 0).GetValue<int32_t>() == 7);
	CHECK(result->GetValue(2, 0).GetValue<int32_t>() == 2);
	{
		Parser parser;
		parser.ParseQuery("SELECT * FROM base AT (VERSION => sequence(1))");
		auto &select = parser.statements[0]->Cast<SelectStatement>();
		vgi::QualifyNativeMacroQuery(*select.node, {"sequence"}, "owner", "s");
		auto &table = select.node->Cast<SelectNode>().from_table->Cast<BaseTableRef>();
		auto &call = table.at_clause->ExpressionMutable()->Cast<FunctionExpression>();
		CHECK(call.catalog == "owner");
		CHECK(call.schema == "s");
		CHECK(call.function_name == "sequence");
	}
	for (const auto &sql : {"SELECT * FROM main.main.base", "SELECT main.main.abs(-1)",
	                       "SELECT * FROM base AT (VERSION => main.main.abs(1))",
	                       "SELECT * FROM base AT (VERSION => $1)",
	                       "WITH RECURSIVE q AS (SELECT 1 UNION ALL SELECT * FROM q) SELECT * FROM q"}) {
		Parser parser;
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
	auto repeated = arrow::Concatenate({Parameter(), Parameter(Null(), Null(), Null(), "X")}).ValueOrDie();
	CHECK_THROWS(vgi::ParseMacroInfo(Family(Overload(repeated)), "test"));
	auto empty =
	    arrow::MakeArrayOfNull(arrow::struct_(vgi::generated::MacroOverloadSchema()->fields()), 0).ValueOrDie();
	CHECK_THROWS(vgi::ParseMacroInfo(Family(empty), "test"));
	for (bool annotated : {false, true}) {
		auto field = arrow::field("x", arrow::binary());
		if (annotated)
			field = field->WithMetadata(arrow::key_value_metadata({"vgi.logical_type.v1"}, {"variant"}));
		auto batch = arrow::RecordBatch::Make(arrow::schema({field}), 1, {vgi::BuildBinaryScalarRequired({0, 255})});
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
		auto list = List(binary);
		auto wrapper = arrow::RecordBatch::Make(arrow::schema({arrow::field("items", list->type())}), 1, {list});
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
	auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
	auto writer = arrow::ipc::MakeStreamWriter(sink, family->schema()).ValueOrDie();
	vgi::ThrowOnArrowError(writer->WriteRecordBatch(*family));
	vgi::ThrowOnArrowError(writer->WriteRecordBatch(*family));
	vgi::ThrowOnArrowError(writer->Close());
	auto twice = sink->Finish().ValueOrDie();
	check(vgi::BuildBinaryScalarRequired(std::vector<uint8_t>(twice->data(), twice->data() + twice->size())), false);
}

TEST_CASE("Macro collections retain every batch and reject a malformed later family", "[macro-contract]") {
	auto family = Family(Overload(Parameter()));
	auto second = family->SetColumn(family->schema()->GetFieldIndex("name"), family->schema()->GetFieldByName("name"),
	                                vgi::BuildStringScalar("m2"))
	                  .ValueOrDie();
	auto stream = [&](const std::vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
		auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
		auto writer = arrow::ipc::MakeStreamWriter(sink, family->schema()).ValueOrDie();
		for (const auto &batch : batches) vgi::ThrowOnArrowError(writer->WriteRecordBatch(*batch));
		vgi::ThrowOnArrowError(writer->Close());
		auto buffer = sink->Finish().ValueOrDie();
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
	for (auto type : {arrow::int32(), arrow::date32(), arrow::decimal128(10, 2), arrow::binary(),
	                  arrow::struct_({arrow::field("key", arrow::int64())})}) {
		auto schema = arrow::schema({arrow::field("x", type)});
		auto batch = arrow::RecordBatch::Make(schema, 1, {arrow::MakeArrayOfNull(type, 1).ValueOrDie()});
		auto family =
		    Family(Overload(Parameter(vgi::BuildBinaryScalarRequired(vgi::SerializeToIpcBytes(batch)),
		                              vgi::BuildBinaryScalarRequired(vgi::SerializeSchemaToIpcBytes(schema)))));
		auto parameter = vgi::ParseMacroInfo(family, "test").overloads[0].parameters[0];
		auto value = vgi::NativeMacroDefaultValue(*con.context, parameter);
		CHECK(value.IsNull());
		CHECK(value.type() == vgi::NativeMacroParameterType(*con.context, parameter));
	}
	auto schema = arrow::schema({arrow::field("x", arrow::binary())});
	auto batch = arrow::RecordBatch::Make(schema, 1, {vgi::BuildBinaryScalarRequired({0, 255, 65})});
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
	CHECK(Query(con, "SELECT m.main.family(7), m.main.family('seven')")->GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT m.main.family('seven')")->GetValue(0, 0) == Value("SEVEN"));
	CHECK(Query(con, "SELECT m.main.family(x := 7)")->GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT m.main.defaults(), typeof(m.main.typed_null()), m.main.typed_null() IS NULL")
	          ->GetValue(0, 0) == Value::BIGINT(43));
	CHECK(Query(con, "SELECT m.main.defaults(x := 7)")->GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT typeof(m.main.typed_null())")->GetValue(0, 0) == Value("INTEGER"));
	CHECK(Query(con, "SELECT value FROM m.main.table_family(7)")->GetValue(0, 0) == Value(7));
	CHECK(Query(con, "SELECT value FROM m.main.table_family('seven')")->GetValue(0, 0) == Value("seven"));
	Query(con, "CREATE TEMP MACRO upper(x) AS 'shadow'");
	CHECK(Query(con, "SELECT m.main.family('seven')")->GetValue(0, 0) == Value("SEVEN"));
	auto docs = Query(con, "SELECT overload_index,arg_type,arg_description FROM vgi_function_arguments() WHERE "
	                       "catalog_name='m' AND function_name='family' ORDER BY overload_index");
	REQUIRE(docs->RowCount() == 2);
	CHECK(docs->GetValue(0, 0) == Value::BIGINT(0));
	CHECK(docs->GetValue(1, 0) == Value("INTEGER"));
	CHECK(docs->GetValue(2, 0) == Value(""));
	CHECK(docs->GetValue(2, 1).IsNull());
	Query(con, "BEGIN");
	CHECK(Query(con, "SELECT m.main.family(7)")->GetValue(0, 0) == Value(8));
	Query(con, "SET default_order='desc'");
	auto changed = con.Query("SELECT m.main.family(7)");
	REQUIRE(changed->HasError());
	CHECK((changed->GetError().find("context") != std::string::npos ||
	       changed->GetError().find("settings") != std::string::npos));
	Query(con, "ROLLBACK");
	Query(con, "SET default_order='asc'");
	CHECK(Query(con, "SELECT m.main.family(7)")->GetValue(0, 0) == Value(8));
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
	CHECK(Query(con, "SELECT late_bind.main.family(7)")->GetValue(0, 0) == Value(8));
	auto missing_function = con.Query("SELECT late_bind.main.family('text')");
	REQUIRE(missing_function->HasError());
	INFO(missing_function->GetError());
	CHECK(missing_function->GetError().find("missing_function") != std::string::npos);
	CHECK(Query(con, "SELECT late_bind.main.family(7)")->GetValue(0, 0) == Value(8));
	CHECK(Query(con, "SELECT 11")->GetValue(0, 0) == Value(11));
}
