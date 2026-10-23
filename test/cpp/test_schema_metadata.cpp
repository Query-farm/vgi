// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "vgi_extension.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_rpc_types.hpp"
#include <cstdlib>

using namespace duckdb;

namespace {
void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
}

void CreateSchema(Connection &con, const string &catalog, const string &name, const Value &comment,
                  const string &owner, OnCreateConflict conflict = OnCreateConflict::ERROR_ON_CONFLICT) {
	auto info = make_uniq<CreateSchemaInfo>();
	info->catalog = catalog;
	info->schema = name;
	info->comment = comment;
	info->tags["owner"] = owner;
	info->tags["empty"] = "";
	info->tags["a.b"] = "literal";
	info->on_conflict = conflict;
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	auto result = con.Query(std::move(statement));
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
}

void CheckMetadata(Connection &con) {
	auto result = con.Query("SELECT schema_name, comment, tags['owner'], tags['empty'], tags['a.b'] "
	                        "FROM duckdb_schemas() WHERE database_name = 'metadata' AND schema_name != 'main' "
	                        "ORDER BY schema_name");
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->RowCount() == 3);
	REQUIRE(result->GetValue(0, 0).ToString() == "no_comment");
	REQUIRE(result->GetValue(1, 0).IsNull());
	REQUIRE(result->GetValue(0, 1).ToString() == "with\"quote");
	REQUIRE(result->GetValue(1, 1).ToString() == "kept");
	REQUIRE(result->GetValue(0, 2).ToString() == "with.dot");
	REQUIRE_FALSE(result->GetValue(1, 2).IsNull());
	REQUIRE(result->GetValue(1, 2).ToString().empty());
	for (idx_t row = 0; row < 3; ++row) {
		REQUIRE(result->GetValue(2, row).ToString() == "one");
		REQUIRE_FALSE(result->GetValue(3, row).IsNull());
		REQUIRE(result->GetValue(3, row).ToString().empty());
		REQUIRE(result->GetValue(4, row).ToString() == "literal");
	}
}
} // namespace

TEST_CASE("Unqualified logical types fail before Arrow storage conversion", "[wire-contract-guards]") {
	auto tuple_metadata = arrow::key_value_metadata({"vgi.logical_type.v1"}, {"tuple"});
	auto tuple = arrow::field("value", arrow::struct_({arrow::field("element1", arrow::int64())}), true,
	                          tuple_metadata);
	for (auto type : {tuple->type(), arrow::list(tuple), arrow::dictionary(arrow::int32(), arrow::struct_({tuple}))}) {
		ArrowSchemaWrapper exported;
		auto field = type == tuple->type() ? tuple : arrow::field("nested", type);
		CHECK_THROWS_AS(vgi::ExportSchema(arrow::schema({field}), exported), NotImplementedException);
	}
	for (const auto &key : {"vgi.logical_type.v1", "vgi.logical_type.v99"}) {
		ArrowSchemaWrapper exported;
		auto field = arrow::field("value", arrow::binary(), true, arrow::key_value_metadata({key}, {"variant"}));
		CHECK_THROWS_AS(vgi::ExportSchema(arrow::schema({field}), exported), NotImplementedException);
	}
	ArrowSchemaWrapper parquet_variant;
	auto variant = arrow::field("value", arrow::struct_({arrow::field("metadata", arrow::binary(), false),
	                                                   arrow::field("value", arrow::binary())}),
	                            true, arrow::key_value_metadata({"ARROW:extension:name"}, {"arrow.parquet.variant"}));
	CHECK_THROWS_AS(vgi::ExportSchema(arrow::schema({variant}), parquet_variant), NotImplementedException);
	ArrowSchemaWrapper ordinary;
	CHECK_NOTHROW(vgi::ExportSchema(arrow::schema({arrow::field("plain", tuple->type())}), ordinary));
}

TEST_CASE("Unqualified column expressions fail before Arrow schema conversion", "[wire-contract-guards]") {
	for (const auto &key : {"vgi.column.default.v1", "vgi.column.generated.v1", "vgi.column.generation.v1",
	                        "vgi.column.default.v99", "vgi.column.generated.v99", "vgi.column.generation.v99"}) {
		auto field = arrow::field("value", arrow::int64(), true, arrow::key_value_metadata({key}, {"opaque"}));
		for (const auto &candidate :
		     {field, arrow::field("nested", arrow::struct_({field})), arrow::field("items", arrow::list(field)),
		      arrow::field("encoded", arrow::dictionary(arrow::int32(), arrow::struct_({field})))}) {
			ArrowSchemaWrapper exported;
			CHECK_THROWS_AS(vgi::ExportSchema(arrow::schema({candidate}), exported), NotImplementedException);
		}
	}
	ArrowSchemaWrapper legacy;
	auto field = arrow::field("value", arrow::int64(), true, arrow::key_value_metadata({"default"}, {"7"}));
	CHECK_NOTHROW(vgi::ExportSchema(arrow::schema({field}), legacy));
}

TEST_CASE("Schema create and copy preserve metadata through both cache paths", "[catalog-metadata]") {
	auto worker = std::getenv("VGI_TEST_WORKER");
	REQUIRE(worker != nullptr);
	REQUIRE(std::string(worker).size() > 0);
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	Execute(con, "ATTACH 'contents_reval' AS metadata (TYPE vgi, LOCATION '" +
	                 StringUtil::Replace(worker, "'", "''") + "')");
	Execute(con, "ATTACH ':memory:' AS source");
	// 1.5 rejects COMMENT ON SCHEMA in SQL. CreateInfo already carries both
	// metadata fields, including through COPY FROM DATABASE's GetInfo path.
	CreateSchema(con, "source", "with.dot", Value(""), "one");
	CreateSchema(con, "source", "with\"quote", Value("kept"), "one");
	CreateSchema(con, "source", "no_comment", Value(), "one");
	Execute(con, "COPY FROM DATABASE source TO metadata (SCHEMA)");
	CheckMetadata(con);
	CreateSchema(con, "metadata", "with.dot", Value("ignored"), "ignored", OnCreateConflict::IGNORE_ON_CONFLICT);
	CheckMetadata(con);
	Execute(con, "SET vgi_catalog_contents = false");
	Execute(con, "CALL vgi_clear_cache()");
	CheckMetadata(con);
	Execute(con, "SET vgi_catalog_contents = true");
	Execute(con, "CALL vgi_clear_cache()");
	CheckMetadata(con);
	Execute(con, "DETACH source");
	Execute(con, "DETACH metadata");
}
