// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "vgi_extension.hpp"
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
