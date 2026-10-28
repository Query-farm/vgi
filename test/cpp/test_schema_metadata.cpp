// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
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
using namespace duckdb::vgi;
using namespace duckdb::vgi_test;

namespace {
void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
}

void CreateSchema(Connection &con, const string &catalog, const string &name, const Value &comment, const string &owner,
                  OnCreateConflict conflict = OnCreateConflict::ERROR_ON_CONFLICT) {
	auto info = make_uniq<CreateSchemaInfo>();
	info->SetCatalog(Identifier(catalog));
	info->SetSchema(Identifier(name));
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
	REQUIRE(result->Collection().GetValue(0, 0).ToString() == "no_comment");
	REQUIRE(result->Collection().GetValue(1, 0).IsNull());
	REQUIRE(result->Collection().GetValue(0, 1).ToString() == "with\"quote");
	REQUIRE(result->Collection().GetValue(1, 1).ToString() == "kept");
	REQUIRE(result->Collection().GetValue(0, 2).ToString() == "with.dot");
	REQUIRE_FALSE(result->Collection().GetValue(1, 2).IsNull());
	REQUIRE(result->Collection().GetValue(1, 2).ToString().empty());
	for (idx_t row = 0; row < 3; ++row) {
		REQUIRE(result->Collection().GetValue(2, row).ToString() == "one");
		REQUIRE_FALSE(result->Collection().GetValue(3, row).IsNull());
		REQUIRE(result->Collection().GetValue(3, row).ToString().empty());
		REQUIRE(result->Collection().GetValue(4, row).ToString() == "literal");
	}
}
} // namespace

TEST_CASE("Unqualified logical types fail before Arrow storage conversion", "[wire-contract-guards]") {
	auto plain_tuple = type::Struct({type::Int64().Named("element1")});
	auto tuple = plain_tuple.Named("value").WithMetadata(Metadata({"vgi.logical_type.v1"}, {"tuple"}));
	for (const auto &field : {tuple, type::List(tuple).Named("nested"),
	                          type::Dictionary(type::Int32(), type::Struct({tuple})).Named("nested")}) {
		ArrowSchemaWrapper exported;
		CHECK_THROWS_AS(ExportSchema(MakeSchema({field}), exported), NotImplementedException);
	}
	for (const auto &key : {"vgi.logical_type.v1", "vgi.logical_type.v99"}) {
		ArrowSchemaWrapper exported;
		auto field = type::Binary().Named("value").WithMetadata(Metadata({key}, {"variant"}));
		CHECK_THROWS_AS(ExportSchema(MakeSchema({field}), exported), NotImplementedException);
	}
	ArrowSchemaWrapper parquet_variant;
	auto variant = type::Struct({type::Binary().Named("metadata", false), type::Binary().Named("value")})
	                   .Named("value")
	                   .WithMetadata(Metadata({"ARROW:extension:name"}, {"arrow.parquet.variant"}));
	CHECK_THROWS_AS(ExportSchema(MakeSchema({variant}), parquet_variant), NotImplementedException);
	ArrowSchemaWrapper ordinary;
	CHECK_NOTHROW(ExportSchema(MakeSchema({plain_tuple.Named("plain")}), ordinary));
}

TEST_CASE("Unqualified column expressions fail before Arrow schema conversion", "[wire-contract-guards]") {
	for (const auto &key : {"vgi.column.default.v1", "vgi.column.generated.v1", "vgi.column.generation.v1",
	                        "vgi.column.default.v99", "vgi.column.generated.v99", "vgi.column.generation.v99"}) {
		auto field = type::Int64().Named("value").WithMetadata(Metadata({key}, {"opaque"}));
		for (const auto &candidate : {field, type::Struct({field}).Named("nested"), type::List(field).Named("items"),
		                              type::Dictionary(type::Int32(), type::Struct({field})).Named("encoded")}) {
			ArrowSchemaWrapper exported;
			CHECK_THROWS_AS(ExportSchema(MakeSchema({candidate}), exported), NotImplementedException);
		}
	}
	ArrowSchemaWrapper legacy;
	auto field = type::Int64().Named("value").WithMetadata(Metadata({"default"}, {"7"}));
	CHECK_NOTHROW(ExportSchema(MakeSchema({field}), legacy));
}

TEST_CASE("Schema create and copy preserve metadata through both cache paths", "[catalog-metadata]") {
	auto worker = std::getenv("VGI_TEST_WORKER");
	REQUIRE(worker != nullptr);
	REQUIRE(std::string(worker).size() > 0);
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	Execute(con, "ATTACH 'contents_reval' AS metadata (TYPE vgi, LOCATION '" + StringUtil::Replace(worker, "'", "''") +
	                 "')");
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
