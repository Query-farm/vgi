// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "duckdb.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_rpc_types.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

using namespace duckdb;

namespace {
vgi::VgiBatchPtr AttachResult(const std::optional<std::string> &default_schema) {
	vgi::generated::CatalogAttachResult value;
	value.attach_opaque_data = {1};
	value.catalog_version = 1;
	value.default_schema = default_schema;
	return vgi::generated::ToBatch(value);
}
} // namespace

TEST_CASE("Native attach preserves explicit raw default schema names", "[default-schema]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto schema = vgi::generated::CatalogAttachResultSchema();
	REQUIRE(schema->field_nullable(schema->GetFieldIndex("default_schema")));
	for (const auto &name : {"main", "root.with.dot", "MixedCase", "with\"quote", " leading space "}) {
		INFO(name);
		auto parsed = vgi::ParseCatalogAttachResult(AttachResult(name), "default-schema-test", *con.context);
		CHECK(parsed.default_schema == name);
	}
}

TEST_CASE("Native attach accepts no default and rejects invalid names before settings", "[default-schema]") {
	DuckDB db(nullptr);
	Connection con(db);
	CHECK_FALSE(vgi::ParseCatalogAttachResult(AttachResult(std::nullopt), "default-schema-test", *con.context).default_schema);
	for (const auto &name : {std::optional<std::string>(""),
	                         std::optional<std::string>(std::string("main\0other", 10))}) {
		auto batch = AttachResult(name);
		// If default-schema validation is delayed, parsing this setting fails
		// first. Attachment setup must not process or register supplied settings.
		batch = vgi_test::ReplaceColumn(batch, batch->schema()->GetFieldIndex("settings"), vgi::BuildBinaryListScalar({{0xff}}));
		CHECK_THROWS_WITH(vgi::ParseCatalogAttachResult(batch, "default-schema-test", *con.context),
		                  Catch::Contains("nonempty raw identifier without NUL"));
	}
}
