// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_rpc_types.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

using namespace duckdb;

namespace {
std::shared_ptr<arrow::RecordBatch> AttachResult(const std::optional<std::string> &default_schema) {
	auto schema = vgi::generated::CatalogAttachResultSchema();
	std::vector<std::shared_ptr<arrow::Array>> columns;
	for (const auto &field : schema->fields()) {
		if (field->nullable()) {
			columns.push_back(arrow::MakeArrayOfNull(field->type(), 1).ValueOrDie());
			continue;
		}
		switch (field->type()->id()) {
		case arrow::Type::BOOL:
			columns.push_back(vgi::BuildBoolScalar(false));
			break;
		case arrow::Type::INT64:
			columns.push_back(vgi::BuildInt64Scalar(1));
			break;
		case arrow::Type::STRING:
			columns.push_back(vgi::BuildStringScalar(""));
			break;
		case arrow::Type::BINARY:
			columns.push_back(vgi::BuildBinaryScalarRequired({1}));
			break;
		case arrow::Type::LIST:
			REQUIRE(field->type()->Equals(arrow::list(arrow::binary())));
			columns.push_back(vgi::BuildBinaryListScalar({}));
			break;
		case arrow::Type::MAP:
			columns.push_back(vgi::BuildStringMapScalar({}));
			break;
		default:
			FAIL("unhandled required attach field in test");
		}
	}
	columns[schema->GetFieldIndex("default_schema")] = vgi::BuildOptionalStringScalar(default_schema);
	return arrow::RecordBatch::Make(schema, 1, columns);
}
} // namespace

TEST_CASE("Native attach preserves explicit raw default schema names", "[default-schema]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE(vgi::generated::CatalogAttachResultSchema()->GetFieldByName("default_schema")->nullable());
	for (const auto &name : {"main", "root.with.dot", "MixedCase", "with\"quote", " leading space "}) {
		INFO(name);
		auto parsed = vgi::ParseCatalogAttachResult(AttachResult(name), "default-schema-test", *con.context);
		CHECK(parsed.default_schema == name);
	}
}

TEST_CASE("Native attach rejects absent and invalid default schema before settings", "[default-schema]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (const auto &name : {std::optional<std::string>(), std::optional<std::string>(""),
	                         std::optional<std::string>(std::string("main\0other", 10))}) {
		auto batch = AttachResult(name);
		// If default-schema validation is delayed, parsing this setting fails
		// first. Attachment setup must not process or register supplied settings.
		auto columns = batch->columns();
		columns[batch->schema()->GetFieldIndex("settings")] = vgi::BuildBinaryListScalar({{0xff}});
		batch = arrow::RecordBatch::Make(batch->schema(), 1, columns);
		CHECK_THROWS_WITH(vgi::ParseCatalogAttachResult(batch, "default-schema-test", *con.context),
		                  Catch::Contains(name ? "nonempty raw identifier without NUL" : "no implicit default"));
	}
}
