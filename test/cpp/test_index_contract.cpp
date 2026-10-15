// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "vgi_index_contract.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_rpc_types.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

using namespace duckdb;
namespace {
std::vector<uint8_t> Pack(const std::shared_ptr<arrow::Schema> &schema,
                          std::vector<std::shared_ptr<arrow::Array>> arrays) {
	return vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(schema, 1, std::move(arrays)));
}
std::shared_ptr<arrow::Array> Null(const std::shared_ptr<arrow::DataType> &type) {
	return arrow::MakeArrayOfNull(type, 1).ValueOrDie();
}
std::vector<uint8_t> Key(std::vector<std::string> path = {"a.b"}) {
	return Pack(vgi::generated::IndexKeySchema(), {vgi::BuildStringListScalar(path), Null(arrow::binary())});
}
std::vector<uint8_t> Definition(const std::vector<uint8_t> &key = Key(), const std::string &kind = "UNIQUE",
                                bool association = false) {
	auto table = vgi::BuildNativeIndexTableRef("main", "items");
	return Pack(vgi::generated::IndexDefinitionSchema(), {vgi::BuildBinaryScalarRequired(table),
	    vgi::BuildStringScalar("SQLITE.BTREE.V1"), vgi::BuildBinaryListScalar({key}),
	    vgi::BuildBinaryScalarRequired(Pack(arrow::schema({}), {})),
	    vgi::BuildEnumArray(kind, {"NONE", "UNIQUE", "PRIMARY"}),
	    association ? vgi::BuildBinaryScalarRequired(table) : Null(arrow::binary())});
}
std::shared_ptr<arrow::RecordBatch> Info(const std::vector<uint8_t> &definition = Definition()) {
	auto options = Pack(vgi::generated::OptionStateSchema(), {
	    vgi::BuildBinaryScalarRequired(Pack(arrow::schema({}), {})), vgi::BuildStringListScalar({})});
	return arrow::RecordBatch::Make(vgi::generated::IndexInfoSchema(), 1, {
	    Null(arrow::utf8()), vgi::BuildStringMapScalar({}), vgi::BuildStringScalar("index.with.dots"),
	    vgi::BuildStringListScalar({"main"}), vgi::BuildBinaryScalarRequired(definition),
	    vgi::BuildBinaryScalarRequired(options)});
}
} // namespace

TEST_CASE("Typed index discovery preserves raw keys without SQL parsing", "[index-contract]") {
	for (int i = 0; i < 32; ++i) {
		auto index = vgi::ParseNativeIndexInfo(Info());
		CHECK(index.name == "index.with.dots");
		CHECK(index.table_name == "items");
		CHECK(index.algorithm == "SQLITE.BTREE.V1");
		CHECK(index.columns == std::vector<std::string>{"a.b"});
		CHECK(index.unique);
	}
}
TEST_CASE("Native index rejects unsupported or malformed definitions", "[index-contract]") {
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(Definition(Key({"nested", "field"})))));
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(Definition(Key(), "PRIMARY"))));
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(Definition(Key(), "UNIQUE", true))));
	auto key = Pack(vgi::generated::IndexKeySchema(), {Null(arrow::list(arrow::utf8())), Null(arrow::binary())});
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(Definition(key))));
	key = Pack(vgi::generated::IndexKeySchema(), {vgi::BuildStringListScalar({"a.b"}), vgi::BuildBinaryScalarRequired({1})});
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(Definition(key))));
	auto definition = Definition(); definition.push_back(0);
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(definition)));
	CHECK_THROWS(vgi::ParseNativeIndexInfo(Info(Definition(Key({""})))));
}
TEST_CASE("Native index CREATE uses typed parent and explicit profiles", "[index-contract]") {
	auto bytes = vgi::BuildNativeIndexCreate({1}, {2}, "main", "ix", Definition(), "IGNORE");
	auto request = vgi::DeserializeFromIpcBytes(bytes);
	CHECK(request->schema()->Equals(*vgi::generated::IndexCreateRequestSchema()));
	vgi::RecordBatchSingleRow row(request, 0, "IndexCreateRequest", "test");
	CHECK(row["definition"].value_not_null<std::vector<uint8_t>>() == Definition());
	CHECK(row["on_conflict"].value_not_null<std::string>() == "IGNORE");
	vgi::CatalogAttachResult attached;
	CHECK_THROWS(vgi::RequireNativeIndexCapabilities(attached, vgi::VGI_INDEX_CREATE));
	attached.constraint_profiles = {vgi::VGI_INDEX_READ, vgi::VGI_INDEX_CREATE};
	CHECK_NOTHROW(vgi::RequireNativeIndexCapabilities(attached, vgi::VGI_INDEX_CREATE));
	CHECK_THROWS(vgi::RequireNativeIndexCapabilities(attached, vgi::VGI_INDEX_CREATE, true));
	CHECK_THROWS(vgi::RequireNativeIndexCapabilities(attached, vgi::VGI_INDEX_CREATE, false, true));
	attached.constraint_profiles.insert(vgi::VGI_CONSTRAINT_READ);
	attached.constraint_profiles.insert(vgi::VGI_CONSTRAINT_ENFORCE);
	attached.constraint_profiles.insert("vgi.transaction.ddl.v1");
	CHECK_NOTHROW(vgi::RequireNativeIndexCapabilities(attached, vgi::VGI_INDEX_CREATE, true, true));
}
TEST_CASE("Native index option handshake requires an empty exact typed schema", "[index-contract]") {
	auto batch = arrow::RecordBatch::Make(vgi::generated::OptionDefinitionsSchema(), 1, {
	    vgi::BuildBinaryScalarRequired(vgi::SerializeSchemaToIpcBytes(arrow::schema({}))),
	    vgi::BuildBinaryScalarRequired(Pack(arrow::schema({}), {})),
	    vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({})});
	CHECK_NOTHROW(vgi::ValidateNativeIndexOptionDefinitions(batch));
	auto columns = batch->columns();
	columns[0] = vgi::BuildBinaryScalarRequired(vgi::SerializeSchemaToIpcBytes(arrow::schema({arrow::field("fill", arrow::int32())})));
	CHECK_THROWS(vgi::ValidateNativeIndexOptionDefinitions(arrow::RecordBatch::Make(batch->schema(), 1, columns)));
	auto bytes = vgi::SerializeSchemaToIpcBytes(arrow::schema({}));
	bytes.push_back(0);
	columns[0] = vgi::BuildBinaryScalarRequired(bytes);
	CHECK_THROWS(vgi::ValidateNativeIndexOptionDefinitions(arrow::RecordBatch::Make(batch->schema(), 1, columns)));
}

TEST_CASE("Native index encoding resolves raw catalog column spelling", "[index-contract]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_FALSE(connection.Query("CREATE TABLE t(\"a.b\" BIGINT)")->HasError());
	REQUIRE_FALSE(connection.Query("BEGIN")->HasError());
	auto &table = Catalog::GetEntry<TableCatalogEntry>(*connection.context, "memory", "main", "t");
	CreateIndexInfo create;
	create.index_name = "ix";
	create.index_type = "SQLITE.BTREE.V1";
	create.constraint_type = IndexConstraintType::NONE;
	create.expressions.push_back(make_uniq<ColumnRefExpression>("A.B", "T"));
	auto definition_bytes = vgi::EncodeNativeIndexDefinition(create, table);
	auto definition = vgi::DeserializeFromIpcBytes(definition_bytes);
	vgi::RecordBatchSingleRow row(definition, 0, "IndexDefinition", "test");
	auto keys = row["keys"].value_not_null<std::vector<std::vector<uint8_t>>>();
	auto key = vgi::DeserializeFromIpcBytes(keys[0]);
	vgi::RecordBatchSingleRow key_row(key, 0, "IndexKey", "test");
	CHECK(key_row["column_path"].value_not_null<std::vector<std::string>>() == std::vector<std::string>{"a.b"});
	create.options["unsupported"] = Value::INTEGER(1);
	CHECK_THROWS(vgi::EncodeNativeIndexDefinition(create, table));
	REQUIRE_FALSE(connection.Query("ROLLBACK")->HasError());
}
