// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "storage/vgi_table_entry.hpp"
#include "vgi_extension.hpp"
#include "vgi_column_metadata.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_catalog_rpc.hpp"
#include <cstdlib>
#include <fstream>
#include <iterator>

using namespace duckdb;
namespace {
vgi::VgiField MetadataField(const std::string &key, const std::string &value) {
	return vgi_test::Field("a.b", vgi::type::Int32(), true, vgi_test::Metadata({key}, {value}));
}
vgi::NativeColumnMetadata ReadMetadata(const vgi::VgiField &field) {
	return vgi::ReadNativeColumnMetadata(vgi::MakeSchema({field})->field(0));
}
vgi::NativeColumnMetadata ReadMetadata(const ArrowSchema &field) {
	return vgi::ReadNativeColumnMetadata(field);
}
unique_ptr<QueryResult> Execute(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
	return result;
}
unique_ptr<QueryResult> Create(Connection &con, const std::string &name, const std::optional<std::string> &comment) {
	auto info = make_uniq<CreateTableInfo>();
	info->SetCatalog("metadata");
	info->SetSchema("main");
	info->SetTableName(Identifier(name));
	ColumnDefinition column("a.b", LogicalType::INTEGER);
	if (comment)
		column.SetComment(Value(*comment));
	info->columns.AddColumn(std::move(column));
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	return con.Query(std::move(statement));
}
unique_ptr<QueryResult> Add(Connection &con, const std::string &name, const std::optional<std::string> &comment) {
	ColumnDefinition column(Identifier(name), LogicalType::INTEGER);
	if (comment)
		column.SetComment(Value(*comment));
	auto statement = make_uniq<AlterStatement>();
	statement->info = make_uniq<AddColumnInfo>(
	    AlterEntryData(QualifiedName("metadata", "main", "created"), OnEntryNotFound::THROW_EXCEPTION),
	    std::move(column), false, AddColumnConstraints {});
	return con.Query(std::move(statement));
}
} // namespace

TEST_CASE("Column metadata preserves presence and raw Unicode JSON strings", "[column-metadata]") {
	auto absent = ReadMetadata(vgi_test::Field("x", vgi::type::Int32()));
	CHECK_FALSE(absent.comment);
	CHECK_FALSE(absent.tags);
	auto empty =
	    vgi_test::Field("x", vgi::type::Int32(), true,
	                    vgi_test::Metadata({"comment", "tags", "opaque"}, {"", "{}", std::string("\xff\0", 2)}));
	auto decoded = ReadMetadata(empty);
	REQUIRE(decoded.comment);
	CHECK(decoded.comment->empty());
	REQUIRE(decoded.tags);
	CHECK(decoded.tags->empty());
	CHECK(empty.metadata.value(2) == std::string("\xff\0", 2));
	auto field =
	    MetadataField("tags", R"({"":"", "a.b":"雪", "a/b":"raw", "A":"upper", "a":"lower", "\u0000":"x\u0000y"})");
	auto tags = *ReadMetadata(field).tags;
	CHECK(tags.size() == 6);
	CHECK(tags.at("a.b") == "雪");
	CHECK(tags.at(std::string(1, '\0')) == std::string("x\0y", 3));
	CHECK(ReadMetadata(MetadataField("comment", std::string("snow 雪\0tail", 13))).comment->size() == 13);
}

TEST_CASE("Column metadata rejects malformed JSON UTF8 and duplicate keys recursively", "[column-metadata]") {
	for (const auto &invalid :
	     {std::string("[]"), std::string("null"), std::string("{\"x\":1}"), std::string("{\"x\":null}"),
	      std::string("{\"x\":{}}"), std::string("{\"x\":\"a\",\"x\":\"b\"}"),
	      std::string("{\"x\":\"a\",\"\\u0078\":\"b\"}"), std::string("{\"x\":\"\\ud800\"}"), std::string("{} {}"),
	      std::string("{\"x\":\"\xff\"}")}) {
		INFO(invalid);
		CHECK_THROWS(ReadMetadata(MetadataField("tags", invalid)));
	}
	auto bad = MetadataField("comment", std::string("\xc0\xaf", 2));
	CHECK_THROWS(ReadMetadata(bad));
	for (const auto &key : {"comment", "tags"}) {
		auto duplicate = vgi_test::Field("x", vgi::type::Int32(), true,
		                                 vgi_test::Metadata({key, key}, {key == std::string("tags") ? "{}" : "", ""}));
		CHECK_THROWS(ReadMetadata(duplicate));
	}
	auto nested = vgi::type::Struct({bad});
	auto fixed = vgi::type::List(bad);
	fixed.format = "+w:2";
	std::vector<vgi::VgiField> containers = {
	    nested,
	    vgi::type::List(bad),
	    vgi::type::LargeList(bad),
	    fixed,
	    vgi::type::Map(vgi::type::Utf8(), nested),
	    vgi::type::Dictionary(vgi::type::Int32(), nested),
	    nested.WithMetadata(vgi_test::Metadata({"ARROW:extension:name"}, {"test.column_metadata"}))};
	for (auto type : containers) {
		auto schema = vgi::MakeSchema({vgi_test::Field("outer", type)});
		CHECK_THROWS(vgi::ValidateNativeColumnMetadata(schema));
	}
}

TEST_CASE("Native CREATE ADD and discovery share metadata validation before dispatch", "[column-metadata]") {
	DuckDB db(nullptr);
	Connection con(db);
	ColumnList columns;
	columns.AddColumn(ColumnDefinition("absent", LogicalType::INTEGER));
	ColumnDefinition empty("empty", LogicalType::INTEGER);
	empty.SetComment(Value(""));
	columns.AddColumn(std::move(empty));
	ColumnDefinition comment("a.b", LogicalType::INTEGER);
	comment.SetComment(Value("雪"));
	comment.SetDefaultValue(ConstantExpression::Integer(7));
	columns.AddColumn(std::move(comment));
	auto schema = vgi::DuckDBColumnsToArrowSchema(*con.context, columns, "main");
	CHECK_FALSE(ReadMetadata(schema->field(0)).comment);
	CHECK(ReadMetadata(schema->field(1)).comment == std::optional<std::string>(""));
	CHECK(ReadMetadata(schema->field(2)).comment == std::optional<std::string>("雪"));
	CHECK(schema->field_metadata(2).FindKey(vgi::VGI_COLUMN_DEFAULT) >= 0);
	auto tagged = vgi_test::CopyField(schema->field(1))
	                  .WithMetadata(vgi_test::Metadata({"comment", "tags", "opaque"}, {"", "{}", "retained"}));
	auto authoritative =
	    vgi::MakeSchema({vgi_test::CopyField(schema->field(0)), tagged, vgi_test::CopyField(schema->field(2))});
	vgi::VgiTableInfo table;
	table.name = "t";
	table.schema_name = "main";
	table.arrow_schema = authoritative;
	auto create = vgi::CreateTableInfoFromVgiTable(*con.context, table, "main");
	CHECK(create.columns.GetColumn(LogicalIndex(0)).Comment().IsNull());
	CHECK(create.columns.GetColumn(LogicalIndex(1)).Comment() == Value(""));
	CHECK(create.columns.GetColumn(LogicalIndex(2)).Comment() == Value("雪"));
	CHECK(table.arrow_schema->Equals(*authoritative, true));
	CHECK(ReadMetadata(table.arrow_schema->field(1)).tags->empty());
	auto bad = vgi::MakeSchema({MetadataField("tags", "{\"x\":1}")});
	vgi::CatalogRpcContext unavailable {nullptr, {}, {}};
	CHECK_THROWS_WITH(vgi::InvokeCatalogTableCreate(unavailable, "main", "bad", bad, "ERROR", {}, *con.context),
	                  Catch::Contains("tag values must be strings"));
	CHECK_THROWS_WITH(vgi::InvokeCatalogTableColumnAdd(unavailable, "main", "t", bad, false, *con.context),
	                  Catch::Contains("tag values must be strings"));
	table.arrow_schema = bad;
	CHECK_THROWS_WITH(vgi::CreateTableInfoFromVgiTable(*con.context, table, "main"),
	                  Catch::Contains("tag values must be strings"));
}

TEST_CASE("Native column metadata agrees with the shared schema corpus", "[column-metadata-corpus]") {
	auto directory = std::getenv("VGI_COLUMN_METADATA_VECTORS");
	REQUIRE(directory);
	std::ifstream manifest(std::string(directory) + "/vectors.txt");
	REQUIRE(manifest.good());
	std::string expected, filename, name;
	size_t count = 0;
	while (manifest >> expected >> filename >> name) {
		INFO(name);
		std::ifstream input(std::string(directory) + "/" + filename, std::ios::binary);
		REQUIRE(input.good());
		std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
		auto schema = vgi::DeserializeSchema(bytes);
		if (expected == "valid") {
			auto before = vgi::SerializeSchemaToIpcBytes(schema);
			CHECK_NOTHROW(vgi::ValidateNativeColumnMetadata(schema));
			CHECK(vgi::SerializeSchemaToIpcBytes(schema) == before);
		} else {
			REQUIRE(expected == "invalid");
			CHECK_THROWS(vgi::ValidateNativeColumnMetadata(schema));
		}
		count++;
	}
	CHECK(count >= 25);
}

TEST_CASE("Native column comment CREATE ADD COPY and Arrow discovery retain metadata", "[column-metadata-native]") {
	auto worker = std::getenv("VGI_COLUMN_METADATA_WORKER");
	REQUIRE(worker);
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	Execute(con, "ATTACH 'columns' AS metadata (TYPE vgi, LOCATION '" + StringUtil::Replace(worker, "'", "''") + "')");
	auto created = Create(con, "created", "");
	INFO((created->HasError() ? created->GetError() : "ok"));
	REQUIRE_FALSE(created->HasError());
	for (auto item : {std::pair<std::string, std::optional<std::string>> {"absent", std::nullopt},
	                  {"empty", ""},
	                  {"text", "snow 雪"}}) {
		auto added = Add(con, item.first, item.second);
		INFO((added->HasError() ? added->GetError() : "ok"));
		REQUIRE_FALSE(added->HasError());
	}
	auto comments = Execute(con, "SELECT column_name, comment FROM duckdb_columns() WHERE database_name='metadata' AND "
	                             "table_name='created' ORDER BY column_index");
	REQUIRE(comments->RowCount() == 4);
	CHECK(comments->Collection().GetValue(1, 0) == Value(""));
	CHECK(comments->Collection().GetValue(1, 1).IsNull());
	CHECK(comments->Collection().GetValue(1, 2) == Value(""));
	CHECK(comments->Collection().GetValue(1, 3) == Value("snow 雪"));
	Execute(con, "CALL vgi_clear_cache()");
	CHECK(Execute(con, "SELECT comment FROM duckdb_columns() WHERE database_name='metadata' AND table_name='created' "
	                   "AND column_name='a.b'")
	          ->Collection()
	          .GetValue(0, 0) == Value(""));
	Execute(con, "ATTACH ':memory:' AS source");
	Execute(con, "CREATE TABLE source.main.copied (x INTEGER, y INTEGER)");
	Execute(con, "COMMENT ON COLUMN source.main.copied.x IS ''");
	Execute(con, "COMMENT ON COLUMN source.main.copied.y IS 'copy 雪'");
	Execute(con, "COPY FROM DATABASE source TO metadata (SCHEMA)");
	auto copied = Execute(con, "SELECT comment FROM duckdb_columns() WHERE database_name='metadata' AND "
	                           "table_name='copied' ORDER BY column_index");
	REQUIRE(copied->RowCount() == 2);
	CHECK(copied->Collection().GetValue(0, 0) == Value(""));
	CHECK(copied->Collection().GetValue(0, 1) == Value("copy 雪"));
	for (bool bulk : {true, false}) {
		Execute(con, std::string("SET vgi_catalog_contents = ") + (bulk ? "true" : "false"));
		Execute(con, "CALL vgi_clear_cache()");
		Execute(con, "BEGIN");
		auto &entry =
		    Catalog::GetEntry<TableCatalogEntry>(*con.context, "metadata", "main", "seeded").Cast<VgiTableEntry>();
		auto schema = entry.GetTableInfo().arrow_schema;
		CHECK_FALSE(ReadMetadata(schema->field(0)).tags);
		CHECK(ReadMetadata(schema->field(1)).tags->empty());
		CHECK(ReadMetadata(schema->field(2)).tags->at("a.b") == "snow 雪");
		CHECK(schema->field_metadata(2).Get("opaque").value() == std::string("\xff\0", 2));
		const auto &child = *schema->field(3).children[0];
		CHECK(ReadMetadata(child).comment == std::optional<std::string>("nested"));
		CHECK(ReadMetadata(child).tags->at("raw/key") == "value");
		Execute(con, "COMMIT");
	}
}
