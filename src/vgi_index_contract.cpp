// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_index_contract.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_protocol_schemas.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "vgi_ipc.hpp"
#include "nanoarrow/nanoarrow.hpp"
#include <set>

namespace duckdb {
namespace vgi {
namespace {
constexpr size_t MAX_BYTES = 8 * 1024 * 1024;
void Require(bool ok, const std::string &message) {
	if (!ok)
		throw InvalidInputException("VGI index protocol: %s", message);
}
void Identifier(const std::string &value) {
	Require(!value.empty() && value.size() <= 4096 && value.find('\0') == std::string::npos, "invalid identifier");
}
void Strict(const VgiBatchPtr &batch, const VgiSchemaPtr &schema) {
	Require(batch && batch->num_rows() == 1 && batch->TotalBufferSize() <= MAX_BYTES,
	        "record must contain one bounded row");
	ValidateVgiQueryResultBatch(batch, schema);
	for (int column_index = 0; column_index < batch->num_columns(); ++column_index) {
		auto column = batch->column(column_index);
		if (column.type().type != NANOARROW_TYPE_LIST || column.IsNull(0))
			continue;
		int64_t begin = 0, end = 0;
		column.ListRange(0, begin, end);
		for (auto i = begin; i < end; ++i)
			Require(!column.child(0).IsNull(i), "NULL list element");
	}
}
VgiBatchPtr Decode(const std::vector<uint8_t> &bytes, const VgiSchemaPtr &schema) {
	Require(!bytes.empty() && bytes.size() <= MAX_BYTES, "invalid nested IPC size");
	auto input = std::make_shared<VgiBufferReader>(VgiBuffer::FromVector(bytes));
	auto reader = VgiIpcReader::Open(input);
	auto batch = reader->ReadNext();
	Strict(batch, schema);
	Require(!reader->ReadNext(), "unexpected extra batch");
	Require(input->remaining() == 0, "trailing IPC bytes");
	return batch;
}
std::vector<uint8_t> Encode(const VgiSchemaPtr &schema, std::vector<ArrowArray> columns) {
	return SerializeToIpcBytes(VgiBatch::FromColumns(schema, 1, std::move(columns)));
}
std::vector<uint8_t> EmptyOptions() {
	return Encode(VgiSchema::Empty(), {});
}
void EmptyValues(const std::vector<uint8_t> &bytes) {
	auto values = Decode(bytes, VgiSchema::Empty());
	Require(values->schema()->metadata().empty(), "native index options with semantic metadata are unsupported");
}
void ReadDefinition(VgiIndexInfo &index, const std::vector<uint8_t> &bytes) {
	auto batch = Decode(bytes, generated::IndexDefinitionSchema());
	RecordBatchSingleRow row(batch, 0, "IndexDefinition", "index");
	auto table = Decode(row["table"].value_not_null<std::vector<uint8_t>>(), generated::CatalogObjectRefSchema());
	RecordBatchSingleRow ref(table, 0, "CatalogObjectRef", "index");
	Require(ref["kind"].value_not_null<std::string>() == "table" && table->GetColumnByName("owner_table").IsNull(0),
	        "invalid index table reference");
	auto schema = ref["schema_path"].value_not_null<std::vector<std::string>>();
	Require(schema.size() == 1 && schema[0] == index.schema_name,
	        "native index requires table in the same flat schema");
	index.table_name = ref["name"].value_not_null<std::string>();
	Identifier(index.table_name);
	index.algorithm = row["algorithm"].value_not_null<std::string>();
	Identifier(index.algorithm);
	auto kind = row["constraint_type"].value_not_null<std::string>();
	Require(kind == "NONE" || kind == "UNIQUE", "native index PRIMARY semantics are unsupported");
	Require(batch->GetColumnByName("associated_constraint").IsNull(0),
	        "native associated-constraint indexes are unsupported");
	index.unique = kind == "UNIQUE";
	EmptyValues(row["options"].value_not_null<std::vector<uint8_t>>());
	auto keys = row["keys"].value_not_null<std::vector<std::vector<uint8_t>>>();
	Require(!keys.empty() && keys.size() <= 1024, "invalid index key count");
	std::set<std::string> seen;
	for (const auto &bytes : keys) {
		auto key = Decode(bytes, generated::IndexKeySchema());
		Require(!key->GetColumnByName("column_path").IsNull(0) && key->GetColumnByName("expression").IsNull(0),
		        "native index expression keys are unsupported");
		RecordBatchSingleRow part(key, 0, "IndexKey", "index");
		auto path = part["column_path"].value_not_null<std::vector<std::string>>();
		Require(path.size() == 1, "native nested index paths are unsupported");
		Identifier(path[0]);
		Require(seen.insert(path[0]).second, "duplicate index key");
		index.columns.push_back(path[0]);
	}
}
} // namespace

void RequireNativeIndexCapabilities(const CatalogAttachResult &result, const char *operation, bool unique,
                                    bool transaction) {
	Require(result.constraint_profiles.count(VGI_INDEX_READ) && result.constraint_profiles.count(operation),
	        "index profile was not negotiated");
	if (unique)
		RequireNativeConstraintCapabilities(result,
		                                    std::string(operation) == VGI_INDEX_READ ? "read" : VGI_CONSTRAINT_ENFORCE);
	if (transaction)
		Require(result.constraint_profiles.count("vgi.transaction.ddl.v1"),
		        "index DDL in a transaction requires vgi.transaction.ddl.v1");
}
VgiIndexInfo ParseNativeIndexInfo(const VgiBatchPtr &batch) {
	Strict(batch, generated::IndexInfoSchema());
	RecordBatchSingleRow row(batch, 0, "IndexInfo", "index");
	VgiIndexInfo index;
	index.name = row["name"].value_not_null<std::string>();
	Identifier(index.name);
	auto schema = row["schema_path"].value_not_null<std::vector<std::string>>();
	Require(schema.size() == 1, "native adapter requires flat index schema paths");
	index.schema_name = schema[0];
	Identifier(index.schema_name);
	index.comment = row["comment"].as<std::string>();
	index.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
	ReadDefinition(index, row["definition"].value_not_null<std::vector<uint8_t>>());
	auto state = Decode(row["options"].value_not_null<std::vector<uint8_t>>(), generated::OptionStateSchema());
	RecordBatchSingleRow options(state, 0, "OptionState", "index");
	EmptyValues(options["values"].value_not_null<std::vector<uint8_t>>());
	Require(options["explicit_names"].value_not_null<std::vector<std::string>>().empty(),
	        "native index options are unsupported");
	return index;
}
std::vector<uint8_t> BuildNativeIndexTableRef(const std::string &schema, const std::string &table) {
	Identifier(schema);
	Identifier(table);
	return Encode(generated::CatalogObjectRefSchema(),
	              {BuildStringScalar("table"), BuildStringListScalar({schema}), BuildStringScalar(table),
	               BuildOptionalStringScalar(std::nullopt)});
}
std::vector<uint8_t> EncodeNativeIndexDefinition(const CreateIndexInfo &info, const TableCatalogEntry &table) {
	Require(info.constraint_type == IndexConstraintType::NONE || info.constraint_type == IndexConstraintType::UNIQUE,
	        "native index supports regular and UNIQUE only");
	Require(info.options.empty(), "native index options are unsupported");
	Require(!info.expressions.empty() && info.expressions.size() <= 1024, "invalid index key count");
	Identifier(info.index_type);
	std::vector<std::vector<uint8_t>> keys;
	std::set<std::string> seen;
	for (const auto &expression : info.expressions) {
		Require(expression->GetExpressionClass() == ExpressionClass::COLUMN_REF,
		        "native index expression keys are unsupported");
		auto &ref = expression->Cast<ColumnRefExpression>();
		Require(ref.ColumnNames().size() == 1 || (ref.ColumnNames().size() == 2 && ref.ColumnNames()[0] == table.name),
		        "native nested index paths are unsupported");
		auto &column = table.GetColumns().GetColumn(ref.GetColumnName());
		Require(!column.Generated(), "native generated-column indexes are unsupported");
		Require(column.Type().id() != LogicalTypeId::VARCHAR ||
		            (StringType::GetCollation(column.Type()).empty() ||
		             StringUtil::CIEquals(StringType::GetCollation(column.Type()), "binary")),
		        "native raw index keys require binary column collation");
		const auto &name = column.Name().GetIdentifierName();
		Require(seen.insert(name).second, "duplicate index key");
		keys.push_back(Encode(generated::IndexKeySchema(),
		                      {BuildStringListScalar({name}), BuildOptionalBinaryScalar(std::nullopt)}));
	}
	return Encode(generated::IndexDefinitionSchema(),
	              {BuildBinaryScalarRequired(BuildNativeIndexTableRef(table.ParentSchema().name.GetIdentifierName(),
	                                                                  table.name.GetIdentifierName())),
	               BuildStringScalar(info.index_type), BuildBinaryListScalar(keys),
	               BuildBinaryScalarRequired(EmptyOptions()),
	               BuildEnumArray(info.constraint_type == IndexConstraintType::UNIQUE ? "UNIQUE" : "NONE",
	                              {"NONE", "UNIQUE", "PRIMARY"}),
	               BuildOptionalBinaryScalar(std::nullopt)});
}
std::vector<uint8_t> BuildNativeIndexCreate(const std::vector<uint8_t> &attachment,
                                            const std::vector<uint8_t> &transaction, const std::string &schema,
                                            const std::string &name, const std::vector<uint8_t> &definition,
                                            const std::string &on_conflict) {
	Identifier(schema);
	Identifier(name);
	VgiIndexInfo index;
	index.schema_name = schema;
	ReadDefinition(index, definition);
	return Encode(
	    generated::IndexCreateRequestSchema(),
	    {BuildBinaryScalarRequired(attachment), BuildStringListScalar({schema}), BuildStringScalar(name),
	     BuildBinaryScalarRequired(definition), BuildEnumArray(on_conflict, {"ERROR", "IGNORE", "REPLACE"}),
	     transaction.empty() ? BuildOptionalBinaryScalar(std::nullopt) : BuildBinaryScalarRequired(transaction)});
}
void ValidateNativeIndexOptionDefinitions(const VgiBatchPtr &batch) {
	Strict(batch, generated::OptionDefinitionsSchema());
	RecordBatchSingleRow row(batch, 0, "OptionDefinitions", "index");
	auto schema_bytes = row["schema"].value_not_null<std::vector<uint8_t>>();
	Require(!schema_bytes.empty() && schema_bytes.size() <= MAX_BYTES, "invalid option schema size");
	auto input = std::make_shared<VgiBufferReader>(VgiBuffer::FromVector(schema_bytes));
	auto reader = VgiIpcReader::Open(input);
	Require(input->remaining() == 0, "option schema has trailing IPC bytes");
	auto schema = reader->schema();
	Require(schema->num_fields() == 0 && schema->metadata().empty(),
	        "native index option declarations are unsupported");
	EmptyValues(row["defaults"].value_not_null<std::vector<uint8_t>>());
	Require(row["create_names"].value_not_null<std::vector<std::string>>().empty() &&
	            row["alter_names"].value_not_null<std::vector<std::string>>().empty(),
	        "native index options are unsupported");
}
} // namespace vgi
} // namespace duckdb
