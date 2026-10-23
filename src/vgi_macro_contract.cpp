// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_macro_contract.hpp"
#include "generated/vgi_protocol_schemas.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/function/table/arrow.hpp"
#include <arrow/util/byte_size.h>

namespace duckdb {
namespace vgi {
namespace {
void Require(bool condition, const char *message) {
	if (!condition)
		throw InvalidInputException("VGI macro family: %s", message);
}
void Identifier(const std::string &name) {
	Require(!name.empty() && name.size() <= 4096 && name.find('\0') == std::string::npos, "invalid identifier");
}
void Strict(const std::shared_ptr<arrow::RecordBatch> &batch, const std::shared_ptr<arrow::Schema> &schema) {
	Require(batch && batch->num_rows() == 1, "record requires exactly one row");
	Require(arrow::util::TotalBufferSize(*batch) <= 8 * 1024 * 1024, "record is too large");
	ValidateVgiQueryResultBatch(batch, schema);
}
std::shared_ptr<arrow::RecordBatch> StructRow(const std::shared_ptr<arrow::StructArray> &array, int64_t index,
                                              const std::shared_ptr<arrow::Schema> &schema) {
	Require(!array->IsNull(index), "NULL record in family");
	std::vector<std::shared_ptr<arrow::Array>> values;
	for (int i = 0; i < array->num_fields(); ++i)
		values.push_back(array->field(i)->Slice(index, 1));
	auto result = arrow::RecordBatch::Make(schema, 1, values);
	Strict(result, schema);
	return result;
}
void TypeMetadata(const std::shared_ptr<arrow::Schema> &schema) {
	Require(!schema->metadata() || schema->metadata()->size() == 0,
	        "schema type metadata requires a qualified adapter");
	std::vector<std::shared_ptr<arrow::Field>> pending = schema->fields();
	while (!pending.empty()) {
		auto field = pending.back();
		pending.pop_back();
		Require(!field->metadata() || field->metadata()->size() == 0,
		        "field type metadata requires a qualified adapter");
		auto type = field->type();
		Require(type->id() != arrow::Type::EXTENSION, "extension types require a qualified adapter");
		while (type->id() == arrow::Type::DICTIONARY) {
			type = std::static_pointer_cast<arrow::DictionaryType>(type)->value_type();
		}
		Require(type->id() != arrow::Type::EXTENSION, "extension types require a qualified adapter");
		for (const auto &child : type->fields())
			pending.push_back(child);
	}
}
std::shared_ptr<arrow::Schema> TypeSchema(const std::vector<uint8_t> &bytes) {
	Require(!bytes.empty() && bytes.size() <= 1024 * 1024, "invalid declared type schema size");
	auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromVector(bytes));
	arrow::ipc::DictionaryMemo memo;
	auto result = arrow::ipc::ReadSchema(reader.get(), &memo);
	ThrowOnArrowError(result.status());
	Require(reader->Tell().ValueOrDie() == static_cast<int64_t>(bytes.size()), "trailing declared type schema bytes");
	return result.ValueOrDie();
}
} // namespace

std::shared_ptr<arrow::RecordBatch> DecodeMacroInfoRecord(const uint8_t *data, size_t size) {
	Require(size > 0 && size <= 8 * 1024 * 1024, "invalid macro record size");
	auto record = DeserializeVgiQueryRecord(std::vector<uint8_t>(data, data + size));
	Strict(record, generated::MacroInfoSchema());
	return record;
}

VgiMacroInfo ParseMacroInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	Strict(batch, generated::MacroInfoSchema());
	RecordBatchSingleRow row(batch, 0, "MacroInfo", worker_path);
	VgiMacroInfo info;
	info.name = row["name"].value_not_null<std::string>();
	Identifier(info.name);
	auto path = row["schema_path"].value_not_null<std::vector<std::string>>();
	Require(path.size() == 1, "native 1.5 requires a raw root owner schema");
	Identifier(path[0]);
	info.schema_name = path[0];
	info.macro_type = StringUtil::Lower(row["macro_type"].value_not_null<std::string>());
	Require(info.macro_type == "scalar" || info.macro_type == "table", "invalid macro kind");
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
	auto list = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName("overloads"));
	Require(list->value_length(0) > 0 && list->value_length(0) <= 256, "family must have 1 to 256 overloads");
	auto values = std::static_pointer_cast<arrow::StructArray>(list->values());
	for (int64_t index = list->value_offset(0); index < list->value_offset(0) + list->value_length(0); ++index) {
		auto record = StructRow(values, index, generated::MacroOverloadSchema());
		RecordBatchSingleRow overload(record, 0, "MacroOverload", worker_path);
		Require(overload["profile"].value_not_null<std::string>() == "duckdb.macro.v1",
		        "unsupported definition profile");
		Require(overload["macro_type"].value_not_null<std::string>() == info.macro_type,
		        "overload kind differs from family");
		Require(overload["schema_path"].value_not_null<std::vector<std::string>>() == path,
		        "overload owner differs from family");
		auto settings =
		    StructRow(std::static_pointer_cast<arrow::StructArray>(record->GetColumnByName("evaluation_context")), 0,
		              generated::QueryEvaluationContextSchema());
		RecordBatchSingleRow context(settings, 0, "Macro context", worker_path);
		Require(context["timezone"].value_not_null<std::string>() == "UTC" &&
		            context["collation"].value_not_null<std::string>() == "binary" &&
		            context["default_order"].value_not_null<std::string>() == "asc" &&
		            context["default_null_order"].value_not_null<std::string>() == "nulls_last" &&
		            context["preserve_insertion_order"].value_not_null<bool>(),
		        "unsupported evaluation context");
		VgiMacroOverload item;
		item.definition = overload["definition"].value_not_null<std::string>();
		Require(!item.definition.empty() && item.definition.size() <= 1024 * 1024 &&
		            item.definition.find('\0') == std::string::npos,
		        "invalid definition SQL");
		auto parameters = std::static_pointer_cast<arrow::ListArray>(record->GetColumnByName("parameters"));
		Require(parameters->value_length(0) <= 1024, "too many parameters");
		auto parameter_values = std::static_pointer_cast<arrow::StructArray>(parameters->values());
		case_insensitive_set_t names;
		bool seen_default = false;
		for (int64_t p = parameters->value_offset(0); p < parameters->value_offset(0) + parameters->value_length(0);
		     ++p) {
			auto parameter_record = StructRow(parameter_values, p, generated::MacroParameterSchema());
			RecordBatchSingleRow parameter(parameter_record, 0, "MacroParameter", worker_path);
			VgiMacroParameter value;
			value.name = parameter["name"].value_not_null<std::string>();
			Identifier(value.name);
			Require(names.insert(value.name).second, "duplicate parameter name");
			Require(parameter["default_expression"].is_null(), "discovery must return resolved literal defaults");
			if (auto declared = parameter["declared_type"].as<std::vector<uint8_t>>()) {
				value.declared_type = TypeSchema(*declared);
				Require(value.declared_type->num_fields() == 1 && value.declared_type->field(0)->name() == value.name &&
				            value.declared_type->field(0)->nullable() &&
				            value.declared_type->field(0)->type()->id() != arrow::Type::NA,
				        "invalid declared parameter type");
				TypeMetadata(value.declared_type);
			}
			if (auto literal = parameter["default_value"].as<std::vector<uint8_t>>()) {
				value.default_value = DeserializeVgiQueryRecord(*literal);
				Strict(value.default_value, value.default_value->schema());
				Require(value.default_value->num_columns() == 1 &&
				            value.default_value->schema()->field(0)->name() == value.name,
				        "literal default must contain its single named parameter");
				TypeMetadata(value.default_value->schema());
			}
			Require(!seen_default || value.default_value, "required parameter follows a default");
			seen_default = seen_default || bool(value.default_value);
			value.documentation = parameter["documentation"].as<std::string>();
			Require(!value.documentation || (value.documentation->size() <= 64 * 1024 &&
			                                 value.documentation->find('\0') == std::string::npos),
			        "invalid parameter documentation");
			item.parameters.push_back(std::move(value));
		}
		info.overloads.push_back(std::move(item));
	}
	return info;
}

void RequireNativeMacroContext(ClientContext &context) {
	// The same bounded context qualification is needed at invocation, including
	// cache hits: macro bodies bind when called, not when their metadata arrives.
	NativeColumnEvaluationContext(context);
}

LogicalType NativeMacroParameterType(ClientContext &context, const VgiMacroParameter &parameter) {
	if (!parameter.declared_type)
		return LogicalType::UNKNOWN;
	ArrowSchemaWrapper exported;
	ArrowTableSchema table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, parameter.declared_type, exported, table, types, names);
	return types[0];
}

Value NativeMacroDefaultValue(ClientContext &context, const VgiMacroParameter &parameter) {
	Require(bool(parameter.default_value), "missing literal default");
	ArrowSchemaWrapper exported;
	ArrowTableSchema table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, parameter.default_value->schema(), exported, table, types, names);
	auto array = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(parameter.default_value, *array);
	ArrowScanLocalState state(std::move(array), context);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
	chunk.SetCardinality(1);
	ArrowTableFunction::ArrowToDuckDB(state, table.GetColumns(), chunk, false);
	return chunk.GetValue(0, 0);
}
} // namespace vgi
} // namespace duckdb
