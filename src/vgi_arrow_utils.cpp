// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_arrow_utils.hpp"
#include "vgi_protocol_constants.hpp"
#include "vgi_rpc_types.hpp"

#include <arrow/c/bridge.h>
#include <arrow/compute/cast.h>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Settings Extraction
// ============================================================================

std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names) {
	std::map<std::string, Value> settings;
	for (const auto &name : setting_names) {
		Value value;
		if (context.TryGetCurrentSetting(name, value)) {
			settings[name] = value;
		}
	}
	return settings;
}

std::shared_ptr<arrow::Array> MakeSingleStringArray(const std::string &value) {
	arrow::StringBuilder builder;
	ThrowOnArrowError(builder.Append(value));
	std::shared_ptr<arrow::Array> arr;
	ThrowOnArrowError(builder.Finish(&arr));
	return arr;
}

std::shared_ptr<arrow::Array> MakeSingleBinaryArray(const std::vector<uint8_t> &bytes) {
	arrow::BinaryBuilder builder;
	ThrowOnArrowError(builder.Append(bytes.data(), static_cast<int32_t>(bytes.size())));
	std::shared_ptr<arrow::Array> arr;
	ThrowOnArrowError(builder.Finish(&arr));
	return arr;
}

std::shared_ptr<arrow::Array> MakeSingleBinaryArrayOrNull(const std::vector<uint8_t> &bytes) {
	arrow::BinaryBuilder builder;
	AppendBytesOrNull(builder, bytes);
	std::shared_ptr<arrow::Array> arr;
	ThrowOnArrowError(builder.Finish(&arr));
	return arr;
}

// ============================================================================
// Schema Conversions
// ============================================================================

void ExportSchema(const std::shared_ptr<arrow::Schema> &schema, ArrowSchemaWrapper &out) {
	auto status = arrow::ExportSchema(*schema, &out.arrow_schema);
	if (!status.ok()) {
		throw IOException("Failed to export Arrow schema to C ABI: %s", status.ToString());
	}
}

// ============================================================================
// RecordBatch Conversions
// ============================================================================

void ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch, ArrowArrayWrapper &out) {
	auto status = arrow::ExportRecordBatch(*batch, &out.arrow_array);
	if (!status.ok()) {
		throw IOException("Failed to export Arrow RecordBatch to C ABI: %s", status.ToString());
	}
}

// ============================================================================
// DuckDB Type Extraction
// ============================================================================

void GetDuckDBTypesFromArrowTable(const ArrowTableSchema &arrow_table, const ArrowSchema &c_schema,
                                  vector<LogicalType> &types, vector<string> &names) {
	types.clear();
	names.clear();

	auto &columns = arrow_table.GetColumns();
	for (int64_t i = 0; i < c_schema.n_children; i++) {
		auto &child = *c_schema.children[i];

		// Get column name (with fallback)
		string col_name = child.name ? child.name : "column" + to_string(i);
		names.push_back(col_name);

		// Get DuckDB type from ArrowTableSchema
		auto arrow_type = columns.at(i);
		types.push_back(arrow_type->GetDuckType(true));
	}
}

// ============================================================================
// High-Level Conversion Utilities
// ============================================================================

void ArrowSchemaToDuckDBTypes(ClientContext &context, const std::shared_ptr<arrow::Schema> &schema,
                              ArrowSchemaWrapper &c_schema_out, ArrowTableSchema &arrow_table_out,
                              vector<LogicalType> &types, vector<string> &names) {
	// Step 1: Export C++ schema to C ABI
	ExportSchema(schema, c_schema_out);

	// Step 2: Populate ArrowTableSchema using DuckDB's built-in conversion
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table_out, c_schema_out.arrow_schema);

	// Step 3: Extract DuckDB types and names
	GetDuckDBTypesFromArrowTable(arrow_table_out, c_schema_out.arrow_schema, types, names);
}

void ArrowSchemaToColumnList(ClientContext &context, const std::shared_ptr<arrow::Schema> &schema,
                             ColumnList &columns) {
	// Use temporary structures for the conversion
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;

	ArrowSchemaToDuckDBTypes(context, schema, c_schema, arrow_table, types, names);

	// Populate the ColumnList
	for (idx_t i = 0; i < types.size(); i++) {
		columns.AddColumn(ColumnDefinition(names[i], types[i]));
	}
}

std::shared_ptr<arrow::Schema> DuckDBColumnsToArrowSchema(ClientContext &context, const ColumnList &columns) {
	vector<LogicalType> types;
	vector<string> names;

	for (auto &col : columns.Logical()) {
		types.push_back(col.GetType());
		names.push_back(col.GetName());
	}

	return BuildArrowSchemaFromDuckDB(context, types, names);
}

// ============================================================================
// DuckDB DataChunk / Type to Arrow Conversion
// ============================================================================

std::shared_ptr<arrow::RecordBatch> DataChunkToArrow(ClientContext &context, DataChunk &chunk,
                                                      const std::shared_ptr<arrow::Schema> &schema) {
	if (chunk.size() == 0) {
		// Create empty batch with schema
		std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
		for (int i = 0; i < schema->num_fields(); i++) {
			auto builder_result = arrow::MakeBuilder(schema->field(i)->type());
			if (!builder_result.ok()) {
				throw IOException("Failed to create Arrow builder: %s", builder_result.status().ToString());
			}
			auto array_result = builder_result.ValueUnsafe()->Finish();
			if (!array_result.ok()) {
				throw IOException("Failed to finish Arrow array: %s", array_result.status().ToString());
			}
			empty_arrays.push_back(array_result.ValueUnsafe());
		}
		return arrow::RecordBatch::Make(schema, 0, empty_arrays);
	}

	// Use ArrowAppender to convert the DataChunk
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		types.push_back(chunk.data[i].GetType());
		names.push_back(schema->field(i)->name());
	}

	ClientProperties client_props = context.GetClientProperties();
	ArrowAppender appender(types, chunk.size(), client_props, ArrowTypeExtensionData::GetExtensionTypes(context, types));
	appender.Append(chunk, 0, chunk.size(), chunk.size());
	ArrowArray arr = appender.Finalize();

	// Get the Arrow schema for the chunk
	ArrowSchema c_schema;
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

	// Import to Arrow C++
	auto import_result = arrow::ImportRecordBatch(&arr, &c_schema);
	if (!import_result.ok()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		throw IOException("Failed to import Arrow batch: %s", import_result.status().ToString());
	}

	return import_result.ValueUnsafe();
}

// Cached-input fast path: see header for contract.
std::shared_ptr<arrow::RecordBatch> DataChunkToArrowCached(
    ClientContext &context, DataChunk &chunk,
    const std::shared_ptr<arrow::Schema> &schema,
    const vector<LogicalType> &cached_types,
    const vector<string> &cached_names,
    const ClientProperties &cached_client_props,
    const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &cached_extension_types) {
	if (chunk.size() == 0) {
		// Same empty-batch path as DataChunkToArrow.
		std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
		for (int i = 0; i < schema->num_fields(); i++) {
			auto builder_result = arrow::MakeBuilder(schema->field(i)->type());
			if (!builder_result.ok()) {
				throw IOException("Failed to create Arrow builder: %s", builder_result.status().ToString());
			}
			auto array_result = builder_result.ValueUnsafe()->Finish();
			if (!array_result.ok()) {
				throw IOException("Failed to finish Arrow array: %s", array_result.status().ToString());
			}
			empty_arrays.push_back(array_result.ValueUnsafe());
		}
		return arrow::RecordBatch::Make(schema, 0, empty_arrays);
	}

	// Skip the per-batch types/names push-back, ClientProperties copy, and
	// extension-type lookup: caller has pre-built and cached these.
	ArrowAppender appender(cached_types, chunk.size(), cached_client_props, cached_extension_types);
	appender.Append(chunk, 0, chunk.size(), chunk.size());
	ArrowArray arr = appender.Finalize();

	// Schema export still happens per batch — the C-ABI ArrowSchema is consumed
	// by ImportRecordBatch (its release callback is invoked), so we cannot share
	// a single instance across calls. ArrowConverter::ToArrowSchema takes its
	// ClientProperties by non-const reference; copy from the cached const ref
	// (small struct, free copy).
	ArrowSchema c_schema;
	ClientProperties props_copy = cached_client_props;
	ArrowConverter::ToArrowSchema(&c_schema, cached_types, cached_names, props_copy);

	auto import_result = arrow::ImportRecordBatch(&arr, &c_schema);
	if (!import_result.ok()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		throw IOException("Failed to import Arrow batch: %s", import_result.status().ToString());
	}

	return import_result.ValueUnsafe();
}

std::shared_ptr<arrow::Schema> BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                                            const vector<string> &names) {
	// Use DuckDB's converter to get Arrow schema
	ArrowSchema c_schema;
	ClientProperties client_props = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

	auto import_result = arrow::ImportSchema(&c_schema);
	if (!import_result.ok()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		throw IOException("Failed to import Arrow schema: %s", import_result.status().ToString());
	}

	return import_result.ValueUnsafe();
}

// ============================================================================
// DuckDB Value Settings to Arrow RecordBatch
// ============================================================================

std::shared_ptr<arrow::RecordBatch> BuildSettingsBatch(ClientContext &context,
                                                        const std::map<std::string, Value> &settings) {
	vector<LogicalType> field_types;
	vector<string> field_names;

	for (const auto &[name, value] : settings) {
		field_names.push_back(name);
		field_types.push_back(value.type());
	}

	// Create a single-row DataChunk with the setting values
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), field_types);
	chunk.SetCardinality(1);

	idx_t col_idx = 0;
	for (const auto &[name, value] : settings) {
		chunk.SetValue(col_idx++, 0, value);
	}

	// Convert to Arrow RecordBatch using the existing utilities
	auto schema = BuildArrowSchemaFromDuckDB(context, field_types, field_names);
	return DataChunkToArrow(context, chunk, schema);
}

// ============================================================================
// DuckDB Value Secrets to Arrow RecordBatch
// ============================================================================

std::vector<uint8_t> BuildSecretsBatch(ClientContext &context,
                                        const std::map<std::string, std::map<std::string, Value>> &secrets) {
	if (secrets.empty()) {
		return {};
	}

	std::vector<std::shared_ptr<arrow::Field>> outer_fields;
	std::vector<std::shared_ptr<arrow::Array>> outer_arrays;

	for (const auto &[secret_type, kv_pairs] : secrets) {
		if (kv_pairs.empty()) {
			continue;
		}

		// Build DuckDB types and values for this secret's key-value pairs
		vector<LogicalType> field_types;
		vector<string> field_names;

		for (const auto &[key, value] : kv_pairs) {
			field_names.push_back(key);
			field_types.push_back(value.type());
		}

		// Create a single-row DataChunk with the key-value pairs
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), field_types);
		chunk.SetCardinality(1);

		idx_t col_idx = 0;
		for (const auto &[key, value] : kv_pairs) {
			chunk.SetValue(col_idx++, 0, value);
		}

		// Convert to Arrow RecordBatch (preserves DuckDB types as Arrow types)
		auto inner_schema = BuildArrowSchemaFromDuckDB(context, field_types, field_names);
		auto inner_batch = DataChunkToArrow(context, chunk, inner_schema);

		// Convert RecordBatch columns into a StructArray
		std::vector<std::shared_ptr<arrow::Field>> struct_fields;
		std::vector<std::shared_ptr<arrow::Array>> struct_arrays;
		for (int i = 0; i < inner_batch->num_columns(); i++) {
			struct_fields.push_back(inner_batch->schema()->field(i));
			struct_arrays.push_back(inner_batch->column(i));
		}

		auto struct_type = arrow::struct_(struct_fields);
		auto struct_result = arrow::StructArray::Make(struct_arrays, struct_fields);
		if (!struct_result.ok()) {
			throw IOException("Failed to create struct array for secret '%s': %s",
			                  secret_type, struct_result.status().ToString());
		}

		outer_fields.push_back(arrow::field(secret_type, struct_type, true));
		outer_arrays.push_back(struct_result.ValueUnsafe());
	}

	if (outer_fields.empty()) {
		return {};
	}

	auto outer_schema = arrow::schema(outer_fields);
	auto outer_batch = arrow::RecordBatch::Make(outer_schema, 1, outer_arrays);
	return SerializeToIpcBytes(outer_batch);
}

// ============================================================================
// Function Argument Schema Parsing
// ============================================================================

FunctionArgumentTypes ParseFunctionArgumentSchema(ClientContext &context,
                                                  const std::shared_ptr<arrow::Schema> &schema) {
	FunctionArgumentTypes result;

	if (!schema) {
		return result;
	}

	// Export schema to C ABI and get DuckDB types
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	ExportSchema(schema, c_schema);
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, c_schema.arrow_schema);

	auto &columns = arrow_table.GetColumns();
	int num_fields = schema->num_fields();
	for (int field_idx = 0; field_idx < num_fields; field_idx++) {
		auto field = schema->field(field_idx);
		auto &child = c_schema.arrow_schema.children[field_idx];

		// Get column name
		string col_name = child->name ? child->name : "column" + to_string(field_idx);

		// Get DuckDB type
		auto arrow_type = columns.at(field_idx);
		LogicalType duckdb_type = arrow_type->GetDuckType();

		// Check metadata for special markers
		bool is_named = false;
		bool is_varargs = false;
		bool is_table_input = false;
		bool is_any_type = false;
		bool is_const = false;
		if (field->HasMetadata()) {
			auto metadata = field->metadata();

			// Check for named argument marker
			auto named_idx = metadata->FindKey(VGI_ARG_METADATA_KEY);
			if (named_idx >= 0) {
				auto value = metadata->value(named_idx);
				is_named = (value == VGI_ARG_NAMED_VALUE);
			}

			// Check for varargs marker (on field, not schema)
			auto varargs_idx = metadata->FindKey(VGI_VARARGS_METADATA_KEY);
			if (varargs_idx >= 0) {
				auto value = metadata->value(varargs_idx);
				is_varargs = (value == VGI_VARARGS_TRUE_VALUE);
			}

			// Check for vgi_type metadata (table input or any type)
			auto type_idx = metadata->FindKey(VGI_TYPE_METADATA_KEY);
			if (type_idx >= 0) {
				auto value = metadata->value(type_idx);
				is_table_input = (value == VGI_TYPE_TABLE_VALUE);
				is_any_type = (value == VGI_TYPE_ANY_VALUE);
			}

			// Check for const parameter marker
			auto const_idx = metadata->FindKey(VGI_CONST_METADATA_KEY);
			if (const_idx >= 0) {
				auto value = metadata->value(const_idx);
				is_const = (value == VGI_CONST_TRUE_VALUE);
			}
		}

		// Override DuckDB type if this is an "any" type field
		if (is_any_type) {
			duckdb_type = LogicalType::ANY;
		}

		if (is_varargs) {
			// Varargs field - set flag and use declared type (or ANY for untyped/any)
			result.has_varargs = true;
			result.varargs_type = (is_any_type || duckdb_type == LogicalType::SQLNULL)
			                          ? LogicalType::ANY
			                          : duckdb_type;
			// Don't add varargs to positional_types - it's a sentinel
		} else if (is_table_input) {
			// Table input field - record position and name for table-in-out function registration
			// Store the current position (in terms of positional args only)
			result.table_input_position = static_cast<int>(result.positional_types.size());
			result.table_input_name = col_name;
			// Add TABLE type placeholder - this will be replaced with actual TABLE type
			// during function registration
			result.positional_types.push_back(LogicalType::TABLE);
			result.positional_names.push_back(col_name);
			result.positional_is_const.push_back(false); // Table inputs are never const
		} else if (is_named) {
			// Named argument - add to named_parameters map
			result.named_parameters[col_name] = duckdb_type;
		} else {
			// Positional argument
			result.positional_types.push_back(duckdb_type);
			result.positional_names.push_back(col_name);
			result.positional_is_const.push_back(is_const);
		}
	}

	return result;
}

// ============================================================================
// DuckDB Value to Arrow Conversion (using ArrowAppender)
// ============================================================================

ArrowArguments BuildArgumentsFromValues(ClientContext &context, const vector<Value> &positional_args,
                                        const vector<std::pair<string, Value>> &named_args) {
	// Build the struct type with named fields
	vector<LogicalType> field_types;
	vector<string> field_names;

	// Add positional arguments (prefixed with VGI_POSITIONAL_PREFIX per VGI protocol)
	for (idx_t i = 0; i < positional_args.size(); i++) {
		field_names.push_back(VGI_POSITIONAL_PREFIX + to_string(i));
		field_types.push_back(positional_args[i].type());
	}

	// Add named arguments (prefixed with VGI_NAMED_PREFIX per VGI protocol)
	for (auto &[name, value] : named_args) {
		field_names.push_back(VGI_NAMED_PREFIX + name);
		field_types.push_back(value.type());
	}

	// Handle empty arguments case - create a struct array with 1 row but no fields
	// This happens for table-in-out functions that only have a TABLE argument
	if (field_types.empty()) {
		std::vector<std::shared_ptr<arrow::Field>> empty_fields;
		auto empty_struct_type = arrow::struct_(empty_fields);
		// Use MakeArrayOfNull to create a struct array with explicit length
		auto empty_result = arrow::MakeArrayOfNull(empty_struct_type, 1);
		if (!empty_result.ok()) {
			throw IOException("Failed to create empty struct array: %s", empty_result.status().ToString());
		}
		return ArrowArguments {empty_struct_type, empty_result.ValueUnsafe()};
	}

	// Create a single-row DataChunk with the values
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), field_types);
	chunk.SetCardinality(1);

	// Set positional argument values
	idx_t col_idx = 0;
	for (idx_t i = 0; i < positional_args.size(); i++) {
		chunk.SetValue(col_idx++, 0, positional_args[i]);
	}

	// Set named argument values
	for (auto &[name, value] : named_args) {
		chunk.SetValue(col_idx++, 0, value);
	}

	// Use ArrowAppender to convert the DataChunk to Arrow
	ClientProperties client_props = context.GetClientProperties();
	ArrowAppender appender(field_types, 1, client_props,
	                       ArrowTypeExtensionData::GetExtensionTypes(context, field_types));
	appender.Append(chunk, 0, 1, 1);
	ArrowArray arr = appender.Finalize();

	// Get the Arrow schema for the struct
	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, field_types, field_names, client_props);

	// Import the C ABI arrays into Arrow C++ for struct wrapping
	auto import_result = arrow::ImportRecordBatch(&arr, &schema);
	if (!import_result.ok()) {
		if (schema.release) {
			schema.release(&schema);
		}
		throw IOException("Failed to import Arrow array: %s", import_result.status().ToString());
	}
	auto record_batch = import_result.ValueUnsafe();

	// Convert the RecordBatch columns to a struct array
	std::vector<std::shared_ptr<arrow::Field>> struct_fields;
	std::vector<std::shared_ptr<arrow::Array>> struct_arrays;

	for (int i = 0; i < record_batch->num_columns(); i++) {
		struct_fields.push_back(record_batch->schema()->field(i));
		struct_arrays.push_back(record_batch->column(i));
	}

	auto struct_type = arrow::struct_(struct_fields);
	auto struct_result = arrow::StructArray::Make(struct_arrays, struct_fields);
	if (!struct_result.ok()) {
		throw IOException("Failed to create struct array: %s", struct_result.status().ToString());
	}

	return ArrowArguments {struct_type, struct_result.ValueUnsafe()};
}

// ============================================================================
// ColumnValue Implementation
// ============================================================================

ColumnValue::ColumnValue(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &column_name,
                         int64_t row_idx, const std::string &batch_description, const std::string &worker_path)
    : batch_(batch), column_name_(column_name), row_idx_(row_idx), batch_description_(batch_description),
      worker_path_(worker_path) {
}

bool ColumnValue::exists() const {
	if (!batch_) {
		return false;
	}
	return batch_->schema()->GetFieldByName(column_name_) != nullptr;
}

bool ColumnValue::is_null() const {
	if (!exists()) {
		return false;
	}
	auto column = batch_->GetColumnByName(column_name_);
	if (!column) {
		return false;
	}
	return column->IsNull(row_idx_);
}

bool ColumnValue::is_nullable() const {
	if (!batch_) {
		return true;
	}
	auto field = batch_->schema()->GetFieldByName(column_name_);
	if (!field) {
		return true;
	}
	return field->nullable();
}

void ColumnValue::ThrowNotNullError() const {
	if (!exists()) {
		throw InvalidInputException(
		    "VGI protocol error: Required column '%s' not found in %s response from worker '%s'", column_name_,
		    batch_description_, worker_path_);
	}
	throw InvalidInputException(
	    "VGI protocol error: Column '%s' has unexpected null value at row %lld in %s response from worker '%s'",
	    column_name_, row_idx_, batch_description_, worker_path_);
}

// ============================================================================
// ColumnValue Implicit Conversions to std::optional<T>
// ============================================================================

ColumnValue::operator std::optional<std::string>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}

	// Try plain StringArray first
	auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(column);
	if (string_array) {
		if (string_array->IsNull(row_idx_)) {
			return std::nullopt;
		}
		return string_array->GetString(row_idx_);
	}

	// Try DictionaryArray with string values (used for enum-like fields)
	auto dict_array = std::dynamic_pointer_cast<arrow::DictionaryArray>(column);
	if (dict_array) {
		if (dict_array->IsNull(row_idx_)) {
			return std::nullopt;
		}
		// Get the index and look up the value in the dictionary
		auto dict_values = std::dynamic_pointer_cast<arrow::StringArray>(dict_array->dictionary());
		if (!dict_values) {
			return std::nullopt;
		}
		auto index = dict_array->GetValueIndex(row_idx_);
		return dict_values->GetString(index);
	}

	return std::nullopt;
}

ColumnValue::operator std::optional<int64_t>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(column);
	if (!int_array || int_array->IsNull(row_idx_)) {
		return std::nullopt;
	}
	return int_array->Value(row_idx_);
}

ColumnValue::operator std::optional<int32_t>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto int_array = std::dynamic_pointer_cast<arrow::Int32Array>(column);
	if (!int_array || int_array->IsNull(row_idx_)) {
		return std::nullopt;
	}
	return int_array->Value(row_idx_);
}

ColumnValue::operator std::optional<bool>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto bool_array = std::dynamic_pointer_cast<arrow::BooleanArray>(column);
	if (!bool_array || bool_array->IsNull(row_idx_)) {
		return std::nullopt;
	}
	return bool_array->Value(row_idx_);
}

ColumnValue::operator std::optional<std::vector<uint8_t>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(column);
	if (!binary_array || binary_array->IsNull(row_idx_)) {
		return std::nullopt;
	}
	auto value = binary_array->GetView(row_idx_);
	return std::vector<uint8_t>(reinterpret_cast<const uint8_t *>(value.data()),
	                            reinterpret_cast<const uint8_t *>(value.data()) + value.size());
}

ColumnValue::operator std::optional<std::vector<std::vector<uint8_t>>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
	if (!list_array || list_array->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t start = list_array->value_offset(row_idx_);
	int64_t end = list_array->value_offset(row_idx_ + 1);

	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(list_array->values());
	if (!binary_array) {
		return std::nullopt;
	}

	std::vector<std::vector<uint8_t>> result;
	for (int64_t i = start; i < end; i++) {
		if (!binary_array->IsNull(i)) {
			auto value = binary_array->GetView(i);
			result.push_back(std::vector<uint8_t>(reinterpret_cast<const uint8_t *>(value.data()),
			                                      reinterpret_cast<const uint8_t *>(value.data()) + value.size()));
		}
	}
	return result;
}

ColumnValue::operator std::optional<std::vector<std::string>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
	if (!list_array || list_array->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t start = list_array->value_offset(row_idx_);
	int64_t end = list_array->value_offset(row_idx_ + 1);

	auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(list_array->values());
	if (!string_array) {
		return std::nullopt;
	}

	std::vector<std::string> result;
	for (int64_t i = start; i < end; i++) {
		if (!string_array->IsNull(i)) {
			result.push_back(string_array->GetString(i));
		}
	}
	return result;
}

ColumnValue::operator std::optional<std::vector<int32_t>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
	if (!list_array || list_array->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t start = list_array->value_offset(row_idx_);
	int64_t end = list_array->value_offset(row_idx_ + 1);

	auto int_array = std::dynamic_pointer_cast<arrow::Int32Array>(list_array->values());
	if (!int_array) {
		return std::nullopt;
	}

	std::vector<int32_t> result;
	for (int64_t i = start; i < end; i++) {
		if (!int_array->IsNull(i)) {
			result.push_back(int_array->Value(i));
		}
	}
	return result;
}

ColumnValue::operator std::optional<std::vector<std::vector<int32_t>>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto outer_list = std::dynamic_pointer_cast<arrow::ListArray>(column);
	if (!outer_list || outer_list->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t outer_start = outer_list->value_offset(row_idx_);
	int64_t outer_end = outer_list->value_offset(row_idx_ + 1);

	auto inner_list = std::dynamic_pointer_cast<arrow::ListArray>(outer_list->values());
	if (!inner_list) {
		return std::nullopt;
	}

	auto int_array = std::dynamic_pointer_cast<arrow::Int32Array>(inner_list->values());
	if (!int_array) {
		return std::nullopt;
	}

	std::vector<std::vector<int32_t>> result;
	for (int64_t i = outer_start; i < outer_end; i++) {
		std::vector<int32_t> inner_result;
		if (!inner_list->IsNull(i)) {
			int64_t inner_start = inner_list->value_offset(i);
			int64_t inner_end = inner_list->value_offset(i + 1);
			for (int64_t j = inner_start; j < inner_end; j++) {
				if (!int_array->IsNull(j)) {
					inner_result.push_back(int_array->Value(j));
				}
			}
		}
		result.push_back(std::move(inner_result));
	}
	return result;
}

ColumnValue::operator std::optional<std::vector<std::vector<std::string>>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto outer_list = std::dynamic_pointer_cast<arrow::ListArray>(column);
	if (!outer_list || outer_list->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t outer_start = outer_list->value_offset(row_idx_);
	int64_t outer_end = outer_list->value_offset(row_idx_ + 1);

	auto inner_list = std::dynamic_pointer_cast<arrow::ListArray>(outer_list->values());
	if (!inner_list) {
		return std::nullopt;
	}

	auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(inner_list->values());
	if (!string_array) {
		return std::nullopt;
	}

	std::vector<std::vector<std::string>> result;
	for (int64_t i = outer_start; i < outer_end; i++) {
		std::vector<std::string> inner_result;
		if (!inner_list->IsNull(i)) {
			int64_t inner_start = inner_list->value_offset(i);
			int64_t inner_end = inner_list->value_offset(i + 1);
			for (int64_t j = inner_start; j < inner_end; j++) {
				if (!string_array->IsNull(j)) {
					inner_result.push_back(string_array->GetString(j));
				}
			}
		}
		result.push_back(std::move(inner_result));
	}
	return result;
}

ColumnValue::operator std::optional<std::map<std::string, std::string>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto map_array = std::dynamic_pointer_cast<arrow::MapArray>(column);
	if (!map_array || map_array->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t start = map_array->value_offset(row_idx_);
	int64_t end = map_array->value_offset(row_idx_ + 1);

	auto keys = std::dynamic_pointer_cast<arrow::StringArray>(map_array->keys());
	auto values = std::dynamic_pointer_cast<arrow::StringArray>(map_array->items());

	if (!keys || !values) {
		return std::nullopt;
	}

	std::map<std::string, std::string> result;
	for (int64_t i = start; i < end; i++) {
		if (!keys->IsNull(i) && !values->IsNull(i)) {
			result[keys->GetString(i)] = values->GetString(i);
		}
	}
	return result;
}

ColumnValue::operator std::optional<std::map<std::string, int64_t>>() const {
	auto column = batch_ ? batch_->GetColumnByName(column_name_) : nullptr;
	if (!column) {
		return std::nullopt;
	}
	auto map_array = std::dynamic_pointer_cast<arrow::MapArray>(column);
	if (!map_array || map_array->IsNull(row_idx_)) {
		return std::nullopt;
	}

	int64_t start = map_array->value_offset(row_idx_);
	int64_t end = map_array->value_offset(row_idx_ + 1);

	auto keys = std::dynamic_pointer_cast<arrow::StringArray>(map_array->keys());
	auto values = std::dynamic_pointer_cast<arrow::Int64Array>(map_array->items());

	if (!keys || !values) {
		return std::nullopt;
	}

	std::map<std::string, int64_t> result;
	for (int64_t i = start; i < end; i++) {
		if (!keys->IsNull(i) && !values->IsNull(i)) {
			result[keys->GetString(i)] = values->Value(i);
		}
	}
	return result;
}

// ============================================================================
// ColumnValue value_or() Methods
// ============================================================================

std::string ColumnValue::value_or(const std::string &default_val) const {
	auto opt = static_cast<std::optional<std::string>>(*this);
	return opt.value_or(default_val);
}

std::string ColumnValue::value_or(const char *default_val) const {
	return value_or(std::string {default_val});
}

int64_t ColumnValue::value_or(int64_t default_val) const {
	auto opt = static_cast<std::optional<int64_t>>(*this);
	return opt.value_or(default_val);
}

int32_t ColumnValue::value_or(int32_t default_val) const {
	auto opt = static_cast<std::optional<int32_t>>(*this);
	return opt.value_or(default_val);
}

bool ColumnValue::value_or(bool default_val) const {
	auto opt = static_cast<std::optional<bool>>(*this);
	return opt.value_or(default_val);
}

std::vector<uint8_t> ColumnValue::value_or(std::vector<uint8_t> default_val) const {
	auto opt = static_cast<std::optional<std::vector<uint8_t>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::vector<std::string> ColumnValue::value_or(std::vector<std::string> default_val) const {
	auto opt = static_cast<std::optional<std::vector<std::string>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::vector<int32_t> ColumnValue::value_or(std::vector<int32_t> default_val) const {
	auto opt = static_cast<std::optional<std::vector<int32_t>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::vector<std::vector<int32_t>> ColumnValue::value_or(std::vector<std::vector<int32_t>> default_val) const {
	auto opt = static_cast<std::optional<std::vector<std::vector<int32_t>>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::vector<std::vector<std::string>> ColumnValue::value_or(std::vector<std::vector<std::string>> default_val) const {
	auto opt = static_cast<std::optional<std::vector<std::vector<std::string>>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::vector<std::vector<uint8_t>> ColumnValue::value_or(std::vector<std::vector<uint8_t>> default_val) const {
	auto opt = static_cast<std::optional<std::vector<std::vector<uint8_t>>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::map<std::string, std::string> ColumnValue::value_or(std::map<std::string, std::string> default_val) const {
	auto opt = static_cast<std::optional<std::map<std::string, std::string>>>(*this);
	return opt.value_or(std::move(default_val));
}

std::map<std::string, int64_t> ColumnValue::value_or(std::map<std::string, int64_t> default_val) const {
	auto opt = static_cast<std::optional<std::map<std::string, int64_t>>>(*this);
	return opt.value_or(std::move(default_val));
}

// ============================================================================
// RecordBatchSingleRow Implementation
// ============================================================================

RecordBatchSingleRow::RecordBatchSingleRow(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                           const std::string &batch_description, const std::string &worker_path)
    : batch_(batch), row_idx_(row_idx), batch_description_(batch_description), worker_path_(worker_path) {
}

ColumnValue RecordBatchSingleRow::operator[](const std::string &column_name) const {
	return ColumnValue(batch_, column_name, row_idx_, batch_description_, worker_path_);
}

// ============================================================================
// Schema Reconciliation
// ============================================================================

namespace {

// Forward decl — recursion target.
std::shared_ptr<arrow::ArrayData> ReshapeArrayData(const std::shared_ptr<arrow::ArrayData> &source,
                                                    const std::shared_ptr<arrow::Field> &target_field);

// Cast a leaf primitive Array to target_type, returning the underlying ArrayData.
// Caller has already verified that source.type != target_type.
std::shared_ptr<arrow::ArrayData> CastPrimitive(const std::shared_ptr<arrow::ArrayData> &source,
                                                 const std::shared_ptr<arrow::DataType> &target_type) {
	auto array = arrow::MakeArray(source);
	auto result = arrow::compute::Cast(*array, target_type);
	if (!result.ok()) {
		throw IOException("VGI schema reconciliation: failed to cast %s -> %s: %s",
		                  array->type()->ToString(), target_type->ToString(), result.status().ToString());
	}
	return result.ValueOrDie()->data();
}

// Build a new ArrayData with the same buffers/length/null_count as `source`
// but using `target_type` and the reshaped child ArrayData list.
// Used for nested-type reshape paths where children change but the parent
// layout (offsets, null bitmap) is preserved verbatim.
std::shared_ptr<arrow::ArrayData> RewrapArrayData(const std::shared_ptr<arrow::ArrayData> &source,
                                                   const std::shared_ptr<arrow::DataType> &target_type,
                                                   std::vector<std::shared_ptr<arrow::ArrayData>> reshaped_children) {
	auto out = arrow::ArrayData::Make(target_type, source->length, source->buffers, std::move(reshaped_children),
	                                  source->null_count, source->offset);
	if (source->dictionary) {
		out->dictionary = source->dictionary;
	}
	return out;
}

std::shared_ptr<arrow::ArrayData> ReshapeStruct(const std::shared_ptr<arrow::ArrayData> &source,
                                                 const std::shared_ptr<arrow::DataType> &target_type) {
	const auto &target_struct = arrow::internal::checked_cast<const arrow::StructType &>(*target_type);
	if (NumericCast<int>(source->child_data.size()) != target_struct.num_fields()) {
		throw IOException("VGI schema reconciliation: struct child count mismatch (source=%d target=%d)",
		                  static_cast<int>(source->child_data.size()), target_struct.num_fields());
	}
	std::vector<std::shared_ptr<arrow::ArrayData>> reshaped;
	reshaped.reserve(source->child_data.size());
	for (size_t i = 0; i < source->child_data.size(); i++) {
		reshaped.push_back(ReshapeArrayData(source->child_data[i], target_struct.field(NumericCast<int>(i))));
	}
	return RewrapArrayData(source, target_type, std::move(reshaped));
}

std::shared_ptr<arrow::ArrayData> ReshapeListLike(const std::shared_ptr<arrow::ArrayData> &source,
                                                   const std::shared_ptr<arrow::DataType> &target_type,
                                                   const std::shared_ptr<arrow::Field> &target_value_field) {
	if (source->child_data.size() != 1) {
		throw IOException("VGI schema reconciliation: list-like ArrayData expected 1 child, got %d",
		                  static_cast<int>(source->child_data.size()));
	}
	std::vector<std::shared_ptr<arrow::ArrayData>> reshaped;
	reshaped.reserve(1);
	reshaped.push_back(ReshapeArrayData(source->child_data[0], target_value_field));
	return RewrapArrayData(source, target_type, std::move(reshaped));
}

std::shared_ptr<arrow::ArrayData> ReshapeArrayData(const std::shared_ptr<arrow::ArrayData> &source,
                                                    const std::shared_ptr<arrow::Field> &target_field) {
	const auto &target_type = target_field->type();
	const bool same_type = source->type->Equals(*target_type);

	switch (target_type->id()) {
	case arrow::Type::STRUCT: {
		// Even if the struct types compare unequal at the top level (e.g.,
		// child nullability flags differ), the data layout matches; rebuild
		// the struct with reshaped children. If they're already equal we
		// still need to descend in case child fields' metadata differs at
		// deeper levels — the source.type->Equals check above catches the
		// fully-equal case at this exact node and short-circuits via
		// `same_type` below; struct children always need a recursive walk.
		return ReshapeStruct(source, target_type);
	}
	case arrow::Type::LIST:
	case arrow::Type::LARGE_LIST:
	case arrow::Type::FIXED_SIZE_LIST: {
		const auto &list_type = arrow::internal::checked_cast<const arrow::BaseListType &>(*target_type);
		return ReshapeListLike(source, target_type, list_type.value_field());
	}
	case arrow::Type::MAP: {
		// Map is List<Struct<key, value>> — recurse into the entries struct.
		const auto &map_type = arrow::internal::checked_cast<const arrow::MapType &>(*target_type);
		return ReshapeListLike(source, target_type, map_type.value_field());
	}
	default:
		break;
	}

	if (same_type) {
		// Leaf primitive whose type already matches: reuse buffers verbatim.
		// (The caller's RecordBatch::Make will pick up the target field's
		// nullable/metadata flags from the schema we hand it.)
		return source;
	}

	// Leaf primitive whose Arrow type differs (e.g., timestamp[us, tz=session]
	// vs target timestamp[ms, tz=UTC]) — invoke the Arrow cast kernel.
	return CastPrimitive(source, target_type);
}

} // namespace

std::shared_ptr<arrow::RecordBatch> ReconcileBatchToSchema(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<arrow::Schema> &target_schema) {
	if (!batch || !target_schema) {
		return batch;
	}
	// Fast path: schemas already equal (excluding metadata, since
	// ArrowConverter doesn't preserve it).
	if (batch->schema()->Equals(*target_schema, /*check_metadata=*/false)) {
		return batch;
	}
	if (batch->num_columns() != target_schema->num_fields()) {
		throw IOException("VGI schema reconciliation: column count mismatch (batch=%d target=%d)",
		                  batch->num_columns(), target_schema->num_fields());
	}
	std::vector<std::shared_ptr<arrow::ArrayData>> columns;
	columns.reserve(NumericCast<size_t>(batch->num_columns()));
	for (int i = 0; i < batch->num_columns(); i++) {
		const auto &target_field = target_schema->field(i);
		columns.push_back(ReshapeArrayData(batch->column_data(i), target_field));
	}
	return arrow::RecordBatch::Make(target_schema, batch->num_rows(), std::move(columns));
}

} // namespace vgi
} // namespace duckdb
