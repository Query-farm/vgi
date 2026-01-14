#include "vgi_arrow_utils.hpp"

#include <arrow/c/bridge.h>

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
		types.push_back(arrow_type->GetDuckType());
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
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), arrow_table_out,
	                                             c_schema_out.arrow_schema);

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

// ============================================================================
// DuckDB Value to Arrow Conversion (using ArrowAppender)
// ============================================================================

ArrowArguments BuildArgumentsFromValues(ClientContext &context, const vector<Value> &positional_args,
                                        const vector<std::pair<string, Value>> &named_args) {
	// Build the struct type with named fields
	vector<LogicalType> field_types;
	vector<string> field_names;

	// Add positional arguments
	for (idx_t i = 0; i < positional_args.size(); i++) {
		field_names.push_back("positional_" + to_string(i));
		field_types.push_back(positional_args[i].type());
	}

	// Add named arguments
	for (auto &[name, value] : named_args) {
		field_names.push_back(name);
		field_types.push_back(value.type());
	}

	// Handle empty arguments case
	if (field_types.empty()) {
		std::vector<std::shared_ptr<arrow::Field>> empty_fields;
		std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
		auto empty_struct_type = arrow::struct_(empty_fields);
		auto empty_result = arrow::StructArray::Make(empty_arrays, empty_fields);
		if (!empty_result.ok()) {
			throw IOException("Failed to create empty struct array: %s", empty_result.status().ToString());
		}
		return ArrowArguments{empty_struct_type, empty_result.ValueUnsafe()};
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
	ArrowAppender appender(field_types, 1, client_props, ArrowTypeExtensionData::GetExtensionTypes(context, field_types));
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

	return ArrowArguments{struct_type, struct_result.ValueUnsafe()};
}

} // namespace vgi
} // namespace duckdb
