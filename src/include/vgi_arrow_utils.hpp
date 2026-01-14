#pragma once

#include <arrow/api.h>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Schema Conversions (Arrow C++ -> Arrow C ABI)
// ============================================================================

// Export Arrow C++ Schema to C ABI format
// Populates the output wrapper which will own and release the schema
void ExportSchema(const std::shared_ptr<arrow::Schema> &schema, ArrowSchemaWrapper &out);

// ============================================================================
// RecordBatch Conversions (Arrow C++ -> Arrow C ABI)
// ============================================================================

// Export Arrow C++ RecordBatch to C ABI format
// Populates the output wrapper which will own and release the array
void ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch, ArrowArrayWrapper &out);

// ============================================================================
// DuckDB Type Extraction
// ============================================================================

// Extract DuckDB types and column names from an ArrowTableSchema
// The ArrowTableSchema should already be populated via ArrowTableFunction::PopulateArrowTableSchema
void GetDuckDBTypesFromArrowTable(const ArrowTableSchema &arrow_table, const ArrowSchema &c_schema,
                                  vector<LogicalType> &types, vector<string> &names);

// ============================================================================
// High-Level Conversion Utilities
// ============================================================================

// Full conversion: Arrow C++ Schema -> DuckDB types
// This combines:
//   1. Export C++ schema to C ABI
//   2. Populate ArrowTableSchema via DuckDB's built-in conversion
//   3. Extract DuckDB types and names
//
// Parameters:
//   context: DuckDB client context (needed for DBConfig)
//   schema: Arrow C++ schema to convert
//   c_schema_out: Output C ABI schema wrapper (for later use with ArrowToDuckDB)
//   arrow_table_out: Output ArrowTableSchema (for later use with ArrowToDuckDB)
//   types: Output DuckDB logical types
//   names: Output column names
void ArrowSchemaToDuckDBTypes(ClientContext &context, const std::shared_ptr<arrow::Schema> &schema,
                              ArrowSchemaWrapper &c_schema_out, ArrowTableSchema &arrow_table_out,
                              vector<LogicalType> &types, vector<string> &names);

// Simplified conversion: Arrow C++ Schema -> DuckDB ColumnList
// Use this when you only need column definitions (e.g., for CreateTableInfo)
// and don't need to keep the ArrowTableSchema for data conversion.
void ArrowSchemaToColumnList(ClientContext &context, const std::shared_ptr<arrow::Schema> &schema, ColumnList &columns);

// ============================================================================
// DuckDB Value to Arrow Conversion
// ============================================================================

// Result of building function arguments as Arrow
struct ArrowArguments {
	std::shared_ptr<arrow::DataType> type;   // Struct type with positional_0, positional_1, etc.
	std::shared_ptr<arrow::Array> array;     // Single-row struct array
};

// Convert DuckDB Values to Arrow arguments struct using ArrowAppender
// Creates a struct with fields named positional_0, positional_1, etc.
// Each field's type is preserved from the DuckDB Value type
ArrowArguments BuildArgumentsFromValues(ClientContext &context, const vector<Value> &positional_args,
                                        const vector<std::pair<string, Value>> &named_args = {});

} // namespace vgi
} // namespace duckdb
