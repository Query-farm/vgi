#pragma once

#include <map>
#include <optional>
#include <vector>

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

// ============================================================================
// RecordBatch Value Extraction
// ============================================================================

// Forward declaration
class RecordBatchSingleRow;

// Proxy object for type-safe column value access
// Returned by RecordBatchSingleRow::operator[]
// Provides implicit conversion to std::optional<T> and value_or(default) methods
class ColumnValue {
public:
	ColumnValue(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &column_name, int64_t row_idx,
	            const std::string &batch_description, const std::string &worker_path);

	// Check if column exists in schema
	bool exists() const;

	// Check if value is null (column exists but value is null)
	bool is_null() const;

	// Check if column is nullable according to schema
	bool is_nullable() const;

	// ========================================================================
	// Implicit conversions to std::optional<T>
	// Returns nullopt if column missing OR value is null
	// ========================================================================
	operator std::optional<std::string>() const;
	operator std::optional<int64_t>() const;
	operator std::optional<int32_t>() const;
	operator std::optional<bool>() const;
	operator std::optional<std::vector<uint8_t>>() const;
	operator std::optional<std::vector<std::string>>() const;
	operator std::optional<std::vector<int32_t>>() const;
	operator std::optional<std::vector<std::vector<int32_t>>>() const;
	operator std::optional<std::map<std::string, std::string>>() const;

	// ========================================================================
	// value_or() - returns value or default if null/missing
	// Does NOT throw if column missing - just returns default
	// ========================================================================
	std::string value_or(const std::string &default_val) const;
	int64_t value_or(int64_t default_val) const;
	int32_t value_or(int32_t default_val) const;
	bool value_or(bool default_val) const;
	std::vector<uint8_t> value_or(std::vector<uint8_t> default_val) const;
	std::vector<std::string> value_or(std::vector<std::string> default_val) const;
	std::vector<int32_t> value_or(std::vector<int32_t> default_val) const;
	std::vector<std::vector<int32_t>> value_or(std::vector<std::vector<int32_t>> default_val) const;
	std::map<std::string, std::string> value_or(std::map<std::string, std::string> default_val) const;

	// ========================================================================
	// value_not_null<T>() - returns value, throws InvalidInputException if null or missing
	// Use for fields that are declared non-nullable in the protocol
	// ========================================================================
	template <typename T>
	T value_not_null() const {
		std::optional<T> opt = static_cast<std::optional<T>>(*this);
		if (!opt) {
			ThrowNotNullError();
		}
		return *opt;
	}

	// Explicit template for ambiguous contexts
	template <typename T>
	std::optional<T> as() const {
		return static_cast<std::optional<T>>(*this);
	}

private:
	std::shared_ptr<arrow::RecordBatch> batch_;
	std::string column_name_;
	int64_t row_idx_;
	std::string batch_description_;
	std::string worker_path_;

	// Helper to throw appropriate error for value_not_null()
	void ThrowNotNullError() const;
};

// Wrapper for accessing a single row of an Arrow RecordBatch
// Provides clean syntax: row["column"].value_or(default)
//
// Parameters:
//   batch: The Arrow RecordBatch to access
//   row_idx: The row index to access
//   batch_description: Human-readable description of what this batch represents (e.g., "SchemaInfo", "TableInfo")
//   worker_path: Path to the VGI worker that produced this batch (for error messages)
class RecordBatchSingleRow {
public:
	RecordBatchSingleRow(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
	                     const std::string &batch_description, const std::string &worker_path);

	// Returns proxy for type-safe column access
	ColumnValue operator[](const std::string &column_name) const;

	// Batch metadata
	int64_t row_idx() const {
		return row_idx_;
	}
	int64_t num_rows() const {
		return batch_ ? batch_->num_rows() : 0;
	}
	bool IsValid() const {
		return batch_ && row_idx_ >= 0 && row_idx_ < batch_->num_rows();
	}

	// Access underlying batch
	const std::shared_ptr<arrow::RecordBatch> &batch() const {
		return batch_;
	}

	// Context for error messages
	const std::string &batch_description() const {
		return batch_description_;
	}
	const std::string &worker_path() const {
		return worker_path_;
	}

private:
	std::shared_ptr<arrow::RecordBatch> batch_;
	int64_t row_idx_;
	std::string batch_description_;
	std::string worker_path_;
};

} // namespace vgi
} // namespace duckdb
