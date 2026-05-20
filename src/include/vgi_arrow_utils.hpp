// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/status.h>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Arrow Status / Builder Helpers
// ============================================================================

// Throw an IOException if an Arrow Status is not OK.
// Use at RPC/IPC boundaries where an arrow::Status must not silently fail.
inline void ThrowOnArrowError(const arrow::Status &status) {
	if (!status.ok()) {
		throw IOException("Arrow error in VGI: %s", status.ToString());
	}
}

// Append a byte vector to a BinaryBuilder, or AppendNull if empty.
// Throws IOException on builder failure.
inline void AppendBytesOrNull(arrow::BinaryBuilder &builder, const std::vector<uint8_t> &bytes) {
	if (bytes.empty()) {
		ThrowOnArrowError(builder.AppendNull());
	} else {
		ThrowOnArrowError(builder.Append(bytes.data(), static_cast<int32_t>(bytes.size())));
	}
}

// Build a single-row arrow::StringArray holding the given value.
// Throws IOException on builder failure.
std::shared_ptr<arrow::Array> MakeSingleStringArray(const std::string &value);

// Build a single-row arrow::BinaryArray holding the given bytes (always non-null).
std::shared_ptr<arrow::Array> MakeSingleBinaryArray(const std::vector<uint8_t> &bytes);

// Build a single-row arrow::BinaryArray; null if bytes is empty.
std::shared_ptr<arrow::Array> MakeSingleBinaryArrayOrNull(const std::vector<uint8_t> &bytes);

// ============================================================================
// Settings Extraction
// ============================================================================

// Fetch the named settings from the client context, skipping any that are unset.
std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names);

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

// Convert DuckDB ColumnList to Arrow Schema
// This is the inverse of ArrowSchemaToColumnList: takes DuckDB column definitions
// and produces an Arrow schema using DuckDB's ArrowConverter via the C ABI bridge.
std::shared_ptr<arrow::Schema> DuckDBColumnsToArrowSchema(ClientContext &context, const ColumnList &columns);

// ============================================================================
// DuckDB DataChunk / Type to Arrow Conversion
// ============================================================================

// Convert DuckDB DataChunk to Arrow RecordBatch using the given schema
std::shared_ptr<arrow::RecordBatch> DataChunkToArrow(ClientContext &context, DataChunk &chunk,
                                                      const std::shared_ptr<arrow::Schema> &schema);

// Build Arrow C++ Schema from DuckDB logical types and names
std::shared_ptr<arrow::Schema> BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                                            const vector<string> &names);

// ============================================================================
// Schema Reconciliation
// ============================================================================

// Reconcile a RecordBatch produced by DuckDB to the schema a worker declared.
//
// DuckDB's LogicalType system can't preserve every Arrow attribute on the
// round-trip:
//   * nullability flags (LogicalType has no nullable bit; ArrowConverter
//     always emits nullable=true)
//   * TIMESTAMP_TZ unit/tz collapses to us-precision + session timezone
//   * decimal precision/scale shifts in some narrow cases
//
// This helper takes a batch DataChunkToArrow produced and returns a new batch
// whose schema is bit-for-bit equal to ``target_schema`` — recasting columns
// where Arrow types differ (via arrow::compute::Cast) and reshaping fields
// (nullability/metadata) where only field flags differ. Recurses into struct,
// list, list-of-struct, fixed-size-list, large-list, and map types so child
// fields' nullability is reconciled too.
//
// Fast path: when batch->schema()->Equals(*target_schema, /*check_metadata=*/false)
// returns true (nested structure included), the input batch is returned
// unchanged — zero allocations, zero kernel calls.
//
// Throws IOException if a column's Arrow type cannot be cast to the target
// type (e.g., genuinely incompatible logical types).
std::shared_ptr<arrow::RecordBatch> ReconcileBatchToSchema(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<arrow::Schema> &target_schema);

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

// Build a settings RecordBatch from a map of setting name → DuckDB Value
// Creates a single-row RecordBatch where each field preserves its DuckDB type as Arrow type
// Uses DataChunk + ArrowAppender to ensure proper type conversion (same pattern as BuildArgumentsFromValues)
std::shared_ptr<arrow::RecordBatch> BuildSettingsBatch(ClientContext &context,
                                                        const std::map<std::string, Value> &settings);

// Build a secrets RecordBatch from a map of secret_type → {key → Value}
// Creates a single-row RecordBatch where each column is a struct containing the secret's key-value pairs
// Returns empty vector if no secrets. Each struct column preserves DuckDB types as Arrow types.
std::vector<uint8_t> BuildSecretsBatch(ClientContext &context,
                                        const std::map<std::string, std::map<std::string, Value>> &secrets);

// ============================================================================
// Function Argument Schema Parsing
// ============================================================================

// Result of parsing function argument schema with positional/named/varargs distinction
// Named arguments are identified by metadata key "vgi_arg" with value "named"
// Varargs arguments are identified by metadata key "vgi_varargs" with value "true"
struct FunctionArgumentTypes {
	vector<LogicalType> positional_types;
	vector<string> positional_names;
	vector<bool> positional_is_const;  // Parallel to positional_types: true if param is a constant
	case_insensitive_map_t<LogicalType> named_parameters;
	// Varargs support: if has_varargs is true, function accepts additional arguments of varargs_type
	bool has_varargs = false;
	LogicalType varargs_type = LogicalType::ANY;
	// Table input support: position of the table input argument (-1 if none)
	// Table inputs are marked with vgi_type: table metadata in the Arrow schema
	// These become TABLE parameters in DuckDB function registration
	int table_input_position = -1;
	string table_input_name;

	bool HasTableInput() const {
		return table_input_position >= 0;
	}
};

// Parse function argument schema, distinguishing positional from named arguments
// Arguments with metadata "vgi_arg: named" are placed in named_parameters
// Others are treated as positional arguments in schema field order
FunctionArgumentTypes ParseFunctionArgumentSchema(ClientContext &context, const std::shared_ptr<arrow::Schema> &schema);

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
	operator std::optional<std::vector<std::vector<uint8_t>>>() const;
	operator std::optional<std::vector<std::string>>() const;
	operator std::optional<std::vector<int32_t>>() const;
	operator std::optional<std::vector<std::vector<int32_t>>>() const;
	operator std::optional<std::map<std::string, std::string>>() const;
	operator std::optional<std::map<std::string, int64_t>>() const;

	// ========================================================================
	// value_or() - returns value or default if null/missing
	// Does NOT throw if column missing - just returns default
	// ========================================================================
	std::string value_or(const std::string &default_val) const;
	std::string value_or(const char *default_val) const;  // Prevent implicit conversion to optional
	int64_t value_or(int64_t default_val) const;
	int32_t value_or(int32_t default_val) const;
	bool value_or(bool default_val) const;
	std::vector<uint8_t> value_or(std::vector<uint8_t> default_val) const;
	std::vector<std::vector<uint8_t>> value_or(std::vector<std::vector<uint8_t>> default_val) const;
	std::vector<std::string> value_or(std::vector<std::string> default_val) const;
	std::vector<int32_t> value_or(std::vector<int32_t> default_val) const;
	std::vector<std::vector<int32_t>> value_or(std::vector<std::vector<int32_t>> default_val) const;
	std::map<std::string, std::string> value_or(std::map<std::string, std::string> default_val) const;
	std::map<std::string, int64_t> value_or(std::map<std::string, int64_t> default_val) const;

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
