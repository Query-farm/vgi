// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "vgi_arrow.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Settings Extraction
// ============================================================================

// Fetch the named settings from the client context, skipping any that are unset.
std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names);

// ============================================================================
// C ABI exports
// ============================================================================

// Export a schema to C ABI format (deep copy, owned by `out`; a previous export is released).
void ExportSchema(const VgiSchemaPtr &schema, ArrowSchemaWrapper &out);

// Export a batch to C ABI format; the array keeps the batch's buffers alive.
void ExportRecordBatch(const VgiBatchPtr &batch, ArrowArrayWrapper &out);

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

// Full conversion: schema -> DuckDB types
// This combines:
//   1. Export schema to C ABI
//   2. Populate ArrowTableSchema via DuckDB's built-in conversion
//   3. Extract DuckDB types and names
//
// Parameters:
//   context: DuckDB client context (needed for DBConfig)
//   schema: schema to convert
//   c_schema_out: Output C ABI schema wrapper (for later use with ArrowToDuckDB)
//   arrow_table_out: Output ArrowTableSchema (for later use with ArrowToDuckDB)
//   types: Output DuckDB logical types
//   names: Output column names
void ArrowSchemaToDuckDBTypes(ClientContext &context, const VgiSchemaPtr &schema,
                              ArrowSchemaWrapper &c_schema_out, ArrowTableSchema &arrow_table_out,
                              vector<LogicalType> &types, vector<string> &names);

// Simplified conversion: schema -> DuckDB ColumnList
// Use this when you only need column definitions (e.g., for CreateTableInfo)
// and don't need to keep the ArrowTableSchema for data conversion.
void ArrowSchemaToDuckDBTypes(ClientContext &context, const VgiSchemaPtr &schema,
                             ArrowSchemaWrapper &c_schema_out, ArrowTableSchema &arrow_table_out,
                             vector<LogicalType> &types, vector<Identifier> &names);

void ArrowSchemaToColumnList(ClientContext &context, const VgiSchemaPtr &schema, ColumnList &columns);

// Convert DuckDB ColumnList to Arrow Schema
// This is the inverse of ArrowSchemaToColumnList: takes DuckDB column definitions
// and produces an Arrow schema using DuckDB's ArrowConverter via the C ABI bridge.
VgiSchemaPtr DuckDBColumnsToArrowSchema(ClientContext &context, const ColumnList &columns,
                                       const std::string &owner_schema = "");

// ============================================================================
// DuckDB DataChunk / Type to Arrow Conversion
// ============================================================================

// Convert DuckDB DataChunk to a batch named after `schema`'s fields (types are DuckDB's
// ArrowConverter output for the chunk's types; a 0-row chunk yields an empty batch of `schema`)
VgiBatchPtr DataChunkToArrow(ClientContext &context, DataChunk &chunk, const VgiSchemaPtr &schema);

// Cached-input version of DataChunkToArrow for hot-path scalar-style callers
// where every batch shares the same logical types, names, extension types, and
// client properties. The caller maintains a small cache (typically on a per-
// thread local-state struct) and passes the four cached structures in by const
// ref. This skips the per-batch
//   * vector<LogicalType> + vector<string> push-backs,
//   * ClientProperties copy,
//   * ArrowTypeExtensionData::GetExtensionTypes() lookup
// that DataChunkToArrow does internally — they are all stable for the lifetime
// of the function call (e.g. a scalar's input schema never changes after bind).
//
// The caller is responsible for ensuring the cached vectors match `chunk`'s
// column layout. Calls with mismatched shapes are undefined behaviour (no
// runtime check — this is the cost of the fast path).
VgiBatchPtr DataChunkToArrowCached(
    ClientContext &context, DataChunk &chunk,
    const VgiSchemaPtr &schema,
    const vector<LogicalType> &cached_types,
    const vector<string> &cached_names,
    const ClientProperties &cached_client_props,
    const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &cached_extension_types);

// Build a schema from DuckDB logical types and names
VgiSchemaPtr BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                        const vector<string> &names);
VgiSchemaPtr BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                      const vector<Identifier> &names);

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
// whose schema is ``target_schema``:
//   * where every node has the same physical layout (only names, nullability flags,
//     metadata or a timestamp's timezone differ) the batch is relabeled zero-copy;
//   * otherwise the batch is rebuilt, converting the leaves that differ: timestamp /
//     time / duration unit rescale, integer width changes, string <-> large_string,
//     binary <-> large_binary, dictionary -> its value type, decimal width change.
//
// Fast path: when the schemas are equal (ignoring metadata) the input batch is
// returned unchanged.
//
// Throws IOException if a column cannot be converted to the target type (e.g.,
// genuinely incompatible logical types).
VgiBatchPtr ReconcileBatchToSchema(const VgiBatchPtr &batch, const VgiSchemaPtr &target_schema);

// ============================================================================
// DuckDB Value to Arrow Conversion
// ============================================================================

// Function arguments for the bind call: a one-row batch whose single column "args" is
// a struct with positional_0, positional_1, ..., named_<name> fields.
struct ArrowArguments {
	VgiBatchPtr batch;
	//! Rows of the arguments batch (1), or 0 when there are none.
	int64_t length() const {
		return batch ? batch->num_rows() : 0;
	}
	explicit operator bool() const {
		return batch != nullptr;
	}
};

// Convert DuckDB Values to Arrow arguments struct using ArrowAppender
// Creates a struct with fields named positional_0, positional_1, etc.
// Each field's type is preserved from the DuckDB Value type
ArrowArguments BuildArgumentsFromValues(ClientContext &context, const vector<Value> &positional_args,
                                        const vector<std::pair<string, Value>> &named_args = {});

// Build a settings RecordBatch from a map of setting name → DuckDB Value
// Creates a single-row RecordBatch where each field preserves its DuckDB type as Arrow type
// Uses DataChunk + ArrowAppender to ensure proper type conversion (same pattern as BuildArgumentsFromValues)
VgiBatchPtr BuildSettingsBatch(ClientContext &context, const std::map<std::string, Value> &settings);

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
FunctionArgumentTypes ParseFunctionArgumentSchema(ClientContext &context, const VgiSchemaPtr &schema);

} // namespace vgi
} // namespace duckdb
