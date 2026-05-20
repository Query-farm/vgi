// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {
namespace vgi {

// ============================================================================
// VGI Protocol Constants
// ============================================================================
// These constants define the metadata keys and values used in the VGI protocol
// for serializing function arguments and schema information via Arrow.

// ----------------------------------------------------------------------------
// Argument Metadata Keys
// ----------------------------------------------------------------------------
// Arrow schema metadata key that indicates the argument type
constexpr const char *VGI_ARG_METADATA_KEY = "vgi_arg";

// Value for VGI_ARG_METADATA_KEY indicating a named (keyword) argument
constexpr const char *VGI_ARG_NAMED_VALUE = "named";

// ----------------------------------------------------------------------------
// Argument Serialization Prefixes
// ----------------------------------------------------------------------------
// Prefix for positional arguments in serialized Arrow structs
// e.g., "positional_0", "positional_1"
constexpr const char *VGI_POSITIONAL_PREFIX = "positional_";

// Prefix for named arguments in serialized Arrow structs
// e.g., "named_increment", "named_format"
constexpr const char *VGI_NAMED_PREFIX = "named_";

// ----------------------------------------------------------------------------
// Type Metadata (for special argument types like table input)
// ----------------------------------------------------------------------------
// Arrow schema metadata key that indicates a special type
constexpr const char *VGI_TYPE_METADATA_KEY = "vgi_type";

// Value for VGI_TYPE_METADATA_KEY indicating a table input argument
// Table inputs are streaming RecordBatches passed via Stream 5
constexpr const char *VGI_TYPE_TABLE_VALUE = "table";

// Value for VGI_TYPE_METADATA_KEY indicating any Arrow type is accepted
constexpr const char *VGI_TYPE_ANY_VALUE = "any";

// ----------------------------------------------------------------------------
// Varargs Metadata
// ----------------------------------------------------------------------------
// Arrow schema metadata key indicating the function accepts varargs
constexpr const char *VGI_VARARGS_METADATA_KEY = "vgi_varargs";

// Value for VGI_VARARGS_METADATA_KEY indicating varargs are enabled
constexpr const char *VGI_VARARGS_TRUE_VALUE = "true";

// ----------------------------------------------------------------------------
// Const Parameter Metadata
// ----------------------------------------------------------------------------
// Arrow schema metadata key indicating the parameter is a constant (scalar value passed at bind time)
constexpr const char *VGI_CONST_METADATA_KEY = "vgi_const";

// Value for VGI_CONST_METADATA_KEY indicating the parameter is constant
constexpr const char *VGI_CONST_TRUE_VALUE = "true";

// ----------------------------------------------------------------------------
// Row ID Metadata
// ----------------------------------------------------------------------------
// Arrow field metadata key indicating the field is the table's row identifier.
// Presence of this key is sufficient — no value check needed.
constexpr const char *VGI_ROW_ID_METADATA_KEY = "is_row_id";

// ----------------------------------------------------------------------------
// Generated Column Metadata
// ----------------------------------------------------------------------------
// Arrow field metadata key indicating the field is a generated (virtual) column.
// Value is a SQL expression string (e.g., "x + y") that DuckDB evaluates on read.
constexpr const char *VGI_GENERATED_EXPRESSION_METADATA_KEY = "generated_expression";

} // namespace vgi
} // namespace duckdb
