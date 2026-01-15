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
// Varargs Metadata
// ----------------------------------------------------------------------------
// Arrow schema metadata key indicating the function accepts varargs
constexpr const char *VGI_VARARGS_METADATA_KEY = "vgi_varargs";

// Value for VGI_VARARGS_METADATA_KEY indicating varargs are enabled
constexpr const char *VGI_VARARGS_TRUE_VALUE = "true";

} // namespace vgi
} // namespace duckdb
