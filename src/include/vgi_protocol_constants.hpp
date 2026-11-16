// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {
namespace vgi {

// Arrow metadata keys and values for VGI function arguments and schemas.

// Argument kind: VGI_ARG_METADATA_KEY = "named" marks a keyword argument.
constexpr const char *VGI_ARG_METADATA_KEY = "vgi_arg";
constexpr const char *VGI_ARG_NAMED_VALUE = "named";

// Field-name prefixes in serialized argument structs ("positional_0", "named_format").
constexpr const char *VGI_POSITIONAL_PREFIX = "positional_";
constexpr const char *VGI_NAMED_PREFIX = "named_";

// Special argument types: a table input, or any Arrow type accepted.
constexpr const char *VGI_TYPE_METADATA_KEY = "vgi_type";
constexpr const char *VGI_TYPE_TABLE_VALUE = "table";
constexpr const char *VGI_TYPE_ANY_VALUE = "any";

// The function accepts varargs.
constexpr const char *VGI_VARARGS_METADATA_KEY = "vgi_varargs";
constexpr const char *VGI_VARARGS_TRUE_VALUE = "true";

// The parameter is a constant (a scalar value passed at bind time).
constexpr const char *VGI_CONST_METADATA_KEY = "vgi_const";
constexpr const char *VGI_CONST_TRUE_VALUE = "true";

// Per-argument description (UTF-8), absent when undocumented. The `vgi_doc_*`
// prefix is reserved for future doc variants.
constexpr const char *VGI_DOC_METADATA_KEY = "vgi_doc";

// Per-argument constraints surfaced through vgi_function_arguments(); all
// presence-only and UTF-8 encoded:
//   vgi_default — JSON scalar (the argument's default value)
//   vgi_choices — JSON array (the closed set of allowed values)
//   vgi_range   — interval notation built from ge/le/gt/lt (e.g. "[0, 100]")
//   vgi_pattern — raw regex the value must match
// Must match the writer in vgi-python (vgi/argument_spec.py) byte for byte.
constexpr const char *VGI_DEFAULT_METADATA_KEY = "vgi_default";
constexpr const char *VGI_CHOICES_METADATA_KEY = "vgi_choices";
constexpr const char *VGI_RANGE_METADATA_KEY = "vgi_range";
constexpr const char *VGI_PATTERN_METADATA_KEY = "vgi_pattern";

// On a SecretTypeSpec's parameters schema, surfaced through vgi_secret_types():
//   field  vgi_doc      — what the value is and where to find it (VGI_DOC_METADATA_KEY)
//   field  vgi_required — "true" when CREATE SECRET must supply the key
//   field  redact       — "true" to mask the value in SHOW SECRETS (pre-existing)
//   schema vgi_doc_url  — page explaining how to obtain the credentials
// Presence-only and UTF-8, like the per-argument keys above.
constexpr const char *VGI_REQUIRED_METADATA_KEY = "vgi_required";
constexpr const char *VGI_DOC_URL_METADATA_KEY = "vgi_doc_url";

// Field metadata marking the table's row identifier (presence-only).
constexpr const char *VGI_ROW_ID_METADATA_KEY = "is_row_id";

// Field metadata marking a generated column; the value is its SQL expression.
constexpr const char *VGI_GENERATED_EXPRESSION_METADATA_KEY = "generated_expression";

} // namespace vgi
} // namespace duckdb
