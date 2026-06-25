// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_function_arguments() diagnostic table function.
//!
//! Returns one row per (catalog, schema, function, argument) across every
//! attached VGI catalog. Surfaces per-argument metadata that
//! duckdb_functions() flattens away — named-vs-positional, const, varargs,
//! table-input, any-type — plus the per-argument description (vgi_doc).
//! See vgi_function_arguments_function.cpp.
void RegisterVgiFunctionArgumentsFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
