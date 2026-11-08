// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_secret_types() discovery table function.
//!
//! One row per secret type each attached VGI catalog declares: its description,
//! where to obtain the credentials (vgi_doc_url), every CREATE SECRET parameter
//! (type, vgi_required, redact, vgi_doc), and the functions that require it.
//! duckdb_secret_types() has a fixed shape and keeps only the type name.
//! See vgi_secret_types_function.cpp.
void RegisterVgiSecretTypesFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
