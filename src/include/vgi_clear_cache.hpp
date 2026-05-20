// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_clear_cache() table function
void RegisterVgiClearCacheFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
