#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_clear_cache() table function
void RegisterVgiClearCacheFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
