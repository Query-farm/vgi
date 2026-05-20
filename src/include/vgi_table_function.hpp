// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the vgi_table_function table function
// Usage: SELECT * FROM vgi_table_function('worker_path', 'function_name', [arg1, arg2, ...])
// Returns results from invoking the specified VGI table function
void RegisterVgiTableFunction(ExtensionLoader &loader);

} // namespace duckdb
