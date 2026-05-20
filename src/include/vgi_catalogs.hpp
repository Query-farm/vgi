// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the vgi_catalogs table function
// Usage: SELECT * FROM vgi_catalogs('/path/to/vgi-worker')
// Returns a table with a single column "catalog" containing catalog names from the VGI worker
void RegisterVgiCatalogsFunction(ExtensionLoader &loader);

} // namespace duckdb
