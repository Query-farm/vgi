// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the vgi_protocols table function:
//   SELECT * FROM vgi_protocols('/path/to/vgi-worker');
//   SELECT * FROM vgi_protocols(catalog := 'attached_alias');
// One row per protocol the worker hosts (vgi_rpc.Reflection.v1 list_protocols).
void RegisterVgiProtocolsFunction(ExtensionLoader &loader);

} // namespace duckdb
