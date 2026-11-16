// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register vgi_table_branches(): one row per (catalog, schema, table,
//! branch_index) across every attached VGI catalog.
void RegisterVgiTableBranchesFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
