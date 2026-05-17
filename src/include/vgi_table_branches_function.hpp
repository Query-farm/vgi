#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_table_branches() diagnostic table function.
//!
//! Returns one row per (catalog_name, schema_name, table_name, branch_index)
//! across every attached VGI catalog. Used to introspect the multi-branch
//! shape that VgiMultiScanRewriter consumes. See vgi_table_branches_function.cpp.
void RegisterVgiTableBranchesFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
