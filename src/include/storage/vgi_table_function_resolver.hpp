// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <string>

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class Catalog;
class ClientContext;
class TableFunctionCatalogEntry;

namespace vgi {

//! One function a VGI table descriptor names: its scan (single-branch scan or a
//! multi-branch arm) or one of its write functions.
struct VgiTableFunctionRequest {
	//! The VGI catalog that holds the table.
	Catalog *catalog = nullptr;
	//! The table's own schema and name (diagnostics name the table).
	std::string table_schema;
	std::string table_name;
	//! "scan", "insert", "update" or "delete" (diagnostics only).
	const char *role = "scan";
	std::string function_name;
	//! Schema the worker says the function lives in (ScanBranch / ScanFunctionResult
	//! schema_path); empty when the worker did not say.
	std::string worker_schema;
	//! The catalog's default schema; empty when it has none.
	std::string default_schema;
	//! Whether a function the worker does not list may resolve to a DuckDB built-in
	//! in the system catalog (scan delegation to read_parquet, iceberg_scan, ...).
	//! False for write functions, which are always worker RPCs.
	bool allow_system_catalog = true;
	//! A format branch's reader (read_parquet, iceberg_scan, ...): a DuckDB
	//! function, never a worker's table_only one, so the hidden sets are skipped.
	bool format_reader = false;
};

struct VgiResolvedTableFunction {
	optional_ptr<TableFunctionCatalogEntry> entry;
	//! The VGI schema it resolved in (the bind request names it); empty for a
	//! system-catalog function.
	std::string schema_name;
	bool from_system_catalog = false;
	//! Resolved from a schema's hidden (protocol 3.3 table_only) set.
	bool hidden = false;
};

//! The ONE resolver for the functions a VGI table names. Single-branch scans
//! (VgiTableEntry::GetScanFunctionImpl), multi-branch arms
//! (vgi_multi_scan_rewriter.cpp) and writes (vgi_physical_write.cpp) all use it,
//! so a function's capabilities (projection/filter/sampling pushdown,
//! required_secrets, late materialization, partitioning, ordering, filter
//! profiles, has_finalize, ...) come from the same listing entry everywhere.
//!
//! Order:
//!   1. the visible entries: the worker-claimed schema (only when it names a third
//!      schema, neither the table's nor the default; see the comment in
//!      GetScanFunctionImpl about cache/filter_pushdown_keys.test), the table's
//!      schema, then the catalog's default schema;
//!   2. the system catalog, when request.allow_system_catalog (before the hidden
//!      sets, so a DuckDB built-in never forces a function listing RPC);
//!   3. the hidden (table_only) set of those same schemas, in the same order.
//! A format branch's reader (request.format_reader) skips step 3.
//!
//! Throws a BinderException naming the worker, catalog, table and function when
//! nothing resolves: the worker must list every function its tables name. There
//! is no silent fallback to "no capabilities".
VgiResolvedTableFunction ResolveVgiTableFunction(ClientContext &context, const VgiTableFunctionRequest &request);

} // namespace vgi
} // namespace duckdb
