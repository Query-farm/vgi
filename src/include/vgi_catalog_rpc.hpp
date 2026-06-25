// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Typed catalog RPC surface: every InvokeCatalog*/InvokeTableFunction* call,
// the DDL operations, statistics, and the SecretManager / CreateTableInfo
// helpers. Split out of vgi_catalog_api.hpp so that editing an RPC signature
// (the part that churns most) doesn't recompile pure-metadata consumers.
//
// This header legitimately carries the Arrow IPC umbrella (via vgi_rpc_types.hpp
// for TableFunctionCardinalityResult / InsertionOrderPreservingMap), so include
// it only from .cpp files that actually issue catalog RPCs.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

#include "vgi_attach_parameters.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {

class ClientContext;
struct CreateTableInfo;

namespace vgi {

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;
// Forward declaration — full definition in vgi_cookie_jar.hpp
class SessionCookieJar;

// Convert DuckDB OnCreateConflict enum to the string expected by VGI RPC protocol.
inline std::string MapOnConflict(OnCreateConflict conflict) {
	switch (conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		return "ERROR";
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return "IGNORE";
	case OnCreateConflict::REPLACE_ON_CONFLICT:
		return "REPLACE";
	default:
		return "ERROR";
	}
}

// ============================================================================
// Typed Catalog RPC Functions (vgi_rpc protocol)
// ============================================================================

// Invoke catalog_attach: attach to a catalog and get configuration
// Called BEFORE a catalog exists — takes individual parameters, not CatalogRpcContext.
//
// ``data_version_spec`` and ``implementation_version`` are pass-through semver
// strings the user supplied at ATTACH time (empty = unconstrained); the worker
// validates them and returns the resolved concrete versions in the result.
// ``cookie_jar`` is the per-catalog HTTP session cookie store (null for
// subprocess transport); if present, Set-Cookie response headers are captured
// into it and Cookie request headers are read from it on subsequent RPCs.
CatalogAttachResult InvokeCatalogAttach(const std::string &worker_path, const std::string &catalog_name,
                                        ClientContext &context, bool worker_debug = false, bool use_pool = true,
                                        const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                        const std::string &data_version_spec = "",
                                        const std::string &implementation_version = "",
                                        const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                                        const std::map<std::string, Value> &attach_options = {},
                                        std::optional<int64_t> launcher_idle_timeout_seconds = std::nullopt,
                                        std::optional<std::string> launcher_state_dir = std::nullopt);

// List catalogs exposed by a worker. Returns per-catalog discovery records
// carrying implementation_version and data_version_spec metadata alongside the
// catalog name.
//
// The two launcher_* trailing parameters mirror InvokeCatalogAttach: when an
// ATTACH carries `launcher_idle_timeout` / `launcher_state_dir`, those values
// must flow into the discovery RPC so the launcher cache is primed with the
// user's overrides — otherwise this RPC pins the cache with [defaults] and
// the subsequent real ATTACH (which does carry the overrides) trips the
// cache-conflict BinderException.
std::vector<VgiCatalogInfo> InvokeCatalogs(const std::string &worker_path, ClientContext &context,
                                           bool worker_debug = false, bool use_pool = true,
                                           const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                           std::optional<int64_t> launcher_idle_timeout_seconds = std::nullopt,
                                           std::optional<std::string> launcher_state_dir = std::nullopt);

// Invoke catalog_schemas: list schemas in an attached catalog
std::vector<VgiSchemaInfo> InvokeCatalogSchemas(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_schema_contents_tables: list tables in a schema
std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const CatalogRpcContext &ctx,
                                                            const std::string &schema_name, ClientContext &context);

// Invoke catalog_schema_contents_views: list views in a schema
std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const CatalogRpcContext &ctx,
                                                          const std::string &schema_name, ClientContext &context);

// Invoke catalog_schema_contents_macros: list macros in a schema
// macro_type: "SCALAR_MACRO" or "TABLE_MACRO"
std::vector<VgiMacroInfo> InvokeCatalogSchemaContentsMacros(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &macro_type, ClientContext &context);

// Invoke catalog_schema_contents_functions: list functions in a schema
// function_type: "SCALAR_FUNCTION" or "TABLE_FUNCTION"
std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &function_type, ClientContext &context);

// Invoke catalog_table_get: get a specific table's metadata
// Returns nullopt if table not found (empty response)
std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context);

// Invoke catalog_table_get with time-travel AT clause
std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name,
                                                   const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::string &at_unit,
                                                   const std::string &at_value);

// Invoke catalog_view_get: get a specific view's metadata
std::optional<VgiViewInfo> InvokeCatalogViewGet(const CatalogRpcContext &ctx,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context);

// Invoke catalog_table_scan_function_get: get scan function for a table
VgiScanFunctionResult InvokeCatalogTableScanFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit,
    const std::string &at_value);

// Invoke catalog_table_scan_branches_get: new-protocol multi-branch dispatch.
// Tries the new method first; on MethodNotImplemented falls back to
// InvokeCatalogTableScanFunctionGet and synthesises a one-branch result.
// All other exceptions propagate. See A7 in the plan.
VgiScanBranchesResult InvokeCatalogTableScanBranchesGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit,
    const std::string &at_value);

// Parse a ScanBranchesResult RecordBatch into the in-memory C++ struct.
// Loud-rejects empty branches lists and surfaces parse errors on
// branch_filter expressions as BinderException.
VgiScanBranchesResult ParseScanBranchesResult(ClientContext &context,
                                                const std::shared_ptr<arrow::RecordBatch> &batch,
                                                const std::string &worker_path);

// Write function discovery uses the same result type as scan function discovery.
using VgiWriteFunctionResult = VgiScanFunctionResult;

// Invoke catalog_table_{insert,update,delete}_function_get: get write function for a table
// `writable_branch_function_name` is set ONLY when the C++ side is
// dispatching an INSERT against a multi-branch VGI table that has a
// writable arm — in which case it carries the writable arm's
// `ScanBranch.function_name`, so the worker can dispatch without
// re-resolving which branch is writable. Empty string means single-branch
// (or worker-decided routing); workers serving single-branch tables can
// ignore the field.
VgiWriteFunctionResult InvokeCatalogTableInsertFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context,
    const std::optional<std::string> &writable_branch_function_name = std::nullopt);

VgiWriteFunctionResult InvokeCatalogTableUpdateFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context);

VgiWriteFunctionResult InvokeCatalogTableDeleteFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context);

// ============================================================================
// Transaction Lifecycle
// ============================================================================

// Invoke catalog_version: query the current catalog version from the worker.
// Returns the version number (int64). Returns 0 if the RPC is not implemented
// or fails (graceful fallback for older workers).
int64_t InvokeCatalogVersion(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_transaction_begin: begin a new transaction.
// Returns the transaction_opaque_data bytes from the worker (empty if not supported).
std::vector<uint8_t> InvokeCatalogTransactionBegin(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_transaction_commit: commit a transaction.
void InvokeCatalogTransactionCommit(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_transaction_rollback: rollback a transaction.
void InvokeCatalogTransactionRollback(const CatalogRpcContext &ctx, ClientContext &context);

// ============================================================================
// DDL Operations
// ============================================================================

// Invoke catalog_table_create: create a new table in the catalog.
void InvokeCatalogTableCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &columns_schema,
    const std::string &on_conflict,
    const std::vector<int> &not_null_constraints,
    const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints,
    const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    ClientContext &context);

// Invoke catalog_table_drop: drop a table from the catalog.
void InvokeCatalogTableDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context);

// Invoke catalog_table_rename: rename a table in the catalog.
void InvokeCatalogTableRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context);

// Invoke catalog_table_column_add: add a column to a table.
void InvokeCatalogTableColumnAdd(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    bool if_column_not_exists,
    ClientContext &context);

// Invoke catalog_table_column_drop: drop a column from a table.
void InvokeCatalogTableColumnDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    ClientContext &context);

// Invoke catalog_table_column_rename: rename a column in a table.
void InvokeCatalogTableColumnRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    ClientContext &context);

// Set or clear the comment on a table
void InvokeCatalogTableCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

// Set or clear the comment on a table column
void InvokeCatalogTableColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

// Invoke catalog_table_column_type_change: change a column's type in a table.
void InvokeCatalogTableColumnTypeChange(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    const std::string &expression,
    ClientContext &context);

// Invoke catalog_table_column_default_set: set a column's default expression.
void InvokeCatalogTableColumnDefaultSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, const std::string &expression,
    ClientContext &context);

// Invoke catalog_table_column_default_drop: drop a column's default expression.
void InvokeCatalogTableColumnDefaultDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// Invoke catalog_table_not_null_set: set NOT NULL constraint on a column.
void InvokeCatalogTableNotNullSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// Invoke catalog_table_not_null_drop: drop NOT NULL constraint from a column.
void InvokeCatalogTableNotNullDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// ============================================================================
// View DDL Operations
// ============================================================================

// Invoke catalog_view_create: create a new view in the catalog.
void InvokeCatalogViewCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &definition, const std::string &on_conflict,
    ClientContext &context);

// Invoke catalog_view_drop: drop a view from the catalog.
void InvokeCatalogViewDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context);

// Invoke catalog_view_rename: rename a view in the catalog.
void InvokeCatalogViewRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context);

// Set or clear the comment on a view
void InvokeCatalogViewCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

// Invoke catalog_schema_create: create a new schema in the catalog.
void InvokeCatalogSchemaCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &on_conflict,
    ClientContext &context);

// Invoke catalog_schema_drop: drop a schema from the catalog.
void InvokeCatalogSchemaDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, bool ignore_not_found, bool cascade,
    ClientContext &context);

// ============================================================================
// Column Statistics
// ============================================================================

// Invoke catalog_table_column_statistics_get: fetch column statistics for a table.
// Returns a map from column_name to BaseStatistics, empty map on failure.
// Also returns the cache_max_age_seconds via the output parameter (-1 = cache forever).
struct ColumnStatisticsRpcResult {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> stats;
	int64_t cache_max_age_seconds = -1;  // -1 = cache forever, 0 = don't cache
};

ColumnStatisticsRpcResult InvokeCatalogTableColumnStatisticsGet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    ClientContext &context);

// Deserialize a column-statistics IPC RecordBatch into a per-column
// `BaseStatistics` map. Reused on three paths:
//   - the on-demand `catalog_table_column_statistics_get` RPC response
//     (`vgi_table_entry.cpp`)
//   - the per-bind `table_function_statistics` RPC response
//     (`vgi_table_function_impl.cpp`)
//   - inlined `column_statistics` bytes carried on `VgiTableInfo`, parsed at
//     first `GetStatistics` call by `VgiTableEntry`
// Throws on malformed input; callers wrap in try/catch when fail-soft is
// desired (the inline-blob path leaves the cache empty + marks fetched).
std::unordered_map<std::string, unique_ptr<BaseStatistics>> ParseColumnStatisticsBatch(
    const std::shared_ptr<arrow::RecordBatch> &result_batch,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &worker_path, const std::string &log_source_key,
    const std::string &log_source_value, ClientContext &context);

// ============================================================================
// Table Function Cardinality
// ============================================================================

// Invoke table_function_cardinality RPC: get cardinality estimate for a table function.
// Uses the serialized BindRequest bytes from a completed bind phase.
// Returns TableFunctionCardinalityResult with estimate and max (-1 = unknown).
TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context);

// Invoke table_function_statistics RPC: get per-output-column statistics for a table function.
// Wire shape mirrors table_function_cardinality — the worker receives a full copy of the
// BindRequest (with parsed user arguments) and returns per-column stats in the same shape
// as catalog_table_column_statistics_get. Returns an empty map if the worker returns null
// (stats unknown) or on parse errors; no cache_max_age_seconds — bind-lifetime cache only.
std::unordered_map<std::string, unique_ptr<BaseStatistics>> InvokeTableFunctionStatistics(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &function_name, ClientContext &context);

// Invoke table_function_dynamic_to_string RPC: ask the worker for diagnostics that should
// surface as Extra Info under EXPLAIN ANALYZE. Fired once per parallel scan thread at
// end-of-stream; the user implementation is responsible for persisting whatever metrics
// it cares about and retrieving them by ``global_execution_id``. Catches and logs any
// exception; returns an empty map so EXPLAIN ANALYZE never aborts the query.
InsertionOrderPreservingMap<std::string> InvokeTableFunctionDynamicToString(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<uint8_t> &global_execution_id,
    ClientContext &context);

// ============================================================================
// Secret Extraction from DuckDB SecretManager
// ============================================================================

// Extract secrets from DuckDB's SecretManager based on function requirements.
// For each VgiSecretRequirement, looks up the secret by type (required),
// optionally filtered by name and/or scope.
// Returns map of secret_type → {key → Value}. Missing secrets are skipped.
std::map<std::string, std::map<std::string, Value>> ExtractVgiSecrets(
    ClientContext &context, const std::vector<VgiSecretRequirement> &requirements);

// ============================================================================
// DuckDB Type Conversion
// ============================================================================

// Convert VgiTableInfo to DuckDB CreateTableInfo.
// This handles converting the Arrow schema to DuckDB column definitions.
CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, VgiTableInfo &table_info,
                                            const std::string &schema_name);

} // namespace vgi
} // namespace duckdb
