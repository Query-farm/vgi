// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// Typed catalog RPC surface: InvokeCatalog* / InvokeTableFunction*, DDL,
// statistics, and the SecretManager / CreateTableInfo helpers. It pulls in
// vgi_rpc_types.hpp, so include it only from .cpp files that issue catalog RPCs.

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
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {

class ClientContext;
struct CreateTableInfo;

namespace vgi {

class CatalogAuth;      // vgi_oauth.hpp
class SessionCookieJar; // vgi_cookie_jar.hpp

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

// catalog_attach. Runs before the catalog exists, so it takes individual
// parameters rather than a CatalogRpcContext. data_version_spec /
// implementation_version are the user's ATTACH semver constraints (empty =
// unconstrained); the worker returns the resolved versions. cookie_jar (HTTP
// only) captures Set-Cookie and supplies Cookie on later RPCs.
// Negotiates the protocol version first (docs/protocol_versions.md): over a
// byte transport the worker's ranges come from its reflection answer before
// catalog_attach; over HTTP from VGI-Protocol-Versions on the capability probe.
// Disjoint ranges fail with the "protocol_version_unsupported" ATTACH error
// naming both; a refusal is retried once at the highest common version
// (catalog_attach is idempotent). session_out, when non-null, receives the
// endpoint's ProtocolSession (versions and capabilities) for the attachment.
CatalogAttachResult InvokeCatalogAttach(const std::string &worker_path, const std::string &catalog_name,
                                        ClientContext &context, bool worker_debug = false, bool use_pool = true,
                                        const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                        const std::string &data_version_spec = "",
                                        const std::string &implementation_version = "",
                                        const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                                        const std::map<std::string, Value> &attach_options = {},
                                        std::optional<int64_t> launcher_idle_timeout_seconds = std::nullopt,
                                        std::optional<std::string> launcher_state_dir = std::nullopt,
                                        std::shared_ptr<void> worker_artifact_anchor = nullptr,
                                        const std::string &tcp_proxy = "",
                                        std::shared_ptr<IrohClientConfig> iroh = nullptr,
                                        std::shared_ptr<ProtocolSession> *session_out = nullptr);

// Tell the worker the attachment was detached and may be released.
void InvokeCatalogDetach(const CatalogRpcContext &ctx, ClientContext &context);

// List the worker's catalogs (with implementation_version / data_version_spec).
// The launcher_* parameters MUST carry the ATTACH's overrides: this RPC primes
// the launcher cache, and priming it with defaults makes the real ATTACH fail
// with the cache-conflict BinderException.
std::vector<VgiCatalogInfo> InvokeCatalogs(const std::string &worker_path, ClientContext &context,
                                           bool worker_debug = false, bool use_pool = true,
                                           const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                           std::optional<int64_t> launcher_idle_timeout_seconds = std::nullopt,
                                           std::optional<std::string> launcher_state_dir = std::nullopt,
                                           std::shared_ptr<void> worker_artifact_anchor = nullptr,
                                           const std::string &tcp_proxy = "",
                                           std::shared_ptr<IrohClientConfig> iroh = nullptr);

// One uncached vgi_rpc.Reflection.v1/list_protocols call, returning the one-row
// ProtocolList payload; use the vgi_reflection.hpp helpers instead. A worker
// without reflection throws VgiRpcException (protocol_not_supported, or
// method_not_implemented from a server that predates routing keys).
// response_metadata, when non-null, receives the reply's custom metadata
// (vgi_rpc.protocol_versions rides there).
VgiBatchPtr InvokeReflectionListProtocols(const CatalogRpcContext &ctx, ClientContext &context,
                                          VgiMetadataPtr *response_metadata = nullptr);

// One unary call to a non-vgi.v3 protocol (routing key and, when non-empty,
// version from `protocol`). Returns the one-row payload of the `result`
// envelope. Worker errors throw VgiRpcException with error_kind / error_code.
VgiBatchPtr InvokeProtocolUnary(const CatalogRpcContext &ctx, ClientContext &context,
                                const VgiProtocolId &protocol, const std::string &method_name,
                                const VgiBatchPtr &params, VgiMetadataPtr *response_metadata = nullptr);

std::vector<VgiSchemaInfo> InvokeCatalogSchemas(const CatalogRpcContext &ctx, ClientContext &context);

// catalog_contents: every schema and its contents in one RPC. Only when the
// attach result sets supports_catalog_contents. With `if_none_match` (an earlier
// etag) the worker may answer not_modified with no schemas.
VgiCatalogContents InvokeCatalogContents(const CatalogRpcContext &ctx, ClientContext &context,
                                         const std::optional<std::string> &if_none_match = std::nullopt);

// Validate and index every row/batch of the six embedded metadata streams.
VgiCatalogContents ParseCatalogContents(const VgiBatchPtr &batch,
                                       const std::string &worker_path,
                                       const std::optional<std::string> &if_none_match = std::nullopt);

// The `vgi_catalog_contents` setting (default true); false disables
// catalog_contents for both load and revalidation.
bool UseCatalogContents(ClientContext &context);

// Lazy decoders for a catalog_contents schema. function_type / macro_type use the
// RPC spellings (SCALAR_FUNCTION, TABLE_MACRO, ...).
VgiSchemaInfo DecodeContentsSchemaInfo(const VgiSchemaContents &contents);

std::vector<VgiTableInfo> DecodeContentsTables(const VgiSchemaContents &contents, ClientContext &context);
std::vector<VgiViewInfo> DecodeContentsViews(const VgiSchemaContents &contents);
std::vector<VgiFunctionInfo> DecodeContentsFunctions(const VgiSchemaContents &contents,
                                                     const std::string &function_type);
std::vector<VgiMacroInfo> DecodeContentsMacros(const VgiSchemaContents &contents, const std::string &macro_type);

std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const CatalogRpcContext &ctx,
                                                            const std::string &schema_name, ClientContext &context);

std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const CatalogRpcContext &ctx,
                                                          const std::string &schema_name, ClientContext &context);

// macro_type: "SCALAR_MACRO" or "TABLE_MACRO"
std::vector<VgiMacroInfo> InvokeCatalogSchemaContentsMacros(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &macro_type, ClientContext &context);

std::vector<VgiIndexInfo> DecodeContentsIndexes(const VgiSchemaContents &contents);
std::vector<VgiIndexInfo> InvokeCatalogSchemaContentsIndexes(const CatalogRpcContext &ctx,
    const std::string &schema_name, ClientContext &context);
std::optional<VgiIndexInfo> InvokeCatalogIndexGet(const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &name, ClientContext &context);
void InvokeCatalogIndexCreate(const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &name, const std::vector<uint8_t> &definition, const std::string &on_conflict,
    ClientContext &context);
void InvokeCatalogIndexDrop(const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &name, bool ignore_not_found, bool cascade, ClientContext &context);
void RequireCatalogIndexAlgorithm(const CatalogRpcContext &ctx, const std::string &schema,
    const std::string &table, const std::string &algorithm, ClientContext &context);

// function_type: "SCALAR_FUNCTION" or "TABLE_FUNCTION"
std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &function_type, ClientContext &context);

// Catalog-level custom COPY ... FROM formats (empty when none). Does not catch
// method_not_implemented: the ATTACH call site treats that as "no formats".
std::vector<VgiCopyFromFormatInfo> InvokeCatalogCopyFromFormats(const CatalogRpcContext &ctx, ClientContext &context);

// nullopt when the table is not found (empty response).
std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context);

// With a time-travel AT clause.
std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name,
                                                   const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::string &at_unit,
                                                   const std::string &at_value);

std::optional<VgiViewInfo> InvokeCatalogViewGet(const CatalogRpcContext &ctx,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context);

// The only scan-discovery RPC (protocol 3.3); a MethodNotImplemented answer is a
// protocol-violation IOException. `pushdown_filters`: an advisory VGI Filter
// Encoding v2 snapshot over the declared columns, sent for
// scan_planning='filtered' tables.
VgiScanBranchesResult InvokeCatalogTableScanBranchesGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit,
    const std::string &at_value, const std::optional<std::vector<uint8_t>> &pushdown_filters = std::nullopt);

// Rejects an empty branches list; branch_filter parse errors throw
// BinderException.
VgiScanBranchesResult ParseScanBranchesResult(ClientContext &context,
                                                const VgiBatchPtr &batch,
                                                const std::string &worker_path);

using VgiWriteFunctionResult = VgiScanFunctionResult;

// catalog_table_{insert,update,delete}_function_get. writable_branch_function_name
// is set only for an INSERT into a multi-branch table with a writable arm: that
// arm's ScanBranch.function_name, so the worker need not re-resolve it.
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

// Transaction lifecycle.

// 0 ("unknown version") when the worker returns no row or answers
// method_not_implemented; every other failure throws.
int64_t InvokeCatalogVersion(const CatalogRpcContext &ctx, ClientContext &context);

// Returns transaction_opaque_data (empty when the worker returns none).
std::vector<uint8_t> InvokeCatalogTransactionBegin(const CatalogRpcContext &ctx, ClientContext &context);

void InvokeCatalogTransactionCommit(const CatalogRpcContext &ctx, ClientContext &context);

void InvokeCatalogTransactionRollback(const CatalogRpcContext &ctx, ClientContext &context);

// DDL.

// Returns the reply's `created`: false when on_conflict IGNORE (IF NOT EXISTS)
// found an existing table.
bool InvokeCatalogTableCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const VgiSchemaPtr &columns_schema,
    const std::string &on_conflict,
    const std::vector<std::vector<uint8_t>> &constraint_definitions,
    ClientContext &context);

void InvokeCatalogTableConstraintsApply(const CatalogRpcContext &ctx, const VgiTableInfo &table,
                                      const std::vector<uint8_t> &change, ClientContext &context);

void InvokeCatalogTableDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context);

void InvokeCatalogTableRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context);

void InvokeCatalogTableColumnAdd(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const VgiSchemaPtr &column_definition,
    bool if_column_not_exists,
    ClientContext &context);

void InvokeCatalogTableColumnDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    ClientContext &context);

void InvokeCatalogTableColumnRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    ClientContext &context);

void InvokeCatalogTableCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

void InvokeCatalogTableColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

void InvokeCatalogTableColumnTypeChange(const CatalogRpcContext &ctx, const std::string &schema_name,
                                        const std::string &table_name,
                                        const VgiSchemaPtr &column_definition,
                                        const std::optional<std::vector<uint8_t>> &expression, ClientContext &context);

void InvokeCatalogTableColumnDefaultSet(const CatalogRpcContext &ctx, const std::string &schema_name,
                                        const std::string &table_name, const std::string &column_name,
                                        const std::vector<uint8_t> &expression, ClientContext &context);

void InvokeCatalogTableColumnDefaultDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

void InvokeCatalogTableNotNullSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

void InvokeCatalogTableNotNullDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// View and schema DDL.
void InvokeCatalogViewCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &definition, const std::vector<std::string> &aliases, const std::string &on_conflict,
    const std::optional<std::string> &comment, const std::map<std::string, std::string> &tags,
    const std::map<std::string, std::string> &column_comments,
    ClientContext &context);

void InvokeCatalogViewDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context);

void InvokeCatalogViewRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context);

void InvokeCatalogViewColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name, const std::string &column_name,
    const std::string &comment, bool comment_is_null, bool ignore_not_found,
    ClientContext &context);

void InvokeCatalogViewCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

void InvokeCatalogSchemaCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &on_conflict,
    const std::optional<std::string> &comment, const std::map<std::string, std::string> &tags,
    ClientContext &context);

void InvokeCatalogSchemaDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, bool ignore_not_found, bool cascade,
    ClientContext &context);

// Column statistics.

// column_name -> BaseStatistics (empty when the worker has none), plus the
// cache_max_age_seconds from the batch's custom metadata.
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

// Shared decoder for catalog_table_column_statistics_get, table_function_statistics
// and the inlined VgiTableInfo column_statistics. Returns an empty map when a
// required field is missing and skips (logs) rows that fail to parse; Arrow
// conversion errors throw, so fail-soft callers wrap it.
std::unordered_map<std::string, unique_ptr<BaseStatistics>> ParseColumnStatisticsBatch(
    const VgiBatchPtr &result_batch,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &worker_path, const std::string &log_source_key,
    const std::string &log_source_value, ClientContext &context);

// Table-function plan / cardinality / statistics / dynamic_to_string, all keyed
// on the serialized BindRequest bytes of a completed bind.

//! Plan a table-function scan into named, independently redeemable splits.
//! Bounded by a page cap, a total split cap and a per-page cancellation check;
//! a breach throws rather than scanning a partial enumeration.
VgiScanPlan InvokeTableFunctionPlan(const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
                                    const std::vector<uint8_t> &bind_opaque_data,
                                    const std::vector<int32_t> &projection_ids,
                                    const std::vector<uint8_t> &pushdown_filters, int64_t min_splits,
                                    int64_t target_split_bytes, const std::string &function_name,
                                    ClientContext &context);

//! Parse one PlanResponse batch into a page.
VgiScanPlanPage ParseVgiScanPlanPage(const VgiBatchPtr &batch, const std::string &worker_path);

// Estimate and max (-1 = unknown).
TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context);

// Per-output-column stats in the catalog_table_column_statistics_get shape. Empty
// when the worker returns null (see ParseColumnStatisticsBatch for malformed
// input). No cache_max_age_seconds: cached for the bind's lifetime only.
std::unordered_map<std::string, unique_ptr<BaseStatistics>> InvokeTableFunctionStatistics(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &function_name, ClientContext &context);

// EXPLAIN ANALYZE Extra Info from the worker, which must look up its own metrics
// by global_execution_id. Catches and logs every exception (returning an empty
// map) so EXPLAIN ANALYZE never aborts the query.
InsertionOrderPreservingMap<std::string> InvokeTableFunctionDynamicToString(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<uint8_t> &global_execution_id,
    ClientContext &context);

// Resolve each requirement from DuckDB's SecretManager: by name (type must
// match) when named, else by scope + type. Returns secret name (the type for an
// unnamed secret) -> {key -> Value}; missing secrets are skipped.
std::map<std::string, std::map<std::string, Value>> ExtractVgiSecrets(
    ClientContext &context, const std::vector<VgiSecretRequirement> &requirements);

CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, VgiTableInfo &table_info,
                                            const std::string &schema_name);

} // namespace vgi
} // namespace duckdb
