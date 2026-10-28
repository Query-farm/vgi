// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "vgi_arrow.hpp"
namespace duckdb {
class ClientContext;
namespace vgi {
struct CatalogRpcContext;

inline constexpr const char *VGI_QUERY_ENCODING = "duckdb.sql.v1";
inline constexpr const char *VGI_QUERY_EXPLICIT_PROFILE = "vgi.remote.readonly.v1";
inline constexpr const char *VGI_QUERY_EXECUTOR = "__vgi_catalog_query";
inline constexpr int64_t VGI_QUERY_MAX_SQL_BYTES = 1024 * 1024;
inline constexpr int64_t VGI_QUERY_MAX_SCHEMA_BYTES = 1024 * 1024;
inline constexpr int64_t VGI_QUERY_MAX_RECORD_BYTES = 8 * 1024 * 1024;
inline constexpr int64_t VGI_QUERY_MAX_TOKEN_BYTES = 64 * 1024;

struct VgiQueryEvaluationContext {
	std::string timezone = "UTC";
	std::string collation = "binary";
	std::string default_order = "asc";
	std::string default_null_order = "nulls_last";
	bool preserve_insertion_order = true;
};

struct VgiQueryCapabilities {
	std::string encoding = VGI_QUERY_ENCODING;
	std::vector<std::string> semantic_profiles {VGI_QUERY_EXPLICIT_PROFILE};
	std::vector<std::string> execution_modes {"explicit"};
	std::string backend_engine;
	std::string backend_version;
	std::string capability_revision;
	VgiQueryEvaluationContext default_evaluation_context;
	int64_t max_query_bytes = VGI_QUERY_MAX_SQL_BYTES;
};

struct VgiQueryPrepareResult {
	bool accepted = false;
	std::string reason_code;
	std::string reason_detail;
	std::vector<uint8_t> prepared_query_token;
	VgiSchemaPtr output_schema;
	std::vector<uint8_t> output_schema_ipc;
	std::string result_ordering;
	int64_t expires_at_epoch_ms = 0;
	std::optional<int64_t> validated_catalog_version;
	std::optional<std::string> validated_catalog_etag;
	std::optional<int64_t> estimated_rows;
	std::optional<int64_t> estimated_bytes;
};

bool IsVgiQueryEnabled(ClientContext &context);
int64_t QueryEpochMillisNow();
std::vector<uint8_t> QueryEvaluationContextDigest(const VgiQueryEvaluationContext &context);
VgiBatchPtr BuildClientQueryCapabilities();
std::optional<VgiQueryCapabilities> ParseVgiQueryCapabilities(const VgiColumn &array,
                                                            bool offered, const std::string &worker_path);
VgiBatchPtr BuildVgiQueryPrepareRequest(
    const CatalogRpcContext &ctx, const VgiQueryCapabilities &capabilities, const std::string &sql,
    const VgiSchemaPtr &parameter_schema, const std::string &required_ordering = "query_order");
VgiBatchPtr DeserializeVgiQueryRecord(const std::vector<uint8_t> &bytes);
VgiQueryPrepareResult ParseVgiQueryPrepareResult(const VgiBatchPtr &batch,
                                              const VgiQueryCapabilities &capabilities,
                                              const std::string &required_ordering, int64_t now_epoch_ms,
                                              const std::string &worker_path);
VgiQueryPrepareResult InvokeCatalogQueryPrepare(const CatalogRpcContext &ctx,
                                              const VgiQueryCapabilities &capabilities, const std::string &sql,
                                              const VgiSchemaPtr &parameter_schema,
                                              ClientContext &context,
                                              const std::string &required_ordering = "query_order");
void InvokeCatalogQueryRelease(const CatalogRpcContext &ctx, const std::vector<uint8_t> &prepared_query_token,
                               ClientContext &context);

} // namespace vgi
} // namespace duckdb
