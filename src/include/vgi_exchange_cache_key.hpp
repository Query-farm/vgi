// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "duckdb/common/types/value.hpp"

// Arrow forward-declaration (the .cpp deserializes/serializes; keep this header light).
namespace arrow {
class RecordBatch;
} // namespace arrow

namespace duckdb {

class ClientContext;
class DataChunk;

namespace vgi {

struct VgiResultCacheKey;      // vgi_result_cache.hpp
struct VgiTableInOutBindData;  // vgi_table_in_out_impl.hpp

// ============================================================================
// Canonical key-component serializers (shared with the producer path)
// ============================================================================
// These MUST produce byte-identical output for producer- and exchange-mode keys,
// so a single definition lives here (was static in vgi_table_function_impl.cpp).

//! Canonical (sorted-key) serialization of the settings map for the cache key.
std::string SerializeSettingsForKey(const std::map<std::string, Value> &settings);

//! Canonical serialization of the projection id list for the cache key.
std::string SerializeProjectionForKey(const std::vector<int32_t> &projection_ids);

// ============================================================================
// Input-hash helpers for EXCHANGE-mode cache keys
// ============================================================================

//! Order-DEPENDENT hash of one input batch: SHA-256 over its deterministic Arrow
//! IPC-stream framing. For streaming table-in-out maps, whose output is
//! positionally aligned to input (identical ordered bytes == identical result).
//! Divergent-but-equal framing only ever causes a miss (safe), never a wrong hit.
std::string HashInputBatchOrdered(const std::shared_ptr<arrow::RecordBatch> &input_batch);

//! Order-INDEPENDENT, multiset-correct hash of a DataChunk: per-row canonical
//! sort-key blobs (CreateSortKey — NULL-aware, all types), sorted, then SHA-256
//! over (count + length-prefixed blobs). For correlated LATERAL, whose decorrelated
//! input is an unordered multiset and whose operator is NO_ORDER. Hash the FULL
//! chunk (all columns) — the correlated columns are baked into the cached output.
std::string HashInputChunkUnordered(ClientContext &context, DataChunk &chunk);

// ============================================================================
// Static cache-key builder for EXCHANGE-mode functions
// ============================================================================

//! Build the STATIC portion of an exchange-mode cache key from a table-in-out
//! bind data (mirrors the producer EvaluateCacheEligibility, minus the
//! producer-only filter/order/sample components). `key.input_hash` is left EMPTY —
//! the caller sets it per memoization event. Returns false + sets `*reason` when
//! ineligible (global switch off, per-catalog opt-out, unresolved identity,
//! unknown catalog version). On true, fills `key`, `catalog_name`, `catalog_version`.
//!
//! SECURITY: identity_scope folds in the caller's auth principal
//! (BuildCatalogIdentityScope); "" fails closed so two identities never cross-serve.
bool BuildExchangeCacheKeyStatic(ClientContext &context, const VgiTableInOutBindData &bd,
                                 const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                 std::string &catalog_name, int64_t &catalog_version, const char *&reason);

} // namespace vgi
} // namespace duckdb
