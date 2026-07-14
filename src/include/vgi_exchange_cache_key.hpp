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
struct VgiResultCacheEntry;    // vgi_result_cache.hpp
struct VgiCachedBatch;         // vgi_result_cache.hpp
struct VgiCacheControl;        // vgi_cache_control.hpp
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

//! Order-INDEPENDENT streaming digest for whole-input keying (buffered functions):
//! folds a chunk's per-row hashes into a two-lane 64-bit ADDITIVE accumulator (+ row
//! count). Additive => associative + commutative => independent of both chunk order
//! and the (parallel) thread that folded each chunk, and duplicate-preserving (unlike
//! XOR). Partial accumulators merge by field-wise addition. FinalizeInputDigest
//! renders the accumulator to a stable hex string for VgiResultCacheKey::input_hash.
void AccumulateInputDigest(DataChunk &chunk, uint64_t &sum_lo, uint64_t &sum_hi, uint64_t &row_count);
std::string FinalizeInputDigest(uint64_t sum_lo, uint64_t sum_hi, uint64_t row_count);

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

// ============================================================================
// Per-unit memoization serve/store (shared by the streaming table-in-out and
// LATERAL operators). An exchange cache entry is "the output batches for ONE
// input unit" keyed by (static key + input_hash).
// ============================================================================

//! Deserialize one cached batch's self-contained IPC blob back to an Arrow
//! RecordBatch. Handles both the in-RAM `ipc` path AND a disk-backed batch
//! (`ipc==nullptr`, `disk_ipc_offset>=0`) by positioned-reading just that batch's
//! bytes from `entry.disk_path` — so a disk-tier exchange entry that Lookup returned
//! as a streaming (TOC-only) entry serves correctly, not just materialized ones.
std::shared_ptr<arrow::RecordBatch> DeserializeCachedRecordBatch(const VgiResultCacheEntry &entry,
                                                                 const VgiCachedBatch &cached);

//! Result of a StoreExchangeMemoEntry attempt (for the caller's result_cache.store /
//! result_cache.store_skipped observability, kept parallel to the producer path).
struct ExchangeStoreResult {
	bool stored = false;
	const char *reason = nullptr; // set when !stored (not_cacheable / transaction_scoped / …)
	int64_t rows = 0;
	int64_t bytes = 0;
};

//! On a 304 not_modified reply, re-insert the stored entry with a slid TTL (and any
//! refreshed validators from `cc`) so future lookups hit fresh without re-fetching.
//! Mirrors the producer's MaybeSlideRevalidatedEntry. Best-effort.
void SlideRevalidatedExchangeEntry(const VgiResultCacheEntry &entry, const VgiCacheControl &cc,
                                   int64_t default_ttl_seconds, bool allow_disk);

//! Build a cache entry from one input unit's output batches and Insert it. Freshness
//! from `cc`: a positive ttl (from cc.ttl_seconds or default_ttl_seconds) is required;
//! no_store and transaction-scoped results are refused. `allow_disk` opts the entry
//! into the on-disk tier (content-addressed; cross-process + cross-restart). All
//! exchange callers pass true — the disk tier is off by default (needs
//! `vgi_result_cache_dir`), so this only persists when the user configured it.
ExchangeStoreResult StoreExchangeMemoEntry(const VgiResultCacheKey &key, const VgiCacheControl &cc,
                                           const std::string &catalog_name, int64_t default_ttl_seconds,
                                           const std::vector<std::shared_ptr<arrow::RecordBatch>> &out_batches,
                                           bool allow_disk = false);

} // namespace vgi
} // namespace duckdb
