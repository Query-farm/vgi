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
struct VgiAttachParameters;    // vgi_attach_parameters.hpp

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

//! Canonical byte-string for ONE partition-value tuple (per-partition producer cache).
//! Builds a 1-row DataChunk from `tuple` (each Value cast to the matching
//! `partition_types[i]`, in declared partition-column order) and runs CreateSortKey —
//! the same NULL-aware, all-types machinery HashInputChunkUnordered uses. BOTH the
//! capture side (fed the decoded partition min-values) and the serve side (fed the
//! `=`/`IN` filter constants) call this with the same types + order, so their
//! discriminator bytes are byte-identical (the per-partition-cache correctness
//! linchpin). Returns the raw sort-key bytes; callers prefix "p:" + SHA-256 for the
//! VgiResultCacheKey::input_hash discriminator. Throws on a cast failure.
std::string CanonicalPartitionTupleKey(ClientContext &context, const std::vector<Value> &tuple,
                                       const std::vector<LogicalType> &partition_types);

//! Per-VALUE hash: one input_hash per row of `chunk` (a canonical NULL-aware sort-key
//! blob → SHA-256, prefixed with a "v:" granularity discriminator so a per-value key can
//! never collide with a per-batch/per-chunk `input_hash` in the same keyspace). Used for
//! the per-value memoization tier over a DEDUPED input chunk (one key per distinct tuple).
std::vector<std::string> HashInputRowsPerValue(ClientContext &context, DataChunk &chunk);

//! Order-INDEPENDENT streaming digest for whole-input keying (buffered functions):
//! folds a chunk's per-row hashes into a two-lane 64-bit ADDITIVE accumulator (+ row
//! count). Additive => associative + commutative => independent of both chunk order
//! and the (parallel) thread that folded each chunk, and duplicate-preserving (unlike
//! XOR). Partial accumulators merge by field-wise addition. FinalizeInputDigest
//! renders the accumulator to a stable hex string for VgiResultCacheKey::input_hash.
void AccumulateInputDigest(DataChunk &chunk, uint64_t &sum_lo, uint64_t &sum_hi, uint64_t &row_count);
std::string FinalizeInputDigest(uint64_t sum_lo, uint64_t sum_hi, uint64_t row_count);

//! Push this query's cache-config settings (byte caps, disk dir/caps, compression,
//! exchange ref-count cap) into the VgiResultCache singleton, so a `SET` takes effect
//! on paths that don't otherwise run an eligibility check (e.g. vgi_result_cache_reap()).
//! Lock-free when the settings are unchanged (ConfigureIfChanged fast path).
void SyncResultCacheSettings(ClientContext &context);

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

//! Field-based core of the above — decoupled from any bind-data struct so the SCALAR
//! path (which has its own bind data) shares the exact eligibility + key-shape logic.
//! `canonical_arguments` is the caller's canonical const-arg serialization.
bool BuildExchangeCacheKeyStaticFields(ClientContext &context,
                                       const std::shared_ptr<VgiAttachParameters> &attach_params,
                                       const std::string &function_name,
                                       const std::string &canonical_arguments,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                       std::string &catalog_name, int64_t &catalog_version,
                                       const char *&reason);

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
//!
//! `allow_immediately_stale` (default true) governs the ttl=0 "always-revalidate"
//! contract (etag + revalidatable, stored memory-only, immediately stale). The
//! PER-VALUE callers pass FALSE: per-value keys are probed by LookupBatch, which has
//! no revalidation path, so an immediately-stale per-value entry would be found stale
//! and evicted on the very next probe — a store/insert/evict/serialize churn loop
//! with a 0% hit rate every chunk (B3). Refusing to store it is strictly better.
ExchangeStoreResult StoreExchangeMemoEntry(const VgiResultCacheKey &key, const VgiCacheControl &cc,
                                           const std::string &catalog_name, int64_t default_ttl_seconds,
                                           const std::vector<std::shared_ptr<arrow::RecordBatch>> &out_batches,
                                           bool allow_disk = false, bool allow_immediately_stale = true);

} // namespace vgi
} // namespace duckdb
