// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "duckdb/common/types/value.hpp"

namespace duckdb {

class ClientContext;
class DataChunk;

namespace vgi {
class VgiResultCache;

struct VgiResultCacheKey;      // vgi_result_cache.hpp
struct VgiResultCacheEntry;    // vgi_result_cache.hpp
struct VgiCachedBatch;         // vgi_result_cache.hpp
struct VgiCacheControl;        // vgi_cache_control.hpp
struct VgiTableInOutBindData;  // vgi_table_in_out_impl.hpp
struct VgiAttachParameters;    // vgi_attach_parameters.hpp

// Canonical key-component serializers, shared with the producer path so producer-
// and exchange-mode keys are byte-identical.

//! Canonical (sorted-key) serialization of the settings map for the cache key.
std::string SerializeSettingsForKey(const std::map<std::string, Value> &settings);

//! Canonical serialization of the projection id list for the cache key.
std::string SerializeProjectionForKey(const std::vector<int32_t> &projection_ids);

//! Canonical key component for a bind's const-argument batch (ArrowArguments::batch):
//! SHA-256 hex of its deterministic IPC serialization (no message metadata); "" when
//! there is none.
std::string SerializeArgumentsForKey(const VgiBatchPtr &arguments_batch);

// Input-hash helpers for exchange-mode cache keys.

//! Order-DEPENDENT: SHA-256 of the batch's deterministic IPC framing, for streaming
//! table-in-out, whose output aligns positionally with input. Equal data with
//! different framing only misses, never wrongly hits.
std::string HashInputBatchOrdered(const VgiBatchPtr &input_batch);

//! Order-INDEPENDENT, multiset-correct: per-row CreateSortKey blobs (NULL-aware, all
//! types), sorted, then SHA-256 over count + length-prefixed blobs. For correlated
//! LATERAL, whose input is an unordered multiset. Hashes ALL columns, since the
//! correlated columns are baked into the cached output.
std::string HashInputChunkUnordered(ClientContext &context, DataChunk &chunk);

//! Raw sort-key bytes for ONE partition-value tuple (per-partition producer cache),
//! each Value cast to `partition_types[i]` in declared order. The capture side
//! (decoded partition min-values) and the serve side (`=`/`IN` filter constants)
//! MUST both call this with the same types and order so their bytes match. Callers
//! form input_hash as "p:" + SHA-256. Throws on a cast failure.
std::string CanonicalPartitionTupleKey(ClientContext &context, const std::vector<Value> &tuple,
                                       const std::vector<LogicalType> &partition_types);

//! One input_hash per row: "v:" + SHA-256 of its sort-key blob. The prefix keeps
//! per-value keys from colliding with per-unit (pure hex) input_hash values.
std::vector<std::string> HashInputRowsPerValue(ClientContext &context, DataChunk &chunk);

//! Raw per-row sort-key blobs (no SHA-256, no prefix): the per-value memo arena's
//! slot keys. The arena is already scoped to a static key and compares blobs by
//! value, so no digest is needed and there are no collisions.
std::vector<std::string> InputRowSortKeys(ClientContext &context, DataChunk &chunk);

//! Order-INDEPENDENT whole-input digest (buffered functions): per-row hashes fold
//! into a two-lane 64-bit ADDITIVE accumulator plus row count. Addition is
//! commutative and associative (independent of chunk order and of which thread
//! folded a chunk) and, unlike XOR, keeps duplicates; partials merge field-wise.
//! FinalizeInputDigest renders it as hex for VgiResultCacheKey::input_hash.
void AccumulateInputDigest(DataChunk &chunk, uint64_t &sum_lo, uint64_t &sum_hi, uint64_t &row_count);
std::string FinalizeInputDigest(uint64_t sum_lo, uint64_t sum_hi, uint64_t row_count);

//! Push this query's result-cache settings into the database's VgiResultCache and
//! memo arena registry, so a `SET` takes effect (without it disk_dir stays empty and
//! allow_disk is a silent no-op). Also for paths with no eligibility check (e.g.
//! vgi_result_cache_reap()). Cheap when unchanged (ConfigureIfChanged).
void SyncResultCacheSettings(ClientContext &context);

//! Build the STATIC part of an exchange-mode cache key (the producer's
//! EvaluateCacheEligibility minus filter/order/sample/time-travel); the caller sets
//! key.input_hash per memoization event. Returns false with `reason` set when
//! ineligible (global switch off, per-catalog opt-out, unresolved identity, unknown
//! catalog version, secret dependency without a fingerprint).
//!
//! A secret-dependent function is keyed on BindResult::secret_scope, so a changed
//! secret misses. SECURITY: identity_scope folds in the caller's auth principal;
//! an unresolved one fails closed so two identities never cross-serve.
//! `operator_kind` ("stream", "buffered", ...) becomes key.shape_key so different
//! operators never cross-serve.
bool BuildExchangeCacheKeyStatic(ClientContext &context, const VgiTableInOutBindData &bd,
                                 const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                 std::string &catalog_name, int64_t &catalog_version, const char *&reason,
                                 const std::string &operator_kind);

//! Field-based core of the above, also used by the scalar path. `operator_kind` is
//! required: an empty value would let two operator shapes collide.
bool BuildExchangeCacheKeyStaticFields(ClientContext &context,
                                       const std::shared_ptr<VgiAttachParameters> &attach_params,
                                       const std::string &function_name, const std::string &schema_name,
                                       const std::string &canonical_arguments,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<int32_t> &projection_ids, bool secret_dependent,
                                       const std::string &secret_scope, VgiResultCacheKey &key,
                                       std::string &catalog_name, int64_t &catalog_version, const char *&reason,
                                       const std::string &operator_kind);

// Per-unit memoization: an exchange cache entry is the output batches for ONE
// input unit, keyed by static key + input_hash.

//! Decode one cached batch: from `ipc` in RAM, or, for a disk-streaming (TOC-only)
//! entry (`ipc==nullptr`), by a positioned read of just its bytes from
//! `entry.disk_path`.
VgiBatchPtr DeserializeCachedRecordBatch(const VgiResultCacheEntry &entry,
                                                                 const VgiCachedBatch &cached);

//! For the caller's result_cache.store / store_skipped logging.
struct ExchangeStoreResult {
	bool stored = false;
	const char *reason = nullptr; // set when !stored (not_cacheable / transaction_scoped / …)
	int64_t rows = 0;
	int64_t bytes = 0;
};

//! On a 304 not_modified reply, re-insert the entry with a slid TTL and any
//! refreshed validators from `cc`. Mirrors the producer's MaybeSlideRevalidatedEntry.
void SlideRevalidatedExchangeEntry(VgiResultCache &cache, const VgiResultCacheEntry &entry, const VgiCacheControl &cc,
                                   int64_t default_ttl_seconds, bool allow_disk);

//! Build a cache entry from one input unit's output batches and Insert it. Needs a
//! positive ttl (cc.ttl_seconds, else default_ttl_seconds) unless the ttl=0
//! always-revalidate contract applies (etag + revalidatable; memory-only, stored
//! immediately stale). no_store and transaction-scoped results are refused.
//! `allow_disk` opts into the disk tier, which is active only when
//! vgi_result_cache_dir is set.
//!
//! Pass allow_immediately_stale=false for keys probed only via LookupBatch: it has
//! no revalidation path, so an immediately-stale entry would be evicted on the next
//! probe, churning with a 0% hit rate.
ExchangeStoreResult StoreExchangeMemoEntry(VgiResultCache &cache, const VgiResultCacheKey &key, const VgiCacheControl &cc,
                                           const std::string &catalog_name, int64_t default_ttl_seconds,
                                           const std::vector<VgiBatchPtr> &out_batches,
                                           bool allow_disk = false, bool allow_immediately_stale = true);

} // namespace vgi
} // namespace duckdb
