// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// vgi.cache.* metadata vocabulary + the parsed cache-control advertisement.
//
// Kept in its own lightweight header (no Arrow, no heavy deps) so the widely
// included IFunctionConnection interface can expose GetLastCacheControl()
// without pulling in the full result-cache machinery. Arrow appears only as a
// forward-declared shared_ptr parameter to the parser.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace arrow {
class KeyValueMetadata;
} // namespace arrow

namespace duckdb {
namespace vgi {

// Response-side keys, read off the first data batch's custom_metadata.
constexpr const char *VGI_CACHE_TTL_KEY = "vgi.cache.ttl";
constexpr const char *VGI_CACHE_EXPIRES_KEY = "vgi.cache.expires";
constexpr const char *VGI_CACHE_NO_STORE_KEY = "vgi.cache.no_store";
constexpr const char *VGI_CACHE_SCOPE_KEY = "vgi.cache.scope";
constexpr const char *VGI_CACHE_ETAG_KEY = "vgi.cache.etag";
constexpr const char *VGI_CACHE_LAST_MODIFIED_KEY = "vgi.cache.last_modified";
constexpr const char *VGI_CACHE_REVALIDATABLE_KEY = "vgi.cache.revalidatable";
constexpr const char *VGI_CACHE_STALE_WHILE_REVALIDATE_KEY = "vgi.cache.stale_while_revalidate";
constexpr const char *VGI_CACHE_STALE_IF_ERROR_KEY = "vgi.cache.stale_if_error";
// 304-equivalent + request-side conditional keys (revalidation milestone).
constexpr const char *VGI_CACHE_NOT_MODIFIED_KEY = "vgi.cache.not_modified";
// Per-partition caching opt-in: when a SINGLE_VALUE_PARTITIONS function sets this,
// the client ALSO caches the result split by partition value (one entry per distinct
// partition-value tuple), so a later `=`/`IN`-filtered scan reuses per-partition
// entries. Additive — today's whole-scan entry is still stored/served. See CLAUDE.md
// "Per-Partition Result Cache".
constexpr const char *VGI_CACHE_PARTITION_SCOPE_KEY = "vgi.cache.partition_scope";
// Per-VALUE memoization opt-in for exchange-mode MAPS (scalar / batched correlated
// LATERAL). When set, the client ALSO memoizes each distinct worker-input tuple's
// output keyed on that tuple, so the same value serves without the worker on a later
// chunk / query. DEFAULT OFF, and deliberately so: a per-value serve carries a fixed
// per-entry cost (key probe + decode + assembly) that only pays back when the worker
// call it replaces is MORE expensive than that. For a cheap arithmetic map it is a
// large net loss — measured ~50x slower than simply calling the worker. Only the
// function author knows whether a call is expensive enough (a model inference, a
// geocode, a rate-limited HTTP fetch); the engine cannot infer it, so this is an
// explicit advertisement rather than a heuristic.
constexpr const char *VGI_CACHE_PER_VALUE_KEY = "vgi.cache.per_value";
constexpr const char *VGI_CACHE_IF_NONE_MATCH_KEY = "vgi.cache.if_none_match";
constexpr const char *VGI_CACHE_IF_MODIFIED_SINCE_KEY = "vgi.cache.if_modified_since";

// Reuse scope: `catalog` (default) reuses across transactions within the
// calling catalog identity; `transaction` folds transaction_id into the key.
constexpr const char *VGI_CACHE_SCOPE_CATALOG = "catalog";
constexpr const char *VGI_CACHE_SCOPE_TRANSACTION = "transaction";

// Upper bound on any worker-advertised / default TTL (seconds). A hostile or
// buggy worker advertising ttl≈INT64_MAX would otherwise overflow the expiry
// arithmetic (steady_clock time_point / wall-clock int64 addition) — UB. Clamp
// to 10 years, which is "effectively forever" for a result cache.
constexpr int64_t VGI_CACHE_MAX_TTL_SECONDS = 315360000;

// The subset of `vgi.cache.*` a worker advertised on a result's first batch.
struct VgiCacheControl {
	bool present = false;                 // any vgi.cache.* key was seen
	bool no_store = false;                // explicit "never cache"
	std::optional<int64_t> ttl_seconds;   // relative freshness (wins over expires)
	std::string expires_rfc3339;          // absolute freshness deadline (empty = none)
	std::string scope = VGI_CACHE_SCOPE_CATALOG;
	std::string etag;                     // validator (revalidation milestone)
	std::string last_modified;            // validator (revalidation milestone)
	bool revalidatable = false;
	std::optional<int64_t> stale_while_revalidate;
	std::optional<int64_t> stale_if_error;
	// 304-equivalent: worker asserts the stored payload is still fresh. Carried
	// on a 0-row batch alongside a fresh ttl/expires (used to slide the entry).
	bool not_modified = false;
	// Opt-in to per-partition caching (SINGLE_VALUE_PARTITIONS only). Additive to
	// the whole-scan cache; the split at capture is gated on this.
	bool partition_scope = false;
	// Opt-in to per-VALUE memoization (exchange-mode maps). Default off — see
	// VGI_CACHE_PER_VALUE_KEY above. Independent of Cacheable(): a function may be
	// whole-result cacheable without per-value memoization paying off.
	bool per_value = false;

	// Opt-in: a freshness key present and not explicitly no_store.
	bool Cacheable() const {
		return present && !no_store && (ttl_seconds.has_value() || !expires_rfc3339.empty());
	}
	bool TransactionScoped() const {
		return scope == VGI_CACHE_SCOPE_TRANSACTION;
	}
};

// Parse `vgi.cache.*` keys off a batch's custom_metadata. Returns a struct with
// present=false when the metadata is null or carries no vgi.cache.* key.
VgiCacheControl ParseVgiCacheControl(const std::shared_ptr<const arrow::KeyValueMetadata> &metadata);

} // namespace vgi
} // namespace duckdb
