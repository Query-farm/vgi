// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// vgi.cache.* metadata vocabulary + the parsed cache-control advertisement.
// A lightweight header so IFunctionConnection can expose GetLastCacheControl()
// without pulling in the result-cache machinery.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

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
// 304-equivalent response key, then the request-side conditional keys.
constexpr const char *VGI_CACHE_NOT_MODIFIED_KEY = "vgi.cache.not_modified";
// Per-partition caching opt-in (SINGLE_VALUE_PARTITIONS functions): the result is
// also cached split by partition-value tuple, so a later `=`/`IN`-filtered scan
// reuses those entries. Additive to the whole-scan entry. See docs/result_cache.md.
constexpr const char *VGI_CACHE_PARTITION_SCOPE_KEY = "vgi.cache.partition_scope";
// Per-value memoization opt-in for exchange-mode maps (scalar / batched
// correlated LATERAL): each distinct input tuple's output is memoized. Off by
// default: a per-value serve has a fixed per-entry cost that only pays off for
// expensive calls (and is a large loss for cheap ones). Only the function author
// knows which, so it is an explicit advertisement, not a heuristic.
constexpr const char *VGI_CACHE_PER_VALUE_KEY = "vgi.cache.per_value";
constexpr const char *VGI_CACHE_IF_NONE_MATCH_KEY = "vgi.cache.if_none_match";
constexpr const char *VGI_CACHE_IF_MODIFIED_SINCE_KEY = "vgi.cache.if_modified_since";

// Reuse scope: `catalog` (default) reuses across transactions within the
// calling catalog identity; `transaction` folds transaction_id into the key.
constexpr const char *VGI_CACHE_SCOPE_CATALOG = "catalog";
constexpr const char *VGI_CACHE_SCOPE_TRANSACTION = "transaction";

// Clamp for any TTL (seconds; 10 years): a worker advertising ttl≈INT64_MAX
// would otherwise overflow the expiry arithmetic (UB).
constexpr int64_t VGI_CACHE_MAX_TTL_SECONDS = 315360000;

// The subset of `vgi.cache.*` a worker advertised on a result's first batch.
struct VgiCacheControl {
	bool present = false;                 // any vgi.cache.* key was seen
	bool no_store = false;                // explicit "never cache"
	std::optional<int64_t> ttl_seconds;   // relative freshness (wins over expires)
	std::string expires_rfc3339;          // absolute freshness deadline (empty = none)
	std::string scope = VGI_CACHE_SCOPE_CATALOG;
	std::string etag;                     // validator
	std::string last_modified;            // validator
	bool revalidatable = false;
	std::optional<int64_t> stale_while_revalidate;
	std::optional<int64_t> stale_if_error;
	// 304-equivalent: worker asserts the stored payload is still fresh. Carried
	// on a 0-row batch alongside a fresh ttl/expires (used to slide the entry).
	bool not_modified = false;
	// See VGI_CACHE_PARTITION_SCOPE_KEY.
	bool partition_scope = false;
	// See VGI_CACHE_PER_VALUE_KEY. Independent of Cacheable().
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
VgiCacheControl ParseVgiCacheControl(const VgiMetadataPtr &metadata);

} // namespace vgi
} // namespace duckdb
