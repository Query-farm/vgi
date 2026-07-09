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
