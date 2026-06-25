// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Connection-level catalog state: the per-catalog ATTACH parameters and the
// RPC call context bundle. Split out of vgi_catalog_api.hpp so consumers that
// only need to identify a catalog/worker don't recompile when metadata structs
// or RPC signatures change. Deliberately light — no Arrow, no vgi_rpc_types.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

// Forward declarations — full definitions live in their owning headers; only
// shared_ptr members / declared-only methods reference these here, so the
// completeness is needed in the .cpp, not this header.
struct HTTPParams;       // duckdb/common/http_util.hpp
class ClientContext;

namespace vgi {

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;
// Forward declaration — full definition in vgi_cookie_jar.hpp
class SessionCookieJar;

// POD constructor argument for ``VgiAttachParameters``.  Replaces the
// 8-positional-default-param constructor that previous versions of this
// struct used — adding the launcher overrides pushed it into the
// "constructor smell" zone.  All fields default-construct sensibly so
// callers only set what they care about.  See ``VgiAttachParameters``
// below for accessor docs.
struct VgiAttachParametersConfig {
	std::string worker_path;
	std::string catalog_name;
	bool worker_debug = false;
	bool use_pool = true;
	std::shared_ptr<CatalogAuth> auth;
	std::string data_version_spec;
	std::string implementation_version;
	std::shared_ptr<SessionCookieJar> cookie_jar;
	// Per-LOCATION launcher overrides.  Both nullopt by default; when set,
	// they're applied to the spawn-time ``LaunchConfig`` for `launch:`
	// LOCATIONs and rejected at parse time for any other transport.
	std::optional<int64_t> launcher_idle_timeout_seconds;
	std::optional<std::string> launcher_state_dir;
};

// Parameters for connecting to a VGI worker
struct VgiAttachParameters {
	// Preferred constructor — takes a config struct.  All fields default to
	// the config's defaults; callers only specify what they care about.
	explicit VgiAttachParameters(VgiAttachParametersConfig cfg)
	    : worker_path_(std::move(cfg.worker_path)), catalog_name_(std::move(cfg.catalog_name)),
	      worker_debug_(cfg.worker_debug), use_pool_(cfg.use_pool), auth_(std::move(cfg.auth)),
	      data_version_spec_(std::move(cfg.data_version_spec)),
	      implementation_version_(std::move(cfg.implementation_version)),
	      cookie_jar_(std::move(cfg.cookie_jar)),
	      launcher_idle_timeout_seconds_(cfg.launcher_idle_timeout_seconds),
	      launcher_state_dir_(std::move(cfg.launcher_state_dir)) {
	}

	// Legacy constructor — thin wrapper for in-tree call sites that haven't
	// migrated yet.  Forwards to the config-struct constructor.  Will be
	// removed in a follow-up after every caller is migrated.
	VgiAttachParameters(const std::string &worker_path, const std::string &catalog_name, bool worker_debug = false,
	                    bool use_pool = true, std::shared_ptr<CatalogAuth> auth = nullptr,
	                    std::string data_version_spec = "", std::string implementation_version = "",
	                    std::shared_ptr<SessionCookieJar> cookie_jar = nullptr)
	    : VgiAttachParameters(VgiAttachParametersConfig {worker_path, catalog_name, worker_debug, use_pool,
	                                                     std::move(auth), std::move(data_version_spec),
	                                                     std::move(implementation_version), std::move(cookie_jar),
	                                                     std::nullopt, std::nullopt}) {
	}

	// Lazy-initialized, per-catalog HTTPParams cache.
	//
	// TODO(#22258): revisit once the upstream DuckDB issue is resolved. We cache
	// HTTPParams because HTTPFSUtil::InitializeParameters reaches into the secret
	// manager, which takes the MetaTransaction mutex. Calling it from inside
	// VgiTransaction::Start (which already holds that mutex) deadlocks on HTTP
	// transport — see https://github.com/duckdb/duckdb/issues/22258. Priming the
	// cache during ATTACH (outside any transaction) avoids the reentrancy.
	//
	// Side effect: session settings like vgi_http_timeout_seconds,
	// http_proxy, bearer_token secrets, extra_http_headers, TLS options etc. are
	// captured once per catalog and never refreshed. If the user mutates them
	// mid-session they won't take effect until re-ATTACH. Acceptable short-term;
	// a proper fix is to make the upstream secret read lock-free, or to move
	// CheckAndInvalidateCache off the Transaction::Start hot path so HTTP I/O
	// never happens under the MetaTransaction lock in the first place.
	std::shared_ptr<HTTPParams> GetOrInitHttpParams(ClientContext &context, const std::string &url) const;

	const std::string &worker_path() const {
		return worker_path_;
	}

	const std::string &catalog_name() const {
		return catalog_name_;
	}

	bool worker_debug() const {
		return worker_debug_;
	}

	bool use_pool() const {
		return use_pool_;
	}

	const std::shared_ptr<CatalogAuth> &auth() const {
		return auth_;
	}

	// Requested semver constraint for the catalog's data version. Empty string
	// means the client did not constrain. Pooled worker lookup uses this as
	// part of the key so mismatched versions never share a process.
	const std::string &data_version_spec() const {
		return data_version_spec_;
	}

	// Requested semver constraint for the worker's implementation version.
	// Empty string means unconstrained.
	const std::string &implementation_version() const {
		return implementation_version_;
	}

	// HTTP cookie jar for this catalog. Null on subprocess transport.
	const std::shared_ptr<SessionCookieJar> &cookie_jar() const {
		return cookie_jar_;
	}

	// Per-LOCATION launcher overrides — only meaningful when
	// ``IsLaunchLocation(worker_path())`` is true.  Validation (range,
	// transport-gating) happens at ATTACH parse time; the cache layer
	// pins these values to the worker's lifetime and throws
	// ``BinderException`` on a second ATTACH that disagrees.
	const std::optional<int64_t> &launcher_idle_timeout_seconds() const {
		return launcher_idle_timeout_seconds_;
	}

	const std::optional<std::string> &launcher_state_dir() const {
		return launcher_state_dir_;
	}

public:
	// Capability cache for the new catalog_table_scan_branches_get RPC.
	// Tri-state: 0 = unknown (probe), 1 = supported, 2 = not supported.
	// Set by InvokeCatalogTableScanBranchesGet after the first call against
	// this attach: on success → 1, on MethodNotImplemented fallback → 2.
	// Subsequent calls short-circuit straight to the legacy RPC when the
	// state is 2, avoiding a per-call round-trip-and-throw. Atomic for
	// lock-free reads on the hot path; benign races (two threads probing
	// once each before the cache settles) are acceptable.
	int LoadBranchesCapability() const noexcept {
		return branches_capability_.load(std::memory_order_relaxed);
	}
	void StoreBranchesCapability(bool supported) const noexcept {
		branches_capability_.store(supported ? 1 : 2, std::memory_order_relaxed);
	}

private:
	std::string worker_path_;
	std::string catalog_name_;
	bool worker_debug_;
	bool use_pool_;
	std::shared_ptr<CatalogAuth> auth_;
	std::string data_version_spec_;
	std::string implementation_version_;
	std::shared_ptr<SessionCookieJar> cookie_jar_;
	std::optional<int64_t> launcher_idle_timeout_seconds_;
	std::optional<std::string> launcher_state_dir_;

	// See GetOrInitHttpParams above for the rationale behind caching these.
	mutable std::mutex http_params_mutex_;
	mutable std::shared_ptr<HTTPParams> cached_http_params_;

	// Capability cache for catalog_table_scan_branches_get. See accessors above.
	mutable std::atomic<int> branches_capability_{0};
};

// Bundles all catalog state needed for an RPC call.
// Eliminates the 5-6 individual catalog parameters that every InvokeCatalog* function used to take.
struct CatalogRpcContext {
	std::shared_ptr<VgiAttachParameters> params;
	std::vector<uint8_t> attach_opaque_data;       // from CatalogAttachResult
	std::vector<uint8_t> transaction_opaque_data;  // from VgiTransaction (empty if N/A)

	// Optional entity context for instrumentation. When populated, the RPC
	// chokepoint logs them on every catalog.rpc event so analyses can group
	// latency by which entity DuckDB was resolving. Empty values are omitted.
	std::string entity_kind;       // "table" | "schema" | "view" | "function" | "macro" | ""
	std::string entity_qualifier;  // e.g. "schema.table" or just "schema"
};

} // namespace vgi
} // namespace duckdb
