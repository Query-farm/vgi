// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Per-catalog ATTACH parameters and the RPC call context bundle. Kept light
// (no Arrow, no vgi_rpc_types) so catalog/worker identity consumers don't
// recompile when metadata structs or RPC signatures change.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "vgi_hosted_protocols.hpp"     // arrow-free; per-catalog reflection cache
#include "vgi_server_capabilities.hpp" // arrow-free; per-catalog capability snapshot

namespace duckdb {

struct HTTPParams;       // duckdb/common/http_util.hpp
class ClientContext;

namespace vgi {

class CatalogAuth;        // vgi_oauth.hpp
class SessionCookieJar;   // vgi_cookie_jar.hpp
class VgiHttpClientPool;  // vgi_http_client.hpp
struct IrohClientConfig;
struct VgiRetainedAttach; // vgi_attach_tickets.hpp

// Constructor argument for ``VgiAttachParameters``; every field has a usable
// default. The legacy constructor below aggregate-initializes it positionally,
// so append new fields at the end.
struct VgiAttachParametersConfig {
	std::string worker_path;
	std::string catalog_name;
	bool worker_debug = false;
	bool use_pool = true;
	// Per-catalog result-cache opt-out (ATTACH `cache` option). Default true;
	// when false, this catalog's table-function results are never cached.
	bool cache = true;
	std::shared_ptr<CatalogAuth> auth;
	std::string data_version_spec;
	std::string implementation_version;
	std::shared_ptr<SessionCookieJar> cookie_jar;
	// Native Iroh endpoint/identity configuration. Null for non-Iroh transports
	// and for Emscripten, where the page-owned adapter owns the endpoint.
	std::shared_ptr<IrohClientConfig> iroh;
	// Per-LOCATION launcher overrides, applied to the spawn-time
	// ``LaunchConfig`` for `launch:` LOCATIONs; rejected at parse time for any
	// other transport.
	std::optional<int64_t> launcher_idle_timeout_seconds;
	std::optional<std::string> launcher_state_dir;
	// Canonical (sorted `k=v;`) serialization of the ATTACH options, EXCLUDING
	// the secret tokens (bearer_token / oauth_refresh_token). Folded into the
	// result-cache key so two attaches with different options (which may route
	// to different data/locations) never share a cache entry. Empty = none.
	std::string attach_options_canonical;
	// Opaque lifetime pin for a resolved worker artifact (prevents eviction
	// between calls). Null for ordinary transports.
	std::shared_ptr<void> worker_artifact_anchor;
	// Explicit userspace-networking proxy for tcp:// only. Consumed by the
	// transport and never forwarded in catalog_attach worker options.
	std::string tcp_proxy;
	// What the user attached with (remote catalog name, catalog options
	// INCLUDING secret ones, version specs as typed), kept in memory only so
	// vgi_export_session() can seal_attach it. Never logged or serialized
	// anywhere else. Null for internal/temporary parameters.
	std::shared_ptr<const VgiRetainedAttach> retained_attach;
};

// Parameters for connecting to a VGI worker
struct VgiAttachParameters {
	explicit VgiAttachParameters(VgiAttachParametersConfig cfg)
	    : worker_path_(std::move(cfg.worker_path)), catalog_name_(std::move(cfg.catalog_name)),
	      worker_debug_(cfg.worker_debug), use_pool_(cfg.use_pool), cache_(cfg.cache),
	      auth_(std::move(cfg.auth)),
	      data_version_spec_(std::move(cfg.data_version_spec)),
	      implementation_version_(std::move(cfg.implementation_version)),
	      cookie_jar_(std::move(cfg.cookie_jar)), iroh_(std::move(cfg.iroh)),
	      launcher_idle_timeout_seconds_(cfg.launcher_idle_timeout_seconds),
	      launcher_state_dir_(std::move(cfg.launcher_state_dir)),
	      tcp_proxy_(std::move(cfg.tcp_proxy)),
	      attach_options_canonical_(std::move(cfg.attach_options_canonical)) {
		worker_artifact_anchor_ = std::move(cfg.worker_artifact_anchor);
		retained_attach_ = std::move(cfg.retained_attach);
	}

	// Legacy positional constructor, still used by vgi_table_function_impl.cpp.
	VgiAttachParameters(const std::string &worker_path, const std::string &catalog_name, bool worker_debug = false,
	                    bool use_pool = true, std::shared_ptr<CatalogAuth> auth = nullptr,
	                    std::string data_version_spec = "", std::string implementation_version = "",
	                    std::shared_ptr<SessionCookieJar> cookie_jar = nullptr)
	    : VgiAttachParameters(VgiAttachParametersConfig {worker_path, catalog_name, worker_debug, use_pool,
	                                                     /*cache=*/true, std::move(auth),
	                                                     std::move(data_version_spec),
	                                                     std::move(implementation_version), std::move(cookie_jar),
	                                                     /*iroh=*/nullptr,
	                                                     std::nullopt, std::nullopt,
	                                                     /*attach_options_canonical=*/std::string()}) {
	}

	// Lazy-initialized, per-catalog HTTPParams cache (ordinary and
	// single-attempt sets are initialized together).
	//
	// TODO(duckdb#22258): HTTPFSUtil::InitializeParameters reads the secret
	// manager, which takes the MetaTransaction mutex; calling it inside
	// VgiTransaction::Start (which holds that mutex) deadlocks on HTTP. So the
	// cache is primed during ATTACH, outside any transaction, and BEGIN must
	// never initialize it. Consequence: vgi_http_timeout_seconds, http_proxy,
	// secrets, extra_http_headers, TLS options etc. are captured once per
	// catalog and only refresh on re-ATTACH.
	std::shared_ptr<HTTPParams> GetOrInitHttpParams(ClientContext &context, const std::string &url,
	                                             bool single_attempt = false) const;

	const std::string &worker_path() const {
		return worker_path_;
	}

	const std::string &tcp_proxy() const {
		return tcp_proxy_;
	}

	const std::string &catalog_name() const {
		return catalog_name_;
	}

	// Per-catalog result-cache opt-out (ATTACH `cache` option; default true).
	bool cache() const {
		return cache_;
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

	const std::shared_ptr<IrohClientConfig> &iroh() const {
		return iroh_;
	}

	// Requested semver constraint for the catalog's data version; empty =
	// unconstrained. Part of the pooled-worker key so mismatched versions never
	// share a process.
	const std::string &data_version_spec() const {
		return data_version_spec_;
	}

	// Requested semver constraint for the worker's implementation version;
	// empty = unconstrained.
	const std::string &implementation_version() const {
		return implementation_version_;
	}

	// HTTP cookie jar for this catalog. Null unless http:// or httpi://.
	const std::shared_ptr<SessionCookieJar> &cookie_jar() const {
		return cookie_jar_;
	}

	// Only meaningful for ``IsLaunchLocation(worker_path())``. Validated at
	// ATTACH parse time; the cache layer pins them to the worker's lifetime and
	// throws ``BinderException`` on a second ATTACH that disagrees.
	const std::optional<int64_t> &launcher_idle_timeout_seconds() const {
		return launcher_idle_timeout_seconds_;
	}

	const std::optional<std::string> &launcher_state_dir() const {
		return launcher_state_dir_;
	}

	// See VgiAttachParametersConfig::attach_options_canonical.
	const std::string &attach_options_canonical() const {
		return attach_options_canonical_;
	}

	const std::shared_ptr<void> &worker_artifact_anchor() const {
		return worker_artifact_anchor_;
	}
	// See VgiAttachParametersConfig::retained_attach. Null when not retained.
	const std::shared_ptr<const VgiRetainedAttach> &retained_attach() const {
		return retained_attach_;
	}

	void ReleaseWorkerArtifactAnchor() {
		worker_artifact_anchor_.reset();
	}

public:
	// Per-catalog HTTP ServerCapabilities snapshot, harvested from response
	// capability headers and shared by all of this catalog's connections so a
	// new one skips the HEAD /health probe. Load() returns discovered=false
	// until something has been harvested.
	void StoreServerCapabilities(const ServerCapabilities &caps) const {
		server_caps_->Store(caps);
	}
	ServerCapabilities LoadServerCapabilities() const {
		return server_caps_->Load();
	}
	// The same snapshot as a shared holder, for paths that don't hold the
	// VgiAttachParameters (unary catalog RPCs via UnaryRpcOptions). Sharing the
	// holder, not a copy, lets a codec downgrade learned by one path apply to
	// all.
	const std::shared_ptr<ServerCapabilitiesCache> &server_caps() const {
		return server_caps_;
	}

	// Lazy-initialized per-catalog keep-alive HTTP client pool for unary
	// catalog RPCs. HTTP transports only.
	std::shared_ptr<VgiHttpClientPool> GetOrInitHttpClientPool() const;

	// Per-catalog cache of the worker's vgi_rpc.Reflection.v1 listing. Never
	// null. Read through vgi::GetHostedProtocols / vgi::HostsProtocol
	// (vgi_reflection.hpp), not directly.
	HostedProtocolsCache &hosted_protocols() const {
		return *hosted_protocols_;
	}

private:
	std::string worker_path_;
	std::string catalog_name_;
	bool worker_debug_;
	bool use_pool_;
	bool cache_;
	std::shared_ptr<CatalogAuth> auth_;
	std::string data_version_spec_;
	std::string implementation_version_;
	std::shared_ptr<SessionCookieJar> cookie_jar_;
	std::shared_ptr<IrohClientConfig> iroh_;
	std::optional<int64_t> launcher_idle_timeout_seconds_;
	std::optional<std::string> launcher_state_dir_;
	std::string tcp_proxy_;
	std::string attach_options_canonical_;
	std::shared_ptr<void> worker_artifact_anchor_;
	std::shared_ptr<const VgiRetainedAttach> retained_attach_;

	mutable std::mutex http_params_mutex_;
	mutable std::shared_ptr<HTTPParams> cached_http_params_;
	mutable std::shared_ptr<HTTPParams> cached_single_attempt_http_params_;

	// Never null; shared_ptr so server_caps() can hand out the same holder.
	std::shared_ptr<ServerCapabilitiesCache> server_caps_ = std::make_shared<ServerCapabilitiesCache>();

	// Lazy init guarded by http_params_mutex_.
	mutable std::shared_ptr<VgiHttpClientPool> http_client_pool_;

	std::shared_ptr<HostedProtocolsCache> hosted_protocols_ = std::make_shared<HostedProtocolsCache>();
};

// Bundles all catalog state needed for an RPC call.
struct CatalogRpcContext {
	std::shared_ptr<VgiAttachParameters> params;
	std::vector<uint8_t> attach_opaque_data;       // from CatalogAttachResult
	std::vector<uint8_t> transaction_opaque_data;  // from VgiTransaction (empty if N/A)

	// Optional entity context logged on every catalog.rpc event (empty values
	// omitted), so latency can be grouped by the entity being resolved.
	std::string entity_kind;       // "table" | "schema" | "view" | "function" | "macro" | ""
	std::string entity_qualifier;  // e.g. "schema.table" or just "schema"
};

} // namespace vgi
} // namespace duckdb
