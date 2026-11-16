// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/secret/secret_storage.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class KeyValueSecret;
struct CatalogTransaction;

namespace vgi {

class CatalogAuth;

//! Lazy, remote-backed SecretStorage for Orchard. While active it takes part in
//! every SecretManager (path, type) lookup; on a cache miss it calls the secret
//! service's VgiSecretProtocol `secret_lookup` over HTTP with the catalog's
//! `CatalogAuth`, caches the result with a TTL bounded by the credential's own
//! expiry, and synthesizes a `KeyValueSecret` of the requested type. One per
//! attached catalog; the SecretManager owns it and VgiStorageExtension keeps a
//! raw pointer in its provider registry. Inert after DETACH (see Deactivate).
class VgiRemoteSecretStorage : public SecretStorage {
public:
	VgiRemoteSecretStorage(DatabaseInstance &db, std::string storage_name, std::string endpoint,
	                       std::shared_ptr<CatalogAuth> auth, std::chrono::seconds default_ttl,
	                       int64_t tie_break_offset);
	~VgiRemoteSecretStorage() override = default;

	//! SecretStorage API
	SecretMatch LookupSecret(const string &path, const string &type,
	                         optional_ptr<CatalogTransaction> transaction = nullptr) override;
	unique_ptr<SecretEntry> GetSecretByName(const string &name,
	                                        optional_ptr<CatalogTransaction> transaction = nullptr) override;
	vector<SecretEntry> AllSecrets(optional_ptr<CatalogTransaction> transaction = nullptr) override;
	//! The one allowed throw: a `CREATE SECRET (STORAGE <ours>)` is not supported.
	unique_ptr<SecretEntry> StoreSecret(unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                    optional_ptr<CatalogTransaction> transaction = nullptr) override;
	void DropSecretByName(const Identifier &name, OnEntryNotFound on_entry_not_found,
	                      optional_ptr<CatalogTransaction> transaction = nullptr) override;
	bool IncludeInLookups() override {
		return active_.load();
	}
	bool Persistent() const override {
		return false;
	}

	//! DETACH hook. SecretManager has no unload, so this only drops us from
	//! lookups and flushes the cache; the object lives as long as the DB.
	void Deactivate();
	//! Clear the TTL caches. Returns the number of cached positive secrets dropped.
	idx_t FlushCache();
	idx_t CachedSecretCount();

	bool Active() const {
		return active_.load();
	}
	const std::string &Endpoint() const {
		return endpoint_;
	}
	int64_t TieBreakOffset() const {
		return tie_break_offset;
	}
	int64_t DefaultTtlSeconds() const {
		return static_cast<int64_t>(default_ttl_.count());
	}

private:
	using SteadyTime = std::chrono::steady_clock::time_point;

	struct PositiveEntry {
		std::unique_ptr<const KeyValueSecret> secret;
		SteadyTime expires_at;
		SteadyTime last_access;
	};

	struct NegativeEntry {
		SteadyTime expires_at;
		SteadyTime last_access;
	};

	// Hard entry limits besides TTLs: the remote controls scope cardinality and
	// callers control lookup paths. Negative entries are cheap but per path.
	static constexpr size_t kMaxPositiveCacheEntries = 1024;
	static constexpr size_t kMaxNegativeCacheEntries = 4096;

	//! Remove every expired entry. Caller must hold cache_mutex_.
	void PruneExpiredCachesLocked(SteadyTime now);
	//! Apply access-order eviction until both maps satisfy their hard limits.
	//! Caller must hold cache_mutex_.
	void EnforceCacheLimitsLocked();

	//! Result of a remote secret_lookup decode.
	struct FetchResult {
		bool found = false;
		std::unique_ptr<const KeyValueSecret> secret; // populated when found
		int64_t ttl_seconds = 0;
		int64_t expires_at_unix = 0; // credential's own expiry (0 = none)
	};

	//! Perform and validate the secret_lookup RPC. Mints a transient connection
	//! when the transaction has no context (the system-transaction path httpfs
	//! uses). "Not found" returns ``found=false``; any transport / auth /
	//! protocol failure throws.
	FetchResult FetchRemote(const string &path, const string &type, optional_ptr<CatalogTransaction> transaction);

	//! SelectBestMatch for ``path`` on a clone of ``secret``.
	SecretMatch MatchFromSecret(const KeyValueSecret &secret, const string &path);

	//! Single-flight state for one in-progress ``(type, path)`` lookup, so
	//! concurrent scan threads on the same path share one RPC.
	struct InflightLookup {
		std::mutex m;
		std::condition_variable cv;
		bool done = false;
		std::exception_ptr error;                      // set if the leader's fetch threw
		bool found = false;
		std::shared_ptr<const KeyValueSecret> secret;  // shared result (null = not found)
		int64_t ttl_seconds = 0;
		int64_t expires_at_unix = 0;
	};

	//! Build a KeyValueSecret of the requested type. Each column of the one-row
	//! ``values_batch`` (may be null) becomes a typed secret_map Value.
	std::unique_ptr<const KeyValueSecret> Synthesize(ClientContext &context, const string &type,
	                                                 const std::string &provider, const std::string &name,
	                                                 const std::vector<std::string> &scope,
	                                                 const VgiBatchPtr &values_batch,
	                                                 const std::vector<std::string> &redact_keys);

	DatabaseInstance &db_;
	std::string endpoint_;
	std::shared_ptr<CatalogAuth> auth_;
	std::chrono::seconds default_ttl_;
	std::atomic<bool> active_ {true};

	std::mutex cache_mutex_;
	//! Positive cache keyed by (type, first scope prefix). Lookups run
	//! SelectBestMatch over all entries of the type, so a cached scope covers
	//! every path under it.
	std::map<std::pair<std::string, std::string>, PositiveEntry> positive_cache_;
	//! Negative cache keyed by (type, exact path), TTL min(default_ttl, 30s).
	//! Consulted after the positive cache, so it never shadows a cached match.
	std::map<std::pair<std::string, std::string>, NegativeEntry> negative_cache_;
	//! In-flight lookups keyed by (type, path); guarded by cache_mutex_.
	std::map<std::pair<std::string, std::string>, std::shared_ptr<InflightLookup>> inflight_;
};

} // namespace vgi
} // namespace duckdb
