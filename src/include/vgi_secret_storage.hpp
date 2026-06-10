// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/secret/secret_storage.hpp"

namespace arrow {
class RecordBatch;
}

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class KeyValueSecret;
struct CatalogTransaction;

namespace vgi {

class CatalogAuth;

//! Lazy, remote-backed SecretStorage for Orchard.
//!
//! Registered as a catch-all (scope `[""]`) so it sees every (path, type)
//! secret lookup the SecretManager performs. On a cache miss it fetches the
//! matching credential from Orchard's separately-versioned secret microservice
//! over HTTP (VgiSecretProtocol `secret_lookup`), reusing the attached catalog's
//! OAuth/bearer identity (`CatalogAuth`), caches it with a TTL bounded by the
//! credential's own expiry, and synthesizes a `KeyValueSecret` of the requested
//! type. Created/owned per attached VGI catalog (see the per-DB registry on
//! VgiStorageExtension); inert after the catalog detaches.
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
	void DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
	                      optional_ptr<CatalogTransaction> transaction = nullptr) override;
	bool IncludeInLookups() override {
		return active_.load();
	}
	bool Persistent() const override {
		return false;
	}

	//! Lifecycle + diagnostics (used by the provider registry / SQL functions).
	//! Detach is best-effort: SecretManager has no unload, so we flip the active
	//! flag (drops us from lookups) and flush the cache. The object lives for the
	//! DB's lifetime.
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
	};

	//! Result of a remote secret_lookup decode.
	struct FetchResult {
		bool found = false;
		std::unique_ptr<const KeyValueSecret> secret; // populated when found
		int64_t ttl_seconds = 0;
		int64_t expires_at_unix = 0; // credential's own expiry (0 = none)
	};

	//! Perform the HTTP secret_lookup RPC and decode/validate the response.
	//! Mints a transient ClientContext when the caller's transaction has none
	//! (the system-transaction / null-context path httpfs uses). Throws on
	//! transport / auth / protocol failure (a legitimate "not found" returns
	//! ``found=false`` without throwing).
	FetchResult FetchRemote(const string &path, const string &type, optional_ptr<CatalogTransaction> transaction);

	//! Build a SecretMatch for ``path`` from a cached/decoded secret (clone +
	//! SelectBestMatch with this storage's tie-break offset).
	SecretMatch MatchFromSecret(const KeyValueSecret &secret, const string &path);

	//! Single-flight coordination for one in-progress ``(type, path)`` lookup so
	//! N concurrent scan threads on the same path share one secret_lookup RPC.
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

	//! Build a KeyValueSecret of the requested type. ``values_batch`` is the
	//! one-row RecordBatch decoded from the response (may be null = no values);
	//! each column becomes a secret_map entry with a fully-typed DuckDB Value
	//! (string / int64 / bool / struct / list / nested) via the Arrow→DuckDB bridge.
	std::unique_ptr<const KeyValueSecret> Synthesize(ClientContext &context, const string &type,
	                                                 const std::string &provider, const std::string &name,
	                                                 const std::vector<std::string> &scope,
	                                                 const std::shared_ptr<arrow::RecordBatch> &values_batch,
	                                                 const std::vector<std::string> &redact_keys);

	DatabaseInstance &db_;
	std::string endpoint_;
	std::shared_ptr<CatalogAuth> auth_;
	std::chrono::seconds default_ttl_;
	std::atomic<bool> active_ {true};

	std::mutex cache_mutex_;
	//! Positive cache keyed by (type, scope-prefix). A lookup iterates entries
	//! of the requested type and runs SelectBestMatch, so a cached scope covers
	//! every path under it.
	std::map<std::pair<std::string, std::string>, PositiveEntry> positive_cache_;
	//! Negative cache keyed by (type, exact path). Short-TTL and coarse so it
	//! cannot shadow a positive scope match.
	std::map<std::pair<std::string, std::string>, SteadyTime> negative_cache_;
	//! In-flight lookups keyed by (type, path); guarded by cache_mutex_.
	std::map<std::pair<std::string, std::string>, std::shared_ptr<InflightLookup>> inflight_;
};

} // namespace vgi
} // namespace duckdb
