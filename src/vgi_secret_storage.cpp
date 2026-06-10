// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_secret_storage.hpp"

#include <algorithm>

#include <arrow/api.h>

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "generated/vgi_secret_protocol_schemas.hpp"
#include "generated/vgi_secret_protocol_version.hpp"
#include "generated/vgi_secret_request_builders.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_profiling.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_unary_rpc.hpp"

namespace duckdb {
namespace vgi {

VgiRemoteSecretStorage::VgiRemoteSecretStorage(DatabaseInstance &db, std::string storage_name, std::string endpoint,
                                               std::shared_ptr<CatalogAuth> auth, std::chrono::seconds default_ttl,
                                               int64_t tie_break_offset)
    : SecretStorage(storage_name, tie_break_offset), db_(db), endpoint_(std::move(endpoint)), auth_(std::move(auth)),
      default_ttl_(default_ttl) {
}

namespace {

//! Wrap a cloned secret in a SecretEntry with our storage's provenance set.
SecretEntry MakeEntry(unique_ptr<const BaseSecret> secret, const std::string &storage_mode) {
	SecretEntry entry(std::move(secret));
	entry.persist_type = SecretPersistType::TEMPORARY;
	entry.storage_mode = storage_mode;
	return entry;
}

} // namespace

SecretMatch VgiRemoteSecretStorage::MatchFromSecret(const KeyValueSecret &secret, const string &path) {
	SecretMatch best;
	SecretEntry entry = MakeEntry(secret.Clone(), GetName());
	return SelectBestMatch(entry, path, tie_break_offset, best);
}

SecretMatch VgiRemoteSecretStorage::LookupSecret(const string &path, const string &type,
                                                 optional_ptr<CatalogTransaction> transaction) {
	if (!active_.load()) {
		return SecretMatch();
	}

	// Reentrancy guard: when endpoint_ is http(s)://, the outgoing RPC re-enters
	// LookupSecret(type="http") on this same thread (InitializeParameters ->
	// HTTPFSUtil -> KeyValueSecretReader). Break the cycle by short-circuiting
	// any nested call BEFORE any locking. RAII so it clears on every exit path.
	static thread_local bool in_lookup = false;
	if (in_lookup) {
		return SecretMatch();
	}
	struct ReentryGuard {
		bool &flag;
		explicit ReentryGuard(bool &f) : flag(f) {
			flag = true;
		}
		~ReentryGuard() {
			flag = false;
		}
	} guard(in_lookup);

	const auto now = std::chrono::steady_clock::now();
	const std::pair<std::string, std::string> key {type, path};

	std::shared_ptr<InflightLookup> leader;
	std::shared_ptr<InflightLookup> waiter;

	// 1) Cache check + single-flight bookkeeping under the cache lock.
	{
		std::lock_guard<std::mutex> lk(cache_mutex_);
		SecretMatch best;
		for (auto &kv : positive_cache_) {
			if (kv.first.first != type || kv.second.expires_at <= now) {
				continue;
			}
			SecretEntry entry = MakeEntry(kv.second.secret->Clone(), GetName());
			best = SelectBestMatch(entry, path, tie_break_offset, best);
		}
		if (best.HasMatch()) {
			return best;
		}
		auto nit = negative_cache_.find(key);
		if (nit != negative_cache_.end()) {
			if (nit->second > now) {
				return SecretMatch();
			}
			negative_cache_.erase(nit);
		}
		// Single-flight: one in-progress fetch per (type, path). Concurrent
		// scan threads on the same path share the leader's one RPC.
		auto fit = inflight_.find(key);
		if (fit != inflight_.end()) {
			waiter = fit->second;
		} else {
			leader = std::make_shared<InflightLookup>();
			inflight_[key] = leader;
		}
	}

	// 2a) Waiter: block on the leader, then reuse its shared result/error.
	if (waiter) {
		std::unique_lock<std::mutex> wl(waiter->m);
		waiter->cv.wait(wl, [&] { return waiter->done; });
		if (waiter->error) {
			std::rethrow_exception(waiter->error);
		}
		if (waiter->found && waiter->secret) {
			return MatchFromSecret(*waiter->secret, path);
		}
		return SecretMatch();
	}

	// 2b) Leader: fetch with no lock held (so the nested http re-entry and the
	// HTTP round-trip never contend on cache_mutex_).
	FetchResult fr;
	std::exception_ptr err;
	try {
		fr = FetchRemote(path, type, transaction);
	} catch (...) {
		err = std::current_exception();
	}

	// Compute the effective TTL and a shareable copy of the secret.
	std::shared_ptr<const KeyValueSecret> shared_secret;
	int64_t ttl = 0;
	if (!err && fr.found && fr.secret) {
		const int64_t now_unix = std::chrono::duration_cast<std::chrono::seconds>(
		                             std::chrono::system_clock::now().time_since_epoch())
		                             .count();
		ttl = fr.ttl_seconds > 0 ? fr.ttl_seconds : static_cast<int64_t>(default_ttl_.count());
		if (fr.expires_at_unix > 0) {
			// Never serve a credential past its own expiry (short-lived STS tokens).
			ttl = std::min<int64_t>(ttl, fr.expires_at_unix - now_unix);
		}
		if (ttl < 0) {
			ttl = 0;
		}
		shared_secret = std::make_shared<const KeyValueSecret>(*fr.secret);
	}

	// 3) Publish to the caches and drop the in-flight entry. On *failure* we
	// deliberately cache nothing — masking a transport/auth error behind a
	// negative entry would hide an outage as a silent "no credential".
	{
		std::lock_guard<std::mutex> lk(cache_mutex_);
		inflight_.erase(key);
		if (!err) {
			if (fr.found && fr.secret) {
				const auto &scope = fr.secret->GetScope();
				std::string scope_key = scope.empty() ? std::string() : scope.front();
				positive_cache_[{type, scope_key}] =
				    PositiveEntry {std::move(fr.secret), now + std::chrono::seconds(ttl)};
			} else {
				negative_cache_[key] =
				    now + std::min<std::chrono::seconds>(default_ttl_, std::chrono::seconds(30));
			}
		}
	}

	// 4) Wake any waiters with the shared outcome.
	{
		std::lock_guard<std::mutex> fl(leader->m);
		leader->error = err;
		leader->found = (!err && shared_secret != nullptr);
		leader->secret = shared_secret;
		leader->ttl_seconds = ttl;
		leader->expires_at_unix = err ? 0 : fr.expires_at_unix;
		leader->done = true;
	}
	leader->cv.notify_all();

	// 5) Leader's own return: surface the failure, or build the match.
	if (err) {
		std::rethrow_exception(err);
	}
	if (shared_secret) {
		return MatchFromSecret(*shared_secret, path);
	}
	return SecretMatch();
}

VgiRemoteSecretStorage::FetchResult
VgiRemoteSecretStorage::FetchRemote(const string &path, const string &type,
                                    optional_ptr<CatalogTransaction> transaction) {
	// Resolve a usable ClientContext. On the httpfs / DB-filesystem path the
	// lookup runs under the system transaction whose `context` is null, so mint
	// a transient connection (cheap; no transaction, no catalog touch). One per
	// fetch → thread-safe (ClientContext is not safe to share).
	unique_ptr<Connection> owned_conn;
	ClientContext *ctx = nullptr;
	if (transaction && transaction->context) {
		ctx = transaction->context.get();
	} else {
		owned_conn = make_uniq<Connection>(db_);
		ctx = owned_conn->context.get();
	}

	ScopedTimer timer("secret.lookup");
	const auto rpc_start = std::chrono::high_resolution_clock::now();

	// A legitimate "not found" returns found=false (no throw). Any transport /
	// auth / protocol failure is wrapped in a clear, attributable IOException and
	// surfaced to the caller — never silently degraded to "no credential".
	try {
		auto params = generated::BuildSecretLookupParams(path, type);

		UnaryRpcOptions opts {*ctx, endpoint_};
		opts.use_pool = false;
		opts.auth = auth_;
		opts.phase = "secret_lookup";
		opts.protocol_version_override = std::string(generated::VGI_SECRET_PROTOCOL_VERSION);

		auto response = InvokePooledUnaryRpc(opts, "secret_lookup", params);

		if (!response.batch || response.batch->num_rows() == 0) {
			throw IOException("empty response");
		}
		auto result_col = response.batch->GetColumnByName("result");
		if (!result_col || result_col->type()->id() != arrow::Type::BINARY) {
			throw IOException("response missing a Binary 'result' column");
		}
		auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(result_col);
		if (binary_array->IsNull(0)) {
			throw IOException("response 'result' column was null");
		}
		auto view = binary_array->GetView(0);
		auto inner = DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(view.data()), view.size());

		// Validate against our own schema — the secret protocol deliberately does
		// NOT enter the shared catalog schema registry.
		if (!inner->schema()->Equals(*generated::SecretLookupResultSchema(), /*check_metadata=*/false)) {
			throw IOException("response schema mismatch (expected %s, got %s)",
			                  generated::SecretLookupResultSchema()->ToString(), inner->schema()->ToString());
		}

		RecordBatchSingleRow row(inner, 0, "SecretLookupResponse", endpoint_);
		FetchResult fr;
		fr.found = row["found"].value_not_null<bool>();
		fr.ttl_seconds = row["ttl_seconds"].value_or(static_cast<int64_t>(0));
		fr.expires_at_unix = row["expires_at_unix"].value_or(static_cast<int64_t>(0));

		if (fr.found) {
			auto provider = row["provider"].value_or(std::string("orchard"));
			auto name = row["name"].value_or(std::string(""));
			auto scope = row["scope"].value_or(std::vector<std::string> {});
			auto redact = row["redact_keys"].value_or(std::vector<std::string> {});
			// `values` is the one-row RecordBatch (IPC bytes) of typed key→value
			// columns; empty when the worker shipped no values.
			std::shared_ptr<arrow::RecordBatch> values_batch;
			auto values_bytes = row["values"].value_or(std::vector<uint8_t> {});
			if (!values_bytes.empty()) {
				values_batch = DeserializeFromIpcBytes(values_bytes.data(), values_bytes.size());
			}
			// Synthesize with the REQUESTED type so KeyValueSecretReader's probe
			// (which breaks on first type match without re-checking) lines up.
			fr.secret = Synthesize(*ctx, type, provider, name, scope, values_batch, redact);
		}

		const double duration_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - rpc_start).count();
		VGI_LOG(*ctx, "secret.lookup",
		        {{"endpoint", endpoint_},
		         {"type", type},
		         {"outcome", fr.found ? "found" : "not_found"},
		         {"duration_ms", std::to_string(duration_ms)}});

		return fr;
	} catch (const std::exception &e) {
		VGI_LOG(*ctx, "secret.lookup",
		        {{"endpoint", endpoint_}, {"type", type}, {"outcome", "error"}, {"error", e.what()}});
		throw IOException("VGI remote secret lookup failed [endpoint: %s, type: %s]: %s", endpoint_, type, e.what());
	}
}

std::unique_ptr<const KeyValueSecret>
VgiRemoteSecretStorage::Synthesize(ClientContext &context, const string &type, const std::string &provider,
                                   const std::string &name, const std::vector<std::string> &scope,
                                   const std::shared_ptr<arrow::RecordBatch> &values_batch,
                                   const std::vector<std::string> &redact_keys) {
	vector<string> prefix_paths(scope.begin(), scope.end());
	if (prefix_paths.empty()) {
		prefix_paths.push_back(""); // catch-all
	}
	std::string secret_name = name.empty() ? ("vgi_orchard_" + type) : name;
	std::string secret_provider = provider.empty() ? std::string("orchard") : provider;

	auto secret = make_uniq<KeyValueSecret>(prefix_paths, type, secret_provider, secret_name);

	// Convert the one-row values RecordBatch → DuckDB DataChunk via the Arrow→DuckDB
	// bridge, then read each column's row-0 cell as a fully-typed Value (string,
	// int64, bool, struct, list, nested — whatever the worker shipped). Same
	// pattern as the column-statistics decode in vgi_catalog_api.cpp.
	if (values_batch && values_batch->num_rows() >= 1) {
		ArrowSchemaWrapper schema_root;
		ExportSchema(values_batch->schema(), schema_root);

		vector<LogicalType> col_types;
		ArrowTableSchema arrow_table;
		vector<std::string> col_names;
		for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(schema_root.arrow_schema.n_children); col_idx++) {
			auto &schema_item = *schema_root.arrow_schema.children[col_idx];
			auto arrow_type = ArrowType::GetArrowLogicalType(context, schema_item);
			col_types.push_back(arrow_type->GetDuckType());
			col_names.emplace_back(schema_item.name);
			arrow_table.AddColumn(col_idx, std::move(arrow_type), schema_item.name);
		}

		auto array_wrapper = make_uniq<ArrowArrayWrapper>();
		ExportRecordBatch(values_batch, *array_wrapper);

		DataChunk values_chunk;
		values_chunk.Initialize(Allocator::Get(context), col_types,
		                        static_cast<idx_t>(array_wrapper->arrow_array.length));
		values_chunk.SetCardinality(static_cast<idx_t>(array_wrapper->arrow_array.length));

		ArrowScanLocalState fake_local_state(std::move(array_wrapper), context);
		ArrowTableFunction::ArrowToDuckDB(fake_local_state, arrow_table.GetColumns(), values_chunk, false);
		values_chunk.Verify();

		for (idx_t col_idx = 0; col_idx < col_names.size(); col_idx++) {
			secret->secret_map[col_names[col_idx]] = values_chunk.GetValue(col_idx, 0);
		}
	}

	for (const auto &rk : redact_keys) {
		secret->redact_keys.insert(rk);
	}
	return std::move(secret);
}

unique_ptr<SecretEntry> VgiRemoteSecretStorage::GetSecretByName(const string &name,
                                                               optional_ptr<CatalogTransaction> transaction) {
	std::lock_guard<std::mutex> lk(cache_mutex_);
	const auto now = std::chrono::steady_clock::now();
	for (auto &kv : positive_cache_) {
		if (kv.second.expires_at <= now) {
			continue;
		}
		if (kv.second.secret->GetName() == name) {
			return make_uniq<SecretEntry>(MakeEntry(kv.second.secret->Clone(), GetName()));
		}
	}
	return nullptr;
}

vector<SecretEntry> VgiRemoteSecretStorage::AllSecrets(optional_ptr<CatalogTransaction> transaction) {
	vector<SecretEntry> result;
	std::lock_guard<std::mutex> lk(cache_mutex_);
	const auto now = std::chrono::steady_clock::now();
	for (auto &kv : positive_cache_) {
		if (kv.second.expires_at <= now) {
			continue;
		}
		result.push_back(MakeEntry(kv.second.secret->Clone(), GetName()));
	}
	return result;
}

unique_ptr<SecretEntry> VgiRemoteSecretStorage::StoreSecret(unique_ptr<const BaseSecret> secret,
                                                           OnCreateConflict on_conflict,
                                                           optional_ptr<CatalogTransaction> transaction) {
	throw NotImplementedException(
	    "CREATE SECRET (STORAGE %s) is not supported: the VGI remote secret provider only serves "
	    "credentials fetched from its Orchard endpoint; it cannot store user-created secrets.",
	    GetName());
}

void VgiRemoteSecretStorage::DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
                                              optional_ptr<CatalogTransaction> transaction) {
	// No-op: nothing user-droppable lives here. Must not throw — DROP SECRET and
	// ambiguity scans call this on every storage.
}

void VgiRemoteSecretStorage::Deactivate() {
	active_.store(false);
	FlushCache();
}

idx_t VgiRemoteSecretStorage::FlushCache() {
	std::lock_guard<std::mutex> lk(cache_mutex_);
	const idx_t dropped = positive_cache_.size();
	positive_cache_.clear();
	negative_cache_.clear();
	return dropped;
}

idx_t VgiRemoteSecretStorage::CachedSecretCount() {
	std::lock_guard<std::mutex> lk(cache_mutex_);
	const auto now = std::chrono::steady_clock::now();
	idx_t count = 0;
	for (auto &kv : positive_cache_) {
		if (kv.second.expires_at > now) {
			count++;
		}
	}
	return count;
}

} // namespace vgi
} // namespace duckdb
