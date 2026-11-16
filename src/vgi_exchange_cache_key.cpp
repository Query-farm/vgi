// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_exchange_cache_key.hpp"

#include <algorithm>
#include <chrono>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp" // FileSystem positioned reads (disk-tier serve)
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/main/client_context.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_cache_control.hpp"
#include "vgi_memo_arena.hpp" // per-value arena registry (byte budget)
#include "vgi_oauth.hpp"
#include "vgi_result_cache.hpp"
#include "vgi_sha256.hpp"
#include "vgi_table_in_out_impl.hpp"

namespace duckdb {
namespace vgi {

namespace {

std::string Sha256Hex(const std::string &bytes) {
	return VgiSha256Hex(bytes); // WASM-safe (native mbedtls / emscripten engine primitive)
}

} // namespace

std::string SerializeSettingsForKey(const std::map<std::string, Value> &settings) {
	std::string out;
	for (const auto &kv : settings) { // std::map iterates in sorted key order
		out += kv.first;
		out += '=';
		out += kv.second.ToString();
		out += ';';
	}
	return out;
}

std::string SerializeProjectionForKey(const std::vector<int32_t> &projection_ids) {
	std::string out;
	for (auto id : projection_ids) {
		out += std::to_string(id);
		out += ',';
	}
	return out;
}

std::string SerializeArgumentsForKey(const VgiBatchPtr &arguments_batch) {
	if (!arguments_batch) {
		return "";
	}
	return Sha256Hex(SerializeRecordBatch(arguments_batch)->ToString());
}


std::string HashInputBatchOrdered(const VgiBatchPtr &input_batch) {
	if (!input_batch) {
		return "";
	}
	// Deterministic: no message custom_metadata is written.
	auto buffer = SerializeRecordBatch(input_batch);
	return Sha256Hex(buffer->ToString());
}

std::string HashInputChunkUnordered(ClientContext & /*context*/, DataChunk &chunk) {
	const idx_t count = chunk.size();
	if (count == 0 || chunk.ColumnCount() == 0) {
		// All empty / column-less chunks share one constant hash.
		return Sha256Hex("vgi-exchange-empty-input");
	}
	// One sort-key blob per row over ALL columns (see the header).
	vector<OrderModifiers> mods(chunk.ColumnCount(), OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	Vector keys(LogicalType::BLOB);
	CreateSortKeyHelpers::CreateSortKey(chunk, mods, keys);
	keys.Flatten(count);
	auto key_data = FlatVector::GetData<string_t>(keys);

	// Sorting makes it order-independent; duplicates are kept.
	std::vector<std::string> rows;
	rows.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		rows.emplace_back(key_data[i].GetData(), key_data[i].GetSize());
	}
	std::sort(rows.begin(), rows.end());

	// Layout: count ":" then "len:bytes" per row.
	std::string buf;
	buf += std::to_string(static_cast<uint64_t>(count));
	buf += ":";
	for (auto &r : rows) {
		buf += std::to_string(r.size());
		buf += ":";
		buf += r;
	}
	return VgiSha256Hex(buf);
}

std::string CanonicalPartitionTupleKey(ClientContext & /*context*/, const std::vector<Value> &tuple,
                                       const std::vector<LogicalType> &partition_types) {
	D_ASSERT(tuple.size() == partition_types.size());
	// The cast matters on the serve side: filter constants may arrive as a different
	// (coercible) literal type than the declared partition type.
	DataChunk chunk;
	duckdb::vector<LogicalType> dtypes(partition_types.begin(), partition_types.end());
	chunk.Initialize(Allocator::DefaultAllocator(), dtypes, 1);
	for (idx_t c = 0; c < partition_types.size(); c++) {
		Value v = tuple[c].DefaultCastAs(partition_types[c]);
		chunk.SetValue(c, 0, v);
	}
	chunk.SetCardinality(1);
	vector<OrderModifiers> mods(chunk.ColumnCount(),
	                            OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	Vector keys(LogicalType::BLOB);
	CreateSortKeyHelpers::CreateSortKey(chunk, mods, keys);
	keys.Flatten(1);
	auto key_data = FlatVector::GetData<string_t>(keys);
	return std::string(key_data[0].GetData(), key_data[0].GetSize());
}

std::vector<std::string> HashInputRowsPerValue(ClientContext & /*context*/, DataChunk &chunk) {
	const idx_t count = chunk.size();
	std::vector<std::string> out(count);
	if (count == 0 || chunk.ColumnCount() == 0) {
		return out;
	}
	vector<OrderModifiers> mods(chunk.ColumnCount(), OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	Vector keys(LogicalType::BLOB);
	CreateSortKeyHelpers::CreateSortKey(chunk, mods, keys);
	keys.Flatten(count);
	auto key_data = FlatVector::GetData<string_t>(keys);
	for (idx_t i = 0; i < count; i++) {
		out[i] = "v:" + Sha256Hex(std::string(key_data[i].GetData(), key_data[i].GetSize()));
	}
	return out;
}

std::vector<std::string> InputRowSortKeys(ClientContext & /*context*/, DataChunk &chunk) {
	const idx_t count = chunk.size();
	std::vector<std::string> out(count);
	if (count == 0 || chunk.ColumnCount() == 0) {
		return out;
	}
	vector<OrderModifiers> mods(chunk.ColumnCount(), OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	Vector keys(LogicalType::BLOB);
	CreateSortKeyHelpers::CreateSortKey(chunk, mods, keys);
	keys.Flatten(count);
	auto key_data = FlatVector::GetData<string_t>(keys);
	for (idx_t i = 0; i < count; i++) {
		out[i].assign(key_data[i].GetData(), key_data[i].GetSize());
	}
	return out;
}

void AccumulateInputDigest(DataChunk &chunk, uint64_t &sum_lo, uint64_t &sum_hi, uint64_t &row_count) {
	const idx_t n = chunk.size();
	if (n == 0 || chunk.ColumnCount() == 0) {
		return;
	}
	// One hash_t per row over ALL columns (join-hashtable pattern).
	Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(chunk.data[0], hashes, n);
	for (idx_t c = 1; c < chunk.ColumnCount(); c++) {
		VectorOperations::CombineHash(hashes, chunk.data[c], n);
	}
	hashes.Flatten(n);
	auto h = FlatVector::GetData<hash_t>(hashes);
	for (idx_t i = 0; i < n; i++) {
		// Two lanes with different odd-constant mixes make additive collisions
		// astronomically unlikely while staying commutative/associative.
		sum_lo += static_cast<uint64_t>(h[i]);
		sum_hi += static_cast<uint64_t>(h[i]) * 0x9E3779B97F4A7C15ULL + 0x165667B19E3779F9ULL;
	}
	row_count += static_cast<uint64_t>(n);
}

std::string FinalizeInputDigest(uint64_t sum_lo, uint64_t sum_hi, uint64_t row_count) {
	// Row count discriminates multisets that happen to fold to the same sums.
	unsigned char buf[24];
	auto put = [&](uint64_t v, int off) {
		for (int i = 0; i < 8; i++) {
			buf[off + i] = static_cast<unsigned char>((v >> (8 * i)) & 0xFF);
		}
	};
	put(sum_lo, 0);
	put(sum_hi, 8);
	put(row_count, 16);
	return Sha256Hex(std::string(reinterpret_cast<const char *>(buf), sizeof(buf)));
}

void SyncResultCacheSettings(ClientContext &context) {
	VgiResultCache::Settings s;
	Value sv;
	auto u64 = [&](const char *name, uint64_t &dst) {
		if (context.TryGetCurrentSetting(name, sv) && !sv.IsNull()) {
			dst = sv.GetValue<uint64_t>();
		}
	};
	auto str = [&](const char *name, std::string &dst) {
		if (context.TryGetCurrentSetting(name, sv) && !sv.IsNull()) {
			dst = sv.GetValue<std::string>();
		}
	};
	u64("vgi_result_cache_max_bytes", s.max_bytes);
	u64("vgi_result_cache_max_entry_bytes", s.max_entry_bytes);
	u64("vgi_result_cache_max_entries", s.max_entries);
	u64("vgi_result_cache_max_inflight_bytes", s.max_inflight_bytes);
	str("vgi_result_cache_dir", s.disk_dir);
	u64("vgi_result_cache_disk_max_bytes", s.disk_max_bytes);
	u64("vgi_result_cache_disk_reap_interval_seconds", s.disk_reap_interval_seconds);
	str("vgi_result_cache_disk_compression", s.disk_compression);
	u64("vgi_result_cache_disk_compression_level", s.disk_compression_level);
	u64("vgi_result_cache_exchange_disk_max_refs", s.exchange_disk_max_refs);
	auto boolean = [&](const char *name, bool &dst) {
		if (context.TryGetCurrentSetting(name, sv) && !sv.IsNull()) {
			dst = sv.GetValue<bool>();
		}
	};
	boolean("vgi_result_cache_pack", s.pack);
	u64("vgi_result_cache_pack_max_entry_bytes", s.pack_max_entry_bytes);
	u64("vgi_result_cache_pack_target_bytes", s.pack_target_bytes);
	u64("vgi_result_cache_pack_compaction_dead_pct", s.pack_compaction_dead_pct);
	GetResultCache(context).ConfigureIfChanged(s);
	// The memo arena has its own budget of the same size — NOT a combined cap.
	GetMemoArenaRegistry(context).SetMaxBytes(static_cast<int64_t>(s.max_bytes));
	// vgi_result_cache_dir also enables the per-value disk tier, capped by
	// vgi_result_cache_per_value_disk_max_bytes (0 = unlimited).
	uint64_t pv_disk_max = defaults::RESULT_CACHE_PER_VALUE_DISK_MAX_BYTES;
	Value pdmv;
	if (context.TryGetCurrentSetting("vgi_result_cache_per_value_disk_max_bytes", pdmv) && !pdmv.IsNull()) {
		pv_disk_max = pdmv.GetValue<uint64_t>();
	}
	GetMemoArenaRegistry(context).EnsureSqliteBackend(s.disk_dir, static_cast<int64_t>(pv_disk_max));
}

bool BuildExchangeCacheKeyStaticFields(ClientContext &context,
                                       const std::shared_ptr<VgiAttachParameters> &attach_params,
                                       const std::string &function_name, const std::string &schema_name,
                                       const std::string &canonical_arguments,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<int32_t> &projection_ids, bool secret_dependent,
                                       const std::string &secret_scope, VgiResultCacheKey &key,
                                       std::string &catalog_name, int64_t &catalog_version, const char *&reason,
                                       const std::string &operator_kind) {
	reason = nullptr;
	catalog_version = 0;

	SyncResultCacheSettings(context);

	Value master;
	if (context.TryGetCurrentSetting("vgi_result_cache", master) && !master.GetValue<bool>()) {
		reason = "disabled_global";
		return false;
	}
	if (!attach_params || !attach_params->cache()) {
		reason = "disabled_attach";
		return false;
	}
	// secret_scope is a fingerprint, never secret values. Without one there is
	// nothing to key on: fail closed.
	if (secret_dependent && secret_scope.empty()) {
		reason = "secret_fingerprint_missing";
		return false;
	}

	// A catalog-attached function is keyed on the runtime catalog_version (0 =
	// unknown, never cache; a bump or vgi_clear_cache() invalidates). Otherwise
	// fall back to worker-path identity, TTL-only (version 0).
	std::string identity_scope;
	const std::string &cat = attach_params->catalog_name();
	if (!cat.empty()) {
		auto catalog = Catalog::GetCatalogEntry(context, Identifier(cat));
		if (catalog && catalog->GetCatalogType() == "vgi") {
			auto &vcat = catalog->Cast<VgiCatalog>();
			catalog_version = vcat.GetKnownCatalogVersion();
			if (catalog_version == 0) {
				reason = "unknown_version";
				return false;
			}
			// SECURITY: identity_scope carries the auth principal; "" fails closed.
			identity_scope = BuildCatalogIdentityScope(cat, attach_params->auth());
			if (identity_scope.empty()) {
				reason = "identity_unresolved";
				return false;
			}
			catalog_name = cat;
		}
	}
	if (identity_scope.empty()) {
		std::string principal = ComputeCatalogIdentityFingerprint(attach_params->auth());
		if (principal.empty()) {
			reason = "identity_unresolved";
			return false;
		}
		identity_scope = "worker:" + attach_params->worker_path() + "\x1f" + principal;
		catalog_name = attach_params->worker_path();
	}

	key.identity_scope = identity_scope;
	key.worker_path = attach_params->worker_path();
	key.function_name = function_name;
	key.schema_name = schema_name;
	key.canonical_arguments = canonical_arguments;
	key.canonical_settings = SerializeSettingsForKey(settings);
	key.attach_options = attach_params->attach_options_canonical();
	key.projection = SerializeProjectionForKey(projection_ids);
	key.attached_data_version = attach_params->data_version_spec();
	key.implementation_version = attach_params->implementation_version();
	key.catalog_version = catalog_version;
	key.shape_key = operator_kind; // SHAPE discriminator — see VgiResultCacheKey::shape_key
	key.secret_scope = secret_dependent ? secret_scope : "";
	// Producer-only dimensions (at_unit/at_value, filter_bytes, order_by_hint,
	// sample_hint, transaction_id) stay empty; the caller sets input_hash.
	return true;
}

bool BuildExchangeCacheKeyStatic(ClientContext &context, const VgiTableInOutBindData &bd,
                                 const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                 std::string &catalog_name, int64_t &catalog_version, const char *&reason,
                                 const std::string &operator_kind) {
	return BuildExchangeCacheKeyStaticFields(context, bd.attach_params, bd.function_name, bd.schema_name,
	                                         SerializeArgumentsForKey(bd.arguments.batch), bd.settings,
	                                         projection_ids, bd.secret_dependent, bd.bind_result.secret_scope, key,
	                                         catalog_name, catalog_version, reason, operator_kind);
}

// Process-static: a FileHandle references the FileSystem that opened it, so the
// FS must outlive the database-owned replay entries.
namespace {
FileSystem &ExchangeReplayFs() {
	static std::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return *fs;
}
} // namespace

VgiBatchPtr DeserializeCachedRecordBatch(const VgiResultCacheEntry &entry,
                                                                 const VgiCachedBatch &cached) {
	// Lookup returns a disk entry TOC-only (ipc==nullptr) when its payload exceeds
	// max_entry_bytes.
	VgiBufferPtr ipc = cached.ipc;
	if (!ipc && cached.disk_ipc_offset >= 0) {
		auto handle = ExchangeReplayFs().OpenFile(entry.disk_path, FileFlags::FILE_FLAGS_READ);
		uint8_t *dst = nullptr;
		VgiBufferPtr b;
		try {
			b = VgiBuffer::Allocate(cached.disk_ipc_length, &dst);
		} catch (const std::bad_alloc &) {
			throw IOException("VGI exchange cache: failed to allocate %lld bytes for disk batch",
			                  static_cast<long long>(cached.disk_ipc_length));
		}
		ExchangeReplayFs().Read(*handle, dst, cached.disk_ipc_length, static_cast<idx_t>(cached.disk_ipc_offset));
		ipc = std::move(b);
	}
	if (!ipc) {
		throw IOException("VGI exchange cache: cached batch has neither in-memory nor on-disk IPC bytes");
	}
	std::unique_ptr<VgiIpcReader> reader;
	try {
		reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(ipc)); // zero-copy over `ipc`
	} catch (const std::exception &e) {
		ErrorData error(e);
		throw IOException("VGI exchange cache: failed to open cached batch IPC stream: %s", error.RawMessage());
	}
	VgiBatchPtr batch;
	try {
		batch = reader->ReadNext();
	} catch (const std::exception &) {
		batch = nullptr;
	}
	if (!batch) {
		throw IOException("VGI exchange cache: cached batch IPC stream was empty");
	}
	return batch;
}

void SlideRevalidatedExchangeEntry(VgiResultCache &cache, const VgiResultCacheEntry &entry, const VgiCacheControl &cc,
                                   int64_t /*default_ttl_seconds*/, bool allow_disk) {
	auto fresh = std::make_shared<VgiResultCacheEntry>(entry); // shallow copy (shared Buffers)
	fresh->stored_at = std::chrono::steady_clock::now();
	// A ttl on the not_modified batch wins; else reuse the PRIOR lifetime, so an
	// always-revalidate entry (lifetime 0) stays immediately stale. Do NOT fall
	// back to a positive default.
	int64_t ttl = cc.ttl_seconds.value_or(0);
	if (ttl <= 0 && !entry.never_expires) {
		auto prior =
		    std::chrono::duration_cast<std::chrono::seconds>(entry.expires_at - entry.stored_at).count();
		ttl = prior > 0 ? prior : 0;
	}
	if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
		ttl = VGI_CACHE_MAX_TTL_SECONDS;
	}
	if (!entry.never_expires) {
		fresh->expires_at = fresh->stored_at + std::chrono::seconds(ttl > 0 ? ttl : 0);
	}
	if (!cc.etag.empty()) {
		fresh->etag = cc.etag;
	}
	if (!cc.last_modified.empty()) {
		fresh->last_modified = cc.last_modified;
	}
	// Immediately-stale entries are memory-only: LookupForRevalidation probes only
	// the in-memory index, and a disk immediately-stale blob is un-loadable.
	bool eff_allow_disk = allow_disk && (entry.never_expires || fresh->expires_at > fresh->stored_at);
	cache.Insert(fresh, eff_allow_disk);
}

ExchangeStoreResult StoreExchangeMemoEntry(VgiResultCache &cache, const VgiResultCacheKey &key, const VgiCacheControl &cc,
                                           const std::string &catalog_name, int64_t default_ttl_seconds,
                                           const std::vector<VgiBatchPtr> &out_batches,
                                           bool allow_disk, bool allow_immediately_stale) {
	ExchangeStoreResult res;
	auto skip = [&](const char *r) {
		res.reason = r;
		return res;
	};
	if (!cc.present || cc.no_store || !cc.Cacheable()) {
		return skip("not_cacheable");
	}
	// Exchange caching supports catalog scope only; refuse transaction-scoped
	// results rather than risk serving them across transactions.
	if (cc.TransactionScoped()) {
		return skip("transaction_scoped");
	}
	int64_t ttl = cc.ttl_seconds.value_or(default_ttl_seconds);
	if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
		ttl = VGI_CACHE_MAX_TTL_SECONDS;
	}
	// ttl=0 is the always-revalidate (HTTP no-cache) contract: stored immediately
	// stale, memory-only, every read revalidates and a 304 reuses the bytes.
	const bool immediately_stale = ttl <= 0;
	if (immediately_stale && !allow_immediately_stale) {
		return skip("immediately_stale_per_value");
	}
	const bool revalidatable_no_cache = immediately_stale && cc.revalidatable && !cc.etag.empty();
	if (immediately_stale && !revalidatable_no_cache) {
		return skip("no_freshness");
	}
	const bool eff_allow_disk = allow_disk && !immediately_stale;

	auto entry = std::make_shared<VgiResultCacheEntry>();
	entry->key = key;
	entry->catalog_name = catalog_name;
	entry->scope = cc.scope;
	entry->etag = cc.etag;
	entry->last_modified = cc.last_modified;
	entry->revalidatable = cc.revalidatable;
	entry->stored_at = std::chrono::steady_clock::now();
	entry->expires_at = entry->stored_at + std::chrono::seconds(immediately_stale ? 0 : ttl);

	CachedStream stream;
	int64_t rows = 0;
	int64_t bytes = 0;
	for (const auto &b : out_batches) {
		if (!b) {
			continue;
		}
		auto buffer = SerializeRecordBatch(b); // self-contained IPC (schema + batch)
		VgiCachedBatch cb;
		cb.ipc = buffer;
		cb.rows = b->num_rows();
		rows += cb.rows;
		bytes += static_cast<int64_t>(buffer->size());
		stream.batches.push_back(std::move(cb));
	}
	stream.rows = rows;
	stream.bytes = bytes;
	entry->streams.push_back(std::move(stream));
	entry->rows = rows;
	entry->total_bytes = bytes;

	// No minimum size for disk: small-but-expensive results benefit most. File
	// fan-out is bounded by vgi_result_cache_exchange_disk_max_refs in the reaper.
	if (!cache.Insert(std::move(entry), eff_allow_disk)) {
		return skip("too_large_for_memory");
	}
	res.stored = true;
	res.rows = rows;
	res.bytes = bytes;
	return res;
}

} // namespace vgi
} // namespace duckdb
