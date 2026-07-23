// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_exchange_cache_key.hpp"

#include <algorithm>
#include <chrono>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

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

// ----------------------------------------------------------------------------
// Promoted canonical key-component serializers (single definition; was static in
// vgi_table_function_impl.cpp — that TU now includes this header and calls these).
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// Input hashing
// ----------------------------------------------------------------------------

std::string HashInputBatchOrdered(const std::shared_ptr<arrow::RecordBatch> &input_batch) {
	if (!input_batch) {
		return "";
	}
	auto buffer = SerializeRecordBatch(input_batch); // deterministic IPC-stream framing
	std::string bytes(reinterpret_cast<const char *>(buffer->data()), static_cast<size_t>(buffer->size()));
	return Sha256Hex(bytes);
}

std::string HashInputChunkUnordered(ClientContext & /*context*/, DataChunk &chunk) {
	const idx_t count = chunk.size();
	if (count == 0 || chunk.ColumnCount() == 0) {
		// Empty / column-less input: a stable constant hash (all such chunks are
		// interchangeable for keying). Distinct from any non-empty chunk.
		return Sha256Hex("vgi-exchange-empty-input");
	}
	// One canonical, NULL-aware, all-types order-preserving blob per row over ALL
	// columns of the chunk (correlated columns included — see the LATERAL note).
	vector<OrderModifiers> mods(chunk.ColumnCount(), OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	Vector keys(LogicalType::BLOB);
	CreateSortKeyHelpers::CreateSortKey(chunk, mods, keys);
	keys.Flatten(count);
	auto key_data = FlatVector::GetData<string_t>(keys);

	// Collect the per-row blobs and sort them → canonical multiset serialization
	// (order-independent; duplicates preserved so cardinality is part of identity).
	std::vector<std::string> rows;
	rows.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		rows.emplace_back(key_data[i].GetData(), key_data[i].GetSize());
	}
	std::sort(rows.begin(), rows.end());

	// Serialize the sorted multiset into one buffer and hash it once — byte-identical
	// to streaming the same (count ":" then per-row "len:bytes") sequence, and WASM-safe
	// (see VgiSha256Hex). The rows are already materialized above, so one-shot vs
	// incremental is equivalent here (this is a per-chunk key, not a large blob).
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
	// One-row chunk of the partition columns in declared order. Cast each Value to the
	// declared partition type so the capture side (min-value decoded from
	// vgi_partition_values, already the declared Arrow type) and the serve side (filter
	// constants, which may arrive as a coercible literal type) produce identical bytes.
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
		// "v:" discriminator: a per-batch/-chunk input_hash is pure hex, so this prefix
		// guarantees per-value and per-unit keys never collide in the shared keyspace.
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
		// Raw canonical blob — the arena compares it by value; no SHA, no prefix.
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

// ----------------------------------------------------------------------------
// Static key builder
// ----------------------------------------------------------------------------

// Sync the process-global cache config (byte caps, disk dir/caps, compression) from
// this query's settings into the singleton, so SET takes effect on the exchange path
// exactly as it does on the producer path (which does this before eligibility). Without
// this the singleton's disk_dir stays empty and allow_disk=true is a silent no-op.
// ConfigureIfChanged is lock-free when the settings are unchanged (the steady state).
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
	VgiResultCache::Instance().ConfigureIfChanged(s);
	// The per-value memo arena is a separate registry with its own byte budget; it shares
	// the whole-cache max_bytes so `SET vgi_result_cache_max_bytes` bounds it too.
	VgiMemoArenaRegistry::Instance().SetMaxBytes(static_cast<int64_t>(s.max_bytes));
	// `SET vgi_result_cache_dir` turns on the per-value disk tier (same-host multi-process
	// + cross-restart warm reuse); `..._per_value_disk_max_bytes` caps its on-disk size
	// (LRU eviction + expired-row reaping), 0 = unlimited.
	uint64_t pv_disk_max = 0;
	Value pdmv;
	if (context.TryGetCurrentSetting("vgi_result_cache_per_value_disk_max_bytes", pdmv) && !pdmv.IsNull()) {
		pv_disk_max = pdmv.GetValue<uint64_t>();
	}
	VgiMemoArenaRegistry::Instance().EnsureSqliteBackend(s.disk_dir, static_cast<int64_t>(pv_disk_max));
}

bool BuildExchangeCacheKeyStaticFields(ClientContext &context,
                                       const std::shared_ptr<VgiAttachParameters> &attach_params,
                                       const std::string &function_name, const std::string &schema_name,
                                       const std::string &canonical_arguments,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                       std::string &catalog_name, int64_t &catalog_version,
                                       const char *&reason, const std::string &operator_kind) {
	reason = nullptr;
	catalog_version = 0;

	// Apply cache config from this query's settings (disk tier, caps) to the singleton.
	SyncResultCacheSettings(context);

	// Global master switch.
	Value master;
	if (context.TryGetCurrentSetting("vgi_result_cache", master) && !master.GetValue<bool>()) {
		reason = "disabled_global";
		return false;
	}
	// Per-catalog opt-out.
	if (!attach_params || !attach_params->cache()) {
		reason = "disabled_attach";
		return false;
	}

	// Identity + version. Catalog-attached functions resolve a VgiCatalog by name so
	// the runtime catalog_version gates freshness (0 = unknown → never cache; a bump
	// or vgi_clear_cache() invalidates). A catalog-less call falls back to worker-path
	// identity, TTL-only (version stays 0 = "no version dimension").
	std::string identity_scope;
	const std::string &cat = attach_params->catalog_name();
	if (!cat.empty()) {
		auto catalog = Catalog::GetCatalogEntry(context, cat);
		if (catalog && catalog->GetCatalogType() == "vgi") {
			auto &vcat = catalog->Cast<VgiCatalog>();
			catalog_version = vcat.GetKnownCatalogVersion();
			if (catalog_version == 0) {
				reason = "unknown_version";
				return false;
			}
			// SECURITY-CRITICAL: fold the caller's auth principal into identity_scope
			// so two identities never share a cache entry. "" → fail closed.
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
	// Producer-only dimensions stay empty for exchange-mode: at_unit/at_value,
	// filter_bytes, order_by_hint, sample_hint, transaction_id. input_hash is set by
	// the caller per memoization event.
	return true;
}

bool BuildExchangeCacheKeyStatic(ClientContext &context, const VgiTableInOutBindData &bd,
                                 const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                 std::string &catalog_name, int64_t &catalog_version, const char *&reason,
                                 const std::string &operator_kind) {
	return BuildExchangeCacheKeyStaticFields(context, bd.attach_params, bd.function_name, bd.schema_name,
	                                         bd.arguments.array ? bd.arguments.array->ToString() : "",
	                                         bd.settings, projection_ids, key, catalog_name, catalog_version,
	                                         reason, operator_kind);
}

// ----------------------------------------------------------------------------
// Per-unit memoization serve / store
// ----------------------------------------------------------------------------

// Process-static local FileSystem for disk-backed replay. A FileHandle keeps a
// reference to the FileSystem that opened it, so the FS must outlive the handle —
// a function-local static (leaked, like the cache singleton) guarantees that.
namespace {
FileSystem &ExchangeReplayFs() {
	static std::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return *fs;
}
} // namespace

std::shared_ptr<arrow::RecordBatch> DeserializeCachedRecordBatch(const VgiResultCacheEntry &entry,
                                                                 const VgiCachedBatch &cached) {
	// In-RAM (memory tier or a materialized disk entry) uses cached.ipc directly. A
	// disk-STREAMING entry (Lookup returned it TOC-only for a payload > max_entry_bytes)
	// has ipc==nullptr; positioned-read just this batch's bytes from the blob file.
	std::shared_ptr<arrow::Buffer> ipc = cached.ipc;
	if (!ipc && cached.disk_ipc_offset >= 0) {
		auto handle = ExchangeReplayFs().OpenFile(entry.disk_path, FileFlags::FILE_FLAGS_READ);
		auto buf_result = arrow::AllocateBuffer(cached.disk_ipc_length);
		if (!buf_result.ok()) {
			throw IOException("VGI exchange cache: failed to allocate %lld bytes for disk batch",
			                  static_cast<long long>(cached.disk_ipc_length));
		}
		std::shared_ptr<arrow::Buffer> b = std::move(buf_result.ValueUnsafe());
		ExchangeReplayFs().Read(*handle, b->mutable_data(), cached.disk_ipc_length,
		                        static_cast<idx_t>(cached.disk_ipc_offset));
		ipc = b;
	}
	if (!ipc) {
		throw IOException("VGI exchange cache: cached batch has neither in-memory nor on-disk IPC bytes");
	}
	auto input = std::make_shared<arrow::io::BufferReader>(ipc);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		throw IOException("VGI exchange cache: failed to open cached batch IPC stream: %s",
		                  reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();
	auto next_result = reader->ReadNext();
	if (!next_result.ok() || !next_result.ValueUnsafe().batch) {
		throw IOException("VGI exchange cache: cached batch IPC stream was empty");
	}
	return next_result.ValueUnsafe().batch;
}

void SlideRevalidatedExchangeEntry(const VgiResultCacheEntry &entry, const VgiCacheControl &cc,
                                   int64_t /*default_ttl_seconds*/, bool allow_disk) {
	auto fresh = std::make_shared<VgiResultCacheEntry>(entry); // shallow copy (shared Buffers)
	fresh->stored_at = std::chrono::steady_clock::now();
	// A fresh ttl on the not_modified batch wins; else reuse the PRIOR lifetime so a
	// validator-only 304 slides forward — and, crucially, an always-revalidate entry
	// (prior lifetime 0 / immediately stale) stays immediately stale so it keeps
	// revalidating (do NOT fall to a positive default). Mirrors the producer.
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
	// An immediately-stale (always-revalidate) entry is memory-only: LookupForRevalidation
	// probes the in-memory index, and a disk immediately-stale blob is un-loadable.
	bool eff_allow_disk = allow_disk && (entry.never_expires || fresh->expires_at > fresh->stored_at);
	VgiResultCache::Instance().Insert(fresh, eff_allow_disk);
}

ExchangeStoreResult StoreExchangeMemoEntry(const VgiResultCacheKey &key, const VgiCacheControl &cc,
                                           const std::string &catalog_name, int64_t default_ttl_seconds,
                                           const std::vector<std::shared_ptr<arrow::RecordBatch>> &out_batches,
                                           bool allow_disk, bool allow_immediately_stale) {
	ExchangeStoreResult res;
	auto skip = [&](const char *r) {
		res.reason = r;
		return res;
	};
	if (!cc.present || cc.no_store || !cc.Cacheable()) {
		return skip("not_cacheable");
	}
	// v1: exchange caching supports catalog scope only. A transaction-scoped result
	// (output depends on transaction-local state) is process-/txn-ephemeral; refuse
	// rather than risk serving it across transactions.
	if (cc.TransactionScoped()) {
		return skip("transaction_scoped");
	}
	int64_t ttl = cc.ttl_seconds.value_or(default_ttl_seconds);
	if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
		ttl = VGI_CACHE_MAX_TTL_SECONDS;
	}
	// A positive ttl is required UNLESS this is the "always-revalidate" (HTTP no-cache)
	// contract: ttl=0 + a validator (etag) + revalidatable — stored but immediately
	// stale, so every read revalidates via a conditional request and a 304 reuses the
	// stored bytes. Such an entry is memory-only (LookupForRevalidation probes memory;
	// a disk immediately-stale blob is un-loadable).
	const bool immediately_stale = ttl <= 0;
	// B3: per-value callers refuse the immediately-stale contract outright — LookupBatch
	// has no revalidation path, so such an entry churns (stored then evicted every probe).
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
		auto buffer = SerializeRecordBatch(b); // self-contained IPC (schema+batch)
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

	// [S9] Every eligible exchange memo may go to disk regardless of payload size — a
	// small-but-expensive result is exactly where a warm disk cache pays off. Per-chunk
	// file fan-out is bounded instead by the reaper's exchange ref-count cap
	// (vgi_result_cache_exchange_disk_max_refs), not by excluding tiny entries here.
	if (!VgiResultCache::Instance().Insert(std::move(entry), eff_allow_disk)) {
		return skip("too_large_for_memory");
	}
	res.stored = true;
	res.rows = rows;
	res.bytes = bytes;
	return res;
}

} // namespace vgi
} // namespace duckdb
