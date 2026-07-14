// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_exchange_cache_key.hpp"

#include <algorithm>
#include <chrono>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/main/client_context.hpp"

#include "mbedtls_wrapper.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_cache_control.hpp"
#include "vgi_oauth.hpp"
#include "vgi_result_cache.hpp"
#include "vgi_table_in_out_impl.hpp"

namespace duckdb {
namespace vgi {

namespace {

std::string HexEncode(const std::string &raw) {
	static const char *digits = "0123456789abcdef";
	std::string hex(raw.size() * 2, '0');
	for (size_t i = 0; i < raw.size(); i++) {
		hex[i * 2] = digits[(static_cast<unsigned char>(raw[i]) >> 4) & 0xF];
		hex[i * 2 + 1] = digits[static_cast<unsigned char>(raw[i]) & 0xF];
	}
	return hex;
}

std::string Sha256Hex(const std::string &bytes) {
	return HexEncode(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
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

	duckdb_mbedtls::MbedTlsWrapper::SHA256State sha;
	sha.AddString(std::to_string(static_cast<uint64_t>(count)));
	sha.AddString(":");
	for (auto &r : rows) {
		sha.AddString(std::to_string(r.size()));
		sha.AddString(":");
		sha.AddString(r);
	}
	char hex[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT];
	sha.FinishHex(hex);
	return std::string(hex, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT);
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

bool BuildExchangeCacheKeyStatic(ClientContext &context, const VgiTableInOutBindData &bd,
                                 const std::vector<int32_t> &projection_ids, VgiResultCacheKey &key,
                                 std::string &catalog_name, int64_t &catalog_version, const char *&reason) {
	reason = nullptr;
	catalog_version = 0;

	// Global master switch.
	Value master;
	if (context.TryGetCurrentSetting("vgi_result_cache", master) && !master.GetValue<bool>()) {
		reason = "disabled_global";
		return false;
	}
	// Per-catalog opt-out.
	if (!bd.attach_params || !bd.attach_params->cache()) {
		reason = "disabled_attach";
		return false;
	}

	// Identity + version. Catalog-attached functions resolve a VgiCatalog by name so
	// the runtime catalog_version gates freshness (0 = unknown → never cache; a bump
	// or vgi_clear_cache() invalidates). A catalog-less call falls back to worker-path
	// identity, TTL-only (version stays 0 = "no version dimension").
	std::string identity_scope;
	const std::string &cat = bd.attach_params->catalog_name();
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
			identity_scope = BuildCatalogIdentityScope(cat, bd.attach_params->auth());
			if (identity_scope.empty()) {
				reason = "identity_unresolved";
				return false;
			}
			catalog_name = cat;
		}
	}
	if (identity_scope.empty()) {
		std::string principal = ComputeCatalogIdentityFingerprint(bd.attach_params->auth());
		if (principal.empty()) {
			reason = "identity_unresolved";
			return false;
		}
		identity_scope = "worker:" + bd.worker_path() + "\x1f" + principal;
		catalog_name = bd.worker_path();
	}

	key.identity_scope = identity_scope;
	key.worker_path = bd.worker_path();
	key.function_name = bd.function_name;
	key.canonical_arguments = bd.arguments.array ? bd.arguments.array->ToString() : "";
	key.canonical_settings = SerializeSettingsForKey(bd.settings);
	key.attach_options = bd.attach_params->attach_options_canonical();
	key.projection = SerializeProjectionForKey(projection_ids);
	key.attached_data_version = bd.attach_params->data_version_spec();
	key.implementation_version = bd.attach_params->implementation_version();
	key.catalog_version = catalog_version;
	// Producer-only dimensions stay empty for exchange-mode: at_unit/at_value,
	// filter_bytes, order_by_hint, sample_hint, transaction_id. input_hash is set by
	// the caller per memoization event.
	return true;
}

// ----------------------------------------------------------------------------
// Per-unit memoization serve / store
// ----------------------------------------------------------------------------

std::shared_ptr<arrow::RecordBatch> DeserializeCachedRecordBatch(const VgiCachedBatch &cached) {
	// Exchange entries are memory-only (allow_disk=false), so the in-RAM IPC blob is
	// always present. Deserialize the self-contained per-batch IPC stream.
	if (!cached.ipc) {
		throw IOException("VGI exchange cache: cached batch has no in-memory IPC bytes");
	}
	auto input = std::make_shared<arrow::io::BufferReader>(cached.ipc);
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

ExchangeStoreResult StoreExchangeMemoEntry(const VgiResultCacheKey &key, const VgiCacheControl &cc,
                                           const std::string &catalog_name, int64_t default_ttl_seconds,
                                           const std::vector<std::shared_ptr<arrow::RecordBatch>> &out_batches,
                                           bool allow_disk) {
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
	// v1: a positive ttl is required (expires-only advertisements fall back to the
	// default; if that is also 0 there is no freshness basis → refuse).
	if (ttl <= 0) {
		return skip("no_freshness");
	}

	auto entry = std::make_shared<VgiResultCacheEntry>();
	entry->key = key;
	entry->catalog_name = catalog_name;
	entry->scope = cc.scope;
	entry->etag = cc.etag;
	entry->last_modified = cc.last_modified;
	entry->revalidatable = cc.revalidatable;
	entry->stored_at = std::chrono::steady_clock::now();
	entry->expires_at = entry->stored_at + std::chrono::seconds(ttl);

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

	if (!VgiResultCache::Instance().Insert(std::move(entry), allow_disk)) {
		return skip("too_large_for_memory");
	}
	res.stored = true;
	res.rows = rows;
	res.bytes = bytes;
	return res;
}

} // namespace vgi
} // namespace duckdb
