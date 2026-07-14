// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_exchange_cache_key.hpp"

#include <algorithm>

#include <arrow/api.h>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/main/client_context.hpp"

#include "mbedtls_wrapper.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_arrow_ipc.hpp"
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

} // namespace vgi
} // namespace duckdb
