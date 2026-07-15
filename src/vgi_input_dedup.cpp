// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_input_dedup.hpp"

#include <string>
#include <unordered_map>

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/create_sort_key.hpp"

namespace duckdb {
namespace vgi {

static InputDedup TrivialDedup(idx_t n) {
	InputDedup d;
	d.n = n;
	d.k = n;
	d.trivial = true;
	d.distinct.Initialize(n == 0 ? 1 : n);
	d.orig_to_distinct.resize(n);
	d.groups.resize(n);
	for (idx_t i = 0; i < n; i++) {
		d.distinct.set_index(i, i);
		d.orig_to_distinct[i] = i;
		d.groups[i].push_back(i);
	}
	return d;
}

InputDedup BuildInputDedup(DataChunk &chunk, const std::vector<column_t> &key_cols) {
	const idx_t n = chunk.size();
	if (n == 0 || key_cols.empty()) {
		return TrivialDedup(n);
	}

	// Canonical NULL-aware per-row sort key over the worker-input columns only. Build a
	// view chunk that References just those columns, then CreateSortKey over it.
	DataChunk key_chunk;
	vector<LogicalType> key_types;
	key_types.reserve(key_cols.size());
	for (auto c : key_cols) {
		key_types.push_back(chunk.data[c].GetType());
	}
	key_chunk.InitializeEmpty(key_types);
	for (idx_t i = 0; i < key_cols.size(); i++) {
		key_chunk.data[i].Reference(chunk.data[key_cols[i]]);
	}
	key_chunk.SetCardinality(n);

	vector<OrderModifiers> mods(key_cols.size(),
	                            OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	Vector keys(LogicalType::BLOB);
	CreateSortKeyHelpers::CreateSortKey(key_chunk, mods, keys);
	keys.Flatten(n);
	auto key_data = FlatVector::GetData<string_t>(keys);

	InputDedup d;
	d.n = n;
	d.orig_to_distinct.resize(n);
	std::vector<idx_t> reps; // representative original row per distinct tuple
	reps.reserve(n);
	std::unordered_map<std::string, idx_t> seen;
	seen.reserve(n * 2);
	for (idx_t i = 0; i < n; i++) {
		std::string key(key_data[i].GetData(), key_data[i].GetSize());
		auto it = seen.find(key);
		idx_t di;
		if (it == seen.end()) {
			di = reps.size();
			seen.emplace(std::move(key), di);
			reps.push_back(i);
			d.groups.emplace_back();
			d.groups.back().push_back(i);
		} else {
			di = it->second;
			d.groups[di].push_back(i);
		}
		d.orig_to_distinct[i] = di;
	}
	d.k = reps.size();
	d.trivial = (d.k == d.n);
	d.distinct.Initialize(d.k == 0 ? 1 : d.k);
	for (idx_t di = 0; di < d.k; di++) {
		d.distinct.set_index(di, reps[di]);
	}
	return d;
}

} // namespace vgi
} // namespace duckdb
