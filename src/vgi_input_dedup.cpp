// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_input_dedup.hpp"

#include <cstring>
#include <string_view>
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

namespace {
struct DedupBudgetExceeded {};

struct KeyView {
	std::vector<UnifiedVectorFormat> columns;
	idx_t bytes_left;
	idx_t comparisons_left;

	void Charge(idx_t bytes) {
		if (bytes > bytes_left) {
			throw DedupBudgetExceeded();
		}
		bytes_left -= bytes;
	}

	size_t HashRow(idx_t row) {
		size_t hash = 0;
		for (const auto &column : columns) {
			Charge(1);
			auto index = column.sel->get_index(row);
			size_t part = 0x9e3779b9;
			if (column.validity.RowIsValid(index)) {
				auto value = column.GetData<string_t>()[index];
				Charge(value.GetSize());
				part = std::hash<std::string_view>()(std::string_view(value.GetData(), value.GetSize()));
			}
			hash ^= part + 0x9e3779b9 + (hash << 6) + (hash >> 2);
		}
#ifdef VGI_DEDUP_TEST_HASH_COLLISIONS
		// Only the separate collision-test executable sets this definition.
		return 0;
#else
		return hash;
#endif
	}

	bool EqualRows(idx_t left, idx_t right) {
		if (comparisons_left == 0) {
			throw DedupBudgetExceeded();
		}
		comparisons_left--;
		for (const auto &column : columns) {
			Charge(1);
			auto l = column.sel->get_index(left), r = column.sel->get_index(right);
			const bool lv = column.validity.RowIsValid(l), rv = column.validity.RowIsValid(r);
			if (lv != rv) {
				return false;
			}
			if (!lv || l == r) {
				continue;
			}
			auto values = column.GetData<string_t>();
			auto a = values[l], b = values[r];
			if (a.GetSize() != b.GetSize()) {
				return false;
			}
			if (a.GetData() == b.GetData()) {
				continue;
			}
			Charge(a.GetSize());
			if (std::memcmp(a.GetData(), b.GetData(), a.GetSize()) != 0) {
				return false;
			}
		}
		return true;
	}
};

struct RowHash {
	KeyView *keys;
	size_t operator()(idx_t row) const {
		return keys->HashRow(row);
	}
};
struct RowEqual {
	KeyView *keys;
	bool operator()(idx_t a, idx_t b) const {
		return keys->EqualRows(a, b);
	}
};
using RowMap = std::unordered_map<idx_t, idx_t, RowHash, RowEqual>;

bool SampleWorthwhile(KeyView &keys, idx_t n, const InputDedupOptions &options) {
	keys.bytes_left = options.sample_work_bytes;
	keys.comparisons_left = options.max_comparisons;
	RowMap seen(0, RowHash {&keys}, RowEqual {&keys});
	const idx_t samples = MinValue<idx_t>(64, n);
	seen.reserve(samples);
	// Jitter each stratum: a fixed stride aliases periodic repeated inputs.
	uint64_t random = 0x6a09e667f3bcc909ULL;
	for (idx_t i = 0; i < samples; i++) {
		random ^= random << 13;
		random ^= random >> 7;
		random ^= random << 17;
		const idx_t begin = i * n / samples, end = (i + 1) * n / samples;
		const idx_t row = begin + random % (end - begin);
		seen.emplace(row, 0);
	}
	// Require 1/8 duplicate sample rows. A bad estimate only forgoes savings.
	return (samples - seen.size()) * 8 >= samples;
}
} // namespace

InputDedup BuildInputDedup(DataChunk &chunk, const std::vector<column_t> &key_cols, const InputDedupOptions &options) {
	const idx_t n = chunk.size();
	if (n == 0 || key_cols.empty() || n > 65536 || key_cols.size() > 2048) {
		return TrivialDedup(n);
	}
	KeyView keys;
	keys.columns.reserve(key_cols.size() + 1);
	vector<column_t> canonical_columns;
	vector<LogicalType> canonical_types;
	bool large = false;
	idx_t payload_left = options.large_input_bytes;
	for (auto c : key_cols) {
		auto &input = chunk.data[c];
		const auto type = input.GetType().id();
		if (type != LogicalTypeId::VARCHAR && type != LogicalTypeId::BLOB) {
			canonical_columns.push_back(c);
			canonical_types.push_back(input.GetType());
			continue;
		}
		keys.columns.emplace_back();
		input.ToUnifiedFormat(n, keys.columns.back());
		const auto &column = keys.columns.back();
		for (idx_t row = 0; row < n && !large; row++) {
			auto index = column.sel->get_index(row);
			if (column.validity.RowIsValid(index)) {
				auto size = column.GetData<string_t>()[index].GetSize();
				if (size > payload_left) {
					large = true;
				} else {
					payload_left -= size;
				}
			}
		}
	}

	// Canonical semantics for the remaining columns (including nested types).
	// The vector outlives the lookup. No second owned copy of its keys is made.
	Vector canonical_keys(LogicalType::BLOB);
	if (!canonical_columns.empty()) {
		DataChunk key_chunk;
		key_chunk.InitializeEmpty(canonical_types);
		key_chunk.SetCapacity(chunk.GetCapacity());
		for (idx_t i = 0; i < canonical_columns.size(); i++) {
			key_chunk.data[i].Reference(chunk.data[canonical_columns[i]]);
		}
		key_chunk.SetCardinality(n);
		vector<OrderModifiers> mods(canonical_columns.size(),
		                            OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
		CreateSortKeyHelpers::CreateSortKey(key_chunk, mods, canonical_keys);
		keys.columns.emplace_back();
		canonical_keys.ToUnifiedFormat(n, keys.columns.back());
	}

	try {
		if (large && !SampleWorthwhile(keys, n, options)) {
			return TrivialDedup(n);
		}
		keys.bytes_left = options.max_work_bytes;
		keys.comparisons_left = options.max_comparisons;
		RowMap seen(0, RowHash {&keys}, RowEqual {&keys});
		seen.reserve(n);
		InputDedup d;
		d.n = n;
		d.orig_to_distinct.resize(n);
		std::vector<idx_t> reps;
		reps.reserve(n);
		for (idx_t row = 0; row < n; row++) {
			auto inserted = seen.emplace(row, reps.size());
			auto distinct = inserted.first->second;
			if (inserted.second) {
				reps.push_back(row);
				d.groups.emplace_back();
			}
			d.groups[distinct].push_back(row);
			d.orig_to_distinct[row] = distinct;
		}
		d.k = reps.size();
		d.trivial = d.k == n;
		d.distinct.Initialize(d.k == 0 ? 1 : d.k);
		for (idx_t i = 0; i < d.k; i++) {
			d.distinct.set_index(i, reps[i]);
		}
		return d;
	} catch (const DedupBudgetExceeded &) {
		return TrivialDedup(n);
	}
}

} // namespace vgi
} // namespace duckdb
