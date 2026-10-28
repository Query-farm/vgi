// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_input_dedup.hpp"

#include <cstring>
#include <string_view>

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/hash.hpp"
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
	d.group_offsets.resize(n + 1);
	d.group_rows.resize(n);
	for (idx_t i = 0; i < n; i++) {
		d.distinct.set_index(i, i);
		d.orig_to_distinct[i] = i;
		d.group_offsets[i] = i;
		d.group_rows[i] = i;
	}
	d.group_offsets[n] = n;
	return d;
}

namespace {
struct DedupBudgetExceeded {};

// Types whose equality is bitwise equality of the physical value. Floats (NaN, -0.0),
// intervals (normalized months/days/micros), TIME_TZ and nested types are not: they keep
// the canonical sort-key path.
bool HasBitwiseEquality(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UUID:
	case LogicalTypeId::ENUM:
		return true;
	default:
		return false;
	}
}

struct KeyColumn {
	UnifiedVectorFormat format;
	idx_t width = 0; // fixed-width value size in bytes; 0 for string_t (VARCHAR/BLOB/sort key)
};

struct KeyView {
	std::vector<KeyColumn> columns;
	idx_t bytes_left;
	idx_t comparisons_left;

	void Charge(idx_t bytes) {
		if (bytes > bytes_left) {
			throw DedupBudgetExceeded();
		}
		bytes_left -= bytes;
	}

	template <class T>
	static void HashFixedColumn(const UnifiedVectorFormat &format, idx_t n, uint64_t *hashes) {
		// Bitwise view of the physical value (signedness is irrelevant to equality).
		auto data = reinterpret_cast<const T *>(format.data);
		for (idx_t row = 0; row < n; row++) {
			auto index = format.sel->get_index(row);
			uint64_t part = 0x9e3779b9;
			if (format.validity.RowIsValid(index)) {
				part = Hash<T>(data[index]);
			}
			hashes[row] ^= part + 0x9e3779b9 + (hashes[row] << 6) + (hashes[row] >> 2);
		}
	}

	// Row hashes for rows [0, n), one column at a time.
	void HashAll(idx_t n, uint64_t *hashes) {
		std::fill(hashes, hashes + n, uint64_t(0));
		for (const auto &column : columns) {
			Charge(n);
			auto &format = column.format;
			switch (column.width) {
			case 1:
				HashFixedColumn<uint8_t>(format, n, hashes);
				continue;
			case 2:
				HashFixedColumn<uint16_t>(format, n, hashes);
				continue;
			case 4:
				HashFixedColumn<uint32_t>(format, n, hashes);
				continue;
			case 8:
				HashFixedColumn<uint64_t>(format, n, hashes);
				continue;
			case 16:
				HashFixedColumn<uhugeint_t>(format, n, hashes);
				continue;
			default:
				break;
			}
			auto values = format.GetData<string_t>();
			for (idx_t row = 0; row < n; row++) {
				auto index = format.sel->get_index(row);
				uint64_t part = 0x9e3779b9;
				if (format.validity.RowIsValid(index)) {
					auto value = values[index];
					Charge(value.GetSize());
					part = std::hash<std::string_view>()(std::string_view(value.GetData(), value.GetSize()));
				}
				hashes[row] ^= part + 0x9e3779b9 + (hashes[row] << 6) + (hashes[row] >> 2);
			}
		}
#ifdef VGI_DEDUP_TEST_HASH_COLLISIONS
		// Only the separate collision-test executable sets this definition.
		std::fill(hashes, hashes + n, uint64_t(0));
#endif
	}

	static uint64_t HashFixed(const_data_ptr_t data, idx_t width, idx_t index) {
		switch (width) {
		case 1:
			return Hash<uint8_t>(data[index]);
		case 2:
			return Hash<uint16_t>(reinterpret_cast<const uint16_t *>(data)[index]);
		case 4:
			return Hash<uint32_t>(reinterpret_cast<const uint32_t *>(data)[index]);
		case 8:
			return Hash<uint64_t>(reinterpret_cast<const uint64_t *>(data)[index]);
		default:
			return Hash<uhugeint_t>(reinterpret_cast<const uhugeint_t *>(data)[index]);
		}
	}

	// One row's hash, for sampling. Matches HashAll.
	uint64_t HashRow(idx_t row) {
		uint64_t hash = 0;
		for (const auto &column : columns) {
			Charge(1);
			auto &format = column.format;
			auto index = format.sel->get_index(row);
			uint64_t part = 0x9e3779b9;
			if (format.validity.RowIsValid(index)) {
				if (column.width) {
					part = HashFixed(format.data, column.width, index);
				} else {
					auto value = format.GetData<string_t>()[index];
					Charge(value.GetSize());
					part = std::hash<std::string_view>()(std::string_view(value.GetData(), value.GetSize()));
				}
			}
			hash ^= part + 0x9e3779b9 + (hash << 6) + (hash >> 2);
		}
#ifdef VGI_DEDUP_TEST_HASH_COLLISIONS
		hash = 0;
#endif
		return hash;
	}

	template <class T>
	static bool FixedEqual(const_data_ptr_t data, idx_t l, idx_t r) {
		T a, b;
		std::memcpy(&a, data + l * sizeof(T), sizeof(T));
		std::memcpy(&b, data + r * sizeof(T), sizeof(T));
		return a == b;
	}

	static bool FixedEqual(const_data_ptr_t data, idx_t width, idx_t l, idx_t r) {
		switch (width) {
		case 1:
			return FixedEqual<uint8_t>(data, l, r);
		case 2:
			return FixedEqual<uint16_t>(data, l, r);
		case 4:
			return FixedEqual<uint32_t>(data, l, r);
		case 8:
			return FixedEqual<uint64_t>(data, l, r);
		default:
			return FixedEqual<uhugeint_t>(data, l, r);
		}
	}

	bool EqualRows(idx_t left, idx_t right) {
		if (comparisons_left == 0) {
			throw DedupBudgetExceeded();
		}
		comparisons_left--;
		for (const auto &column : columns) {
			Charge(1);
			auto &format = column.format;
			auto l = format.sel->get_index(left), r = format.sel->get_index(right);
			const bool lv = format.validity.RowIsValid(l), rv = format.validity.RowIsValid(r);
			if (lv != rv) {
				return false;
			}
			if (!lv || l == r) {
				continue;
			}
			if (column.width) {
				if (!FixedEqual(format.data, column.width, l, r)) {
					return false;
				}
				continue;
			}
			auto values = format.GetData<string_t>();
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

// Open-addressing table of distinct tuples, keyed by row. Slots are 8 bytes (hash tag +
// distinct id) in a per-thread buffer reused across chunks, so a 2048-row chunk probes a
// 32 KiB table that stays cache-resident and costs no per-row allocation. Only one
// table may be live per thread. Rows are < 65536 (see caller).
class DistinctTable {
public:
	DistinctTable(KeyView &keys, idx_t expected) : keys(keys), slots(SlotBuffer()) {
		idx_t capacity = 16;
		while (capacity < expected * 2) {
			capacity *= 2;
		}
		slots.assign(capacity, Slot {0, kEmpty});
		mask = capacity - 1;
		reps.reserve(expected);
	}

	// The distinct id of `row`'s tuple (row hash `row_hash`); a new tuple gets the next id.
	idx_t FindOrInsert(idx_t row, uint64_t row_hash) {
		const uint64_t hash = Spread(row_hash);
		const auto tag = static_cast<uint32_t>(hash >> 32);
		for (idx_t i = hash & mask;; i = (i + 1) & mask) {
			auto &slot = slots[i];
			if (slot.id == kEmpty) {
				slot = Slot {tag, static_cast<uint32_t>(reps.size())};
				reps.push_back(static_cast<uint32_t>(row));
				return reps.size() - 1;
			}
			if (slot.tag == tag && keys.EqualRows(reps[slot.id], row)) {
				return slot.id;
			}
		}
	}

	idx_t size() const {
		return reps.size();
	}
	// The first row seen for each distinct id.
	const std::vector<uint32_t> &Representatives() const {
		return reps;
	}

private:
	static constexpr uint32_t kEmpty = UINT32_MAX;
	struct Slot {
		uint32_t tag;
		uint32_t id;
	};
	static std::vector<Slot> &SlotBuffer() {
		thread_local std::vector<Slot> buffer;
		return buffer;
	}
	// Row hashes combine per-column parts without a final mix; spread them so the slot
	// index (low bits) and the tag (high bits) both depend on the whole hash.
	static uint64_t Spread(uint64_t h) {
		h ^= h >> 33;
		h *= 0xff51afd7ed558ccdULL;
		h ^= h >> 33;
		return h;
	}

	KeyView &keys;
	std::vector<Slot> &slots;
	idx_t mask;
	std::vector<uint32_t> reps;
};

bool SampleWorthwhile(KeyView &keys, idx_t n, const InputDedupOptions &options) {
	keys.bytes_left = options.sample_work_bytes;
	keys.comparisons_left = options.max_comparisons;
	const idx_t samples = MinValue<idx_t>(64, n);
	DistinctTable seen(keys, samples);
	// Jitter each stratum: a fixed stride aliases periodic repeated inputs.
	uint64_t random = 0x6a09e667f3bcc909ULL;
	for (idx_t i = 0; i < samples; i++) {
		random ^= random << 13;
		random ^= random >> 7;
		random ^= random << 17;
		const idx_t begin = i * n / samples, end = (i + 1) * n / samples;
		const idx_t row = begin + random % (end - begin);
		seen.FindOrInsert(row, keys.HashRow(row));
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
		if (HasBitwiseEquality(input.GetType())) {
			keys.columns.emplace_back();
			keys.columns.back().width = GetTypeIdSize(input.GetType().InternalType());
			input.ToUnifiedFormat(n, keys.columns.back().format);
			continue;
		}
		if (type != LogicalTypeId::VARCHAR && type != LogicalTypeId::BLOB) {
			canonical_columns.push_back(c);
			canonical_types.push_back(input.GetType());
			continue;
		}
		keys.columns.emplace_back();
		input.ToUnifiedFormat(n, keys.columns.back().format);
		const auto &column = keys.columns.back().format;
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
		for (idx_t i = 0; i < canonical_columns.size(); i++) {
			key_chunk.data[i].Reference(chunk.data[canonical_columns[i]]);
		}
		key_chunk.SetCardinalityUnsafe(n);
		vector<OrderModifiers> mods(canonical_columns.size(),
		                            OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
		CreateSortKeyHelpers::CreateSortKey(key_chunk, mods, canonical_keys);
		keys.columns.emplace_back();
		canonical_keys.ToUnifiedFormat(n, keys.columns.back().format);
	}

	try {
		if (large && !SampleWorthwhile(keys, n, options)) {
			return TrivialDedup(n);
		}
		keys.bytes_left = options.max_work_bytes;
		keys.comparisons_left = options.max_comparisons;
		DistinctTable seen(keys, n);
		InputDedup d;
		d.n = n;
		d.orig_to_distinct.resize(n);
		std::vector<uint64_t> hashes(n);
		keys.HashAll(n, hashes.data());
		for (idx_t row = 0; row < n; row++) {
			d.orig_to_distinct[row] = seen.FindOrInsert(row, hashes[row]);
		}
		const auto &reps = seen.Representatives();
		d.k = reps.size();
		d.trivial = d.k == n;
		d.distinct.Initialize(d.k == 0 ? 1 : d.k);
		for (idx_t i = 0; i < d.k; i++) {
			d.distinct.set_index(i, reps[i]);
		}
		// Counting sort of rows by distinct id: each group's rows stay ascending, so a
		// group's first row is its representative.
		d.group_offsets.assign(d.k + 1, 0);
		for (idx_t row = 0; row < n; row++) {
			d.group_offsets[d.orig_to_distinct[row] + 1]++;
		}
		for (idx_t i = 0; i < d.k; i++) {
			d.group_offsets[i + 1] += d.group_offsets[i];
		}
		d.group_rows.resize(n);
		std::vector<idx_t> next(d.group_offsets.begin(), d.group_offsets.end() - 1);
		for (idx_t row = 0; row < n; row++) {
			d.group_rows[next[d.orig_to_distinct[row]]++] = row;
		}
		return d;
	} catch (const DedupBudgetExceeded &) {
		return TrivialDedup(n);
	}
}

} // namespace vgi
} // namespace duckdb
