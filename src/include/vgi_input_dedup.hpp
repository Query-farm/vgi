// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Input dedup for exchange-mode map operators (scalar / streaming table-in-out /
// batched correlated LATERAL): group rows by distinct worker-input tuple (the
// function's args, not outer columns) so only distinct tuples are shipped and the
// results scattered/expanded back. See docs/exchange_dedup_pervalue.md.

#include <cstdint>
#include <vector>

#include "duckdb/common/constants.hpp" // idx_t, column_t
#include "duckdb/common/types/selection_vector.hpp"

namespace duckdb {

class DataChunk;

namespace vgi {

struct InputDedup {
	idx_t n = 0;         // original row count
	idx_t k = 0;         // worker-input group count (<= n)
	bool trivial = true; // identity mapping: all unique, or optimization bypassed

	// Length k: a representative ORIGINAL row index for each distinct tuple. Use with a
	// DataChunk slice to build the k-row deduped input to ship to the worker.
	SelectionVector distinct;
	// Length n: for each original row, the distinct-tuple index d ∈ [0, k) it belongs to.
	// Scatter for a 1:1 map: result[row] = worker_out[orig_to_distinct[row]].
	std::vector<idx_t> orig_to_distinct;
	// The original row indices sharing each distinct tuple (the inverse map), stored flat:
	// group d is group_rows[group_offsets[d], group_offsets[d + 1]), rows ascending.
	// Expansion for a 1:N map: a worker output row whose parent is d fans out to every
	// original row in Group(d), stamped with THAT original row's outer columns.
	std::vector<idx_t> group_offsets; // length k + 1
	std::vector<idx_t> group_rows;    // length n

	struct GroupRows {
		const idx_t *first;
		const idx_t *last;
		const idx_t *begin() const {
			return first;
		}
		const idx_t *end() const {
			return last;
		}
		idx_t size() const {
			return static_cast<idx_t>(last - first);
		}
		bool empty() const {
			return first == last;
		}
		idx_t operator[](idx_t i) const {
			return first[i];
		}
	};
	GroupRows Group(idx_t d) const {
		const idx_t *rows = group_rows.data();
		return GroupRows {rows + group_offsets[d], rows + group_offsets[d + 1]};
	}
};

// Internal work limits; bypassing returns a complete identity mapping.
struct InputDedupOptions {
	idx_t large_input_bytes = 4 * 1024 * 1024;
	idx_t sample_work_bytes = 8 * 1024 * 1024;
	idx_t max_work_bytes = 512 * 1024 * 1024;
	idx_t max_comparisons = 65536;
};

// Groups by `key_cols` (indices into `chunk`); NULLs group together. VARCHAR/BLOB
// and bitwise-comparable fixed-width columns (integers, dates, timestamps,
// decimals, UUIDs, enums) are hashed and compared in place; other types (floats,
// intervals, nested) use CreateSortKey. Hash matches are always confirmed exactly.
// Large string inputs are sampled first; poor sampled reduction or an exhausted
// work budget returns an identity mapping, which may contain duplicates.
InputDedup BuildInputDedup(DataChunk &chunk, const std::vector<column_t> &key_cols,
                           const InputDedupOptions &options = InputDedupOptions());

} // namespace vgi
} // namespace duckdb
