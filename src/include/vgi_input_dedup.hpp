// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Input dedup for exchange-mode MAP operators (scalar / streaming table-in-out /
// batched correlated LATERAL). Given an input chunk and which of its columns are the
// worker-input columns (the args passed to the function — NOT correlated/outer
// columns), group the rows by distinct worker-input tuple so the operator can ship
// only the distinct tuples to the worker and scatter/expand the results back. Pure
// compute optimization; no cache, no Arrow. See docs/exchange_dedup_pervalue.md.

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
	// Length k: the original row indices sharing each distinct tuple (the inverse map).
	// Expansion for a 1:N map: a worker output row whose parent is d fans out to every
	// original row in groups[d], stamped with THAT original row's outer columns.
	std::vector<std::vector<idx_t>> groups;
};

// Internal work limits; bypassing returns a complete identity mapping.
struct InputDedupOptions {
	idx_t large_input_bytes = 4 * 1024 * 1024;
	idx_t sample_work_bytes = 8 * 1024 * 1024;
	idx_t max_work_bytes = 512 * 1024 * 1024;
	idx_t max_comparisons = 65536;
};

// VARCHAR/BLOB input buffers are hashed and compared without payload copies.
// Large inputs are sampled before full-batch hashing. Exact equality is always
// required after hash matches. Other types retain the canonical-key semantics.
// Poor sampled reduction or exhausted work budgets return an identity mapping.
// Only `key_cols` (indices into `chunk`) participate. NULLs group together; nested
// and numeric types use CreateSortKey. An identity result may contain duplicates:
// callers must preserve all original rows when the optimization is bypassed.
InputDedup BuildInputDedup(DataChunk &chunk, const std::vector<column_t> &key_cols,
                           const InputDedupOptions &options = InputDedupOptions());

} // namespace vgi
} // namespace duckdb
