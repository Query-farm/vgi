#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/window/window_aggregator.hpp"

#include "vgi_aggregate_function_impl.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// VgiAggregateWindowLocalState
// ============================================================================
// Placement-new'd into DuckDB's l_state buffer. The buffer is sized by
// VgiAggregateStateSize() which returns max(sizeof(VgiAggregateState),
// sizeof(VgiAggregateWindowLocalState)) so the same buffer serves both
// code paths. Discriminated by the tag field (must be at offset 0, same
// as VgiAggregateState::tag).

struct VgiAggregateWindowLocalState {
	VgiAggregateStateTag tag = VgiAggregateStateTag::WINDOW;
	// Assigned in VgiAggregateWindowInit from the ExecState counter.
	int64_t partition_id = -1;
	// Cached bind_data pointer so the per-row window callback doesn't need
	// to re-fetch from AggregateInputData.
	const VgiAggregateBindData *bind_data = nullptr;
};

// ============================================================================
// Window callbacks
// ============================================================================

// Called once per partition with the full partition data. Assigns a
// partition_id, ships the partition via aggregate_window_init RPC, and
// stashes the partition_id on the l_state.
void VgiAggregateWindowInit(AggregateInputData &aggr_input_data,
                             const WindowPartitionInput &partition,
                             data_ptr_t g_state);

// Called once per output row. Sends aggregate_window RPC with
// (rid, subframes) and writes the returned scalar into result[rid].
void VgiAggregateWindow(AggregateInputData &aggr_input_data,
                         const WindowPartitionInput &partition,
                         const_data_ptr_t g_state, data_ptr_t l_state,
                         const SubFrames &subframes, Vector &result, idx_t rid);

// Batched variant — called once per Evaluate() with frame bounds for all
// `count` output rows pre-computed. Issues a single aggregate_window_batch
// RPC that returns a count-row result batch, which is copied into `result`
// at positions [0..count). Reduces RPCs from N to ~N/STANDARD_VECTOR_SIZE.
void VgiAggregateWindowBatch(AggregateInputData &aggr_input_data,
                              const WindowPartitionInput &partition,
                              const_data_ptr_t g_state, data_ptr_t l_state,
                              const SubFrames *subframes_per_row, idx_t count,
                              Vector &result, idx_t row_idx);

} // namespace vgi
} // namespace duckdb
