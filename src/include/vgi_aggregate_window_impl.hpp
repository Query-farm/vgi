// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <string>
#include <vector>


#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/window/window_aggregator.hpp"

#include "vgi_aggregate_function_impl.hpp"

namespace duckdb {
namespace vgi {

// Placement-new'd by VgiAggregateWindowInit into the window *global* state
// buffer (despite the name). VgiAggregateStateSize() sizes that buffer for the
// larger of this and VgiAggregateState; `tag` must stay at offset 0, matching
// VgiAggregateState::tag, so teardown can tell them apart.

struct VgiAggregateWindowLocalState {
	VgiAggregateStateTag tag = VgiAggregateStateTag::WINDOW;
	// Assigned in VgiAggregateWindowInit from the ExecState counter.
	int64_t partition_id = -1;
	const VgiAggregateBindData *bind_data = nullptr;
};

// Called once per partition with the full partition data. Assigns a
// partition_id, ships the partition via aggregate_window_init, and stores the
// partition_id in g_state.
void VgiAggregateWindowInit(AggregateInputData &aggr_input_data,
                             const WindowPartitionInput &partition,
                             data_ptr_t g_state);

// Per-row fallback: one aggregate_window RPC per output row. DuckDB prefers the
// batch callback below whenever it is registered (it always is), so this runs
// only on an engine that ignores window_batch.
void VgiAggregateWindow(AggregateInputData &aggr_input_data,
                         const WindowPartitionInput &partition,
                         const_data_ptr_t g_state, data_ptr_t l_state,
                         const SubFrames &subframes, Vector &result, idx_t rid);

// Batched variant: DuckDB calls it once per WindowCustomAggregator::Evaluate
// with all `count` rows' subframes; one aggregate_window_batch RPC fills
// result[0..count).
void VgiAggregateWindowBatch(AggregateInputData &aggr_input_data,
                              const WindowPartitionInput &partition,
                              const_data_ptr_t g_state, data_ptr_t l_state,
                              const SubFrames *subframes_per_row, idx_t count,
                              Vector &result, idx_t row_idx);

} // namespace vgi
} // namespace duckdb
