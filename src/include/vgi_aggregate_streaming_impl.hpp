#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/main/client_context.hpp"

#include "vgi_aggregate_function_impl.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Streaming-partitioned aggregate protocol
// ============================================================================
// Three unary RPCs: aggregate_streaming_open, _chunk, _close.
//
// Used by the streaming-window physical operator that bypasses DuckDB's
// PhysicalWindow entirely — the operator pipes input chunks straight to the
// worker (no partition materialisation), and the worker maintains per-
// partition state in-process for the duration of the session, dispatching
// each row to its partition's state and returning a same-length output array
// of cumulative snapshots per chunk.

// Session token for an open streaming-partitioned aggregate. Returned by
// VgiAggregateStreamingOpen, threaded through every subsequent _chunk call,
// and surrendered by _close. Carries the worker-issued execution_id plus
// the attach/function identity needed to dispatch follow-up RPCs.
struct VgiStreamingSession {
	std::vector<uint8_t> execution_id;
	std::string function_name;
	std::vector<uint8_t> attach_id;
};

// Open a streaming-partitioned session. Sends AggregateStreamingOpenRequest
// and returns the worker's session token.
//
// input_schema describes the chunks that subsequent _chunk calls will ship.
// Column layout: [partition_key_cols..., order_key_cols..., value_cols...].
// partition_key_count and order_key_count tell the worker how to split the
// columns; remaining columns are the function's positional arguments.
VgiStreamingSession VgiAggregateStreamingOpen(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const std::shared_ptr<arrow::Schema> &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count);

// Process one chunk through the streaming session. Sends
// AggregateStreamingChunkRequest with input_batch (Arrow IPC bytes) and
// returns the deserialised one-column RecordBatch with one row per input row.
std::shared_ptr<arrow::RecordBatch> VgiAggregateStreamingChunk(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    const std::shared_ptr<arrow::RecordBatch> &input_batch);

// End the session and free the worker-side state. Idempotent — calling
// against a closed/unknown execution_id is a no-op on the worker side.
//
// enable_logging mirrors InvokeAggregateRpc's flag — destructor-flavored
// callers running on task-scheduler threads during pipeline teardown
// should pass false.
void VgiAggregateStreamingClose(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    bool enable_logging = true);

} // namespace vgi
} // namespace duckdb
