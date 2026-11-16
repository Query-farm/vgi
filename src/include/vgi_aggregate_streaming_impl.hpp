// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <string>
#include <vector>


#include "duckdb/main/client_context.hpp"

#include "vgi_aggregate_function_impl.hpp"

namespace duckdb {
namespace vgi {

// Streaming-partitioned aggregate protocol: unary RPCs aggregate_streaming_open,
// _chunk and _close, used by the streaming-window operator that bypasses
// PhysicalWindow. Input chunks go straight to the worker (no partition
// materialisation); the worker keeps per-partition state for the session and
// returns one cumulative snapshot per input row.

// Session token from VgiAggregateStreamingOpen: the worker-issued execution_id
// plus the identity every follow-up _chunk / _close RPC must repeat.
struct VgiStreamingSession {
	std::vector<uint8_t> execution_id;
	std::string function_name;
	// Owning catalog schema, so _chunk / _close name the same (schema, name)
	// pair _open resolved. Empty serialises as null.
	std::string schema_name;
	std::vector<uint8_t> attach_opaque_data;
};

// input_schema describes the chunks later _chunk calls ship:
// [partition_key_cols..., order_key_cols..., value_cols...]; the counts tell the
// worker how to split them, and the remaining columns are positional arguments.
VgiStreamingSession VgiAggregateStreamingOpen(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiSchemaPtr &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count);

// Returns a one-column batch with exactly one row per input row (enforced).
VgiBatchPtr VgiAggregateStreamingChunk(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    const VgiBatchPtr &input_batch);

// Free the worker-side state; a closed/unknown execution_id is a worker no-op.
// Callers on task-scheduler threads during pipeline teardown (destructors)
// should pass enable_logging=false (see InvokeAggregateRpc).
void VgiAggregateStreamingClose(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    bool enable_logging = true);

} // namespace vgi
} // namespace duckdb
