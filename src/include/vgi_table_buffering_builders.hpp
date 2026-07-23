// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

namespace duckdb {
namespace vgi {

// ============================================================================
// Buffered-table RPC inner-request builders + outer-response decoder
// ============================================================================
// Shared between FunctionConnection (subprocess) and HttpFunctionConnection.
// Each Build*Inner function constructs the inner RecordBatch matching the
// worker's expected schema, serializes it to IPC bytes, and wraps the bytes
// in the outer RPC envelope (BuildTableBuffering*Params from the generated
// header). Callers serialize the resulting outer batch with WriteRpcRequest
// (subprocess) or HttpInvokeUnary (HTTP).

// ----------------------------------------------------------------------------
// Table sink+source builders.
//
// Wire-shape differences from table_buffering_*:
//   * process() is UNARY; the worker-chosen state_id is on the *response*,
//     not the request. There is no state_id field in the request.
//   * state_ids / finalize_state_ids are opaque bytes, not int64.
//   * No transaction_id field today, but the schema reserves it (nullable
//     binary) so future routing can attach without a wire bump.
//
//   * schema_name names the catalog schema that declares the function. A
//     function name is unique only within a schema, so without it a worker
//     re-resolving these unary calls by bare name can run a *different*
//     schema's implementation than the bind picked. Empty string serialises
//     as null — the legitimate "caller names no schema" form (COPY TO, and
//     any path with no schema to name), which the worker treats as a
//     cross-schema lookup.
//
// Inner schemas match TableBuffering{Process,Combine,Destructor}Request in
// vgi-python/vgi/protocol.py.
std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingProcessInner(const std::string &function_name,
                                  const std::string &schema_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<uint8_t> &input_batch_bytes,
                                  const std::vector<uint8_t> &attach_opaque_data,
                                  std::optional<int64_t> batch_index = std::nullopt);

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingCombineInner(const std::string &function_name,
                                  const std::string &schema_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<std::vector<uint8_t>> &state_ids,
                                  const std::vector<uint8_t> &attach_opaque_data);

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingDestructorInner(const std::string &function_name,
                                     const std::string &schema_name,
                                     const std::vector<uint8_t> &execution_id,
                                     const std::vector<uint8_t> &attach_opaque_data);

} // namespace vgi
} // namespace duckdb
