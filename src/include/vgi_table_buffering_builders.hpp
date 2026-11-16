// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>


namespace duckdb {
namespace vgi {

// Buffered-table (table_buffering_*) RPC request builders, shared by the
// subprocess, HTTP and web-worker connections. Each builds the inner request
// record, serializes it, and wraps it in the outer RPC params batch
// (generated BuildTableBuffering*Params).
//
//   * process() is unary; the worker-chosen state_id is on the response, not
//     the request. state_ids are opaque bytes.
//   * schema_name names the catalog schema declaring the function: a function
//     name is unique only within a schema, so without it a worker re-resolving
//     by bare name can run a different schema's implementation than the bind
//     picked. Empty serializes as null ("no schema", e.g. COPY TO), which the
//     worker treats as a cross-schema lookup.
VgiBatchPtr
BuildTableBufferingProcessInner(const std::string &function_name,
                                  const std::string &schema_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<uint8_t> &input_batch_bytes,
                                  const std::vector<uint8_t> &attach_opaque_data,
                                  std::optional<int64_t> batch_index = std::nullopt);

VgiBatchPtr
BuildTableBufferingCombineInner(const std::string &function_name,
                                  const std::string &schema_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<std::vector<uint8_t>> &state_ids,
                                  const std::vector<uint8_t> &attach_opaque_data);

VgiBatchPtr
BuildTableBufferingDestructorInner(const std::string &function_name,
                                     const std::string &schema_name,
                                     const std::vector<uint8_t> &execution_id,
                                     const std::vector<uint8_t> &attach_opaque_data);

} // namespace vgi
} // namespace duckdb
