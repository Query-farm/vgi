#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/main/client_context.hpp"

#include "vgi_rpc_client.hpp"

namespace duckdb {
namespace vgi {

// Content type for Arrow IPC streams over HTTP
constexpr const char *ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

// Invoke a unary RPC method over HTTP.
// worker_path includes the prefix, e.g. "http://localhost:8000/vgi".
// Posts to {worker_path}/{method_name}.
UnaryResponseResult HttpInvokeUnary(ClientContext &context,
                                     const std::string &worker_path,
                                     const std::string &method_name,
                                     const std::shared_ptr<arrow::RecordBatch> &params);

// POST Arrow IPC bytes to a URL, return raw response body bytes.
// Used for catalog, stream init, and exchange operations.
// Timeout is controlled by the vgi_http_timeout_seconds setting (default 300s).
std::string HttpPostArrowIpc(ClientContext &context,
                              const std::string &url,
                              const std::vector<uint8_t> &body);

} // namespace vgi
} // namespace duckdb
