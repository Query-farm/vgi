#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "vgi_protocol.hpp"

namespace duckdb {

class ClientContext;
class Value;

namespace vgi {

struct VgiSecretRequirement;

// ============================================================================
// Bind Protocol Orchestration
// ============================================================================

// Callback type: given serialized bind request bytes, sends them via transport
// and returns the deserialized bind response RecordBatch.
using BindTransportFn = std::function<std::shared_ptr<arrow::RecordBatch>(const std::vector<uint8_t> &request_bytes)>;

// Orchestrates the full bind protocol:
// 1. Serialize arguments, settings, input_schema, secrets
// 2. Build and send initial BindRequest via transport_fn
// 3. Handle two-phase bind (scoped secrets) if requested by worker
// 4. Parse final BindResponse → BindResult
//
// The transport_fn abstracts away subprocess vs HTTP send/receive.
// It receives serialized BindRequest bytes and must return the deserialized
// BindResponse RecordBatch (after validating the transport-level response).
BindResult PerformBindProtocol(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const std::shared_ptr<arrow::Array> &arguments_array,
    const std::shared_ptr<arrow::Schema> &input_schema,
    const std::vector<uint8_t> &attach_id,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::string &worker_label,  // for error messages (worker_path or base_url)
    const BindTransportFn &transport_fn);

} // namespace vgi
} // namespace duckdb
