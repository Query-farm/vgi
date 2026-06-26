// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
struct CopyFromBindContext;

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
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::string &worker_label,  // for error messages (worker_path or base_url)
    const BindTransportFn &transport_fn,
    const std::string &at_unit = {},    // time travel; empty = null
    const std::string &at_value = {},   // time travel; empty = null
    const CopyFromBindContext *copy_from = nullptr);  // COPY FROM; null = omit

// Build the IPC-serialized BindRequest bytes that PerformBindProtocol sends
// over the wire. Factored out so the inline-bind path (which has the
// pre-built BindResponse but still needs RPC-equivalent bind_request_bytes
// for cardinality / statistics / init / dynamic_to_string) can produce the
// same bytes without going through the network.
//
// `resolved_secrets` is the already-resolved secrets map (typed as a
// {key: Value} for symmetry with the rest of the bind path); the function
// constructs the secrets batch from it.
std::vector<uint8_t> BuildBindRequestBytes(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const std::shared_ptr<arrow::Array> &arguments_array,
    const std::shared_ptr<arrow::Schema> &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets,
    bool resolved_secrets_provided,
    const std::string &worker_label,
    const std::string &at_unit = {},    // time travel; empty = null
    const std::string &at_value = {},   // time travel; empty = null
    const CopyFromBindContext *copy_from = nullptr);  // COPY FROM; null = omit

// Non-network entrypoint: given pre-built bind_request_bytes (typically from
// `BuildBindRequestBytes`) and an inlined bind_response blob (from
// `TableInfo.bind_result`), return a populated `BindResult` ready to hand
// to PerformInit / cardinality / statistics. Skips the wire RPC entirely.
//
// Throws IOException if the inlined blob is malformed or carries a
// secret-scope request (which can only be resolved over the RPC path).
// Callers should catch and fall back to the on-demand bind RPC.
BindResult BuildBindResultFromInlinedBytes(
    std::vector<uint8_t> bind_request_bytes,
    const std::vector<uint8_t> &bind_response_bytes,
    const std::string &worker_label);

} // namespace vgi
} // namespace duckdb
