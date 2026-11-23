// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>


#include "vgi_protocol.hpp"
#include "vgi_rpc_types.hpp" // ArgumentsLocationRef

namespace duckdb {

class ClientContext;
class Value;

namespace vgi {

struct VgiSecretRequirement;
struct CopyFromBindContext;
struct CopyToBindContext;

// Sends serialized BindRequest bytes over the transport (subprocess, HTTP, ...)
// and returns the transport-validated BindResponse batch.
using BindTransportFn = std::function<VgiBatchPtr(const std::vector<uint8_t> &request_bytes)>;

// Argument references (protocol 3.4, docs/large-values-3.4.md §4). Given the
// bind's serialized arguments IPC, either returns the location to send instead
// of the bytes (having uploaded them, or reused an upload from this query), or
// nullopt to send them inline. Only the HTTP and Iroh transports supply one;
// it is called once per bind, before the first BindRequest is built.
using ArgumentsExternalizerFn =
    std::function<std::optional<ArgumentsLocationRef>(const std::vector<uint8_t> &arguments_ipc)>;

// Full bind: send the BindRequest, and if the worker answers with a secret-scope
// request, resolve those secrets and resend once (a second request is an error).
BindResult PerformBindProtocol(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const VgiBatchPtr &arguments_batch,
    const VgiSchemaPtr &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::string &worker_label,  // for error messages (worker_path or base_url)
    const BindTransportFn &transport_fn,
    const std::string &at_unit = {},    // time travel; empty = null
    const std::string &at_value = {},   // time travel; empty = null
    const CopyFromBindContext *copy_from = nullptr,  // COPY FROM; null = omit
    const CopyToBindContext *copy_to = nullptr,      // COPY TO; null = omit
    const std::string &schema_name = {},  // owning catalog schema; empty = null
    const std::optional<std::vector<std::optional<std::string>>> &argument_names = std::nullopt,
    const ArgumentsExternalizerFn &externalize_arguments = nullptr);  // null = always inline

// The IPC BindRequest bytes PerformBindProtocol sends. Also used by the inline-
// bind path, which has the BindResponse already but still needs identical
// request bytes for cardinality / statistics / init / dynamic_to_string.
std::vector<uint8_t> BuildBindRequestBytes(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const VgiBatchPtr &arguments_batch,
    const VgiSchemaPtr &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets,
    bool resolved_secrets_provided,
    const std::string &worker_label,
    const std::string &at_unit = {},    // time travel; empty = null
    const std::string &at_value = {},   // time travel; empty = null
    const CopyFromBindContext *copy_from = nullptr,  // COPY FROM; null = omit
    const CopyToBindContext *copy_to = nullptr,      // COPY TO; null = omit
    const std::string &schema_name = {},  // owning catalog schema; empty = null
    const std::optional<std::vector<std::optional<std::string>>> &argument_names = std::nullopt);

// Builds a BindResult from an inlined bind_response blob (TableInfo.bind_result)
// without a bind RPC. `resolved_secrets` must be the map that built
// `bind_request_bytes`: init hands it to the worker, so the result cache keys
// on it. Throws IOException if the blob is malformed or is a secret-scope
// request (only the RPC path can resolve one); callers fall back to the RPC.
BindResult BuildBindResultFromInlinedBytes(std::vector<uint8_t> bind_request_bytes,
                                           const std::vector<uint8_t> &bind_response_bytes,
                                           const std::string &worker_label, bool secret_dependent,
                                           const std::map<std::string, std::map<std::string, Value>> &resolved_secrets);

} // namespace vgi
} // namespace duckdb
