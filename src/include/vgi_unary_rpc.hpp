#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

#include "duckdb/main/client_context.hpp"

#include "vgi_rpc_client.hpp"

namespace duckdb {
namespace vgi {

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;

// Options for a pooled unary RPC invocation.
// context outlives the call; worker_path/phase may be string_view-like but we
// copy for safety since errors may propagate to another thread.
struct UnaryRpcOptions {
	ClientContext &context;
	std::string worker_path;
	bool worker_debug = false;
	bool use_pool = true;
	// Logged via VGI_LOG on acquire/release events (e.g. "rpc_catalog",
	// "aggregate_update"). Helps distinguish pool traffic by caller.
	std::string phase;
	// Forwarded to HttpInvokeUnary for HTTP transport. Ignored for subprocess.
	std::shared_ptr<CatalogAuth> auth;
};

// Send a single unary RPC and return the response.
//
// Subprocess transport:
//   - Acquires a worker from the pool (or spawns fresh on miss).
//   - Drains the worker's stderr in a background thread for the duration of
//     the call — prevents the Python worker from blocking on stderr when the
//     pipe buffer fills (the bug this function was extracted to fix).
//   - Sends the request, reads the response.
//   - Returns the worker to the pool with stderr fd preserved so the pool's
//     cleanup thread can close it when the worker is eventually evicted.
//   - On IOException from a *pooled* worker, retries once with a fresh spawn
//     (matches AcquireAndBindConnection's stale-pool semantics).
//
// HTTP transport: dispatches to HttpInvokeUnary (no pool involvement).
//
// params may be nullptr, in which case an empty RPC request is sent.
UnaryResponseResult InvokePooledUnaryRpc(const UnaryRpcOptions &opts, const std::string &method_name,
                                          const std::shared_ptr<arrow::RecordBatch> &params);

} // namespace vgi
} // namespace duckdb
