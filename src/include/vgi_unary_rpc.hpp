#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include <arrow/api.h>

#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_rpc_client.hpp"

namespace duckdb {
namespace vgi {

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;
// Forward declaration — full definition in vgi_cookie_jar.hpp
class SessionCookieJar;

// Options for a pooled unary RPC invocation.
// context outlives the call; worker_path/phase may be string_view-like but we
// copy for safety since errors may propagate to another thread.
struct UnaryRpcOptions {
	ClientContext &context;
	std::string worker_path;
	bool worker_debug = false;
	bool use_pool = true;
	// Additional pool-key dimensions. A pooled worker may only be reused when
	// all three of (worker_path, data_version_spec, implementation_version)
	// match — otherwise a worker attached at one version could silently serve
	// another version's query.
	std::string data_version_spec;
	std::string implementation_version;
	// Logged via VGI_LOG on acquire/release events (e.g. "rpc_catalog",
	// "aggregate_update"). Helps distinguish pool traffic by caller.
	std::string phase;
	// Forwarded to HttpInvokeUnary for HTTP transport. Ignored for subprocess.
	std::shared_ptr<CatalogAuth> auth;
	// HTTP cookie jar for sticky-session routing. Null on subprocess transport
	// and for pre-attach RPCs that have no per-catalog state yet.
	std::shared_ptr<SessionCookieJar> cookie_jar;
	// When false, the RPC path skips all VGI_LOG calls (pool acquire/release,
	// stale events) and skips StderrDrainer::DrainToLog. Set false only for
	// best-effort calls that run off the main thread during pipeline teardown
	// (aggregate destructors), where DuckDB's ClientContext-backed logger is
	// not guaranteed safe to invoke. Buffered stderr stays on the drainer and
	// will be drained the next time the pooled worker is used.
	bool enable_logging = true;
	// Per-catalog HTTPParams cache. Populated at ATTACH (or lazily on first
	// HTTP RPC) and reused by all subsequent HTTP requests for this catalog —
	// avoids re-entering the secret manager (and its MetaTransaction mutex) on
	// each call. TODO(#22258): drop this field once the upstream DuckDB bug is
	// fixed (https://github.com/duckdb/duckdb/issues/22258). Placed last so
	// that positional aggregate-init call sites (see vgi_aggregate_function_impl)
	// don't need changes.
	std::shared_ptr<HTTPParams> cached_http_params;
	// Per-LOCATION launcher overrides — only meaningful when ``worker_path``
	// is a ``launch:`` URL.  Forwarded into ``ResolveAndConnect`` where the
	// cache layer pins them to the worker's lifetime and throws
	// ``BinderException`` on conflicting subsequent ATTACHes.  Both nullopt
	// → use ``LaunchConfig`` defaults.
	std::optional<std::chrono::milliseconds> launcher_idle_timeout;
	std::optional<std::string> launcher_state_dir;
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
