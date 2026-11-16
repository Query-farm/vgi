// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <string>


#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_rpc_client.hpp"

namespace duckdb {
namespace vgi {

class CatalogAuth;             // vgi_oauth.hpp
class SessionCookieJar;        // vgi_cookie_jar.hpp
class VgiHttpClientPool;       // vgi_http_client.hpp
class ServerCapabilitiesCache; // vgi_server_capabilities.hpp
struct IrohClientConfig;

// Options for a pooled unary RPC invocation. Several call sites aggregate-
// initialize the fields up to enable_logging positionally: add new fields
// after it.
struct UnaryRpcOptions {
	ClientContext &context;
	std::string worker_path;
	bool worker_debug = false;
	bool use_pool = true;
	// Pool-key dimensions with worker_path: a pooled worker is reused only when
	// all three match, so a worker attached at one version never serves another.
	std::string data_version_spec;
	std::string implementation_version;
	// Caller tag logged on pool acquire/release (e.g. "rpc_catalog").
	std::string phase;
	// HTTP only.
	std::shared_ptr<CatalogAuth> auth;
	// HTTP cookie jar for sticky-session routing. Null on subprocess transport
	// and for pre-attach RPCs that have no per-catalog state yet.
	std::shared_ptr<SessionCookieJar> cookie_jar;
	// False skips every VGI_LOG, in-band log forwarding and
	// StderrDrainer::DrainToLog. Use only for best-effort calls off the query
	// thread during pipeline teardown (aggregate destructors), where the
	// ClientContext logger is not safe to call.
	bool enable_logging = true;
	// Per-catalog HTTPParams, reused so each call does not re-enter the secret
	// manager (and its MetaTransaction mutex; duckdb/duckdb#22258 deadlock).
	std::shared_ptr<HTTPParams> cached_http_params;
	// launch: overrides, passed to ResolveAndConnect (which pins them to the
	// worker's lifetime and rejects conflicts). nullopt = LaunchConfig default.
	std::optional<std::chrono::milliseconds> launcher_idle_timeout;
	std::optional<std::string> launcher_state_dir;
	// Application protocol addressed: selects the vgi_rpc.protocol routing key
	// (the URL's protocol segment on HTTP) and the version stamped with it.
	VgiProtocolId protocol = VGI_MAIN_PROTOCOL;
	// Per-catalog keep-alive client pool (http:// only): a client is checked
	// out per call and returned on success. Null = fresh connection per call.
	std::shared_ptr<VgiHttpClientPool> http_client_pool;
	// Per-catalog capability snapshot (VgiAttachParameters::server_caps), read
	// to pick the request Content-Encoding and stored back from the response.
	// Null = each call starts from the pre-discovery default (zstd). HTTP only.
	std::shared_ptr<ServerCapabilitiesCache> server_caps;
	// Explicit SOCKS5h proxy for tcp://. Transport-only: never serialized as a
	// worker/catalog option.
	std::string tcp_proxy;
	// Immutable native Iroh identity/relay configuration captured by ATTACH.
	// Null for all non-Iroh transports and for browser Iroh (page-owned adapter).
	std::shared_ptr<IrohClientConfig> iroh;
};

// Send one unary RPC on whatever transport worker_path names, then resolve an
// externalized (vgi_rpc.location) result.
//
// Subprocess transport: the worker comes from the worker pool (or a fresh
// spawn) and its stderr is drained on a background thread for the whole call,
// so a worker cannot block on a full stderr pipe; it goes back to the pool
// with its drainer. On IOException a method that permits replay is retried
// once on a fresh worker; RequiresSingleAttemptUnary methods are not.
// Other transports connect per call (launch:/unix:// via ResolveAndConnect).
//
// params may be nullptr, in which case an empty RPC request is sent.
UnaryResponseResult InvokePooledUnaryRpc(const UnaryRpcOptions &opts, const std::string &method_name,
                                          const VgiBatchPtr &params);

} // namespace vgi
} // namespace duckdb
