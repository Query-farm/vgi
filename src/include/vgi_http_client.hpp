// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_http_compression.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_server_capabilities.hpp"

namespace duckdb {
namespace vgi {

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;
// Forward declaration — full definition in vgi_cookie_jar.hpp
class SessionCookieJar;

// Content type for Arrow IPC streams over HTTP
constexpr const char *ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

// Invoke a unary RPC method over HTTP.
// worker_path includes the prefix, e.g. "http://localhost:8000/vgi".
// Posts to {worker_path}/{method_name}.
// auth: per-catalog auth state for bearer token injection and 401 handling.
// cookie_jar: per-catalog HTTP cookie store. Null skips Cookie / Set-Cookie
// handling entirely. Non-null means request carries a Cookie header built
// from the jar, and any Set-Cookie response headers update the jar.
// cached_http_params: optional per-catalog HTTPParams cache. When non-null,
// HttpPostArrowIpcInternal will reuse it instead of calling
// HTTPUtil::InitializeParameters, which would otherwise re-enter the secret
// manager on every RPC (deadlock hazard under the MetaTransaction mutex — see
// https://github.com/duckdb/duckdb/issues/22258). Callers that have a
// VgiAttachParameters should pass params->GetOrInitHttpParams(context, url).
// client_holder: optional caller-owned keep-alive HTTP client (see
// HttpPostArrowIpc). Same contract: one base URL per holder, never shared
// across threads. Null = fresh connection per call (previous behavior).
// caps: optional IN/OUT ServerCapabilities snapshot, forwarded verbatim to
// HttpPostArrowIpc — see its contract. Callers with a per-catalog
// ServerCapabilitiesCache should load into a local, pass it, and store it back,
// so the codec choice (in particular "this server takes identity bodies only")
// is learned once per catalog instead of once per RPC.
UnaryResponseResult HttpInvokeUnary(ClientContext &context,
                                     const std::string &worker_path,
                                     const std::string &method_name,
                                     const std::shared_ptr<arrow::RecordBatch> &params,
                                     const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                     const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                                     const std::shared_ptr<HTTPParams> &cached_http_params = nullptr,
                                     const std::string &invocation_id_hex = "",
                                     const std::string &attach_opaque_data_hex = "",
                                     const std::string &transaction_opaque_data_hex = "",
                                     const std::string &conn_id_hex = "",
                                     const std::string &protocol_version_override = "",
                                     duckdb::unique_ptr<HTTPClient> *client_holder = nullptr,
                                     ServerCapabilities *caps = nullptr);

// Small thread-safe pool of keep-alive HTTPClients for unary RPCs against ONE
// base URL (a pool instance lives on a catalog's VgiAttachParameters, whose
// worker_path is fixed). HTTPClient itself is not thread-safe and catalog RPCs
// arrive from many threads, so callers CHECK OUT a client into a thread-local
// holder (null on a cold pool — HttpPostArrowIpc lazily creates one), pass the
// holder through the request, and RETURN it only on success. A client whose
// request threw may hold a wedged TCP connection and is dropped instead
// (DuckDB's retry loop already refreshes the client on transport errors, but
// an exception that escapes means we can't trust its state).
class VgiHttpClientPool {
public:
	duckdb::unique_ptr<HTTPClient> Checkout() {
		std::lock_guard<std::mutex> lock(mu_);
		if (clients_.empty()) {
			return nullptr;
		}
		auto client = std::move(clients_.back());
		clients_.pop_back();
		return client;
	}
	void Return(duckdb::unique_ptr<HTTPClient> client) {
		if (!client) {
			return;
		}
		std::lock_guard<std::mutex> lock(mu_);
		if (clients_.size() < kMaxPooled) {
			clients_.push_back(std::move(client));
		}
	}

private:
	// Bounds idle keep-alive connections per catalog; excess returns are
	// simply dropped (closing the connection). 8 covers DuckDB's typical
	// catalog-RPC concurrency without holding sockets open needlessly.
	static constexpr size_t kMaxPooled = 8;
	std::mutex mu_;
	std::vector<duckdb::unique_ptr<HTTPClient>> clients_;
};

// POST Arrow IPC bytes to a URL, return raw response body bytes.
// Used for catalog, stream init, and exchange operations.
// Timeout is controlled by the vgi_http_timeout_seconds setting (default 300s).
// auth: per-catalog auth state for bearer token injection and 401 handling.
// cookie_jar: per-catalog HTTP cookie store (see HttpInvokeUnary).
// client_holder: optional caller-owned HTTP client cache. When non-null, the
// underlying DuckDB HTTPClient (and thus its keep-alive TCP connection) is
// created on first use and REUSED across calls instead of a fresh connection
// per request — the win on the chatty per-batch exchange path, where a 500k
// scan otherwise opens ~one TCP connection per 2048-row vector. The client is
// keyed by host:port, so a single holder is only valid for one host: pass a
// holder that is used exclusively for one base_url (e.g. a per-connection
// member), and NEVER share it across threads (HTTPClient is not thread-safe).
// Null (the default) preserves the previous fresh-connection-per-call behavior.
// harvested_caps: when non-null, an IN/OUT snapshot of what this server can do.
// On the way in, a discovered snapshot selects the request body's
// Content-Encoding — notably, a server that advertised an EMPTY
// VGI-Supported-Encodings (it speaks no compression) gets identity bodies. On
// the way out it is refreshed from the capability headers the server middleware
// stamps on every response, whatever its status, so callers discover
// ServerCapabilities from traffic they already generate instead of paying a
// separate HEAD /health round trip per connection. Pass the same per-catalog
// snapshot (see ServerCapabilitiesCache) on every call to keep the codec
// choice warm.
std::string HttpPostArrowIpc(ClientContext &context,
                              const std::string &url,
                              const std::vector<uint8_t> &body,
                              const std::shared_ptr<CatalogAuth> &auth = nullptr,
                              const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                              const std::shared_ptr<HTTPParams> &cached_http_params = nullptr,
                              duckdb::unique_ptr<HTTPClient> *client_holder = nullptr,
                              ServerCapabilities *harvested_caps = nullptr);

// HTTP GET raw bytes from a URL. Used for fetching externalized batches.
// Handles X-VGI-Content-Encoding decompression (zstd or gzip). No auth
// headers sent.
std::string HttpGetBytes(ClientContext &context, const std::string &url);

// Resolve an external location pointer batch by fetching and parsing the URL.
// Returns the resolved data batch. Throws on redirect loops or fetch failures.
// worker_path, invocation_id_hex, attach_opaque_data_hex are passed to HandleBatchLogMessage
// for any log batches embedded in the externalized IPC stream.
// If pointer_metadata is provided and contains RPC_LOCATION_SHA256_KEY,
// the fetched bytes are verified against the expected SHA-256 checksum.
UnaryResponseResult ResolveExternalLocation(ClientContext &context,
                                             const std::string &location_url,
                                             const std::string &worker_path = "",
                                             const std::string &invocation_id_hex = "",
                                             const std::string &attach_opaque_data_hex = "",
                                             const std::shared_ptr<arrow::KeyValueMetadata> &pointer_metadata = nullptr);

// Check if a batch result is a pointer batch and resolve it if so.
// Returns the original result if not a pointer batch, or the resolved result.
// worker_path is forwarded to ResolveExternalLocation for log context.
UnaryResponseResult MaybeResolveExternalLocation(ClientContext &context,
                                                   UnaryResponseResult &result,
                                                   const std::string &worker_path = "");

// ServerCapabilities moved to vgi_server_capabilities.hpp (arrow-free) so the
// per-catalog cache on VgiAttachParameters can include it without Arrow.

// Upload URL returned by __upload_url__/init
struct UploadUrl {
	std::string upload_url;
	std::string download_url;
};

// Discover server capabilities via HEAD {base_url}/health.
// Capability headers are returned on every response; /health is the
// canonical mandatory, auth-exempt target. Honours Cache-Control:
// max-age=N to schedule a future re-probe via cache_expires_at.
ServerCapabilities HttpDiscoverCapabilities(ClientContext &context, const std::string &base_url);

// Request upload URLs from the server. Posts to {base_url}/__upload_url__/init.
std::vector<UploadUrl> HttpRequestUploadUrls(ClientContext &context,
                                               const std::string &base_url,
                                               int count,
                                               const std::shared_ptr<CatalogAuth> &auth = nullptr);

// HTTP PUT raw bytes to a URL with optional compression.  ``encoding`` ==
// ``HttpEncoding::NONE`` (the default) writes the body verbatim; any other
// codec is applied before PUT and stamped on Content-Encoding /
// X-VGI-Content-Encoding so the receiving side knows how to decode.
void HttpPutBytes(ClientContext &context, const std::string &url,
                   const std::vector<uint8_t> &data, HttpEncoding encoding = HttpEncoding::NONE);

// Serialize a pointer batch: zero-row batch with schema + vgi_rpc.location metadata.
// Optionally includes stream_state token in metadata.
std::vector<uint8_t> SerializePointerBatch(const std::shared_ptr<arrow::Schema> &schema,
                                             const std::string &location_url,
                                             const std::string &stream_state_token = "");

} // namespace vgi
} // namespace duckdb
