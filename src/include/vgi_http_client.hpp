// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>


#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_http_compression.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_server_capabilities.hpp"

namespace duckdb {
namespace vgi {

class CatalogAuth;      // vgi_oauth.hpp
class SessionCookieJar; // vgi_cookie_jar.hpp
struct IrohClientConfig;

constexpr const char *ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

// Invoke a unary RPC over HTTP. worker_path is the base URL (e.g.
// "http://localhost:8000/vgi"); the request posts to
// {worker_path}/{protocol}/{method_name}. Discovers capabilities first when
// `caps` is missing or expired.
// auth: per-catalog bearer token / 401 handling.
// cookie_jar: null skips Cookie / Set-Cookie handling; otherwise the request
// carries the jar's cookies and Set-Cookie responses update it.
// cached_http_params: per-catalog HTTPParams (pass
// VgiAttachParameters::GetOrInitHttpParams). Without it every RPC re-enters the
// secret manager, which deadlocks under the MetaTransaction mutex
// (duckdb/duckdb#22258).
// client_holder, caps: as for HttpPostArrowIpc. Callers with a per-catalog
// ProtocolSession should load, pass and store back `caps` so the codec
// choice is learned once per catalog, not per RPC.
// protocol: selects both the URL's protocol segment and the vgi_rpc.protocol
// routing key (the server requires them to agree). Reserved methods (`__x__`)
// are mounted flat at {worker_path}/{method_name} and carry neither.
UnaryResponseResult HttpInvokeUnary(ClientContext &context,
                                     const std::string &worker_path,
                                     const std::string &method_name,
                                     const VgiBatchPtr &params,
                                     const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                     const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                                     const std::shared_ptr<HTTPParams> &cached_http_params = nullptr,
                                     const std::string &execution_id_hex = "",
                                     const std::string &attach_opaque_data_digest = "",
                                     const std::string &transaction_opaque_data_digest = "",
                                     const std::string &conn_id_hex = "",
                                     const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL,
                                     duckdb::unique_ptr<HTTPClient> *client_holder = nullptr,
                                     ServerCapabilities *caps = nullptr,
                                     const std::shared_ptr<IrohClientConfig> &iroh_config = nullptr,
                                     const std::unordered_map<std::string, std::string> *error_fields = nullptr);

// Thread-safe pool of keep-alive HTTPClients for ONE base URL (one per
// catalog, VgiAttachParameters::GetOrInitHttpClientPool). HTTPClient is not
// thread-safe, so callers check a client out into a local holder (null on a
// cold pool; HttpPostArrowIpc creates one lazily) and Return it only on
// success: a client whose request threw may hold a wedged connection.
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
	// Idle keep-alive connections kept per catalog; excess returns are closed.
	static constexpr size_t kMaxPooled = 8;
	std::mutex mu_;
	std::vector<duckdb::unique_ptr<HTTPClient>> clients_;
};

// POST Arrow IPC bytes and return the decoded response body. Timeout is
// vgi_http_timeout_seconds; the decoded-response budget is
// vgi_http_accepted_max_response_bytes, advertised on every request and
// tightened by the server's VGI-Max-Response-Bytes. Handles one 401 retry via
// `auth` and one codec renegotiation.
// client_holder: when non-null, the HTTPClient (and its keep-alive connection)
// is created on first use and reused across calls — a big win on the per-batch
// exchange path. The client is bound to one host: use a holder for a single
// base URL only and never share it across threads. Null = fresh connection.
// harvested_caps: IN/OUT. On the way in, a discovered snapshot picks the
// request Content-Encoding (an EMPTY VGI-Supported-Encodings means identity
// bodies). On the way out it is refreshed from the capability headers the
// server stamps on every response, whatever its status.
// cancellation: opt-in query context whose interrupt aborts the transfer and
// throws InterruptException. Never defaulted to `context`: rollback,
// table_buffering_destructor and the cancel dispatcher's server-side cancel
// run while the query is marked interrupted and must still go out.
std::string HttpPostArrowIpc(ClientContext &context,
                              const std::string &url,
                              const std::vector<uint8_t> &body,
                              const std::shared_ptr<CatalogAuth> &auth = nullptr,
                              const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                              const std::shared_ptr<HTTPParams> &cached_http_params = nullptr,
                              duckdb::unique_ptr<HTTPClient> *client_holder = nullptr,
                              ServerCapabilities *harvested_caps = nullptr,
                              const std::shared_ptr<IrohClientConfig> &iroh_config = nullptr,
                              ClientContext *cancellation = nullptr,
                              const RpcErrorContext *error_context = nullptr);

// Whether HttpGetBytes resolves DuckDB "http" secrets for the URL.
enum class HttpSecrets {
	NONE,   // pre-signed URL; safe inside VgiTransaction::Start (see vgi_http_params.hpp)
	LOOKUP, // secret-scoped proxy etc. apply; never from VgiTransaction::Start
};

// GET a URL (externalized batches), decoding X-VGI-Content-Encoding /
// Content-Encoding. Sends no auth headers (pre-signed URLs reject them).
// cancellation: as for HttpPostArrowIpc.
// max_bytes > 0 bounds both the transferred body and its decoded size
// (IOException RESPONSE_TOO_LARGE); 0 = unbounded.
std::string HttpGetBytes(ClientContext &context, const std::string &url,
                         ClientContext *cancellation = nullptr, HttpSecrets secrets = HttpSecrets::NONE,
                         uint64_t max_bytes = 0);

// The test hook that admits plain-http loopback external locations
// (http://127.0.0.1, http://[::1], http://localhost). Production stores are https.
inline constexpr const char *kAllowLoopbackExternalLocationsEnv = "VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS";

// Whether ResolveExternalLocation may fetch `url`: https:// on any host, plain
// http:// on any host when allow_plain_http (vgi_storage_allow_plain_http), or
// loopback http with kAllowLoopbackExternalLocationsEnv=1 (always, in the browser).
bool ExternalLocationUrlAllowed(const std::string &url, bool allow_plain_http = false);
// The same, reading vgi_storage_allow_plain_http from the session.
bool ExternalLocationUrlAllowed(ClientContext &context, const std::string &url);
bool StorageAllowsPlainHttp(ClientContext &context);

// Storage transfers (HttpGetBytes, HttpPutBytes): connect and stall timeout
// vgi_storage_stall_timeout_seconds, never a whole-request deadline; at most
// 1 + kStorageTransferMaxRetries attempts (fewer under a lower http_retries),
// backoff kStorageTransferRetryWaitMs x kStorageTransferRetryBackoff^n.
// The values match vgi-rpc's storage clients.
inline constexpr uint64_t kStorageTransferMaxRetries = 2;
inline constexpr uint64_t kStorageTransferRetryWaitMs = 250;
inline constexpr float kStorageTransferRetryBackoff = 2.0f;
uint64_t GetStorageStallTimeoutSeconds(ClientContext &context);

// vgi_external_location_max_bytes for this session.
uint64_t GetExternalLocationMaxBytes(ClientContext &context);

// Fetch and parse an external location. Throws on a URL that is not
// ExternalLocationUrlAllowed, an object over vgi_external_location_max_bytes,
// fetch failure, an empty
// body, a SHA-256 mismatch (when pointer_metadata carries
// RPC_LOCATION_SHA256_KEY) or a pointer to another location. The worker/id
// arguments are log context for log batches embedded in the fetched stream.
UnaryResponseResult ResolveExternalLocation(ClientContext &context,
                                             const std::string &location_url,
                                             const std::string &worker_path = "",
                                             const std::string &execution_id_hex = "",
                                             const std::string &attach_opaque_data_digest = "",
                                             const VgiMetadataPtr &pointer_metadata = nullptr,
                                             ClientContext *cancellation = nullptr,
                                             const std::string &conn_id_hex = "",
                                             const std::string &transaction_opaque_data_digest = "");

// Resolve `result` if it carries vgi_rpc.location metadata; otherwise return it.
UnaryResponseResult MaybeResolveExternalLocation(ClientContext &context,
                                                   UnaryResponseResult &result,
                                                   const std::string &worker_path = "",
                                                   const std::string &execution_id_hex = "",
                                                   const std::string &attach_opaque_data_digest = "",
                                                   const std::string &transaction_opaque_data_digest = "",
                                                   const std::string &conn_id_hex = "");

// The upload-URL protocol version this client speaks (VGI-Upload-URL-Support).

// One upload to request from __upload_url__/init (v2): the exact byte length
// and lowercase hex SHA-256 of the body the client will PUT.
struct UploadRequest {
	int64_t size;
	std::string sha256_hex;
};

// One entry returned by __upload_url__/init.
struct UploadUrl {
	std::string upload_url;
	std::string download_url;
	// Headers the PUT must carry verbatim (signed by the server; e.g.
	// x-amz-checksum-sha256). Never Content-Length.
	std::vector<std::pair<std::string, std::string>> upload_headers;
	// Protocol 3.4 grant columns (vgi-rpc STATE_REF_V1.md §10.2), read by name
	// and optional: the storage id of the object this PUT creates, and a sealed
	// argument reference to it. Only argument references need them.
	std::string object_id;
	std::vector<uint8_t> ref;
	bool has_ref = false;
};

// Probe {base_url}/health (mandatory and auth-exempt): HEAD natively, since
// DuckDB's HTTP layer has no OPTIONS; OPTIONS over httpi://. Cache-Control
// max-age=N sets cache_expires_at for a re-probe. Pass the catalog's cached
// HTTPParams: a probe can run inside VgiTransaction::Start, where a secret
// lookup deadlocks.
ServerCapabilities HttpDiscoverCapabilities(
    ClientContext &context, const std::string &base_url,
    const std::shared_ptr<IrohClientConfig> &iroh_config = nullptr,
    const std::shared_ptr<HTTPParams> &cached_http_params = nullptr);

// Posts to {base_url}/__upload_url__/init (upload URLs v2): one URL per
// request entry, in order.
std::vector<UploadUrl> HttpRequestUploadUrls(ClientContext &context,
                                               const std::string &base_url,
                                               const std::vector<UploadRequest> &uploads,
                                               const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                               const std::shared_ptr<IrohClientConfig> &iroh_config = nullptr);

// PUT to a pre-signed URL with a plain Content-Length (never chunked). A
// non-NONE encoding compresses the body and stamps both Content-Encoding and
// X-VGI-Content-Encoding. `extra_headers` are sent verbatim (upload_headers).
void HttpPutBytes(ClientContext &context, const std::string &url,
                   const std::vector<uint8_t> &data, HttpEncoding encoding = HttpEncoding::NONE,
                   const std::vector<std::pair<std::string, std::string>> &extra_headers = {});
// The same, PUTting `size` bytes at `data` without copying them (an argument
// is uploaded straight from its IPC buffer).
void HttpPutBytes(ClientContext &context, const std::string &url, const uint8_t *data, size_t size,
                   HttpEncoding encoding = HttpEncoding::NONE,
                   const std::vector<std::pair<std::string, std::string>> &extra_headers = {});

// A request body over the server's VGI-Max-Request-Bytes, when the server
// advertises VGI-Upload-URL-Support: 2: hash it, request one sized URL for
// (size, sha256), PUT exactly those bytes with the returned upload_headers,
// and replace `body` with a zero-row pointer batch (same schema, the
// request's metadata plus vgi_rpc.location and vgi_rpc.location.sha256).
// Returns whether it did. Throws when the server advertises only v1
// ("true"), when the body also exceeds VGI-Max-Upload-Bytes, or when a vended
// URL is not https (see ExternalLocationUrlAllowed). Without upload support
// the body is left inline for the server to refuse with 413, and so is a body
// whose metadata alone reaches the cap (the pointer keeps every metadata key,
// so no upload could bring it under).
// --- Argument references (protocol 3.4, docs/large-values-3.4.md §4-§5) ---

// The worker capability header that enables argument references.
inline constexpr const char *kArgumentsRefBytesHeader = "VGI-Arguments-Ref-Bytes";

// Parse a VGI-Arguments-Ref-Bytes value: a positive decimal integer, no sign,
// no leading zero, at most 19 digits; -1 for anything else (= absent).
int64_t ParseArgumentsRefBytes(const std::string &value);

// The build-time rule: whether a bind's `arguments_size`-byte arguments IPC
// goes up once and travels as BindRequest.arguments_location. True iff the
// worker sends a valid VGI-Arguments-Ref-Bytes (A), advertises upload URLs v2,
// arguments_size > min(A, VGI-Max-Request-Bytes / 4) (an unset request cap
// counts as infinite) and the size is within any advertised
// VGI-Max-Upload-Bytes. The caller only asks for HTTP and Iroh connections.
bool ShouldExternalizeArguments(const ServerCapabilities &caps, uint64_t arguments_size);

// An uploaded argument: the grant's sealed `ref`, and the exact size and
// lowercase hex SHA-256 of the uploaded bytes.
struct UploadedArguments {
	std::vector<uint8_t> ref;
	int64_t size = 0;
	std::string sha256_hex;
};

// Hash `size` bytes at `data`, request one sized upload URL from the worker's
// __upload_url__/init, PUT the bytes straight from `data` with the returned
// upload_headers, and return the grant's `ref`. Throws when the grant carries
// no `ref` (a worker that advertises VGI-Arguments-Ref-Bytes must mint one) or
// vends a URL that is not https (see ExternalLocationUrlAllowed).
UploadedArguments UploadArgumentsForReference(ClientContext &context, const std::string &base_url,
                                              const uint8_t *data, size_t size,
                                              const std::shared_ptr<CatalogAuth> &auth,
                                              const std::shared_ptr<IrohClientConfig> &iroh_config);

bool MaybeExternalizeRequestBody(ClientContext &context, const std::string &base_url, std::vector<uint8_t> &body,
                                 const ServerCapabilities &caps, const std::shared_ptr<CatalogAuth> &auth,
                                 const std::shared_ptr<IrohClientConfig> &iroh_config);

} // namespace vgi
} // namespace duckdb
