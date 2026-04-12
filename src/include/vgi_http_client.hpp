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

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;

// Content type for Arrow IPC streams over HTTP
constexpr const char *ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

// Invoke a unary RPC method over HTTP.
// worker_path includes the prefix, e.g. "http://localhost:8000/vgi".
// Posts to {worker_path}/{method_name}.
// auth: per-catalog auth state for bearer token injection and 401 handling.
UnaryResponseResult HttpInvokeUnary(ClientContext &context,
                                     const std::string &worker_path,
                                     const std::string &method_name,
                                     const std::shared_ptr<arrow::RecordBatch> &params,
                                     const std::shared_ptr<CatalogAuth> &auth = nullptr);

// POST Arrow IPC bytes to a URL, return raw response body bytes.
// Used for catalog, stream init, and exchange operations.
// Timeout is controlled by the vgi_http_timeout_seconds setting (default 300s).
// auth: per-catalog auth state for bearer token injection and 401 handling.
std::string HttpPostArrowIpc(ClientContext &context,
                              const std::string &url,
                              const std::vector<uint8_t> &body,
                              const std::shared_ptr<CatalogAuth> &auth = nullptr);

// HTTP GET raw bytes from a URL. Used for fetching externalized batches.
// Handles X-VGI-Content-Encoding: zstd decompression. No auth headers sent.
std::string HttpGetBytes(ClientContext &context, const std::string &url);

// Resolve an external location pointer batch by fetching and parsing the URL.
// Returns the resolved data batch. Throws on redirect loops or fetch failures.
// worker_path, invocation_id_hex, attach_id_hex are passed to HandleBatchLogMessage
// for any log batches embedded in the externalized IPC stream.
// If pointer_metadata is provided and contains RPC_LOCATION_SHA256_KEY,
// the fetched bytes are verified against the expected SHA-256 checksum.
UnaryResponseResult ResolveExternalLocation(ClientContext &context,
                                             const std::string &location_url,
                                             const std::string &worker_path = "",
                                             const std::string &invocation_id_hex = "",
                                             const std::string &attach_id_hex = "",
                                             const std::shared_ptr<arrow::KeyValueMetadata> &pointer_metadata = nullptr);

// Check if a batch result is a pointer batch and resolve it if so.
// Returns the original result if not a pointer batch, or the resolved result.
// worker_path is forwarded to ResolveExternalLocation for log context.
UnaryResponseResult MaybeResolveExternalLocation(ClientContext &context,
                                                   UnaryResponseResult &result,
                                                   const std::string &worker_path = "");

// Server capabilities discovered via OPTIONS /__capabilities__
struct ServerCapabilities {
	bool discovered = false;
	int64_t max_request_bytes = -1;   // -1 = no limit advertised
	bool upload_url_support = false;
	int64_t max_upload_bytes = -1;    // -1 = no limit advertised
};

// Upload URL returned by __upload_url__/init
struct UploadUrl {
	std::string upload_url;
	std::string download_url;
};

// Discover server capabilities via OPTIONS request.
ServerCapabilities HttpDiscoverCapabilities(ClientContext &context, const std::string &base_url);

// Request upload URLs from the server. Posts to {base_url}/__upload_url__/init.
std::vector<UploadUrl> HttpRequestUploadUrls(ClientContext &context,
                                               const std::string &base_url,
                                               int count,
                                               const std::shared_ptr<CatalogAuth> &auth = nullptr);

// HTTP PUT raw bytes to a URL with optional zstd compression.
void HttpPutBytes(ClientContext &context, const std::string &url,
                   const std::vector<uint8_t> &data, bool compress_zstd = false);

// Serialize a pointer batch: zero-row batch with schema + vgi_rpc.location metadata.
// Optionally includes stream_state token in metadata.
std::vector<uint8_t> SerializePointerBatch(const std::shared_ptr<arrow::Schema> &schema,
                                             const std::string &location_url,
                                             const std::string &stream_state_token = "");

} // namespace vgi
} // namespace duckdb
