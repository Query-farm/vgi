#include "vgi_http_client.hpp"

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "zstd.h"

#include "vgi_cookie_jar.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_rpc_client.hpp"

#include "mbedtls_wrapper.hpp"

using namespace duckdb_zstd;

namespace duckdb {
namespace vgi {

// Decompress a zstd-encoded buffer. Returns decompressed bytes.
static std::string ZstdDecompress(const char *data, size_t size) {
	auto frame_size = ZSTD_getFrameContentSize(data, size);
	if (frame_size == ZSTD_CONTENTSIZE_ERROR) {
		throw IOException("VGI zstd decompression failed: not valid zstd data");
	}

	if (frame_size != ZSTD_CONTENTSIZE_UNKNOWN) {
		// Known size — single-call decompress
		std::string decompressed(frame_size, '\0');
		auto result = ZSTD_decompress(decompressed.data(), frame_size, data, size);
		if (ZSTD_isError(result)) {
			throw IOException("VGI zstd decompression failed: %s", ZSTD_getErrorName(result));
		}
		decompressed.resize(result);
		return decompressed;
	}

	// Unknown size — streaming decompress
	auto *dstream = ZSTD_createDStream();
	if (!dstream) {
		throw IOException("VGI zstd decompression failed: could not create stream");
	}
	ZSTD_initDStream(dstream);

	std::string output;
	ZSTD_inBuffer input_buf = {data, size, 0};
	std::vector<char> tmp(ZSTD_DStreamOutSize());

	while (input_buf.pos < input_buf.size) {
		ZSTD_outBuffer output_buf = {tmp.data(), tmp.size(), 0};
		auto result = ZSTD_decompressStream(dstream, &output_buf, &input_buf);
		if (ZSTD_isError(result)) {
			ZSTD_freeDStream(dstream);
			throw IOException("VGI zstd decompression failed: %s", ZSTD_getErrorName(result));
		}
		output.append(tmp.data(), output_buf.pos);
	}

	ZSTD_freeDStream(dstream);
	return output;
}

// Compress a buffer with zstd. Returns compressed bytes.
static std::vector<uint8_t> ZstdCompress(const uint8_t *data, size_t size, int level = 3) {
	auto bound = ZSTD_compressBound(size);
	std::vector<uint8_t> compressed(bound);
	auto result = ZSTD_compress(compressed.data(), bound, data, size, level);
	if (ZSTD_isError(result)) {
		throw IOException("VGI zstd compression failed: %s", ZSTD_getErrorName(result));
	}
	compressed.resize(result);
	return compressed;
}

// Helper: strip trailing slash from a URL
static std::string NormalizeBaseUrl(const std::string &url) {
	if (!url.empty() && url.back() == '/') {
		return url.substr(0, url.size() - 1);
	}
	return url;
}

// Helper: apply the configurable HTTP timeout setting to request params.
static void ApplyHttpTimeout(ClientContext &context, HTTPParams &params) {
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
		params.timeout = static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
	} else {
		params.timeout = 300; // fallback: 5 minutes
	}
}

// Check whether ``url`` is an https origin — used to gate Secure cookies.
static bool UrlIsHttps(const std::string &url) {
	// Case-insensitive prefix match — HTTPUtil accepts both cases.
	if (url.size() < 8) {
		return false;
	}
	return (url[0] == 'h' || url[0] == 'H') && (url[1] == 't' || url[1] == 'T') &&
	       (url[2] == 't' || url[2] == 'T') && (url[3] == 'p' || url[3] == 'P') &&
	       (url[4] == 's' || url[4] == 'S') && url[5] == ':';
}

// Collect all Set-Cookie response headers. The HTTP layer may fold duplicate
// headers into a comma-separated string; proxies and browsers both treat
// Set-Cookie as non-foldable, so we split on newlines inserted by the
// underlying client or, when absent, fall back to the single value.
static std::vector<std::string> CollectSetCookieHeaders(const HTTPResponse &response) {
	std::vector<std::string> out;
	if (!response.HasHeader("Set-Cookie")) {
		return out;
	}
	out.push_back(response.GetHeaderValue("Set-Cookie"));
	return out;
}

// Internal: perform a single HTTP POST with optional auth header
static std::string HttpPostArrowIpcInternal(ClientContext &context,
                                             const std::string &url,
                                             const std::vector<uint8_t> &body,
                                             const std::string &bearer_token,
                                             const std::shared_ptr<SessionCookieJar> &cookie_jar,
                                             std::unique_ptr<HTTPResponse> &out_response) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);

	ApplyHttpTimeout(context, *params);

	// Compress the request body with zstd
	auto compressed_body = ZstdCompress(body.data(), body.size());

	HTTPHeaders headers;
	headers.Insert("Content-Type", ARROW_IPC_CONTENT_TYPE);
	headers.Insert("Content-Encoding", "zstd");
	headers.Insert("X-VGI-Accept-Encoding", "zstd");
	if (!bearer_token.empty()) {
		headers.Insert("Authorization", "Bearer " + bearer_token);
	}
	if (cookie_jar) {
		auto cookie_header = cookie_jar->BuildCookieHeader();
		if (!cookie_header.empty()) {
			headers.Insert("Cookie", cookie_header);
		}
	}

	PostRequestInfo post(url, headers, *params,
	                     reinterpret_cast<const_data_ptr_t>(compressed_body.data()),
	                     static_cast<idx_t>(compressed_body.size()));

	out_response = http_util.Request(post);

	if (cookie_jar && out_response) {
		auto set_cookie_headers = CollectSetCookieHeaders(*out_response);
		if (!set_cookie_headers.empty()) {
			cookie_jar->UpdateFromSetCookie(set_cookie_headers, UrlIsHttps(url));
		}
	}

	if (out_response->status == HTTPStatusCode::Unauthorized_401) {
		// Return empty — caller handles 401
		return "";
	}

	if (!out_response->Success()) {
		// Try to parse Arrow IPC error batch from the response body
		auto &error_body = post.buffer_out.empty() ? out_response->body : post.buffer_out;
		if (!error_body.empty()) {
			try {
				auto error_result = ReadUnaryResponseFromBuffer(
				    reinterpret_cast<const uint8_t *>(error_body.data()),
				    error_body.size(), nullptr, url);
			} catch (const IOException &) {
				throw; // Worker error — propagate with the original message
			} catch (...) {
				// Not Arrow IPC — fall through to plain error
			}
		}
		throw IOException("VGI HTTP request failed (HTTP %d): %s [url: %s]",
		                  static_cast<int>(out_response->status),
		                  error_body.empty() ? out_response->GetError() : error_body, url);
	}

	// Decompress zstd response if server sent it
	if (out_response->HasHeader("X-VGI-Content-Encoding") &&
	    out_response->GetHeaderValue("X-VGI-Content-Encoding") == "zstd" && !post.buffer_out.empty()) {
		post.buffer_out = ZstdDecompress(post.buffer_out.data(), post.buffer_out.size());
	}

	// Server errors are sent as HTTP 200 with X-VGI-RPC-Error: true header
	// (so that clients which discard response bodies on 5xx still receive
	// the Arrow IPC error metadata). Parse the error batch and throw.
	if (out_response->HasHeader("X-VGI-RPC-Error") &&
	    out_response->GetHeaderValue("X-VGI-RPC-Error") == "true") {
		auto &error_body = post.buffer_out;
		if (!error_body.empty()) {
			// ReadUnaryResponseFromBuffer dispatches the error batch which
			// calls HandleBatchLogMessage → ThrowVgiIOException with the
			// worker's original error message.
			ReadUnaryResponseFromBuffer(
			    reinterpret_cast<const uint8_t *>(error_body.data()),
			    error_body.size(), nullptr, url);
		}
		throw IOException("VGI HTTP RPC error [url: %s]", url);
	}

	return std::move(post.buffer_out);
}

std::string HttpPostArrowIpc(ClientContext &context,
                              const std::string &url,
                              const std::vector<uint8_t> &body,
                              const std::shared_ptr<CatalogAuth> &auth,
                              const std::shared_ptr<SessionCookieJar> &cookie_jar) {
	// Get cached token from per-catalog auth (if any)
	std::string token;
	if (auth) {
		token = auth->GetToken();
	}

	std::unique_ptr<HTTPResponse> response;
	auto result = HttpPostArrowIpcInternal(context, url, body, token, cookie_jar, response);

	if (response->status != HTTPStatusCode::Unauthorized_401) {
		return result;
	}

	// Got 401 — need auth to handle it
	if (!auth) {
		throw IOException("VGI HTTP authentication required (HTTP 401) but no auth configured for this catalog "
		                  "[url: %s]. Use bearer_token or oauth_refresh_token in ATTACH options.",
		                  url);
	}

	// Check for WWW-Authenticate header (OAuth discovery). If absent, the server
	// rejected the token outright (e.g., static bearer auth) — let the auth handler decide.
	if (!response->HasHeader("WWW-Authenticate")) {
		// No WWW-Authenticate: let auth->HandleUnauthorized decide. For bearer tokens
		// this throws immediately. For OAuth without a challenge, we provide an empty one.
		OAuthChallenge empty_challenge;
		auth->HandleUnauthorized(empty_challenge, context);
		// If HandleUnauthorized didn't throw (shouldn't happen), surface a generic error
		throw IOException("VGI HTTP authentication failed (HTTP 401) [url: %s]", url);
	}

	auto www_auth = response->GetHeaderValue("WWW-Authenticate");
	auto challenge = ParseWWWAuthenticate(www_auth);
	if (!challenge.has_value()) {
		throw IOException("VGI HTTP authentication required (HTTP 401) but no OAuth resource_metadata "
		                  "in WWW-Authenticate header [url: %s]. Response: %s",
		                  url, response->body);
	}

	VGI_STDERR_DEBUG("[VGI] http.401_received url=%s resource_metadata=%s\n",
	                 url.c_str(), challenge->resource_metadata_url.c_str());

	// Perform or wait for auth flow (OAuth PKCE/device code)
	auto new_token = auth->HandleUnauthorized(*challenge, context);

	// Retry with new token
	result = HttpPostArrowIpcInternal(context, url, body, new_token, cookie_jar, response);
	if (response->status == HTTPStatusCode::Unauthorized_401) {
		throw IOException("VGI HTTP authentication failed after auth flow (HTTP 401) [url: %s]. "
		                  "Response: %s", url, response->body);
	}

	return result;
}

UnaryResponseResult HttpInvokeUnary(ClientContext &context,
                                     const std::string &worker_path,
                                     const std::string &method_name,
                                     const std::shared_ptr<arrow::RecordBatch> &params,
                                     const std::shared_ptr<CatalogAuth> &auth,
                                     const std::shared_ptr<SessionCookieJar> &cookie_jar) {
	std::string base_url = NormalizeBaseUrl(worker_path);
	std::string url = base_url + "/" + method_name;

	VGI_LOG(context, "http.invoke_unary",
	        {{"url", url}, {"method", method_name}});

	// Serialize the RPC request to Arrow IPC bytes
	std::vector<uint8_t> body;
	if (params) {
		body = SerializeRpcRequest(method_name, params);
	} else {
		body = SerializeEmptyRpcRequest(method_name);
	}

	// POST to {worker_path}/{method_name} using standard HTTP timeout
	auto response_body = HttpPostArrowIpc(context, url, body, auth, cookie_jar);

	// Parse the Arrow IPC response
	auto result = ReadUnaryResponseFromBuffer(
	    reinterpret_cast<const uint8_t *>(response_body.data()),
	    response_body.size(), &context, url);

	// Resolve external location pointer batches
	result = MaybeResolveExternalLocation(context, result, base_url);

	VGI_LOG(context, "http.invoke_unary_result",
	        {{"url", url},
	         {"method", method_name},
	         {"has_batch", result.batch ? "true" : "false"}});

	return result;
}

// ============================================================================
// External Location Support
// ============================================================================

std::string HttpGetBytes(ClientContext &context, const std::string &url) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);

	ApplyHttpTimeout(context, *params);

	HTTPHeaders headers;
	headers.Insert("X-VGI-Accept-Encoding", "zstd");
	// No auth headers — pre-signed URLs break with extra Authorization headers

	// Accumulate response body via content handler
	std::string body;
	auto response_handler = [](const HTTPResponse &) { return true; };
	auto content_handler = [&body](const_data_ptr_t data, idx_t data_length) {
		body.append(reinterpret_cast<const char *>(data), data_length);
		return true;
	};

	GetRequestInfo get(url, headers, *params, response_handler, content_handler);
	auto response = http_util.Request(get);

	if (!response->Success()) {
		throw IOException("VGI external location fetch failed (HTTP %d) [url: %s]",
		                  static_cast<int>(response->status), url);
	}

	// Decompress zstd if server indicates it
	if (response->HasHeader("X-VGI-Content-Encoding") &&
	    response->GetHeaderValue("X-VGI-Content-Encoding") == "zstd" && !body.empty()) {
		return ZstdDecompress(body.data(), body.size());
	}

	return body;
}

UnaryResponseResult ResolveExternalLocation(ClientContext &context,
                                             const std::string &location_url,
                                             const std::string &worker_path,
                                             const std::string &invocation_id_hex,
                                             const std::string &attach_id_hex,
                                             const std::shared_ptr<arrow::KeyValueMetadata> &pointer_metadata) {
	// Fetch the external data
	auto body = HttpGetBytes(context, location_url);

	if (body.empty()) {
		throw IOException("VGI external location returned empty response [url: %s]", location_url);
	}

	// Verify SHA-256 checksum if present in pointer batch metadata
	if (pointer_metadata) {
		int sha_idx = pointer_metadata->FindKey(RPC_LOCATION_SHA256_KEY);
		if (sha_idx >= 0) {
			auto expected_hex = pointer_metadata->value(sha_idx);
			// Compute SHA-256 of the fetched body bytes
			auto raw_hash = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(body);
			// Convert raw hash to hex string
			char hex_buf[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT + 1];
			duckdb_mbedtls::MbedTlsWrapper::ToBase16(const_cast<char *>(raw_hash.data()), hex_buf,
			                                          duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
			hex_buf[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT] = '\0';
			std::string actual_hex(hex_buf, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT);

			if (actual_hex != expected_hex) {
				throw IOException("VGI external location SHA-256 checksum mismatch [url: %s]: "
				                  "expected %s, got %s",
				                  location_url, expected_hex, actual_hex);
			}
		}
	}

	// Parse the externalized IPC stream with proper log context.
	// The stream may contain log batches (bundled by maybe_externalize_collector)
	// followed by a data batch.
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(body.size()));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for external location: %s",
		                  alloc_result.status().ToString());
	}
	auto owned = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(owned->data()), body.data(), body.size());

	auto input = std::make_shared<arrow::io::BufferReader>(owned);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		throw IOException("Failed to open external location IPC stream: %s [url: %s]",
		                  reader_result.status().ToString(), location_url);
	}
	auto reader = reader_result.ValueUnsafe();

	// Use the provided worker_path for log context, falling back to location_url
	auto &log_worker_path = worker_path.empty() ? location_url : worker_path;

	UnaryResponseResult result;
	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok() || !read_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = read_result.ValueUnsafe();
		auto batch_type = ClassifyBatch(bwm.batch, bwm.custom_metadata);

		if (batch_type == RpcBatchType::ERROR) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path,
			                     -1, invocation_id_hex, attach_id_hex);
			throw IOException("VGI external location error [url: %s]", location_url);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path,
			                     -1, invocation_id_hex, attach_id_hex);
			continue;
		}
		if (batch_type == RpcBatchType::EXTERNAL_LOCATION) {
			throw IOException("VGI external location redirect loop: resolved batch from %s "
			                  "contains another vgi_rpc.location", location_url);
		}

		// Data batch
		result.batch = bwm.batch;
		result.metadata = bwm.custom_metadata;
		break;
	}

	// Drain remaining
	while (true) {
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = drain_result.ValueUnsafe();
		auto bt = ClassifyBatch(bwm.batch, bwm.custom_metadata);
		if (bt == RpcBatchType::LOG || bt == RpcBatchType::ERROR) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path,
			                     -1, invocation_id_hex, attach_id_hex);
		}
	}

	if (!result.batch) {
		throw IOException("VGI external location contained no data batch [url: %s]", location_url);
	}

	return result;
}

UnaryResponseResult MaybeResolveExternalLocation(ClientContext &context,
                                                   UnaryResponseResult &result,
                                                   const std::string &worker_path) {
	if (!result.metadata) {
		return std::move(result);
	}
	int loc_idx = result.metadata->FindKey(RPC_LOCATION_KEY);
	if (loc_idx < 0) {
		return std::move(result);
	}

	auto location_url = result.metadata->value(loc_idx);
	return ResolveExternalLocation(context, location_url, worker_path, "", "", result.metadata);
}

// ============================================================================
// Capability Discovery and Upload URLs
// ============================================================================

// Parse capability headers from an HTTP response (set by middleware on every response).
static ServerCapabilities ParseCapabilityHeaders(const HTTPResponse &response) {
	ServerCapabilities caps;
	caps.discovered = true;

	if (response.HasHeader("VGI-Max-Request-Bytes")) {
		try {
			caps.max_request_bytes = std::stoll(response.GetHeaderValue("VGI-Max-Request-Bytes"));
		} catch (...) {}
	}
	if (response.HasHeader("VGI-Upload-URL-Support")) {
		caps.upload_url_support = response.GetHeaderValue("VGI-Upload-URL-Support") == "true";
	}
	if (response.HasHeader("VGI-Max-Upload-Bytes")) {
		try {
			caps.max_upload_bytes = std::stoll(response.GetHeaderValue("VGI-Max-Upload-Bytes"));
		} catch (...) {}
	}

	return caps;
}

ServerCapabilities HttpDiscoverCapabilities(ClientContext &context, const std::string &base_url) {
	// Capability headers are set by middleware on ALL responses (including errors).
	// Use a HEAD request to discover them cheaply. Append "/" to ensure a valid path.
	auto url = NormalizeBaseUrl(base_url) + "/";

	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	ApplyHttpTimeout(context, *params);

	HTTPHeaders headers;
	HeadRequestInfo head(url, headers, *params);
	auto response = http_util.Request(head);

	// Parse capability headers regardless of status code — middleware adds them to all responses
	return ParseCapabilityHeaders(*response);
}

std::vector<UploadUrl> HttpRequestUploadUrls(ClientContext &context,
                                               const std::string &base_url,
                                               int count,
                                               const std::shared_ptr<CatalogAuth> &auth) {
	// Serialize Arrow batch {count: int64}
	auto count_field = arrow::field("count", arrow::int64());
	auto schema = arrow::schema({count_field});
	auto count_array_result = arrow::MakeArrayFromScalar(arrow::Int64Scalar(count), 1);
	if (!count_array_result.ok()) {
		throw IOException("Failed to create count array for upload URL request");
	}
	auto batch = arrow::RecordBatch::Make(schema, 1, {count_array_result.ValueUnsafe()});

	// POST to __upload_url__/init (HttpInvokeUnary normalizes base_url internally)
	auto result = HttpInvokeUnary(context, base_url, "__upload_url__/init", batch, auth);

	if (!result.batch || result.batch->num_rows() == 0) {
		throw IOException("VGI server returned no upload URLs [url: %s]", base_url);
	}

	// Parse response: {upload_url: utf8, download_url: utf8, expires_at: timestamp}
	auto upload_col = std::static_pointer_cast<arrow::StringArray>(result.batch->GetColumnByName("upload_url"));
	auto download_col = std::static_pointer_cast<arrow::StringArray>(result.batch->GetColumnByName("download_url"));

	if (!upload_col || !download_col) {
		throw IOException("VGI upload URL response missing required columns [url: %s]", base_url);
	}

	std::vector<UploadUrl> urls;
	for (int64_t i = 0; i < result.batch->num_rows(); i++) {
		urls.push_back({upload_col->GetString(i), download_col->GetString(i)});
	}
	return urls;
}

void HttpPutBytes(ClientContext &context, const std::string &url,
                   const std::vector<uint8_t> &data, bool compress_zstd) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	ApplyHttpTimeout(context, *params);

	HTTPHeaders headers;
	std::string content_type = "application/octet-stream";

	const uint8_t *body_data = data.data();
	size_t body_size = data.size();
	std::vector<uint8_t> compressed;

	if (compress_zstd) {
		compressed = ZstdCompress(data.data(), data.size());
		body_data = compressed.data();
		body_size = compressed.size();
		headers.Insert("Content-Encoding", "zstd");
		headers.Insert("X-VGI-Content-Encoding", "zstd");
	}

	PutRequestInfo put(url, headers, *params,
	                   reinterpret_cast<const_data_ptr_t>(body_data),
	                   static_cast<idx_t>(body_size),
	                   content_type);
	auto response = http_util.Request(put);

	if (!response->Success()) {
		throw IOException("VGI upload failed (HTTP %d) [url: %s]",
		                  static_cast<int>(response->status), url);
	}
}

std::vector<uint8_t> SerializePointerBatch(const std::shared_ptr<arrow::Schema> &schema,
                                             const std::string &location_url,
                                             const std::string &stream_state_token) {
	// Build zero-row batch with empty arrays matching the schema
	std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
	for (int i = 0; i < schema->num_fields(); i++) {
		auto empty_result = arrow::MakeEmptyArray(schema->field(i)->type());
		if (!empty_result.ok()) {
			throw IOException("Failed to create empty array for pointer batch: %s",
			                  empty_result.status().ToString());
		}
		empty_arrays.push_back(empty_result.ValueUnsafe());
	}
	auto zero_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);

	// Build metadata: location key + optional stream_state
	auto metadata = arrow::KeyValueMetadata::Make({RPC_LOCATION_KEY}, {location_url});
	if (!stream_state_token.empty()) {
		metadata = metadata->Merge(*arrow::KeyValueMetadata::Make(
		    {RPC_STREAM_STATE_KEY}, {stream_state_token}));
	}

	// Serialize as IPC stream
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw IOException("Failed to create buffer for pointer batch: %s", sink_result.status().ToString());
	}
	auto sink = sink_result.ValueUnsafe();
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
	if (!writer_result.ok()) {
		throw IOException("Failed to create IPC writer for pointer batch: %s", writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();
	auto status = writer->WriteRecordBatch(*zero_batch, metadata);
	if (!status.ok()) {
		throw IOException("Failed to write pointer batch: %s", status.ToString());
	}
	status = writer->Close();
	if (!status.ok()) {
		throw IOException("Failed to close pointer batch writer: %s", status.ToString());
	}
	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish pointer batch buffer: %s", finish_result.status().ToString());
	}
	auto buffer = finish_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

} // namespace vgi
} // namespace duckdb
