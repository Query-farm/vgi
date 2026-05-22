// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_http_client.hpp"

#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/enums/http_status_code.hpp"

#include "vgi_cookie_jar.hpp"
#include "vgi_http_compression.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_rpc_client.hpp"

#include "mbedtls_wrapper.hpp"

namespace duckdb {
namespace vgi {

// Header used to advertise / negotiate the set of supported content
// encodings.  Mirrors the constant in ``vgi_rpc/http/_common.py``.
static constexpr const char *kSupportedEncodingsHeader = "VGI-Supported-Encodings";

// Comma-joined list of codecs we can produce / decode, in our preference
// order — sent on every request so the server can pick a codec for its
// response.  When the chosen request codec doesn't match the server's
// advertised set, the server returns 415 and the caller can re-pick from
// the ``VGI-Supported-Encodings`` header it carries back.
static std::string ClientAcceptEncoding() {
	return "zstd, gzip";
}

// Resolve the encoding advertised on a response.  Prefer the custom
// ``X-VGI-Content-Encoding`` header (older servers stamp this on every
// response — generic proxies don't fold it), falling back to the standard
// ``Content-Encoding``.  Returns ``NONE`` when no codec is advertised or
// when the token is unknown.
static HttpEncoding ResolveResponseEncoding(const HTTPResponse &response) {
	if (response.HasHeader("X-VGI-Content-Encoding")) {
		auto enc = ParseEncoding(response.GetHeaderValue("X-VGI-Content-Encoding"));
		if (enc != HttpEncoding::NONE) {
			return enc;
		}
	}
	if (response.HasHeader("Content-Encoding")) {
		auto enc = ParseEncoding(response.GetHeaderValue("Content-Encoding"));
		if (enc != HttpEncoding::NONE) {
			return enc;
		}
	}
	return HttpEncoding::NONE;
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

// Collect all Set-Cookie response headers.
//
// Caveat: DuckDB's HTTPHeaders is a single-value map keyed on header name
// (case_insensitive_map_t<string>), and the upstream Set-Cookie spec is
// explicitly non-foldable (RFC 6265 §3). When a server emits N Set-Cookie
// headers, DuckDB's wire→HTTPHeaders adapter chooses one of:
//   (a) keep the last (most underlying clients on a plain map[k]=v assignment),
//   (b) join with "\r\n" or "\n" between values,
//   (c) join with ", " (the generic header-folding rule, broken for cookies
//       whose Expires attribute legitimately contains commas).
// We can't tell which path the underlying client took, but newline-joins are
// safely splittable. Splitting on commas is unsafe and is intentionally not
// attempted — at worst we recover one cookie under (a)/(c), which matches the
// pre-fix behavior, and recover all of them under (b).
static std::vector<std::string> CollectSetCookieHeaders(const HTTPResponse &response) {
	std::vector<std::string> out;
	if (!response.HasHeader("Set-Cookie")) {
		return out;
	}
	const std::string raw = response.GetHeaderValue("Set-Cookie");
	// Split on \n; tolerate \r\n by trimming trailing \r.
	size_t start = 0;
	while (start < raw.size()) {
		size_t end = raw.find('\n', start);
		size_t len = (end == std::string::npos) ? raw.size() - start : end - start;
		std::string line = raw.substr(start, len);
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}
		if (!line.empty()) {
			out.push_back(std::move(line));
		}
		if (end == std::string::npos) {
			break;
		}
		start = end + 1;
	}
	return out;
}

// Internal: perform a single HTTP POST with optional auth header.
//
// cached_http_params: when non-null, use it instead of calling
// HTTPUtil::InitializeParameters. The cached path avoids re-entering the
// secret manager (which takes the MetaTransaction mutex) on every request —
// required for HTTP RPCs invoked from VgiTransaction::Start to not deadlock.
// TODO(#22258): drop when https://github.com/duckdb/duckdb/issues/22258 is fixed.
// Pick the first encoding from ``offered`` that we can also produce.  Falls
// back to GZIP (always available; the C++ side links miniz unconditionally)
// when no overlap exists — the caller has nothing left to retry with anyway.
static HttpEncoding PickAlternateEncoding(const std::vector<HttpEncoding> &offered, HttpEncoding tried) {
	for (auto enc : offered) {
		if (enc != tried && enc != HttpEncoding::NONE) {
			return enc;
		}
	}
	return tried == HttpEncoding::ZSTD ? HttpEncoding::GZIP : HttpEncoding::ZSTD;
}

static std::string HttpPostArrowIpcInternal(ClientContext &context,
                                             const std::string &url,
                                             const std::vector<uint8_t> &body,
                                             const std::string &bearer_token,
                                             const std::shared_ptr<SessionCookieJar> &cookie_jar,
                                             std::unique_ptr<HTTPResponse> &out_response,
                                             const std::shared_ptr<HTTPParams> &cached_http_params = nullptr,
                                             HttpEncoding request_encoding = HttpEncoding::ZSTD,
                                             bool allow_codec_retry = true) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);

	// Reuse the cached HTTPParams when available (see cache rationale above);
	// otherwise fall back to a per-request InitializeParameters (still used by
	// call sites that don't have a VgiAttachParameters to cache on, e.g. the
	// very first catalog_attach before we have an attach handle).
	std::shared_ptr<HTTPParams> params = cached_http_params;
	if (!params) {
		auto owned = http_util.InitializeParameters(context, url);
		params = std::shared_ptr<HTTPParams>(owned.release());
	}
	// Always re-apply the timeout from the current setting. The cached path
	// was previously skipping this, so users who tweaked vgi_http_timeout_seconds
	// to debug a stuck endpoint saw no effect until re-ATTACH.
	ApplyHttpTimeout(context, *params);

	// Compress the request body with the chosen codec.  Default zstd matches
	// every existing VGI server; a future gzip-only server is recovered via
	// the 415-retry path below using ``VGI-Supported-Encodings``.
	auto compressed_body = Compress(request_encoding, body.data(), body.size());

	HTTPHeaders headers;
	headers.Insert("Content-Type", ARROW_IPC_CONTENT_TYPE);
	headers.Insert("Content-Encoding", EncodingName(request_encoding));
	headers.Insert("X-VGI-Accept-Encoding", ClientAcceptEncoding());
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
	if (!out_response) {
		throw IOException("VGI HTTP POST returned no response (transport failure) [url: %s]", url);
	}

	if (cookie_jar) {
		auto set_cookie_headers = CollectSetCookieHeaders(*out_response);
		if (!set_cookie_headers.empty()) {
			cookie_jar->UpdateFromSetCookie(set_cookie_headers, UrlIsHttps(url));
		}
	}

	if (out_response->status == HTTPStatusCode::Unauthorized_401) {
		// Return empty — caller handles 401
		return "";
	}

	// 415-retry: the codec we picked isn't enabled on this server.  When the
	// response carries ``VGI-Supported-Encodings`` (stamped on every response
	// by ``_CapabilitiesMiddleware``), re-pick and retry once.  This is the
	// forward-compat hook for "first call against a gzip-only server" —
	// before caps are discovered we default to zstd, and gzip-only servers
	// reject with 415 + the right header.
	if (out_response->status == HTTPStatusCode::UnsupportedMediaType_415 && allow_codec_retry &&
	    out_response->HasHeader(kSupportedEncodingsHeader)) {
		auto offered = ParseAcceptList(out_response->GetHeaderValue(kSupportedEncodingsHeader));
		if (!offered.empty()) {
			auto alternate = PickAlternateEncoding(offered, request_encoding);
			if (alternate != request_encoding) {
				return HttpPostArrowIpcInternal(context, url, body, bearer_token, cookie_jar,
				                                 out_response, cached_http_params, alternate,
				                                 /*allow_codec_retry=*/false);
			}
		}
	}

	if (!out_response->Success()) {
		std::string error_body = post.buffer_out.empty() ? out_response->body
		                                                 : std::string(post.buffer_out.data(),
		                                                               post.buffer_out.data() + post.buffer_out.size());
		// Decompress if the server advertised a codec we know — otherwise
		// Arrow IPC parsing would see compressed-stream magic bytes and
		// throw "negative continuation token".
		auto error_enc = ResolveResponseEncoding(*out_response);
		if (!error_body.empty() && error_enc != HttpEncoding::NONE) {
			try {
				error_body = Decompress(error_enc, error_body.data(), error_body.size());
			} catch (...) {
				// Leave body as-is; the raw bytes will appear in the preview.
			}
		}
		std::string content_type = out_response->HasHeader("Content-Type")
		                               ? out_response->GetHeaderValue("Content-Type")
		                               : std::string();
		bool is_arrow_ipc = content_type.find(ARROW_IPC_CONTENT_TYPE) != std::string::npos;

		// If the body looks like Arrow IPC, parse it. Two outcomes:
		// 1) ReadUnaryResponseFromBuffer dispatches a VGI-protocol error batch and throws
		//    an IOException with the worker's original message — we re-throw.
		// 2) It returns a data batch (e.g., a server-specific error representation
		//    carrying fields like {"exception_type", "exception_message", "traceback"}).
		//    Extract textual columns as a readable preview.
		std::string body_preview;
		if (is_arrow_ipc && !error_body.empty()) {
			try {
				auto error_result = ReadUnaryResponseFromBuffer(
				    reinterpret_cast<const uint8_t *>(error_body.data()),
				    error_body.size(), nullptr, url);
				if (error_result.batch) {
					const auto &batch = error_result.batch;
					for (int c = 0; c < batch->num_columns() && body_preview.size() < 1024; ++c) {
						auto col = batch->column(c);
						auto name = batch->schema()->field(c)->name();
						auto str = std::dynamic_pointer_cast<arrow::StringArray>(col);
						auto lstr = std::dynamic_pointer_cast<arrow::LargeStringArray>(col);
						auto bin = std::dynamic_pointer_cast<arrow::BinaryArray>(col);
						auto lbin = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(col);
						for (int64_t i = 0; i < col->length() && body_preview.size() < 1024; ++i) {
							std::string val;
							if (str && !str->IsNull(i)) {
								val = str->GetString(i);
							} else if (lstr && !lstr->IsNull(i)) {
								val = lstr->GetString(i);
							} else if (bin && !bin->IsNull(i)) {
								auto view = bin->GetView(i);
								val = std::string(view.data(), view.size());
							} else if (lbin && !lbin->IsNull(i)) {
								auto view = lbin->GetView(i);
								val = std::string(view.data(), view.size());
							} else {
								continue;
							}
							if (!body_preview.empty()) body_preview += "; ";
							body_preview += name + "=" + val;
						}
					}
				}
			} catch (const Exception &) {
				throw; // VGI worker error (IOException, InvalidInputException, etc.) — propagate
			} catch (...) {
				// Not Arrow IPC despite the header — fall through.
			}
		}
		if (body_preview.empty() && !error_body.empty()) {
			body_preview = error_body;
		}
		if (body_preview.size() > 1024) {
			body_preview.resize(1024);
			body_preview += "...<truncated>";
		}
		throw IOException(
		    "VGI HTTP request failed (HTTP %d)%s%s: %s [url: %s]",
		    static_cast<int>(out_response->status),
		    content_type.empty() ? "" : " Content-Type=",
		    content_type,
		    body_preview.empty() ? out_response->GetError() : body_preview, url);
	}

	// In the browser (WASM), fetch/XHR transparently decompresses any STANDARD
	// Content-Encoding (gzip/br/zstd) before the bytes reach us, leaving the body
	// decoded while Content-Length still reports the *compressed* size. The custom
	// X-VGI-Content-Encoding header is NOT folded by the browser, so it still
	// routes through our own decompressor. So when the server stamped a standard
	// Content-Encoding (and not X-VGI-Content-Encoding), treat the body as already
	// decoded: skip the size check and skip our own decompress below.
	bool browser_decoded = false;
#if defined(__EMSCRIPTEN__)
	browser_decoded = !out_response->HasHeader("X-VGI-Content-Encoding") &&
	                  out_response->HasHeader("Content-Encoding");
#endif

	// Defensive: if the server sent a Content-Length header, ensure the
	// buffered body matches. Truncated responses (mid-stream proxy timeout,
	// dropped connection on a chunked transfer that the underlying client
	// silently swallows) would otherwise be parsed as a valid empty result
	// — silent wrong-result. Arrow IPC tolerates a truncated body by
	// returning Invalid which we'd previously treat as EOS. Skipped when the
	// browser transparently decompressed (body no longer matches Content-Length).
	if (!browser_decoded && out_response->HasHeader("Content-Length")) {
		const auto content_length_str = out_response->GetHeaderValue("Content-Length");
		try {
			auto declared = std::stoull(content_length_str);
			if (declared != post.buffer_out.size()) {
				throw IOException(
				    "VGI HTTP response body size mismatch: Content-Length=%llu, got %llu bytes [url: %s]",
				    static_cast<unsigned long long>(declared),
				    static_cast<unsigned long long>(post.buffer_out.size()), url);
			}
		} catch (const std::invalid_argument &) {
			// Malformed Content-Length — let the caller's Arrow parser
			// decide whether the body is intelligible.
		} catch (const std::out_of_range &) {
			// Same.
		}
	}

	// Decompress the response if the server stamped a codec we know. On WASM the
	// browser already decoded a standard Content-Encoding (browser_decoded), so
	// there's nothing left for us to decompress in that case.
	auto resp_enc = browser_decoded ? HttpEncoding::NONE : ResolveResponseEncoding(*out_response);
	// Capture the on-the-wire size before app-level decompression so the log
	// below reflects what was actually read off the socket. VGI uses the custom
	// X-VGI-Content-Encoding header (which generic proxies/clients don't fold),
	// so post.buffer_out here is still the compressed application body.
	const size_t resp_wire_bytes = post.buffer_out.size();
	if (resp_enc != HttpEncoding::NONE && !post.buffer_out.empty()) {
		post.buffer_out = Decompress(resp_enc, post.buffer_out.data(), post.buffer_out.size());
	}
	const size_t resp_decoded_bytes = post.buffer_out.size();

	// Per-response payload accounting for HTTP-transport debugging: how many
	// bytes were read, whether the response was compressed (and with which
	// codec), the decompressed size, and the decompression ratio. Both request
	// directions are included so a slow scan can be attributed to wire volume vs
	// codec choice. Surfaced via VGI_LOG (duckdb_logs type 'VGI' + stderr when
	// VGI_STDERR_LOG=1). The matching Content-Length check above guards truncation.
	{
		const bool resp_compressed = resp_enc != HttpEncoding::NONE;
		char ratio_buf[32];
		std::snprintf(ratio_buf, sizeof(ratio_buf), "%.2f",
		              resp_wire_bytes > 0
		                  ? static_cast<double>(resp_decoded_bytes) / static_cast<double>(resp_wire_bytes)
		                  : 0.0);
		VGI_LOG(context, "http.response",
		        {{"url", url},
		         {"status", std::to_string(static_cast<int>(out_response->status))},
		         {"req_encoding", EncodingName(request_encoding)},
		         {"req_raw_bytes", std::to_string(body.size())},
		         {"req_wire_bytes", std::to_string(compressed_body.size())},
		         {"resp_compressed", resp_compressed ? "true" : "false"},
		         {"resp_encoding", resp_compressed ? EncodingName(resp_enc) : "none"},
		         {"resp_wire_bytes", std::to_string(resp_wire_bytes)},
		         {"resp_decoded_bytes", std::to_string(resp_decoded_bytes)},
		         {"resp_decompress_ratio", ratio_buf}});
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
                              const std::shared_ptr<SessionCookieJar> &cookie_jar,
                              const std::shared_ptr<HTTPParams> &cached_http_params) {
	// Get cached token from per-catalog auth (if any)
	std::string token;
	if (auth) {
		token = auth->GetToken();
	}

	std::unique_ptr<HTTPResponse> response;
	auto result = HttpPostArrowIpcInternal(context, url, body, token, cookie_jar, response, cached_http_params);

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
		// Special case: the default auth handler when the user supplied
		// neither bearer_token nor oauth_refresh_token is an empty
		// OAuthCatalogAuth. Feeding an empty OAuthChallenge into its
		// HandleUnauthorized would eventually call FetchResourceMetadata("")
		// and surface "VGI OAuth: resource metadata URL must use HTTPS:"
		// — a confusing diagnostic for a non-OAuth situation. Catch it
		// here while we still have the URL in scope and produce an
		// actionable error that names the fix.
		if (!auth->IsExplicitlyConfigured()) {
			throw IOException(
			    "VGI HTTP authentication failed (HTTP 401) [url: %s]. The server requires "
			    "authentication but advertised no OAuth challenge (no WWW-Authenticate "
			    "header). Pass bearer_token in ATTACH options, or oauth_refresh_token if "
			    "the server uses OAuth without challenge advertising.",
			    url);
		}
		// User opted into auth (bearer_token or oauth_refresh_token) — let
		// the handler decide. BearerTokenCatalogAuth throws a token-rejected
		// error; a seeded OAuthCatalogAuth attempts refresh.
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
	result = HttpPostArrowIpcInternal(context, url, body, new_token, cookie_jar, response, cached_http_params);
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
                                     const std::shared_ptr<SessionCookieJar> &cookie_jar,
                                     const std::shared_ptr<HTTPParams> &cached_http_params,
                                     const std::string &invocation_id_hex,
                                     const std::string &attach_opaque_data_hex,
                                     const std::string &transaction_opaque_data_hex,
                                     const std::string &conn_id_hex) {
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
	auto response_body = HttpPostArrowIpc(context, url, body, auth, cookie_jar, cached_http_params);

	// Parse the Arrow IPC response
	auto result = ReadUnaryResponseFromBuffer(
	    reinterpret_cast<const uint8_t *>(response_body.data()),
	    response_body.size(), &context, url,
	    invocation_id_hex, attach_opaque_data_hex,
	    transaction_opaque_data_hex, conn_id_hex);

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
	headers.Insert("X-VGI-Accept-Encoding", ClientAcceptEncoding());
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
	if (!response) {
		throw IOException("VGI external location fetch returned no response (transport failure) [url: %s]",
		                  url);
	}

	if (!response->Success()) {
		throw IOException("VGI external location fetch failed (HTTP %d) [url: %s]",
		                  static_cast<int>(response->status), url);
	}

	// Decompress if the server indicates a codec we know.
	auto get_enc = ResolveResponseEncoding(*response);
	if (get_enc != HttpEncoding::NONE && !body.empty()) {
		return Decompress(get_enc, body.data(), body.size());
	}

	return body;
}

UnaryResponseResult ResolveExternalLocation(ClientContext &context,
                                             const std::string &location_url,
                                             const std::string &worker_path,
                                             const std::string &invocation_id_hex,
                                             const std::string &attach_opaque_data_hex,
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
			                     -1, invocation_id_hex, attach_opaque_data_hex);
			throw IOException("VGI external location error [url: %s]", location_url);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path,
			                     -1, invocation_id_hex, attach_opaque_data_hex);
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
			                     -1, invocation_id_hex, attach_opaque_data_hex);
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

// Parse "Cache-Control: max-age=N" (seconds) from a response, if present.
// Returns 0 seconds when no max-age directive is found or it does not parse.
static std::chrono::seconds ParseCacheControlMaxAge(const HTTPResponse &response) {
	const char *header_names[] = {"Cache-Control", "cache-control"};
	for (const char *name : header_names) {
		if (!response.HasHeader(name)) {
			continue;
		}
		auto value = response.GetHeaderValue(name);
		// Tokenise on commas, look for "max-age=N" (case-insensitive, trimmed).
		size_t pos = 0;
		while (pos < value.size()) {
			size_t comma = value.find(',', pos);
			std::string token = value.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
			// trim
			size_t a = token.find_first_not_of(" \t");
			size_t b = token.find_last_not_of(" \t");
			if (a != std::string::npos) {
				token = token.substr(a, b - a + 1);
			} else {
				token.clear();
			}
			// lowercase prefix check
			std::string lower;
			lower.reserve(token.size());
			for (char c : token) {
				lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
			}
			constexpr const char *kMaxAge = "max-age=";
			if (lower.rfind(kMaxAge, 0) == 0) {
				try {
					int64_t secs = std::stoll(lower.substr(std::strlen(kMaxAge)));
					if (secs < 0) {
						secs = 0;
					}
					return std::chrono::seconds(secs);
				} catch (...) {
					return std::chrono::seconds(0);
				}
			}
			if (comma == std::string::npos) {
				break;
			}
			pos = comma + 1;
		}
		break;
	}
	return std::chrono::seconds(0);
}

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
	if (response.HasHeader(kSupportedEncodingsHeader)) {
		caps.supported_encodings = ParseAcceptList(response.GetHeaderValue(kSupportedEncodingsHeader));
	}

	auto max_age = ParseCacheControlMaxAge(response);
	if (max_age.count() > 0) {
		caps.cache_expires_at = std::chrono::steady_clock::now() + max_age;
	}

	return caps;
}

ServerCapabilities HttpDiscoverCapabilities(ClientContext &context, const std::string &base_url) {
	// Capability headers are emitted by middleware on every response. Probe
	// {base_url}/health: it is mandatory in every implementation and exempt
	// from auth, matching the Python reference client.
	auto url = NormalizeBaseUrl(base_url) + "/health";

	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	ApplyHttpTimeout(context, *params);

	HTTPHeaders headers;
	HeadRequestInfo head(url, headers, *params);
	auto response = http_util.Request(head);
	if (!response) {
		// Transport failure — treat as "no capabilities discovered" rather
		// than crashing. Caller's defaults apply.
		return ServerCapabilities{};
	}

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
                   const std::vector<uint8_t> &data, HttpEncoding encoding) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	ApplyHttpTimeout(context, *params);

	HTTPHeaders headers;
	std::string content_type = "application/octet-stream";

	const uint8_t *body_data = data.data();
	size_t body_size = data.size();
	std::vector<uint8_t> compressed;

	if (encoding != HttpEncoding::NONE) {
		compressed = Compress(encoding, data.data(), data.size());
		body_data = compressed.data();
		body_size = compressed.size();
		headers.Insert("Content-Encoding", EncodingName(encoding));
		headers.Insert("X-VGI-Content-Encoding", EncodingName(encoding));
	}

	PutRequestInfo put(url, headers, *params,
	                   reinterpret_cast<const_data_ptr_t>(body_data),
	                   static_cast<idx_t>(body_size),
	                   content_type);
	auto response = http_util.Request(put);
	if (!response) {
		throw IOException("VGI upload returned no response (transport failure) [url: %s]", url);
	}

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
