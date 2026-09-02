// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_http_client.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <optional>

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/enums/http_status_code.hpp"

#include "vgi_cookie_jar.hpp"
#include "vgi_http_compression.hpp"
#include "vgi_httpi.hpp"
#include "vgi_iroh_native.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_transport.hpp"

#include "mbedtls_wrapper.hpp"

namespace duckdb {
namespace vgi {

// Header used to advertise / negotiate the set of supported content
// encodings.  Mirrors the constant in ``vgi_rpc/http/_common.py``.
static constexpr const char *kSupportedEncodingsHeader = "VGI-Supported-Encodings";
static constexpr const char *kAcceptMaxResponseBytesHeader = "VGI-Accept-Max-Response-Bytes";
static constexpr const char *kAcceptMaxResponseBytesSupportHeader =
    "VGI-Accept-Max-Response-Bytes-Support";
static constexpr int64_t kMinAcceptedMaxResponseBytes = 64LL << 10;
static constexpr int64_t kMaxSafeInteger = 9007199254740991LL;
// Independent safety ceiling for a buffered HTTP representation. This is not
// the VGI decoded Arrow budget: a compressed representation can legitimately
// be larger than the bytes produced after decoding. HTTPI enforces this while
// streaming chunks into WASM; DuckDB's native POST API currently exposes the
// completed string only, so its check is necessarily post-read.
static constexpr size_t kMaxBufferedRepresentationBytes = 1024ULL * 1024ULL * 1024ULL;
#if defined(__EMSCRIPTEN__)
static constexpr int64_t kDefaultAcceptedMaxResponseBytes = 64LL << 20;
#else
static constexpr int64_t kDefaultAcceptedMaxResponseBytes = 256LL << 20;
#endif

// Comma-joined list of codecs we can produce / decode, in our preference
// order — sent on every request so the server can pick a codec for its
// response.  The reverse direction (what the server accepts on REQUEST
// bodies) is the ``VGI-Supported-Encodings`` header it stamps on every
// response; see the renegotiation path in HttpPostArrowIpcInternal.
static std::string ClientAcceptEncoding() {
	return "zstd, gzip";
}

static bool HeaderNameEqual(const std::string &left, const std::string &right) {
	if (left.size() != right.size())
		return false;
	for (size_t i = 0; i < left.size(); ++i) {
		if (std::tolower(static_cast<unsigned char>(left[i])) != std::tolower(static_cast<unsigned char>(right[i])))
			return false;
	}
	return true;
}

// Preserve the native HTTPResponse behavior for http(s), while retaining the
// ordered duplicate header fields and explicit raw representation marker that
// arrive through the browser HTTPI SAB envelope.
struct RpcHttpResponse {
	std::unique_ptr<HTTPResponse> native;
	std::optional<httpi::Response> browser;
	std::optional<IrohNativeHttpResponse> iroh;

	bool HasHeader(const std::string &name) const {
		if (native)
			return native->HasHeader(name);
		if (browser) {
			for (const auto &header : browser->headers) {
				if (HeaderNameEqual(header.name, name))
					return true;
			}
			return false;
		}
		for (const auto &header : iroh->headers) {
			if (HeaderNameEqual(header.first, name))
				return true;
		}
		return false;
	}

	std::string GetHeaderValue(const std::string &name) const {
		if (native)
			return native->GetHeaderValue(name);
		if (browser) {
			for (auto it = browser->headers.rbegin(); it != browser->headers.rend(); ++it) {
				if (HeaderNameEqual(it->name, name))
					return it->value;
			}
			return "";
		}
		for (auto it = iroh->headers.rbegin(); it != iroh->headers.rend(); ++it) {
			if (HeaderNameEqual(it->first, name))
				return it->second;
		}
		return "";
	}

	std::vector<std::string> HeaderValues(const std::string &name) const {
		if (native) {
			if (!native->HasHeader(name))
				return {};
			return {native->GetHeaderValue(name)};
		}
		std::vector<std::string> result;
		if (browser) {
			for (const auto &header : browser->headers) {
				if (HeaderNameEqual(header.name, name))
					result.push_back(header.value);
			}
		} else {
			for (const auto &header : iroh->headers) {
				if (HeaderNameEqual(header.first, name))
					result.push_back(header.second);
			}
		}
		return result;
	}

	HTTPStatusCode Status() const {
		return native ? native->status : static_cast<HTTPStatusCode>(browser ? browser->status : iroh->status);
	}
	bool Success() const {
		if (native)
			return native->Success();
		auto status = browser ? browser->status : iroh->status;
		return status >= 200 && status < 300;
	}
	bool HasRequestError() const {
		return native && native->HasRequestError();
	}
	std::string GetError() const {
		return native ? native->GetError() : std::string();
	}
	std::string FallbackBody() const {
		if (native)
			return native->body;
		if (browser)
			return browser->body;
		return std::string(reinterpret_cast<const char *>(iroh->body.data()), iroh->body.size());
	}
	bool RawRepresentation() const {
		return iroh.has_value() || (browser.has_value() && browser->raw_representation);
	}
};

// Resolve the encoding advertised on a response.  Prefer the custom
// ``X-VGI-Content-Encoding`` header (older servers stamp this on every
// response — generic proxies don't fold it), falling back to the standard
// ``Content-Encoding``.  Returns ``NONE`` when no codec is advertised or
// when the token is unknown.
template <class RESPONSE>
static HttpEncoding ResolveResponseEncoding(const RESPONSE &response) {
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
static uint64_t GetHttpTimeoutSeconds(ClientContext &context) {
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
		return static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
	}
	return 300; // fallback: 5 minutes
}

static int64_t GetAcceptedMaxResponseBytes(ClientContext &context) {
	Value value;
	int64_t result = kDefaultAcceptedMaxResponseBytes;
	if (context.TryGetCurrentSetting("vgi_http_accepted_max_response_bytes", value)) {
		result = value.GetValue<int64_t>();
	}
	if (result < kMinAcceptedMaxResponseBytes || result > kMaxSafeInteger) {
		throw InvalidInputException(
		    "vgi_http_accepted_max_response_bytes must be between 65536 and 9007199254740991");
	}
#if defined(__EMSCRIPTEN__)
	// The current HTTPI bridge materializes one response in wasm32 linear
	// memory. Advertise the tighter transport cap so the server never relies on
	// a value the browser envelope cannot honor.
	result = std::min<int64_t>(result, static_cast<int64_t>(httpi::kMaxBufferedBodyBytes));
#endif
	return result;
}

static void ApplyHttpTimeout(ClientContext &context, HTTPParams &params) {
	params.timeout = GetHttpTimeoutSeconds(context);
}

// Check whether ``url`` is an https origin — used to gate Secure cookies.
static bool UrlIsSecure(const std::string &url) {
	// Case-insensitive prefix match — HTTPUtil accepts both cases.
	if (url.size() < 8) {
		return false;
	}
	const bool https = (url[0] == 'h' || url[0] == 'H') && (url[1] == 't' || url[1] == 'T') &&
	       (url[2] == 't' || url[2] == 'T') && (url[3] == 'p' || url[3] == 'P') &&
	       (url[4] == 's' || url[4] == 'S') && url[5] == ':';
	return https || IsHttpiTransport(url);
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
static std::vector<std::string> CollectSetCookieHeaders(const RpcHttpResponse &response) {
	std::vector<std::string> out;
	if (!response.HasHeader("Set-Cookie")) {
		return out;
	}
	if (response.browser || response.iroh) {
		return response.HeaderValues("Set-Cookie");
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

// Forward declarations — defined below with the capability-header parsing.
template <class RESPONSE>
static ServerCapabilities ParseCapabilityHeaders(const RESPONSE &response);
template <class RESPONSE>
static bool HasCapabilityHeaders(const RESPONSE &response);

// Internal: perform a single HTTP POST with optional auth header.
//
// cached_http_params: when non-null, use it instead of calling
// HTTPUtil::InitializeParameters. The cached path avoids re-entering the
// secret manager (which takes the MetaTransaction mutex) on every request —
// required for HTTP RPCs invoked from VgiTransaction::Start to not deadlock.
// TODO(#22258): drop when https://github.com/duckdb/duckdb/issues/22258 is fixed.
//
// request_encoding: codec for the body, chosen by the caller from the
// capabilities harvested so far. When the server turns out not to accept it,
// the renegotiate-and-retry path below re-encodes once (gated by
// allow_codec_retry so a retry can't recurse).
// harvested_caps: in/out — refreshed from this response's capability headers.
static std::string HttpPostArrowIpcInternal(
    ClientContext &context, const std::string &url, const std::vector<uint8_t> &body, const std::string &bearer_token,
    const std::shared_ptr<SessionCookieJar> &cookie_jar, std::unique_ptr<RpcHttpResponse> &out_response,
    const std::shared_ptr<HTTPParams> &cached_http_params = nullptr, HttpEncoding request_encoding = HttpEncoding::ZSTD,
    bool allow_codec_retry = true, duckdb::unique_ptr<HTTPClient> *client_holder = nullptr,
                                             ServerCapabilities *harvested_caps = nullptr,
                                             const std::shared_ptr<IrohClientConfig> &iroh_config = nullptr) {
	const bool is_httpi = IsHttpiTransport(url);
	const uint64_t timeout_seconds = GetHttpTimeoutSeconds(context);
	const int64_t accepted_max_response_bytes = GetAcceptedMaxResponseBytes(context);
	int64_t effective_max_response_bytes = accepted_max_response_bytes;
	if (harvested_caps && harvested_caps->max_response_bytes >= kMinAcceptedMaxResponseBytes) {
		effective_max_response_bytes =
		    std::min(effective_max_response_bytes, harvested_caps->max_response_bytes);
	}

	// Skip compression for tiny bodies (producer ticks, small unary
	// envelopes): zstd adds CPU on the per-request hot path and often GROWS a
	// sub-KiB payload past its raw size. The server treats an absent
	// Content-Encoding header as identity (do NOT send a literal "identity"
	// token — the server 415s unknown codecs).
	static constexpr size_t kMinCompressBytes = 1024;
	if (request_encoding != HttpEncoding::NONE && body.size() < kMinCompressBytes) {
		request_encoding = HttpEncoding::NONE;
	}

	// Compress the request body with the chosen codec.  The caller picked it
	// from whatever ``VGI-Supported-Encodings`` we have already harvested for
	// this server (zstd when nothing is known yet — the pre-update default);
	// a server that turns out not to accept it is recovered by the
	// renegotiate-and-retry path below.  The NONE path posts ``body``
	// directly — no copy.
	std::vector<uint8_t> compressed_body;
	const uint8_t *req_body_data = body.data();
	size_t req_body_size = body.size();
	if (request_encoding != HttpEncoding::NONE) {
		compressed_body = Compress(request_encoding, body.data(), body.size());
		req_body_data = compressed_body.data();
		req_body_size = compressed_body.size();
	}

	HTTPHeaders headers;
	std::vector<httpi::Header> httpi_headers;
	auto add_header = [&](const std::string &name, const std::string &value) {
		headers.Insert(name, value);
		httpi_headers.push_back({name, value});
	};
	add_header("Content-Type", ARROW_IPC_CONTENT_TYPE);
	if (request_encoding != HttpEncoding::NONE) {
		add_header("Content-Encoding", EncodingName(request_encoding));
	}
	add_header("X-VGI-Accept-Encoding", ClientAcceptEncoding());
	add_header(kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes));
	if (!bearer_token.empty()) {
		add_header("Authorization", "Bearer " + bearer_token);
	}
	if (cookie_jar) {
		auto cookie_header = cookie_jar->BuildCookieHeader();
		if (!cookie_header.empty()) {
			add_header("Cookie", cookie_header);
		}
	}

	std::string response_body;
	out_response = std::make_unique<RpcHttpResponse>();
	if (is_httpi) {
#if defined(__EMSCRIPTEN__)
		// Only the backend changes: the surrounding auth, cookie, capability,
		// compression, error and continuation state machine remains identical.
		// BrowserPost never retries a transport failure, because POST dispatch may
		// be ambiguous once Iroh accepted request bytes.
		out_response->browser = httpi::BrowserPost(context, url, httpi_headers, req_body_data,
		                                        req_body_size, timeout_seconds,
		                                        kMaxBufferedRepresentationBytes,
		                                        static_cast<size_t>(effective_max_response_bytes));
		response_body = out_response->browser->body;
#else
		if (!iroh_config) {
			throw InternalException("vgi: native httpi:// request is missing its ATTACH configuration");
		}
		auto parsed = ParseHttpiUrl(url);
		out_response->iroh = PerformIrohHttpRequest(
		    iroh_config, &context, "POST", parsed.path.empty() ? "/" : parsed.path,
		    [&]() {
			    std::vector<std::pair<std::string, std::string>> result;
			    result.reserve(httpi_headers.size());
			    for (const auto &header : httpi_headers) result.emplace_back(header.name, header.value);
			    return result;
		    }(),
		    req_body_data, req_body_size,
		    std::min<uint64_t>(kMaxBufferedRepresentationBytes,
		                       static_cast<uint64_t>(effective_max_response_bytes)));
		response_body.assign(reinterpret_cast<const char *>(out_response->iroh->body.data()),
		                     out_response->iroh->body.size());
#endif
	} else {
		auto &http_util = HTTPUtil::Get(*context.db);
		// Reuse cached parameters when available; unlike httpi:// this remains an
		// ordinary DuckDB HTTP request, including its established retry behavior.
		std::shared_ptr<HTTPParams> params = cached_http_params;
		if (!params) {
			auto owned = http_util.InitializeParameters(context, url);
			params = std::shared_ptr<HTTPParams>(owned.release());
		}
		params->timeout = timeout_seconds;
		PostRequestInfo post(url, headers, *params, reinterpret_cast<const_data_ptr_t>(req_body_data),
	                     static_cast<idx_t>(req_body_size));
	post.try_request = true;
	if (client_holder) {
			out_response->native = http_util.Request(post, *client_holder);
	} else {
			out_response->native = http_util.Request(post);
	}
		if (!out_response->native) {
		throw IOException("VGI HTTP POST returned no response (transport failure) [url: %s]", url);
	}
		if (!post.buffer_out.empty()) {
			response_body.assign(post.buffer_out.data(), post.buffer_out.data() + post.buffer_out.size());
		}
	}
	if (response_body.size() > kMaxBufferedRepresentationBytes) {
		throw IOException("VGI HTTP response representation exceeds buffered transport limit (%llu > %llu) [url: %s]",
		                  static_cast<unsigned long long>(response_body.size()),
		                  static_cast<unsigned long long>(kMaxBufferedRepresentationBytes), url);
	}

	if (cookie_jar) {
		auto set_cookie_headers = CollectSetCookieHeaders(*out_response);
		if (!set_cookie_headers.empty()) {
			cookie_jar->UpdateFromSetCookie(set_cookie_headers, UrlIsSecure(url));
		}
	}

	// Harvest server capabilities off this response — ANY response, whatever
	// its status. The server middleware stamps the capability headers on every
	// one, so callers that pass a harvest slot learn ServerCapabilities from
	// traffic they were already generating (avoiding later re-probes after the
	// mandatory initial discovery),
	// and — the reason this runs before the status handling below — a failure
	// caused by a codec the server can't decode still tells us which codecs it
	// can. Responses with no VGI capability header at all (a proxy error page,
	// a non-VGI endpoint) are ignored rather than cached as "server advertises
	// nothing", which would be indistinguishable from a real answer.
	ServerCapabilities response_caps;
	if (HasCapabilityHeaders(*out_response)) {
		response_caps = ParseCapabilityHeaders(*out_response);
		if (response_caps.max_response_bytes >= kMinAcceptedMaxResponseBytes) {
			effective_max_response_bytes =
			    std::min(effective_max_response_bytes, response_caps.max_response_bytes);
		}
		if (harvested_caps) {
			// Optional capability fields refine the discovery snapshot; omission
			// does not revoke a previously advertised hard bound. Exact support is
			// intentionally not inherited because every response must prove it.
			if (response_caps.max_response_bytes < 0) {
				response_caps.max_response_bytes = harvested_caps->max_response_bytes;
			}
			if (response_caps.max_request_bytes < 0) {
				response_caps.max_request_bytes = harvested_caps->max_request_bytes;
			}
			if (response_caps.max_upload_bytes < 0) {
				response_caps.max_upload_bytes = harvested_caps->max_upload_bytes;
			}
			if (!response_caps.encodings_advertised && harvested_caps->encodings_advertised) {
				response_caps.encodings_advertised = true;
				response_caps.supported_encodings = harvested_caps->supported_encodings;
			}
			if (response_caps.cache_expires_at == std::chrono::steady_clock::time_point{}) {
				response_caps.cache_expires_at = harvested_caps->cache_expires_at;
			}
			*harvested_caps = response_caps;
		}
	}
	if (!response_caps.discovered || !response_caps.accept_max_response_bytes_support) {
		throw IOException("VGI HTTP response does not advertise %s: true [url: %s]",
		                  kAcceptMaxResponseBytesSupportHeader, url);
	}

	if (out_response->Status() == HTTPStatusCode::Unauthorized_401) {
		// Return empty — caller handles 401
		return "";
	}

	// Codec renegotiation: the request failed and the server just told us which
	// content encodings it accepts — and ours isn't one of them. Re-encode and
	// retry once. Covers both directions of the mismatch:
	//   * a gzip-only server (advertises "gzip", 415s our zstd body), and
	//   * a compression-disabled server, which advertises the header with an
	//     EMPTY value. That is a positive "I speak no compression", distinct
	//     from an absent header (a pre-update server, still assumed zstd), and
	//     routes us into the same identity path small bodies already take: post
	//     the bytes verbatim with no Content-Encoding header at all. Such a
	//     server has no decompressor, so it can't answer 415 — it fails while
	//     parsing the body (HTTP 500) — which is why this is keyed on the
	//     capability header rather than on a particular status code.
	// Only fires when the codec we used is genuinely unacceptable, so a request
	// the server did decode and then failed on is never re-sent.
	//
	// Cost note: recovery here is the fallback, not the steady state. The
	// harvested snapshot is cached per catalog (ServerCapabilitiesCache), so a
	// compression-disabled server costs one rejected request for the whole
	// ATTACH — and, because 500 is retryable, HTTPUtil burns its own retry
	// budget on that one before handing it back (~0.5 s of backoff, once).
	// Everything after it goes out as identity on the first try.
	if (allow_codec_retry && !out_response->Success() && response_caps.discovered &&
	    !ServerAcceptsRequestEncoding(response_caps, request_encoding)) {
		auto alternate = ChooseRequestEncoding(response_caps);
		if (alternate != request_encoding) {
			return HttpPostArrowIpcInternal(context, url, body, bearer_token, cookie_jar, out_response,
			                                cached_http_params, alternate,
			                                /*allow_codec_retry=*/false, client_holder, harvested_caps, iroh_config);
		}
	}

	// Transport-level failure (connection refused, timeout, cancellation): with
	// try_request set, HTTPUtil hands these back as a response carrying a
	// request error rather than throwing. There is no HTTP status or body to
	// report, so surface the transport error directly instead of falling into
	// the status-code formatting below.
	if (out_response->HasRequestError()) {
		throw IOException("VGI HTTP request failed (transport error): %s [url: %s]", out_response->GetError(), url);
	}

	if (!out_response->Success()) {
		std::string error_body = response_body.empty() ? out_response->FallbackBody() : response_body;
		if (error_body.size() > kMaxBufferedRepresentationBytes) {
			throw IOException("VGI HTTP response representation exceeds buffered transport limit (%llu > %llu) [url: %s]",
			                  static_cast<unsigned long long>(error_body.size()),
			                  static_cast<unsigned long long>(kMaxBufferedRepresentationBytes), url);
		}
		// Decompress if the server advertised a codec we know — otherwise
		// Arrow IPC parsing would see compressed-stream magic bytes and
		// throw "negative continuation token".
		auto error_enc = ResolveResponseEncoding(*out_response);
		if (!error_body.empty() && error_enc != HttpEncoding::NONE) {
			error_body = Decompress(error_enc, error_body.data(), error_body.size(),
			                           static_cast<size_t>(effective_max_response_bytes));
		}
		if (error_body.size() > static_cast<uint64_t>(effective_max_response_bytes)) {
			throw IOException("VGI HTTP response exceeds max_response_bytes (%llu > %llu) [url: %s]",
			                  static_cast<unsigned long long>(error_body.size()),
			                  static_cast<unsigned long long>(effective_max_response_bytes), url);
		}
		std::string content_type =
		    out_response->HasHeader("Content-Type") ? out_response->GetHeaderValue("Content-Type") : std::string();
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
				auto error_result = ReadUnaryResponseFromBuffer(reinterpret_cast<const uint8_t *>(error_body.data()),
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
							if (!body_preview.empty())
								body_preview += "; ";
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
		throw IOException("VGI HTTP request failed (HTTP %d)%s%s: %s [url: %s]",
		                  static_cast<int>(out_response->Status()),
		                  content_type.empty() ? "" : " Content-Type=", content_type,
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
	// The browser's HTTP stack transparently decompresses any standard
	// Content-Encoding (gzip/br/zstd) and MAY strip the Content-Encoding header
	// while leaving a stale, compressed Content-Length. The only encoding we
	// decompress ourselves in the browser is the custom X-VGI-Content-Encoding,
	// which the browser never folds. So when X-VGI-Content-Encoding is absent the
	// body is already final (browser-decoded or uncompressed): skip the
	// Content-Length check and our own decompress. We deliberately do NOT gate on
	// HasHeader("Content-Encoding") — that header is unreliable post-decompression.
	browser_decoded = !out_response->RawRepresentation() && !out_response->HasHeader("X-VGI-Content-Encoding");
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
			if (declared != response_body.size()) {
				throw IOException("VGI HTTP response body size mismatch: Content-Length=%llu, got %llu bytes [url: %s]",
				    static_cast<unsigned long long>(declared),
				                  static_cast<unsigned long long>(response_body.size()), url);
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
	// so response_body here is still the compressed application body.
	const size_t resp_wire_bytes = response_body.size();
	if (resp_enc != HttpEncoding::NONE && !response_body.empty()) {
		response_body = Decompress(resp_enc, response_body.data(), response_body.size(),
		                           static_cast<size_t>(effective_max_response_bytes));
	}
	const size_t resp_decoded_bytes = response_body.size();
	if (resp_decoded_bytes > static_cast<uint64_t>(effective_max_response_bytes)) {
		throw IOException("VGI HTTP response exceeds max_response_bytes (%llu > %llu) [url: %s]",
		                  static_cast<unsigned long long>(resp_decoded_bytes),
		                  static_cast<unsigned long long>(effective_max_response_bytes), url);
	}

	// Per-response payload accounting for HTTP-transport debugging: how many
	// bytes were read, whether the response was compressed (and with which
	// codec), the decompressed size, and the decompression ratio. Both request
	// directions are included so a slow scan can be attributed to wire volume vs
	// codec choice. Surfaced via VGI_LOG (duckdb_logs type 'VGI' + stderr when
	// VGI_STDERR_LOG=1). The matching Content-Length check above guards truncation.
	// Gated: this fires once per request on the hot path, so skip the ~10-field
	// string build (snprintf, to_string) when no sink is listening.
	if (VgiInfoLogActive(context)) {
		const bool resp_compressed = resp_enc != HttpEncoding::NONE;
		char ratio_buf[32];
		std::snprintf(
		    ratio_buf, sizeof(ratio_buf), "%.2f",
		    resp_wire_bytes > 0 ? static_cast<double>(resp_decoded_bytes) / static_cast<double>(resp_wire_bytes) : 0.0);
		VGI_LOG(context, "http.response",
		        {{"url", url},
		         {"status", std::to_string(static_cast<int>(out_response->Status()))},
		         {"req_encoding", request_encoding == HttpEncoding::NONE ? "none" : EncodingName(request_encoding)},
		         {"req_raw_bytes", std::to_string(body.size())},
		         {"req_wire_bytes", std::to_string(req_body_size)},
		         {"resp_compressed", resp_compressed ? "true" : "false"},
		         {"resp_encoding", resp_compressed ? EncodingName(resp_enc) : "none"},
		         {"resp_wire_bytes", std::to_string(resp_wire_bytes)},
		         {"resp_decoded_bytes", std::to_string(resp_decoded_bytes)},
		         {"resp_decompress_ratio", ratio_buf}});
	}

	// Server errors are sent as HTTP 200 with X-VGI-RPC-Error: true header
	// (so that clients which discard response bodies on 5xx still receive
	// the Arrow IPC error metadata). Parse the error batch and throw.
	if (out_response->HasHeader("X-VGI-RPC-Error") && out_response->GetHeaderValue("X-VGI-RPC-Error") == "true") {
		auto &error_body = response_body;
		if (!error_body.empty()) {
			// Walk EVERY concatenated IPC stream in the body, not just the
			// first. A streaming RPC replies with a header stream plus a data
			// stream, so a producer that raises on its first next_batch puts
			// the error in stream two; reading only stream one finds a valid
			// header, returns without throwing, and loses the message to the
			// generic throw below. DispatchErrorStreamsFromBuffer dispatches
			// the error batch, which throws with the worker's own message.
			DispatchErrorStreamsFromBuffer(reinterpret_cast<const uint8_t *>(error_body.data()), error_body.size(),
			                               nullptr, url);
		}
		throw IOException("VGI HTTP RPC error [url: %s]", url);
	}

	return response_body;
}

std::string HttpPostArrowIpc(ClientContext &context, const std::string &url, const std::vector<uint8_t> &body,
                              const std::shared_ptr<CatalogAuth> &auth,
                              const std::shared_ptr<SessionCookieJar> &cookie_jar,
                              const std::shared_ptr<HTTPParams> &cached_http_params,
                             duckdb::unique_ptr<HTTPClient> *client_holder, ServerCapabilities *harvested_caps,
                             const std::shared_ptr<IrohClientConfig> &iroh_config) {
	// Get cached token from per-catalog auth (if any)
	std::string token;
	if (auth) {
		token = auth->GetToken();
	}

	// ``harvested_caps`` is in/out: a discovered snapshot on the way in picks
	// the request codec (so a server that already told us it speaks no
	// compression is never sent a compressed body again), and it is refreshed
	// from this response's capability headers on the way out.
	const HttpEncoding request_encoding = harvested_caps ? ChooseRequestEncoding(*harvested_caps) : HttpEncoding::ZSTD;

	std::unique_ptr<RpcHttpResponse> response;
	auto result = HttpPostArrowIpcInternal(context, url, body, token, cookie_jar, response, cached_http_params,
	                                       request_encoding, /*allow_codec_retry=*/true, client_holder, harvested_caps,
	                                       iroh_config);

	if (response->Status() != HTTPStatusCode::Unauthorized_401) {
		return result;
	}

	// Got 401 — need auth to handle it
	if (!auth) {
		throw IOException("VGI HTTP authentication required (HTTP 401) but no auth configured for this catalog "
		                  "[url: %s]. Use bearer_token or oauth_refresh_token in ATTACH options.",
		                  url);
	}

	// Look for an OAuth challenge. Two cases lead to "there is none", and they
	// must be handled IDENTICALLY: no WWW-Authenticate header at all, and a
	// header that is a perfectly valid challenge carrying no OAuth
	// resource_metadata — RFC 6750's `Bearer realm="..."`, which is what a
	// plain bearer-auth server is supposed to send. Treating the second as a
	// hard error rejected the more standards-compliant server: the vgi-java
	// worker sends the RFC-6750 form, so a rejected bearer token surfaced as
	// "no OAuth resource_metadata in WWW-Authenticate header" instead of
	// "bearer token was rejected" — and because that text contains "HTTP",
	// DuckDB's sqllogic runner silently SKIPPED the whole bearer_auth test
	// file rather than failing it, so the suite reported green with zero
	// bearer coverage.
	const bool has_www_auth = response->HasHeader("WWW-Authenticate");
	std::optional<OAuthChallenge> challenge;
	if (has_www_auth) {
		challenge = ParseWWWAuthenticate(response->GetHeaderValue("WWW-Authenticate"));
	}

	if (!challenge.has_value()) {
		// The server rejected the credential outright (static bearer auth, or
		// OAuth without challenge advertising) — let the auth handler decide.
		//
		// Special case: the default handler when the user supplied neither
		// bearer_token nor oauth_refresh_token is an empty OAuthCatalogAuth.
		// Feeding an empty OAuthChallenge into its HandleUnauthorized would
		// eventually call FetchResourceMetadata("") and surface "VGI OAuth:
		// resource metadata URL must use HTTPS:" — a confusing diagnostic for
		// a non-OAuth situation. Catch it here while the URL is still in
		// scope and produce an actionable error that names the fix.
		if (!auth->IsExplicitlyConfigured()) {
			throw IOException("VGI HTTP authentication failed (HTTP 401) [url: %s]. The server requires "
			    "authentication but advertised no OAuth challenge (%s). Pass bearer_token "
			    "in ATTACH options, or oauth_refresh_token if the server uses OAuth "
			    "without challenge advertising.",
			    url,
			    has_www_auth ? "its WWW-Authenticate header carries no resource_metadata"
			                 : "no WWW-Authenticate header");
		}
		// User opted into auth (bearer_token or oauth_refresh_token) — let
		// the handler decide. BearerTokenCatalogAuth throws a token-rejected
		// error; a seeded OAuthCatalogAuth attempts refresh.
		OAuthChallenge empty_challenge;
		auth->HandleUnauthorized(empty_challenge, context);
		// If HandleUnauthorized didn't throw (shouldn't happen), surface a generic error
		throw IOException("VGI HTTP authentication failed (HTTP 401) [url: %s]", url);
	}

	VGI_STDERR_DEBUG("[VGI] http.401_received url=%s resource_metadata=%s\n", url.c_str(),
	                 challenge->resource_metadata_url.c_str());

	// Perform or wait for auth flow (OAuth PKCE/device code)
	auto new_token = auth->HandleUnauthorized(*challenge, context);

	// Retry with new token
	result = HttpPostArrowIpcInternal(context, url, body, new_token, cookie_jar, response, cached_http_params,
	                                  harvested_caps ? ChooseRequestEncoding(*harvested_caps) : request_encoding,
	                                  /*allow_codec_retry=*/true, client_holder, harvested_caps, iroh_config);
	if (response->Status() == HTTPStatusCode::Unauthorized_401) {
		throw IOException("VGI HTTP authentication failed after auth flow (HTTP 401) [url: %s]. "
		                  "Response: %s",
		                  url, response->FallbackBody());
	}

	return result;
}

UnaryResponseResult HttpInvokeUnary(ClientContext &context, const std::string &worker_path,
                                    const std::string &method_name, const std::shared_ptr<arrow::RecordBatch> &params,
                                     const std::shared_ptr<CatalogAuth> &auth,
                                     const std::shared_ptr<SessionCookieJar> &cookie_jar,
                                     const std::shared_ptr<HTTPParams> &cached_http_params,
                                    const std::string &invocation_id_hex, const std::string &attach_opaque_data_hex,
                                    const std::string &transaction_opaque_data_hex, const std::string &conn_id_hex,
                                     const std::string &protocol_version_override,
                                    duckdb::unique_ptr<HTTPClient> *client_holder, ServerCapabilities *caps,
                                    const std::shared_ptr<IrohClientConfig> &iroh_config) {
	std::string base_url = NormalizeBaseUrl(worker_path);
	std::string url = base_url + "/" + method_name;
	ServerCapabilities local_caps;
	auto *effective_caps = caps ? caps : &local_caps;
	if (!effective_caps->discovered ||
	    (effective_caps->cache_expires_at != std::chrono::steady_clock::time_point{} &&
	     std::chrono::steady_clock::now() >= effective_caps->cache_expires_at)) {
		*effective_caps = HttpDiscoverCapabilities(context, base_url, iroh_config);
	}
	if (!effective_caps->discovered || !effective_caps->accept_max_response_bytes_support) {
		throw IOException("VGI HTTP server does not advertise %s: true [url: %s]",
		                  kAcceptMaxResponseBytesSupportHeader, base_url);
	}

	// Gated: fires once per unary RPC (hot on catalog bursts / buffered sinks).
	const bool log_active = VgiInfoLogActive(context);
	if (log_active) {
		VGI_LOG(context, "http.invoke_unary", {{"url", url}, {"method", method_name}});
	}

	// Serialize the RPC request to Arrow IPC bytes. A non-empty
	// protocol_version_override stamps a different application protocol version
	// (the secret protocol) into the request metadata.
	std::vector<uint8_t> body;
	if (params) {
		body = SerializeRpcRequest(method_name, params, protocol_version_override);
	} else {
		body = SerializeEmptyRpcRequest(method_name);
	}

	// POST to {worker_path}/{method_name} using standard HTTP timeout
	auto response_body =
	    HttpPostArrowIpc(context, url, body, auth, cookie_jar, cached_http_params, client_holder,
	                     effective_caps, iroh_config);

	// Parse the Arrow IPC response. Move the body in — the string becomes the
	// owning Arrow buffer, avoiding an alloc+memcpy of the whole payload.
	auto result = ReadUnaryResponseFromBuffer(std::move(response_body), &context, url, invocation_id_hex,
	                                          attach_opaque_data_hex, transaction_opaque_data_hex, conn_id_hex);

	// Resolve external location pointer batches
	result = MaybeResolveExternalLocation(context, result, base_url);

	if (log_active) {
		VGI_LOG(context, "http.invoke_unary_result",
		        {{"url", url}, {"method", method_name}, {"has_batch", result.batch ? "true" : "false"}});
	}

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
	auto response_handler = [](const HTTPResponse &) {
		return true;
	};
	auto content_handler = [&body](const_data_ptr_t data, idx_t data_length) {
		body.append(reinterpret_cast<const char *>(data), data_length);
		return true;
	};

	GetRequestInfo get(url, headers, *params, response_handler, content_handler);
	auto response = http_util.Request(get);
	if (!response) {
		throw IOException("VGI external location fetch returned no response (transport failure) [url: %s]", url);
	}

	if (!response->Success()) {
		throw IOException("VGI external location fetch failed (HTTP %d) [url: %s]", static_cast<int>(response->status),
		                  url);
	}

	// Decompress if the server indicates a codec we know.
	auto get_enc = ResolveResponseEncoding(*response);
	if (get_enc != HttpEncoding::NONE && !body.empty()) {
		return Decompress(get_enc, body.data(), body.size());
	}

	return body;
}

UnaryResponseResult ResolveExternalLocation(ClientContext &context, const std::string &location_url,
                                            const std::string &worker_path, const std::string &invocation_id_hex,
                                             const std::string &attach_opaque_data_hex,
                                             const std::shared_ptr<arrow::KeyValueMetadata> &pointer_metadata) {
	if (IsHttpiTransport(location_url)) {
		throw IOException("VGI external locations over httpi:// are unsupported; the worker must return an "
		                  "https:// pre-signed URL [url: %s]",
		                  location_url);
	}
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
	// Adopt the fetched body as the owning Arrow buffer — this is the
	// large-batch path, so avoiding a full alloc+memcpy of the payload matters.
	// The SHA-256 check above already consumed ``body`` by reference.
	auto owned = arrow::Buffer::FromString(std::move(body));

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
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path, -1, invocation_id_hex,
			                      attach_opaque_data_hex);
			throw IOException("VGI external location error [url: %s]", location_url);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path, -1, invocation_id_hex,
			                      attach_opaque_data_hex);
			continue;
		}
		if (batch_type == RpcBatchType::EXTERNAL_LOCATION) {
			throw IOException("VGI external location redirect loop: resolved batch from %s "
			                  "contains another vgi_rpc.location",
			                  location_url);
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
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context, log_worker_path, -1, invocation_id_hex,
			                      attach_opaque_data_hex);
		}
	}

	if (!result.batch) {
		throw IOException("VGI external location contained no data batch [url: %s]", location_url);
	}

	return result;
}

UnaryResponseResult MaybeResolveExternalLocation(ClientContext &context, UnaryResponseResult &result,
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
template <class RESPONSE>
static std::chrono::seconds ParseCacheControlMaxAge(const RESPONSE &response) {
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

// Did this response come from something that speaks the VGI capability
// contract at all? Used to gate the harvest-off-every-response path: an error
// page synthesized by an intermediary carries none of these headers, and
// caching it as a discovered snapshot would look identical to a server that
// genuinely advertises nothing.
template <class RESPONSE>
static bool HasCapabilityHeaders(const RESPONSE &response) {
	return response.HasHeader(kAcceptMaxResponseBytesSupportHeader) ||
	       response.HasHeader(kSupportedEncodingsHeader) || response.HasHeader("VGI-Max-Request-Bytes") ||
	       response.HasHeader("VGI-Upload-URL-Support") || response.HasHeader("VGI-Max-Upload-Bytes");
}

template <class RESPONSE>
static bool HasSingleResponseBudgetSupport(const RESPONSE &response) {
	return response.HasHeader(kAcceptMaxResponseBytesSupportHeader) &&
	       response.GetHeaderValue(kAcceptMaxResponseBytesSupportHeader) == "true";
}

static bool HasSingleResponseBudgetSupport(const RpcHttpResponse &response) {
	const auto values = response.HeaderValues(kAcceptMaxResponseBytesSupportHeader);
	return values.size() == 1 && values[0] == "true";
}

static int64_t ParseCanonicalResponseMaximum(const std::string &value) {
	if (value.empty() || value.front() == '0') {
		throw IOException("VGI-Max-Response-Bytes must match [1-9][0-9]*");
	}
	int64_t parsed = 0;
	for (const auto byte : value) {
		if (byte < '0' || byte > '9') {
			throw IOException("VGI-Max-Response-Bytes must match [1-9][0-9]*");
		}
		const auto digit = static_cast<int64_t>(byte - '0');
		if (parsed > (kMaxSafeInteger - digit) / 10) {
			throw IOException("VGI-Max-Response-Bytes must be between 65536 and 9007199254740991");
		}
		parsed = parsed * 10 + digit;
	}
	if (parsed < kMinAcceptedMaxResponseBytes || parsed > kMaxSafeInteger) {
		throw IOException("VGI-Max-Response-Bytes must be between 65536 and 9007199254740991");
	}
	return parsed;
}

template <class RESPONSE>
static std::optional<int64_t> ParseAdvertisedResponseMaximum(const RESPONSE &response) {
	if (!response.HasHeader("VGI-Max-Response-Bytes")) {
		return std::nullopt;
	}
	return ParseCanonicalResponseMaximum(response.GetHeaderValue("VGI-Max-Response-Bytes"));
}

static std::optional<int64_t> ParseAdvertisedResponseMaximum(const RpcHttpResponse &response) {
	const auto values = response.HeaderValues("VGI-Max-Response-Bytes");
	if (values.empty()) {
		return std::nullopt;
	}
	if (values.size() != 1) {
		throw IOException("VGI-Max-Response-Bytes must occur exactly once");
	}
	return ParseCanonicalResponseMaximum(values[0]);
}

// Parse capability headers from an HTTP response (set by middleware on every response).
template <class RESPONSE>
static ServerCapabilities ParseCapabilityHeaders(const RESPONSE &response) {
	ServerCapabilities caps;
	caps.discovered = true;
	caps.accept_max_response_bytes_support = HasSingleResponseBudgetSupport(response);

	if (response.HasHeader("VGI-Max-Request-Bytes")) {
		try {
			caps.max_request_bytes = std::stoll(response.GetHeaderValue("VGI-Max-Request-Bytes"));
		} catch (...) {
		}
	}
	if (const auto max_response = ParseAdvertisedResponseMaximum(response)) {
		caps.max_response_bytes = *max_response;
	}
	if (response.HasHeader("VGI-Upload-URL-Support")) {
		caps.upload_url_support = response.GetHeaderValue("VGI-Upload-URL-Support") == "true";
	}
	if (response.HasHeader("VGI-Max-Upload-Bytes")) {
		try {
			caps.max_upload_bytes = std::stoll(response.GetHeaderValue("VGI-Max-Upload-Bytes"));
		} catch (...) {
		}
	}
	// Three-valued, and the empty case matters: an absent header is a
	// pre-update server (assume zstd), whereas a present-but-empty one is the
	// server stating it speaks no compression. ParseAcceptList("") yields an
	// empty vector for both, so record the presence separately.
	caps.encodings_advertised = response.HasHeader(kSupportedEncodingsHeader);
	if (caps.encodings_advertised) {
		caps.supported_encodings = ParseAcceptList(response.GetHeaderValue(kSupportedEncodingsHeader));
	}

	auto max_age = ParseCacheControlMaxAge(response);
	if (max_age.count() > 0) {
		caps.cache_expires_at = std::chrono::steady_clock::now() + max_age;
	}

	return caps;
}

ServerCapabilities HttpDiscoverCapabilities(ClientContext &context, const std::string &base_url,
                                             const std::shared_ptr<IrohClientConfig> &iroh_config) {
	// Capability headers are emitted by middleware on every response. Probe
	// {base_url}/health: it is mandatory in every implementation and exempt
	// from auth, matching the Python reference client.
	auto url = NormalizeBaseUrl(base_url) + "/health";
	const auto accepted_max_response_bytes = GetAcceptedMaxResponseBytes(context);
	if (IsHttpiTransport(url)) {
		// HTTPI carries an explicit OPTIONS envelope over Iroh. It is safe to
		// retry because it never dispatches an application method.
#if defined(__EMSCRIPTEN__)
		RpcHttpResponse response;
		response.browser = httpi::BrowserRequest(
		    context, url, "OPTIONS",
		    {{kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes)}}, nullptr, 0,
		    GetHttpTimeoutSeconds(context), kMaxBufferedRepresentationBytes,
		    static_cast<size_t>(accepted_max_response_bytes));
		if (!response.Success()) {
			throw IOException("VGI HTTPI capability discovery failed (HTTP %d) [url: %s]",
			                  static_cast<int>(response.Status()), url);
		}
		return ParseCapabilityHeaders(response);
#else
		if (!iroh_config) {
			throw InternalException("vgi: native httpi:// capability discovery is missing its ATTACH configuration");
		}
		auto parsed = ParseHttpiUrl(url);
		RpcHttpResponse response;
		response.iroh = PerformIrohHttpRequest(
		    iroh_config, &context, "OPTIONS", parsed.path.empty() ? "/" : parsed.path,
		    {{kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes)}}, nullptr, 0,
		    static_cast<uint64_t>(accepted_max_response_bytes));
		if (!response.Success()) {
			throw IOException("VGI HTTPI capability discovery failed (HTTP %d) [url: %s]",
			                  static_cast<int>(response.Status()), url);
		}
		return ParseCapabilityHeaders(response);
#endif
	}

	// DuckDB's native HTTP abstraction has no OPTIONS request type. HEAD against
	// the mandatory health route is the native equivalent and reads the same
	// capability middleware headers before the first application POST.
	VGI_LOG(context, "http.capability_probe", {{"url", url}});

	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	ApplyHttpTimeout(context, *params);

	HTTPHeaders headers;
	headers.Insert(kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes));
	HeadRequestInfo head(url, headers, *params);
	auto response = http_util.Request(head);
	if (!response) {
		throw IOException("VGI HTTP capability discovery returned no response [url: %s]", url);
	}
	const auto status = static_cast<int>(response->status);
	if (status < 200 || status >= 300) {
		throw IOException("VGI HTTP capability discovery failed (HTTP %d) [url: %s]", status, url);
	}

	return ParseCapabilityHeaders(*response);
}

std::vector<UploadUrl> HttpRequestUploadUrls(ClientContext &context, const std::string &base_url, int count,
                                               const std::shared_ptr<CatalogAuth> &auth,
                                               const std::shared_ptr<IrohClientConfig> &iroh_config) {
	// Serialize Arrow batch {count: int64}
	auto count_field = arrow::field("count", arrow::int64());
	auto schema = arrow::schema({count_field});
	auto count_array_result = arrow::MakeArrayFromScalar(arrow::Int64Scalar(count), 1);
	if (!count_array_result.ok()) {
		throw IOException("Failed to create count array for upload URL request");
	}
	auto batch = arrow::RecordBatch::Make(schema, 1, {count_array_result.ValueUnsafe()});

	// POST to __upload_url__/init (HttpInvokeUnary normalizes base_url internally)
	auto result = HttpInvokeUnary(context, base_url, "__upload_url__/init", batch, auth, nullptr, nullptr,
	                              "", "", "", "", "", nullptr, nullptr, iroh_config);

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

void HttpPutBytes(ClientContext &context, const std::string &url, const std::vector<uint8_t> &data,
                  HttpEncoding encoding) {
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

	PutRequestInfo put(url, headers, *params, reinterpret_cast<const_data_ptr_t>(body_data),
	                   static_cast<idx_t>(body_size), content_type);
	auto response = http_util.Request(put);
	if (!response) {
		throw IOException("VGI upload returned no response (transport failure) [url: %s]", url);
	}

	if (!response->Success()) {
		throw IOException("VGI upload failed (HTTP %d) [url: %s]", static_cast<int>(response->status), url);
	}
}

std::vector<uint8_t> SerializePointerBatch(const std::shared_ptr<arrow::Schema> &schema,
                                           const std::string &location_url, const std::string &stream_state_token,
                                             const std::string &call_state_token) {
	// Build zero-row batch with empty arrays matching the schema
	std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
	for (int i = 0; i < schema->num_fields(); i++) {
		auto empty_result = arrow::MakeEmptyArray(schema->field(i)->type());
		if (!empty_result.ok()) {
			throw IOException("Failed to create empty array for pointer batch: %s", empty_result.status().ToString());
		}
		empty_arrays.push_back(empty_result.ValueUnsafe());
	}
	auto zero_batch = arrow::RecordBatch::Make(schema, 0, empty_arrays);

	// Build metadata: location key + optional stream_state / call_state.
	// Both tokens have to ride the pointer batch: the worker strips the
	// location and reads the request's metadata, so a token left off here is
	// simply absent from the request.
	auto metadata = arrow::KeyValueMetadata::Make({RPC_LOCATION_KEY}, {location_url});
	if (!stream_state_token.empty()) {
		metadata = metadata->Merge(*arrow::KeyValueMetadata::Make({RPC_STREAM_STATE_KEY}, {stream_state_token}));
	}
	if (!call_state_token.empty()) {
		metadata = metadata->Merge(*arrow::KeyValueMetadata::Make({RPC_CALL_STATE_KEY}, {call_state_token}));
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
