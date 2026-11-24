// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_engine_compat.hpp"
#include "vgi_settings_defaults.hpp"
#include "vgi_http_client.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <optional>

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/enums/http_status_code.hpp"

#include "vgi_cookie_jar.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_compression.hpp"
#include "vgi_http_params.hpp"
#include "vgi_http_request_stats.hpp"
#include "vgi_httpi.hpp"
#include "vgi_iroh_native.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_unary_retry.hpp"
#include "vgi_protocol_codec.hpp"
#include "vgi_sha256.hpp"
#include "vgi_transport.hpp"

#include "nanoarrow/nanoarrow.h"

namespace duckdb {
namespace vgi {

// Mirrors vgi_rpc/http/_common.py.
static constexpr const char *kSupportedEncodingsHeader = "VGI-Supported-Encodings";
static constexpr const char *kAcceptMaxResponseBytesHeader = "VGI-Accept-Max-Response-Bytes";
static constexpr const char *kAcceptMaxResponseBytesSupportHeader =
    "VGI-Accept-Max-Response-Bytes-Support";
static constexpr int64_t kMinAcceptedMaxResponseBytes = 64LL << 10;
static constexpr int64_t kMaxSafeInteger = 9007199254740991LL;
// Ceiling on a buffered (possibly still encoded) HTTP representation, separate
// from the decoded-response budget. HTTPI enforces it while streaming; DuckDB's
// native POST returns only the completed body, so there the check is post-read.
static constexpr size_t kMaxBufferedRepresentationBytes = 1024ULL * 1024ULL * 1024ULL;
static constexpr int64_t kDefaultAcceptedMaxResponseBytes = defaults::HTTP_ACCEPTED_MAX_RESPONSE_BYTES;

// Codecs we decode, in preference order, for the server's response. What the
// server accepts on request bodies comes back as VGI-Supported-Encodings.
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

// One of: a native http(s) HTTPResponse, a browser HTTPI response, or a native
// Iroh response. The latter two keep ordered duplicate headers and mark a raw
// (not browser-decoded) representation.
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

// Prefer X-VGI-Content-Encoding (generic proxies and browsers don't fold it),
// then Content-Encoding; NONE when absent or unknown.
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

static std::string NormalizeBaseUrl(const std::string &url) {
	if (!url.empty() && url.back() == '/') {
		return url.substr(0, url.size() - 1);
	}
	return url;
}

static uint64_t GetHttpTimeoutSeconds(ClientContext &context) {
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
		return static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
	}
	return defaults::HTTP_TIMEOUT_SECONDS;
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
	// The HTTPI bridge buffers a whole response in wasm32 memory; never
	// advertise more than it can hold.
	result = std::min<int64_t>(result, static_cast<int64_t>(httpi::kMaxBufferedBodyBytes));
#endif
	return result;
}

static void ApplyHttpTimeout(ClientContext &context, HTTPParams &params) {
	params.timeout = GetHttpTimeoutSeconds(context);
}

uint64_t GetStorageStallTimeoutSeconds(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("vgi_storage_stall_timeout_seconds", value) && !value.IsNull()) {
		return static_cast<uint64_t>(value.GetValue<int64_t>());
	}
	return static_cast<uint64_t>(defaults::STORAGE_STALL_TIMEOUT_SECONDS);
}

// Storage transfers (pre-signed PUTs and GETs) move bodies of up to the 1 GiB
// caps over links of any speed, so they never get a whole-request deadline.
// httpfs maps HTTPParams::timeout to a connect timeout plus a stall timeout:
// curl's CURLOPT_CONNECTTIMEOUT and CURLOPT_LOW_SPEED_TIME (under 1 KiB/s for
// that long) with CURLOPT_TIMEOUT 0, httplib's per-socket-operation connect,
// read and write timeouts. Both are what vgi-rpc's storage clients use: connect
// and stall 30 s (VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS on workers), at most 3
// attempts with 0.25 s x 2^n backoff (HTTPUtil adds the jitter and keeps 429 and
// 503 on their own throttle budget). Only connect errors, stalls, 408/429 and
// 5xx are retried, so a progressing transfer is never cut off and resent. A
// retry resends a PUT body whole: a pre-signed PUT cannot resume.
static void ApplyStorageTransferParams(ClientContext &context, HTTPParams &params) {
	params.timeout = GetStorageStallTimeoutSeconds(context);
	params.timeout_usec = 0;
	params.retries = MinValue<uint64_t>(params.retries, kStorageTransferMaxRetries);
	params.retry_wait_ms = kStorageTransferRetryWaitMs;
	params.retry_backoff = kStorageTransferRetryBackoff;
}

bool StorageAllowsPlainHttp(ClientContext &context) {
	Value value;
	return context.TryGetCurrentSetting("vgi_storage_allow_plain_http", value) && !value.IsNull() &&
	       value.GetValue<bool>();
}

// https (case-insensitive) or httpi: gates Secure cookies.
static bool UrlIsSecure(const std::string &url) {
	if (url.size() < 8) {
		return false;
	}
	const bool https = (url[0] == 'h' || url[0] == 'H') && (url[1] == 't' || url[1] == 'T') &&
	       (url[2] == 't' || url[2] == 'T') && (url[3] == 'p' || url[3] == 'P') &&
	       (url[4] == 's' || url[4] == 'S') && url[5] == ':';
	return https || IsHttpiTransport(url);
}

// Native responses: DuckDB's HTTPHeaders holds one value per name, and
// Set-Cookie must not be comma-folded (RFC 6265 §3; Expires contains commas).
// Depending on the backend, N Set-Cookie headers arrive as the last one, or
// joined by newlines, or joined by ", ". Only newline joins are split; comma
// splitting is deliberately not attempted.
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

// Whether a request armed with `cancellation` ended because of the interrupt.
// A backend that cannot abort mid-transfer fails the request or lets it
// finish; a failure while interrupted is still the interrupt. A response that
// completed normally is kept: the scan loop reports the interrupt at its next
// check, keeping the stream token in step.
static bool IsInterruptedRequest(ClientContext *cancellation, const HTTPResponse *response) {
	if (!cancellation || !cancellation->IsInterrupted()) {
		return false;
	}
#if VGI_ENGINE_HAS_HTTP_CANCELLATION
	return !response || response->IsCancelled() || response->HasRequestError();
#else
	return !response || response->HasRequestError();
#endif
}

template <class RESPONSE>
static ServerCapabilities ParseCapabilityHeaders(const RESPONSE &response);
template <class RESPONSE>
static bool HasCapabilityHeaders(const RESPONSE &response);
template <class RESPONSE>
[[noreturn]] static void ThrowNotAVgiServer(const RESPONSE &response, int status, const std::string &body,
                                            const std::string &url);

// One POST (plus at most one codec-renegotiation retry, gated by
// allow_codec_retry). Returns "" on 401; the caller handles auth.
// cached_http_params: see HttpInvokeUnary.
// TODO(duckdb/duckdb#22258): drop the cached-params requirement once fixed.
static std::string HttpPostArrowIpcInternal(
    ClientContext &context, const std::string &url, const std::vector<uint8_t> &body, const std::string &bearer_token,
    const std::shared_ptr<SessionCookieJar> &cookie_jar, std::unique_ptr<RpcHttpResponse> &out_response,
    const std::shared_ptr<HTTPParams> &cached_http_params = nullptr, HttpEncoding request_encoding = HttpEncoding::ZSTD,
    bool allow_codec_retry = true, duckdb::unique_ptr<HTTPClient> *client_holder = nullptr,
                                             ServerCapabilities *harvested_caps = nullptr,
                                             const std::shared_ptr<IrohClientConfig> &iroh_config = nullptr,
                                             ClientContext *cancellation = nullptr,
                                             const RpcErrorContext *error_context = nullptr) {
	bool single_attempt = false;
	if (error_context) {
		auto method = error_context->fields.find(error_key::kRpcMethod);
		single_attempt = method != error_context->fields.end() && RequiresSingleAttemptUnary(method->second);
	}
	const bool is_httpi = IsHttpiTransport(url);
	const uint64_t timeout_seconds = GetHttpTimeoutSeconds(context);
	const int64_t accepted_max_response_bytes = GetAcceptedMaxResponseBytes(context);
	int64_t effective_max_response_bytes = accepted_max_response_bytes;
	if (harvested_caps && harvested_caps->max_response_bytes >= kMinAcceptedMaxResponseBytes) {
		effective_max_response_bytes =
		    std::min(effective_max_response_bytes, harvested_caps->max_response_bytes);
	}

	// Tiny bodies (ticks, small unary envelopes) go uncompressed: compression
	// costs CPU and often grows them. Identity means NO Content-Encoding header
	// — the server 415s a literal "identity" token.
	static constexpr size_t kMinCompressBytes = 1024;
	if (request_encoding != HttpEncoding::NONE && body.size() < kMinCompressBytes) {
		request_encoding = HttpEncoding::NONE;
	}

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
	add_header(kClientProtocolVersionsHeader, ClientProtocolVersionsString());
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
		// BrowserPost never retries a transport failure: once Iroh accepted the
		// request bytes, whether the POST dispatched is ambiguous.
		out_response->browser = httpi::BrowserPost(context, url, httpi_headers, req_body_data,
		                                        req_body_size, timeout_seconds,
		                                        kMaxBufferedRepresentationBytes,
		                                        static_cast<size_t>(effective_max_response_bytes));
		response_body = out_response->browser->body;
#else
		if (!iroh_config) {
			throw InvalidInputException("vgi: native httpi:// request is missing its ATTACH configuration");
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
		// ATTACH primes provider parameters before BEGIN acquires the MetaTransaction
		// mutex. Reuse that snapshot without re-entering HTTPFS secret lookup.
		std::shared_ptr<HTTPParams> params = cached_http_params;
		if (!params) {
			auto owned = InitHttpParamsWithSecrets(context, url);
			owned->timeout = timeout_seconds;
			if (single_attempt) owned->retries = 0;
			params = std::shared_ptr<HTTPParams>(owned.release());
		}
#if VGI_ENGINE_HAS_HTTP_PARAMS_CLONE
		// The cached params are shared across queries: clone (polymorphically,
		// keeping provider fields) before setting this request's interrupt/retry
		// policy. Clear any interrupt so a rollback or cleanup still runs.
		std::shared_ptr<HTTPParams> request_params = params->Clone();
		request_params->interrupt_context.reset();
		if (cancellation) {
			request_params->interrupt_context = cancellation->shared_from_this();
		}
		if (single_attempt) {
			request_params->retries = 0;
		}
#else
		// No polymorphic Clone here, and copying the base would drop provider
		// TLS/credential fields, so the shared snapshot is used unmodified.
		// Single-attempt methods must arrive with the retries=0 snapshot from
		// GetOrInitHttpParams; anything else is a caller bug (building fresh
		// params would hit the duckdb#22258 deadlock).
		std::shared_ptr<HTTPParams> request_params = params;
		if (single_attempt && request_params->retries != 0) {
			throw InternalException("vgi: single-attempt HTTP RPC to %s was given HTTP parameters that retry (%llu); "
			                        "single-attempt methods must use the retries=0 snapshot from "
			                        "GetOrInitHttpParams(..., single_attempt=true)",
			                        url, static_cast<unsigned long long>(request_params->retries));
		}
#endif
		PostRequestInfo post(url, headers, *request_params, reinterpret_cast<const_data_ptr_t>(req_body_data),
		                     static_cast<idx_t>(req_body_size));
		post.try_request = true;
		if (client_holder) {
			out_response->native = http_util.Request(post, *client_holder);
		} else {
			out_response->native = http_util.Request(post);
		}
		// Before reading the response: an aborted transfer would otherwise surface
		// as a misleading "not a VGI server" or transport error.
		if (IsInterruptedRequest(cancellation, out_response->native.get())) {
			throw InterruptException();
		}
		if (!out_response->native) {
			throw IOException(BuildHttpExtraInfo(http_error::kTransportFailure, url),
			                  "VGI HTTP POST returned no response (transport failure) [url: %s]", url);
		}
		if (!post.buffer_out.empty()) {
			response_body.assign(post.buffer_out.data(), post.buffer_out.data() + post.buffer_out.size());
		}
	}
	if (response_body.size() > kMaxBufferedRepresentationBytes) {
		throw IOException(BuildHttpExtraInfo(http_error::kResponseTooLarge, url),
		                  "VGI HTTP response representation exceeds buffered transport limit (%llu > %llu) [url: %s]",
		                  static_cast<unsigned long long>(response_body.size()),
		                  static_cast<unsigned long long>(kMaxBufferedRepresentationBytes), url);
	}

	if (cookie_jar) {
		auto set_cookie_headers = CollectSetCookieHeaders(*out_response);
		if (!set_cookie_headers.empty()) {
			cookie_jar->UpdateFromSetCookie(set_cookie_headers, UrlIsSecure(url));
		}
	}

	// With try_request, HTTPUtil returns transport failures (refused, timeout,
	// reset) as a response carrying a request error, with no status or headers.
	if (out_response->HasRequestError()) {
		throw IOException(BuildHttpExtraInfo(http_error::kTransportFailure, url),
		                  "VGI HTTP request failed (transport error): %s [url: %s]", out_response->GetError(), url);
	}
	// Harvest capabilities from EVERY response, before status handling: a
	// failure caused by a codec the server can't decode still says which codecs
	// it can. No capability header at all means it did not come from a VGI server.
	if (!HasCapabilityHeaders(*out_response)) {
		ThrowNotAVgiServer(*out_response, static_cast<int>(out_response->Status()), out_response->FallbackBody(),
		                   url);
	}
	auto response_caps = ParseCapabilityHeaders(*out_response);
	if (response_caps.max_response_bytes >= kMinAcceptedMaxResponseBytes) {
		effective_max_response_bytes =
		    std::min(effective_max_response_bytes, response_caps.max_response_bytes);
	}
	if (harvested_caps) {
		// An omitted optional field keeps the snapshot's value; budget support
		// is not inherited because every response must prove it.
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
		if (!response_caps.protocol_ranges) {
			response_caps.protocol_ranges = harvested_caps->protocol_ranges;
		}
		*harvested_caps = response_caps;
	}
	if (!CapabilitiesSupport(response_caps, ProtocolFeature::ACCEPT_MAX_RESPONSE_BYTES)) {
		throw IOException(BuildHttpExtraInfo(http_error::kServerTooOld, url),
		                  "VGI HTTP server does not advertise %s: true; it predates the response size budget "
		                  "this client requires, so upgrade the worker [url: %s]",
		                  kAcceptMaxResponseBytesSupportHeader, url);
	}

	if (out_response->Status() == HTTPStatusCode::Unauthorized_401) {
		return "";
	}

	// Codec renegotiation: the request failed and the server's advertised
	// encodings exclude ours, so re-encode and retry once. Covers a gzip-only
	// server (415s zstd) and a compression-disabled one (EMPTY
	// VGI-Supported-Encodings; an absent header is a pre-update server, assumed
	// zstd). The latter has no decompressor and fails parsing with HTTP 500, so
	// this keys on the capability header, not the status. With the snapshot
	// cached per catalog this costs one rejected request per ATTACH.
	// Single-attempt methods retry only on 415 (rejected before dispatch): a 500
	// can follow a successful mutation and must never be replayed.
	if (allow_codec_retry && (!single_attempt || out_response->Status() == HTTPStatusCode::UnsupportedMediaType_415) &&
	    !out_response->Success() && response_caps.discovered &&
	    !ServerAcceptsRequestEncoding(response_caps, request_encoding)) {
		auto alternate = ChooseRequestEncoding(response_caps);
		if (alternate != request_encoding) {
			return HttpPostArrowIpcInternal(context, url, body, bearer_token, cookie_jar, out_response,
			                                cached_http_params, alternate,
			                                /*allow_codec_retry=*/false, client_holder, harvested_caps, iroh_config,
			                                cancellation, error_context);
		}
	}

	if (!out_response->Success()) {
		std::string error_body = response_body.empty() ? out_response->FallbackBody() : response_body;
		if (error_body.size() > kMaxBufferedRepresentationBytes) {
			throw IOException(BuildHttpExtraInfo(http_error::kResponseTooLarge, url),
		                  "VGI HTTP response representation exceeds buffered transport limit (%llu > %llu) [url: %s]",
			                  static_cast<unsigned long long>(error_body.size()),
			                  static_cast<unsigned long long>(kMaxBufferedRepresentationBytes), url);
		}
		// Decode first, or the IPC parse below would read compressed bytes.
		auto error_enc = ResolveResponseEncoding(*out_response);
		if (!error_body.empty() && error_enc != HttpEncoding::NONE) {
			error_body = Decompress(error_enc, error_body.data(), error_body.size(),
			                           static_cast<size_t>(effective_max_response_bytes));
		}
		if (error_body.size() > static_cast<uint64_t>(effective_max_response_bytes)) {
			throw IOException(BuildHttpExtraInfo(http_error::kResponseTooLarge, url),
		                  "VGI HTTP response exceeds max_response_bytes (%llu > %llu) [url: %s]",
			                  static_cast<unsigned long long>(error_body.size()),
			                  static_cast<unsigned long long>(effective_max_response_bytes), url);
		}
		std::string content_type =
		    out_response->HasHeader("Content-Type") ? out_response->GetHeaderValue("Content-Type") : std::string();
		bool is_arrow_ipc = content_type.find(ARROW_IPC_CONTENT_TYPE) != std::string::npos;

		// An Arrow IPC body either holds a VGI error batch (ReadUnaryResponseFromBuffer
		// throws the worker's message; rethrown) or a data batch, whose textual
		// columns become the preview.
		std::string body_preview;
		if (is_arrow_ipc && !error_body.empty()) {
			try {
				auto error_result = ReadUnaryResponseFromBuffer(reinterpret_cast<const uint8_t *>(error_body.data()),
				    error_body.size(), nullptr, url);
				if (error_result.batch) {
					const auto &batch = error_result.batch;
					for (int c = 0; c < batch->num_columns() && body_preview.size() < 1024; ++c) {
						auto col = batch->column(c);
						const auto &name = batch->column_name(c);
						// Only plain (large) string / binary columns are previewed;
						// a dictionary column reports its index type as storage.
						const int storage = col.storage_type();
						const bool textual = !col.is_dictionary() &&
						                     (storage == NANOARROW_TYPE_STRING || storage == NANOARROW_TYPE_LARGE_STRING ||
						                      storage == NANOARROW_TYPE_BINARY || storage == NANOARROW_TYPE_LARGE_BINARY);
						if (!textual) {
							continue;
						}
						for (int64_t i = 0; i < col.length() && body_preview.size() < 1024; ++i) {
							if (col.IsNull(i)) {
								continue;
							}
							std::string val = col.GetString(i);
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
		auto extra_info =
		    BuildHttpExtraInfo(http_error::kHttpError, url, static_cast<int>(out_response->Status()));
		if (!content_type.empty()) {
			extra_info["content_type"] = content_type;
		}
		throw IOException(extra_info, "VGI HTTP request failed (HTTP %d)%s%s: %s [url: %s]",
		                  static_cast<int>(out_response->Status()),
		                  content_type.empty() ? "" : " Content-Type=", content_type,
		    body_preview.empty() ? out_response->GetError() : body_preview, url);
	}

	bool browser_decoded = false;
#if defined(__EMSCRIPTEN__)
	// The browser decodes any standard Content-Encoding itself and may strip that
	// header while leaving the compressed Content-Length. Only the custom
	// X-VGI-Content-Encoding is left for us, so without it (and without a raw
	// representation) the body is final: skip the Content-Length check and our
	// decompress. Content-Encoding is not a reliable signal here.
	browser_decoded = !out_response->RawRepresentation() && !out_response->HasHeader("X-VGI-Content-Encoding");
#endif

	// A truncated body (proxy timeout, dropped connection the client swallowed)
	// could parse as a valid short result: enforce Content-Length when present.
	if (!browser_decoded && out_response->HasHeader("Content-Length")) {
		const auto content_length_str = out_response->GetHeaderValue("Content-Length");
		try {
			auto declared = std::stoull(content_length_str);
			if (declared != response_body.size()) {
				throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, url),
				                  "VGI HTTP response body size mismatch: Content-Length=%llu, got %llu bytes [url: %s]",
				    static_cast<unsigned long long>(declared),
				                  static_cast<unsigned long long>(response_body.size()), url);
			}
		} catch (const std::invalid_argument &) {
			// Malformed Content-Length: leave it to the Arrow parser.
		} catch (const std::out_of_range &) {
			// Same.
		}
	}

	auto resp_enc = browser_decoded ? HttpEncoding::NONE : ResolveResponseEncoding(*out_response);
	const size_t resp_wire_bytes = response_body.size();
	if (resp_enc != HttpEncoding::NONE && !response_body.empty()) {
		response_body = Decompress(resp_enc, response_body.data(), response_body.size(),
		                           static_cast<size_t>(effective_max_response_bytes));
	}
	const size_t resp_decoded_bytes = response_body.size();
	if (resp_decoded_bytes > static_cast<uint64_t>(effective_max_response_bytes)) {
		throw IOException(BuildHttpExtraInfo(http_error::kResponseTooLarge, url),
		                  "VGI HTTP response exceeds max_response_bytes (%llu > %llu) [url: %s]",
		                  static_cast<unsigned long long>(resp_decoded_bytes),
		                  static_cast<unsigned long long>(effective_max_response_bytes), url);
	}

	// Per-response wire/codec accounting. Gated: runs once per request on the
	// hot path, so skip building the fields when no log sink is listening.
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

	// Worker errors arrive as HTTP 200 + `X-VGI-RPC-Error: true`, so clients that
	// discard 5xx bodies still get the Arrow IPC error batch.
	if (out_response->HasHeader("X-VGI-RPC-Error") && out_response->GetHeaderValue("X-VGI-RPC-Error") == "true") {
		auto &error_body = response_body;
		if (!error_body.empty()) {
			// Walk EVERY concatenated IPC stream: a streaming RPC replies with a
			// header stream then a data stream, so an error raised on the first
			// batch lives in stream two. With an error_context, errors name the
			// worker/execution/function and the worker's log lines reach
			// duckdb_logs; without one (e.g. the cancel dispatcher, off the query
			// thread) nothing is logged.
			DispatchErrorStreamsFromBuffer(reinterpret_cast<const uint8_t *>(error_body.data()), error_body.size(),
			                               error_context ? &context : nullptr, url, error_context);
		}
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, url),
		                  "VGI HTTP RPC error [url: %s]", url);
	}

	return response_body;
}

std::string HttpPostArrowIpc(ClientContext &context, const std::string &url, const std::vector<uint8_t> &body,
                              const std::shared_ptr<CatalogAuth> &auth,
                              const std::shared_ptr<SessionCookieJar> &cookie_jar,
                              const std::shared_ptr<HTTPParams> &cached_http_params,
                             duckdb::unique_ptr<HTTPClient> *client_holder, ServerCapabilities *harvested_caps,
                             const std::shared_ptr<IrohClientConfig> &iroh_config,
                             ClientContext *cancellation, const RpcErrorContext *error_context) {
	std::string token;
	if (auth) {
		token = auth->GetTokenForRequest(context);
	}

	const HttpEncoding request_encoding = harvested_caps ? ChooseRequestEncoding(*harvested_caps) : HttpEncoding::ZSTD;

	std::unique_ptr<RpcHttpResponse> response;
	auto result = HttpPostArrowIpcInternal(context, url, body, token, cookie_jar, response, cached_http_params,
	                                       request_encoding, /*allow_codec_retry=*/true, client_holder, harvested_caps,
	                                       iroh_config, cancellation, error_context);

	if (response->Status() != HTTPStatusCode::Unauthorized_401) {
		return result;
	}

	if (!auth) {
		throw IOException(BuildHttpExtraInfo(http_error::kAuthRequired, url, 401),
		                  "VGI HTTP authentication required (HTTP 401) but no auth configured for this catalog "
		                  "[url: %s]. Use bearer_token or oauth_refresh_token in ATTACH options.",
		                  url);
	}

	// Auth that owns its 401 handling (Orchard): any challenge is ignored. One
	// refresh (deduplicated against the token this request was rejected with),
	// one retry; a second 401 fails.
	if (auth->HandlesRejectedTokens()) {
		VGI_STDERR_DEBUG("[VGI] http.401_received url=%s handler=rejected_token\n", url.c_str());
		auto retry_token = auth->HandleRejectedToken(token, context);
		result = HttpPostArrowIpcInternal(context, url, body, retry_token, cookie_jar, response, cached_http_params,
		                                  harvested_caps ? ChooseRequestEncoding(*harvested_caps) : request_encoding,
		                                  /*allow_codec_retry=*/true, client_holder, harvested_caps, iroh_config,
		                                  cancellation, error_context);
		if (response->Status() == HTTPStatusCode::Unauthorized_401) {
			throw IOException(BuildHttpExtraInfo(http_error::kAuthFailed, url, 401),
			                  "VGI HTTP authentication failed (HTTP 401) after refreshing the access token "
			                  "[url: %s]. The worker rejected the new token as well.",
			                  url);
		}
		return result;
	}

	// No WWW-Authenticate header and a valid challenge without OAuth
	// resource_metadata (RFC 6750 `Bearer realm="..."`, what a plain bearer
	// server sends) must be handled IDENTICALLY: neither is an OAuth challenge.
	const bool has_www_auth = response->HasHeader("WWW-Authenticate");
	std::optional<OAuthChallenge> challenge;
	if (has_www_auth) {
		challenge = ParseWWWAuthenticate(response->GetHeaderValue("WWW-Authenticate"));
	}

	if (!challenge.has_value()) {
		// With no credentials configured, the default handler is an empty
		// OAuthCatalogAuth whose HandleUnauthorized would fail confusingly on
		// FetchResourceMetadata(""); give an actionable error instead.
		if (!auth->IsExplicitlyConfigured()) {
			throw IOException(BuildHttpExtraInfo(http_error::kAuthRequired, url, 401),
			                  "VGI HTTP authentication failed (HTTP 401) [url: %s]. The server requires "
			    "authentication but advertised no OAuth challenge (%s). Pass bearer_token "
			    "in ATTACH options, or oauth_refresh_token if the server uses OAuth "
			    "without challenge advertising.",
			    url,
			    has_www_auth ? "its WWW-Authenticate header carries no resource_metadata"
			                 : "no WWW-Authenticate header");
		}
		// BearerTokenCatalogAuth throws token-rejected; a seeded
		// OAuthCatalogAuth attempts a refresh.
		OAuthChallenge empty_challenge;
		auth->HandleUnauthorized(empty_challenge, context);
		// HandleUnauthorized is expected to throw here.
		throw IOException(BuildHttpExtraInfo(http_error::kAuthFailed, url, 401),
		                  "VGI HTTP authentication failed (HTTP 401) [url: %s]", url);
	}

	VGI_STDERR_DEBUG("[VGI] http.401_received url=%s resource_metadata=%s\n", url.c_str(),
	                 challenge->resource_metadata_url.c_str());

	// Runs (or waits for) the OAuth flow.
	auto new_token = auth->HandleUnauthorized(*challenge, context);

	result = HttpPostArrowIpcInternal(context, url, body, new_token, cookie_jar, response, cached_http_params,
	                                  harvested_caps ? ChooseRequestEncoding(*harvested_caps) : request_encoding,
	                                  /*allow_codec_retry=*/true, client_holder, harvested_caps, iroh_config,
	                                  cancellation, error_context);
	if (response->Status() == HTTPStatusCode::Unauthorized_401) {
		throw IOException(BuildHttpExtraInfo(http_error::kAuthFailed, url, 401),
		                  "VGI HTTP authentication failed after auth flow (HTTP 401) [url: %s]. "
		                  "Response: %s",
		                  url, response->FallbackBody());
	}

	return result;
}

UnaryResponseResult HttpInvokeUnary(ClientContext &context, const std::string &worker_path,
                                    const std::string &method_name, const VgiBatchPtr &params,
                                     const std::shared_ptr<CatalogAuth> &auth,
                                     const std::shared_ptr<SessionCookieJar> &cookie_jar,
                                     const std::shared_ptr<HTTPParams> &cached_http_params,
                                    const std::string &execution_id_hex, const std::string &attach_opaque_data_digest,
                                    const std::string &transaction_opaque_data_digest, const std::string &conn_id_hex,
                                     const VgiProtocolId &protocol,
                                    duckdb::unique_ptr<HTTPClient> *client_holder, ServerCapabilities *caps,
                                    const std::shared_ptr<IrohClientConfig> &iroh_config,
                                    const std::unordered_map<std::string, std::string> *error_fields) {
	std::string base_url = NormalizeBaseUrl(worker_path);
	// The protocol segment projects the body's vgi_rpc.protocol key so an edge
	// device can route without an Arrow parser; the server checks they agree.
	// The upload-URL method is mounted at {base}/__upload_url__/init.
	std::string url = IsReservedRpcMethod(method_name)
	                      ? base_url + "/" + method_name + (method_name == UPLOAD_URL_METHOD ? "/init" : "")
	                      : base_url + "/" + std::string(protocol.name) + "/" + method_name;
	ServerCapabilities local_caps;
	auto *effective_caps = caps ? caps : &local_caps;
	if (!effective_caps->discovered ||
	    (effective_caps->cache_expires_at != std::chrono::steady_clock::time_point{} &&
	     std::chrono::steady_clock::now() >= effective_caps->cache_expires_at)) {
		*effective_caps = HttpDiscoverCapabilities(context, base_url, iroh_config, cached_http_params);
	}
	if (!CapabilitiesSupport(*effective_caps, ProtocolFeature::ACCEPT_MAX_RESPONSE_BYTES)) {
		throw IOException(BuildHttpExtraInfo(http_error::kServerTooOld, base_url),
		                  "VGI HTTP server does not advertise %s: true; it predates the response size budget "
		                  "this client requires, so upgrade the worker [url: %s]",
		                  kAcceptMaxResponseBytesSupportHeader, base_url);
	}

	// Gated: fires once per unary RPC (hot on catalog bursts / buffered sinks).
	const bool log_active = VgiInfoLogActive(context);
	if (log_active) {
		VGI_LOG(context, "http.invoke_unary", {{"url", url}, {"method", method_name}});
	}

	std::vector<uint8_t> body;
	if (params) {
		body = SerializeRpcRequest(method_name, params, protocol);
	} else {
		body = SerializeEmptyRpcRequest(method_name, protocol);
	}
	// A request over VGI-Max-Request-Bytes goes up through an upload URL
	// (never the upload-URL request itself).
	if (method_name != UPLOAD_URL_METHOD) {
		MaybeExternalizeRequestBody(context, base_url, body, *effective_caps, auth, iroh_config);
	}

	// Worker errors name the worker and this call; the URL is its own field.
	RpcErrorContext error_context;
	error_context.worker_path = base_url;
	error_context.execution_id_hex = execution_id_hex;
	error_context.attach_opaque_data_digest = attach_opaque_data_digest;
	error_context.conn_id_hex = conn_id_hex;
	if (error_fields) {
		error_context.fields = *error_fields;
	}
	error_context.fields[error_key::kUrl] = RedactCredentials(url);
	error_context.fields[error_key::kRpcMethod] = method_name;
	RecordHttpRequestBody(base_url, body.size());
	auto response_body =
	    HttpPostArrowIpc(context, url, body, auth, cookie_jar, cached_http_params, client_holder,
	                     effective_caps, iroh_config, /*cancellation=*/nullptr, &error_context);

	// Moved in: the string becomes the owning Arrow buffer (no copy).
	auto result = ReadUnaryResponseFromBuffer(std::move(response_body), &context, url, execution_id_hex,
	                                          attach_opaque_data_digest, transaction_opaque_data_digest, conn_id_hex);

	result = MaybeResolveExternalLocation(context, result, base_url, execution_id_hex, attach_opaque_data_digest,
	                                      transaction_opaque_data_digest, conn_id_hex);

	if (log_active) {
		VGI_LOG(context, "http.invoke_unary_result",
		        {{"url", url}, {"method", method_name}, {"has_batch", result.batch ? "true" : "false"}});
	}

	return result;
}

// External locations

// An engine HTTP error (connection refused, TLS, timeout) names the URL it
// requested in full. For a pre-signed URL that is the credential: rethrow with
// every copy of it replaced by its redacted form. Called only from a catch
// handler; an exception that does not mention the URL is rethrown unchanged.
[[noreturn]] static void RethrowRedactingUrl(const std::exception &e, const std::string &url,
                                             const std::string &shown) {
	ErrorData data(e);
	bool mentioned = data.RawMessage().find(url) != std::string::npos;
	for (const auto &entry : data.ExtraInfo()) {
		mentioned = mentioned || entry.second.find(url) != std::string::npos;
	}
	if (!mentioned || url == shown) {
		throw;
	}
	auto message = StringUtil::Replace(data.RawMessage(), url, shown);
	std::unordered_map<std::string, std::string> extra_info;
	for (const auto &entry : data.ExtraInfo()) {
		if (entry.first != "stack_trace_pointers") {
			extra_info[entry.first] = StringUtil::Replace(entry.second, url, shown);
		}
	}
	if (data.Type() == ExceptionType::IO || data.Type() == ExceptionType::HTTP) {
		throw IOException(extra_info, message);
	}
	throw Exception(extra_info, data.Type(), message);
}

std::string HttpGetBytes(ClientContext &context, const std::string &url, ClientContext *cancellation,
                         HttpSecrets secrets, uint64_t max_bytes) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	HTTPHeaders headers;
	std::unique_ptr<HTTPParams> params;
	if (secrets == HttpSecrets::LOOKUP) {
		params = InitHttpParamsWithSecrets(context, url);
	} else {
		// May run inside VgiTransaction::Start (e.g. catalog_contents
		// revalidation), where a secret lookup deadlocks; see vgi_http_params.hpp.
		params = InitSecretFreeHttpParams(context);
		headers.Insert("User-Agent", SecretFreeUserAgent(context));
	}
	ApplyStorageTransferParams(context, *params);

	headers.Insert("X-VGI-Accept-Encoding", ClientAcceptEncoding());
	// No auth headers: pre-signed URLs reject an extra Authorization header.

	std::string body;
	// max_bytes bounds the transferred body: a declared Content-Length over it
	// stops before any body is read, and a body that grows past it (chunked, or
	// a lying length) stops at the limit.
	bool over_limit = false;
	// Called once per attempt, before its body: a retry after a stall starts
	// the body over instead of appending to the stalled attempt's prefix.
	auto response_handler = [&body, &over_limit, max_bytes](const HTTPResponse &response) {
		body.clear();
		over_limit = false;
		if (max_bytes > 0 && response.HasHeader("Content-Length")) {
			try {
				if (std::stoull(response.GetHeaderValue("Content-Length")) > max_bytes) {
					over_limit = true;
					return false;
				}
			} catch (...) {
			}
		}
		return true;
	};
	auto content_handler = [&body, &over_limit, max_bytes](const_data_ptr_t data, idx_t data_length) {
		if (max_bytes > 0 && body.size() + data_length > max_bytes) {
			over_limit = true;
			return false;
		}
		body.append(reinterpret_cast<const char *>(data), data_length);
		return true;
	};
	// Callers fetch pre-signed URLs: never report the query.
	const auto shown = RedactPresignedUrl(url);
	auto throw_too_large = [&]() {
		throw IOException(BuildHttpExtraInfo(http_error::kResponseTooLarge, shown),
		                  "VGI external location exceeds vgi_external_location_max_bytes (%llu) [url: %s]",
		                  static_cast<unsigned long long>(max_bytes), shown);
	};

	GetRequestInfo get(url, headers, *params, response_handler, content_handler);
#if VGI_ENGINE_HAS_HTTP_CANCELLATION
	params->interrupt_context.reset();
	if (cancellation) {
		params->interrupt_context = cancellation->shared_from_this();
	}
#endif
	unique_ptr<HTTPResponse> response;
	try {
		response = http_util.Request(get);
	} catch (std::exception &e) {
		RethrowRedactingUrl(e, url, shown);
	}
	if (IsInterruptedRequest(cancellation, response.get())) {
		throw InterruptException();
	}
	if (over_limit) {
		throw_too_large();
	}
	if (!response) {
		throw IOException(BuildHttpExtraInfo(http_error::kTransportFailure, shown),
		                  "VGI external location fetch returned no response (transport failure) [url: %s]", shown);
	}

	if (!response->Success()) {
		throw IOException(BuildHttpExtraInfo(http_error::kHttpError, shown, static_cast<int>(response->status)),
		                  "VGI external location fetch failed (HTTP %d) [url: %s]", static_cast<int>(response->status),
		                  shown);
	}

	// As for POSTs: in the browser only X-VGI-Content-Encoding is still encoded.
	auto get_enc = ResolveResponseEncoding(*response);
#if defined(__EMSCRIPTEN__)
	if (!response->HasHeader("X-VGI-Content-Encoding")) {
		get_enc = HttpEncoding::NONE;
	}
#endif
	if (get_enc != HttpEncoding::NONE && !body.empty()) {
		// The codec's own error names the cap when the decoded size exceeds it.
		return max_bytes == 0 ? Decompress(get_enc, body.data(), body.size())
		                      : Decompress(get_enc, body.data(), body.size(), static_cast<size_t>(max_bytes));
	}

	return body;
}

uint64_t GetExternalLocationMaxBytes(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("vgi_external_location_max_bytes", value) && !value.IsNull()) {
		return value.GetValue<uint64_t>();
	}
	return defaults::EXTERNAL_LOCATION_MAX_BYTES;
}

bool ExternalLocationUrlAllowed(const std::string &url, bool allow_plain_http) {
	if (url.size() >= 8 && StringUtil::Lower(url.substr(0, 8)) == "https://") {
		return true;
	}
	if (allow_plain_http && url.size() > 7 && StringUtil::Lower(url.substr(0, 7)) == "http://") {
		return true;
	}
	if (!IsLoopbackHttpUrl(url)) {
		return false;
	}
#if defined(__EMSCRIPTEN__)
	// No process environment in the browser, which also blocks mixed content
	// on its own; a page served from localhost may use a loopback store.
	return true;
#else
	const char *flag = std::getenv(kAllowLoopbackExternalLocationsEnv);
	return flag && std::string(flag) == "1";
#endif
}

bool ExternalLocationUrlAllowed(ClientContext &context, const std::string &url) {
	return ExternalLocationUrlAllowed(url, StorageAllowsPlainHttp(context));
}

// Every failure while resolving a pointer is a storage failure: tag it
// `delivery=external_location` so callers can tell a damaged or missing object
// from the worker's own answer (catalog_contents must not fall back on one).
// Called only from a catch handler.
[[noreturn]] static void RethrowAsExternalLocationFailure(const std::exception &e) {
	ErrorData data(e);
	if (data.Type() == ExceptionType::INTERRUPT) {
		throw;
	}
	auto it = data.ExtraInfo().find(error_key::kDelivery);
	if (it != data.ExtraInfo().end()) {
		throw;
	}
	std::unordered_map<std::string, std::string> extra_info;
	for (const auto &entry : data.ExtraInfo()) {
		if (entry.first != "stack_trace_pointers") {
			extra_info[entry.first] = entry.second;
		}
	}
	extra_info[error_key::kDelivery] = "external_location";
	if (data.Type() == ExceptionType::IO || data.Type() == ExceptionType::HTTP) {
		throw IOException(extra_info, data.RawMessage());
	}
	if (data.Type() == ExceptionType::INVALID_INPUT) {
		throw InvalidInputException(extra_info, data.RawMessage());
	}
	throw Exception(extra_info, data.Type(), data.RawMessage());
}

static UnaryResponseResult ResolveExternalLocationImpl(ClientContext &context, const std::string &location_url,
                                                       const std::string &worker_path,
                                                       const std::string &execution_id_hex,
                                                       const std::string &attach_opaque_data_digest,
                                                       const VgiMetadataPtr &pointer_metadata,
                                                       ClientContext *cancellation, const std::string &conn_id_hex,
                                                       const std::string &transaction_opaque_data_digest) {
	// The URL is pre-signed: its query is the credential, so every message,
	// field and log line names only scheme://host/path.
	const auto shown_url = RedactPresignedUrl(location_url);
	// The fetched URL is a pre-signed object, not the worker; name both.
	auto location_error = [&](const char *error_subtype) {
		auto extra_info = BuildHttpExtraInfo(error_subtype, shown_url);
		auto worker_info = BuildExtraInfo(worker_path, -1, execution_id_hex);
		extra_info.insert(worker_info.begin(), worker_info.end());
		extra_info[error_key::kDelivery] = "external_location";
		return extra_info;
	};
	if (IsHttpiTransport(location_url)) {
		throw IOException(location_error(http_error::kUnsupported),
		                  "VGI external locations over httpi:// are unsupported; the worker must return an "
		                  "https:// pre-signed URL [url: %s]",
		                  shown_url);
	}
	if (!ExternalLocationUrlAllowed(context, location_url)) {
		throw IOException(location_error(http_error::kUnsupported),
		                  "VGI external location refused: the URL must be https:// (plain http is accepted only with "
		                  "SET vgi_storage_allow_plain_http = true, or for a loopback host with %s=1) [url: %s]",
		                  kAllowLoopbackExternalLocationsEnv, shown_url);
	}
	// Protocol 3.4: the pointer's SHA-256 is mandatory. Without it the client
	// cannot tell a damaged object from a good one, so refuse before fetching.
	int64_t sha_idx = pointer_metadata ? pointer_metadata->FindKey(RPC_LOCATION_SHA256_KEY) : -1;
	if (sha_idx < 0) {
		throw IOException(location_error(http_error::kProtocolViolation),
		                  "VGI external location pointer is missing vgi_rpc.location.sha256, which protocol %s "
		                  "requires; the worker must send the SHA-256 of the stored bytes [url: %s]",
		                  std::string(generated::VGI_PROTOCOL_VERSION), shown_url);
	}
	const auto expected_hex = pointer_metadata->value(sha_idx);

	auto body = HttpGetBytes(context, location_url, cancellation, HttpSecrets::NONE,
	                         GetExternalLocationMaxBytes(context));

	if (body.empty()) {
		throw IOException(location_error(http_error::kProtocolViolation),
		                  "VGI external location returned empty response [url: %s]", shown_url);
	}

	// VgiSha256Hex, not mbedtls: side modules on WASM can't call it directly.
	auto actual_hex = VgiSha256Hex(body);
	if (actual_hex != expected_hex) {
		throw IOException(location_error(http_error::kChecksumMismatch).ExpectedActual(expected_hex, actual_hex),
		                  "VGI external location SHA-256 checksum mismatch [url: %s]: "
		                  "expected %s, got %s",
		                  shown_url, expected_hex, actual_hex);
	}

	// The stream may hold log batches followed by the data batch. Adopt the
	// body as the owning buffer (large-batch path; avoid a copy).
	auto owned = VgiBuffer::FromString(std::move(body));

	// Errors name the fetched URL; log records keep the worker that pointed at it.
	WorkerStreamOptions opts;
	opts.context = &context;
	opts.worker = shown_url;
	opts.log_worker = worker_path;
	opts.http_messages = true;
	opts.lenient = true;
	opts.noun = "external location";
	opts.execution_id_hex = execution_id_hex;
	opts.attach_opaque_data_digest = attach_opaque_data_digest;
	opts.transaction_opaque_data_digest = transaction_opaque_data_digest;
	opts.conn_id_hex = conn_id_hex;
	auto result = ReadWorkerUnaryStream(std::make_shared<VgiBufferReader>(std::move(owned)), opts);
	if (result.batch && ClassifyBatch(result.batch, result.metadata) == RpcBatchType::EXTERNAL_LOCATION) {
		throw IOException(location_error(http_error::kProtocolViolation),
		                  "VGI external location redirect loop: resolved batch from %s "
		                  "contains another vgi_rpc.location",
		                  shown_url);
	}

	if (!result.batch) {
		throw IOException(location_error(http_error::kProtocolViolation),
		                  "VGI external location contained no data batch [url: %s]", shown_url);
	}

	return result;
}

UnaryResponseResult ResolveExternalLocation(ClientContext &context, const std::string &location_url,
                                            const std::string &worker_path, const std::string &execution_id_hex,
                                            const std::string &attach_opaque_data_digest,
                                            const VgiMetadataPtr &pointer_metadata, ClientContext *cancellation,
                                            const std::string &conn_id_hex,
                                            const std::string &transaction_opaque_data_digest) {
	try {
		return ResolveExternalLocationImpl(context, location_url, worker_path, execution_id_hex,
		                                   attach_opaque_data_digest, pointer_metadata, cancellation, conn_id_hex,
		                                   transaction_opaque_data_digest);
	} catch (std::exception &e) {
		RethrowAsExternalLocationFailure(e);
	}
}

UnaryResponseResult MaybeResolveExternalLocation(ClientContext &context, UnaryResponseResult &result,
                                                   const std::string &worker_path, const std::string &execution_id_hex,
                                                   const std::string &attach_opaque_data_digest,
                                                   const std::string &transaction_opaque_data_digest,
                                                   const std::string &conn_id_hex) {
	if (!result.metadata) {
		return std::move(result);
	}
	int64_t loc_idx = result.metadata->FindKey(RPC_LOCATION_KEY);
	if (loc_idx < 0) {
		return std::move(result);
	}

	auto location_url = result.metadata->value(loc_idx);
	return ResolveExternalLocation(context, location_url, worker_path, execution_id_hex, attach_opaque_data_digest,
	                               result.metadata, nullptr, conn_id_hex, transaction_opaque_data_digest);
}

// Capability discovery and upload URLs

// "Cache-Control: max-age=N" in seconds; 0 when absent or unparseable.
template <class RESPONSE>
static std::chrono::seconds ParseCacheControlMaxAge(const RESPONSE &response) {
	const char *header_names[] = {"Cache-Control", "cache-control"};
	for (const char *name : header_names) {
		if (!response.HasHeader(name)) {
			continue;
		}
		auto value = response.GetHeaderValue(name);
		size_t pos = 0;
		while (pos < value.size()) {
			size_t comma = value.find(',', pos);
			std::string token = value.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
			size_t a = token.find_first_not_of(" \t");
			size_t b = token.find_last_not_of(" \t");
			if (a != std::string::npos) {
				token = token.substr(a, b - a + 1);
			} else {
				token.clear();
			}
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

// An intermediary's error page carries none of these; caching it as a
// discovered snapshot would look like a server that advertises nothing.
template <class RESPONSE>
static bool HasCapabilityHeaders(const RESPONSE &response) {
	return response.HasHeader(kAcceptMaxResponseBytesSupportHeader) ||
	       response.HasHeader(kSupportedEncodingsHeader) || response.HasHeader("VGI-Max-Request-Bytes") ||
	       response.HasHeader("VGI-Upload-URL-Support") || response.HasHeader("VGI-Max-Upload-Bytes") ||
	       response.HasHeader(kProtocolVersionsHeader);
}

// Printable, single-line, bounded preview of a response body for an error
// message. Bodies here are usually an intermediary's HTML or plain-text page.
static std::string BodyPreviewForError(const std::string &body) {
	static constexpr size_t kMaxPreview = 200;
	std::string out;
	bool pending_space = false;
	for (const char c : body) {
		const auto byte = static_cast<unsigned char>(c);
		if (byte == ' ' || byte == '\t' || byte == '\n' || byte == '\r') {
			pending_space = !out.empty();
			continue;
		}
		if (pending_space) {
			out += ' ';
			pending_space = false;
		}
		out += (byte < 0x20 || byte == 0x7f) ? '?' : c;
		if (out.size() >= kMaxPreview) {
			out += "...";
			break;
		}
	}
	return out;
}

// For a response with no VGI capability header (a proxy error page, a load
// balancer with no backend, the wrong URL): say so and show the response,
// rather than naming a protocol header the user has never heard of.
template <class RESPONSE>
[[noreturn]] static void ThrowNotAVgiServer(const RESPONSE &response, int status, const std::string &body,
                                            const std::string &url) {
	auto extra_info = BuildHttpExtraInfo(http_error::kNotVgiServer, url, status);
	std::string detail = "HTTP " + std::to_string(status);
	if (response.HasHeader("Content-Type")) {
		const auto content_type = response.GetHeaderValue("Content-Type");
		if (!content_type.empty()) {
			detail += ", Content-Type: " + content_type;
			extra_info["content_type"] = content_type;
		}
	}
	const auto preview = BodyPreviewForError(body);
	if (!preview.empty()) {
		detail += ", body: \"" + preview + "\"";
		extra_info["body_preview"] = preview;
	}
	throw IOException(extra_info, "VGI HTTP endpoint did not respond as a VGI server (%s). Check that the worker is running and "
	                  "reachable at this URL [url: %s]",
	                  detail, url);
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
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, std::string()), "VGI-Max-Response-Bytes must match [1-9][0-9]*");
	}
	int64_t parsed = 0;
	for (const auto byte : value) {
		if (byte < '0' || byte > '9') {
			throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, std::string()), "VGI-Max-Response-Bytes must match [1-9][0-9]*");
		}
		const auto digit = static_cast<int64_t>(byte - '0');
		if (parsed > (kMaxSafeInteger - digit) / 10) {
			throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, std::string()), "VGI-Max-Response-Bytes must be between 65536 and 9007199254740991");
		}
		parsed = parsed * 10 + digit;
	}
	if (parsed < kMinAcceptedMaxResponseBytes || parsed > kMaxSafeInteger) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, std::string()), "VGI-Max-Response-Bytes must be between 65536 and 9007199254740991");
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
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, std::string()), "VGI-Max-Response-Bytes must occur exactly once");
	}
	return ParseCanonicalResponseMaximum(values[0]);
}

// VGI-Upload-URL-Support: "true" is v1 (the retired count shape), a positive
// integer is that version, anything else means no upload support.
static int ParseUploadUrlVersion(const std::string &value) {
	if (value == "true") {
		return 1;
	}
	if (value.empty() || value.size() > 4 || value.front() == '0') {
		return 0;
	}
	int version = 0;
	for (const auto c : value) {
		if (c < '0' || c > '9') {
			return 0;
		}
		version = version * 10 + (c - '0');
	}
	return version;
}

// VGI-Arguments-Ref-Bytes: a positive decimal integer, no sign, no leading
// zero, at most 19 digits (docs/large-values-3.4.md §5). Anything else is
// treated as absent (-1).
int64_t ParseArgumentsRefBytes(const std::string &value) {
	if (value.empty() || value.size() > 19 || value.front() == '0') {
		return -1;
	}
	uint64_t parsed = 0;
	for (const auto c : value) {
		if (c < '0' || c > '9') {
			return -1;
		}
		parsed = parsed * 10 + static_cast<uint64_t>(c - '0');
	}
	if (parsed > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
		return -1;
	}
	return static_cast<int64_t>(parsed);
}

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
		caps.upload_url_version = ParseUploadUrlVersion(response.GetHeaderValue("VGI-Upload-URL-Support"));
	}
	if (response.HasHeader("VGI-Max-Upload-Bytes")) {
		try {
			caps.max_upload_bytes = std::stoll(response.GetHeaderValue("VGI-Max-Upload-Bytes"));
		} catch (...) {
		}
	}
	if (response.HasHeader(kArgumentsRefBytesHeader)) {
		caps.arguments_ref_bytes = ParseArgumentsRefBytes(response.GetHeaderValue(kArgumentsRefBytesHeader));
	}
	// Absent = pre-update server (assume zstd); present-but-empty = speaks no
	// compression. Both parse to an empty list, so record presence separately.
	caps.encodings_advertised = response.HasHeader(kSupportedEncodingsHeader);
	if (caps.encodings_advertised) {
		caps.supported_encodings = ParseAcceptList(response.GetHeaderValue(kSupportedEncodingsHeader));
	}

	auto max_age = ParseCacheControlMaxAge(response);
	if (max_age.count() > 0) {
		caps.cache_expires_at = std::chrono::steady_clock::now() + max_age;
	}
	// Absent (a server released before negotiation) or unparseable: unknown.
	if (response.HasHeader(kProtocolVersionsHeader)) {
		caps.protocol_ranges = ParseProtocolRanges(response.GetHeaderValue(kProtocolVersionsHeader));
	}

	return caps;
}

ServerCapabilities HttpDiscoverCapabilities(ClientContext &context, const std::string &base_url,
                                             const std::shared_ptr<IrohClientConfig> &iroh_config,
                                             const std::shared_ptr<HTTPParams> &cached_http_params) {
	auto url = NormalizeBaseUrl(base_url) + "/health";
	const auto accepted_max_response_bytes = GetAcceptedMaxResponseBytes(context);
	if (IsHttpiTransport(url)) {
		// OPTIONS never dispatches an application method, so it is safe to retry.
#if defined(__EMSCRIPTEN__)
		RpcHttpResponse response;
		response.browser = httpi::BrowserRequest(
		    context, url, "OPTIONS",
		    {{kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes)},
		     {kClientProtocolVersionsHeader, ClientProtocolVersionsString()}},
		    nullptr, 0,
		    GetHttpTimeoutSeconds(context), kMaxBufferedRepresentationBytes,
		    static_cast<size_t>(accepted_max_response_bytes));
		if (!HasCapabilityHeaders(response)) {
			ThrowNotAVgiServer(response, static_cast<int>(response.Status()), response.FallbackBody(), url);
		}
		if (!response.Success()) {
			throw IOException(BuildHttpExtraInfo(http_error::kHttpError, url, static_cast<int>(response.Status())),
			                  "VGI HTTPI capability discovery failed (HTTP %d) [url: %s]",
			                  static_cast<int>(response.Status()), url);
		}
		return ParseCapabilityHeaders(response);
#else
		if (!iroh_config) {
			throw InvalidInputException("vgi: native httpi:// capability discovery is missing its ATTACH configuration");
		}
		auto parsed = ParseHttpiUrl(url);
		RpcHttpResponse response;
		response.iroh = PerformIrohHttpRequest(
		    iroh_config, &context, "OPTIONS", parsed.path.empty() ? "/" : parsed.path,
		    {{kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes)},
		     {kClientProtocolVersionsHeader, ClientProtocolVersionsString()}},
		    nullptr, 0,
		    static_cast<uint64_t>(accepted_max_response_bytes));
		if (!HasCapabilityHeaders(response)) {
			ThrowNotAVgiServer(response, static_cast<int>(response.Status()), response.FallbackBody(), url);
		}
		if (!response.Success()) {
			throw IOException(BuildHttpExtraInfo(http_error::kHttpError, url, static_cast<int>(response.Status())),
			                  "VGI HTTPI capability discovery failed (HTTP %d) [url: %s]",
			                  static_cast<int>(response.Status()), url);
		}
		return ParseCapabilityHeaders(response);
#endif
	}

	VGI_LOG(context, "http.capability_probe", {{"url", url}});

	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	// Cached params already carry the timeout (see header for why to pass them).
	std::unique_ptr<HTTPParams> owned_params;
	HTTPParams *params = cached_http_params.get();
	if (!params) {
		owned_params = InitHttpParamsWithSecrets(context, url);
		ApplyHttpTimeout(context, *owned_params);
		params = owned_params.get();
	}

	HTTPHeaders headers;
	headers.Insert(kAcceptMaxResponseBytesHeader, std::to_string(accepted_max_response_bytes));
	headers.Insert(kClientProtocolVersionsHeader, ClientProtocolVersionsString());
	HeadRequestInfo head(url, headers, *params);
	// Report transport failures ourselves, with structured extra_info.
	head.try_request = true;
	auto response = http_util.Request(head);
	if (!response) {
		throw IOException(BuildHttpExtraInfo(http_error::kTransportFailure, url),
		                  "VGI HTTP capability discovery returned no response [url: %s]", url);
	}
	if (response->HasRequestError()) {
		throw IOException(BuildHttpExtraInfo(http_error::kTransportFailure, url),
		                  "VGI HTTP endpoint is unreachable: %s [url: %s]", response->GetError(), url);
	}
	const auto status = static_cast<int>(response->status);
	if (!HasCapabilityHeaders(*response)) {
		// HEAD carries no body; status and Content-Type are all there is to show.
		ThrowNotAVgiServer(*response, status, std::string(), url);
	}
	if (status < 200 || status >= 300) {
		throw IOException(BuildHttpExtraInfo(http_error::kHttpError, url, status),
		                  "VGI HTTP capability discovery failed (HTTP %d) [url: %s]", status, url);
	}

	return ParseCapabilityHeaders(*response);
}

std::vector<UploadUrl> HttpRequestUploadUrls(ClientContext &context, const std::string &base_url,
                                               const std::vector<UploadRequest> &uploads,
                                               const std::shared_ptr<CatalogAuth> &auth,
                                               const std::shared_ptr<IrohClientConfig> &iroh_config) {
	// Upload URLs v2: uploads list<struct<size: int64 not null, sha256: utf8 not null>> not null.
	static const VgiSchemaPtr schema = MakeSchema({type::List(type::Struct({type::Int64().Named("size", false),
	                                                                         type::Utf8().Named("sha256", false)}))
	                                                   .Named("uploads", false)});
	VgiBatchBuilder builder(schema);
	ArrowArray *list = builder.column(0);
	ArrowArray *item = list->children[0];
	for (const auto &upload : uploads) {
		append::Int(item->children[0], upload.size);
		append::String(item->children[1], upload.sha256_hex);
		append::FinishElement(item);
	}
	append::FinishElement(list);
	builder.FinishRow();
	auto batch = builder.Finish();

	auto result = HttpInvokeUnary(context, base_url, UPLOAD_URL_METHOD, batch, auth, nullptr, nullptr,
	                              "", "", "", "", VGI_MAIN_PROTOCOL, nullptr, nullptr, iroh_config);

	if (!result.batch || result.batch->num_rows() == 0) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI server returned no upload URLs [url: %s]", base_url);
	}
	if (result.batch->num_rows() != static_cast<int64_t>(uploads.size())) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI server returned %lld upload URLs for %llu requested [url: %s]",
		                  static_cast<long long>(result.batch->num_rows()),
		                  static_cast<unsigned long long>(uploads.size()), base_url);
	}

	// Response columns: upload_url, download_url, expires_at (unused), upload_headers.
	auto upload_col = result.batch->GetColumnByName("upload_url");
	auto download_col = result.batch->GetColumnByName("download_url");
	auto headers_col = result.batch->GetColumnByName("upload_headers");

	if (!upload_col || !download_col || !headers_col) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI upload URL response missing required columns (upload_url, download_url, "
		                  "upload_headers) [url: %s]",
		                  base_url);
	}
	if (headers_col.storage_type() != NANOARROW_TYPE_MAP || headers_col.num_children() != 1 ||
	    headers_col.child(0).num_children() != 2) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI upload URL response column upload_headers must be map<utf8, utf8>, got %s [url: %s]",
		                  headers_col.type_string(), base_url);
	}
	auto header_keys = headers_col.child(0).child(0);
	auto header_values = headers_col.child(0).child(1);
	// Protocol 3.4 grant columns, by name and optional (STATE_REF_V1.md §10.2):
	// a client that does not use argument references ignores them.
	auto object_id_col = result.batch->GetColumnByName("object_id");
	auto ref_col = result.batch->GetColumnByName("ref");
	if (ref_col && ref_col.storage_type() != NANOARROW_TYPE_BINARY &&
	    ref_col.storage_type() != NANOARROW_TYPE_LARGE_BINARY) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI upload URL response column ref must be binary, got %s [url: %s]", ref_col.type_string(),
		                  base_url);
	}
	if (object_id_col && object_id_col.storage_type() != NANOARROW_TYPE_STRING &&
	    object_id_col.storage_type() != NANOARROW_TYPE_LARGE_STRING) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI upload URL response column object_id must be utf8, got %s [url: %s]",
		                  object_id_col.type_string(), base_url);
	}

	std::vector<UploadUrl> urls;
	for (int64_t i = 0; i < result.batch->num_rows(); i++) {
		if (upload_col.IsNull(i) || download_col.IsNull(i) || headers_col.IsNull(i)) {
			throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
			                  "VGI upload URL response row %lld has a null upload_url, download_url or "
			                  "upload_headers [url: %s]",
			                  static_cast<long long>(i), base_url);
		}
		UploadUrl url {upload_col.GetString(i), download_col.GetString(i), {}};
		if (object_id_col && !object_id_col.IsNull(i)) {
			url.object_id = object_id_col.GetString(i);
		}
		if (ref_col && !ref_col.IsNull(i)) {
			const auto ref = ref_col.GetView(i);
			url.ref.assign(ref.begin(), ref.end());
			url.has_ref = true;
		}
		int64_t begin = 0;
		int64_t end = 0;
		headers_col.ListRange(i, begin, end);
		for (int64_t h = begin; h < end; h++) {
			if (header_values.IsNull(h)) {
				throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
				                  "VGI upload URL response has a null upload_headers value [url: %s]", base_url);
			}
			auto name = header_keys.GetString(h);
			if (StringUtil::CIEquals(name, "Content-Length")) {
				// The client sends Content-Length itself, exactly `size`.
				continue;
			}
			url.upload_headers.emplace_back(std::move(name), header_values.GetString(h));
		}
		urls.push_back(std::move(url));
	}
	return urls;
}

// The S3-style `<Code>` of a storage error body (BadDigest, IncompleteBody,
// SignatureDoesNotMatch, ...), or "" when there is none. Only the code is ever
// reported: the rest of such a body can echo the request (canonical request,
// string to sign), which is too close to the credential to print.
static std::string StorageErrorCode(const std::string &body) {
	const auto open = body.find("<Code>");
	if (open == std::string::npos) {
		return std::string();
	}
	const auto start = open + 6;
	const auto close = body.find("</Code>", start);
	if (close == std::string::npos || close == start || close - start > 64) {
		return std::string();
	}
	auto code = body.substr(start, close - start);
	for (const auto c : code) {
		if (!std::isalnum(static_cast<unsigned char>(c)) && c != '.' && c != '_' && c != '-') {
			return std::string();
		}
	}
	return code;
}

void HttpPutBytes(ClientContext &context, const std::string &url, const std::vector<uint8_t> &data,
                  HttpEncoding encoding, const std::vector<std::pair<std::string, std::string>> &extra_headers) {
	HttpPutBytes(context, url, data.data(), data.size(), encoding, extra_headers);
}

void HttpPutBytes(ClientContext &context, const std::string &url, const uint8_t *data, size_t size,
                  HttpEncoding encoding, const std::vector<std::pair<std::string, std::string>> &extra_headers) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	// A pre-signed upload URL: no secrets (see vgi_http_params.hpp).
	auto params = InitSecretFreeHttpParams(context);
	ApplyStorageTransferParams(context, *params);

	HTTPHeaders headers;
	headers.Insert("User-Agent", SecretFreeUserAgent(context));
	std::string content_type = "application/octet-stream";

	const uint8_t *body_data = data;
	size_t body_size = size;
	std::vector<uint8_t> compressed;

	if (encoding != HttpEncoding::NONE) {
		compressed = Compress(encoding, data, size);
		body_data = compressed.data();
		body_size = compressed.size();
		headers.Insert("Content-Encoding", EncodingName(encoding));
		headers.Insert("X-VGI-Content-Encoding", EncodingName(encoding));
	}
	// Signed headers (upload URLs v2), verbatim. A Content-Type among them
	// replaces the default rather than being sent twice.
	for (const auto &header : extra_headers) {
		if (StringUtil::CIEquals(header.first, "Content-Type")) {
			content_type = header.second;
			continue;
		}
		headers.Insert(header.first, header.second);
	}

	// A pre-signed upload URL: never report its query.
	const auto shown = RedactPresignedUrl(url);
	PutRequestInfo put(url, headers, *params, reinterpret_cast<const_data_ptr_t>(body_data),
	                   static_cast<idx_t>(body_size), content_type);
	unique_ptr<HTTPResponse> response;
	try {
		response = http_util.Request(put);
	} catch (std::exception &e) {
		RethrowRedactingUrl(e, url, shown);
	}
	if (!response) {
		throw IOException(BuildHttpExtraInfo(http_error::kTransportFailure, shown),
		                  "VGI upload returned no response (transport failure) [url: %s]", shown);
	}

	if (!response->Success()) {
		const auto code = StorageErrorCode(response->body);
		// The URL is signed for the body's size and SHA-256 (upload URLs v2), so
		// storage itself refuses bytes that are not the ones hashed: S3 and R2
		// answer BadDigest, MinIO XAmzContentChecksumMismatch.
		std::string detail;
		const char *subtype = http_error::kHttpError;
		if (code == "BadDigest" || code == "XAmzContentSHA256Mismatch" || code == "XAmzContentChecksumMismatch" ||
		    code == "InvalidDigest") {
			detail = ": storage checksum mismatch (" + code + "), the uploaded bytes do not match their SHA-256";
			subtype = http_error::kChecksumMismatch;
		} else if (code == "IncompleteBody" || code == "EntityTooSmall" || code == "EntityTooLarge") {
			detail = ": storage size mismatch (" + code + "), the uploaded bytes do not match their declared size";
			subtype = http_error::kChecksumMismatch;
		} else if (!code.empty()) {
			detail = ": storage error " + code;
		}
		throw IOException(BuildHttpExtraInfo(subtype, shown, static_cast<int>(response->status)),
		                  "VGI upload failed (HTTP %d)%s [url: %s]", static_cast<int>(response->status), detail, shown);
	}
}

bool ShouldExternalizeArguments(const ServerCapabilities &caps, uint64_t arguments_size) {
	// The header is the capability signal: without it, never.
	if (!CapabilitiesSupport(caps, ProtocolFeature::ARGUMENTS_REF)) {
		return false;
	}
	uint64_t threshold = static_cast<uint64_t>(caps.arguments_ref_bytes);
	if (caps.max_request_bytes > 0) {
		// M / 4 in integer division; an unset (or <= 0) cap counts as infinite.
		threshold = std::min(threshold, static_cast<uint64_t>(caps.max_request_bytes) / 4);
	}
	if (arguments_size <= threshold) {
		return false;
	}
	// Over the upload cap: an upload would be refused. Left inline, the bind
	// takes the ordinary path (and its whole-request upload, which refuses it
	// with a clear message when it does not fit either).
	if (caps.max_upload_bytes > 0 && arguments_size > static_cast<uint64_t>(caps.max_upload_bytes)) {
		return false;
	}
	return true;
}

UploadedArguments UploadArgumentsForReference(ClientContext &context, const std::string &base_url,
                                              const uint8_t *data, size_t size,
                                              const std::shared_ptr<CatalogAuth> &auth,
                                              const std::shared_ptr<IrohClientConfig> &iroh_config) {
	const auto started = std::chrono::steady_clock::now();
	UploadedArguments uploaded;
	uploaded.size = static_cast<int64_t>(size);
	uploaded.sha256_hex = VgiSha256Hex(data, size);
	// The grant is requested with the same credentials and connection settings
	// as the bind that presents its ref: both feed the worker's ARGREF AAD.
	auto urls = HttpRequestUploadUrls(context, base_url, {UploadRequest {uploaded.size, uploaded.sha256_hex}}, auth,
	                                  iroh_config);
	if (urls.empty()) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI server returned no upload URLs [url: %s]", base_url);
	}
	if (!urls[0].has_ref || urls[0].ref.empty()) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI server advertises %s but its upload grant carries no argument reference (column "
		                  "'ref'); upgrade the worker [url: %s]",
		                  kArgumentsRefBytesHeader, base_url);
	}
	if (!ExternalLocationUrlAllowed(context, urls[0].upload_url)) {
		throw IOException(BuildHttpExtraInfo(http_error::kUnsupported, base_url),
		                  "VGI server vended an upload URL that is not https:// (plain http is accepted only with "
		                  "SET vgi_storage_allow_plain_http = true, or for a loopback host with %s=1) [url: %s]",
		                  kAllowLoopbackExternalLocationsEnv, RedactPresignedUrl(urls[0].upload_url));
	}
	// Exactly the hashed bytes, uncompressed, straight from the caller's buffer.
	HttpPutBytes(context, urls[0].upload_url, data, size, HttpEncoding::NONE, urls[0].upload_headers);
	uploaded.ref = std::move(urls[0].ref);

	const auto normalized = NormalizeBaseUrl(base_url);
	RecordArgumentsRefUpload(normalized, size);
	const auto elapsed_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
	VGI_LOG(context, "arguments_ref.upload",
	        {{"url", RedactCredentials(normalized)},
	         {"size", std::to_string(size)},
	         {"ms", std::to_string(elapsed_ms)}});
	return uploaded;
}

bool MaybeExternalizeRequestBody(ClientContext &context, const std::string &base_url, std::vector<uint8_t> &body,
                                 const ServerCapabilities &caps, const std::shared_ptr<CatalogAuth> &auth,
                                 const std::shared_ptr<IrohClientConfig> &iroh_config) {
	if (caps.max_request_bytes <= 0 || static_cast<int64_t>(body.size()) <= caps.max_request_bytes) {
		return false;
	}
	if (caps.upload_url_version == 0) {
		// Sent inline: the server's 413 names its cap and the missing upload path.
		return false;
	}
	if (caps.upload_url_version != kUploadUrlProtocolVersion) {
		// v1 ("true") vended blank URLs that accept any bytes; never fall back to it.
		// Wording follows vgi-rpc's reference client (UPLOAD_URL_V2.md).
		const bool too_old = caps.upload_url_version < kUploadUrlProtocolVersion;
		throw IOException(BuildHttpExtraInfo(too_old ? http_error::kServerTooOld : http_error::kUnsupported, base_url),
		                  "VGI server upload protocol is %s: it advertises VGI-Upload-URL-Support v%d and this "
		                  "client needs %d (the request of %llu bytes exceeds the server's VGI-Max-Request-Bytes "
		                  "(%lld) and must be uploaded). %s [url: %s]",
		                  too_old ? "too old" : "newer than this client", caps.upload_url_version,
		                  kUploadUrlProtocolVersion, static_cast<unsigned long long>(body.size()),
		                  static_cast<long long>(caps.max_request_bytes),
		                  too_old ? "Upgrade the worker" : "Upgrade the vgi extension", base_url);
	}
	if (caps.max_upload_bytes > 0 && static_cast<int64_t>(body.size()) > caps.max_upload_bytes) {
		throw IOException(BuildHttpExtraInfo(http_error::kLimitExceeded, base_url),
		                  "VGI request of %llu bytes exceeds both the server's VGI-Max-Request-Bytes (%lld) and "
		                  "VGI-Max-Upload-Bytes (%lld) [url: %s]",
		                  static_cast<unsigned long long>(body.size()), static_cast<long long>(caps.max_request_bytes),
		                  static_cast<long long>(caps.max_upload_bytes), base_url);
	}
	// The pointer keeps the request's schema and every metadata key (method,
	// protocol, state tokens, validators): the server routes and reads tokens
	// from the outer batch, then resolves the pointer for the payload. The
	// reader borrows `body` (no copy); both are released before `body` is
	// replaced below.
	auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(VgiBuffer::Wrap(body.data(), body.size())));
	auto request_batch = reader->ReadNext();
	if (!request_batch) {
		throw InternalException("vgi: request body to externalize holds no batch [url: %s]", base_url);
	}
	// The pointer carries every metadata key, so it is never smaller than the
	// request's metadata. When that alone reaches the cap (large state tokens;
	// a producer tick or a cancel is nothing but metadata), an upload cannot
	// bring the request under it: send it inline rather than pay for an upload
	// whose pointer the server refuses anyway. Compression may still fit it,
	// and otherwise the server's 413 names its cap.
	if (request_batch->metadata()) {
		uint64_t metadata_bytes = 0;
		for (const auto &pair : request_batch->metadata()->pairs()) {
			metadata_bytes += pair.first.size() + pair.second.size();
		}
		if (static_cast<int64_t>(metadata_bytes) >= caps.max_request_bytes) {
			VGI_STDERR_DEBUG("[VGI] http.request_not_externalized url=%s body_bytes=%llu metadata_bytes=%llu "
			                 "max_request_bytes=%lld\n",
			                 base_url.c_str(), static_cast<unsigned long long>(body.size()),
			                 static_cast<unsigned long long>(metadata_bytes),
			                 static_cast<long long>(caps.max_request_bytes));
			return false;
		}
	}
	// Hashed in place: no copy of a possibly large body.
	const auto body_sha256 = VgiSha256Hex(body.data(), body.size());
	auto urls = HttpRequestUploadUrls(context, base_url, {UploadRequest {static_cast<int64_t>(body.size()), body_sha256}},
	                                  auth, iroh_config);
	if (urls.empty()) {
		throw IOException(BuildHttpExtraInfo(http_error::kProtocolViolation, base_url),
		                  "VGI server returned no upload URLs [url: %s]", base_url);
	}
	// The same scheme rule as a fetched location: request bytes never leave
	// over plaintext to a non-loopback host.
	for (const auto *vended : {&urls[0].upload_url, &urls[0].download_url}) {
		if (!ExternalLocationUrlAllowed(context, *vended)) {
			throw IOException(BuildHttpExtraInfo(http_error::kUnsupported, base_url),
			                  "VGI server vended an upload URL that is not https:// (plain http is accepted only with "
			                  "SET vgi_storage_allow_plain_http = true, or for a loopback host with %s=1) [url: %s]",
			                  kAllowLoopbackExternalLocationsEnv, RedactPresignedUrl(*vended));
		}
	}
	// Exactly the hashed bytes, uncompressed: the URL is signed for this size
	// and digest, so storage rejects anything else.
	HttpPutBytes(context, urls[0].upload_url, body, HttpEncoding::NONE, urls[0].upload_headers);

	VgiMetadata metadata(request_batch->metadata()->pairs());
	metadata.Set(RPC_LOCATION_KEY, urls[0].download_url);
	metadata.Set(RPC_LOCATION_SHA256_KEY, body_sha256);
	auto pointer = VgiBatch::MakeEmpty(request_batch->schema());
	const auto inline_bytes = body.size();
	auto pointer_body = SerializeBatch(*pointer, &metadata)->ToVector();
	// Drop everything that borrows the old body before it is freed.
	pointer.reset();
	request_batch.reset();
	reader.reset();
	body = std::move(pointer_body);
	RecordHttpRequestUpload(NormalizeBaseUrl(base_url), inline_bytes);
	VGI_STDERR_DEBUG("[VGI] http.request_externalized url=%s inline_bytes=%llu pointer_bytes=%llu\n", base_url.c_str(),
	                 static_cast<unsigned long long>(inline_bytes), static_cast<unsigned long long>(body.size()));
	return true;
}

} // namespace vgi
} // namespace duckdb
