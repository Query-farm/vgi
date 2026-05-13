#include "vgi_oauth.hpp"

#include <cctype>
#include <future>
#ifdef __EMSCRIPTEN__
#include <unistd.h>  // usleep
// duckdb-wasm js-stubs — callable from WASM side modules (extensions)
extern "C" void duckdb_wasm_crypto_random(void *buf, int len);
extern "C" void duckdb_wasm_sha256(const void *data, int len, void *out_hash);
extern "C" char *duckdb_wasm_open_auth_url(const char *url, int timeout_ms);
extern "C" char *duckdb_wasm_get_auth_error(int unused);
extern "C" char *duckdb_wasm_get_page_origin(void);
#else
#include <thread>
#endif

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "yyjson.hpp"

#include "vgi_logging.hpp"
#include "duckdb/logging/logger.hpp"

#ifndef __EMSCRIPTEN__
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#endif

#ifndef __EMSCRIPTEN__
#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#include <shellapi.h>
#endif
#endif

#ifndef __EMSCRIPTEN__
// DuckDB-vendored httplib for the local callback server (PKCE flow only)
#include "httplib.hpp"
#endif

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

// Forward declaration (defined later, used by both device code and PKCE flows)
static std::string GetResourceDisplayName(const OAuthResourceMetadata &meta);

//===--------------------------------------------------------------------===//
// OAuthTokenSet
//===--------------------------------------------------------------------===//

bool OAuthTokenSet::IsValid() const {
	if (access_token.empty()) {
		return false;
	}
	if (expires_at != std::chrono::steady_clock::time_point{}) {
		return std::chrono::steady_clock::now() < expires_at;
	}
	return true;
}

std::string OAuthTokenSet::BearerToken() const {
	// Prefer access_token per OAuth 2.0 spec. If the resource server needs
	// id_token (e.g., when access_token is opaque), use_id_token_as_bearer
	// in the resource metadata controls this.
	if (use_id_token && !id_token.empty()) {
		return id_token;
	}
	return access_token;
}

bool OAuthServerMetadata::SupportsGrantType(const std::string &grant_type) const {
	// If the server doesn't advertise grant_types_supported, don't restrict
	if (grant_types_supported.empty()) {
		return true;
	}
	for (const auto &gt : grant_types_supported) {
		if (gt == grant_type) {
			return true;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// PKCE Crypto Utilities
//===--------------------------------------------------------------------===//

std::string Base64UrlEncode(const unsigned char *data, size_t len) {
	static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	std::string result;
	result.reserve(4 * ((len + 2) / 3));

	for (size_t i = 0; i < len; i += 3) {
		uint32_t n = static_cast<uint32_t>(data[i]) << 16;
		if (i + 1 < len) n |= static_cast<uint32_t>(data[i + 1]) << 8;
		if (i + 2 < len) n |= static_cast<uint32_t>(data[i + 2]);

		result += table[(n >> 18) & 0x3F];
		result += table[(n >> 12) & 0x3F];
		if (i + 1 < len) {
			result += table[(n >> 6) & 0x3F];
		}
		if (i + 2 < len) {
			result += table[n & 0x3F];
		}
	}

	// Convert to base64url: + -> -, / -> _, strip =
	for (auto &c : result) {
		if (c == '+') c = '-';
		else if (c == '/') c = '_';
	}

	return result;
}

std::string Base64UrlDecode(const std::string &input) {
	// Map base64url alphabet -> value. Returns 0xFF for non-alphabet bytes.
	auto val = [](unsigned char c) -> int {
		if (c >= 'A' && c <= 'Z') return c - 'A';
		if (c >= 'a' && c <= 'z') return c - 'a' + 26;
		if (c >= '0' && c <= '9') return c - '0' + 52;
		if (c == '-' || c == '+') return 62;
		if (c == '_' || c == '/') return 63;
		return -1;
	};

	// Strip any accidental padding / whitespace
	std::string clean;
	clean.reserve(input.size());
	for (unsigned char c : input) {
		if (c == '=' || c == '\n' || c == '\r' || c == ' ' || c == '\t') continue;
		clean.push_back(static_cast<char>(c));
	}

	std::string out;
	out.reserve((clean.size() * 3) / 4);
	uint32_t buf = 0;
	int bits = 0;
	for (unsigned char c : clean) {
		int v = val(c);
		if (v < 0) {
			return ""; // malformed
		}
		buf = (buf << 6) | static_cast<uint32_t>(v);
		bits += 6;
		if (bits >= 8) {
			bits -= 8;
			out.push_back(static_cast<char>((buf >> bits) & 0xFF));
		}
	}
	return out;
}

OAuthIdentity ParseIdTokenClaims(const std::string &id_token) {
	// Not cryptographically verified — id_token arrived over TLS from our own OAuth exchange.
	OAuthIdentity identity;
	if (id_token.empty()) {
		return identity;
	}

	// JWT = header.payload.signature — we only need the payload.
	auto first_dot = id_token.find('.');
	if (first_dot == std::string::npos) {
		return identity;
	}
	auto second_dot = id_token.find('.', first_dot + 1);
	if (second_dot == std::string::npos) {
		return identity;
	}
	auto payload_b64 = id_token.substr(first_dot + 1, second_dot - first_dot - 1);
	auto payload_json = Base64UrlDecode(payload_b64);
	if (payload_json.empty()) {
		return identity;
	}

	auto doc = yyjson_read(payload_json.c_str(), payload_json.size(), 0);
	if (!doc) {
		return identity;
	}
	// Manual RAII: YyjsonDocGuard is defined later in this file.
	struct DocFree {
		yyjson_doc *d;
		~DocFree() { if (d) yyjson_doc_free(d); }
	} doc_guard{doc};
	auto root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		return identity;
	}

	// Lift the four universal OIDC claims into dedicated fields for SQL ergonomics.
	auto sub = yyjson_obj_get(root, "sub");
	if (sub && yyjson_is_str(sub)) {
		identity.sub = yyjson_get_str(sub);
	}
	auto email = yyjson_obj_get(root, "email");
	if (email && yyjson_is_str(email)) {
		identity.email = yyjson_get_str(email);
	}
	auto name = yyjson_obj_get(root, "name");
	if (name && yyjson_is_str(name)) {
		identity.name = yyjson_get_str(name);
	}
	auto iss = yyjson_obj_get(root, "iss");
	if (iss && yyjson_is_str(iss)) {
		identity.issuer = yyjson_get_str(iss);
	}

	// Stash the raw decoded payload — caller exposes it as a JSON column so
	// provider-specific claims (Entra preferred_username/oid/tid, group/role
	// arrays, custom attributes) are reachable without hardcoding keys here.
	identity.claims_json = payload_json;
	identity.present = true;
	return identity;
}

#ifdef __EMSCRIPTEN__
// WASM: use duckdb-wasm js-stubs for crypto (Web Crypto random + JS SHA-256).
// These are callable from side modules via standard WASM imports — no EM_ASM needed.

std::string GenerateCodeVerifier() {
	unsigned char buf[32];
	duckdb_wasm_crypto_random(buf, sizeof(buf));
	return Base64UrlEncode(buf, sizeof(buf));
}

std::string ComputeCodeChallenge(const std::string &verifier) {
	unsigned char hash[32];
	duckdb_wasm_sha256(verifier.data(), static_cast<int>(verifier.size()), hash);
	return Base64UrlEncode(hash, sizeof(hash));
}

std::string GenerateState() {
	unsigned char buf[16];
	duckdb_wasm_crypto_random(buf, sizeof(buf));
	std::string result;
	result.reserve(32);
	static const char hex[] = "0123456789abcdef";
	for (size_t i = 0; i < sizeof(buf); i++) {
		result += hex[(buf[i] >> 4) & 0xF];
		result += hex[buf[i] & 0xF];
	}
	return result;
}
#else
std::string GenerateCodeVerifier() {
	unsigned char buf[32];
	if (RAND_bytes(buf, sizeof(buf)) != 1) {
		throw IOException("VGI OAuth: failed to generate random bytes for code verifier");
	}
	return Base64UrlEncode(buf, sizeof(buf));
}

std::string ComputeCodeChallenge(const std::string &verifier) {
	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int hash_len = 0;
	if (EVP_Digest(verifier.data(), verifier.size(), hash, &hash_len, EVP_sha256(), nullptr) != 1) {
		throw IOException("VGI OAuth: SHA256 computation failed");
	}
	return Base64UrlEncode(hash, hash_len);
}

std::string GenerateState() {
	unsigned char buf[16];
	if (RAND_bytes(buf, sizeof(buf)) != 1) {
		throw IOException("VGI OAuth: failed to generate random bytes for state");
	}

	// Hex-encode
	std::string result;
	result.reserve(32);
	static const char hex[] = "0123456789abcdef";
	for (size_t i = 0; i < sizeof(buf); i++) {
		result += hex[(buf[i] >> 4) & 0xF];
		result += hex[buf[i] & 0xF];
	}
	return result;
}
#endif // !__EMSCRIPTEN__

std::string UrlEncode(const std::string &str) {
	std::string result;
	result.reserve(str.size() * 3);
	for (unsigned char c : str) {
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
			result += static_cast<char>(c);
		} else {
			static const char hex[] = "0123456789ABCDEF";
			result += '%';
			result += hex[(c >> 4) & 0xF];
			result += hex[c & 0xF];
		}
	}
	return result;
}

// Render a secret for inclusion in debug error messages. The previous version
// echoed the full secret to keep IdP-failure debugging tractable, but
// IOException messages travel further than "the user's shell" — DuckDB's log
// manager, telemetry, JDBC clients, and any caller that catches IOException
// and forwards .what() to logs all see them. A leaked refresh_token is
// effectively account compromise. Render a fingerprint instead: length plus
// 4-char prefix/suffix is enough to correlate against IdP logs without
// exposing the bytes.
static std::string DebugSecret(const std::string &secret) {
	if (secret.empty()) {
		return "(empty)";
	}
	std::string fingerprint = "(" + std::to_string(secret.size()) + " chars)";
	if (secret.size() <= 8) {
		// Short enough that prefix+suffix overlap; just say redacted.
		return fingerprint + " <redacted>";
	}
	return fingerprint + " " + secret.substr(0, 4) + "..." + secret.substr(secret.size() - 4);
}

//===--------------------------------------------------------------------===//
// WWW-Authenticate Header Parsing
//===--------------------------------------------------------------------===//

std::optional<OAuthChallenge> ParseWWWAuthenticate(const std::string &header) {
	// Parse: Bearer resource_metadata="<url>", client_id="<id>"
	// or: Bearer resource_metadata="<url>"
	if (header.substr(0, 7) != "Bearer ") {
		return std::nullopt;
	}

	OAuthChallenge challenge;

	// Extract resource_metadata="..."
	auto rm_pos = header.find("resource_metadata=\"");
	if (rm_pos == std::string::npos) {
		return std::nullopt;
	}
	auto rm_start = rm_pos + 19; // length of 'resource_metadata="'
	auto rm_end = header.find('"', rm_start);
	if (rm_end == std::string::npos) {
		return std::nullopt;
	}
	challenge.resource_metadata_url = header.substr(rm_start, rm_end - rm_start);

	// Extract client_id="..." (optional in header, may come from resource metadata)
	auto ci_pos = header.find("client_id=\"");
	if (ci_pos != std::string::npos) {
		auto ci_start = ci_pos + 11;
		auto ci_end = header.find('"', ci_start);
		if (ci_end != std::string::npos) {
			challenge.client_id = header.substr(ci_start, ci_end - ci_start);
		}
	}

	return challenge;
}

//===--------------------------------------------------------------------===//
// HTTP GET Helper
//===--------------------------------------------------------------------===//

std::string HttpGet(ClientContext &context, const std::string &url) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	params->timeout = 30; // 30s for metadata fetches

	HTTPHeaders headers;
	std::string response_body;

	GetRequestInfo get_info(
		url, headers, *params,
		[](const HTTPResponse &) { return true; },
		[&response_body](const_data_ptr_t data, idx_t data_length) {
			response_body.append(reinterpret_cast<const char *>(data), data_length);
			return true;
		});

	auto response = http_util.Request(get_info);
	if (!response->Success()) {
		throw IOException("VGI OAuth: HTTP GET failed for %s (HTTP %d): %s",
		                  url, static_cast<int>(response->status), response->GetError());
	}

	return response_body;
}

// RAII wrapper for yyjson documents
struct YyjsonDocGuard {
	yyjson_doc *doc;
	explicit YyjsonDocGuard(yyjson_doc *d) : doc(d) {}
	~YyjsonDocGuard() { if (doc) yyjson_doc_free(doc); }
	YyjsonDocGuard(const YyjsonDocGuard &) = delete;
	YyjsonDocGuard &operator=(const YyjsonDocGuard &) = delete;
};

//===--------------------------------------------------------------------===//
// Metadata Discovery
//===--------------------------------------------------------------------===//

static void EnforceHttpsUrl(const std::string &url, const std::string &context_name) {
	// Belt-and-suspenders: the upstream caller in vgi_http_client.cpp now
	// short-circuits on an empty challenge URL with a clean "no credential"
	// error, so a non-empty URL is expected here. If anything else ever
	// feeds an empty URL into the OAuth path, surface a readable message
	// rather than the trailing-whitespace "must use HTTPS: " confusion.
	if (url.empty()) {
		throw IOException("VGI OAuth: %s URL is empty (no OAuth challenge from server)", context_name);
	}
	if (url.substr(0, 8) != "https://" && url.substr(0, 16) != "http://127.0.0.1" &&
	    url.substr(0, 16) != "http://localhost") {
		throw IOException("VGI OAuth: %s URL must use HTTPS: %s", context_name, url);
	}
}

// Join scopes_supported with spaces, falling back to "openid" when the
// resource metadata didn't advertise any scopes.
static std::string BuildScopeString(const OAuthResourceMetadata &resource_meta) {
	if (resource_meta.scopes_supported.empty()) {
		return "openid";
	}
	std::string scope_str;
	for (size_t i = 0; i < resource_meta.scopes_supported.size(); i++) {
		if (i > 0) scope_str += " ";
		scope_str += resource_meta.scopes_supported[i];
	}
	return scope_str;
}

OAuthResourceMetadata FetchResourceMetadata(ClientContext &context, const std::string &url) {
	EnforceHttpsUrl(url, "resource metadata");
	auto body = HttpGet(context, url);

	auto doc = yyjson_read(body.c_str(), body.size(), 0);
	if (!doc) {
		throw IOException("VGI OAuth: failed to parse resource metadata JSON from %s", url);
	}
	YyjsonDocGuard guard(doc);

	OAuthResourceMetadata meta;
	auto root = yyjson_doc_get_root(doc);

	// authorization_servers (array of strings)
	auto servers = yyjson_obj_get(root, "authorization_servers");
	if (servers && yyjson_is_arr(servers)) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(servers, idx, max, val) {
			if (yyjson_is_str(val)) {
				meta.authorization_servers.emplace_back(yyjson_get_str(val));
			}
		}
	}

	// scopes_supported (array of strings)
	auto scopes = yyjson_obj_get(root, "scopes_supported");
	if (scopes && yyjson_is_arr(scopes)) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(scopes, idx, max, val) {
			if (yyjson_is_str(val)) {
				meta.scopes_supported.emplace_back(yyjson_get_str(val));
			}
		}
	}

	// client_id (string, optional)
	auto client_id = yyjson_obj_get(root, "client_id");
	if (client_id && yyjson_is_str(client_id)) {
		meta.client_id = yyjson_get_str(client_id);
	}

	// client_secret (string, optional — for providers like Google that require it even for public/desktop clients)
	auto client_secret = yyjson_obj_get(root, "client_secret");
	if (client_secret && yyjson_is_str(client_secret)) {
		meta.client_secret = yyjson_get_str(client_secret);
	}

	// device_code_client_id (string, optional — separate client for device code flow, e.g., Google TV type)
	auto dc_client_id = yyjson_obj_get(root, "device_code_client_id");
	if (dc_client_id && yyjson_is_str(dc_client_id)) {
		meta.device_code_client_id = yyjson_get_str(dc_client_id);
	}

	// device_code_client_secret (string, optional)
	auto dc_client_secret = yyjson_obj_get(root, "device_code_client_secret");
	if (dc_client_secret && yyjson_is_str(dc_client_secret)) {
		meta.device_code_client_secret = yyjson_get_str(dc_client_secret);
	}

	// resource (string, optional)
	auto resource = yyjson_obj_get(root, "resource");
	if (resource && yyjson_is_str(resource)) {
		meta.resource = yyjson_get_str(resource);
	}

	// resource_name (string, optional — human-readable name)
	auto resource_name = yyjson_obj_get(root, "resource_name");
	if (resource_name && yyjson_is_str(resource_name)) {
		meta.resource_name = yyjson_get_str(resource_name);
	}

	// use_id_token_as_bearer (bool, optional — for servers that validate id_tokens instead of access_tokens)
	auto use_id = yyjson_obj_get(root, "use_id_token_as_bearer");
	if (use_id && yyjson_is_bool(use_id)) {
		meta.use_id_token_as_bearer = yyjson_get_bool(use_id);
	}

	// token_endpoint (string, optional — non-standard RFC 9728 extension)
	// When set, this is the VGI server's PKCE token-exchange proxy URL.
	// Use it instead of the IdP's discovered token_endpoint so the extension
	// doesn't have to hold the IdP's client_secret. See the comment on
	// OAuthResourceMetadata::token_endpoint for the security rationale.
	auto token_ep = yyjson_obj_get(root, "token_endpoint");
	if (token_ep && yyjson_is_str(token_ep)) {
		meta.token_endpoint = yyjson_get_str(token_ep);
	}

	if (meta.authorization_servers.empty()) {
		throw IOException("VGI OAuth: resource metadata at %s has no authorization_servers", url);
	}

	return meta;
}

OAuthServerMetadata FetchAuthServerMetadata(ClientContext &context, const std::string &server_url) {
	EnforceHttpsUrl(server_url, "authorization server");
	// Append well-known path
	std::string url = server_url;
	if (!url.empty() && url.back() == '/') {
		url.pop_back();
	}
	url += "/.well-known/openid-configuration";

	auto body = HttpGet(context, url);

	auto doc = yyjson_read(body.c_str(), body.size(), 0);
	if (!doc) {
		throw IOException("VGI OAuth: failed to parse OpenID configuration JSON from %s", url);
	}
	YyjsonDocGuard guard(doc);

	OAuthServerMetadata meta;
	auto root = yyjson_doc_get_root(doc);

	auto auth_ep = yyjson_obj_get(root, "authorization_endpoint");
	if (auth_ep && yyjson_is_str(auth_ep)) {
		meta.authorization_endpoint = yyjson_get_str(auth_ep);
	}

	auto token_ep = yyjson_obj_get(root, "token_endpoint");
	if (token_ep && yyjson_is_str(token_ep)) {
		meta.token_endpoint = yyjson_get_str(token_ep);
	}

	auto device_ep = yyjson_obj_get(root, "device_authorization_endpoint");
	if (device_ep && yyjson_is_str(device_ep)) {
		meta.device_authorization_endpoint = yyjson_get_str(device_ep);
	}

	auto grant_types = yyjson_obj_get(root, "grant_types_supported");
	if (grant_types && yyjson_is_arr(grant_types)) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(grant_types, idx, max, val) {
			if (yyjson_is_str(val)) {
				meta.grant_types_supported.emplace_back(yyjson_get_str(val));
			}
		}
	}

	if (meta.token_endpoint.empty()) {
		throw IOException("VGI OAuth: OpenID configuration at %s missing token_endpoint", url);
	}
	if (meta.authorization_endpoint.empty() && meta.device_authorization_endpoint.empty()) {
		throw IOException("VGI OAuth: OpenID configuration at %s has neither authorization_endpoint nor device_authorization_endpoint", url);
	}

	return meta;
}

// Resolve the token endpoint to use for token-exchange / refresh requests.
//
// When the VGI server's resource metadata advertises its own token_endpoint
// (a non-standard RFC 9728 extension), prefer it over the IdP's discovered
// endpoint. The advertised URL points at the server's PKCE token-exchange
// proxy, which injects the configured server-side client_secret before
// forwarding to the IdP. This lets the WASM build complete token exchanges
// without holding the IdP's client_secret locally — which matters for IdPs
// (notably Google) that reject token-endpoint requests from "Web
// application" clients without a secret.
static std::string ResolveTokenEndpoint(const OAuthResourceMetadata &resource_meta,
                                         const OAuthServerMetadata &server_meta) {
	return resource_meta.token_endpoint.empty() ? server_meta.token_endpoint
	                                              : resource_meta.token_endpoint;
}

//===--------------------------------------------------------------------===//
// Browser Opening
//===--------------------------------------------------------------------===//

void OpenBrowser(const std::string &url) {
#if defined(__EMSCRIPTEN__)
	// In WASM, we can't open a browser from a Worker — the URL is printed to stderr for the user
	fprintf(stderr, "[VGI] Please open this URL in your browser: %s\n", url.c_str());
#elif defined(__APPLE__)
	pid_t pid = fork();
	if (pid == 0) {
		execlp("open", "open", url.c_str(), nullptr);
		_exit(1);
	}
#elif defined(_WIN32)
	ShellExecuteA(nullptr, "open", url.c_str(), nullptr, nullptr, SW_SHOWNORMAL);
#else
	pid_t pid = fork();
	if (pid == 0) {
		// Redirect stderr to /dev/null
		int devnull = open("/dev/null", O_WRONLY);
		if (devnull >= 0) {
			dup2(devnull, STDERR_FILENO);
			close(devnull);
		}
		execlp("xdg-open", "xdg-open", url.c_str(), nullptr);
		_exit(1);
	}
#endif
}

//===--------------------------------------------------------------------===//
// Token Exchange
//===--------------------------------------------------------------------===//

// Parse OAuth token response JSON into an OAuthTokenSet
static OAuthTokenSet ParseTokenResponse(const std::string &json_body, const std::string &error_context) {
	auto doc = yyjson_read(json_body.c_str(), json_body.size(), 0);
	if (!doc) {
		throw IOException("VGI OAuth: failed to parse %s JSON", error_context);
	}
	YyjsonDocGuard guard(doc);

	OAuthTokenSet tokens;
	auto root = yyjson_doc_get_root(doc);

	auto at = yyjson_obj_get(root, "access_token");
	if (at && yyjson_is_str(at)) {
		tokens.access_token = yyjson_get_str(at);
	}

	auto rt = yyjson_obj_get(root, "refresh_token");
	if (rt && yyjson_is_str(rt)) {
		tokens.refresh_token = yyjson_get_str(rt);
	}

	auto it = yyjson_obj_get(root, "id_token");
	if (it && yyjson_is_str(it)) {
		tokens.id_token = yyjson_get_str(it);
		tokens.identity = ParseIdTokenClaims(tokens.id_token);
	}

	auto sc = yyjson_obj_get(root, "scope");
	if (sc && yyjson_is_str(sc)) {
		tokens.scope = yyjson_get_str(sc);
	}

	auto exp = yyjson_obj_get(root, "expires_in");
	if (exp && yyjson_is_num(exp)) {
		auto seconds = yyjson_get_int(exp);
		tokens.expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
	}

	if (tokens.access_token.empty()) {
		throw IOException("VGI OAuth: %s missing access_token", error_context);
	}

	return tokens;
}

// POST form-encoded body to a token endpoint, return status code and body without throwing on non-200.
// Required for device code polling where 400 + "authorization_pending" is expected.
struct RawTokenResponse {
	int status_code;
	std::string body;
};

static RawTokenResponse PostTokenRequestRaw(ClientContext &context,
                                             const std::string &token_endpoint,
                                             const std::string &body) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, token_endpoint);
	params->timeout = 30;

	HTTPHeaders headers;
	headers.Insert("Content-Type", "application/x-www-form-urlencoded");

	PostRequestInfo post(token_endpoint, headers, *params,
	                     reinterpret_cast<const_data_ptr_t>(body.data()),
	                     static_cast<idx_t>(body.size()));

	auto response = http_util.Request(post);
	auto &resp_body = post.buffer_out.empty() ? response->body : post.buffer_out;
	return {static_cast<int>(response->status), resp_body};
}

// POST form-encoded body to a token endpoint, return parsed tokens
static std::string PostTokenRequest(ClientContext &context,
                                     const std::string &token_endpoint,
                                     const std::string &body,
                                     const std::string &error_context) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, token_endpoint);
	params->timeout = 30;

	HTTPHeaders headers;
	headers.Insert("Content-Type", "application/x-www-form-urlencoded");

	PostRequestInfo post(token_endpoint, headers, *params,
	                     reinterpret_cast<const_data_ptr_t>(body.data()),
	                     static_cast<idx_t>(body.size()));

	auto response = http_util.Request(post);
	if (!response->Success()) {
		auto &error_body = post.buffer_out.empty() ? response->body : post.buffer_out;
		throw IOException("VGI OAuth: %s failed (HTTP %d): %s",
		                  error_context, static_cast<int>(response->status), error_body);
	}

	return post.buffer_out.empty() ? response->body : post.buffer_out;
}

static OAuthTokenSet ExchangeCodeForTokens(ClientContext &context,
                                            const std::string &token_endpoint,
                                            const std::string &code,
                                            const std::string &redirect_uri,
                                            const std::string &code_verifier,
                                            const std::string &client_id,
                                            const std::string &client_secret) {
	EnforceHttpsUrl(token_endpoint, "token endpoint");
	std::string body = "grant_type=authorization_code"
	                   "&code=" + UrlEncode(code) +
	                   "&redirect_uri=" + UrlEncode(redirect_uri) +
	                   "&code_verifier=" + UrlEncode(code_verifier) +
	                   "&client_id=" + UrlEncode(client_id);
	if (!client_secret.empty()) {
		body += "&client_secret=" + UrlEncode(client_secret);
	}

	auto resp = PostTokenRequestRaw(context, token_endpoint, body);
	if (resp.status_code != 200) {
		// Surface the request shape through the exception so IdP-specific
		// failures (Microsoft Entra "AADSTS9002313: Invalid request") are
		// diagnosable from the user-visible error alone. Secrets are
		// fingerprinted (length + first/last 4 chars) so server-side IdP
		// logs can be correlated without leaking the bytes — IOException
		// messages travel to log manager / telemetry / JDBC clients.
		throw IOException(
		    "VGI OAuth: token exchange failed (HTTP %d): %s\n"
		    "  request:\n"
		    "    token_endpoint=%s\n"
		    "    grant_type=authorization_code\n"
		    "    code=%s\n"
		    "    redirect_uri=%s\n"
		    "    code_verifier=%s\n"
		    "    client_id=%s\n"
		    "    client_secret=%s",
		    resp.status_code, resp.body,
		    token_endpoint,
		    DebugSecret(code),
		    redirect_uri.empty() ? "<empty>" : redirect_uri,
		    DebugSecret(code_verifier),
		    client_id.empty() ? "<empty>" : client_id,
		    client_secret.empty() ? "<not sent>" : DebugSecret(client_secret));
	}
	return ParseTokenResponse(resp.body, "token response");
}

//===--------------------------------------------------------------------===//
// Headless Environment Detection
//===--------------------------------------------------------------------===//

bool IsHeadlessEnvironment() {
	// SSH session
	if (std::getenv("SSH_CONNECTION") || std::getenv("SSH_CLIENT")) {
		return true;
	}
	// CI environment
	auto ci = std::getenv("CI");
	if (ci && std::string(ci) == "true") {
		return true;
	}
	// Docker container
	if (std::getenv("DOCKER_CONTAINER")) {
		return true;
	}
#if defined(__linux__) || defined(__APPLE__)
	struct stat st;
	if (stat("/.dockerenv", &st) == 0) {
		return true;
	}
#endif
	// Kubernetes pod
	if (std::getenv("KUBERNETES_SERVICE_HOST")) {
		return true;
	}
	// No GUI available
#if defined(__linux__)
	if (!std::getenv("DISPLAY") && !std::getenv("WAYLAND_DISPLAY")) {
		return true;
	}
#elif defined(__APPLE__)
	if (!std::getenv("TERM_PROGRAM") && !std::getenv("DISPLAY")) {
		return true;
	}
#endif
	return false;
}

//===--------------------------------------------------------------------===//
// Device Code Flow (RFC 8628)
//===--------------------------------------------------------------------===//

static OAuthTokenSet PerformDeviceCodeFlowImpl(const OAuthChallenge &challenge,
                                                      const OAuthResourceMetadata &resource_meta,
                                                      const OAuthServerMetadata &server_meta,
                                                      ClientContext &context) {
	if (server_meta.device_authorization_endpoint.empty()) {
		throw IOException("VGI OAuth: device code flow requested but server has no device_authorization_endpoint");
	}
	if (!server_meta.SupportsGrantType("urn:ietf:params:oauth:grant-type:device_code")) {
		throw IOException("VGI OAuth: device code flow requested but server does not list "
		                  "urn:ietf:params:oauth:grant-type:device_code in grant_types_supported");
	}

	// Get timeout setting
	int64_t timeout_seconds = 120;
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_oauth_timeout_seconds", timeout_val)) {
		timeout_seconds = timeout_val.GetValue<int64_t>();
	}

	VGI_STDERR_DEBUG("[VGI] oauth.device_code_flow resource_metadata=%s\n",
	                 challenge.resource_metadata_url.c_str());

	// Prefer device_code_client_id for device code flow (e.g., Google requires separate TV-type client)
	std::string client_id = resource_meta.device_code_client_id;
	std::string client_secret = resource_meta.device_code_client_secret;
	if (client_id.empty()) {
		// Fall back to the standard client_id
		client_id = resource_meta.client_id.empty() ? challenge.client_id : resource_meta.client_id;
		client_secret = resource_meta.client_secret;
	}
	if (client_id.empty()) {
		throw IOException("VGI OAuth: no client_id found in WWW-Authenticate header or resource metadata");
	}

	// Build scope string
	std::string scope_str = BuildScopeString(resource_meta);

	// Step 1: POST to device_authorization_endpoint
	EnforceHttpsUrl(server_meta.device_authorization_endpoint, "device authorization endpoint");
	std::string device_body = "client_id=" + UrlEncode(client_id) +
	                          "&scope=" + UrlEncode(scope_str);

	auto device_resp = PostTokenRequestRaw(context, server_meta.device_authorization_endpoint, device_body);
	if (device_resp.status_code != 200) {
		throw IOException("VGI OAuth: device authorization request failed (HTTP %d): %s",
		                  device_resp.status_code, device_resp.body);
	}

	// Parse device authorization response
	auto doc = yyjson_read(device_resp.body.c_str(), device_resp.body.size(), 0);
	if (!doc) {
		throw IOException("VGI OAuth: failed to parse device authorization response JSON");
	}
	YyjsonDocGuard guard(doc);
	auto root = yyjson_doc_get_root(doc);

	auto dc_val = yyjson_obj_get(root, "device_code");
	if (!dc_val || !yyjson_is_str(dc_val)) {
		throw IOException("VGI OAuth: device authorization response missing device_code");
	}
	std::string device_code = yyjson_get_str(dc_val);

	auto uc_val = yyjson_obj_get(root, "user_code");
	if (!uc_val || !yyjson_is_str(uc_val)) {
		throw IOException("VGI OAuth: device authorization response missing user_code");
	}
	std::string user_code = yyjson_get_str(uc_val);

	// Accept both verification_uri (RFC standard) and verification_url (non-standard)
	std::string verification_uri;
	auto vu_val = yyjson_obj_get(root, "verification_uri");
	if (vu_val && yyjson_is_str(vu_val)) {
		verification_uri = yyjson_get_str(vu_val);
	} else {
		auto vu2_val = yyjson_obj_get(root, "verification_url");
		if (vu2_val && yyjson_is_str(vu2_val)) {
			verification_uri = yyjson_get_str(vu2_val);
		}
	}
	if (verification_uri.empty()) {
		throw IOException("VGI OAuth: device authorization response missing verification_uri");
	}

	std::string verification_uri_complete;
	auto vuc_val = yyjson_obj_get(root, "verification_uri_complete");
	if (vuc_val && yyjson_is_str(vuc_val)) {
		verification_uri_complete = yyjson_get_str(vuc_val);
	}

	int64_t expires_in = 300; // default 5 minutes
	auto ei_val = yyjson_obj_get(root, "expires_in");
	if (ei_val && yyjson_is_num(ei_val)) {
		expires_in = yyjson_get_int(ei_val);
	}

	int64_t interval = 5; // default per RFC 8628
	auto iv_val = yyjson_obj_get(root, "interval");
	if (iv_val && yyjson_is_num(iv_val)) {
		interval = yyjson_get_int(iv_val);
	}

	// Step 2: Print user instructions via DuckDB warning log
	std::string auth_msg = "Authentication required for " + GetResourceDisplayName(resource_meta) + ".\n"
	    "Visit: " + verification_uri + "\n"
	    "Enter code: " + user_code;
	if (!verification_uri_complete.empty()) {
		auth_msg += "\nOr visit: " + verification_uri_complete;
	}
	DUCKDB_LOG_WARNING(context, auth_msg);
	// Note: in WASM, EM_ASM can't be used from side modules (extensions).
	// The auth message is logged via DUCKDB_LOG_WARNING. The terminal UI
	// should display DuckDB warning-level logs to show the user code.

	// Step 3: Poll token endpoint
	EnforceHttpsUrl(server_meta.token_endpoint, "token endpoint");
	auto effective_timeout = std::min(timeout_seconds, expires_in);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(effective_timeout);
	auto last_status_print = std::chrono::steady_clock::now();
	int network_retries = 0;
	const int max_network_retries = 3;

	std::string poll_body = "grant_type=" + UrlEncode("urn:ietf:params:oauth:grant-type:device_code") +
	                        "&device_code=" + UrlEncode(device_code) +
	                        "&client_id=" + UrlEncode(client_id);
	if (!client_secret.empty()) {
		poll_body += "&client_secret=" + UrlEncode(client_secret);
	}

#ifndef __EMSCRIPTEN__
	// Use condition_variable for interruptible sleep (not available in WASM)
	std::mutex poll_mutex;
	std::condition_variable poll_cv;
#endif

	while (true) {
		// Sleep between poll attempts.
		// In WASM Workers, duckdb-wasm overrides _emscripten_yield with Atomics.wait,
		// so standard sleep functions block properly instead of busy-spinning.
#ifndef __EMSCRIPTEN__
		{
			std::unique_lock<std::mutex> lock(poll_mutex);
			poll_cv.wait_for(lock, std::chrono::seconds(interval));
		}
#else
		usleep(static_cast<useconds_t>(interval * 1000000));
#endif

		// Check timeout
		if (std::chrono::steady_clock::now() >= deadline) {
			throw IOException("VGI OAuth: device code authentication timed out after %lld seconds", effective_timeout);
		}

		// Periodic "still waiting" message
		auto now = std::chrono::steady_clock::now();
		auto since_last = std::chrono::duration_cast<std::chrono::seconds>(now - last_status_print).count();
		if (since_last >= 30) {
			DUCKDB_LOG_WARNING(context, "Still waiting for authentication...");
			last_status_print = now;
		}

		// Poll token endpoint
		RawTokenResponse resp;
		try {
			resp = PostTokenRequestRaw(context, server_meta.token_endpoint, poll_body);
		} catch (const std::exception &e) {
			// Network error — retry up to 3 times
			network_retries++;
			if (network_retries > max_network_retries) {
				throw IOException("VGI OAuth: device code polling failed after %d network errors: %s",
				                  max_network_retries, e.what());
			}
			VGI_STDERR_DEBUG("[VGI] oauth.device_poll network_error retry=%d: %s\n", network_retries, e.what());
			continue;
		}

		// Reset network retry counter on successful HTTP round-trip
		network_retries = 0;

		if (resp.status_code == 200) {
			// Success
			auto tokens = ParseTokenResponse(resp.body, "device code token response");
			tokens.use_id_token = resource_meta.use_id_token_as_bearer;
			DUCKDB_LOG_WARNING(context, "Authentication successful.");
			return tokens;
		}

		if (resp.status_code >= 500) {
			// Server error — retry
			network_retries++;
			if (network_retries > max_network_retries) {
				throw IOException("VGI OAuth: device code polling failed after %d server errors (HTTP %d): %s",
				                  max_network_retries, resp.status_code, resp.body);
			}
			VGI_STDERR_DEBUG("[VGI] oauth.device_poll server_error=%d retry=%d\n", resp.status_code, network_retries);
			continue;
		}

		// Parse error JSON BEFORE checking status code — servers may use non-standard
		// status codes (e.g., 403 with slow_down, 428 with authorization_pending)
		std::string error_code;
		std::string error_desc;
		auto err_doc = yyjson_read(resp.body.c_str(), resp.body.size(), 0);
		if (err_doc) {
			YyjsonDocGuard err_guard(err_doc);
			auto err_root = yyjson_doc_get_root(err_doc);
			auto err_val = yyjson_obj_get(err_root, "error");
			if (err_val && yyjson_is_str(err_val)) {
				error_code = yyjson_get_str(err_val);
			}
			auto desc_val = yyjson_obj_get(err_root, "error_description");
			if (desc_val && yyjson_is_str(desc_val)) {
				error_desc = yyjson_get_str(desc_val);
			}
		}

		if (error_code == "authorization_pending") {
			// Expected — keep polling
			continue;
		} else if (error_code == "slow_down" || resp.status_code == 429) {
			interval += 5;
			VGI_STDERR_DEBUG("[VGI] oauth.device_poll slow_down new_interval=%lld\n", interval);
			continue;
		} else if (error_code == "expired_token") {
			throw IOException("VGI OAuth: device code expired. Please try again.");
		} else if (error_code == "access_denied") {
			throw IOException("VGI OAuth: authentication was denied by the user.");
		} else if (!error_code.empty()) {
			throw IOException("VGI OAuth: device code authentication failed: %s - %s",
			                  error_code, error_desc);
		}

		// Unexpected status code with no parseable error
		throw IOException("VGI OAuth: unexpected response during device code polling (HTTP %d): %s",
		                  resp.status_code, resp.body);
	}
}

// Forward declarations for platform-specific flow implementations
static OAuthTokenSet PerformPKCEFlowImpl(const OAuthChallenge &challenge,
                                          const OAuthResourceMetadata &resource_meta,
                                          const OAuthServerMetadata &server_meta,
                                          ClientContext &context);

//===--------------------------------------------------------------------===//
// Auth Flow Orchestrator
//===--------------------------------------------------------------------===//

OAuthTokenSet PerformAuthFlow(const OAuthChallenge &challenge,
                               ClientContext &context,
                               OAuthRefreshContext &refresh_ctx_out) {
	// Read flow setting
	std::string flow = "auto";
	Value flow_val;
	if (context.TryGetCurrentSetting("vgi_oauth_flow", flow_val)) {
		flow = flow_val.GetValue<std::string>();
	}

	// Validate setting
	if (flow != "auto" && flow != "device_code" && flow != "pkce") {
		throw InvalidInputException("vgi_oauth_flow must be 'auto', 'device_code', or 'pkce' (got '%s')", flow);
	}

	VGI_STDERR_DEBUG("[VGI] oauth.auth_flow flow=%s resource_metadata=%s\n",
	                 flow.c_str(), challenge.resource_metadata_url.c_str());

	// Fetch metadata once (shared by both flows)
	auto resource_meta = FetchResourceMetadata(context, challenge.resource_metadata_url);

	std::string client_id = resource_meta.client_id.empty() ? challenge.client_id : resource_meta.client_id;
	if (client_id.empty()) {
		throw IOException("VGI OAuth: no client_id found in WWW-Authenticate header or resource metadata");
	}

	auto server_meta = FetchAuthServerMetadata(context, resource_meta.authorization_servers[0]);

	// Populate refresh context for future token refresh
	refresh_ctx_out.token_endpoint = ResolveTokenEndpoint(resource_meta, server_meta);
	refresh_ctx_out.client_id = client_id;
	// When the proxy is in use the server injects client_secret upstream, so
	// we don't need to store one locally. Only carry it when no proxy is set.
	refresh_ctx_out.client_secret = resource_meta.token_endpoint.empty() ? resource_meta.client_secret : std::string();
	refresh_ctx_out.use_id_token = resource_meta.use_id_token_as_bearer;
	refresh_ctx_out.resource_metadata_url = challenge.resource_metadata_url;
	refresh_ctx_out.scope = BuildScopeString(resource_meta);

	bool has_device_ep = !server_meta.device_authorization_endpoint.empty() &&
	                     server_meta.SupportsGrantType("urn:ietf:params:oauth:grant-type:device_code");
	bool has_auth_ep = !server_meta.authorization_endpoint.empty();

#ifdef __EMSCRIPTEN__
	// WASM: prefer PKCE (popup-based) if server supports it, fall back to device code.
	// PKCE in WASM uses a popup window opened by the main thread, with the auth code
	// flowing back via SharedArrayBuffer.
	if (has_auth_ep) {
		return PerformPKCEFlowImpl(challenge, resource_meta, server_meta, context);
	}
	if (has_device_ep) {
		return PerformDeviceCodeFlowImpl(challenge, resource_meta, server_meta, context);
	}
	throw IOException("VGI OAuth: server supports neither authorization_endpoint (PKCE) nor device_authorization_endpoint");
#endif

	if (flow == "device_code") {
		return PerformDeviceCodeFlowImpl(challenge, resource_meta, server_meta, context);
	}

	if (flow == "pkce") {
		if (!has_auth_ep) {
			throw IOException("VGI OAuth: PKCE flow requested but server has no authorization_endpoint");
		}
		return PerformPKCEFlowImpl(challenge, resource_meta, server_meta, context);
	}

	// Auto mode
	// Server only has device endpoint
	if (!has_auth_ep && has_device_ep) {
		VGI_STDERR_DEBUG("[VGI] oauth.auto_flow chose=device_code reason=no_authorization_endpoint\n");
		return PerformDeviceCodeFlowImpl(challenge, resource_meta, server_meta, context);
	}

	// Headless environment
	if (has_device_ep && IsHeadlessEnvironment()) {
		VGI_STDERR_DEBUG("[VGI] oauth.auto_flow chose=device_code reason=headless_environment\n");
		return PerformDeviceCodeFlowImpl(challenge, resource_meta, server_meta, context);
	}

	// Try PKCE, fallback to device code on socket bind failure
	if (has_auth_ep) {
		try {
			return PerformPKCEFlowImpl(challenge, resource_meta, server_meta, context);
		} catch (const IOException &e) {
			// Only fallback on socket bind/listen failures
			std::string msg = e.what();
			if (has_device_ep && msg.find("failed to bind local callback server") != std::string::npos) {
				VGI_STDERR_DEBUG("[VGI] oauth.auto_flow chose=device_code reason=pkce_bind_failed\n");
				return PerformDeviceCodeFlowImpl(challenge, resource_meta, server_meta, context);
			}
			throw;
		}
	}

	// No usable flow
	throw IOException("VGI OAuth: server has no supported authorization endpoints");
}

//===--------------------------------------------------------------------===//
// OAuth Callback Page HTML
//===--------------------------------------------------------------------===//

// Simple HTML entity escaping for user-provided strings in HTML content
static std::string HtmlEscape(const std::string &s) {
	std::string out;
	out.reserve(s.size());
	for (char c : s) {
		switch (c) {
		case '&': out += "&amp;"; break;
		case '<': out += "&lt;"; break;
		case '>': out += "&gt;"; break;
		case '"': out += "&quot;"; break;
		case '\'': out += "&#39;"; break;
		default: out += c;
		}
	}
	return out;
}

static const char *OAUTH_PAGE_STYLE = R"(
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: "Inter", system-ui, -apple-system, sans-serif;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
  }
  @media (prefers-color-scheme: light) {
    body { background-color: #faf8f0; color: #2c2c1e; }
    .card { background: white; border: 1px solid #f0ece0; }
    .icon-circle-success { background: #e8f5e0; }
    .icon-success { color: #4a7c23; }
    .icon-circle-error { background: #fde8e8; }
    .icon-error { color: #c53030; }
    .subtitle { color: #6b6b5a; }
    .resource { color: #2c2c1e; }
    .footer-text { color: #6b6b5a; }
    .footer-link { color: #4a7c23; }
    .footer-link:hover { color: #2d5016; }
  }
  @media (prefers-color-scheme: dark) {
    body { background-color: #1a1a0e; color: #f5f0e0; }
    .card { background: #252518; border: 1px solid #3a3a28; }
    .icon-circle-success { background: #2d501640; }
    .icon-success { color: #6ba034; }
    .icon-circle-error { background: #c5303020; }
    .icon-error { color: #fc8181; }
    .subtitle { color: #b8b0a0; }
    .resource { color: #f5f0e0; }
    .footer-text { color: #b8b0a0; }
    .footer-link { color: #6ba034; }
    .footer-link:hover { color: #4a7c23; }
  }
  .card {
    max-width: 560px;
    width: 100%;
    border-radius: 12px;
    padding: 2.5rem;
    box-shadow: 0 4px 24px rgba(0,0,0,0.06);
    display: flex;
    align-items: center;
    gap: 2rem;
  }
  .logos { display: flex; flex-direction: column; align-items: center; gap: 0.75rem; flex-shrink: 0; }
  .logo { width: 120px; height: 120px; }
  .logo-duckdb { width: 100px; }
  .content { text-align: left; }
  .status-row { display: flex; align-items: center; gap: 0.625rem; margin-bottom: 0.5rem; }
  .icon-circle {
    width: 36px; height: 36px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
  }
  .icon-circle svg { width: 20px; height: 20px; }
  h1 { font-size: 1.375rem; font-weight: 600; }
  .subtitle { font-size: 0.938rem; line-height: 1.5; }
  .resource { font-weight: 600; }
  .footer {
    margin-top: 2rem;
    font-size: 0.8125rem;
    text-align: center;
  }
  .footer-link {
    text-decoration: none;
    transition: color 0.15s;
  }
</style>
)";

static std::string OAuthSuccessPage(const std::string &resource_display, const std::string &resource_url) {
	auto escaped = HtmlEscape(resource_display);
	auto escaped_url = HtmlEscape(resource_url);
	return std::string(R"(<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authentication Successful</title>)") +
	       OAUTH_PAGE_STYLE +
	       R"(</head><body>
<div class="card">
  <div class="logos">
    <a href="https://query.farm"><img class="logo" src="https://vgi-rpc.query.farm/logo-hero.png" alt="VGI"></a>
    <a href="https://duckdb.org"><img class="logo-duckdb" src="https://duckdb.org/images/logo-dl/DuckDB_Logo-horizontal.svg" alt="DuckDB"></a>
  </div>
  <div class="content">
    <div class="status-row">
      <div class="icon-circle icon-circle-success">
        <svg class="icon-success" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/>
        </svg>
      </div>
      <h1>Authentication Successful</h1>
    </div>
    <p class="subtitle">Connected to <span class="resource">)" + escaped + R"(</span></p>
    <p class="subtitle" style="margin-top:0.25rem;font-size:0.8125rem;font-family:'JetBrains Mono','Fira Code',monospace;opacity:0.7;word-break:break-all;" title=")" + escaped_url + R"(">)" + escaped_url + R"(</p>
    <p class="subtitle" style="margin-top:0.75rem;">You can close this window.</p>
  </div>
</div>
<div class="footer">
  <span class="footer-text">&copy; 2026 &#x1F69C; <a href="https://query.farm" class="footer-link">Query.Farm LLC</a></span>
</div>
</body></html>)";
}

static std::string OAuthErrorPage(const std::string &message, const std::string &resource_display) {
	auto escaped_msg = HtmlEscape(message);
	auto escaped_resource = HtmlEscape(resource_display);
	return std::string(R"(<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authentication Failed</title>)") +
	       OAUTH_PAGE_STYLE +
	       R"(</head><body>
<div class="card">
  <div class="logos">
    <a href="https://query.farm"><img class="logo" src="https://vgi-rpc.query.farm/logo-hero.png" alt="VGI"></a>
    <a href="https://duckdb.org"><img class="logo-duckdb" src="https://duckdb.org/images/logo-dl/DuckDB_Logo-horizontal.svg" alt="DuckDB"></a>
  </div>
  <div class="content">
    <div class="status-row">
      <div class="icon-circle icon-circle-error">
        <svg class="icon-error" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/>
        </svg>
      </div>
      <h1>Authentication Failed</h1>
    </div>
    <p class="subtitle">)" + escaped_msg + R"(</p>
    <p class="subtitle" style="margin-top:0.5rem;">Resource: <span class="resource">)" + escaped_resource + R"(</span></p>
  </div>
</div>
<div class="footer">
  <span class="footer-text">&copy; 2026 &#x1F69C; <a href="https://query.farm" class="footer-link">Query.Farm LLC</a></span>
</div>
</body></html>)";
}

// Extract a display name for the resource: prefer resource_name, fall back to hostname from resource URL
static std::string GetResourceDisplayName(const OAuthResourceMetadata &meta) {
	if (!meta.resource_name.empty()) {
		return meta.resource_name;
	}
	if (!meta.resource.empty()) {
		// Extract hostname from URL
		auto scheme_end = meta.resource.find("://");
		if (scheme_end != std::string::npos) {
			auto host_start = scheme_end + 3;
			auto host_end = meta.resource.find('/', host_start);
			if (host_end == std::string::npos) {
				return meta.resource.substr(host_start);
			}
			return meta.resource.substr(host_start, host_end - host_start);
		}
	}
	return "VGI Service";
}

//===--------------------------------------------------------------------===//
// PKCE Flow
//===--------------------------------------------------------------------===//

#ifdef __EMSCRIPTEN__
static OAuthTokenSet PerformPKCEFlowImpl(const OAuthChallenge &challenge,
                                                const OAuthResourceMetadata &resource_meta,
                                                const OAuthServerMetadata &server_meta,
                                                ClientContext &context) {
	// WASM PKCE: all crypto and URL construction happens here in C++.
	// The only JS interaction is duckdb_wasm_open_auth_url() which opens a popup
	// on the main thread and blocks until the auth code comes back via SharedArrayBuffer.
	// No EM_ASM needed — all JS stubs are in duckdb-wasm's js-stubs.js.

	std::string client_id = resource_meta.client_id.empty() ? challenge.client_id : resource_meta.client_id;
	if (client_id.empty()) {
		throw IOException("VGI OAuth: no client_id found in WWW-Authenticate header or resource metadata");
	}
	// When the resource advertises its own token_endpoint (the server's PKCE
	// proxy), the proxy injects the configured client_secret upstream — we
	// must not also pass our local copy, since echoing it back over CORS
	// would defeat the purpose. Only carry the secret when no proxy is set.
	std::string client_secret = resource_meta.token_endpoint.empty() ? resource_meta.client_secret : std::string();

	// Generate PKCE parameters (uses duckdb_wasm_crypto_random + duckdb_wasm_sha256)
	auto code_verifier = GenerateCodeVerifier();
	auto code_challenge = ComputeCodeChallenge(code_verifier);
	auto state = GenerateState();

	// The redirect_uri must be on the same origin as the page hosting duckdb-wasm.
	char *origin_ptr = duckdb_wasm_get_page_origin();
	if (!origin_ptr) {
		throw IOException("Cannot determine page origin for OAuth redirect URI. "
		                  "Set globalThis._duckdb_page_origin in the worker before loading extensions.");
	}
	std::string redirect_uri = std::string(origin_ptr) + "/oauth-callback.html";
	free(origin_ptr);

	// Build authorization URL
	std::string auth_url = server_meta.authorization_endpoint +
	    "?response_type=code"
	    "&client_id=" + UrlEncode(client_id) +
	    "&redirect_uri=" + UrlEncode(redirect_uri) +
	    "&code_challenge=" + UrlEncode(code_challenge) +
	    "&code_challenge_method=S256"
	    "&state=" + UrlEncode(state);

	// Add scopes
	auth_url += "&scope=" + UrlEncode(BuildScopeString(resource_meta));

	DUCKDB_LOG_WARNING(context, "Authentication required for " + GetResourceDisplayName(resource_meta) +
	    ". Opening browser...");

	// Get timeout setting
	int64_t timeout_seconds = 120;
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_oauth_timeout_seconds", timeout_val)) {
		timeout_seconds = timeout_val.GetValue<int64_t>();
	}

	// Open popup and block until auth code comes back (or timeout)
	char *code_ptr = duckdb_wasm_open_auth_url(auth_url.c_str(), static_cast<int>(timeout_seconds * 1000));
	if (!code_ptr) {
		// Auth failed or cancelled — get error message
		char *err_ptr = duckdb_wasm_get_auth_error(0);
		std::string err_msg = err_ptr ? std::string(err_ptr) : "authentication failed or cancelled";
		if (err_ptr) free(err_ptr);
		throw IOException("VGI OAuth: %s", err_msg);
	}

	std::string code(code_ptr);
	free(code_ptr);

	VGI_STDERR_DEBUG("[VGI] oauth.pkce_wasm code_received\n");

	// Exchange code for tokens (uses duckdb-wasm's HTTP layer).
	// Use the server's token-proxy URL when advertised so the WASM build
	// doesn't need to hold the IdP's client_secret to satisfy Google.
	auto tokens = ExchangeCodeForTokens(context, ResolveTokenEndpoint(resource_meta, server_meta),
	                                     code, redirect_uri, code_verifier, client_id, client_secret);
	tokens.use_id_token = resource_meta.use_id_token_as_bearer;

	DUCKDB_LOG_WARNING(context, "Authentication successful.");
	return tokens;
}
#else
static OAuthTokenSet PerformPKCEFlowImpl(const OAuthChallenge &challenge,
                                                const OAuthResourceMetadata &resource_meta,
                                                const OAuthServerMetadata &server_meta,
                                                ClientContext &context) {
	// Get timeout setting
	int64_t timeout_seconds = 120;
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_oauth_timeout_seconds", timeout_val)) {
		timeout_seconds = timeout_val.GetValue<int64_t>();
	}

	VGI_STDERR_DEBUG("[VGI] oauth.pkce_flow resource_metadata=%s\n",
	                 challenge.resource_metadata_url.c_str());

	// Use client_id from resource metadata if available, otherwise from challenge
	std::string client_id = resource_meta.client_id.empty() ? challenge.client_id : resource_meta.client_id;
	if (client_id.empty()) {
		throw IOException("VGI OAuth: no client_id found in WWW-Authenticate header or resource metadata");
	}
	// When the proxy is in use, don't echo our local client_secret upstream —
	// the proxy injects the configured one server-side.
	std::string client_secret = resource_meta.token_endpoint.empty() ? resource_meta.client_secret : std::string();

	// Step 3: Generate PKCE parameters
	auto code_verifier = GenerateCodeVerifier();
	auto code_challenge = ComputeCodeChallenge(code_verifier);
	auto state = GenerateState();

	// Step 4: Start local callback server
	CPPHTTPLIB_NAMESPACE::Server svr;

	std::promise<std::string> code_promise;
	auto code_future = code_promise.get_future();
	std::string expected_state = state;

	auto resource_display = GetResourceDisplayName(resource_meta);
	auto resource_url = resource_meta.resource;

	svr.Get("/oauth-callback.html", [&code_promise, &expected_state, resource_display, resource_url](const CPPHTTPLIB_NAMESPACE::Request &req,
	                                                                         CPPHTTPLIB_NAMESPACE::Response &res) {
		auto state_param = req.get_param_value("state");
		auto code_param = req.get_param_value("code");
		auto error_param = req.get_param_value("error");

		if (!error_param.empty()) {
			auto error_desc = req.get_param_value("error_description");
			res.set_content(OAuthErrorPage(error_param + " — " + error_desc, resource_display), "text/html");
			try {
				code_promise.set_exception(std::make_exception_ptr(
				    IOException("VGI OAuth: authorization failed: %s - %s",
				                error_param, error_desc)));
			} catch (...) {} // promise already set
			return;
		}

		if (state_param != expected_state) {
			res.set_content(OAuthErrorPage("State mismatch — this may be a forged request.", resource_display), "text/html");
			try {
				code_promise.set_exception(std::make_exception_ptr(
				    IOException("VGI OAuth: state mismatch in callback")));
			} catch (...) {}
			return;
		}

		if (code_param.empty()) {
			res.set_content(OAuthErrorPage("No authorization code was received.", resource_display), "text/html");
			try {
				code_promise.set_exception(std::make_exception_ptr(
				    IOException("VGI OAuth: no authorization code in callback")));
			} catch (...) {}
			return;
		}

		res.set_content(OAuthSuccessPage(resource_display, resource_url), "text/html");
		try {
			code_promise.set_value(code_param);
		} catch (...) {} // promise already set
	});

	int port = svr.bind_to_any_port("127.0.0.1");
	if (port < 0) {
		throw IOException("VGI OAuth: failed to bind local callback server");
	}

	// Use "localhost" instead of "127.0.0.1" in the redirect URI. Azure AD
	// treats http://localhost specially — matching any port — but does NOT
	// extend that treatment to http://127.0.0.1.
	std::string redirect_uri = "http://localhost:" + std::to_string(port) + "/oauth-callback.html";

	// Start server in background thread
	std::thread server_thread([&svr]() {
		svr.listen_after_bind();
	});

	// Wait for server to be ready
	svr.wait_until_ready();

	// Step 5: Build authorization URL and open browser
	std::string auth_url = server_meta.authorization_endpoint +
	    "?response_type=code"
	    "&client_id=" + UrlEncode(client_id) +
	    "&redirect_uri=" + UrlEncode(redirect_uri) +
	    "&code_challenge=" + UrlEncode(code_challenge) +
	    "&code_challenge_method=S256"
	    "&state=" + UrlEncode(state);

	// Add scopes (default to "openid" if resource metadata doesn't specify any)
	auth_url += "&scope=" + UrlEncode(BuildScopeString(resource_meta));

	// NOTE: Do NOT add a &resource= parameter here. The resource parameter is
	// an OAuth 1.0/v1.0 concept (used by Azure AD v1.0 endpoints) and conflicts
	// with the v2.0 scope-based model. Azure AD v2.0 returns AADSTS9010010
	// ("resource parameter doesn't match requested scopes") when both are present.
	// Google and other standard OAuth 2.0 providers don't use it either.

	// Add prompt parameter if configured (controls account picker / re-auth behavior).
	// Valid values: none (default, omit parameter), login, select_account, consent.
	// See: https://openid.net/specs/openid-connect-core-1_0.html#AuthRequest
	{
		std::string prompt = "none";
		Value prompt_val;
		if (context.TryGetCurrentSetting("vgi_oauth_prompt", prompt_val)) {
			prompt = prompt_val.GetValue<std::string>();
		}
		if (prompt == "login" || prompt == "select_account" || prompt == "consent") {
			auth_url += "&prompt=" + UrlEncode(prompt);
		} else if (prompt != "none") {
			throw InvalidInputException(
			    "vgi_oauth_prompt must be 'none', 'login', 'select_account', or 'consent' (got '%s')", prompt);
		}
	}

	// Always print the URL so user can manually navigate if browser fails
	DUCKDB_LOG_WARNING(context, "Authentication required for " + GetResourceDisplayName(resource_meta) +
	    ". Opening browser...\nIf the browser doesn't open, visit this URL:\n" + auth_url);

	OpenBrowser(auth_url);

	// Step 6: Wait for callback with timeout
	OAuthTokenSet tokens;
	try {
		auto wait_status = code_future.wait_for(std::chrono::seconds(timeout_seconds));
		if (wait_status == std::future_status::timeout) {
			svr.stop();
			server_thread.join();
			throw IOException("VGI OAuth: authentication timed out after %lld seconds", timeout_seconds);
		}

		auto code = code_future.get(); // may throw if error was set

		// Stop callback server
		svr.stop();
		server_thread.join();

		VGI_STDERR_DEBUG("[VGI] oauth.code_received\n");

		// Step 7: Exchange code for tokens (via proxy if advertised)
		tokens = ExchangeCodeForTokens(context, ResolveTokenEndpoint(resource_meta, server_meta),
		                                code, redirect_uri, code_verifier, client_id, client_secret);
	} catch (...) {
		svr.stop();
		if (server_thread.joinable()) {
			server_thread.join();
		}
		throw;
	}

	tokens.use_id_token = resource_meta.use_id_token_as_bearer;

	DUCKDB_LOG_WARNING(context, "Authentication successful.");
	return tokens;
}
#endif // !__EMSCRIPTEN__

//===--------------------------------------------------------------------===//
// BearerTokenCatalogAuth
//===--------------------------------------------------------------------===//

BearerTokenCatalogAuth::BearerTokenCatalogAuth(std::string token) : token_(std::move(token)) {
}

std::string BearerTokenCatalogAuth::GetToken() {
	std::lock_guard<std::mutex> lock(mutex_);
	return token_;
}

std::string BearerTokenCatalogAuth::HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) {
	throw IOException("VGI HTTP authentication failed (HTTP 401): bearer token was rejected by the server. "
	                  "The static bearer_token provided at ATTACH time is not valid or has expired.");
}

void BearerTokenCatalogAuth::ClearTokens() {
	std::lock_guard<std::mutex> lock(mutex_);
	token_.clear();
}

//===--------------------------------------------------------------------===//
// OAuthCatalogAuth
//===--------------------------------------------------------------------===//

OAuthCatalogAuth::OAuthCatalogAuth() {
}

std::string OAuthCatalogAuth::GetToken() {
	std::lock_guard<std::mutex> lock(mutex_);
	if (state_.status != AuthState::Status::COMPLETE || !state_.token.IsValid()) {
		return "";
	}
	return state_.token.BearerToken();
}

void OAuthCatalogAuth::SeedRefreshToken(const std::string &refresh_token) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (state_.status == AuthState::Status::IDLE) {
		state_.token.refresh_token = refresh_token;
	}
}

bool OAuthCatalogAuth::IsExplicitlyConfigured() const {
	// OAuth is "configured" only when the user supplied a refresh token at
	// ATTACH time (oauth_refresh_token option) OR has already completed an
	// auth flow. A freshly-constructed default OAuthCatalogAuth — the
	// fallback path when neither bearer_token nor oauth_refresh_token was
	// set — returns false so the 401-handler in vgi_http_client.cpp can
	// surface a clean "no credential" diagnostic instead of launching an
	// OAuth discovery flow against an empty challenge URL.
	std::lock_guard<std::mutex> lock(mutex_);
	if (!state_.token.refresh_token.empty()) {
		return true;
	}
	if (state_.status == AuthState::Status::COMPLETE && state_.token.IsValid()) {
		return true;
	}
	return false;
}

std::string OAuthCatalogAuth::HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) {
	// Check if OAuth is enabled
	Value oauth_enabled_val;
	if (context.TryGetCurrentSetting("vgi_oauth_enabled", oauth_enabled_val)) {
		if (!oauth_enabled_val.GetValue<bool>()) {
			throw IOException("VGI HTTP authentication required (HTTP 401) but vgi_oauth_enabled is false. "
			                  "Set vgi_oauth_enabled=true to enable OAuth authentication.");
		}
	}

	std::unique_lock<std::mutex> lock(mutex_);

	// Helper: store successful auth result in state (must hold lock).
	// Helpers below assume `lock` is HELD when called. They publish the
	// new state, clear the IN_PROGRESS owner, and notify all waiters.
	auto StoreSuccess = [&](OAuthTokenSet tokens, const OAuthRefreshContext &rctx) -> std::string {
		state_.token = std::move(tokens);
		state_.refresh_ctx = rctx;
		state_.status = AuthState::Status::COMPLETE;
		state_.owner = std::thread::id();
		state_.cv.notify_all();
		return state_.token.BearerToken();
	};

	// Helper: store failure in state (must hold lock).
	auto StoreFailed = [&](const std::string &error_msg) {
		state_.status = AuthState::Status::FAILED;
		state_.error_message = error_msg;
		state_.owner = std::thread::id();
		state_.cv.notify_all();
	};

	// Helper: attempt refresh, then fall back to full auth flow.
	// Called with lock released. Re-acquires lock on success/failure.
	auto RefreshOrFullAuth = [&](std::string refresh_token, OAuthRefreshContext refresh_ctx) -> std::string {
		// Attempt token refresh if we have a refresh_token
		if (!refresh_token.empty()) {
			// Discover metadata if we have a token but no refresh context
			// (e.g., seeded via ATTACH oauth_refresh_token option)
			if (refresh_ctx.token_endpoint.empty()) {
				try {
					refresh_ctx = DiscoverRefreshContext(challenge, context);
					VGI_STDERR_DEBUG("[VGI] oauth.discovered_refresh_context endpoint=%s\n",
					                 refresh_ctx.token_endpoint.c_str());
				} catch (const std::exception &e) {
					DUCKDB_LOG_WARNING(context,
					    "VGI OAuth: failed to discover auth server metadata for token refresh, "
					    "falling back to interactive auth: " + std::string(e.what()));
				}
			}
			if (!refresh_ctx.token_endpoint.empty()) {
				try {
					VGI_STDERR_DEBUG("[VGI] oauth.attempting_refresh\n");
					auto tokens = AttemptTokenRefresh(refresh_ctx, refresh_token, context);
					// Preserve old refresh_token if response omits new one (Google behavior)
					if (tokens.refresh_token.empty()) {
						tokens.refresh_token = refresh_token;
					}
					lock.lock();
					return StoreSuccess(std::move(tokens), refresh_ctx);
				} catch (const std::exception &e) {
					// Do NOT silently fall through to PerformAuthFlow.
					// When token refresh fails (e.g., invalid_grant), rethrow
					// immediately so the caller sees the actual refresh failure.
					// This is critical for WASM/cupola where the origin isn't a
					// registered OAuth client — falling through would mask the
					// real error behind a misleading "token exchange failed" message.
					std::string refresh_err = e.what();
					if (refresh_err.find("invalid_grant") != std::string::npos) {
						VGI_STDERR_DEBUG("[VGI] oauth.clearing_stale_refresh_token\n");
					}
					lock.lock();
					// Clear stale refresh token on invalid_grant
					if (refresh_err.find("invalid_grant") != std::string::npos) {
						state_.token.refresh_token.clear();
					}
					StoreFailed(refresh_err);
					throw;
				} catch (...) {
					// Non-std::exception throw (rare, but possible from
					// nested code). Without this catch the IN_PROGRESS
					// state would never be cleared and cv.notify_all
					// never fires — every waiter hangs forever.
					lock.lock();
					StoreFailed("unknown error during token refresh");
					throw;
				}
			}
		}

		// Full auth flow — only reached when no refresh_token was available
		try {
			OAuthRefreshContext new_refresh_ctx;
			auto tokens = PerformAuthFlow(challenge, context, new_refresh_ctx);
			lock.lock();
			return StoreSuccess(std::move(tokens), new_refresh_ctx);
		} catch (const std::exception &e) {
			lock.lock();
			StoreFailed(e.what());
			throw;
		} catch (...) {
			// Same hang-prevention as the refresh catch above.
			lock.lock();
			StoreFailed("unknown error during interactive auth flow");
			throw;
		}
	};

	const auto this_thread = std::this_thread::get_id();

	switch (state_.status) {
	case AuthState::Status::IDLE:
	case AuthState::Status::FAILED: {
		state_.status = AuthState::Status::IN_PROGRESS;
		state_.owner = this_thread;
		state_.error_message.clear();
		std::string refresh_token = state_.token.refresh_token;
		OAuthRefreshContext refresh_ctx = state_.refresh_ctx;
		lock.unlock();
		return RefreshOrFullAuth(std::move(refresh_token), std::move(refresh_ctx));
	}

	case AuthState::Status::IN_PROGRESS: {
		// Same-thread re-entry: the auth flow itself produced a 401 that
		// landed back in *this* HandleUnauthorized (e.g., the discovery
		// endpoint requires auth from this same auth object, or the token
		// endpoint host returns 401 mid-refresh). Waiting on cv would
		// deadlock against ourselves. Throw cleanly instead.
		if (state_.owner == this_thread) {
			throw IOException(
			    "VGI OAuth: nested 401 inside the auth flow on the same thread; aborting "
			    "to avoid self-deadlock. Likely cause: the token-endpoint or "
			    "discovery endpoint also returned 401.");
		}

		// Another thread is already doing the auth flow — wait
		VGI_STDERR_DEBUG("[VGI] oauth.waiting_for_auth\n");
		state_.cv.wait(lock, [this]() {
			return state_.status != AuthState::Status::IN_PROGRESS;
		});

		if (state_.status == AuthState::Status::COMPLETE && state_.token.IsValid()) {
			return state_.token.BearerToken();
		}
		if (state_.status == AuthState::Status::IDLE) {
			throw IOException("VGI OAuth: tokens were cleared during authentication, please retry");
		}
		throw IOException("VGI OAuth: authentication failed: %s", state_.error_message);
	}

	case AuthState::Status::COMPLETE: {
		// Token exists but server returned 401 — token may be stale.
		state_.status = AuthState::Status::IN_PROGRESS;
		state_.owner = this_thread;
		state_.error_message.clear();
		std::string refresh_token = state_.token.refresh_token;
		OAuthRefreshContext refresh_ctx = state_.refresh_ctx;
		lock.unlock();
		return RefreshOrFullAuth(std::move(refresh_token), std::move(refresh_ctx));
	}
	}

	return "";
}

void OAuthCatalogAuth::ClearTokens() {
	std::lock_guard<std::mutex> lock(mutex_);
	state_.token = OAuthTokenSet();
	state_.refresh_ctx = OAuthRefreshContext();
	state_.status = AuthState::Status::IDLE;
	state_.owner = std::thread::id();
	state_.error_message.clear();
	state_.cv.notify_all();
}

bool OAuthCatalogAuth::GetTokenInfo(TokenInfo &info) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (state_.status != AuthState::Status::COMPLETE) {
		return false;
	}
	info.has_refresh_token = !state_.token.refresh_token.empty();
	auto now = std::chrono::steady_clock::now();
	if (state_.token.expires_at > std::chrono::steady_clock::time_point()) {
		info.has_expires = true;
		info.expires_in_seconds = std::chrono::duration_cast<std::chrono::seconds>(
		    state_.token.expires_at - now).count();
	} else {
		info.has_expires = false;
		info.expires_in_seconds = 0;
	}
	return true;
}

bool OAuthCatalogAuth::GetTokenSetCopy(OAuthTokenSet &out) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (state_.status != AuthState::Status::COMPLETE) {
		return false;
	}
	out = state_.token;
	return true;
}


std::string ExtractOrigin(const std::string &url) {
	// Extract scheme://host:port from a URL
	auto scheme_end = url.find("://");
	if (scheme_end == std::string::npos) {
		return url;
	}
	auto host_start = scheme_end + 3;
	auto path_start = url.find('/', host_start);
	if (path_start == std::string::npos) {
		return url;
	}
	return url.substr(0, path_start);
}

OAuthRefreshContext DiscoverRefreshContext(const OAuthChallenge &challenge, ClientContext &context) {
	auto resource_meta = FetchResourceMetadata(context, challenge.resource_metadata_url);

	std::string client_id = resource_meta.client_id.empty() ? challenge.client_id : resource_meta.client_id;
	if (client_id.empty()) {
		throw IOException("VGI OAuth: no client_id in challenge or resource metadata");
	}

	auto server_meta = FetchAuthServerMetadata(context, resource_meta.authorization_servers[0]);

	OAuthRefreshContext ctx;
	ctx.token_endpoint = ResolveTokenEndpoint(resource_meta, server_meta);
	ctx.client_id = client_id;
	// See refresh_ctx_out comment in PerformAuthFlow: skip the local secret
	// when the server's token-proxy is in use.
	ctx.client_secret = resource_meta.token_endpoint.empty() ? resource_meta.client_secret : std::string();
	ctx.use_id_token = resource_meta.use_id_token_as_bearer;
	ctx.resource_metadata_url = challenge.resource_metadata_url;
	ctx.scope = BuildScopeString(resource_meta);
	return ctx;
}

//===--------------------------------------------------------------------===//
// Token Refresh
//===--------------------------------------------------------------------===//

OAuthTokenSet AttemptTokenRefresh(const OAuthRefreshContext &ctx,
                                  const std::string &refresh_token,
                                  ClientContext &context) {
	EnforceHttpsUrl(ctx.token_endpoint, "token endpoint (refresh)");

	std::string body = "grant_type=refresh_token"
	                   "&refresh_token=" + UrlEncode(refresh_token) +
	                   "&client_id=" + UrlEncode(ctx.client_id);
	if (!ctx.client_secret.empty()) {
		body += "&client_secret=" + UrlEncode(ctx.client_secret);
	}
	if (!ctx.scope.empty()) {
		body += "&scope=" + UrlEncode(ctx.scope);
	}

	auto resp = PostTokenRequestRaw(context, ctx.token_endpoint, body);
	if (resp.status_code != 200) {
		// Surface the request shape through the exception. The IdP's
		// "invalid_grant" / "AADSTS9002313" body doesn't tell you which
		// parameter is wrong; the request shape does. Refresh tokens are
		// fingerprinted (length + first/last 4 chars) — IOException text
		// travels to log manager / telemetry / JDBC clients, so a leaked
		// refresh_token is a credential leak.
		throw IOException(
		    "VGI OAuth: token refresh failed (HTTP %d): %s\n"
		    "  request:\n"
		    "    token_endpoint=%s\n"
		    "    grant_type=refresh_token\n"
		    "    refresh_token=%s\n"
		    "    client_id=%s\n"
		    "    client_secret=%s\n"
		    "    scope=%s\n"
		    "    resource_metadata_url=%s\n"
		    "    use_id_token_as_bearer=%s",
		    resp.status_code, resp.body,
		    ctx.token_endpoint,
		    DebugSecret(refresh_token),
		    ctx.client_id.empty() ? "<empty>" : ctx.client_id,
		    ctx.client_secret.empty() ? "<not sent>" : DebugSecret(ctx.client_secret),
		    ctx.scope.empty() ? "<not sent>" : ctx.scope,
		    ctx.resource_metadata_url.empty() ? "<not set>" : ctx.resource_metadata_url,
		    ctx.use_id_token ? "true" : "false");
	}

	auto tokens = ParseTokenResponse(resp.body, "refresh token response");
	tokens.use_id_token = ctx.use_id_token;
	return tokens;
}

} // namespace vgi
} // namespace duckdb
