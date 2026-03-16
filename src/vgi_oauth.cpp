#include "vgi_oauth.hpp"

#include <cctype>
#include <future>
#include <thread>

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "yyjson.hpp"

#include "vgi_logging.hpp"
#include "duckdb/logging/logger.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#include <shellapi.h>
#endif

// DuckDB-vendored httplib for the local callback server
#include "httplib.hpp"

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

std::string GenerateCodeVerifier() {
	// RFC 7636: 43-128 characters from unreserved set [A-Z] / [a-z] / [0-9] / "-" / "." / "_" / "~"
	// Generate 32 random bytes and base64url-encode them (gives 43 chars)
	unsigned char buf[32];
	if (RAND_bytes(buf, sizeof(buf)) != 1) {
		throw IOException("VGI OAuth: failed to generate random bytes for code verifier");
	}
	return Base64UrlEncode(buf, sizeof(buf));
}

std::string ComputeCodeChallenge(const std::string &verifier) {
	// S256: BASE64URL(SHA256(code_verifier))
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
	if (url.substr(0, 8) != "https://" && url.substr(0, 16) != "http://127.0.0.1" &&
	    url.substr(0, 16) != "http://localhost") {
		throw IOException("VGI OAuth: %s URL must use HTTPS: %s", context_name, url);
	}
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

//===--------------------------------------------------------------------===//
// Browser Opening
//===--------------------------------------------------------------------===//

void OpenBrowser(const std::string &url) {
#if defined(__APPLE__)
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

	auto resp = PostTokenRequest(context, token_endpoint, body, "token exchange");
	return ParseTokenResponse(resp, "token response");
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

OAuthTokenSet VgiTokenManager::PerformDeviceCodeFlow(const OAuthChallenge &challenge,
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
	std::string scope_str;
	if (!resource_meta.scopes_supported.empty()) {
		for (size_t i = 0; i < resource_meta.scopes_supported.size(); i++) {
			if (i > 0) scope_str += " ";
			scope_str += resource_meta.scopes_supported[i];
		}
	} else {
		scope_str = "openid";
	}

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

	// Use condition_variable for interruptible sleep
	std::mutex poll_mutex;
	std::condition_variable poll_cv;

	while (true) {
		// Interruptible sleep
		{
			std::unique_lock<std::mutex> lock(poll_mutex);
			poll_cv.wait_for(lock, std::chrono::seconds(interval));
		}

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

		// Parse error JSON from any non-200 response (RFC 8628 says 400, but
		// some servers like Google use 428 for authorization_pending)
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

//===--------------------------------------------------------------------===//
// Auth Flow Orchestrator
//===--------------------------------------------------------------------===//

OAuthTokenSet VgiTokenManager::PerformAuthFlow(const OAuthChallenge &challenge,
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
	refresh_ctx_out.token_endpoint = server_meta.token_endpoint;
	refresh_ctx_out.client_id = client_id;
	refresh_ctx_out.client_secret = resource_meta.client_secret;
	refresh_ctx_out.use_id_token = resource_meta.use_id_token_as_bearer;
	refresh_ctx_out.resource_metadata_url = challenge.resource_metadata_url;
	// Build scope string
	if (!resource_meta.scopes_supported.empty()) {
		std::string scope_str;
		for (size_t i = 0; i < resource_meta.scopes_supported.size(); i++) {
			if (i > 0) scope_str += " ";
			scope_str += resource_meta.scopes_supported[i];
		}
		refresh_ctx_out.scope = scope_str;
	} else {
		refresh_ctx_out.scope = "openid";
	}

	bool has_device_ep = !server_meta.device_authorization_endpoint.empty() &&
	                     server_meta.SupportsGrantType("urn:ietf:params:oauth:grant-type:device_code");
	bool has_auth_ep = !server_meta.authorization_endpoint.empty();

	if (flow == "device_code") {
		return PerformDeviceCodeFlow(challenge, resource_meta, server_meta, context);
	}

	if (flow == "pkce") {
		if (!has_auth_ep) {
			throw IOException("VGI OAuth: PKCE flow requested but server has no authorization_endpoint");
		}
		return PerformPKCEFlow(challenge, resource_meta, server_meta, context);
	}

	// Auto mode
	// Server only has device endpoint
	if (!has_auth_ep && has_device_ep) {
		VGI_STDERR_DEBUG("[VGI] oauth.auto_flow chose=device_code reason=no_authorization_endpoint\n");
		return PerformDeviceCodeFlow(challenge, resource_meta, server_meta, context);
	}

	// Headless environment
	if (has_device_ep && IsHeadlessEnvironment()) {
		VGI_STDERR_DEBUG("[VGI] oauth.auto_flow chose=device_code reason=headless_environment\n");
		return PerformDeviceCodeFlow(challenge, resource_meta, server_meta, context);
	}

	// Try PKCE, fallback to device code on socket bind failure
	if (has_auth_ep) {
		try {
			return PerformPKCEFlow(challenge, resource_meta, server_meta, context);
		} catch (const IOException &e) {
			// Only fallback on socket bind/listen failures
			std::string msg = e.what();
			if (has_device_ep && msg.find("failed to bind local callback server") != std::string::npos) {
				VGI_STDERR_DEBUG("[VGI] oauth.auto_flow chose=device_code reason=pkce_bind_failed\n");
				return PerformDeviceCodeFlow(challenge, resource_meta, server_meta, context);
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

OAuthTokenSet VgiTokenManager::PerformPKCEFlow(const OAuthChallenge &challenge,
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
	std::string client_secret = resource_meta.client_secret;

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

	svr.Get("/callback", [&code_promise, &expected_state, resource_display, resource_url](const CPPHTTPLIB_NAMESPACE::Request &req,
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
	std::string redirect_uri = "http://localhost:" + std::to_string(port) + "/callback";

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
	{
		std::string scope_str;
		if (!resource_meta.scopes_supported.empty()) {
			for (size_t i = 0; i < resource_meta.scopes_supported.size(); i++) {
				if (i > 0) scope_str += " ";
				scope_str += resource_meta.scopes_supported[i];
			}
		} else {
			scope_str = "openid";
		}
		auth_url += "&scope=" + UrlEncode(scope_str);
	}

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

		// Step 7: Exchange code for tokens
		tokens = ExchangeCodeForTokens(context, server_meta.token_endpoint,
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

//===--------------------------------------------------------------------===//
// VgiTokenManager
//===--------------------------------------------------------------------===//

VgiTokenManager &VgiTokenManager::Instance() {
	static VgiTokenManager instance;
	return instance;
}

std::string VgiTokenManager::ExtractOrigin(const std::string &url) {
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

std::string VgiTokenManager::GetToken(const std::string &origin) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = auth_states_.find(origin);
	if (it == auth_states_.end()) {
		return "";
	}

	auto &state = it->second;
	if (state.status != AuthState::Status::COMPLETE || !state.token.IsValid()) {
		return "";
	}

	return state.token.BearerToken();
}

std::string VgiTokenManager::HandleUnauthorized(const std::string &origin,
                                                 const OAuthChallenge &challenge,
                                                 ClientContext &context) {
	// Check if OAuth is enabled
	Value oauth_enabled_val;
	if (context.TryGetCurrentSetting("vgi_oauth_enabled", oauth_enabled_val)) {
		if (!oauth_enabled_val.GetValue<bool>()) {
			throw IOException("VGI HTTP authentication required (HTTP 401) but vgi_oauth_enabled is false. "
			                  "Set vgi_oauth_enabled=true to enable OAuth authentication.");
		}
	}

	std::unique_lock<std::mutex> lock(mutex_);

	// Helper: store successful auth result in state (must hold lock), then persist.
	// Returns bearer token. Unlocks, persists, and returns.
	auto StoreAndPersist = [&](OAuthTokenSet tokens, const OAuthRefreshContext &rctx) -> std::string {
		auto &s = auth_states_[origin]; // re-lookup: map entry may have been recreated
		s.token = std::move(tokens);
		s.refresh_ctx = rctx;
		s.status = AuthState::Status::COMPLETE;
		s.cv.notify_all();
		auto bearer = s.token.BearerToken();
		auto persist_tokens = s.token;
		lock.unlock();
		PersistRefreshToken(context, origin, persist_tokens, rctx);
		return bearer;
	};

	// Helper: store failure in state (must hold lock).
	auto StoreFailed = [&](const std::string &error_msg) {
		auto &s = auth_states_[origin]; // re-lookup
		s.status = AuthState::Status::FAILED;
		s.error_message = error_msg;
		s.cv.notify_all();
	};

	// Helper: attempt refresh, then fall back to full auth flow.
	// Called with lock released. Re-acquires lock on success/failure.
	auto RefreshOrFullAuth = [&](std::string refresh_token, OAuthRefreshContext refresh_ctx) -> std::string {
		// Try loading persisted refresh token if we don't have one in memory
		if (refresh_token.empty()) {
			try {
				LoadPersistedRefreshToken(context, origin, refresh_ctx, refresh_token);
			} catch (...) {}
		}

		// Attempt token refresh if we have a refresh_token
		if (!refresh_token.empty() && !refresh_ctx.token_endpoint.empty()) {
			try {
				VGI_STDERR_DEBUG("[VGI] oauth.attempting_refresh origin=%s\n", origin.c_str());
				auto tokens = AttemptTokenRefresh(refresh_ctx, refresh_token, context);
				// Preserve old refresh_token if response omits new one (Google behavior)
				if (tokens.refresh_token.empty()) {
					tokens.refresh_token = refresh_token;
				}
				lock.lock();
				return StoreAndPersist(std::move(tokens), refresh_ctx);
			} catch (const std::exception &e) {
				VGI_STDERR_DEBUG("[VGI] oauth.refresh_failed origin=%s error=%s\n",
				                 origin.c_str(), e.what());
			}
		}

		// Full auth flow
		try {
			OAuthRefreshContext new_refresh_ctx;
			auto tokens = PerformAuthFlow(challenge, context, new_refresh_ctx);
			lock.lock();
			return StoreAndPersist(std::move(tokens), new_refresh_ctx);
		} catch (const std::exception &e) {
			lock.lock();
			StoreFailed(e.what());
			throw;
		}
	};
	auto &state = auth_states_[origin];

	switch (state.status) {
	case AuthState::Status::IDLE:
	case AuthState::Status::FAILED: {
		state.status = AuthState::Status::IN_PROGRESS;
		state.error_message.clear();
		std::string refresh_token = state.token.refresh_token;
		OAuthRefreshContext refresh_ctx = state.refresh_ctx;
		lock.unlock();
		return RefreshOrFullAuth(std::move(refresh_token), std::move(refresh_ctx));
	}

	case AuthState::Status::IN_PROGRESS: {
		// Another thread is already doing the auth flow — wait
		VGI_STDERR_DEBUG("[VGI] oauth.waiting_for_auth origin=%s\n", origin.c_str());
		state.cv.wait(lock, [&state]() {
			return state.status != AuthState::Status::IN_PROGRESS;
		});

		if (state.status == AuthState::Status::COMPLETE && state.token.IsValid()) {
			return state.token.BearerToken();
		}
		if (state.status == AuthState::Status::IDLE) {
			// Tokens were cleared while we were waiting — caller should retry
			throw IOException("VGI OAuth: tokens were cleared for %s, please retry", origin);
		}
		throw IOException("VGI OAuth: authentication failed for %s: %s",
		                  origin, state.error_message);
	}

	case AuthState::Status::COMPLETE: {
		// Token exists but server returned 401 — token may be stale.
		state.status = AuthState::Status::IN_PROGRESS;
		state.error_message.clear();
		std::string refresh_token = state.token.refresh_token;
		OAuthRefreshContext refresh_ctx = state.refresh_ctx;
		lock.unlock();
		return RefreshOrFullAuth(std::move(refresh_token), std::move(refresh_ctx));
	}
	}

	return "";
}

void VgiTokenManager::ClearTokens(const std::string &origin) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = auth_states_.find(origin);
	if (it != auth_states_.end()) {
		// Reset to IDLE so next request triggers fresh auth.
		// Don't erase — other threads may hold references to the CV.
		it->second.token = OAuthTokenSet();
		it->second.refresh_ctx = OAuthRefreshContext();
		it->second.status = AuthState::Status::IDLE;
		it->second.error_message.clear();
		it->second.cv.notify_all();
	}
}

void VgiTokenManager::ClearTokens(ClientContext &context, const std::string &origin) {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = auth_states_.find(origin);
		if (it != auth_states_.end()) {
			it->second.token = OAuthTokenSet();
			it->second.refresh_ctx = OAuthRefreshContext();
			it->second.status = AuthState::Status::IDLE;
			it->second.error_message.clear();
			it->second.cv.notify_all();
		}
	}
	RemovePersistedToken(context, origin);
}

void VgiTokenManager::ClearAllTokens(ClientContext &context) {
	std::vector<std::string> origins;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto &entry : auth_states_) {
			origins.push_back(entry.first);
			entry.second.token = OAuthTokenSet();
			entry.second.refresh_ctx = OAuthRefreshContext();
			entry.second.status = AuthState::Status::IDLE;
			entry.second.error_message.clear();
			entry.second.cv.notify_all();
		}
	}
	for (const auto &o : origins) {
		RemovePersistedToken(context, o);
	}
}

std::vector<std::string> VgiTokenManager::GetAllOrigins() {
	std::lock_guard<std::mutex> lock(mutex_);
	std::vector<std::string> origins;
	for (const auto &entry : auth_states_) {
		origins.push_back(entry.first);
	}
	return origins;
}

bool VgiTokenManager::GetTokenInfo(const std::string &origin, TokenInfo &info) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = auth_states_.find(origin);
	if (it == auth_states_.end() || it->second.status != AuthState::Status::COMPLETE) {
		return false;
	}
	auto &state = it->second;
	info.has_refresh_token = !state.token.refresh_token.empty();
	auto now = std::chrono::steady_clock::now();
	if (state.token.expires_at > std::chrono::steady_clock::time_point()) {
		info.has_expires = true;
		info.expires_in_seconds = std::chrono::duration_cast<std::chrono::seconds>(
		    state.token.expires_at - now).count();
	} else {
		info.has_expires = false;
		info.expires_in_seconds = 0;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Token Refresh
//===--------------------------------------------------------------------===//

OAuthTokenSet VgiTokenManager::AttemptTokenRefresh(const OAuthRefreshContext &ctx,
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
		throw IOException("VGI OAuth: token refresh failed (HTTP %d): %s",
		                  resp.status_code, resp.body);
	}

	auto tokens = ParseTokenResponse(resp.body, "refresh token response");
	tokens.use_id_token = ctx.use_id_token;
	return tokens;
}

//===--------------------------------------------------------------------===//
// Secret Persistence
//===--------------------------------------------------------------------===//

std::string VgiTokenManager::SecretNameForOrigin(const std::string &origin) {
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char *>(origin.data()), origin.size(), hash);

	// Take first 16 bytes → 32 hex chars
	static const char hex[] = "0123456789abcdef";
	std::string hex_str;
	hex_str.reserve(32);
	for (int i = 0; i < 16; i++) {
		hex_str += hex[(hash[i] >> 4) & 0xF];
		hex_str += hex[hash[i] & 0xF];
	}
	return "vgi_oauth_" + hex_str;
}

void VgiTokenManager::PersistRefreshToken(ClientContext &context, const std::string &origin,
                                           const OAuthTokenSet &tokens, const OAuthRefreshContext &ctx) {
	if (tokens.refresh_token.empty()) {
		return;
	}

	try {
		// Check vgi_oauth_persist setting
		Value persist_val;
		if (context.TryGetCurrentSetting("vgi_oauth_persist", persist_val)) {
			if (!persist_val.GetValue<bool>()) {
				return;
			}
		}

		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

		auto secret_name = SecretNameForOrigin(origin);
		vector<string> scope_vec = {origin};

		auto BuildSecret = [&]() {
			auto s = make_uniq<KeyValueSecret>(scope_vec, "vgi_oauth_refresh_token", "config", secret_name);
			s->secret_map["refresh_token"] = Value(tokens.refresh_token);
			s->secret_map["token_endpoint"] = Value(ctx.token_endpoint);
			s->secret_map["client_id"] = Value(ctx.client_id);
			s->secret_map["client_secret"] = Value(ctx.client_secret);
			s->secret_map["scope"] = Value(ctx.scope);
			s->secret_map["use_id_token"] = Value(ctx.use_id_token ? "true" : "false");
			s->secret_map["resource_metadata_url"] = Value(ctx.resource_metadata_url);
			s->secret_map["origin"] = Value(origin);
			s->redact_keys.insert("refresh_token");
			s->redact_keys.insert("client_secret");
			return s;
		};

		// Try persistent first, fall back to temporary
		try {
			if (secret_manager.PersistentSecretsEnabled()) {
				secret_manager.RegisterSecret(transaction, BuildSecret(),
				                              OnCreateConflict::REPLACE_ON_CONFLICT,
				                              SecretPersistType::PERSISTENT);
				VGI_STDERR_DEBUG("[VGI] oauth.persisted_refresh_token origin=%s type=persistent\n", origin.c_str());
				return;
			}
		} catch (const std::exception &e) {
			VGI_STDERR_DEBUG("[VGI] oauth.persist_failed origin=%s error=%s (trying temporary)\n",
			                 origin.c_str(), e.what());
		}

		secret_manager.RegisterSecret(transaction, BuildSecret(),
		                              OnCreateConflict::REPLACE_ON_CONFLICT,
		                              SecretPersistType::TEMPORARY);
		VGI_STDERR_DEBUG("[VGI] oauth.persisted_refresh_token origin=%s type=temporary\n", origin.c_str());
	} catch (const std::exception &e) {
		VGI_STDERR_DEBUG("[VGI] oauth.persist_refresh_token_failed origin=%s error=%s\n",
		                 origin.c_str(), e.what());
	}
}

bool VgiTokenManager::LoadPersistedRefreshToken(ClientContext &context, const std::string &origin,
                                                 OAuthRefreshContext &ctx_out, std::string &refresh_token_out) {
	try {
		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto secret_name = SecretNameForOrigin(origin);

		auto entry = secret_manager.GetSecretByName(transaction, secret_name);
		if (!entry) {
			return false;
		}

		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*entry->secret);

		auto rt = kv_secret.TryGetValue("refresh_token");
		if (rt.IsNull() || rt.ToString().empty()) {
			return false;
		}
		refresh_token_out = rt.ToString();

		auto te = kv_secret.TryGetValue("token_endpoint");
		if (!te.IsNull()) ctx_out.token_endpoint = te.ToString();

		auto ci = kv_secret.TryGetValue("client_id");
		if (!ci.IsNull()) ctx_out.client_id = ci.ToString();

		auto cs = kv_secret.TryGetValue("client_secret");
		if (!cs.IsNull()) ctx_out.client_secret = cs.ToString();

		auto sc = kv_secret.TryGetValue("scope");
		if (!sc.IsNull()) ctx_out.scope = sc.ToString();

		auto ui = kv_secret.TryGetValue("use_id_token");
		if (!ui.IsNull()) ctx_out.use_id_token = (ui.ToString() == "true");

		auto rm = kv_secret.TryGetValue("resource_metadata_url");
		if (!rm.IsNull()) ctx_out.resource_metadata_url = rm.ToString();

		VGI_STDERR_DEBUG("[VGI] oauth.loaded_persisted_token origin=%s\n", origin.c_str());
		return true;
	} catch (const std::exception &e) {
		VGI_STDERR_DEBUG("[VGI] oauth.load_persisted_token_failed origin=%s error=%s\n",
		                 origin.c_str(), e.what());
		return false;
	}
}

void VgiTokenManager::RemovePersistedToken(ClientContext &context, const std::string &origin) {
	try {
		auto &secret_manager = SecretManager::Get(context);
		auto secret_name = SecretNameForOrigin(origin);
		secret_manager.DropSecretByName(context, secret_name, OnEntryNotFound::RETURN_NULL);
		VGI_STDERR_DEBUG("[VGI] oauth.removed_persisted_token origin=%s\n", origin.c_str());
	} catch (const std::exception &e) {
		VGI_STDERR_DEBUG("[VGI] oauth.remove_persisted_token_failed origin=%s error=%s\n",
		                 origin.c_str(), e.what());
	}
}

} // namespace vgi
} // namespace duckdb
