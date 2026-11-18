// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// orchard://<workspace>/<deployment> locations. See vgi_orchard.hpp and
// docs/orchard_locations.md.
#include "vgi_orchard.hpp"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <future>
#include <thread>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "yyjson.hpp"

#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_oauth_store.hpp"
#include "vgi_settings_defaults.hpp"

#ifndef __EMSCRIPTEN__
#include "httplib.hpp"
#endif

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

namespace {

constexpr const char *kDefaultIssuer = "https://auth.queryfarm.cloud";
constexpr const char *kDefaultDeskOrigin = "https://desk.queryfarm.cloud";
// How long an issuer's published Field Address patterns are trusted before
// they are fetched again.
constexpr std::chrono::seconds kFieldPatternTtl {3600};
constexpr const char *kDeviceCodeGrant = "urn:ietf:params:oauth:grant-type:device_code";
// Refresh ahead of expiry: one thread refreshes once less than this is left,
// the others keep the still-valid Pass.
constexpr std::chrono::seconds kProactiveRefreshWindow {300};
// Once a sign-in is persisted, store errors must surface (an auto-mode store
// that silently drops a write leaves a stale token for another process).
constexpr const char *kStoreMode = "persistent";

struct YyjsonDoc {
	yyjson_doc *doc;
	explicit YyjsonDoc(const std::string &body) : doc(yyjson_read(body.c_str(), body.size(), 0)) {
	}
	~YyjsonDoc() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
	YyjsonDoc(const YyjsonDoc &) = delete;
	YyjsonDoc &operator=(const YyjsonDoc &) = delete;
	yyjson_val *Root() const {
		auto root = doc ? yyjson_doc_get_root(doc) : nullptr;
		return root && yyjson_is_obj(root) ? root : nullptr;
	}
	std::string Str(const char *key) const {
		auto root = Root();
		auto val = root ? yyjson_obj_get(root, key) : nullptr;
		return val && yyjson_is_str(val) ? std::string(yyjson_get_str(val)) : std::string();
	}
	bool Int(const char *key, int64_t &out) const {
		auto root = Root();
		auto val = root ? yyjson_obj_get(root, key) : nullptr;
		if (!val || !yyjson_is_num(val)) {
			return false;
		}
		out = yyjson_is_real(val) ? static_cast<int64_t>(yyjson_get_real(val)) : yyjson_get_int(val);
		return true;
	}
};

bool EnvFlag(const char *name) {
	auto v = std::getenv(name);
	return v && std::string(v) == "1";
}

// A test-hook override of a fixed origin: https, or loopback http. Trailing
// slashes are dropped.
std::string OriginFromEnv(const char *env_name, const char *fallback) {
	auto v = std::getenv(env_name);
	if (!v || !*v) {
		return fallback;
	}
	std::string value(v);
	while (!value.empty() && value.back() == '/') {
		value.pop_back();
	}
	if (!StringUtil::StartsWith(StringUtil::Lower(value), "https://") && !IsLoopbackHttpUrl(value)) {
		throw InvalidInputException("%s (a test hook) must be an https:// URL or loopback http:// (got '%s')",
		                            env_name, value);
	}
	return value;
}

ErrorInfo OrchardInfo(const char *subtype, const char *stage, const std::string &endpoint,
                      const std::string &oauth_error = std::string()) {
	ErrorInfo info(subtype);
	info.Set(error_key::kOAuthStage, stage).Set(error_key::kOAuthEndpoint, endpoint);
	if (!oauth_error.empty()) {
		info.Set(error_key::kOAuthError, oauth_error);
	}
	return info;
}

// "error: description", or "HTTP <status>" when the body carries neither.
std::string DescribeOAuthError(int status, const std::string &body) {
	auto code = OAuthErrorCode(body);
	auto desc = OAuthErrorDescription(body);
	if (code.empty()) {
		return StringUtil::Format("HTTP %d", status);
	}
	return desc.empty() ? code : code + ": " + desc;
}

// The device/browser prompt, on stderr: the CLI shows DuckDB warnings only
// after the statement ends, too late for a sign-in that blocks it. Also in the
// DuckDB log at INFO (not WARNING, which the CLI would echo a second time).
void Prompt(ClientContext &context, const std::string &msg) {
	DUCKDB_LOG_INFO(context, msg);
	fprintf(stderr, "[VGI] %s\n", msg.c_str());
	fflush(stderr);
}

std::string FormBody(std::initializer_list<std::pair<const char *, std::string>> fields) {
	std::string body;
	for (const auto &field : fields) {
		if (!body.empty()) {
			body += "&";
		}
		body += field.first;
		body += "=";
		body += UrlEncode(field.second);
	}
	return body;
}

bool IsTransient(int status, const std::string &body) {
	return status >= 500 || status == 429 || OAuthErrorCode(body) == "temporarily_unavailable";
}

} // namespace

//===--------------------------------------------------------------------===//
// Location parsing and validation
//===--------------------------------------------------------------------===//

std::string OrchardLocation::ToString() const {
	return "orchard://" + workspace + "/" + deployment;
}

bool IsValidOrchardSlug(const std::string &slug) {
	if (slug.empty() || slug.size() > 63) {
		return false;
	}
	auto alnum = [](char c) {
		return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
	};
	if (!alnum(slug.front()) || !alnum(slug.back())) {
		return false;
	}
	for (char c : slug) {
		if (!alnum(c) && c != '-') {
			return false;
		}
	}
	return true;
}

OrchardLocation ParseOrchardLocation(const std::string &location) {
	static const std::string kScheme = "orchard://";
	auto usage = [&](const std::string &why) {
		return BinderException(ErrorInfo(error_subtype::kInvalidAttachOption).Set(error_key::kAttachOption, "location"),
		                       StringUtil::Format("Invalid Orchard LOCATION '%s': %s. Expected "
		                                          "orchard://<workspace>/<deployment>, where both are slugs of 1-63 "
		                                          "lowercase letters, digits and dashes that start and end with a "
		                                          "letter or digit",
		                                          location, why));
	};
	if (!StringUtil::StartsWith(StringUtil::Lower(location), kScheme)) {
		throw usage("it does not start with orchard://");
	}
	auto rest = location.substr(kScheme.size());
	if (rest.find_first_of("?#@\\") != std::string::npos) {
		throw usage("query strings, fragments, userinfo and backslashes are not allowed");
	}
	auto slash = rest.find('/');
	if (slash == std::string::npos) {
		throw usage("the deployment is missing");
	}
	OrchardLocation out;
	out.workspace = rest.substr(0, slash);
	out.deployment = rest.substr(slash + 1);
	if (out.deployment.find('/') != std::string::npos) {
		throw usage("it has more than two path parts");
	}
	if (!IsValidOrchardSlug(out.workspace)) {
		throw usage(out.workspace.empty() ? std::string("the workspace is empty")
		                                  : "'" + out.workspace + "' is not a valid workspace slug");
	}
	if (!IsValidOrchardSlug(out.deployment)) {
		throw usage(out.deployment.empty() ? std::string("the deployment is empty")
		                                   : "'" + out.deployment + "' is not a valid deployment slug");
	}
	return out;
}

std::string OrchardIssuer() {
	return OriginFromEnv("VGI_ORCHARD_ISSUER", kDefaultIssuer);
}

std::string OrchardDeskOrigin() {
	return OriginFromEnv("VGI_ORCHARD_DESK_ORIGIN", kDefaultDeskOrigin);
}

std::string OrchardResource(const OrchardLocation &location) {
	return OrchardDeskOrigin() + "/" + location.workspace + "/" + location.deployment;
}

namespace {

bool IsDnsLabel(const std::string &label) {
	if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') {
		return false;
	}
	return label.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-") == std::string::npos;
}

// `*.<suffix>`: exactly one label before the suffix; `**.<suffix>`: one or more.
bool FieldHostMatches(const std::string &host, const std::string &pattern) {
	bool single;
	std::string suffix;
	if (StringUtil::StartsWith(pattern, "**.")) {
		single = false;
		suffix = pattern.substr(2);
	} else if (StringUtil::StartsWith(pattern, "*.")) {
		single = true;
		suffix = pattern.substr(1);
	} else {
		return false;
	}
	if (host.size() <= suffix.size() || !StringUtil::EndsWith(host, suffix)) {
		return false;
	}
	auto labels = StringUtil::Split(host.substr(0, host.size() - suffix.size()), '.');
	if (labels.empty() || (single && labels.size() != 1)) {
		return false;
	}
	// StringUtil::Split drops empty pieces, so check for them directly.
	const std::string prefix = host.substr(0, host.size() - suffix.size());
	if (prefix.front() == '.' || prefix.back() == '.' || prefix.find("..") != std::string::npos) {
		return false;
	}
	for (auto &label : labels) {
		if (!IsDnsLabel(label)) {
			return false;
		}
	}
	return true;
}

} // namespace

const std::vector<std::string> &OrchardDefaultFieldPatterns() {
	// Orchard's own domain may nest; a Fly.io app host is exactly one label
	// under fly.dev (random names on Fly's shared wildcard stay out of CT).
	static const std::vector<std::string> defaults {"**.field.queryfarm.cloud", "*.fly.dev"};
	return defaults;
}

std::vector<std::string> ValidateOrchardFieldPatterns(const std::vector<std::string> &published) {
	if (published.empty() || published.size() > 16) {
		return {};
	}
	std::vector<std::string> out;
	for (auto &pattern : published) {
		std::string suffix;
		if (StringUtil::StartsWith(pattern, "**.")) {
			suffix = pattern.substr(3);
		} else if (StringUtil::StartsWith(pattern, "*.")) {
			suffix = pattern.substr(2);
		} else {
			return {};
		}
		if (pattern.size() > 253 || suffix.empty() || suffix.front() == '.' || suffix.back() == '.' ||
		    suffix.find("..") != std::string::npos) {
			return {};
		}
		auto labels = StringUtil::Split(suffix, '.');
		if (labels.size() < 2) {
			return {};
		}
		for (auto &label : labels) {
			if (!IsDnsLabel(label)) {
				return {};
			}
		}
		out.push_back(pattern);
	}
	return out;
}

std::string ValidateOrchardFieldAddress(const std::string &vgi_location) {
	return ValidateOrchardFieldAddress(vgi_location, OrchardDefaultFieldPatterns());
}

std::string ValidateOrchardFieldAddress(const std::string &vgi_location, const std::vector<std::string> &patterns) {
	auto refuse = [&](const std::string &why) {
		return IOException(ErrorInfo(error_subtype::kAuthFailed).Url(vgi_location),
		                   "VGI Orchard: refusing the Field Address '%s' from the Front Desk: %s", vgi_location, why);
	};
	if (vgi_location.empty()) {
		throw refuse("it is empty");
	}
	std::string value = vgi_location;
	if (value.back() == '/') {
		value.pop_back(); // "https://host/" has an empty path
	}
	if (EnvFlag("VGI_TEST_ALLOW_LOOPBACK_DELEGATION") && IsLoopbackHttpUrl(value) &&
	    value.find('/', 7) == std::string::npos) {
		return value;
	}
	static const std::string kHttps = "https://";
	if (!StringUtil::StartsWith(StringUtil::Lower(value), kHttps)) {
		throw refuse("it must be https://");
	}
	auto authority = value.substr(kHttps.size());
	if (authority.find_first_of("/?#") != std::string::npos) {
		throw refuse("it must not have a path, query or fragment");
	}
	if (authority.find('@') != std::string::npos) {
		throw refuse("it must not carry userinfo");
	}
	if (authority.empty() || authority.front() == '[') {
		throw refuse("it must name a host, not an IP literal");
	}
	std::string host = authority;
	std::string port;
	auto colon = authority.find(':');
	if (colon != std::string::npos) {
		host = authority.substr(0, colon);
		port = authority.substr(colon + 1);
		if (port.empty() || port.size() > 5 ||
		    port.find_first_not_of("0123456789") != std::string::npos || std::stoi(port) < 1 ||
		    std::stoi(port) > 65535) {
			throw refuse("its port is invalid");
		}
	}
	host = StringUtil::Lower(host);
	if (host.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789.-") != std::string::npos) {
		throw refuse("its host has invalid characters");
	}
	if (host.find_first_not_of("0123456789.") == std::string::npos) {
		throw refuse("it must name a host, not an IP literal");
	}
	bool allowed = false;
	for (const auto &pattern : patterns) {
		if (FieldHostMatches(host, pattern)) {
			allowed = true;
			break;
		}
	}
	if (!allowed) {
		std::string list;
		for (auto &pattern : patterns) {
			list += (list.empty() ? "" : ", ") + pattern;
		}
		throw refuse("its host is not under an allowed Field Address pattern (" + list + ")");
	}
	std::string canonical = "https://" + host;
	if (!port.empty() && port != "443") {
		canonical += ":" + port;
	}
	return canonical;
}

namespace {

// The issuer's published Field Address patterns, fetched from its metadata
// and cached per process. Any failure falls back to the built-ins, which are
// the stricter choice.
std::vector<std::string> OrchardFieldPatterns(ClientContext &context, const std::string &issuer) {
	struct Entry {
		std::vector<std::string> patterns;
		std::chrono::steady_clock::time_point fetched;
	};
	static std::mutex cache_mutex;
	static std::unordered_map<std::string, Entry> cache;
	const auto now = std::chrono::steady_clock::now();
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		auto it = cache.find(issuer);
		if (it != cache.end() && now - it->second.fetched < kFieldPatternTtl) {
			return it->second.patterns;
		}
	}
	std::vector<std::string> patterns;
	try {
		auto meta = FetchAuthServerMetadata(context, issuer);
		patterns = ValidateOrchardFieldPatterns(meta.vgi_field_address_patterns);
		if (patterns.empty() && !meta.vgi_field_address_patterns.empty()) {
			DUCKDB_LOG_WARNING(context, "VGI Orchard: " + issuer +
			                                " published unusable vgi_field_address_patterns; using the built-in hosts");
		}
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARNING(context, "VGI Orchard: could not read " + issuer +
		                                " metadata for Field Address patterns (" + ex.what() +
		                                "); using the built-in hosts");
	}
	if (patterns.empty()) {
		patterns = OrchardDefaultFieldPatterns();
	}
	std::lock_guard<std::mutex> lock(cache_mutex);
	cache[issuer] = Entry {patterns, now};
	return patterns;
}

} // namespace

//===--------------------------------------------------------------------===//
// OrchardCatalogAuth
//===--------------------------------------------------------------------===//

OrchardCatalogAuth::OrchardCatalogAuth(OrchardLocation location, std::string profile, std::string cache_mode)
    : location_(std::move(location)), issuer_(OrchardIssuer()), resource_(OrchardResource(location_)),
      cache_mode_(std::move(cache_mode)) {
	if (profile.empty()) {
		profile = "default";
	}
	binding_.issuer = issuer_;
	binding_.resource = resource_;
	binding_.client_id = kOrchardClientId;
	binding_.token_endpoint = issuer_ + "/token";
	// One stored sign-in per (profile, issuer, Desk resource, client): every
	// catalog attaching the same deployment under one profile shares it.
	store_key_ = "orchard\x1f" + profile + "\x1f" + issuer_ + "\x1f" + resource_ + "\x1f" + kOrchardClientId;
}

std::string OrchardCatalogAuth::FieldAddress() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return field_address_;
}

void OrchardCatalogAuth::AcceptTokenResponse(ClientContext &context, const std::string &body, bool initial) {
	YyjsonDoc doc(body);
	if (!doc.Root()) {
		throw IOException(OrchardInfo(error_subtype::kOAuthFailed, initial ? "token_exchange" : "refresh",
		                              issuer_ + "/token"),
		                  "VGI Orchard: the Front Desk's token response is not a JSON object");
	}
	auto access_token = doc.Str("access_token");
	if (access_token.empty()) {
		throw IOException(OrchardInfo(error_subtype::kOAuthFailed, initial ? "token_exchange" : "refresh",
		                              issuer_ + "/token"),
		                  "VGI Orchard: the Front Desk's token response has no access_token");
	}
	auto refresh_token = doc.Str("refresh_token");
	auto vgi_location = doc.Str("vgi_location");
	int64_t expires_in = 0;
	bool has_expiry = doc.Int("expires_in", expires_in);

	std::string field;
	if (!vgi_location.empty()) {
		field = ValidateOrchardFieldAddress(vgi_location, OrchardFieldPatterns(context, issuer_));
	} else if (initial) {
		throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "token_exchange", issuer_ + "/token"),
		                  "VGI Orchard: the Front Desk's token response for %s has no vgi_location (Field "
		                  "Address); the deployment may not be running",
		                  location_.ToString());
	}

	std::lock_guard<std::mutex> lock(mutex_);
	if (!initial && !field.empty() && field != field_address_) {
		// MVP: one Field Address for the life of the attach.
		DUCKDB_LOG_WARNING(context, "VGI Orchard: " + location_.ToString() + " moved to " + field +
		                                "; this catalog keeps using " + field_address_ + " until it is re-attached");
	}
	if (initial) {
		field_address_ = field;
		// Test hook: spoil the first Pass so the first RPC draws a 401 and the
		// refresh-and-retry path runs against a live worker.
		if (EnvFlag("VGI_TEST_ORCHARD_TAMPER_FIRST_PASS")) {
			access_token += "x";
		}
	}
	token_.access_token = std::move(access_token);
	token_.identity = ParseIdTokenClaims(token_.access_token); // the Pass's claims, unverified, for display
	// Rotated on every refresh: always keep the newest.
	if (!refresh_token.empty()) {
		token_.refresh_token = std::move(refresh_token);
	}
	// Test hook: a shorter Pass lifetime, so refreshes happen mid-session.
	if (auto ttl = std::getenv("VGI_TEST_ORCHARD_PASS_TTL_SECONDS")) {
		auto cap = std::atoll(ttl);
		if (cap > 0 && (!has_expiry || cap < expires_in)) {
			expires_in = cap;
			has_expiry = true;
		}
	}
	token_.expires_at = has_expiry ? std::chrono::steady_clock::now() + std::chrono::seconds(expires_in)
	                               : std::chrono::steady_clock::time_point();
	dead_message_.clear();
}

void OrchardCatalogAuth::RefreshLocked(ClientContext &context, bool initial) {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!dead_message_.empty()) {
			throw IOException(ErrorInfo(error_subtype::kAuthFailed), "%s", dead_message_);
		}
	}
	// Rotation is shared with every process using the same stored sign-in:
	// under the store's cross-process lease, use the newest stored token, and
	// write the successor back before the lease is released. A rotated token
	// presented more than 30 s later kills the whole family.
	std::unique_ptr<OAuthCredentialLease> lease;
	if (persist_) {
		lease = AcquireOAuthCredentialLease(store_key_, kStoreMode, /*allow_linux=*/true);
		std::string stored;
		if (LoadOAuthRefreshToken(store_key_, binding_, kStoreMode, stored, nullptr, /*allow_linux=*/true)) {
			std::lock_guard<std::mutex> lock(mutex_);
			if (stored != token_.refresh_token) {
				VGI_STDERR_DEBUG("[VGI] orchard.store adopt_newer_token location=%s\n", location_.ToString().c_str());
				token_.refresh_token = stored;
			}
			stored_entry_ = true;
		} else if (stored_entry_) {
			// Forgotten elsewhere (vgi_oauth_logout in another process): keep this
			// session in memory, but never resurrect the stored sign-in.
			VGI_STDERR_DEBUG("[VGI] orchard.store entry_gone location=%s\n", location_.ToString().c_str());
			persist_ = false;
			lease.reset();
		}
	}
	std::string refresh_token;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		refresh_token = token_.refresh_token;
	}
	const std::string endpoint = issuer_ + "/token";
	if (refresh_token.empty()) {
		throw IOException(OrchardInfo(error_subtype::kAuthFailed, "refresh", endpoint),
		                  "VGI Orchard: no Renewal Token for %s. Sign in again: DETACH and re-run ATTACH.",
		                  location_.ToString());
	}
	const auto body = FormBody({{"grant_type", "refresh_token"},
	                            {"refresh_token", refresh_token},
	                            {"client_id", kOrchardClientId},
	                            {"resource", resource_}});
	// Retrying the same token is safe: within 30 s the Front Desk returns the
	// same successor.
	const int delays[] = {1, 2, 4};
	for (int attempt = 0;; attempt++) {
		OAuthFormResponse resp;
		std::string transport_error;
		try {
			resp = PostOAuthForm(context, endpoint, body);
		} catch (const std::exception &e) {
			transport_error = RawMessageOf(e);
		}
		if (transport_error.empty() && resp.status_code == 200) {
			AcceptTokenResponse(context, resp.body, initial);
			VGI_STDERR_DEBUG("[VGI] orchard.refresh ok location=%s\n", location_.ToString().c_str());
			if (persist_) {
				PersistCurrentToken(context);
			}
			return;
		}
		const bool transient = !transport_error.empty() || IsTransient(resp.status_code, resp.body);
		if (transient && attempt < 3) {
			VGI_STDERR_DEBUG("[VGI] orchard.refresh transient attempt=%d\n", attempt + 1);
			std::this_thread::sleep_for(std::chrono::seconds(delays[attempt]));
			continue;
		}
		if (!transport_error.empty()) {
			throw IOException(OrchardInfo(error_subtype::kTransportFailure, "refresh", endpoint),
			                  "VGI Orchard: refreshing the access token for %s failed: %s", location_.ToString(),
			                  transport_error);
		}
		auto code = OAuthErrorCode(resp.body);
		auto detail = DescribeOAuthError(resp.status_code, resp.body);
		if (code == "invalid_grant") {
			if (persist_) {
				// Revoked, signed out, expired or reuse-detected: the stored entry
				// (which is the token just presented: it was re-read under the lease)
				// is dead too.
				try {
					DeleteOAuthRefreshToken(store_key_, kStoreMode, /*allow_linux=*/true);
					VGI_STDERR_DEBUG("[VGI] orchard.store deleted_after_invalid_grant location=%s\n",
					                 location_.ToString().c_str());
				} catch (const std::exception &e) {
					DUCKDB_LOG_WARNING(context, "VGI Orchard: could not delete the stored sign-in: " + RawMessageOf(e));
				}
				stored_entry_ = false;
			}
			std::lock_guard<std::mutex> lock(mutex_);
			token_ = OAuthTokenSet();
			dead_message_ = StringUtil::Format(
			    "VGI Orchard: the sign-in for %s is no longer valid (%s). Sign in again: DETACH and re-run "
			    "ATTACH.",
			    location_.ToString(), detail);
			throw IOException(OrchardInfo(error_subtype::kAuthFailed, "refresh", endpoint, code), "%s",
			                  dead_message_);
		}
		if (code == "invalid_target") {
			throw IOException(OrchardInfo(error_subtype::kAuthFailed, "refresh", endpoint, code),
			                  "VGI Orchard: the Front Desk does not recognise %s as a deployment you can use (%s)",
			                  location_.ToString(), detail);
		}
		throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "refresh", endpoint, code),
		                  "VGI Orchard: refreshing the access token for %s failed (%s)", location_.ToString(),
		                  detail);
	}
}

void OrchardCatalogAuth::PersistCurrentToken(ClientContext &context) {
	std::string token;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		token = token_.refresh_token;
	}
	try {
		StoreOAuthRefreshToken(store_key_, binding_, kStoreMode, token, /*allow_linux=*/true);
		stored_entry_ = true;
		VGI_STDERR_DEBUG("[VGI] orchard.store saved location=%s\n", location_.ToString().c_str());
	} catch (const std::exception &e) {
		// A stale stored token would later be presented by another process and
		// kill the family, so drop the entry and stop persisting.
		DUCKDB_LOG_WARNING(context, "VGI Orchard: could not save the sign-in, it is kept in memory only: " +
		                                RawMessageOf(e));
		try {
			DeleteOAuthRefreshToken(store_key_, kStoreMode, /*allow_linux=*/true);
		} catch (...) {
		}
		persist_ = false;
		stored_entry_ = false;
	}
}

std::string OrchardCatalogAuth::SignIn(ClientContext &context, const std::string &refresh_token) {
	std::lock_guard<std::mutex> refresh_lock(refresh_mutex_);
	if (!refresh_token.empty()) {
		{
			std::lock_guard<std::mutex> lock(mutex_);
			token_.refresh_token = refresh_token;
		}
		try {
			RefreshLocked(context, /*initial=*/true);
		} catch (const std::exception &e) {
			// Natively a failed refresh never falls back to a browser: a batch job
			// must not hang on an invisible sign-in.
			auto info = ExtraInfoOf(e);
			auto it = info.find(error_key::kOAuthError);
			if (it != info.end() && it->second == "invalid_grant") {
				std::lock_guard<std::mutex> lock(mutex_);
				dead_message_.clear();
				throw IOException(OrchardInfo(error_subtype::kAuthFailed, "refresh", issuer_ + "/token",
				                              "invalid_grant"),
				                  "VGI Orchard: the oauth_refresh_token given to ATTACH for %s was rejected by the "
				                  "Front Desk (it is expired, revoked or already used). Re-run ATTACH without "
				                  "oauth_refresh_token to sign in, or pass a current token.",
				                  location_.ToString());
			}
			throw;
		}
		// A refresh response carries vgi_location too; it is required here.
		std::lock_guard<std::mutex> lock(mutex_);
		if (field_address_.empty()) {
			throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "refresh", issuer_ + "/token"),
			                  "VGI Orchard: the Front Desk's token response for %s has no vgi_location (Field "
			                  "Address); the deployment may not be running",
			                  location_.ToString());
		}
		return field_address_;
	}
	// No explicit token: the stored sign-in, if this build and oauth_cache allow it.
	persist_ = false;
	std::string store_note;
#ifndef __EMSCRIPTEN__
	std::string store_problem;
	if (cache_mode_ == "auto" || cache_mode_ == "persistent") {
		try {
			// Throws for oauth_cache='persistent' when there is no usable store
			// (kStoreMode surfaces a lock directory that fails its checks); in
			// auto mode either means memory only, said in the sign-in prompt.
			if (AcquireOAuthCredentialLease(store_key_, kStoreMode, /*allow_linux=*/true)) {
				persist_ = true;
			}
		} catch (const std::exception &e) {
			if (cache_mode_ == "persistent") {
				throw;
			}
			store_problem = RawMessageOf(e);
		}
	}
	store_note = persist_ ? "The sign-in will be saved in " + DescribeOAuthStore(cache_mode_, true) +
	                            "; SELECT * FROM vgi_oauth_logout() forgets it."
	                      : "The sign-in will not be saved (it lasts until this process exits): " +
	                            (store_problem.empty() ? DescribeOAuthStore(cache_mode_, true) : store_problem) +
	                            ".";
	if (persist_) {
		std::string stored;
		bool have = false;
		try {
			have = LoadOAuthRefreshToken(store_key_, binding_, kStoreMode, stored, nullptr, /*allow_linux=*/true);
		} catch (const std::exception &e) {
			if (cache_mode_ == "persistent") {
				throw;
			}
			DUCKDB_LOG_WARNING(context, "VGI Orchard: the credential store failed, signing in without it: " +
			                                RawMessageOf(e));
			persist_ = false;
		}
		if (have) {
			{
				std::lock_guard<std::mutex> lock(mutex_);
				token_.refresh_token = stored;
			}
			stored_entry_ = true;
			try {
				RefreshLocked(context, /*initial=*/true);
				VGI_STDERR_DEBUG("[VGI] orchard.signin source=store location=%s\n", location_.ToString().c_str());
				std::lock_guard<std::mutex> lock(mutex_);
				if (!field_address_.empty()) {
					return field_address_;
				}
				throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "refresh", issuer_ + "/token"),
				                  "VGI Orchard: the Front Desk's token response for %s has no vgi_location (Field "
				                  "Address); the deployment may not be running",
				                  location_.ToString());
			} catch (const std::exception &e) {
				auto info = ExtraInfoOf(e);
				auto it = info.find(error_key::kOAuthError);
				if (it == info.end() || it->second != "invalid_grant") {
					throw;
				}
				// The stored sign-in is dead (and already deleted): sign in again.
				std::lock_guard<std::mutex> lock(mutex_);
				dead_message_.clear();
				token_ = OAuthTokenSet();
				Prompt(context, "The saved sign-in for " + location_.ToString() +
				                    " is no longer valid (revoked, signed out or expired); signing in again.");
			}
		}
	}
	// Test hook: stand in for the interactive sign-in with a Renewal Token read
	// from a file. Unlike oauth_refresh_token, the result is persisted exactly
	// as an interactive sign-in's would be.
	if (auto seed_path = std::getenv("VGI_TEST_ORCHARD_SEED_TOKEN_FILE")) {
		std::ifstream in(seed_path);
		std::string seed;
		std::getline(in, seed);
		while (!seed.empty() && (seed.back() == '\r' || seed.back() == ' ')) {
			seed.pop_back();
		}
		if (seed.empty()) {
			throw IOException("VGI Orchard: VGI_TEST_ORCHARD_SEED_TOKEN_FILE names an empty or unreadable file");
		}
		{
			std::lock_guard<std::mutex> lock(mutex_);
			token_.refresh_token = seed;
		}
		RefreshLocked(context, /*initial=*/true); // persists the successor
		VGI_STDERR_DEBUG("[VGI] orchard.signin source=seed_file location=%s\n", location_.ToString().c_str());
		std::lock_guard<std::mutex> lock(mutex_);
		return field_address_;
	}
#endif
	SignInInteractive(context, store_note);
	if (persist_) {
		auto lease = AcquireOAuthCredentialLease(store_key_, kStoreMode, /*allow_linux=*/true);
		PersistCurrentToken(context);
	}
	std::lock_guard<std::mutex> lock(mutex_);
	interactive_ = true;
	return field_address_;
}

std::string OrchardCatalogAuth::GetToken() {
	std::lock_guard<std::mutex> lock(mutex_);
	return token_.IsValid() ? token_.access_token : std::string();
}

std::string OrchardCatalogAuth::GetTokenForRequest(ClientContext &context) {
	{
		std::unique_lock<std::mutex> lock(mutex_);
		if (!dead_message_.empty()) {
			throw IOException(ErrorInfo(error_subtype::kAuthFailed), "%s", dead_message_);
		}
		if (!token_.access_token.empty() &&
		    TokenStillFresh(token_.expires_at, std::chrono::steady_clock::now(), kProactiveRefreshWindow)) {
			return token_.access_token;
		}
		if (token_.IsValid()) {
			// Inside the proactive window but still valid: one thread refreshes,
			// the others keep using this Pass. (Lock order is refresh_mutex_ then
			// mutex_; a try_lock here cannot deadlock.)
			std::unique_lock<std::mutex> refresh_lock(refresh_mutex_, std::try_to_lock);
			if (!refresh_lock.owns_lock()) {
				return token_.access_token;
			}
			lock.unlock();
			try {
				RefreshLocked(context, /*initial=*/false);
			} catch (...) {
				lock.lock();
				if (!dead_message_.empty() || !token_.IsValid()) {
					throw;
				}
				return token_.access_token; // transient failure: the old Pass is still good
			}
			lock.lock();
			return token_.access_token;
		}
	}
	// Expired (or about to): everyone waits for the single refresh.
	std::lock_guard<std::mutex> refresh_lock(refresh_mutex_);
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (token_.IsValid()) {
			return token_.access_token;
		}
	}
	RefreshLocked(context, /*initial=*/false);
	std::lock_guard<std::mutex> lock(mutex_);
	return token_.access_token;
}

std::string OrchardCatalogAuth::HandleRejectedToken(const std::string &rejected_token, ClientContext &context) {
	std::lock_guard<std::mutex> refresh_lock(refresh_mutex_);
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!dead_message_.empty()) {
			throw IOException(ErrorInfo(error_subtype::kAuthFailed), "%s", dead_message_);
		}
		// Another thread already replaced the rejected Pass: retry with that.
		if (token_.IsValid() && token_.access_token != rejected_token) {
			return token_.access_token;
		}
	}
	RefreshLocked(context, /*initial=*/false);
	std::lock_guard<std::mutex> lock(mutex_);
	return token_.access_token;
}

std::string OrchardCatalogAuth::HandleUnauthorized(const OAuthChallenge &, ClientContext &context) {
	return HandleRejectedToken(GetToken(), context);
}

void OrchardCatalogAuth::ClearTokens() {
	std::lock_guard<std::mutex> refresh_lock(refresh_mutex_);
	if (persist_) {
		// Forget the stored sign-in too; errors propagate so vgi_oauth_logout()
		// never reports success while a saved token remains.
		auto lease = AcquireOAuthCredentialLease(store_key_, kStoreMode, /*allow_linux=*/true);
		DeleteOAuthRefreshToken(store_key_, kStoreMode, /*allow_linux=*/true);
		persist_ = false;
		stored_entry_ = false;
	}
	std::lock_guard<std::mutex> lock(mutex_);
	token_ = OAuthTokenSet();
	dead_message_ = "VGI Orchard: signed out of " + location_.ToString() + ". Sign in again: DETACH and re-run ATTACH.";
}

bool OrchardCatalogAuth::GetTokenInfo(TokenInfo &info) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (token_.access_token.empty()) {
		return false;
	}
	info.has_refresh_token = !token_.refresh_token.empty();
	info.has_expires = token_.expires_at != std::chrono::steady_clock::time_point();
	info.expires_in_seconds =
	    info.has_expires
	        ? std::chrono::duration_cast<std::chrono::seconds>(token_.expires_at - std::chrono::steady_clock::now())
	              .count()
	        : 0;
	return true;
}

bool OrchardCatalogAuth::GetTokenSetCopy(OAuthTokenSet &out) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (token_.access_token.empty()) {
		return false;
	}
	out = token_;
	return true;
}

bool OrchardCatalogAuth::WasInteractive() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return interactive_;
}

//===--------------------------------------------------------------------===//
// Interactive sign-in
//===--------------------------------------------------------------------===//

#ifdef __EMSCRIPTEN__
std::string OrchardCatalogAuth::SignInInteractive(ClientContext &, const std::string &) {
	throw NotImplementedException("VGI Orchard: interactive sign-in for orchard:// is not available in DuckDB-WASM "
	                              "yet; pass oauth_refresh_token");
}
#else

namespace {

int64_t OAuthTimeoutSeconds(ClientContext &context) {
	Value v;
	if (context.TryGetCurrentSetting("vgi_oauth_timeout_seconds", v)) {
		return v.GetValue<int64_t>();
	}
	return defaults::OAUTH_TIMEOUT_SECONDS;
}

std::string OAuthFlowSetting(ClientContext &context) {
	std::string flow = defaults::OAUTH_FLOW;
	Value v;
	if (context.TryGetCurrentSetting("vgi_oauth_flow", v)) {
		flow = v.GetValue<std::string>();
	}
	if (flow != "auto" && flow != "device_code" && flow != "pkce") {
		throw InvalidInputException("vgi_oauth_flow must be 'auto', 'device_code', or 'pkce' (got '%s')", flow);
	}
	return flow;
}

struct OrchardCallbackBindFailure : public IOException {
	OrchardCallbackBindFailure() : IOException("VGI Orchard: failed to bind the local sign-in callback server") {
	}
};

} // namespace

std::string OrchardCatalogAuth::SignInInteractive(ClientContext &context, const std::string &store_note) {
	const auto flow = OAuthFlowSetting(context);
	const std::string token_endpoint = issuer_ + "/token";
	const std::string display = location_.ToString();
	const int64_t timeout_seconds = OAuthTimeoutSeconds(context);

	auto device_flow = [&]() {
		const std::string device_endpoint = issuer_ + "/device_authorization";
		auto resp = PostOAuthForm(context, device_endpoint,
		                          FormBody({{"client_id", kOrchardClientId}, {"resource", resource_}}));
		if (resp.status_code != 200) {
			throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "device_code", device_endpoint,
			                              OAuthErrorCode(resp.body)),
			                  "VGI Orchard: starting device sign-in for %s failed (%s)", display,
			                  DescribeOAuthError(resp.status_code, resp.body));
		}
		YyjsonDoc doc(resp.body);
		auto device_code = doc.Str("device_code");
		auto user_code = doc.Str("user_code");
		auto verification_uri = doc.Str("verification_uri");
		auto verification_uri_complete = doc.Str("verification_uri_complete");
		if (device_code.empty() || user_code.empty() || verification_uri.empty()) {
			throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "device_code", device_endpoint),
			                  "VGI Orchard: the device authorization response is missing device_code, user_code "
			                  "or verification_uri");
		}
		int64_t expires_in = 600;
		doc.Int("expires_in", expires_in);
		int64_t interval = 5;
		doc.Int("interval", interval);
		if (interval < 1) {
			interval = 5;
		}

		std::string msg = "Sign in to " + display + " (Query.Farm Orchard).\nVisit: " + verification_uri +
		                  "\nEnter code: " + user_code;
		if (!verification_uri_complete.empty()) {
			msg += "\nOr visit: " + verification_uri_complete;
		}
		msg += "\nOnly approve if you started this sign-in.";
		if (!store_note.empty()) {
			msg += "\n" + store_note;
		}
		Prompt(context, msg);

		const auto budget = std::min(timeout_seconds, expires_in);
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(budget);
		const auto poll_body = FormBody({{"grant_type", kDeviceCodeGrant},
		                                 {"device_code", device_code},
		                                 {"client_id", kOrchardClientId},
		                                 {"resource", resource_}});
		int failures = 0;
		while (true) {
			for (int64_t waited = 0; waited < interval * 10; waited++) {
				if (context.IsInterrupted()) {
					throw InterruptException();
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
			if (std::chrono::steady_clock::now() >= deadline) {
				throw IOException(OrchardInfo(error_subtype::kOAuthTimeout, "device_poll", token_endpoint),
				                  "VGI Orchard: device sign-in for %s timed out after %lld seconds "
				                  "(vgi_oauth_timeout_seconds)",
				                  display, static_cast<long long>(budget));
			}
			OAuthFormResponse poll;
			try {
				poll = PostOAuthForm(context, token_endpoint, poll_body);
			} catch (const std::exception &) {
				if (++failures > 3) {
					throw;
				}
				continue;
			}
			if (poll.status_code == 200) {
				AcceptTokenResponse(context, poll.body, /*initial=*/true);
				Prompt(context, "Signed in to " + display + ".");
				return;
			}
			if (poll.status_code >= 500) {
				if (++failures > 3) {
					throw IOException(OrchardInfo(error_subtype::kOAuthFailed, "device_poll", token_endpoint),
					                  "VGI Orchard: device sign-in polling failed (HTTP %d)", poll.status_code);
				}
				continue;
			}
			failures = 0;
			auto code = OAuthErrorCode(poll.body);
			if (code == "authorization_pending") {
				continue;
			}
			if (code == "slow_down" || poll.status_code == 429) {
				interval += 5;
				continue;
			}
			if (code == "access_denied") {
				throw IOException(OrchardInfo(error_subtype::kAuthFailed, "device_poll", token_endpoint, code),
				                  "VGI Orchard: sign-in to %s was denied", display);
			}
			if (code == "expired_token") {
				throw IOException(OrchardInfo(error_subtype::kOAuthTimeout, "device_poll", token_endpoint, code),
				                  "VGI Orchard: the sign-in code for %s expired before it was approved; re-run "
				                  "ATTACH",
				                  display);
			}
			throw IOException(OrchardInfo(error_subtype::kAuthFailed, "device_poll", token_endpoint, code),
			                  "VGI Orchard: device sign-in for %s failed (%s)", display,
			                  DescribeOAuthError(poll.status_code, poll.body));
		}
	};

	auto pkce_flow = [&]() {
		const auto verifier = GenerateCodeVerifier();
		const auto challenge = ComputeCodeChallenge(verifier);
		const auto state = GenerateState();
		const std::string issuer = issuer_;

		CPPHTTPLIB_NAMESPACE::Server svr;
		std::promise<std::string> code_promise;
		auto code_future = code_promise.get_future();
		const std::string resource = resource_;
		svr.Get("/oauth-callback.html", [&code_promise, state, issuer, display,
		                                 resource](const CPPHTTPLIB_NAMESPACE::Request &req,
		                                           CPPHTTPLIB_NAMESPACE::Response &res) {
			auto fail = [&](const std::string &page_msg, const std::string &error_msg, const std::string &code) {
				res.set_content(OAuthCallbackErrorPage(page_msg, display), "text/html");
				try {
					code_promise.set_exception(std::make_exception_ptr(IOException(
					    OrchardInfo(error_subtype::kAuthFailed, "authorize", issuer + "/authorize", code), "%s",
					    error_msg)));
				} catch (...) {
				}
			};
			auto error = req.get_param_value("error");
			if (!error.empty()) {
				auto desc = req.get_param_value("error_description");
				fail(error + (desc.empty() ? "" : " - " + desc),
				     "VGI Orchard: sign-in to " + display + " failed: " + error + (desc.empty() ? "" : " - " + desc),
				     error);
				return;
			}
			if (req.get_param_value("state") != state) {
				fail("State mismatch: this may be a forged request.", "VGI Orchard: state mismatch in callback", "");
				return;
			}
			// RFC 9207: the response must name the issuer we sent the user to.
			if (req.get_param_value("iss") != issuer) {
				fail("Issuer mismatch: this may be a forged request.",
				     "VGI Orchard: the sign-in callback's iss does not match " + issuer, "");
				return;
			}
			auto code = req.get_param_value("code");
			if (code.empty()) {
				fail("No authorization code was received.", "VGI Orchard: no authorization code in callback", "");
				return;
			}
			res.set_content(OAuthCallbackSuccessPage(display, resource), "text/html");
			try {
				code_promise.set_value(code);
			} catch (...) {
			}
		});

		int port = svr.bind_to_any_port("127.0.0.1");
		if (port < 0) {
			throw OrchardCallbackBindFailure();
		}
		// Exactly 127.0.0.1, never localhost (the Front Desk registers loopback by IP).
		const std::string redirect_uri = "http://127.0.0.1:" + std::to_string(port) + "/oauth-callback.html";
		std::thread server_thread([&svr]() { svr.listen_after_bind(); });
		svr.wait_until_ready();
		auto stop = [&]() {
			svr.stop();
			if (server_thread.joinable()) {
				server_thread.join();
			}
		};

		const std::string auth_url = issuer_ + "/authorize?response_type=code&client_id=" +
		                             UrlEncode(kOrchardClientId) + "&redirect_uri=" + UrlEncode(redirect_uri) +
		                             "&state=" + UrlEncode(state) + "&code_challenge=" + UrlEncode(challenge) +
		                             "&code_challenge_method=S256&resource=" + UrlEncode(resource_);
		Prompt(context, "Sign in to " + display +
		                    " (Query.Farm Orchard). Opening your browser...\nIf it doesn't open, visit:\n" + auth_url +
		                    (store_note.empty() ? std::string() : "\n" + store_note));
		OpenBrowser(auth_url);

		std::string code;
		try {
			const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
			while (code_future.wait_for(std::chrono::milliseconds(200)) != std::future_status::ready) {
				if (context.IsInterrupted()) {
					throw InterruptException();
				}
				if (std::chrono::steady_clock::now() >= deadline) {
					throw IOException(OrchardInfo(error_subtype::kOAuthTimeout, "authorize", issuer_ + "/authorize"),
					                  "VGI Orchard: browser sign-in for %s timed out after %lld seconds "
					                  "(vgi_oauth_timeout_seconds)",
					                  display, static_cast<long long>(timeout_seconds));
				}
			}
			code = code_future.get();
		} catch (...) {
			stop();
			throw;
		}
		stop();

		auto resp = PostOAuthForm(context, token_endpoint,
		                          FormBody({{"grant_type", "authorization_code"},
		                                    {"code", code},
		                                    {"code_verifier", verifier},
		                                    {"redirect_uri", redirect_uri},
		                                    {"client_id", kOrchardClientId},
		                                    {"resource", resource_}}));
		if (resp.status_code != 200) {
			throw IOException(OrchardInfo(error_subtype::kAuthFailed, "token_exchange", token_endpoint,
			                              OAuthErrorCode(resp.body)),
			                  "VGI Orchard: completing sign-in to %s failed (%s)", display,
			                  DescribeOAuthError(resp.status_code, resp.body));
		}
		AcceptTokenResponse(context, resp.body, /*initial=*/true);
		Prompt(context, "Signed in to " + display + ".");
	};

	if (flow == "device_code" || (flow == "auto" && (IsHeadlessEnvironment() || IsColabEnvironment()))) {
		device_flow();
	} else if (flow == "pkce") {
		pkce_flow();
	} else {
		try {
			pkce_flow();
		} catch (const OrchardCallbackBindFailure &) {
			device_flow();
		}
	}
	return FieldAddress();
}
#endif // !__EMSCRIPTEN__

} // namespace vgi
} // namespace duckdb
