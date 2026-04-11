#pragma once

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace vgi {

struct OAuthChallenge {
	std::string resource_metadata_url;
	std::string client_id;
};

struct OAuthResourceMetadata {
	std::vector<std::string> authorization_servers;
	std::vector<std::string> scopes_supported;
	std::string client_id;
	std::string client_secret;
	std::string device_code_client_id;     // Separate client for device code flow (e.g., Google TV type)
	std::string device_code_client_secret;
	std::string resource;
	std::string resource_name;
	bool use_id_token_as_bearer = false; // If true, send id_token as Bearer instead of access_token
};

struct OAuthServerMetadata {
	std::string authorization_endpoint;
	std::string token_endpoint;
	std::string device_authorization_endpoint; // RFC 8628
	std::vector<std::string> grant_types_supported;

	bool SupportsGrantType(const std::string &grant_type) const;
};

// Parsed OIDC id_token claims. Populated from the JWT payload at token
// ingestion. Not cryptographically verified — the id_token arrived over TLS
// from our own OAuth exchange, so we trust it as an identity hint, not a
// security boundary.
//
// Only the universal OIDC claims (sub, iss, email, name) are lifted into
// dedicated fields. Everything else — provider-specific claims like Entra's
// preferred_username/oid/tid, Google's custom attributes, group/role arrays —
// stays in claims_json verbatim so downstream SQL can reach it via JSON path
// expressions without the extension needing to know each provider's quirks.
struct OAuthIdentity {
	bool present = false;     // true iff parse succeeded and payload was valid JSON
	std::string sub;          // "sub" claim — subject / user id
	std::string email;        // "email" claim
	std::string name;         // "name" claim
	std::string issuer;       // "iss" claim
	std::string claims_json;  // Raw decoded JWT payload (valid JSON)
};

struct OAuthTokenSet {
	std::string access_token;
	std::string refresh_token;
	std::string id_token;
	std::string scope;
	std::chrono::steady_clock::time_point expires_at;
	bool use_id_token = false; // If true, BearerToken() returns id_token instead of access_token
	OAuthIdentity identity;    // Parsed claims from id_token (present=false if unavailable)

	bool IsValid() const;
	// Returns access_token by default; id_token if use_id_token is set
	std::string BearerToken() const;
};

struct OAuthRefreshContext {
	std::string token_endpoint;
	std::string client_id;
	std::string client_secret;
	std::string scope;
	bool use_id_token = false;
	std::string resource_metadata_url;
};

struct AuthState {
	enum class Status { IDLE, IN_PROGRESS, COMPLETE, FAILED };
	Status status = Status::IDLE;
	OAuthTokenSet token;
	OAuthRefreshContext refresh_ctx;
	std::string error_message;
	std::condition_variable cv;
};

class VgiTokenManager {
public:
	static VgiTokenManager &Instance();

	std::string GetToken(const std::string &origin);
	std::string HandleUnauthorized(const std::string &origin,
	                               const OAuthChallenge &challenge,
	                               ClientContext &context);
	void ClearTokens(const std::string &origin);
	void ClearTokens(ClientContext &context, const std::string &origin);
	void ClearAllTokens(ClientContext &context);

	// Diagnostic: get all origins with auth state
	std::vector<std::string> GetAllOrigins();

	// Diagnostic: get token info for an origin (returns false if not found/not complete)
	struct TokenInfo {
		bool has_refresh_token;
		int64_t expires_in_seconds; // negative if expired, 0 if unknown
		bool has_expires;
	};
	bool GetTokenInfo(const std::string &origin, TokenInfo &info);

	// Diagnostic: copy the full token set (including parsed identity) for an origin.
	// Returns true iff the origin has a COMPLETE auth state. Thread-safe.
	bool GetTokenSetCopy(const std::string &origin, OAuthTokenSet &out);

	static std::string ExtractOrigin(const std::string &url);

	// Seed a refresh token for an origin (e.g., from ATTACH oauth_refresh_token option).
	// Only sets the token if the origin is in IDLE state (not already authenticated).
	void SeedRefreshToken(const std::string &origin, const std::string &refresh_token);

	// Persistence helpers
	OAuthTokenSet AttemptTokenRefresh(const OAuthRefreshContext &ctx, const std::string &refresh_token,
	                                  ClientContext &context);
	void PersistRefreshToken(ClientContext &context, const std::string &origin, const OAuthTokenSet &tokens,
	                         const OAuthRefreshContext &ctx);
	bool LoadPersistedRefreshToken(ClientContext &context, const std::string &origin, OAuthRefreshContext &ctx_out,
	                               std::string &refresh_token_out);
	void RemovePersistedToken(ClientContext &context, const std::string &origin);
	static std::string SecretNameForOrigin(const std::string &origin);

private:
	mutable std::mutex mutex_;
	std::map<std::string, AuthState> auth_states_;

	OAuthRefreshContext DiscoverRefreshContext(const OAuthChallenge &challenge, ClientContext &context);

	OAuthTokenSet PerformPKCEFlow(const OAuthChallenge &challenge,
	                               const OAuthResourceMetadata &resource_meta,
	                               const OAuthServerMetadata &server_meta,
	                               ClientContext &context);
	OAuthTokenSet PerformDeviceCodeFlow(const OAuthChallenge &challenge,
	                                     const OAuthResourceMetadata &resource_meta,
	                                     const OAuthServerMetadata &server_meta,
	                                     ClientContext &context);
	OAuthTokenSet PerformAuthFlow(const OAuthChallenge &challenge,
	                               ClientContext &context,
	                               OAuthRefreshContext &refresh_ctx_out);
};

// Environment detection
bool IsHeadlessEnvironment();

// Utility functions
std::optional<OAuthChallenge> ParseWWWAuthenticate(const std::string &header);
std::string GenerateCodeVerifier();
std::string ComputeCodeChallenge(const std::string &verifier);
std::string GenerateState();
std::string Base64UrlEncode(const unsigned char *data, size_t len);
// Decodes a base64url-encoded string (RFC 4648 §5) — accepts missing padding.
// Returns empty string on malformed input.
std::string Base64UrlDecode(const std::string &input);
// Parse an OIDC id_token JWT and extract identity claims. No signature
// verification — see OAuthIdentity doc.
OAuthIdentity ParseIdTokenClaims(const std::string &id_token);
std::string UrlEncode(const std::string &str);
void OpenBrowser(const std::string &url);

// HTTP helper for metadata fetches
std::string HttpGet(ClientContext &context, const std::string &url);

// Metadata discovery
OAuthResourceMetadata FetchResourceMetadata(ClientContext &context, const std::string &url);
OAuthServerMetadata FetchAuthServerMetadata(ClientContext &context, const std::string &server_url);

} // namespace vgi
} // namespace duckdb
