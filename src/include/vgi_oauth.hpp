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

// ============================================================================
// Per-Catalog Authentication
// ============================================================================

// Abstract base class for catalog authentication.
// Each attached VGI catalog owns its own CatalogAuth instance, ensuring
// independent auth state even when multiple catalogs point to the same origin.
class CatalogAuth {
public:
	virtual ~CatalogAuth() = default;

	// Get current bearer token (empty string if none available)
	virtual std::string GetToken() = 0;

	// Handle a 401 response — attempt to obtain a valid token.
	// Returns the new bearer token. Throws if auth fails irrecoverably.
	virtual std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) = 0;

	// Clear any cached tokens (resets to unauthenticated state)
	virtual void ClearTokens() = 0;

	// Diagnostics
	struct TokenInfo {
		bool has_refresh_token = false;
		int64_t expires_in_seconds = 0;
		bool has_expires = false;
	};
	virtual bool GetTokenInfo(TokenInfo &info) { return false; }
	virtual bool GetTokenSetCopy(OAuthTokenSet &out) { return false; }
};

// Static bearer token auth (API keys, service account tokens, pre-obtained JWTs).
// GetToken() returns the static token. HandleUnauthorized() throws — a rejected
// static token has no recovery mechanism.
class BearerTokenCatalogAuth : public CatalogAuth {
public:
	explicit BearerTokenCatalogAuth(std::string token);

	std::string GetToken() override;
	std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) override;
	void ClearTokens() override;

private:
	mutable std::mutex mutex_;
	std::string token_;
};

// Full OAuth flow auth (PKCE, device code, token refresh).
// Per-catalog version of the former VgiTokenManager singleton — same flow logic,
// same thread synchronization, but operating on a single AuthState.
class OAuthCatalogAuth : public CatalogAuth {
public:
	OAuthCatalogAuth();

	std::string GetToken() override;
	std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) override;
	void ClearTokens() override;

	// Seed a refresh token (e.g., from ATTACH oauth_refresh_token option).
	// Only sets the token if auth state is IDLE (not already authenticated).
	void SeedRefreshToken(const std::string &refresh_token);

	bool GetTokenInfo(TokenInfo &info) override;
	bool GetTokenSetCopy(OAuthTokenSet &out) override;

private:
	mutable std::mutex mutex_;
	AuthState state_;
};

// ============================================================================
// Free-function auth flow helpers (stateless — used by OAuthCatalogAuth)
// ============================================================================

OAuthRefreshContext DiscoverRefreshContext(const OAuthChallenge &challenge, ClientContext &context);

OAuthTokenSet AttemptTokenRefresh(const OAuthRefreshContext &ctx, const std::string &refresh_token,
                                  ClientContext &context);

OAuthTokenSet PerformAuthFlow(const OAuthChallenge &challenge, ClientContext &context,
                               OAuthRefreshContext &refresh_ctx_out);


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

// Extract scheme://host:port from a URL
std::string ExtractOrigin(const std::string &url);

} // namespace vgi
} // namespace duckdb
