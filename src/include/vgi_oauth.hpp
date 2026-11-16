// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <thread>
#include <map>
#include <memory>
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
	// Non-standard RFC 9728 extension: the VGI server's PKCE token-exchange
	// proxy, which injects the server-side client_secret (IdPs such as Google
	// require one). Preferred over the IdP's token_endpoint when set, so PKCE
	// works without a local secret (e.g. in WASM).
	std::string token_endpoint;
};

struct OAuthServerMetadata {
	std::string issuer;
	std::string authorization_endpoint;
	std::string token_endpoint;
	std::string device_authorization_endpoint; // RFC 8628
	std::vector<std::string> grant_types_supported;

	bool SupportsGrantType(const std::string &grant_type) const;
};

// Parsed OIDC id_token claims. Not signature-verified: an identity hint for
// display, never a security boundary. sub/iss/email/name get fields; all other
// (provider-specific) claims stay verbatim in claims_json for SQL JSON paths.
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
	std::string BearerToken() const;
};

struct OAuthRefreshContext {
	std::string token_endpoint;
	std::string client_id;
	std::string client_secret;
	std::string scope;
	std::string issuer;
	std::string resource;
	bool use_id_token = false;
	std::string resource_metadata_url;
	// Resource metadata's separate device-code client, when advertised. A
	// device-issued refresh token is persisted under this client and must be
	// redeemed with it, so a reload has to recognize it as the same session.
	std::string device_client_id;
	std::string device_client_secret;
};

struct AuthState {
	enum class Status { IDLE, IN_PROGRESS, COMPLETE, FAILED };
	Status status = Status::IDLE;
	OAuthTokenSet token;
	OAuthRefreshContext refresh_ctx;
	std::string error_message;
	std::condition_variable cv;
	// Thread owning IN_PROGRESS: a 401 from the auth flow itself re-entering
	// HandleUnauthorized on that thread throws instead of self-waiting on cv.
	std::thread::id owner;
	// Sticky: set once PerformAuthFlow (device-code / browser PKCE) ran, as
	// opposed to a silent refresh or cached token. Read by telemetry.
	bool interactive = false;
	uint64_t epoch = 0; // incremented by logout; in-flight auth may not publish across it
	// Persistent-store key of this session. Not derivable from refresh_ctx after
	// a device-code login, whose client_id is the device client.
	std::string store_key;
	mutable std::mutex mutex;
};

// Per-catalog authentication: each attached VGI catalog owns its own instance,
// even when several catalogs point at the same origin.
class CatalogAuth {
public:
	virtual ~CatalogAuth() = default;

	// Current bearer token ("" if none).
	virtual std::string GetToken() = 0;

	// Handle a 401: obtain and return a new bearer token, or throw.
	virtual std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) = 0;

	virtual void ClearTokens() = 0;

	struct TokenInfo {
		bool has_refresh_token = false;
		int64_t expires_in_seconds = 0;
		bool has_expires = false;
	};
	virtual bool GetTokenInfo(TokenInfo &info) { return false; }
	virtual bool GetTokenSetCopy(OAuthTokenSet &out) { return false; }

	// True iff an interactive flow (device-code / browser) ran; see AuthState::interactive.
	virtual bool WasInteractive() const { return false; }

	// Whether a credential is configured. Bearer: always. OAuth: when a refresh
	// token is held (e.g. oauth_refresh_token) or a flow completed. A default
	// OAuthCatalogAuth returns false, so the HTTP 401 handler reports "no
	// credential" instead of starting discovery on an empty challenge.
	virtual bool IsExplicitlyConfigured() const = 0;
};

// Static bearer token (API keys, service account tokens, pre-obtained JWTs).
// HandleUnauthorized() throws: a rejected static token cannot be recovered.
class BearerTokenCatalogAuth : public CatalogAuth {
public:
	explicit BearerTokenCatalogAuth(std::string token);

	std::string GetToken() override;
	std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) override;
	void ClearTokens() override;
	bool IsExplicitlyConfigured() const override { return true; }

private:
	mutable std::mutex mutex_;
	std::string token_;
};

// OAuth (PKCE, device code, token refresh) over one AuthState.
class OAuthCatalogAuth : public CatalogAuth {
public:
	explicit OAuthCatalogAuth(std::string profile = "default", std::string cache_mode = "auto");

	std::string GetToken() override;
	std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) override;
	void ClearTokens() override;

	// Seed a refresh token (ATTACH oauth_refresh_token); only while IDLE.
	void SeedRefreshToken(const std::string &refresh_token);

	bool GetTokenInfo(TokenInfo &info) override;
	bool GetTokenSetCopy(OAuthTokenSet &out) override;
	bool IsExplicitlyConfigured() const override;
	bool WasInteractive() const override;

private:
	std::shared_ptr<AuthState> AdoptSharedSession(const OAuthRefreshContext &refresh_ctx);
	mutable std::mutex binding_mutex_;
	std::shared_ptr<AuthState> state_;
	std::string profile_;
	std::string cache_mode_;
};

// Stateless auth flow helpers used by OAuthCatalogAuth.

OAuthRefreshContext DiscoverRefreshContext(const OAuthChallenge &challenge, ClientContext &context);

OAuthTokenSet AttemptTokenRefresh(const OAuthRefreshContext &ctx, const std::string &refresh_token,
                                  ClientContext &context);

OAuthTokenSet PerformAuthFlow(const OAuthChallenge &challenge, ClientContext &context,
                               OAuthRefreshContext &refresh_ctx_out);


bool IsHeadlessEnvironment();

// Parse the auth-params of a `WWW-Authenticate` challenge into name -> value.
// A real RFC 9110 parser (quoted values with commas/escapes, unquoted values,
// case-insensitive names), not a substring search: `client_id="` also occurs
// inside `device_code_client_id="`.
std::map<std::string, std::string> ParseAuthParams(const std::string &params);

// The loopback callback server could not bind a port. A dedicated type so the
// auto-mode fallback to the device-code flow keys on the kind, not message text.
class PkceCallbackBindFailure : public IOException {
public:
	PkceCallbackBindFailure()
	    : IOException("VGI OAuth: failed to bind local callback server") {
	}
};

// The client credentials a flow presents. A refresh must authenticate as the
// same client the grant was issued to (RFC 6749 §6), so the flow hands its
// choice to the refresh context rather than selecting again.
struct OAuthClientCredentials {
	std::string client_id;
	std::string client_secret;
};

// Which client the device-code flow presents: `device_code_client_id` (e.g.
// Google's TV/device client) when present, else the ordinary client, resource
// metadata before the challenge header.
OAuthClientCredentials SelectDeviceCodeClient(const std::string &device_client_id,
                                              const std::string &device_client_secret,
                                              const std::string &client_id,
                                              const std::string &client_secret,
                                              const std::string &challenge_client_id);

// Persisted refresh-token record: the token plus the issuer/resource/client/scope
// it is bound to. Decode succeeds only for a record of `expected`'s session,
// issued to either its ordinary or its device-code client; `issued` receives the
// refresh context the token must be redeemed with (RFC 6749 §6).
std::string EncodeOAuthRefreshRecord(const OAuthRefreshContext &binding, const std::string &refresh_token);
bool DecodeOAuthRefreshRecord(const std::string &data, const OAuthRefreshContext &expected,
                              std::string &refresh_token, OAuthRefreshContext &issued);

// Whether an access token is still usable `skew` ahead of now (a zero
// `expires_at` never expires). Refreshing early avoids a request going out
// without a token, drawing a 401 and re-sending the whole body.
bool TokenStillFresh(std::chrono::steady_clock::time_point expires_at,
                     std::chrono::steady_clock::time_point now,
                     std::chrono::seconds skew);

// How far ahead of expiry a token is treated as already stale.
static constexpr std::chrono::seconds kTokenRefreshSkew {45};

// True inside a Google Colab kernel, which shows C-level stderr in the cell: the
// device-code prompt goes to stderr there (DUCKDB_LOG_WARNING is not surfaced by
// the Python client, so login would appear to hang).
bool IsColabEnvironment();

// True iff url is http:// whose authority is exactly 127.0.0.1 / localhost / [::1]
// (host case-insensitive), with an optional numeric port and no userinfo, so
// http://127.0.0.1.evil.com and http://localhost@evil.com are rejected.
bool IsLoopbackHttpUrl(const std::string &url);

std::optional<OAuthChallenge> ParseWWWAuthenticate(const std::string &header);
std::string GenerateCodeVerifier();
std::string ComputeCodeChallenge(const std::string &verifier);
std::string GenerateState();
std::string Base64UrlEncode(const unsigned char *data, size_t len);
// Base64url (RFC 4648 §5) decode; padding optional; "" on malformed input.
std::string Base64UrlDecode(const std::string &input);
// Identity claims of an OIDC id_token JWT (unverified; see OAuthIdentity).
OAuthIdentity ParseIdTokenClaims(const std::string &id_token);
std::string UrlEncode(const std::string &str);
void OpenBrowser(const std::string &url);

std::string HttpGet(ClientContext &context, const std::string &url);

OAuthResourceMetadata FetchResourceMetadata(ClientContext &context, const std::string &url);
OAuthServerMetadata FetchAuthServerMetadata(ClientContext &context, const std::string &server_url);

// scheme://host:port of a URL.
std::string ExtractOrigin(const std::string &url);

// Non-secret fingerprint of the calling identity for scoping the result cache,
// so one principal's cached rows are never served to another (security-critical).
//   - "anon"            : no auth, or not explicitly configured.
//   - "oauth:<sha256>"  : salted hash of the exact bearer credential sent; a
//                         refresh deliberately changes the identity.
//   - "bearer:<sha256>" : salted hash of the static token.
//   - ""                : configured but unresolvable (e.g. OAuth not yet
//                         authenticated): the caller MUST fail closed (not cache).
// Never triggers an auth flow.
std::string ComputeCatalogIdentityFingerprint(const std::shared_ptr<CatalogAuth> &auth);

// Catalog name + US + ComputeCatalogIdentityFingerprint, or "" (fail closed).
// The single builder for both the cache key and the per-identity disk-flush path.
std::string BuildCatalogIdentityScope(const std::string &catalog_name,
                                      const std::shared_ptr<CatalogAuth> &auth);

} // namespace vgi
} // namespace duckdb
