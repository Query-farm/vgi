// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// orchard://<workspace>/<deployment> locations: sign in at the Orchard Front
// Desk (a fixed OAuth issuer, no discovery), then talk directly to the
// deployment's hosted worker at the Field Address the token response names.
// See docs/location-delegation.md ("Orchard locations", §4-§9) and
// docs/orchard_locations.md.

#include <chrono>
#include <mutex>
#include <string>

#include "vgi_oauth.hpp"

namespace duckdb {
class ClientContext;

namespace vgi {

struct OrchardLocation {
	std::string workspace;
	std::string deployment;

	// orchard://<workspace>/<deployment>
	std::string ToString() const;
};

// ^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$
bool IsValidOrchardSlug(const std::string &slug);

// Parse an orchard:// LOCATION. Throws BinderException naming what is wrong.
OrchardLocation ParseOrchardLocation(const std::string &location);

// The issuer (`https://auth.queryfarm.cloud`) and Desk origin
// (`https://desk.queryfarm.cloud`). Test hooks only: VGI_ORCHARD_ISSUER and
// VGI_ORCHARD_DESK_ORIGIN override them (https, or loopback http).
std::string OrchardIssuer();
std::string OrchardDeskOrigin();

// The RFC 8707 resource: <desk origin>/<workspace>/<deployment>.
std::string OrchardResource(const OrchardLocation &location);

// Validate a token response's `vgi_location`: https, optional port, no path,
// query, fragment or userinfo, not an IP literal, host under
// `.field.queryfarm.cloud` or exactly `<app>.fly.dev`. Returns the canonical form (lowercase host, no
// trailing slash, no :443) or throws IOException. Loopback http is accepted
// only with VGI_TEST_ALLOW_LOOPBACK_DELEGATION=1 in the environment.
std::string ValidateOrchardFieldAddress(const std::string &vgi_location);

// The OAuth client every native Orchard sign-in presents.
static constexpr const char *kOrchardClientId = "vgi-duckdb";

// Per-catalog auth for an orchard:// ATTACH. Signs in at ATTACH (refresh token
// or interactive), holds the Pass and the rotating Renewal Token, and refreshes
// once per rejected Pass. Never starts an interactive flow after ATTACH.
class OrchardCatalogAuth : public CatalogAuth {
public:
	// `profile` (default "default") and `cache_mode` (oauth_cache) select the
	// stored sign-in shared by every catalog of this deployment.
	OrchardCatalogAuth(OrchardLocation location, std::string profile, std::string cache_mode);

	// Sign in. An explicit `refresh_token` is used for this attach only: it is
	// never read from or written to the credential store. Otherwise the stored
	// sign-in is refreshed; if there is none, or it is refused (invalid_grant,
	// which deletes it), browser PKCE or (headless) device code, saved after.
	// Returns the validated Field Address.
	std::string SignIn(ClientContext &context, const std::string &refresh_token);

	const OrchardLocation &Location() const {
		return location_;
	}
	std::string FieldAddress() const;

	std::string GetToken() override;
	std::string GetTokenForRequest(ClientContext &context) override;
	std::string HandleUnauthorized(const OAuthChallenge &challenge, ClientContext &context) override;
	bool HandlesRejectedTokens() const override {
		return true;
	}
	std::string HandleRejectedToken(const std::string &rejected_token, ClientContext &context) override;
	void ClearTokens() override;
	bool GetTokenInfo(TokenInfo &info) override;
	bool GetTokenSetCopy(OAuthTokenSet &out) override;
	bool WasInteractive() const override;
	bool IsExplicitlyConfigured() const override {
		return true;
	}

private:
	// Refresh with the current Renewal Token. Caller holds refresh_mutex_.
	// `initial` (the ATTACH sign-in) requires and records vgi_location.
	void RefreshLocked(ClientContext &context, bool initial);
	// Store a token response; `initial` requires vgi_location.
	void AcceptTokenResponse(ClientContext &context, const std::string &body, bool initial);
	std::string SignInInteractive(ClientContext &context, const std::string &store_note);
	// Write the current Renewal Token to the store. Caller holds the lease.
	void PersistCurrentToken(ClientContext &context);

	const OrchardLocation location_;
	const std::string issuer_;
	const std::string resource_;
	const std::string cache_mode_;
	OAuthRefreshContext binding_; // what a stored record is bound to
	std::string store_key_;

	// Guarded by refresh_mutex_.
	bool persist_ = false;      // this sign-in is shared through the credential store
	bool stored_entry_ = false; // the store held (or was given) our token

	// Serializes refreshes: N parallel 401s cause one refresh.
	std::mutex refresh_mutex_;
	mutable std::mutex mutex_; // guards everything below
	OAuthTokenSet token_;      // access_token = the Pass; refresh_token = the Renewal Token
	std::string field_address_;
	std::string dead_message_; // set after invalid_grant: every later call fails with it
	bool interactive_ = false;
};

} // namespace vgi
} // namespace duckdb
