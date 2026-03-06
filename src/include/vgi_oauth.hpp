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

struct OAuthTokenSet {
	std::string access_token;
	std::string refresh_token;
	std::string id_token;
	std::string scope;
	std::chrono::steady_clock::time_point expires_at;
	bool use_id_token = false; // If true, BearerToken() returns id_token instead of access_token

	bool IsValid() const;
	// Returns access_token by default; id_token if use_id_token is set
	std::string BearerToken() const;
};

struct AuthState {
	enum class Status { IDLE, IN_PROGRESS, COMPLETE, FAILED };
	Status status = Status::IDLE;
	OAuthTokenSet token;
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

	static std::string ExtractOrigin(const std::string &url);

private:
	mutable std::mutex mutex_;
	std::map<std::string, AuthState> auth_states_;

	OAuthTokenSet PerformPKCEFlow(const OAuthChallenge &challenge,
	                               const OAuthResourceMetadata &resource_meta,
	                               const OAuthServerMetadata &server_meta,
	                               ClientContext &context);
	OAuthTokenSet PerformDeviceCodeFlow(const OAuthChallenge &challenge,
	                                     const OAuthResourceMetadata &resource_meta,
	                                     const OAuthServerMetadata &server_meta,
	                                     ClientContext &context);
	OAuthTokenSet PerformAuthFlow(const OAuthChallenge &challenge,
	                               ClientContext &context);
};

// Environment detection
bool IsHeadlessEnvironment();

// Utility functions
std::optional<OAuthChallenge> ParseWWWAuthenticate(const std::string &header);
std::string GenerateCodeVerifier();
std::string ComputeCodeChallenge(const std::string &verifier);
std::string GenerateState();
std::string Base64UrlEncode(const unsigned char *data, size_t len);
std::string UrlEncode(const std::string &str);
void OpenBrowser(const std::string &url);

// HTTP helper for metadata fetches
std::string HttpGet(ClientContext &context, const std::string &url);

// Metadata discovery
OAuthResourceMetadata FetchResourceMetadata(ClientContext &context, const std::string &url);
OAuthServerMetadata FetchAuthServerMetadata(ClientContext &context, const std::string &server_url);

} // namespace vgi
} // namespace duckdb
