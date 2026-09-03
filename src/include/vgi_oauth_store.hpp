// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <string>

namespace duckdb {
namespace vgi {

struct OAuthRefreshContext;

class OAuthCredentialLease {
public:
	virtual ~OAuthCredentialLease() = default;
};

// Acquire the per-credential cross-process lease. Returns nullptr when the
// selected mode is memory-only. `persistent` throws when the platform backend
// is unavailable; `auto` falls back to memory without creating plaintext data.
std::unique_ptr<OAuthCredentialLease> AcquireOAuthCredentialLease(const std::string &canonical_key,
                                                                  const std::string &cache_mode);

bool LoadOAuthRefreshToken(const std::string &canonical_key, const OAuthRefreshContext &expected,
                           const std::string &cache_mode, std::string &refresh_token);

void StoreOAuthRefreshToken(const std::string &canonical_key, const OAuthRefreshContext &binding,
                            const std::string &cache_mode, const std::string &refresh_token);

void DeleteOAuthRefreshToken(const std::string &canonical_key, const std::string &cache_mode);

} // namespace vgi
} // namespace duckdb
