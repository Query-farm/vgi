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
//
// `allow_linux`: use the Linux backends (Secret Service via a dlopen'd
// libsecret, else an owner-only 0600 file in a 0700 directory under
// $XDG_STATE_HOME/queryfarm-vgi or ~/.local/state/queryfarm-vgi). Only Orchard
// sign-ins opt in; ordinary OAuth stays memory-only on Linux.
std::unique_ptr<OAuthCredentialLease> AcquireOAuthCredentialLease(const std::string &canonical_key,
                                                                  const std::string &cache_mode,
                                                                  bool allow_linux = false);

// `issued` receives the context to redeem the token with: `expected`, or
// `expected` switched to its device-code client when the record was issued to it.
bool LoadOAuthRefreshToken(const std::string &canonical_key, const OAuthRefreshContext &expected,
                           const std::string &cache_mode, std::string &refresh_token,
                           OAuthRefreshContext *issued = nullptr, bool allow_linux = false);

void StoreOAuthRefreshToken(const std::string &canonical_key, const OAuthRefreshContext &binding,
                            const std::string &cache_mode, const std::string &refresh_token,
                            bool allow_linux = false);

// Missing credentials are already deleted. Backend errors propagate in both
// auto and persistent modes so logout cannot report success with a saved token.
void DeleteOAuthRefreshToken(const std::string &canonical_key, const std::string &cache_mode,
                             bool allow_linux = false);

// Where credentials for `cache_mode` go on this build, for messages and docs
// (e.g. "an owner-only file in ~/.local/state/queryfarm-vgi", or "memory only (reason)").
std::string DescribeOAuthStore(const std::string &cache_mode, bool allow_linux = false);

} // namespace vgi
} // namespace duckdb
