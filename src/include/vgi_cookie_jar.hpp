// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {
namespace vgi {

// Per-catalog HTTP session cookie jar: echoes Set-Cookie values back as Cookie
// headers on later RPCs, for sticky-session routing behind proxies.
//
// Single origin, so Domain / Path / HttpOnly / SameSite / Partitioned are
// ignored. Secure cookies are accepted only from https origins (there is no
// send-side check); Max-Age<=0 or a past Expires deletes the cookie.
//
// Shared by all query threads of a catalog; ``mutex_`` serialises access and
// the methods return copies, so no lock is held during I/O.
class SessionCookieJar {
public:
	SessionCookieJar() = default;

	// Apply raw Set-Cookie header values. Cookies marked Secure are dropped
	// unless ``origin_is_https``.
	void UpdateFromSetCookie(const std::vector<std::string> &set_cookie_headers, bool origin_is_https);

	// Build the serialized value for a single ``Cookie:`` header ("k=v; k=v"),
	// expiring cookies lazily. Returns empty string when no live cookies exist.
	std::string BuildCookieHeader();

	// For tests and diagnostics.
	size_t Size();

	// Caps so a hostile or buggy proxy cannot fill memory.
	static constexpr size_t kMaxCookies = 32;
	static constexpr size_t kMaxValueBytes = 4096;

private:
	struct CookieEntry {
		std::string value;
		// Absolute expiry. std::nullopt = session cookie (never expires).
		std::optional<std::chrono::system_clock::time_point> expires;
	};

	std::mutex mutex_;
	std::unordered_map<std::string, CookieEntry> cookies_;
};

} // namespace vgi
} // namespace duckdb
