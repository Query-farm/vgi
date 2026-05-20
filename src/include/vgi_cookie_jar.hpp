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

// Per-catalog HTTP session cookie jar.
//
// Plumbs Set-Cookie response headers from the worker / upstream proxy back into
// Cookie request headers on subsequent HTTP RPC calls, enabling sticky-session
// routing for fly.io-style version-aware proxies.
//
// Scope intentionally narrow: this is a single-origin client-to-catalog
// relationship so we ignore Domain / Path / HttpOnly / SameSite / Partitioned.
// We do honor Secure (refuse to send over plaintext http://) and expiry
// (Max-Age=0 / past Expires trigger immediate deletion).
//
// Thread safety: the jar is shared across all query threads for a catalog.
// Mutations and reads are serialized by ``mutex_``. Callers must release the
// mutex before doing any socket I/O — the public methods enforce this by
// returning owned copies rather than references into the map.
class SessionCookieJar {
public:
	SessionCookieJar() = default;

	// Parse a list of raw Set-Cookie header values and update the jar.
	// ``origin_is_https`` gates whether Secure cookies are accepted. Cookies
	// parsed from responses over plaintext are rejected if they declare
	// Secure (belt-and-suspenders — normally a proxy over HTTP wouldn't
	// mark stickiness cookies Secure anyway).
	void UpdateFromSetCookie(const std::vector<std::string> &set_cookie_headers, bool origin_is_https);

	// Build the serialized value for a single ``Cookie:`` header ("k=v; k=v"),
	// expiring cookies lazily. Returns empty string when no live cookies exist.
	std::string BuildCookieHeader();

	// Snapshot size — for tests and diagnostics only.
	size_t Size();

	// Hard caps to prevent a hostile or buggy proxy from filling memory.
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
