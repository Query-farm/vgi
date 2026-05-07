// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// POSIX-only — depends on vgi_launcher (fork/exec) and vgi_unix_socket
// (AF_UNIX).  Under emscripten this translation unit is empty; the
// dispatch sites in vgi_function_connection / vgi_unary_rpc gate on the
// same macro and surface a clear error if launch:/unix:// LOCATIONs are
// used in a WASM build.

#ifndef __EMSCRIPTEN__

#include "vgi_launcher_cache.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "vgi_launcher.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_transport.hpp"

#include <mutex>
#include <unordered_map>

namespace duckdb {
namespace vgi {

namespace {

// One cache entry per location.  ``recorded_overrides`` holds whatever
// LaunchOverrides the *first* successful resolution used; subsequent
// callers must match these values or the resolver throws BinderException.
// On cache invalidation the entry is removed entirely, so the next
// resolution starts fresh and a new caller can install different overrides.
struct CacheEntry {
	std::string socket_path;
	LaunchOverrides recorded_overrides;
};

std::mutex g_cache_mutex;
std::unordered_map<std::string, CacheEntry> g_cache;

LaunchConfig BuildLaunchConfigFromLocation(const std::string &location, const LaunchOverrides &overrides) {
	LaunchConfig cfg;
	if (IsLaunchLocation(location)) {
		auto payload = StripLaunchScheme(location);
		try {
			cfg.worker_argv = launcher::ParseLaunchArgv(payload);
		} catch (const std::invalid_argument &e) {
			throw InvalidInputException(e.what());
		}
	} else if (IsUnixLocation(location)) {
		// unix:// is "connect-to-existing"; we model it through the same
		// resolve-socket-path API so callers don't branch.  No worker_argv,
		// no launcher invocation: the path is simply the bytes after
		// ``unix://``.
		throw InternalException(
		    "vgi launcher cache: unix:// locations should be handled by "
		    "StripUnixScheme directly, not through the launcher");
	} else {
		throw InvalidInputException("vgi launcher cache: unsupported location scheme: %s",
		                             location);
	}
	// Apply overrides on top of LaunchConfig defaults.
	if (overrides.idle_timeout.has_value()) {
		cfg.idle_timeout = *overrides.idle_timeout;
	}
	if (overrides.state_dir.has_value()) {
		cfg.state_dir_override = *overrides.state_dir;
	}
	return cfg;
}

// Render LaunchOverrides as a short human-readable string for error
// messages.  Empty overrides → ``"defaults"``; otherwise comma-separated
// key=value pairs.  Used only on the BinderException path so allocation
// cost is irrelevant.
std::string FormatOverrides(const LaunchOverrides &o) {
	std::string out;
	if (o.idle_timeout.has_value()) {
		out += "idle_timeout=" + std::to_string(o.idle_timeout->count()) + "ms";
	}
	if (o.state_dir.has_value()) {
		if (!out.empty()) {
			out += ", ";
		}
		out += "state_dir=\"" + *o.state_dir + "\"";
	}
	return out.empty() ? "defaults" : out;
}

} // namespace

std::string ResolveLauncherSocketPath(const std::string &location, const LaunchOverrides &overrides) {
	// unix:// → trivially the bytes after the scheme.  Overrides are
	// rejected at parse time for unix://, so they should always be empty
	// here; we still record them for symmetry.
	if (IsUnixLocation(location)) {
		auto path = StripUnixScheme(location);
		std::lock_guard<std::mutex> lk(g_cache_mutex);
		g_cache[location] = CacheEntry {path, overrides};
		return path;
	}

	// launch: → check the cache first.
	{
		std::lock_guard<std::mutex> lk(g_cache_mutex);
		auto it = g_cache.find(location);
		if (it != g_cache.end()) {
			if (it->second.recorded_overrides != overrides) {
				throw BinderException(
				    "vgi launcher: this LOCATION is already attached with launcher overrides "
				    "[%s]; cannot reattach with [%s].  Detach the existing catalog first, or "
				    "wait for the worker's idle-shutdown so a fresh ATTACH can install new "
				    "overrides.",
				    FormatOverrides(it->second.recorded_overrides), FormatOverrides(overrides));
			}
			return it->second.socket_path;
		}
	}

	// Cache miss: invoke the launcher.  Done outside the cache lock so a
	// long-running launcher invocation doesn't block other locations.
	auto cfg = BuildLaunchConfigFromLocation(location, overrides);
	auto path = Launch(cfg);
	std::lock_guard<std::mutex> lk(g_cache_mutex);
	// A racing caller may have populated the entry while we ran Launch().
	// If so, prefer their entry (we built ours from the same overrides
	// since we hold no shared mutable state in BuildLaunchConfigFromLocation).
	auto [it, inserted] = g_cache.try_emplace(location, CacheEntry {path, overrides});
	if (!inserted && it->second.recorded_overrides != overrides) {
		// Extremely narrow race: another thread spawned with different
		// overrides between our cache miss and our re-acquire.  Treat
		// the same as a regular conflict.
		throw BinderException(
		    "vgi launcher: concurrent ATTACH installed conflicting launcher overrides "
		    "[%s] vs [%s]",
		    FormatOverrides(it->second.recorded_overrides), FormatOverrides(overrides));
	}
	return it->second.socket_path;
}

void InvalidateLauncherSocketCache(const std::string &location) {
	std::lock_guard<std::mutex> lk(g_cache_mutex);
	g_cache.erase(location);
}

void ClearLauncherSocketCache() {
	std::lock_guard<std::mutex> lk(g_cache_mutex);
	g_cache.clear();
}

UnixSocket ResolveAndConnect(const std::string &location,
                              std::chrono::milliseconds connect_timeout,
                              const LaunchOverrides &overrides) {
	auto sock_path = ResolveLauncherSocketPath(location, overrides);
	try {
		return UnixSocket::Connect(sock_path, connect_timeout);
	} catch (const IOException &) {
		// The cached worker is gone — typically because its idle timeout
		// expired between the prior call and this one.  Invalidate and
		// resolve again; the launcher will probe-then-spawn a fresh worker
		// for ``launch:`` locations or fail loudly for ``unix://`` ones
		// (which the operator manages out-of-band).
		InvalidateLauncherSocketCache(location);
		sock_path = ResolveLauncherSocketPath(location, overrides);
		return UnixSocket::Connect(sock_path, connect_timeout);
	}
}

} // namespace vgi
} // namespace duckdb

#endif // !__EMSCRIPTEN__
