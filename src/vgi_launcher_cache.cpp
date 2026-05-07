// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_launcher_cache.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_launcher.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_transport.hpp"

#include <mutex>
#include <unordered_map>

namespace duckdb {
namespace vgi {

namespace {

std::mutex g_cache_mutex;
std::unordered_map<std::string, std::string> g_cache;

LaunchConfig BuildLaunchConfigFromLocation(const std::string &location) {
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
	return cfg;
}

} // namespace

std::string ResolveLauncherSocketPath(const std::string &location) {
	// unix:// → trivially the bytes after the scheme.  No cache lookup
	// needed, but we still cache so callers can use a single API.
	if (IsUnixLocation(location)) {
		auto path = StripUnixScheme(location);
		std::lock_guard<std::mutex> lk(g_cache_mutex);
		g_cache[location] = path;
		return path;
	}

	// launch: → check the cache first.
	{
		std::lock_guard<std::mutex> lk(g_cache_mutex);
		auto it = g_cache.find(location);
		if (it != g_cache.end()) {
			return it->second;
		}
	}

	// Cache miss: invoke the launcher.  Done outside the cache lock so a
	// long-running launcher invocation doesn't block other locations.
	auto cfg = BuildLaunchConfigFromLocation(location);
	auto path = Launch(cfg);
	std::lock_guard<std::mutex> lk(g_cache_mutex);
	g_cache[location] = path;
	return path;
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
                              std::chrono::milliseconds connect_timeout) {
	auto sock_path = ResolveLauncherSocketPath(location);
	try {
		return UnixSocket::Connect(sock_path, connect_timeout);
	} catch (const IOException &) {
		// The cached worker is gone — typically because its idle timeout
		// expired between the prior call and this one.  Invalidate and
		// resolve again; the launcher will probe-then-spawn a fresh worker
		// for ``launch:`` locations or fail loudly for ``unix://`` ones
		// (which the operator manages out-of-band).
		InvalidateLauncherSocketCache(location);
		sock_path = ResolveLauncherSocketPath(location);
		return UnixSocket::Connect(sock_path, connect_timeout);
	}
}

} // namespace vgi
} // namespace duckdb
