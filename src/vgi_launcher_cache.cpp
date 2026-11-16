// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// POSIX-only (fork/exec + AF_UNIX); empty under emscripten. The dispatch sites
// in vgi_function_connection / vgi_unary_rpc gate on the same macro and report
// launch:/unix:// as unsupported in a WASM build.

#include "vgi_platform.hpp"

#if VGI_POSIX_TRANSPORT

#include "vgi_launcher_cache.hpp"
#include "vgi_hosted_protocols.hpp"

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

// One entry per location; ``recorded_overrides`` is the pin described at
// LaunchOverrides.
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
		throw InternalException(
		    "vgi launcher cache: unix:// locations should be handled by "
		    "StripUnixScheme directly, not through the launcher");
	} else {
		throw InvalidInputException("vgi launcher cache: unsupported location scheme: %s",
		                             location);
	}
	if (overrides.idle_timeout.has_value()) {
		cfg.idle_timeout = *overrides.idle_timeout;
	}
	if (overrides.state_dir.has_value()) {
		cfg.state_dir_override = *overrides.state_dir;
	}
	return cfg;
}

// LaunchOverrides for error messages: ``"defaults"`` or comma-separated key=value.
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
	// unix:// is connect-to-existing: the path is the bytes after the scheme.
	// Overrides are rejected at parse time for unix://, so these are empty.
	if (IsUnixLocation(location)) {
		auto path = StripUnixScheme(location);
		std::lock_guard<std::mutex> lk(g_cache_mutex);
		g_cache[location] = CacheEntry {path, overrides};
		return path;
	}

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

	// Launch outside the cache lock so a slow launch doesn't block other locations.
	auto cfg = BuildLaunchConfigFromLocation(location, overrides);
	auto path = Launch(cfg);
	std::lock_guard<std::mutex> lk(g_cache_mutex);
	// A racing caller may have populated the entry while we ran Launch(); with
	// equal overrides, prefer its entry.
	auto [it, inserted] = g_cache.try_emplace(location, CacheEntry {path, overrides});
	if (!inserted && it->second.recorded_overrides != overrides) {
		throw BinderException(
		    "vgi launcher: concurrent ATTACH installed conflicting launcher overrides "
		    "[%s] vs [%s]",
		    FormatOverrides(it->second.recorded_overrides), FormatOverrides(overrides));
	}
	return it->second.socket_path;
}

void InvalidateLauncherSocketCache(const std::string &location) {
	{
		std::lock_guard<std::mutex> lk(g_cache_mutex);
		g_cache.erase(location);
	}
	// The next connect reaches a relaunched (possibly upgraded) worker.
	NoteWorkerConnectionReestablished(location);
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
		// Typically the worker idled out. The retry probe-then-spawns for launch:;
		// for unix:// (managed out-of-band) it fails again and throws.
		InvalidateLauncherSocketCache(location);
		sock_path = ResolveLauncherSocketPath(location, overrides);
		return UnixSocket::Connect(sock_path, connect_timeout);
	}
}

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT
