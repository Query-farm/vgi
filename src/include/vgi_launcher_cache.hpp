// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Per-DuckDB-process cache of resolved launcher socket paths, so only the first
// RPC for a `launch:` LOCATION pays the discover-or-spawn cost (probe → flock →
// maybe spawn → wait for UNIX:<path>); later calls connect AF_UNIX directly.

#pragma once

#include "vgi_unix_socket.hpp"

#include <chrono>
#include <optional>
#include <string>

namespace duckdb {
namespace vgi {

// Per-LOCATION launcher overrides surfaced through ATTACH options. A
// ``nullopt`` field means the launcher default (LaunchConfig: 300 s idle,
// OS-derived state directory).
//
// The first resolution for a location pins its overrides to the cache entry;
// later calls for that location must pass equal overrides or get
// ``BinderException`` ("first ATTACH wins"). Invalidation drops the pin along
// with the socket path, so the next caller's overrides become the new pin.
struct LaunchOverrides {
	std::optional<std::chrono::milliseconds> idle_timeout;
	std::optional<std::string> state_dir;

	bool operator==(const LaunchOverrides &other) const {
		return idle_timeout == other.idle_timeout && state_dir == other.state_dir;
	}
	bool operator!=(const LaunchOverrides &other) const {
		return !(*this == other);
	}
};

// Resolve a "launch:…" or "unix://…" location to an absolute AF_UNIX socket
// path. Thread-safe. The cache trusts entries until evicted: callers that see
// a stale entry (connect refused) call ``InvalidateLauncherSocketCache``.
// Throws ``IOException`` / ``InvalidInputException`` on launcher failure or
// ``BinderException`` on override mismatch (see LaunchOverrides).
std::string ResolveLauncherSocketPath(const std::string &location,
                                       const LaunchOverrides &overrides = {});

// Drop the cached socket path for `location`.  Idempotent.
void InvalidateLauncherSocketCache(const std::string &location);

// Drop every cached entry.  Diagnostic / test helper.
void ClearLauncherSocketCache();

// Resolve the socket path for *location* and connect via AF_UNIX. On a connect
// failure (worker idle-shut-down or killed) invalidate, re-resolve (spawning a
// fresh worker) and retry once, so long-lived sessions survive worker idle
// timeouts. The 10s default absorbs GIL contention and accept-queue delays
// under burst load. Throws ``IOException`` on the second consecutive connect
// failure or ``BinderException`` on override mismatch.
UnixSocket ResolveAndConnect(
    const std::string &location,
    std::chrono::milliseconds connect_timeout = std::chrono::seconds(10),
    const LaunchOverrides &overrides = {});

} // namespace vgi
} // namespace duckdb
