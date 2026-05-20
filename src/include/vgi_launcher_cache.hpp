// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Per-DuckDB-process cache of resolved launcher socket paths.
//
// Without this, every RPC call from a `LOCATION 'launch:…'` ATTACH would
// re-invoke the full discover-or-spawn dance (probe → flock → maybe spawn
// → wait for UNIX:<path>).  The first call genuinely needs that work; every
// subsequent call should just connect AF_UNIX directly to the previously-
// resolved path.

#pragma once

#include "vgi_unix_socket.hpp"

#include <chrono>
#include <optional>
#include <string>

namespace duckdb {
namespace vgi {

// Per-LOCATION launcher overrides surfaced through ATTACH options.
//
// Both fields are ``nullopt`` when the user didn't supply the
// corresponding option; in that case the launcher's compiled-in
// defaults apply (300 s idle, OS-derived state directory).
//
// **Conflict semantics.**  These overrides are pinned to a worker's
// lifetime by the resolver: the first ``ResolveAndConnect`` call for a
// given ``location`` records its overrides; subsequent calls *for the
// same location, while the worker is alive* must pass the same
// overrides or the resolver throws ``BinderException``.  This makes
// "first ATTACH wins" deterministic and visible — versus the original
// silent-inherit semantic which let "most-recent-respawn wins" creep
// in across cache invalidations.
//
// On cache invalidation (worker idle-shut-down or connect failure),
// the recorded overrides are dropped along with the socket path; the
// next caller's overrides become the new pin.
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

// Resolve `location` (which must be a "launch:…" or "unix://…" string) to
// an absolute AF_UNIX socket path.  The first call per `(location, process)`
// pays the launcher cost; subsequent calls return the cached path until
// either (a) the cache is explicitly evicted or (b) a probe in this layer
// detects the worker is gone (caller's responsibility — the cache itself
// trusts entries until told otherwise).
//
// Thread-safe.  Callers that detect a stale cache entry (e.g. AF_UNIX
// connect refused on the cached path) should call ``InvalidateLauncherSocketCache``
// so the next caller does the probe-or-spawn dance.
//
// ``overrides`` are pinned to the cached entry: the first call's
// overrides become the worker's identity for the rest of its lifetime
// in this process; later callers must agree or get ``BinderException``.
//
// Throws ``IOException`` / ``InvalidInputException`` on launcher failure
// or ``BinderException`` on override mismatch.
std::string ResolveLauncherSocketPath(const std::string &location,
                                       const LaunchOverrides &overrides = {});

// Drop the cached socket path for `location`.  Idempotent.
void InvalidateLauncherSocketCache(const std::string &location);

// Drop every cached entry.  Diagnostic / test helper.
void ClearLauncherSocketCache();

// Resolve the socket path for *location* and connect via AF_UNIX.  On a
// connect failure (the cached worker has idle-shut-down or been killed),
// invalidate the cache, re-resolve (which fires the launcher to spawn a
// fresh worker), and retry once.  Mirrors the subprocess transport's
// force_fresh retry policy and is the missing piece that lets long-lived
// DuckDB sessions survive worker idle-timeouts without manual cache flushes.
//
// ``connect_timeout`` defaults to 10s — generous enough to absorb GIL
// contention and accept-queue delays under burst load.  Throws
// ``IOException`` on a second consecutive connect failure or
// ``BinderException`` on override mismatch.
UnixSocket ResolveAndConnect(
    const std::string &location,
    std::chrono::milliseconds connect_timeout = std::chrono::seconds(10),
    const LaunchOverrides &overrides = {});

} // namespace vgi
} // namespace duckdb
