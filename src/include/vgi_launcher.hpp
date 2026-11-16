// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Worker launcher: brings up, or reuses, a long-running shared worker reached
// over AF_UNIX (POSIX) or a named pipe (Windows).
//
// At most one worker exists per (worker_argv, cwd, VGI_RPC_*-env) tuple,
// system-wide and across DuckDB processes; spawns are serialised on a per-hash
// flock (POSIX) or named mutex (Windows). vgi_launcher_internal.hpp holds the
// hash / discovery-line contract shared with the Python implementation.

#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "vgi_platform.hpp"

// The launcher exists on POSIX (AF_UNIX rendezvous) and Windows (named-pipe
// rendezvous). Emscripten has neither.
#if VGI_POSIX_TRANSPORT || defined(_WIN32)

namespace duckdb {
namespace vgi {

struct LaunchConfig {
	// Worker command and arguments; must be non-empty. ``--unix PATH`` and
	// ``--idle-timeout SEC`` are appended by Launch().
	std::vector<std::string> worker_argv;

	// Explicit socket path (skips the per-hash naming; for tests). When unset,
	// derived from the hash of (argv, cwd, env).
	std::optional<std::string> socket_path_override;

	// Worker self-shutdown after this many seconds of zero connected clients.
	std::chrono::milliseconds idle_timeout = std::chrono::seconds(300);

	// Maximum wait for the per-hash spawn lock.
	std::chrono::milliseconds connect_timeout = std::chrono::seconds(30);

	// Maximum wait for the worker's discovery line on stdout. Generous because
	// JVM cold start dominates.
	std::chrono::milliseconds worker_startup_timeout = std::chrono::seconds(60);

	// File to append worker stderr to. Default: discarded once the worker is
	// ready (startup stderr is kept only for launch error messages).
	std::optional<std::string> worker_stderr_path;

	// Per-user state directory for lockfiles and sockets (POSIX only).
	// Default: from $XDG_RUNTIME_DIR / $TMPDIR / euid, as the Python launcher.
	std::optional<std::string> state_dir_override;
};

// Bring up (or reuse) a worker per ``cfg`` and return the rendezvous path to
// connect to (AF_UNIX socket path, or named-pipe name on Windows). Never
// returns empty: throws IOException on lock timeout, early worker exit,
// startup timeout, etc. Thread-safe: concurrent calls with the same ``cfg``
// serialise on the spawn lock and at most one spawns.
std::string Launch(const LaunchConfig &cfg);

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT || _WIN32
