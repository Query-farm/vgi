// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Public façade for the Unix-socket worker launcher.  Brings up — or reuses —
// a long-running worker process that the calling DuckDB extension can then
// connect to via AF_UNIX.
//
// Concurrency contract: at most one worker exists per
// (worker_argv, cwd, VGI_RPC_*-env) tuple, system-wide and across DuckDB
// processes.  Coordination is via per-hash flock in a per-user state dir.
// See vgi_launcher_internal.hpp for the byte-exact wire-protocol contract
// shared with the Python reference implementation.

#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#ifdef _WIN32
#error "VGI Unix-socket launcher is currently POSIX-only."
#endif

namespace duckdb {
namespace vgi {

struct LaunchConfig {
	// The worker command and its arguments.  Must be non-empty.  ``--unix
	// PATH`` and ``--idle-timeout SEC`` are appended automatically; do not
	// include them yourself.
	std::vector<std::string> worker_argv;

	// Optional explicit socket path (skips the per-hash machinery — useful
	// for tests).  When unset, derived from the hash of (argv, cwd, env).
	std::optional<std::string> socket_path_override;

	// Worker self-shutdown after this many seconds of zero connected clients.
	std::chrono::milliseconds idle_timeout = std::chrono::seconds(300);

	// Maximum time we'll wait for the per-hash flock to be acquirable.
	std::chrono::milliseconds connect_timeout = std::chrono::seconds(30);

	// Maximum time we'll wait for the worker to print its
	// ``UNIX:<path>`` discovery line on stdout.  JVM cold-start dominates
	// in practice, so the default is generous.
	std::chrono::milliseconds worker_startup_timeout = std::chrono::seconds(60);

	// Optional path for capturing worker stderr.  Default: discard (the
	// fd is dup'd onto /dev/null in the child).
	std::optional<std::string> worker_stderr_path;

	// Optional override of the per-user state directory (where lockfiles
	// and sockets live).  Default: resolved from $XDG_RUNTIME_DIR / $TMPDIR
	// / euid per the Python contract.
	std::optional<std::string> state_dir_override;
};

// Bring up (or reuse) a worker per ``cfg``; return the absolute AF_UNIX
// socket path the caller should connect to.
//
// Throws ``duckdb::IOException`` on any failure to bring up a worker:
// flock contention timeout, worker exits before readiness, worker startup
// timeout, etc.  Always either returns a path or throws — never
// silent-fails or returns an empty string.
//
// Thread-safe: concurrent calls from multiple DuckDB threads with the same
// ``cfg`` will serialise on the kernel flock, with at most one of them
// actually performing the spawn.
std::string Launch(const LaunchConfig &cfg);

} // namespace vgi
} // namespace duckdb
