// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Pure (no I/O) helpers for the Unix-socket worker launcher, unit-testable in
// isolation. State-dir layout and hash input must stay byte-for-byte compatible
// with vgi-rpc's vgi_rpc/launcher.py so both launchers share a warm worker.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {
namespace launcher {

// JSON-encode `s` (including the surrounding quotes) exactly as Python's
// json.dumps (ensure_ascii=True) encodes os.fsdecode(s) on a UTF-8 filesystem
// encoding: non-ASCII and DEL become \uXXXX (surrogate pairs above the BMP) and
// each byte of invalid UTF-8 becomes its surrogateescape \udcXX. Non-UTF-8
// Python locales decode differently and are not matched.
std::string EncodeJsonString(const std::string &s);

// Canonical hash input, matching Python's
// json.dumps(..., sort_keys=True, separators=(",", ":")):
//     {"cmd":["arg0",…],"cwd":"…","env":{"VGI_RPC_FOO":"x",…}}
// with "env" keys sorted by decoded code point (Python str order).
std::string BuildCanonicalJson(const std::vector<std::string> &argv, const std::string &cwd,
                               const std::map<std::string, std::string> &env_subset);

// sha256(BuildCanonicalJson(argv, cwd, env)).hexdigest()[:16]
std::string ComputeLauncherHash(const std::vector<std::string> &argv, const std::string &cwd,
                                const std::map<std::string, std::string> &env_subset);

// Keep only entries whose key starts with "VGI_RPC_".
std::map<std::string, std::string> FilterVgiRpcEnv(const std::vector<std::pair<std::string, std::string>> &env);

// Per-user launcher state directory (mirrors launcher.py default_state_dir):
//   POSIX:   $XDG_RUNTIME_DIR/vgi-rpc, else ${TMPDIR:-/tmp}/vgi-rpc-$EUID
//   Windows: <tmpdir>\vgi-rpc (xdg_runtime_dir and euid ignored)
// Empty arguments count as unset. No trailing slash.
std::string ResolveStateDir(const std::string &xdg_runtime_dir, const std::string &tmpdir, uint32_t euid);

// AF_UNIX sun_path size, including the trailing NUL.
constexpr std::size_t MaxUnixPathLen() {
#if defined(__APPLE__)
	return 104;
#else
	return 108;
#endif
}

// Throws std::invalid_argument if `path` exceeds the AF_UNIX sun_path cap
// (POSIX) or the named-pipe name cap (Windows). Helpers here throw std
// exceptions, not DuckDB ones, so the unit-test binary need not link libduckdb.
void ValidateRendezvousPathLength(const std::string &path);

// Split the part after "launch:" into argv with POSIX shell quoting: '…' raw,
// "…" with \" \\ \$ \` escapes, bare backslash escapes (backslash is literal
// on Windows). No expansion. Throws std::invalid_argument on unterminated
// quotes or empty argv.
std::vector<std::string> ParseLaunchArgv(const std::string &payload);

// Prefix of the stdout line a worker prints to advertise its rendezvous
// address; protocol constant shared with vgi-rpc.
inline const char *DiscoveryLinePrefix() {
#if defined(_WIN32)
	return "PIPE:";
#else
	return "UNIX:";
#endif
}

// Result of feeding a chunk of bytes to ParseDiscoveryLine.
enum class DiscoveryParseResult {
	kNeedMore,   // No complete line yet; keep reading.
	kFound,      // The expected <prefix><path> line has been consumed.
	kMismatch,   // A <prefix> line was seen but its path doesn't match expected.
};

// Consumes complete lines from the front of `buffer` (the caller appends worker
// stdout to it), skipping lines without `prefix`. kFound when a
// <prefix><expected_path> line is consumed; kMismatch (a worker bug) when a
// <prefix> line names another path; else kNeedMore. Pass DiscoveryLinePrefix()
// for the platform's prefix.
DiscoveryParseResult ParseDiscoveryLine(std::string &buffer, const std::string &expected_path,
                                         const std::string &prefix = "UNIX:");

bool IsUnixLocation(const std::string &location);
bool IsLaunchLocation(const std::string &location);

} // namespace launcher
} // namespace vgi
} // namespace duckdb
