// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Pure helpers for the Unix-socket worker launcher.  No I/O, no fork, no
// socket calls — every function here is a deterministic transformation that
// can be exhaustively unit-tested without the system under it.
//
// The wire-protocol contract (state-dir layout, hash input, file naming)
// must stay byte-for-byte compatible with the Python reference launcher in
// vgi-rpc/vgi_rpc/launcher.py so that `vgi-rpc launch …` and the C++
// launcher can share a warm worker when both are installed.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {
namespace launcher {

// ---------------------------------------------------------------------------
// Hash + canonical JSON
// ---------------------------------------------------------------------------

// JSON-encode `s` per RFC 8259 with the same escaping rules Python's
// json.dumps() uses by default: backslash, double-quote, control chars
// `\x00..\x1f` as `\u00xx` escapes; printable ASCII passes through; UTF-8
// passes through (Python with ensure_ascii=False would do the same; we use
// the default encoding which the Python launcher matches by setting
// ensure_ascii=True implicitly via separators-only configuration).
//
// Returns the encoded string *including* the surrounding double quotes.
std::string EncodeJsonString(const std::string &s);

// Build the canonical JSON payload that gets SHA-256-hashed:
//
//     {"cmd":["arg0",…],"cwd":"…","env":{"VGI_RPC_FOO":"x",…}}
//
// Rules:
//   - Object keys appear in this exact order: "cmd", "cwd", "env" (matches
//     Python's `json.dumps(..., sort_keys=True)` which sorts ASCII-lex —
//     "cmd" < "cwd" < "env").
//   - The "env" object's keys are themselves sorted ASCII-lex.
//   - No whitespace; separators are exactly "," and ":".
//   - Strings JSON-escaped via EncodeJsonString.
std::string BuildCanonicalJson(const std::vector<std::string> &argv, const std::string &cwd,
                               const std::map<std::string, std::string> &env_subset);

// Compute the 16-hex-char (lowercase) launcher hash for the given inputs.
// Equivalent to:
//   sha256(BuildCanonicalJson(argv, cwd, env)).hexdigest()[:16]
std::string ComputeLauncherHash(const std::vector<std::string> &argv, const std::string &cwd,
                                const std::map<std::string, std::string> &env_subset);

// Filter a flat env map (keys → values) down to entries whose key starts
// with "VGI_RPC_".  Output is a sorted map so callers can feed it directly
// to BuildCanonicalJson / ComputeLauncherHash.
std::map<std::string, std::string> FilterVgiRpcEnv(const std::vector<std::pair<std::string, std::string>> &env);

// ---------------------------------------------------------------------------
// State directory resolution
// ---------------------------------------------------------------------------

// Resolve the per-user state directory for launcher coordination.
//
// POSIX rules (matches vgi_rpc/launcher.py:default_state_dir):
//   - if $XDG_RUNTIME_DIR is set and non-empty:  $XDG_RUNTIME_DIR/vgi-rpc
//   - else if $TMPDIR is set and non-empty:      $TMPDIR/vgi-rpc-$EUID
//   - else:                                      /tmp/vgi-rpc-$EUID
//
// `xdg_runtime_dir`, `tmpdir`, and `euid` are passed in explicitly so the
// function is testable without monkeypatching the process environment.
// Returns the resolved path with no trailing slash.
std::string ResolveStateDir(const std::string &xdg_runtime_dir, const std::string &tmpdir, uint32_t euid);

// ---------------------------------------------------------------------------
// AF_UNIX path-length validation
// ---------------------------------------------------------------------------

// AF_UNIX path length cap.  104 on macOS, 108 on Linux/FreeBSD.  Returns
// the platform-appropriate limit at compile time.
constexpr std::size_t MaxUnixPathLen() {
#if defined(__APPLE__)
	return 104;
#else
	return 108;
#endif
}

// Throws std::invalid_argument if `path` exceeds MaxUnixPathLen() bytes
// (counting the trailing NUL).  Otherwise returns silently.  The launcher
// orchestration layer translates std::invalid_argument into DuckDB's
// InvalidInputException at the boundary; helpers here stay DuckDB-free so
// the unit-test binary doesn't need to link libduckdb_static.
void ValidateUnixPathLength(const std::string &path);

// ---------------------------------------------------------------------------
// `launch:` location string parsing (POSIX shell-quote semantics)
// ---------------------------------------------------------------------------

// Parse the argv portion of a `launch:` location.  `payload` is the part
// *after* the "launch:" prefix.  Supports:
//   - simple words separated by whitespace
//   - double-quoted strings ("…") with backslash escapes for \", \\, \$, \`
//   - single-quoted strings ('…') as raw literals (no escapes)
//   - bare backslash escapes outside quotes
// Throws std::invalid_argument on unterminated quotes or empty argv.
std::vector<std::string> ParseLaunchArgv(const std::string &payload);

// ---------------------------------------------------------------------------
// Discovery-line parsing
// ---------------------------------------------------------------------------

// Result of feeding a chunk of bytes to ParseDiscoveryLine.
enum class DiscoveryParseResult {
	kNeedMore,   // No complete line yet; keep reading.
	kFound,      // The expected UNIX:<path> line has been consumed.
	kMismatch,   // A UNIX: line was seen but its path doesn't match expected.
};

// Stateful line-by-line scanner that skips noise lines until it sees a
// UNIX:<path> line matching `expected_path`.  The caller owns a `buffer`
// string into which it appends new bytes from the worker's stdout; this
// function consumes complete `\n`-terminated lines from the front of the
// buffer.
//
// Returns kFound the first time a `UNIX:<expected_path>` line is fully
// consumed.  Returns kMismatch if a `UNIX:` line is seen but its path
// differs from expected (callers should treat this as a fatal worker bug).
// Returns kNeedMore otherwise (no terminator yet, or only non-UNIX prefix
// lines have been consumed).
DiscoveryParseResult ParseDiscoveryLine(std::string &buffer, const std::string &expected_path);

// ---------------------------------------------------------------------------
// Transport detection (location-string scheme dispatching)
// ---------------------------------------------------------------------------

bool IsUnixLocation(const std::string &location);
bool IsLaunchLocation(const std::string &location);

} // namespace launcher
} // namespace vgi
} // namespace duckdb
