// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <string>

namespace duckdb {
namespace vgi {

// SUBPROCESS — bare command path; spawn as a stdin/stdout child.
// HTTP       — http(s):// URL; talk Falcon-style HTTP.
// UNIX       — unix:///path/to/sock; connect to an existing AF_UNIX worker.
// LAUNCH     — launch:<argv>; spawn-or-reuse via the C++ launcher, then
//              connect via AF_UNIX.
enum class TransportType { SUBPROCESS, HTTP, UNIX, LAUNCH };

// Detect what kind of worker location this string represents.  Pure on
// inputs — no I/O, no ambient state.
TransportType DetectTransport(const std::string &worker_path);

// Convenience predicates.  Mutually exclusive (a string matches at most one).
bool IsHttpTransport(const std::string &worker_path);
bool IsUnixLocation(const std::string &worker_path);
bool IsLaunchLocation(const std::string &worker_path);

// Strip the leading scheme prefix from a unix:// or launch: location.
// For unix://, returns the absolute filesystem path.  For launch:, returns
// the shell-quoted argv payload (caller still needs to ParseLaunchArgv it).
// Throws std::invalid_argument if the location doesn't carry the expected
// prefix.
std::string StripUnixScheme(const std::string &location);
std::string StripLaunchScheme(const std::string &location);

} // namespace vgi
} // namespace duckdb
