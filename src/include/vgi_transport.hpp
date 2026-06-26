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
// CONTAINER  — oci://image[:tag] (or the docker:// alias); spawn the worker
//              inside an OCI container via the host container runtime
//              (docker/podman/…), wired over stdin/stdout like a subprocess.
//              See vgi_container_runtime.hpp.
// TCP        — tcp://host:port; connect to an existing out-of-band worker
//              listening on a raw-TCP socket (vgi-rpc serve_tcp). Connect-only,
//              like UNIX. Used by wasm workers (wasip2 has TCP sockets but no
//              AF_UNIX), and for any worker reachable over loopback/trusted TCP.
enum class TransportType { SUBPROCESS, HTTP, UNIX, LAUNCH, CONTAINER, TCP };

// Detect what kind of worker location this string represents.  Pure on
// inputs — no I/O, no ambient state.
TransportType DetectTransport(const std::string &worker_path);

// Convenience predicates.  Mutually exclusive (a string matches at most one).
bool IsHttpTransport(const std::string &worker_path);
bool IsUnixLocation(const std::string &worker_path);
bool IsLaunchLocation(const std::string &worker_path);
// Container location: oci:// (canonical) or docker:// (alias).  A trailing
// "#<hash>" pool-disambiguation suffix (appended at ATTACH) does not affect
// the match — only the scheme prefix is inspected.
bool IsContainerLocation(const std::string &worker_path);

// Internal `container-shared:` token (synthesized at ATTACH; never user-typed)
// for a transparently-reused shared container. The dispatch resolves it via the
// shared-container coordinator at connection time. See vgi_container_runtime.hpp.
bool IsContainerSharedLocation(const std::string &worker_path);

// Raw-TCP location: tcp://host:port. Connect-only against an out-of-band worker
// (vgi-rpc serve_tcp).
bool IsTcpTransport(const std::string &worker_path);

// Parse a tcp:// location into (host, port). Throws std::invalid_argument if the
// location is not a well-formed tcp://host:port.
void ParseTcpLocation(const std::string &location, std::string &host, int &port);

// Strip the leading scheme prefix from a unix:// or launch: location.
// For unix://, returns the absolute filesystem path.  For launch:, returns
// the shell-quoted argv payload (caller still needs to ParseLaunchArgv it).
// Throws std::invalid_argument if the location doesn't carry the expected
// prefix.
std::string StripUnixScheme(const std::string &location);
std::string StripLaunchScheme(const std::string &location);

// Strip the oci:// / docker:// prefix from a container location, returning the
// image reference (e.g. "ghcr.io/query-farm/vgi-sklearn:latest").  A trailing
// "#<hash>" pool-disambiguation suffix, if present, is removed too.  Throws
// std::invalid_argument if the location is not a container location.
std::string StripContainerScheme(const std::string &location);

} // namespace vgi
} // namespace duckdb
