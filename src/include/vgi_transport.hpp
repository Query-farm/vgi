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
// WEBWORKER  — worker:<url>; in-browser Web Worker over a SharedArrayBuffer duplex
//              ring (DuckDB-WASM only). The <url> is whatever `new Worker(url)`
//              accepts. No spawn/pool on the C++ side; the JS bridge owns the
//              worker. See vgi_sab_abi.hpp / docs/sab_transport_abi.md.
enum class TransportType { SUBPROCESS, HTTP, UNIX, LAUNCH, CONTAINER, TCP, WEBWORKER };

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

// Web Worker location: worker:<url>. In-browser SharedArrayBuffer transport
// (DuckDB-WASM only); the JS bridge owns worker lifecycle.
bool IsWebWorkerTransport(const std::string &worker_path);

// Strip the `worker:` scheme prefix, returning the (case-preserved) URL/name the
// JS bridge resolves to a Web Worker. Throws std::invalid_argument if the
// location is not a worker: location.
std::string StripWebWorkerScheme(const std::string &location);

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

// GitHub-release locations.  `github://` downloads a named release asset and runs
// it over the subprocess transport; `github-auto://` builds the asset name from a
// fixed convention (see vgi_github.hpp).  These are distinct schemes — note that
// `github-auto://` does NOT start with `github://`, so the predicates don't
// overlap.  Both are POSIX-only (the cache uses flock/rename); the dispatch throws
// on other builds.  Unlike `oci://ghcr.io/...` (GitHub's *container* registry),
// these fetch *release assets*.
bool IsGithubLocation(const std::string &worker_path);
bool IsGithubAutoLocation(const std::string &worker_path);

// Strip the `github://` / `github-auto://` scheme prefix, preserving the
// case-sensitive remainder (owner/repo are case-sensitive) INCLUDING any
// `#sha256=`/`#path=` fragment — the coordinate parser in vgi_github.cpp needs it.
// Throws std::invalid_argument if the location doesn't carry the expected prefix.
std::string StripGithubScheme(const std::string &location);
std::string StripGithubAutoScheme(const std::string &location);

} // namespace vgi
} // namespace duckdb
