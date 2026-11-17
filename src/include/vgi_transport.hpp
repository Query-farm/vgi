// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <string>

namespace duckdb {
namespace vgi {

// SUBPROCESS — bare command path; spawn as a stdin/stdout child.
// HTTP       — http(s):// URL; the VGI HTTP transport.
// UNIX       — unix:///path/to/sock; connect to an existing AF_UNIX worker.
// LAUNCH     — launch:<argv>; spawn-or-reuse via the C++ launcher, then
//              connect via AF_UNIX.
// CONTAINER  — oci://image[:tag] (or the docker:// alias); spawn the worker
//              inside an OCI container via the host container runtime
//              (docker/podman/…), wired over stdin/stdout like a subprocess.
//              See vgi_container_runtime.hpp.
// TCP        — tcp://host:port or tcp://[IPv6]:port; connect-only to a raw-TCP
//              worker (vgi-rpc serve_tcp), e.g. wasip2 workers, which lack AF_UNIX.
// WEBWORKER  — worker:<url>; in-browser Web Worker over a SharedArrayBuffer duplex
//              ring (DuckDB-WASM only). The <url> is whatever `new Worker(url)`
//              accepts. No spawn/pool on the C++ side; the JS bridge owns the
//              worker. See vgi_sab_abi.hpp / docs/sab_transport_abi.md.
// IROH       — iroh://<EndpointId>; native builds use an embedded Iroh client;
//              browser builds delegate to an application-owned adapter worker.
// HTTPI      — httpi://<EndpointId>[/base/path]; the ordinary VGI HTTP state
//              machine with only its HTTP backend delegated to that adapter.
// DATABASE   — a database-package worker (IsDatabaseLocation), run over SUBPROCESS.
enum class TransportType { SUBPROCESS, HTTP, UNIX, LAUNCH, CONTAINER, TCP, WEBWORKER, IROH, HTTPI, DATABASE };

// The transport a worker location selects. Pure: no I/O, no ambient state.
TransportType DetectTransport(const std::string &worker_path);

// Mutually exclusive predicates (a string matches at most one).
bool IsHttpTransport(const std::string &worker_path);
bool IsUnixLocation(const std::string &worker_path);
bool IsLaunchLocation(const std::string &worker_path);
// oci:// (canonical) or docker:// (alias); only the scheme is inspected, so the
// "#<hash>" pool-disambiguation suffix appended at ATTACH does not matter.
bool IsContainerLocation(const std::string &worker_path);

// Internal `container-shared:` token (synthesized at ATTACH, never user-typed)
// for a reused shared container, resolved by the shared-container coordinator
// at connection time. See vgi_container_runtime.hpp.
bool IsContainerSharedLocation(const std::string &worker_path);

// tcp://host:port or tcp://[IPv6]:port; hostnames resolve at connect time.
bool IsTcpTransport(const std::string &worker_path);

// orchard://<workspace>/<deployment>: an Orchard deployment. Not a transport
// of its own: ATTACH signs in at the Orchard Front Desk and rewrites it to the
// deployment's https Field Address (see vgi_orchard.hpp). Scheme is
// case-insensitive.
bool IsOrchardLocation(const std::string &worker_path);

// worker:<url> (DuckDB-WASM only).
bool IsWebWorkerTransport(const std::string &worker_path);

// Iroh target. The scheme and 64-character hexadecimal EndpointId are both
// strict lowercase, matching the cross-language transport contract.
bool IsIrohTransport(const std::string &worker_path);
std::string CanonicalizeIrohLocation(const std::string &location);

// Browser HTTP-over-Iroh target. The EndpointId is strict lowercase hex. The
// optional base path is absolute, contains no query/fragment/backslash/control
// characters or dot segments, and is canonicalized without a trailing slash.
bool IsHttpiTransport(const std::string &worker_path);
std::string CanonicalizeHttpiLocation(const std::string &location);

struct HttpiUrlParts {
	std::string endpoint_id;
	std::string path;
};

// Parse either a base LOCATION or the per-method URL derived from it. `path` is
// empty for a bare endpoint and otherwise begins with '/'.
HttpiUrlParts ParseHttpiUrl(const std::string &location);

// Return the canonical target identifier handed to the page bridge: worker:
// locations lose their legacy prefix; iroh:// locations remain full canonical
// targets. Throws for every other transport and for malformed Iroh IDs.
std::string CanonicalizeBrowserWorkerTarget(const std::string &location);

// Database-backed worker package. The user-facing URI is resolved once during
// ATTACH into an immutable cached artifact, then executed over SUBPROCESS.
bool IsDatabaseLocation(const std::string &worker_path);

// Internal token produced by the database package resolver. Never user-typed.
bool IsResolvedWorkerLocation(const std::string &worker_path);

// Strip the `worker:` scheme prefix, returning the (case-preserved) URL/name the
// JS bridge resolves to a Web Worker. Throws std::invalid_argument if the
// location is not a worker: location.
std::string StripWebWorkerScheme(const std::string &location);

// Parse a tcp:// location into (host, port), removing IPv6 URI brackets. Throws
// std::invalid_argument if the authority is malformed or ambiguous.
void ParseTcpLocation(const std::string &location, std::string &host, int &port);

// Strip the unix:// / launch: prefix: the absolute socket path, or the
// shell-quoted argv payload (for ParseLaunchArgv). Throws std::invalid_argument
// without the expected prefix.
std::string StripUnixScheme(const std::string &location);
std::string StripLaunchScheme(const std::string &location);

// The image reference of a container location (e.g.
// "ghcr.io/query-farm/vgi-sklearn:latest"), without any "#<hash>" suffix.
// Throws std::invalid_argument for a non-container location.
std::string StripContainerScheme(const std::string &location);

// GitHub release assets (not the container registry): `github://` names the
// asset, `github-auto://` derives it by convention (vgi_github.hpp). Disjoint
// schemes. Run over the subprocess transport; the dispatch throws on builds
// without it.
bool IsGithubLocation(const std::string &worker_path);
bool IsGithubAutoLocation(const std::string &worker_path);

// Strip the scheme, keeping the case-sensitive remainder including any
// `#sha256=`/`#path=` fragment (the coordinate parser needs it). Throws
// std::invalid_argument without the expected prefix.
std::string StripGithubScheme(const std::string &location);
std::string StripGithubAutoScheme(const std::string &location);

} // namespace vgi
} // namespace duckdb
