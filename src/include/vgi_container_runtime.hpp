// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Container (Docker/OCI) worker transport.
//
// A VGI worker can be distributed as an OCI image and run inside a container by
// the host's container runtime (docker, podman, nerdctl, or Apple `container`).
// The worker speaks the same stdin/stdout Arrow-IPC protocol as a bare-command
// subprocess; we just spawn `docker run -i … IMG stdio` instead of the worker
// binary directly.  Because the spawned `docker` CLI's stdin/stdout *are* the
// container's, the existing SubProcess pipe machinery and VgiWorkerPool drive it
// unchanged — keeping the `docker run` process pooled keeps the container warm.
//
// The container-specific work lives entirely here: runtime detection, image
// inspection (auto-mounting volumes the image declares via the
// `farm.query.vgi.volumes` OCI label), run-command construction, a per-process
// launch registry keyed by the (disambiguated) LOCATION, and a small
// SubProcess subclass that force-removes its container on teardown.
//
// Both worker spawn sites (vgi_unary_rpc.cpp and FunctionConnection::
// EnsureWorkerSpawned) route through SpawnWorker(), which consults the registry:
// container locations build a ContainerWorker, everything else a plain
// SubProcess.  See docs/container-transport.md.

#include <memory>
#include <string>
#include <vector>

#include "vgi_platform.hpp"
#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// A resolved container runtime: the CLI binary to invoke and a short kind tag.
struct ContainerRuntime {
	std::string binary;  // resolved executable, e.g. "/usr/local/bin/docker" or "docker"
	std::string kind;    // "docker" | "podman" | "nerdctl" | "container"
};

// One host→container mount.  `host` is a named volume (preferred) or an absolute
// host path; `path` is the in-container mount point.
struct ContainerVolume {
	std::string host;
	std::string path;
};

// Everything needed to build a `docker run …` invocation for one catalog.
struct ContainerSpec {
	ContainerRuntime runtime;
	std::string image;             // "ghcr.io/query-farm/vgi-sklearn:latest"
	std::string transport = "stdio";  // v1 only supports "stdio"
	std::vector<ContainerVolume> volumes;
	std::vector<std::string> env;        // "KEY=VALUE" entries (-e)
	std::vector<std::string> extra_args; // raw passthrough flags (already tokenized)
};

// Detect a usable container runtime.  If `override_kind` is non-empty it must be
// one of docker/podman/nerdctl/container and is required (errors if absent).
// Otherwise probes PATH in order: docker → podman → nerdctl → container.
// Throws IOException listing what was searched when none is found.
ContainerRuntime DetectContainerRuntime(const std::string &override_kind);

// Read the image's `farm.query.vgi.volumes` OCI label and return the volumes it
// declares (possibly empty).  Runs `<runtime> image inspect …`; on a
// "no such image" miss it runs `<runtime> pull <image>` once (deferring auth to
// the runtime's own login) and retries.  Throws IOException on a non-missing
// inspect/pull failure.
std::vector<ContainerVolume> InspectImageVolumes(const ContainerRuntime &runtime, const std::string &image);

// Build the `docker run …` command template as a single shell command string
// (suitable for SubProcess's `/bin/sh -c`).  Contains the literal token
// kContainerNamePlaceholder where a unique `--name` is substituted at spawn
// time, plus a stable `--label` so the orphan reaper can find our containers.
std::string BuildContainerRunCommandTemplate(const ContainerSpec &spec);

// Stable (within this process) 8-hex digest over the spec's pooling-relevant
// fields — runtime, image, sorted volumes, sorted env, extra_args, transport.
// Appended to the LOCATION as "#<hash>" so two catalogs on the same image but
// different options land in different worker-pool buckets.  Excludes the
// per-spawn container name.
std::string ContainerSpecHash(const ContainerSpec &spec);

// Per-process launch registry.  ATTACH registers the resolved runtime binary +
// command template under the (disambiguated) LOCATION; the spawn sites look it
// up.  Idempotent; a second register for the same key overwrites (re-ATTACH).
void RegisterContainerLaunch(const std::string &location, const std::string &runtime_binary,
                             const std::string &command_template);

// True if `location` already has a registry entry.
bool IsContainerRegistered(const std::string &location);

// Resolve `location` with default options (auto runtime, image-declared volumes,
// no extra env/args) and register it, unless it is already registered.  Lets
// the pre-ATTACH entry point (vgi_catalogs) use a
// container LOCATION without an explicit ATTACH-time resolution.  Performs image
// inspection (and a pull on cache miss); throws on no-runtime / inspect failure.
void EnsureContainerRegistered(const std::string &location);

#if VGI_SUBPROCESS_TRANSPORT
// Spawn a worker process for a bare-command OR container LOCATION.  Container
// locations (per IsContainerLocation) are resolved through the launch registry
// into a ContainerWorker; everything else becomes a plain SubProcess running
// `worker_path` as a shell command.  Throws IOException if a container location
// has no registry entry (i.e. ATTACH didn't resolve it).
std::unique_ptr<SubProcess> SpawnWorker(const std::string &worker_path, bool worker_debug);

// SubProcess subclass for the container transport.  The base ctor fork/execs
// `<runtime> run … --name <name> … IMG stdio` (via /bin/sh -c); the dtor adds a
// best-effort `<runtime> rm -f <name>` so a container orphaned by a SIGKILL of
// the `docker` CLI (where `--rm` never fires) is still cleaned up.
class ContainerWorker : public SubProcess {
public:
	ContainerWorker(const std::string &final_command, std::string runtime_binary, std::string container_name,
	                bool stderr_passthrough);
	~ContainerWorker() override;

private:
	std::string runtime_binary_;
	std::string container_name_;
};

// Best-effort cleanup of orphan VGI containers left by dead DuckDB processes
// (e.g. SIGKILL of the whole DuckDB process, where neither `--rm` nor the
// ContainerWorker dtor ran).  Lists our labeled containers, parses the owning
// pid out of the name, and force-removes any whose owner is no longer alive.
// Never throws.  Intended to run once per process on first container ATTACH.
void ReapOrphanContainers(const ContainerRuntime &runtime) noexcept;
#endif // VGI_SUBPROCESS_TRANSPORT

// The placeholder token in the command template replaced with a unique
// `--name` value at spawn time.  Exposed for tests.
extern const char *const kContainerNamePlaceholder;

// The OCI label whose JSON value declares the image's required volumes.
extern const char *const kVgiVolumesLabel;

// ============================================================================
// Shared (transparently reused) container workers
//
// Unlike the per-process `oci://… stdio` path above, a *shared* container is
// started once and reused system-wide: every DuckDB process on the host that
// asks for the same image+options connects to one long-lived container. The
// daemon is the registry — we name the container deterministically
// (vgi-rpc-<ContainerSpecHash>) and introspect (`docker inspect`) to
// find-or-create it. The worker inside exposes one of several servers; the
// container records which via labels so reuse never has to assume. See
// docs/container-transport.md ("shared").
// ============================================================================

// How the host reaches the worker inside a shared container.
enum class ContainerConnMode { HTTP, TCP, UNIX };

ContainerConnMode ParseContainerConnMode(const std::string &s);  // throws on unknown
const char *ContainerConnModeName(ContainerConnMode mode);

// The OCI label by which an image advertises which server modes it supports,
// e.g. farm.query.vgi.transports=["http","tcp","unix"].
extern const char *const kVgiTransportsLabel;

// Container labels stamped at `docker run` recording how THIS container was
// launched (distinct keys from the image-baked capability label above).
extern const char *const kVgiWorkerLabel;  // farm.query.vgi.worker=1
extern const char *const kVgiConnLabel;    // farm.query.vgi.conn=<mode>
extern const char *const kVgiCportLabel;   // farm.query.vgi.cport=<container port>

// A live endpoint for a shared container, resolved by the coordinator.
struct ContainerEndpoint {
	ContainerConnMode mode = ContainerConnMode::HTTP;
	std::string container_name;
	// http/tcp: loopback host mapping of the container's published port.
	std::string host;   // "127.0.0.1"
	int port = 0;       // host-side port
	std::string url;    // "http://127.0.0.1:<port>" (http only)
	// unix: bind-mounted socket path (host == container path).
	std::string socket_path;
};

// Read the image's advertised transports label. Empty when the image declares
// none (→ the image isn't shareable; caller falls back to per-process stdio).
std::vector<ContainerConnMode> InspectImageTransports(const ContainerRuntime &runtime, const std::string &image);

// Ensure the shared container for `spec` is running and return a live endpoint.
// Deterministic name vgi-rpc-<ContainerSpecHash(spec)>; find-or-create via the
// daemon (race-safe: a lost `--name` race re-introspects the winner's
// container). `requested_mode` is used only when *creating*; a container already
// running wins with its own recorded `conn` label. Polls until the endpoint
// accepts a connection or a container-sized deadline elapses. Invoked at
// connection time so it self-heals idle-stopped containers. Throws IOException
// on failure. POSIX-only (uses sockets); guarded by VGI_SUBPROCESS_TRANSPORT.
ContainerEndpoint EnsureSharedContainer(const ContainerSpec &spec, ContainerConnMode requested_mode);

// Drop the cached live endpoint for `spec`, forcing the next EnsureSharedContainer
// to re-introspect (and re-create if the container died). Call after a connect
// failure to recover from a stale/restarted container.
void InvalidateSharedContainer(const ContainerSpec &spec);

#if VGI_SUBPROCESS_TRANSPORT
// Open a connection to a resolved socket endpoint (tcp/unix) and wrap the
// connected fd in a UnixSocketWorker, so the existing fd-based FunctionConnection
// / WriteRpcRequest path drives it. Http endpoints don't use this (they go through
// HttpFunctionConnection). Throws IOException on connect failure. POSIX-only.
std::unique_ptr<SubProcess> ConnectSharedContainer(const ContainerEndpoint &endpoint);

// Open a raw TCP connection to host:port, returning a connected fd (or -1 on
// failure). Shared by the container-shared TCP mode and the tcp:// transport.
int TcpConnect(const std::string &host, int port, int timeout_ms);
#endif

// Per-process registry mapping an internal `container-shared:` worker_path to its
// resolved {spec, requested_mode}. Populated at ATTACH; read by the dispatch
// seams at connection time (which then call EnsureSharedContainer).
void RegisterSharedContainer(const std::string &location, const ContainerSpec &spec,
                             ContainerConnMode requested_mode);
bool LookupSharedContainer(const std::string &location, ContainerSpec &out_spec,
                           ContainerConnMode &out_mode);

// Best-effort: `docker rm -f` shared (vgi-rpc-*) containers whose endpoint no
// longer connects. Backstop for a worker that ignored its idle-timeout after all
// clients died. Never throws; run once per process.
void ReapDeadSharedContainers(const ContainerRuntime &runtime) noexcept;

} // namespace vgi
} // namespace duckdb
