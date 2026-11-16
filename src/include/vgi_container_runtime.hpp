// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Container (Docker/OCI) worker transport. A worker image runs under docker,
// podman, nerdctl or Apple `container` as `<runtime> run -i … IMG stdio`; the
// CLI's stdin/stdout are the container's, so SubProcess and VgiWorkerPool drive
// it unchanged (a pooled `run` process keeps the container warm). Both spawn
// sites (vgi_unary_rpc.cpp, FunctionConnection) go through SpawnWorker().
// See docs/container-transport.md.

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

// Detect a usable container runtime. A non-empty `override_kind` must be one of
// docker/podman/nerdctl/container and must be on PATH. Otherwise probes PATH in
// that order; with several installed, the first whose daemon answers
// `<runtime> info` wins (`container` is taken as-is), else the first found.
// Throws IOException when none is found.
ContainerRuntime DetectContainerRuntime(const std::string &override_kind);

// When `output` from a `<runtime> ...` command shows the runtime is installed
// but unusable (daemon/VM not running, socket not permitted), a sentence saying
// so and how to fix it; otherwise "".
std::string ContainerRuntimeUnavailableReason(const std::string &kind, const std::string &output);

// Return the volumes the image's kVgiVolumesLabel declares (possibly empty).
// On an inspect failure it pulls once (auth deferred to the runtime's login) and
// retries. Throws IOException if the pull or retry fails or the label is
// malformed. Label-supplied names are sanitized so they can only be named
// volumes, never host bind mounts.
std::vector<ContainerVolume> InspectImageVolumes(const ContainerRuntime &runtime, const std::string &image);

// Build the `<runtime> run …` command as one `/bin/sh -c` string. It contains
// kContainerNamePlaceholder (replaced by a unique `--name` at spawn) and a
// stable `--label` so the orphan reaper can find our containers.
std::string BuildContainerRunCommandTemplate(const ContainerSpec &spec);

// 8-hex FNV-1a digest of runtime kind, image, transport, sorted volumes, sorted
// env and ordered extra_args. Appended to the LOCATION as "#<hash>" so catalogs
// on the same image with different options get different pool buckets; also
// names shared containers (vgi-rpc-<hash>).
std::string ContainerSpecHash(const ContainerSpec &spec);

// Per-process launch registry: ATTACH registers the runtime binary and command
// template under the (disambiguated) LOCATION; spawn sites look it up. A second
// register for the same key overwrites (re-ATTACH).
void RegisterContainerLaunch(const std::string &location, const std::string &runtime_binary,
                             const std::string &command_template);

bool IsContainerRegistered(const std::string &location);

// Unless already registered, resolve `location` with defaults (auto runtime,
// image-declared volumes, no extra env/args) and register it, so pre-ATTACH
// entry points (vgi_catalogs, vgi_protocols) can use a raw oci:// LOCATION.
// May inspect/pull the image; throws on no runtime or inspect failure.
void EnsureContainerRegistered(const std::string &location);

#if VGI_SUBPROCESS_TRANSPORT
// Spawn a worker for a resolved-artifact, container or bare-command LOCATION.
// Container locations become a ContainerWorker via the launch registry
// (resolving with defaults on first use); bare commands a plain SubProcess.
std::unique_ptr<SubProcess> SpawnWorker(const std::string &worker_path, bool worker_debug);

// The dtor adds a detached best-effort `<runtime> rm -f <name>`: a SIGKILL of
// the `run` CLI detaches the container and `--rm` never fires.
class ContainerWorker : public SubProcess {
public:
	ContainerWorker(const std::string &final_command, std::string runtime_binary, std::string container_name,
	                bool stderr_passthrough);
	~ContainerWorker() override;

private:
	std::string runtime_binary_;
	std::string container_name_;
};

// Best-effort removal of per-process containers (vgi-rpc-<pid>-<n>) whose
// owning DuckDB pid is no longer alive, e.g. after a SIGKILL of DuckDB itself.
// Never throws; run once per process on first container ATTACH.
void ReapOrphanContainers(const ContainerRuntime &runtime) noexcept;
#endif // VGI_SUBPROCESS_TRANSPORT

#if VGI_POSIX_TRANSPORT
// True only when `pid` certainly no longer exists (kill(pid, 0) fails with
// ESRCH). EPERM means alive under another user, so it is not gone.
bool ContainerOwnerPidIsGone(long long pid) noexcept;
#endif

// Exposed for tests.
extern const char *const kContainerNamePlaceholder;

// The OCI label whose JSON value declares the image's required volumes.
extern const char *const kVgiVolumesLabel;

// Shared container workers: one long-lived container per image+options,
// reused by every DuckDB process on the host. The daemon is the registry: the
// container is named vgi-rpc-<ContainerSpecHash> and found or created via
// `inspect`; labels record which server mode it was started with.

// How the host reaches the worker inside a shared container.
enum class ContainerConnMode { HTTP, TCP, UNIX };

ContainerConnMode ParseContainerConnMode(const std::string &s);  // throws on unknown
const char *ContainerConnModeName(ContainerConnMode mode);

// Image label advertising supported server modes, e.g. ["http","tcp","unix"].
extern const char *const kVgiTransportsLabel;

// Labels stamped at `run` recording how this container was launched.
extern const char *const kVgiWorkerLabel;  // farm.query.vgi.worker=1
extern const char *const kVgiConnLabel;    // farm.query.vgi.conn=<mode>
extern const char *const kVgiCportLabel;   // farm.query.vgi.cport=<container port>

struct ContainerEndpoint {
	ContainerConnMode mode = ContainerConnMode::HTTP;
	std::string container_name;
	// http/tcp: loopback host mapping of the container's published port.
	std::string host;   // "127.0.0.1"
	int port = 0;       // host-side port
	std::string url;    // "http://127.0.0.1:<port>" (http only)
	// unix: bind-mounted socket path (unix mode is not implemented yet).
	std::string socket_path;
};

// The image's kVgiTransportsLabel modes; empty when none are declared (the
// image is not shareable).
std::vector<ContainerConnMode> InspectImageTransports(const ContainerRuntime &runtime, const std::string &image);

// Find or create the shared container for `spec` and return its endpoint
// (cached for 30s). A lost `--name` race re-introspects the winner.
// `requested_mode` applies only when creating; a running container keeps its
// recorded `conn` label. Does not probe readiness: callers connect with retry,
// and a not-yet-published port throws so the retry re-resolves. Called at
// connection time, so idle-stopped containers are recreated. POSIX only.
ContainerEndpoint EnsureSharedContainer(const ContainerSpec &spec, ContainerConnMode requested_mode);

// Drop the cached endpoint for `spec` so the next EnsureSharedContainer
// re-introspects. Call after a connect failure.
void InvalidateSharedContainer(const ContainerSpec &spec);

#if VGI_SUBPROCESS_TRANSPORT
// Connect to a tcp endpoint and wrap the fd in a UnixSocketWorker for the
// fd-based connection path (http endpoints use HttpFunctionConnection; unix is
// not implemented). Throws IOException on failure.
std::unique_ptr<SubProcess> ConnectSharedContainer(const ContainerEndpoint &endpoint);

// Open a raw TCP connection to host:port, resolving DNS and trying IPv4/IPv6
// addresses under one connect deadline. Returns a connected fd, or -1 on
// failure. When error_message is non-null, it receives an actionable resolution
// or connect diagnostic. Shared by container-shared TCP and tcp://.
int TcpConnect(const std::string &host, int port, int timeout_ms,
               std::string *error_message = nullptr, const std::string &socks5h_proxy = "");
#endif

// Per-process registry: internal `container-shared:` worker_path → {spec,
// requested_mode}. Populated at ATTACH; read at connection time.
void RegisterSharedContainer(const std::string &location, const ContainerSpec &spec,
                             ContainerConnMode requested_mode);
bool LookupSharedContainer(const std::string &location, ContainerSpec &out_spec,
                           ContainerConnMode &out_mode);

// Best-effort `rm -f` of labeled vgi-rpc-* containers that are not running, or
// running with a published cport that fails TcpReadyProbe. Backstop for a
// worker that ignored its idle timeout. Never throws; run once per process.
void ReapDeadSharedContainers(const ContainerRuntime &runtime) noexcept;

} // namespace vgi
} // namespace duckdb
