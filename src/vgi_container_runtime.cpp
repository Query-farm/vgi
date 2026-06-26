// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_container_runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "vgi_transport.hpp"
#include "yyjson.hpp"
#if VGI_POSIX_TRANSPORT
#include "vgi_unix_socket_worker.hpp"
#endif

#if VGI_POSIX_TRANSPORT
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif
#if defined(_WIN32)
#include <process.h>
#define popen _popen
#define pclose _pclose
#define VGI_GETPID _getpid
#else
#define VGI_GETPID getpid
#endif

namespace duckdb {
namespace vgi {

const char *const kContainerNamePlaceholder = "__VGI_CONTAINER_NAME__";
const char *const kVgiVolumesLabel = "farm.query.vgi.volumes";
const char *const kVgiTransportsLabel = "farm.query.vgi.transports";
const char *const kVgiWorkerLabel = "farm.query.vgi.worker";
const char *const kVgiConnLabel = "farm.query.vgi.conn";
const char *const kVgiCportLabel = "farm.query.vgi.cport";

namespace {

// Container name prefix.  The owning DuckDB pid is embedded so the orphan reaper
// can tell whose containers these are.  Must match the reaper's parser below.
constexpr const char *kContainerNamePrefix = "vgi-rpc-";
// Stable label stamped on every container we start, so the reaper can list ours
// without scanning unrelated containers.
constexpr const char *kContainerLabel = "farm.query.vgi.worker=1";

// POSIX single-quote shell quoting: wrap in single quotes, escaping any embedded
// single quote as '\''.  Safe for arbitrary content passed to /bin/sh -c.
std::string ShellQuote(const std::string &s) {
	std::string out = "'";
	for (char c : s) {
		if (c == '\'') {
			out += "'\\''";
		} else {
			out += c;
		}
	}
	out += "'";
	return out;
}

struct CommandResult {
	int exit_code = -1;
	std::string output;  // combined stdout+stderr
};

// Run a shell command line, capturing combined stdout+stderr.  Blocking; used
// only at ATTACH (runtime detect / image inspect / pull), never on a hot path.
CommandResult RunCaptureCommand(const std::string &cmd) {
	CommandResult result;
	std::string full = cmd + " 2>&1";
	FILE *pipe = popen(full.c_str(), "r");
	if (!pipe) {
		result.exit_code = -1;
		return result;
	}
	char buf[4096];
	size_t n;
	while ((n = fread(buf, 1, sizeof(buf), pipe)) > 0) {
		result.output.append(buf, n);
	}
	int status = pclose(pipe);
#if VGI_POSIX_TRANSPORT
	if (WIFEXITED(status)) {
		result.exit_code = WEXITSTATUS(status);
	} else {
		result.exit_code = -1;
	}
#else
	result.exit_code = status;
#endif
	return result;
}

// Fire-and-forget a shell command without blocking teardown.  Best-effort —
// the caller ignores the result.  Backgrounded so the destructor returns
// immediately.
void RunDetached(const std::string &cmd) {
#if VGI_POSIX_TRANSPORT
	std::string full = cmd + " >/dev/null 2>&1 &";
	int rc = std::system(full.c_str());
	(void)rc;
#else
	std::string full = "cmd /c \"" + cmd + "\" >NUL 2>&1";
	int rc = std::system(full.c_str());
	(void)rc;
#endif
}

// Resolve an executable name against PATH.  Returns the absolute path if found
// and executable, else empty.
std::string WhichBinary(const std::string &name) {
	const char *path_env = std::getenv("PATH");
	if (!path_env) {
		return "";
	}
#if defined(_WIN32)
	const char sep = ';';
	const std::string exe_suffix = ".exe";
#else
	const char sep = ':';
	const std::string exe_suffix = "";
#endif
	std::string path = path_env;
	size_t start = 0;
	while (start <= path.size()) {
		size_t end = path.find(sep, start);
		std::string dir = path.substr(start, end == std::string::npos ? std::string::npos : end - start);
		if (!dir.empty()) {
			std::string candidate = dir + "/" + name + exe_suffix;
#if VGI_POSIX_TRANSPORT
			if (access(candidate.c_str(), X_OK) == 0) {
				return candidate;
			}
#else
			FILE *f = fopen(candidate.c_str(), "rb");
			if (f) {
				fclose(f);
				return candidate;
			}
#endif
		}
		if (end == std::string::npos) {
			break;
		}
		start = end + 1;
	}
	return "";
}

// 32-bit FNV-1a — stable within a process run, which is all the per-process
// worker pool needs for disambiguation.
uint32_t Fnv1a32(const std::string &s) {
	uint32_t h = 2166136261u;
	for (unsigned char c : s) {
		h ^= c;
		h *= 16777619u;
	}
	return h;
}

std::vector<std::string> kKnownRuntimes() {
	return {"docker", "podman", "nerdctl", "container"};
}

// In-place StringUtil::Trim returns void; this returns a trimmed copy.
std::string TrimCopy(std::string s) {
	StringUtil::Trim(s);
	return s;
}

// Sanitize a string into a docker-volume-name-safe token ([a-zA-Z0-9_.-]).
std::string SanitizeVolumeToken(const std::string &s) {
	std::string out;
	for (char c : s) {
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
		    c == '.' || c == '-') {
			out += c;
		} else {
			out += '_';
		}
	}
	if (out.empty()) {
		out = "vol";
	}
	return out;
}

} // namespace

ContainerRuntime DetectContainerRuntime(const std::string &override_kind) {
	if (!override_kind.empty()) {
		std::string kind = StringUtil::Lower(override_kind);
		auto known = kKnownRuntimes();
		if (std::find(known.begin(), known.end(), kind) == known.end()) {
			throw InvalidInputException(
			    "vgi container: unknown container_runtime '%s'; expected one of docker, podman, nerdctl, container",
			    override_kind);
		}
		std::string bin = WhichBinary(kind);
		if (bin.empty()) {
			throw IOException(
			    "vgi container: requested container_runtime '%s' was not found on PATH", override_kind);
		}
		return ContainerRuntime {bin, kind};
	}
	for (const auto &kind : kKnownRuntimes()) {
		std::string bin = WhichBinary(kind);
		if (!bin.empty()) {
			return ContainerRuntime {bin, kind};
		}
	}
	throw IOException(
	    "vgi container: no container runtime found on PATH (searched docker, podman, nerdctl, container). "
	    "Install one, or use a bare-command / http:// LOCATION instead.");
}

std::vector<ContainerVolume> InspectImageVolumes(const ContainerRuntime &runtime, const std::string &image) {
	using namespace duckdb_yyjson; // NOLINT

	auto run_inspect = [&]() -> CommandResult {
		std::string fmt = std::string("{{ index .Config.Labels \"") + kVgiVolumesLabel + "\" }}";
		std::string cmd = ShellQuote(runtime.binary) + " image inspect --format " + ShellQuote(fmt) + " " +
		                  ShellQuote(image);
		return RunCaptureCommand(cmd);
	};

	CommandResult res = run_inspect();
	if (res.exit_code != 0) {
		// Most likely the image isn't present locally.  Pull once (auth deferred
		// to the runtime's own login), then retry inspect.
		std::string pull_cmd = ShellQuote(runtime.binary) + " pull " + ShellQuote(image);
		CommandResult pull = RunCaptureCommand(pull_cmd);
		if (pull.exit_code != 0) {
			throw IOException(
			    "vgi container: failed to pull image '%s' via %s: %s", image, runtime.kind,
			    StringUtil::Replace(pull.output, "\n", " "));
		}
		res = run_inspect();
		if (res.exit_code != 0) {
			throw IOException("vgi container: failed to inspect image '%s' via %s: %s", image, runtime.kind,
			                  StringUtil::Replace(res.output, "\n", " "));
		}
	}

	std::string label = TrimCopy(res.output);
	// Go template prints "<no value>" when the label is absent.
	if (label.empty() || label == "<no value>") {
		return {};
	}

	std::vector<ContainerVolume> volumes;
	yyjson_doc *doc = yyjson_read(label.c_str(), label.size(), 0);
	if (!doc) {
		// Malformed label — warn-by-throwing so the image author notices.
		throw IOException("vgi container: image '%s' has a malformed %s label: %s", image, kVgiVolumesLabel,
		                  label);
	}
	struct DocGuard {
		yyjson_doc *d;
		~DocGuard() { if (d) yyjson_doc_free(d); }
	} guard {doc};

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		throw IOException("vgi container: image '%s' %s label is not a JSON array: %s", image, kVgiVolumesLabel,
		                  label);
	}
	size_t idx, max;
	yyjson_val *entry;
	yyjson_arr_foreach(root, idx, max, entry) {
		if (!yyjson_is_obj(entry)) {
			continue;
		}
		yyjson_val *path_v = yyjson_obj_get(entry, "path");
		if (!path_v || !yyjson_is_str(path_v)) {
			continue;  // path is required to mount anything
		}
		ContainerVolume vol;
		vol.path = yyjson_get_str(path_v);
		yyjson_val *name_v = yyjson_obj_get(entry, "name");
		if (name_v && yyjson_is_str(name_v)) {
			vol.host = yyjson_get_str(name_v);
		} else {
			// Synthesize a stable named volume so state persists across attaches.
			vol.host = "vgi_" + SanitizeVolumeToken(image) + "_" + SanitizeVolumeToken(vol.path);
		}
		volumes.push_back(std::move(vol));
	}
	return volumes;
}

std::string BuildContainerRunCommandTemplate(const ContainerSpec &spec) {
	std::ostringstream cmd;
	cmd << ShellQuote(spec.runtime.binary) << " run -i --rm";
	// Unique name (substituted at spawn time) + stable label for reaping.
	cmd << " --name " << kContainerNamePlaceholder;
	cmd << " --label " << ShellQuote(kContainerLabel);
	for (const auto &vol : spec.volumes) {
		cmd << " -v " << ShellQuote(vol.host + ":" + vol.path);
	}
	for (const auto &e : spec.env) {
		cmd << " -e " << ShellQuote(e);
	}
	for (const auto &arg : spec.extra_args) {
		cmd << " " << ShellQuote(arg);
	}
	cmd << " " << ShellQuote(spec.image);
	cmd << " " << spec.transport;  // "stdio" — fixed vocabulary, no quoting needed
	return cmd.str();
}

std::string ContainerSpecHash(const ContainerSpec &spec) {
	std::vector<std::string> tokens;
	tokens.push_back("rt:" + spec.runtime.kind);
	tokens.push_back("img:" + spec.image);
	tokens.push_back("tp:" + spec.transport);
	std::vector<std::string> vols;
	for (const auto &v : spec.volumes) {
		vols.push_back("v:" + v.host + ":" + v.path);
	}
	std::sort(vols.begin(), vols.end());
	std::vector<std::string> envs;
	for (const auto &e : spec.env) {
		envs.push_back("e:" + e);
	}
	std::sort(envs.begin(), envs.end());
	// extra_args order is significant — keep as given.
	std::vector<std::string> extras;
	for (const auto &a : spec.extra_args) {
		extras.push_back("x:" + a);
	}
	std::string canonical;
	for (auto *vec : {&tokens, &vols, &envs, &extras}) {
		for (const auto &t : *vec) {
			canonical += t;
			canonical += '\n';
		}
	}
	char hex[9];
	std::snprintf(hex, sizeof(hex), "%08x", Fnv1a32(canonical));
	return std::string(hex);
}

// ---------------------------------------------------------------------------
// Launch registry
// ---------------------------------------------------------------------------

namespace {

struct ContainerLaunchInfo {
	std::string runtime_binary;
	std::string command_template;
};

std::mutex &RegistryMutex() {
	static std::mutex m;
	return m;
}

std::map<std::string, ContainerLaunchInfo> &Registry() {
	static std::map<std::string, ContainerLaunchInfo> r;
	return r;
}

} // namespace

void RegisterContainerLaunch(const std::string &location, const std::string &runtime_binary,
                             const std::string &command_template) {
	std::lock_guard<std::mutex> lock(RegistryMutex());
	Registry()[location] = ContainerLaunchInfo {runtime_binary, command_template};
}

bool IsContainerRegistered(const std::string &location) {
	std::lock_guard<std::mutex> lock(RegistryMutex());
	return Registry().find(location) != Registry().end();
}

void EnsureContainerRegistered(const std::string &location) {
	// ATTACH pre-registers the (option-suffixed) location with the user's
	// volumes/env/runtime. Entry points reached *before* ATTACH — vgi_catalogs()
	// and the standalone vgi_table_function() — pass the raw oci:// location and
	// never hit that path, so resolve it here with defaults (auto runtime,
	// image-declared volumes, no extra env/args) on first use. Idempotent; the
	// registry caches the result so the inspect/pull only happens once.
	if (IsContainerRegistered(location)) {
		return;
	}
	ContainerSpec spec;
	spec.image = StripContainerScheme(location);
	spec.transport = "stdio";
	spec.runtime = DetectContainerRuntime("");
	spec.volumes = InspectImageVolumes(spec.runtime, spec.image);
	std::string command_template = BuildContainerRunCommandTemplate(spec);
	RegisterContainerLaunch(location, spec.runtime.binary, command_template);
}

#if VGI_SUBPROCESS_TRANSPORT

namespace {

// Generate a unique container name: vgi-rpc-<duckdb_pid>-<counter>.  The pid lets
// the reaper attribute the container to this process; the counter keeps
// concurrent spawns from the same process distinct (so parallel pool misses
// don't collide on `--name`).
std::string GenerateContainerName() {
	static std::atomic<uint64_t> counter {0};
	uint64_t n = counter.fetch_add(1, std::memory_order_relaxed);
	std::ostringstream name;
	name << kContainerNamePrefix << static_cast<long long>(VGI_GETPID()) << "-" << n;
	return name.str();
}

} // namespace

std::unique_ptr<SubProcess> SpawnWorker(const std::string &worker_path, bool worker_debug) {
	if (!IsContainerLocation(worker_path)) {
		return std::make_unique<SubProcess>(worker_path, worker_debug);
	}
	// Resolve-on-first-use so pre-ATTACH callers (vgi_catalogs / standalone
	// vgi_table_function) work without an explicit registration. No-op when ATTACH
	// already registered this location.
	EnsureContainerRegistered(worker_path);
	ContainerLaunchInfo info;
	{
		std::lock_guard<std::mutex> lock(RegistryMutex());
		auto it = Registry().find(worker_path);
		if (it == Registry().end()) {
			throw IOException(
			    "vgi container: no resolved launch info for '%s' (was the catalog attached?)", worker_path);
		}
		info = it->second;
	}
	std::string name = GenerateContainerName();
	std::string final_command = StringUtil::Replace(info.command_template, kContainerNamePlaceholder, name);
	return std::make_unique<ContainerWorker>(final_command, info.runtime_binary, name, worker_debug);
}

ContainerWorker::ContainerWorker(const std::string &final_command, std::string runtime_binary,
                                 std::string container_name, bool stderr_passthrough)
    : SubProcess(final_command, stderr_passthrough), runtime_binary_(std::move(runtime_binary)),
      container_name_(std::move(container_name)) {
}

ContainerWorker::~ContainerWorker() {
	// The base SubProcess dtor will SIGTERM/SIGKILL the `docker run` CLI; with
	// `--rm` that normally removes the container.  But a SIGKILL of the CLI
	// detaches the container and leaks it, so force-remove by name as a
	// best-effort backstop.  Detached + non-blocking so teardown never stalls
	// on the runtime.
	if (!runtime_binary_.empty() && !container_name_.empty()) {
		RunDetached(ShellQuote(runtime_binary_) + " rm -f " + ShellQuote(container_name_));
	}
}

void ReapOrphanContainers(const ContainerRuntime &runtime) noexcept {
	try {
		std::string cmd = ShellQuote(runtime.binary) + " ps -a --filter " +
		                  ShellQuote(std::string("label=") + kContainerLabel) + " --format " +
		                  ShellQuote("{{.Names}}");
		CommandResult res = RunCaptureCommand(cmd);
		if (res.exit_code != 0) {
			return;  // runtime unavailable or unsupported flag — give up quietly
		}
		std::istringstream lines(res.output);
		std::string name;
		while (std::getline(lines, name)) {
			name = TrimCopy(name);
			if (name.rfind(kContainerNamePrefix, 0) != 0) {
				continue;
			}
			// name == vgi-rpc-<pid>-<counter>
			std::string rest = name.substr(std::string(kContainerNamePrefix).size());
			auto dash = rest.find('-');
			if (dash == std::string::npos) {
				continue;
			}
			long long owner_pid = 0;
			try {
				owner_pid = std::stoll(rest.substr(0, dash));
			} catch (...) {
				continue;
			}
#if VGI_POSIX_TRANSPORT
			// Only remove a container whose owning DuckDB process is provably gone.
			// kill(pid, 0) == 0 => we can still signal it => alive => leave it.
			if (owner_pid <= 0 || ::kill(static_cast<pid_t>(owner_pid), 0) == 0) {
				continue;
			}
			RunDetached(ShellQuote(runtime.binary) + " rm -f " + ShellQuote(name));
#else
			// No portable liveness probe here — err on the side of NOT removing a
			// container that might belong to a live process.
			(void)owner_pid;
#endif
		}
	} catch (...) {
		// Best-effort — never propagate.
	}
}

#endif // VGI_SUBPROCESS_TRANSPORT

// ============================================================================
// Shared (transparently reused) container workers
// ============================================================================

ContainerConnMode ParseContainerConnMode(const std::string &s) {
	auto l = StringUtil::Lower(s);
	if (l == "http") {
		return ContainerConnMode::HTTP;
	}
	if (l == "tcp") {
		return ContainerConnMode::TCP;
	}
	if (l == "unix") {
		return ContainerConnMode::UNIX;
	}
	throw InvalidInputException("vgi container: unknown connection '%s' (expected http, tcp, unix, or stdio)", s);
}

const char *ContainerConnModeName(ContainerConnMode mode) {
	switch (mode) {
	case ContainerConnMode::HTTP:
		return "http";
	case ContainerConnMode::TCP:
		return "tcp";
	case ContainerConnMode::UNIX:
		return "unix";
	}
	return "http";
}

std::vector<ContainerConnMode> InspectImageTransports(const ContainerRuntime &runtime, const std::string &image) {
	using namespace duckdb_yyjson; // NOLINT
	std::string fmt = std::string("{{ index .Config.Labels \"") + kVgiTransportsLabel + "\" }}";
	std::string cmd =
	    ShellQuote(runtime.binary) + " image inspect --format " + ShellQuote(fmt) + " " + ShellQuote(image);
	CommandResult res = RunCaptureCommand(cmd);
	if (res.exit_code != 0) {
		return {};  // image absent/uninspectable here; caller already inspected volumes (which pulls)
	}
	std::string label = TrimCopy(res.output);
	if (label.empty() || label == "<no value>") {
		return {};
	}
	std::vector<ContainerConnMode> modes;
	yyjson_doc *doc = yyjson_read(label.c_str(), label.size(), 0);
	if (!doc) {
		return {};
	}
	struct DocGuard {
		yyjson_doc *d;
		~DocGuard() { if (d) yyjson_doc_free(d); }
	} guard {doc};
	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		return {};
	}
	size_t idx, max;
	yyjson_val *entry;
	yyjson_arr_foreach(root, idx, max, entry) {
		if (!yyjson_is_str(entry)) {
			continue;
		}
		try {
			modes.push_back(ParseContainerConnMode(yyjson_get_str(entry)));
		} catch (...) {
			// ignore unknown advertised modes
		}
	}
	return modes;
}

// ---- shared registry: internal location → {spec, requested mode} ----
namespace {

struct SharedEntry {
	ContainerSpec spec;
	ContainerConnMode mode;
};

std::mutex &SharedRegistryMutex() {
	static std::mutex m;
	return m;
}
std::map<std::string, SharedEntry> &SharedRegistry() {
	static std::map<std::string, SharedEntry> r;
	return r;
}

// Container-internal port the worker server listens on, per mode.
int ContainerPortForMode(ContainerConnMode mode) {
	switch (mode) {
	case ContainerConnMode::HTTP:
		return 8000;  // image EXPOSE 8000 / PORT default
	case ContainerConnMode::TCP:
		return 8001;  // native vgi-rpc TCP listener (coordinate with vgi-rpc)
	case ContainerConnMode::UNIX:
		return 0;     // socket, not a port
	}
	return 0;
}

} // namespace

void RegisterSharedContainer(const std::string &location, const ContainerSpec &spec, ContainerConnMode mode) {
	std::lock_guard<std::mutex> lock(SharedRegistryMutex());
	SharedRegistry()[location] = SharedEntry {spec, mode};
}

bool LookupSharedContainer(const std::string &location, ContainerSpec &out_spec, ContainerConnMode &out_mode) {
	std::lock_guard<std::mutex> lock(SharedRegistryMutex());
	auto it = SharedRegistry().find(location);
	if (it == SharedRegistry().end()) {
		return false;
	}
	out_spec = it->second.spec;
	out_mode = it->second.mode;
	return true;
}

#if VGI_POSIX_TRANSPORT
namespace {

// Non-blocking TCP connect with a timeout. Returns a connected (blocking-mode) fd,
// or -1. Caller closes the fd.
int TcpConnect(const std::string &host, int port, int timeout_ms) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return -1;
	}
	int flags = ::fcntl(fd, F_GETFL, 0);
	::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	struct sockaddr_in addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(static_cast<uint16_t>(port));
	::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
	bool ok = false;
	int rc = ::connect(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr));
	if (rc == 0) {
		ok = true;
	} else if (errno == EINPROGRESS) {
		fd_set wf;
		FD_ZERO(&wf);
		FD_SET(fd, &wf);
		struct timeval tv;
		tv.tv_sec = timeout_ms / 1000;
		tv.tv_usec = (timeout_ms % 1000) * 1000;
		if (::select(fd + 1, nullptr, &wf, nullptr, &tv) > 0) {
			int soerr = 0;
			socklen_t l = sizeof(soerr);
			::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &l);
			ok = (soerr == 0);
		}
	}
	if (!ok) {
		::close(fd);
		return -1;
	}
	::fcntl(fd, F_SETFL, flags);  // restore blocking
	return fd;
}

// Native-protocol (tcp) readiness probe. A bare TCP connect is fooled by
// docker-proxy, which accepts the host-port handshake before the worker inside is
// listening (then resets when it forwards to a dead upstream). The native server
// instead holds an accepted connection open, sending nothing until it gets a
// request. So: connect, then peek with a short timeout — a timeout (would-block)
// means a live peer is holding the connection; recv==0 / reset means not ready.
bool TcpReadyProbe(const std::string &host, int port, int timeout_ms) {
	int fd = TcpConnect(host, port, timeout_ms);
	if (fd < 0) {
		return false;
	}
	struct timeval tv;
	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	char b;
	ssize_t n = ::recv(fd, &b, 1, MSG_PEEK);
	bool ready;
	if (n > 0) {
		ready = true;  // peer sent something — definitely alive
	} else if (n == 0) {
		ready = false;  // clean EOF — docker-proxy upstream not up yet
	} else {
		ready = (errno == EAGAIN || errno == EWOULDBLOCK);  // timed out ⇒ live peer holding the conn
	}
	::close(fd);
	return ready;
}

// HTTP readiness probe: GET /health and require a 200. A bare TCP connect succeeds
// before the WSGI app is serving, so http mode must check the app actually answers.
bool HttpHealthProbe(const std::string &host, int port, int timeout_ms) {
	int fd = TcpConnect(host, port, timeout_ms);
	if (fd < 0) {
		return false;
	}
	struct timeval tv;
	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;
	::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	std::string req = "GET /health HTTP/1.0\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
	bool ok = false;
	if (::send(fd, req.data(), req.size(), 0) == static_cast<ssize_t>(req.size())) {
		char buf[512];
		ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
		if (n > 0) {
			std::string resp(buf, static_cast<size_t>(n));
			ok = resp.find(" 200") != std::string::npos;
		}
	}
	::close(fd);
	return ok;
}

// `<rt> port <name> <cport>/tcp` → parse the loopback host port (0 if absent).
int ResolveHostPort(const std::string &rt, const std::string &name, int cport) {
	auto res = RunCaptureCommand(ShellQuote(rt) + " port " + ShellQuote(name) + " " + std::to_string(cport) +
	                             "/tcp");
	if (res.exit_code != 0) {
		return 0;
	}
	std::string line = res.output;
	auto nl = line.find('\n');
	if (nl != std::string::npos) {
		line = line.substr(0, nl);
	}
	auto colon = line.rfind(':');
	if (colon == std::string::npos) {
		return 0;
	}
	try {
		return std::stoi(TrimCopy(line.substr(colon + 1)));
	} catch (...) {
		return 0;
	}
}

// Append the spec's -v / -e / extra-args tokens (shared with the per-process builder's intent).
void AppendVolEnvExtra(std::ostringstream &cmd, const ContainerSpec &spec) {
	for (const auto &vol : spec.volumes) {
		cmd << " -v " << ShellQuote(vol.host + ":" + vol.path);
	}
	for (const auto &e : spec.env) {
		cmd << " -e " << ShellQuote(e);
	}
	for (const auto &arg : spec.extra_args) {
		cmd << " " << ShellQuote(arg);
	}
}

} // namespace

namespace {
// Short-TTL endpoint cache so the ~8 discovery RPCs of a single ATTACH (and
// back-to-back queries) reuse one resolution instead of re-running docker
// inspect + a readiness probe each time. Keyed by container name.
struct CachedEndpoint {
	ContainerEndpoint ep;
	std::chrono::steady_clock::time_point at;
};
std::mutex &EndpointCacheMutex() {
	static std::mutex m;
	return m;
}
std::map<std::string, CachedEndpoint> &EndpointCache() {
	static std::map<std::string, CachedEndpoint> c;
	return c;
}
constexpr auto kEndpointTtl = std::chrono::seconds(30);
} // namespace

void InvalidateSharedContainer(const ContainerSpec &spec) {
	std::lock_guard<std::mutex> lock(EndpointCacheMutex());
	EndpointCache().erase("vgi-rpc-" + ContainerSpecHash(spec));
}

ContainerEndpoint EnsureSharedContainer(const ContainerSpec &spec, ContainerConnMode requested_mode) {
	const std::string &rt = spec.runtime.binary;
	const std::string name = "vgi-rpc-" + ContainerSpecHash(spec);

	// Fresh cache hit → skip docker inspect + readiness entirely.
	{
		std::lock_guard<std::mutex> lock(EndpointCacheMutex());
		auto it = EndpointCache().find(name);
		if (it != EndpointCache().end() &&
		    std::chrono::steady_clock::now() - it->second.at < kEndpointTtl) {
			return it->second.ep;
		}
	}
	auto cache_put = [&](ContainerEndpoint ep) -> ContainerEndpoint {
		std::lock_guard<std::mutex> lock(EndpointCacheMutex());
		EndpointCache()[name] = CachedEndpoint {ep, std::chrono::steady_clock::now()};
		return ep;
	};

	if (requested_mode == ContainerConnMode::UNIX) {
		throw IOException(
		    "vgi container: the 'unix' shared connection is not yet implemented (planned); use http or tcp");
	}

	// Inspect: Running? + recorded conn/cport labels.
	auto inspect = [&]() -> CommandResult {
		std::string fmt = std::string("{{.State.Running}}\t{{index .Config.Labels \"") + kVgiConnLabel +
		                  "\"}}\t{{index .Config.Labels \"" + kVgiCportLabel + "\"}}";
		return RunCaptureCommand(ShellQuote(rt) + " inspect --format " + ShellQuote(fmt) + " " +
		                         ShellQuote(name));
	};

	// Parse "Running\tconn\tcport"; returns {running, mode, cport} when present.
	auto parse_inspect = [&](const CommandResult &r, bool &exists, bool &running, ContainerConnMode &mode,
	                         int &cport) {
		exists = (r.exit_code == 0);
		running = false;
		cport = 0;
		if (!exists) {
			return;
		}
		std::string out = TrimCopy(r.output);
		auto t1 = out.find('\t');
		auto t2 = (t1 == std::string::npos) ? std::string::npos : out.find('\t', t1 + 1);
		std::string s_run = (t1 == std::string::npos) ? out : out.substr(0, t1);
		running = (TrimCopy(s_run) == "true");
		if (t1 != std::string::npos && t2 != std::string::npos) {
			std::string s_conn = TrimCopy(out.substr(t1 + 1, t2 - t1 - 1));
			std::string s_port = TrimCopy(out.substr(t2 + 1));
			try {
				mode = ParseContainerConnMode(s_conn);
			} catch (...) {
				mode = requested_mode;
			}
			try {
				cport = std::stoi(s_port);
			} catch (...) {
				cport = ContainerPortForMode(mode);
			}
		}
	};

	auto finish = [&](ContainerConnMode mode, int cport) -> ContainerEndpoint {
		ContainerEndpoint ep;
		ep.mode = mode;
		ep.container_name = name;
		ep.host = "127.0.0.1";
		// Poll for the published host port + readiness (cold start can take a while;
		// the image was already pulled at ATTACH). http requires the app to answer
		// /health — a bare TCP connect succeeds before the WSGI app is serving.
		const int deadline_iters = 240;  // ~60s at 250ms
		for (int i = 0; i < deadline_iters; i++) {
			int hp = ResolveHostPort(rt, name, cport);
			// A live native worker holds the connection open (sending nothing until
			// it gets a request), so TcpReadyProbe blocks for its full timeout on a
			// healthy worker — keep it short. docker-proxy with a dead upstream
			// resets well within this, so 120ms is ample to tell them apart.
			bool ready = hp > 0 && (mode == ContainerConnMode::HTTP
			                            ? HttpHealthProbe("127.0.0.1", hp, 1000)
			                            : TcpReadyProbe("127.0.0.1", hp, 120));
			if (ready) {
				ep.port = hp;
				ep.url = "http://127.0.0.1:" + std::to_string(hp);
				return ep;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(250));
		}
		throw IOException("vgi container: shared container '%s' did not become ready", name);
	};

	// 1. Fast path: already running → reuse its recorded mode.
	{
		bool exists = false, running = false;
		ContainerConnMode mode = requested_mode;
		int cport = 0;
		parse_inspect(inspect(), exists, running, mode, cport);
		if (exists && running) {
			if (cport == 0) {
				cport = ContainerPortForMode(mode);
			}
			return cache_put(finish(mode, cport));
		}
		if (exists && !running) {
			// Stale/exited leftover — remove and recreate.
			RunCaptureCommand(ShellQuote(rt) + " rm -f " + ShellQuote(name));
		}
	}

	// 2. Create (daemon serializes on --name; a lost race re-introspects).
	int cport = ContainerPortForMode(requested_mode);
	std::ostringstream cmd;
	cmd << ShellQuote(rt) << " run -d --rm --name " << ShellQuote(name);
	cmd << " --label " << ShellQuote(std::string(kVgiWorkerLabel) + "=1");
	cmd << " --label " << ShellQuote(std::string(kVgiConnLabel) + "=" + ContainerConnModeName(requested_mode));
	cmd << " --label " << ShellQuote(std::string(kVgiCportLabel) + "=" + std::to_string(cport));
	cmd << " -p 127.0.0.1:0:" << cport;
	AppendVolEnvExtra(cmd, spec);
	cmd << " " << ShellQuote(spec.image);
	cmd << " " << ContainerConnModeName(requested_mode);  // entrypoint mode: http | tcp
	if (requested_mode == ContainerConnMode::TCP) {
		// The tcp worker self-shuts-down when idle (native protocol supports it);
		// the entrypoint forwards --idle-timeout to the worker. http servers may
		// not support idle-shutdown, so they don't get it.
		cmd << " --idle-timeout 300";
	}

	CommandResult run = RunCaptureCommand(cmd.str());
	if (run.exit_code != 0) {
		// Lost the --name race? Re-introspect and use the winner's.
		bool exists = false, running = false;
		ContainerConnMode mode = requested_mode;
		int got = 0;
		parse_inspect(inspect(), exists, running, mode, got);
		if (exists) {
			if (got == 0) {
				got = ContainerPortForMode(mode);
			}
			return cache_put(finish(mode, got));
		}
		throw IOException("vgi container: failed to start shared container '%s': %s", name,
		                  StringUtil::Replace(run.output, "\n", " "));
	}
	return cache_put(finish(requested_mode, cport));
}

std::unique_ptr<SubProcess> ConnectSharedContainer(const ContainerEndpoint &endpoint) {
	if (endpoint.mode == ContainerConnMode::TCP) {
		int fd = TcpConnect(endpoint.host, endpoint.port, 10000);
		if (fd < 0) {
			throw IOException("vgi container: failed to connect to shared tcp endpoint %s:%d",
			                  endpoint.host, endpoint.port);
		}
		return std::make_unique<UnixSocketWorker>(fd);
	}
	if (endpoint.mode == ContainerConnMode::UNIX) {
		throw IOException("vgi container: the 'unix' shared connection is not implemented");
	}
	throw IOException("vgi container: ConnectSharedContainer is only for socket modes (tcp/unix)");
}

void ReapDeadSharedContainers(const ContainerRuntime &runtime) noexcept {
	try {
		std::string cmd = ShellQuote(runtime.binary) + " ps -a --filter " +
		                  ShellQuote(std::string("label=") + kVgiWorkerLabel + "=1") + " --format " +
		                  ShellQuote("{{.Names}}\t{{.State}}");
		CommandResult res = RunCaptureCommand(cmd);
		if (res.exit_code != 0) {
			return;
		}
		std::istringstream lines(res.output);
		std::string line;
		while (std::getline(lines, line)) {
			line = TrimCopy(line);
			if (line.rfind("vgi-rpc-", 0) != 0) {
				continue;
			}
			auto tab = line.find('\t');
			std::string nm = (tab == std::string::npos) ? line : line.substr(0, tab);
			std::string state = (tab == std::string::npos) ? "" : TrimCopy(line.substr(tab + 1));
			if (state == "running") {
#if VGI_POSIX_TRANSPORT
				// Endpoint dead? probe the published port.
				std::string fmt = std::string("{{index .Config.Labels \"") + kVgiCportLabel + "\"}}";
				auto pr = RunCaptureCommand(ShellQuote(runtime.binary) + " inspect --format " +
				                            ShellQuote(fmt) + " " + ShellQuote(nm));
				int cport = 0;
				try {
					cport = std::stoi(TrimCopy(pr.output));
				} catch (...) {
					continue;  // can't tell — leave it
				}
				int hp = ResolveHostPort(runtime.binary, nm, cport);
				if (hp > 0 && TcpReadyProbe("127.0.0.1", hp, 300)) {
					continue;  // alive
				}
#else
				continue;
#endif
			}
			RunDetached(ShellQuote(runtime.binary) + " rm -f " + ShellQuote(nm));
		}
	} catch (...) {
		// best-effort
	}
}
#else  // !VGI_POSIX_TRANSPORT
ContainerEndpoint EnsureSharedContainer(const ContainerSpec &, ContainerConnMode) {
	throw IOException("vgi: shared containers require POSIX sockets (not available in this build)");
}
void InvalidateSharedContainer(const ContainerSpec &) {}
void ReapDeadSharedContainers(const ContainerRuntime &) noexcept {}
#if VGI_SUBPROCESS_TRANSPORT
std::unique_ptr<SubProcess> ConnectSharedContainer(const ContainerEndpoint &) {
	throw IOException("vgi: shared container socket connections require POSIX (not available in this build)");
}
#endif
#endif // VGI_POSIX_TRANSPORT

} // namespace vgi
} // namespace duckdb
