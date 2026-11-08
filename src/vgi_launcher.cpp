// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

// AF_UNIX worker launcher — POSIX-only.  Under emscripten the entire
// translation unit collapses to nothing; LAUNCH/UNIX LOCATION schemes
// surface a clear "not supported in WASM" error at the dispatch layer
// (see vgi_function_connection.cpp, vgi_unary_rpc.cpp).
#include "vgi_platform.hpp"

#if VGI_POSIX_TRANSPORT

#include "vgi_launcher.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_errno_name.hpp"
#include "vgi_exception.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_missing_program.hpp"
#include "vgi_subprocess.hpp" // ResetChildSignalDispositions
#include "vgi_unix_socket.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <optional>
#include <poll.h>
#include <set>
#include <sys/file.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

extern char **environ;

namespace duckdb {
namespace vgi {

namespace {

// extra_info for a launcher-side local failure (state dir, lock, pipe, fork).
ErrorInfo LauncherLocalInfo(int err = 0) {
	ErrorInfo info(error_subtype::kLocalResource);
	info.Set(error_key::kTransport, "launch");
	if (err != 0) {
		info.Set(error_key::kErrno, ErrnoName(err));
	}
	return info;
}

// ---------------------------------------------------------------------------
// Centralised worker-pid reaper
// ---------------------------------------------------------------------------
// Per-spawn detached ``waitpid`` threads collide with hosts that install
// their own SIGCHLD handler (Python embedded, etc.) and accumulate stack +
// kernel-tid resources for the worker's full lifetime (potentially hours
// at the default 300 s idle timeout, multiplied by every spawn this DuckDB
// process performs).  One shared reaper polls registered pids with
// ``WNOHANG``: races with another reaper just return ``ECHILD``, no
// corruption; the kernel's auto-reap under ``SIG_IGN`` likewise just
// drains our registry without us having ever called ``waitpid``.

class PidReaper {
public:
	static PidReaper &Instance() {
		// Intentionally leaked (same rationale as VgiWorkerPool / VgiResultCache):
		// Run() executes on a DETACHED thread that locks mu_ for the whole process
		// lifetime. A function-local ``static PidReaper inst;`` is destroyed during
		// __cxa_finalize at process exit while that thread is still running, so its
		// next mu_.lock() hits a destroyed mutex → std::mutex::lock() throws EINVAL
		// → uncaught on the reaper thread → std::terminate → SIGABRT. (Observed as an
		// intermittent teardown abort under the parallel cross-SDK test matrix.)
		// Leaking keeps mu_ (and pids_) alive so the detached reaper never races
		// static teardown.
		static PidReaper *inst = new PidReaper();
		return *inst;
	}

	void Register(pid_t pid) {
		if (pid <= 0) {
			return;
		}
		std::lock_guard<std::mutex> lk(mu_);
		pids_.insert(pid);
		EnsureThreadStartedLocked();
	}

private:
	PidReaper() = default;
	PidReaper(const PidReaper &) = delete;
	PidReaper &operator=(const PidReaper &) = delete;

	void EnsureThreadStartedLocked() {
		if (started_) {
			return;
		}
		started_ = true;
		std::thread([this]() { Run(); }).detach();
	}

	void Run() {
		using namespace std::chrono_literals;
		while (true) {
			std::this_thread::sleep_for(500ms);
			std::vector<pid_t> snapshot;
			{
				std::lock_guard<std::mutex> lk(mu_);
				snapshot.assign(pids_.begin(), pids_.end());
			}
			for (pid_t pid : snapshot) {
				int status = 0;
				pid_t rc = ::waitpid(pid, &status, WNOHANG);
				// rc > 0: reaped.  rc < 0 + ECHILD: kernel auto-reaped (e.g.
				// host installed ``SIG_IGN``) or already gone — drop either way.
				// rc == 0: still running, keep polling.
				if (rc != 0) {
					std::lock_guard<std::mutex> lk(mu_);
					pids_.erase(pid);
				}
			}
		}
	}

	std::mutex mu_;
	std::set<pid_t> pids_;
	bool started_ = false;
};

// Hard cap on bytes we'll buffer waiting for the worker's discovery line.
// A misbehaving (or hostile) worker that prints unbounded prefix noise
// would otherwise OOM the launcher process.
constexpr std::size_t kDiscoveryBufferLimit = 1u << 20; // 1 MiB

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Read environment as a flat (key, value) list.  Used to pass to the launcher
// helpers' env filter so we hash only VGI_RPC_* entries.
std::vector<std::pair<std::string, std::string>> SnapshotEnvironment() {
	std::vector<std::pair<std::string, std::string>> out;
	for (char **e = environ; e && *e; ++e) {
		std::string entry(*e);
		auto eq = entry.find('=');
		if (eq == std::string::npos) {
			continue;
		}
		out.emplace_back(entry.substr(0, eq), entry.substr(eq + 1));
	}
	return out;
}

std::string EnvOr(const char *name, const char *fallback) {
	const char *v = std::getenv(name);
	return v ? std::string(v) : (fallback ? std::string(fallback) : std::string());
}

// Recursive mkdir up to and including *path*, mode 0700 on each created
// directory.  Existing directories are left alone.  Throws IOException on
// any error other than "already exists".
void MkdirRecursive(const std::string &path) {
	if (path.empty() || path == "/") {
		return;
	}
	// Walk the path and mkdir each component.
	for (size_t i = 1; i <= path.size(); ++i) {
		if (i < path.size() && path[i] != '/') {
			continue;
		}
		std::string sub = path.substr(0, i);
		if (sub.empty()) {
			continue;
		}
		if (::mkdir(sub.c_str(), 0700) == 0) {
			continue;
		}
		if (errno == EEXIST) {
			struct stat st;
			if (::stat(sub.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
				continue;
			}
			throw IOException(LauncherLocalInfo(), "vgi launcher: %s exists but is not a directory", sub);
		}
		const int err = errno;
		throw IOException(LauncherLocalInfo(err), "vgi launcher: mkdir(%s) failed: %s", sub, std::strerror(err));
	}
}

// Owns an open lockfile fd; releases the flock (and closes the fd) on scope
// exit unless ``Release()`` is called.
class FlockGuard {
public:
	FlockGuard() = default;

	// Open *path* (creating with mode 0600 if missing) and acquire an
	// exclusive ``flock`` with a wall-clock budget of ``timeout``.  Throws
	// IOException on timeout or unrecoverable error.
	static FlockGuard Acquire(const std::string &path, std::chrono::milliseconds timeout) {
		int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
		if (fd < 0) {
			const int err = errno;
			throw IOException(LauncherLocalInfo(err), "vgi launcher: open(%s) for flock failed: %s", path,
			                  std::strerror(err));
		}
		auto deadline = std::chrono::steady_clock::now() + timeout;
		// flock(2) doesn't have a native timeout; spin on LOCK_NB with a
		// small sleep until the deadline.  ~50ms granularity is fine for
		// the rare-contention case (typically zero-wait or minutes-of-spawn).
		while (true) {
			if (::flock(fd, LOCK_EX | LOCK_NB) == 0) {
				return FlockGuard(fd);
			}
			// EINTR (signal storm under profilers) is treated identically
			// to EWOULDBLOCK — sleep before retrying so we never burn CPU
			// in a tight loop.
			if (errno != EWOULDBLOCK && errno != EINTR) {
				int saved = errno;
				::close(fd);
				throw IOException(LauncherLocalInfo(saved), "vgi launcher: flock(%s) failed: %s", path,
				                  std::strerror(saved));
			}
			if (std::chrono::steady_clock::now() >= deadline) {
				::close(fd);
				throw IOException(
				    LauncherLocalInfo(), "vgi launcher: timed out acquiring lock %s after %lldms", path,
				    static_cast<long long>(timeout.count()));
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
	}

	FlockGuard(const FlockGuard &) = delete;
	FlockGuard &operator=(const FlockGuard &) = delete;
	FlockGuard(FlockGuard &&other) noexcept : fd_(other.fd_) {
		other.fd_ = -1;
	}
	FlockGuard &operator=(FlockGuard &&other) noexcept {
		if (this != &other) {
			Release();
			fd_ = other.fd_;
			other.fd_ = -1;
		}
		return *this;
	}
	~FlockGuard() {
		Release();
	}

	void Release() {
		if (fd_ >= 0) {
			// Closing the fd implicitly releases the lock (POSIX
			// guarantees: ``flock`` ranges by open file description).
			::close(fd_);
			fd_ = -1;
		}
	}

private:
	explicit FlockGuard(int fd) : fd_(fd) {}
	int fd_ = -1;
};

// Resolve the per-user state directory + ensure it exists.
std::string ResolveAndEnsureStateDir(const std::optional<std::string> &override_dir) {
	std::string dir;
	if (override_dir.has_value()) {
		dir = *override_dir;
	} else {
		dir = launcher::ResolveStateDir(EnvOr("XDG_RUNTIME_DIR", ""), EnvOr("TMPDIR", ""),
		                                 static_cast<uint32_t>(::geteuid()));
	}
	MkdirRecursive(dir);
	// Tighten mode + ownership-check on POSIX.  The Python launcher does
	// the same — this catches cases where /tmp/vgi-rpc-$UID was created by
	// a different user and a malicious one tries to MITM via symlinks.
	::chmod(dir.c_str(), 0700);
	struct stat st;
	if (::stat(dir.c_str(), &st) != 0) {
		const int err = errno;
		throw IOException(LauncherLocalInfo(err), "vgi launcher: stat(%s) failed: %s", dir, std::strerror(err));
	}
	if (st.st_uid != ::geteuid()) {
		throw IOException(
		    LauncherLocalInfo(), "vgi launcher: state directory %s is not owned by current user (uid=%u)", dir,
		    static_cast<unsigned>(st.st_uid));
	}
	return dir;
}

// Connect probe — true iff a worker is listening on path.
//
// A listener whose accept queue is full is alive, only busy — and a false
// "dead" here is destructive, because Launch() then unlinks the socket out
// from under the live worker and spawns a duplicate, orphaning the original
// and racing clients onto a vanished path.  Linux reports a full queue as
// EAGAIN, distinct from the ECONNREFUSED of an unbound socket, so it counts as
// alive.  macOS reports both as ECONNREFUSED, so a refusal is re-probed briefly
// before it is believed; a socket left by a dead worker pays that once.  This
// asks "is anything listening", not "is it responsive": a connect lands in the
// queue of a worker that never accepts, so a successful one never proved that
// either.  Mirrors ``vgi_rpc.launcher._probe``.
bool ProbeAlive(const std::string &path) {
	static const std::chrono::milliseconds kRefusedBackoff[] = {
	    std::chrono::milliseconds(50), std::chrono::milliseconds(100), std::chrono::milliseconds(200)};
	for (std::size_t attempt = 0;; ++attempt) {
		int err = UnixSocket::ProbeOnce(path);
		if (err == 0 || UnixSocket::IsAcceptQueueFull(err)) {
			return true;
		}
		if (err != ECONNREFUSED || attempt == sizeof(kRefusedBackoff) / sizeof(kRefusedBackoff[0])) {
			return false;
		}
		std::this_thread::sleep_for(kRefusedBackoff[attempt]);
	}
}

// A pipe whose ends are both close-on-exec. macOS lacks pipe2(), so the flags
// are set explicitly. Throws on failure.
struct CloexecPipe {
	int read_fd = -1;
	int write_fd = -1;

	CloexecPipe() {
		int fds[2];
		if (::pipe(fds) != 0) {
			const int err = errno;
			throw IOException(LauncherLocalInfo(err), "vgi launcher: pipe() failed: %s", std::strerror(err));
		}
		read_fd = fds[0];
		write_fd = fds[1];
		if (::fcntl(read_fd, F_SETFD, FD_CLOEXEC) != 0 || ::fcntl(write_fd, F_SETFD, FD_CLOEXEC) != 0) {
			const int err = errno;
			CloseRead();
			CloseWrite();
			throw IOException(LauncherLocalInfo(err), "vgi launcher: fcntl(FD_CLOEXEC) failed: %s",
			                  std::strerror(err));
		}
	}
	CloexecPipe(const CloexecPipe &) = delete;
	CloexecPipe &operator=(const CloexecPipe &) = delete;
	~CloexecPipe() {
		CloseRead();
		CloseWrite();
	}
	void CloseRead() {
		if (read_fd >= 0) {
			::close(read_fd);
			read_fd = -1;
		}
	}
	void CloseWrite() {
		if (write_fd >= 0) {
			::close(write_fd);
			write_fd = -1;
		}
	}
	int ReleaseRead() {
		int fd = read_fd;
		read_fd = -1;
		return fd;
	}
};

// What a forked child reports over its close-on-exec status pipe when it
// cannot become the target program. A successful exec closes the pipe, so the
// parent reads either nothing (the program is running) or one of these.
struct ChildFailure {
	enum Stage : int32_t { kNone = 0, kRedirect = 1, kExec = 2 };
	int32_t stage = kNone;
	int32_t err = 0;
};

// Child side, after fork(): only async-signal-safe calls from here on.
[[noreturn]] void ChildFail(int status_fd, ChildFailure::Stage stage, int exit_code) {
	ChildFailure f;
	f.stage = stage;
	f.err = errno;
	ssize_t ignored = ::write(status_fd, &f, sizeof(f));
	(void)ignored;
	::_exit(exit_code);
}

// Parent side: wait for the child to exec (EOF) or report a failure. A pipe
// write end also inherited by another thread's concurrent fork() closes at
// that child's own exec, so the read is bounded rather than trusted to end.
ChildFailure ReadChildFailure(int status_fd) {
	ChildFailure f;
	auto *p = reinterpret_cast<char *>(&f);
	size_t got = 0;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (got < sizeof(f)) {
		auto remaining =
		    std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
		if (remaining.count() <= 0) {
			break;
		}
		struct pollfd pfd {status_fd, POLLIN, 0};
		int pr = ::poll(&pfd, 1, static_cast<int>(remaining.count()));
		if (pr < 0 && errno == EINTR) {
			continue;
		}
		if (pr <= 0) {
			break;
		}
		ssize_t n = ::read(status_fd, p + got, sizeof(f) - got);
		if (n < 0 && errno == EINTR) {
			continue;
		}
		if (n <= 0) {
			break;
		}
		got += static_cast<size_t>(n);
	}
	return got == sizeof(f) ? f : ChildFailure();
}

// Absolute path of an executable `name` on PATH, or "".
std::string ResolveOnPath(const std::string &name) {
	const char *path_env = std::getenv("PATH");
	if (!path_env) {
		return "";
	}
	std::string path = path_env;
	size_t start = 0;
	while (start <= path.size()) {
		size_t end = path.find(':', start);
		std::string dir = path.substr(start, end == std::string::npos ? std::string::npos : end - start);
		std::string candidate = (dir.empty() ? std::string(".") : dir) + "/" + name;
		if (::access(candidate.c_str(), X_OK) == 0) {
			return candidate;
		}
		if (end == std::string::npos) {
			break;
		}
		start = end + 1;
	}
	return "";
}

// The interpreter a script's `#!` line names ("python3" for
// `#!/usr/bin/env python3`), or "" for a binary / unreadable file.
std::string ShebangInterpreter(const std::string &script) {
	std::ifstream in(script, std::ios::binary);
	std::string line;
	if (!in || !std::getline(in, line) || line.compare(0, 2, "#!") != 0) {
		return "";
	}
	std::vector<std::string> words;
	std::string cur;
	for (size_t i = 2; i < line.size(); i++) {
		char c = line[i];
		if (c == ' ' || c == '\t' || c == '\r') {
			if (!cur.empty()) {
				words.push_back(cur);
				cur.clear();
			}
		} else {
			cur.push_back(c);
		}
	}
	if (!cur.empty()) {
		words.push_back(cur);
	}
	if (words.empty()) {
		return "";
	}
	auto slash = words[0].rfind('/');
	if ((slash == std::string::npos ? words[0] : words[0].substr(slash + 1)) == "env") {
		for (size_t i = 1; i < words.size(); i++) {
			if (words[i][0] != '-') {
				return words[i];
			}
		}
		return "";
	}
	return words[0];
}

// The exec of the worker failed in the child: say which program, and why.
[[noreturn]] void ThrowWorkerExecFailure(const std::string &program, const ChildFailure &f) {
	auto info = [&](const char *subtype) {
		return ErrorInfo(subtype).Set(error_key::kTransport, "launch").Set(error_key::kErrno, ErrnoName(f.err));
	};
	if (f.stage == ChildFailure::kRedirect) {
		throw IOException(info(error_subtype::kLocalResource),
		                  "vgi launcher: could not set up the worker's stdin/stdout/stderr: %s",
		                  std::strerror(f.err));
	}
	const bool bare = program.find('/') == std::string::npos;
	std::string msg;
	std::string missing;
	const char *subtype = error_subtype::kWorkerNotExecutable;
	if (f.err == ENOENT) {
		subtype = error_subtype::kWorkerNotFound;
		// execve() also reports ENOENT when the file exists but its `#!`
		// interpreter does not.
		std::string file = bare ? ResolveOnPath(program) : program;
		std::string interp = (!file.empty() && ::access(file.c_str(), F_OK) == 0) ? ShebangInterpreter(file) : "";
		if (!interp.empty()) {
			missing = interp;
			msg = "vgi launcher: cannot start worker '" + program + "': its interpreter '" + interp +
			      "' (from the #! line) was not found";
		} else if (bare) {
			missing = program;
			msg = "vgi launcher: cannot start worker: '" + program + "' was not found on PATH";
		} else {
			msg = "vgi launcher: cannot start worker: '" + program + "' does not exist";
		}
	} else if (f.err == EACCES || f.err == EPERM) {
		msg = "vgi launcher: cannot start worker: '" + program + "' is not executable (permission denied)";
	} else if (f.err == ENOEXEC) {
		msg = "vgi launcher: cannot start worker: '" + program + "' is not a valid executable (exec format error)";
	} else {
		msg = "vgi launcher: cannot start worker '" + program + "': " + std::strerror(f.err);
	}
	std::string hint = MissingProgramHint(missing);
	if (!hint.empty()) {
		msg += "\n" + hint;
	}
	throw IOException(info(subtype).Set(error_key::kMissingProgram, missing), msg);
}

// Spawn the worker.  The caller owns the returned fds and is responsible for
// the pid (WaitForReadinessAndDetach hands it to the reaper).  stderr_fd is the
// read end of the worker's stderr when it is captured, else -1.
struct SpawnResult {
	pid_t pid;
	int stdout_fd;
	int stderr_fd;
};

// `cat`, used as the worker's stderr sink once it is ready (see
// HandOffStderrToSink). "" when there is none, and stderr is then discarded
// from the start, as it always was.
const std::string &StderrSinkProgram() {
	static const std::string program = []() -> std::string {
		for (const char *candidate : {"/bin/cat", "/usr/bin/cat"}) {
			if (::access(candidate, X_OK) == 0) {
				return candidate;
			}
		}
		return "";
	}();
	return program;
}

SpawnResult SpawnWorker(const std::vector<std::string> &final_argv,
                         const std::optional<std::string> &worker_stderr_path, bool capture_stderr) {
	// stdout carries the discovery line. The child clears CLOEXEC on each
	// write end by dup2()ing it onto its stdio fd before exec.
	CloexecPipe out_pipe;
	std::optional<CloexecPipe> err_pipe;
	if (capture_stderr) {
		err_pipe.emplace();
	}
	CloexecPipe status_pipe;

	// Everything the child needs is prepared before fork(): after it, only
	// async-signal-safe calls.
	std::vector<char *> raw;
	raw.reserve(final_argv.size() + 1);
	for (const auto &a : final_argv) {
		raw.push_back(const_cast<char *>(a.c_str()));
	}
	raw.push_back(nullptr);
	const char *stderr_target = worker_stderr_path.has_value() ? worker_stderr_path->c_str() : "/dev/null";

	// See ScopedForkSignalBlock: closes the fork()->reset window.
	ScopedForkSignalBlock fork_signal_block;
	pid_t pid = ::fork();
	if (pid < 0) {
		int saved = errno;
		throw IOException(LauncherLocalInfo(saved), "vgi launcher: fork() failed: %s", std::strerror(saved));
	}
	if (pid == 0) {
		// --- Child ---
		ResetChildSignalDispositions();
		const int status_fd = status_pipe.write_fd;

		if (::dup2(out_pipe.write_fd, STDOUT_FILENO) < 0) {
			ChildFail(status_fd, ChildFailure::kRedirect, 126);
		}

		// stderr → the capture pipe, the user's file, or /dev/null.
		if (err_pipe.has_value()) {
			if (::dup2(err_pipe->write_fd, STDERR_FILENO) < 0) {
				ChildFail(status_fd, ChildFailure::kRedirect, 126);
			}
		} else {
			int err_fd = worker_stderr_path.has_value()
			                 ? ::open(stderr_target, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600)
			                 : ::open(stderr_target, O_WRONLY | O_CLOEXEC);
			if (err_fd < 0 || ::dup2(err_fd, STDERR_FILENO) < 0) {
				ChildFail(status_fd, ChildFailure::kRedirect, 126);
			}
			::close(err_fd);
		}

		// stdin → /dev/null so the worker can't accidentally block on it.
		int in_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
		if (in_fd < 0 || ::dup2(in_fd, STDIN_FILENO) < 0) {
			ChildFail(status_fd, ChildFailure::kRedirect, 126);
		}
		::close(in_fd);

		// We deliberately do NOT setsid() so terminal SIGINT/SIGHUP propagate
		// from the controlling DuckDB process to the worker on POSIX.  Every
		// pipe end other than the dup2'd stdio is CLOEXEC and closes here.
		::execvp(raw[0], raw.data());
		ChildFail(status_fd, ChildFailure::kExec, 127);
	}

	// --- Parent ---
	fork_signal_block.Restore();
	out_pipe.CloseWrite();
	if (err_pipe.has_value()) {
		err_pipe->CloseWrite();
	}
	status_pipe.CloseWrite();
	ChildFailure failure = ReadChildFailure(status_pipe.read_fd);
	if (failure.stage != ChildFailure::kNone) {
		int status = 0;
		while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
		}
		ThrowWorkerExecFailure(final_argv[0], failure);
	}
	return {pid, out_pipe.ReleaseRead(), err_pipe.has_value() ? err_pipe->ReleaseRead() : -1};
}

// Hand the worker's stderr pipe to a detached `cat` writing to /dev/null.
//
// A launch: worker is shared and outlives this DuckDB process, so once it is
// ready its stderr cannot stay a pipe that this process reads: on exit the
// read end would close and the worker's next stderr write would fail (EPIPE,
// or death by SIGPIPE for a worker that resets that signal). Nor can it be a
// file, which would grow without bound for as long as the worker runs. The
// sink drains the pipe for exactly as long as the worker (and anything it
// spawned) keeps it open, then exits. It ignores the terminal's job-control
// signals so a Ctrl-C in the DuckDB shell cannot remove it from under a
// worker that survives that Ctrl-C.
//
// Returns the sink's pid, or -1 with *error set.
pid_t HandOffStderrToSink(int stderr_fd, std::string *error) {
	const std::string &sink = StderrSinkProgram();
	std::optional<CloexecPipe> status_pipe;
	try {
		status_pipe.emplace();
	} catch (const IOException &) {
		*error = std::string("vgi launcher: could not create a pipe for the worker's stderr sink: ") +
		         std::strerror(errno);
		return -1;
	}
	ScopedForkSignalBlock fork_signal_block;
	pid_t pid = ::fork();
	if (pid < 0) {
		*error = std::string("vgi launcher: fork() for the worker's stderr sink failed: ") + std::strerror(errno);
		return -1;
	}
	if (pid == 0) {
		ResetChildSignalDispositions();
		struct sigaction ignore;
		std::memset(&ignore, 0, sizeof(ignore));
		ignore.sa_handler = SIG_IGN;
		sigemptyset(&ignore.sa_mask);
		for (int sig : {SIGINT, SIGQUIT, SIGHUP, SIGTSTP, SIGTTIN, SIGTTOU}) {
			(void)::sigaction(sig, &ignore, nullptr);
		}
		const int status_fd = status_pipe->write_fd;
		int null_fd = ::open("/dev/null", O_RDWR | O_CLOEXEC);
		if (null_fd < 0 || ::dup2(stderr_fd, STDIN_FILENO) < 0 || ::dup2(null_fd, STDOUT_FILENO) < 0 ||
		    ::dup2(null_fd, STDERR_FILENO) < 0) {
			ChildFail(status_fd, ChildFailure::kRedirect, 126);
		}
		::execl(sink.c_str(), "cat", static_cast<char *>(nullptr));
		ChildFail(status_fd, ChildFailure::kExec, 127);
	}
	fork_signal_block.Restore();
	status_pipe->CloseWrite();
	ChildFailure failure = ReadChildFailure(status_pipe->read_fd);
	if (failure.stage != ChildFailure::kNone) {
		int status = 0;
		while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
		}
		*error = "vgi launcher: could not start the worker's stderr sink (" + sink +
		         "): " + std::strerror(failure.err);
		return -1;
	}
	return pid;
}

// The worker's stderr while it starts up: everything it writes before the
// discovery line, tail-capped, for the error message if it never gets there.
class StartupStderr {
public:
	explicit StartupStderr(int fd) : fd_(fd) {
	}
	StartupStderr(const StartupStderr &) = delete;
	StartupStderr &operator=(const StartupStderr &) = delete;
	~StartupStderr() {
		Close();
	}

	int fd() const {
		return fd_;
	}

	// Read what is available. Call when select()/poll() reports fd() readable.
	void ReadAvailable() {
		char chunk[4096];
		ssize_t n = ::read(fd_, chunk, sizeof(chunk));
		if (n < 0 && errno == EINTR) {
			return;
		}
		if (n <= 0) {
			Close(); // EOF: every writer is gone
			return;
		}
		text_.append(chunk, static_cast<size_t>(n));
		if (text_.size() > kMaxBytes) {
			text_.erase(0, text_.size() - kMaxBytes);
		}
	}

	// Read until EOF (the worker and its children have exited) or `budget`
	// elapses: the worker's last words may still be in flight when its
	// stdout closes.
	void DrainFor(std::chrono::milliseconds budget) {
		auto deadline = std::chrono::steady_clock::now() + budget;
		while (fd_ >= 0) {
			auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
			    deadline - std::chrono::steady_clock::now());
			if (remaining.count() <= 0) {
				return;
			}
			struct pollfd pfd {fd_, POLLIN, 0};
			int pr = ::poll(&pfd, 1, static_cast<int>(remaining.count()));
			if (pr < 0 && errno == EINTR) {
				continue;
			}
			if (pr <= 0) {
				return;
			}
			ReadAvailable();
		}
	}

	// Give up ownership of the fd (to the stderr sink).
	int Release() {
		int fd = fd_;
		fd_ = -1;
		return fd;
	}

	void Close() {
		if (fd_ >= 0) {
			::close(fd_);
			fd_ = -1;
		}
	}

	const std::string &text() const {
		return text_;
	}

private:
	static constexpr size_t kMaxBytes = 64 * 1024;
	int fd_;
	std::string text_;
};

std::string JoinArgv(const std::vector<std::string> &argv) {
	std::string out;
	for (const auto &a : argv) {
		if (!out.empty()) {
			out += ' ';
		}
		out += a;
	}
	return out;
}

// Read from `stdout_fd` until either ParseDiscoveryLine returns kFound, the
// worker exits, or `deadline` passes.  On success: closes the pipe (forcing
// SIGPIPE on any contract-violating worker that writes more stdout), hands
// a captured stderr pipe to the sink, and hands the worker pid to the
// singleton reaper.  Throws IOException on any failure, with whatever the
// worker wrote to stderr meanwhile.
//
// Buffer is hard-capped at ``kDiscoveryBufferLimit`` so a hostile or
// runaway worker can't OOM us with unbounded prefix noise.
void WaitForReadinessAndDetach(pid_t worker_pid, int stdout_fd, int stderr_fd,
                                const std::vector<std::string> &worker_argv, const std::string &expected_path,
                                std::chrono::steady_clock::time_point deadline) {
	std::string buffer;
	char chunk[4096];
	StartupStderr startup_stderr(stderr_fd);
	auto launch_info = [&](const char *subtype) {
		return ErrorInfo(subtype)
		    .Set(error_key::kTransport, "launch")
		    .Set(error_key::kWorkerPid, static_cast<int64_t>(worker_pid))
		    .Set(error_key::kSocketPath, expected_path);
	};
	// The tail of the message: the worker's stderr, then a hint naming a
	// program it needed that is not installed.
	auto stderr_and_hint = [&](int exit_code, std::string *missing) {
		std::string text = startup_stderr.text();
		while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
			text.pop_back();
		}
		std::string tail = FormatWorkerStderrSuffix(text);
		std::string hint = WorkerStartupHint(startup_stderr.text(), JoinArgv(worker_argv), exit_code, missing);
		if (!hint.empty()) {
			tail += "\n" + hint;
		}
		return tail;
	};
	auto fail = [&](const std::string &msg, const char *subtype) {
		::kill(worker_pid, SIGTERM);
		::close(stdout_fd);
		startup_stderr.DrainFor(std::chrono::milliseconds(250));
		startup_stderr.Close();
		PidReaper::Instance().Register(worker_pid);
		std::string missing;
		auto tail = stderr_and_hint(-1, &missing);
		throw IOException(launch_info(subtype).Set(error_key::kMissingProgram, missing), msg + tail);
	};
	while (true) {
		auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			fail("vgi launcher: worker did not emit UNIX:<path> within startup timeout",
			     error_subtype::kWorkerStartTimeout);
		}
		auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);

		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(stdout_fd, &rfds);
		int max_fd = stdout_fd;
		if (startup_stderr.fd() >= 0) {
			FD_SET(startup_stderr.fd(), &rfds);
			max_fd = std::max(max_fd, startup_stderr.fd());
		}
		struct timeval tv;
		tv.tv_sec = static_cast<time_t>(remaining.count() / 1000);
		tv.tv_usec = static_cast<suseconds_t>((remaining.count() % 1000) * 1000);
		int sel = ::select(max_fd + 1, &rfds, nullptr, nullptr, &tv);
		if (sel < 0) {
			if (errno == EINTR) {
				continue;
			}
			fail(std::string("vgi launcher: select() failed: ") + std::strerror(errno),
			     error_subtype::kTransportFailure);
		}
		if (sel == 0) {
			// Loop will hit deadline check.
			continue;
		}
		if (startup_stderr.fd() >= 0 && FD_ISSET(startup_stderr.fd(), &rfds)) {
			startup_stderr.ReadAvailable();
		}
		if (!FD_ISSET(stdout_fd, &rfds)) {
			continue;
		}
		ssize_t n = ::read(stdout_fd, chunk, sizeof(chunk));
		if (n == 0) {
			// EOF — worker closed stdout (likely exited).  Collect its last
			// stderr before blocking in waitpid: a worker still writing to a
			// full stderr pipe would otherwise never exit.
			::close(stdout_fd);
			startup_stderr.DrainFor(std::chrono::seconds(2));
			startup_stderr.Close();
			int status = 0;
			while (::waitpid(worker_pid, &status, 0) < 0 && errno == EINTR) {
			}
			// Decode the raw waitpid status: printing it verbatim made exit
			// 127 read as 32512.
			if (WIFSIGNALED(status)) {
				const int sig = WTERMSIG(status);
				throw IOException(launch_info(error_subtype::kWorkerKilled)
				                      .Set(error_key::kExitSignal, static_cast<int64_t>(sig)),
				                  "vgi launcher: worker exited before emitting UNIX:<path> (killed by signal " +
				                      std::to_string(sig) + ")" + stderr_and_hint(-1, nullptr));
			}
			const int code = WIFEXITED(status) ? WEXITSTATUS(status) : status;
			const char *subtype = code == 127   ? error_subtype::kWorkerNotFound
			                      : code == 126 ? error_subtype::kWorkerNotExecutable
			                                    : error_subtype::kWorkerExited;
			std::string missing;
			auto tail = stderr_and_hint(code, &missing);
			throw IOException(launch_info(subtype)
			                      .Set(error_key::kExitCode, static_cast<int64_t>(code))
			                      .Set(error_key::kMissingProgram, missing),
			                  "vgi launcher: worker exited before emitting UNIX:<path> (exit code " +
			                      std::to_string(code) + ")" + tail);
		}
		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			fail(std::string("vgi launcher: read() from worker stdout failed: ") +
			         std::strerror(errno),
			     error_subtype::kTransportFailure);
		}
		if (buffer.size() + static_cast<size_t>(n) > kDiscoveryBufferLimit) {
			fail("vgi launcher: worker emitted >1 MiB of pre-discovery output without "
			     "the UNIX:<path> line; aborting",
			     error_subtype::kProtocolViolation);
		}
		buffer.append(chunk, static_cast<size_t>(n));
		auto parse = launcher::ParseDiscoveryLine(buffer, expected_path);
		if (parse == launcher::DiscoveryParseResult::kMismatch) {
			fail("vgi launcher: worker bound to a different path than requested",
			     error_subtype::kProtocolViolation);
		}
		if (parse == launcher::DiscoveryParseResult::kFound) {
			if (startup_stderr.fd() >= 0) {
				std::string sink_error;
				pid_t sink_pid = HandOffStderrToSink(startup_stderr.fd(), &sink_error);
				if (sink_pid < 0) {
					// Without a sink the worker would lose its stderr as soon
					// as this process exits; better not to use it at all.
					fail(sink_error, error_subtype::kLocalResource);
				}
				PidReaper::Instance().Register(sink_pid);
				startup_stderr.Close(); // the sink holds its own copy now
			}
			// Close our read end of the worker's stdout.  The cross-language
			// launcher contract says the worker must not write to stdout
			// after the UNIX:<path> line — a contract-violating worker
			// gets SIGPIPE here, which is the right outcome (the bug is
			// in the worker, and a noisy crash beats a silent thread leak
			// on our side).
			::close(stdout_fd);
			PidReaper::Instance().Register(worker_pid);
			return;
		}
		// kNeedMore — keep reading.
	}
}

} // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

std::string Launch(const LaunchConfig &cfg) {
	if (cfg.worker_argv.empty()) {
		throw InvalidInputException("vgi launcher: worker_argv must be non-empty");
	}

	std::string state_dir = ResolveAndEnsureStateDir(cfg.state_dir_override);

	// Resolve socket / lock paths.
	std::string sock_path;
	std::string lock_path;
	if (cfg.socket_path_override.has_value()) {
		sock_path = *cfg.socket_path_override;
		lock_path = sock_path + ".lock";
	} else {
		auto env_subset = launcher::FilterVgiRpcEnv(SnapshotEnvironment());
		// Snapshot cwd as the launcher sees it.  Workers honour the same
		// cwd because fork() inherits it.  Allocate dynamically so deeply
		// nested CI paths (Bazel, etc.) longer than 4 KiB don't truncate.
		std::string cwd;
		std::size_t buf_size = 4096;
		while (true) {
			std::vector<char> buf(buf_size);
			if (::getcwd(buf.data(), buf.size()) != nullptr) {
				cwd.assign(buf.data());
				break;
			}
			if (errno != ERANGE) {
				// Unable to read cwd — empty string still produces a valid
				// hash; leaves a documented edge case if the cwd is
				// inaccessible (e.g. unreadable parent dirs).
				break;
			}
			if (buf_size > (1u << 20)) { // 1 MiB ceiling — no real cwd is this long
				break;
			}
			buf_size *= 2;
		}
		auto hash = launcher::ComputeLauncherHash(cfg.worker_argv, cwd, env_subset);
		sock_path = state_dir + "/" + hash + ".sock";
		lock_path = state_dir + "/" + hash + ".lock";
	}

	// Translate any std::invalid_argument out of pure helpers into the
	// appropriate DuckDB exception at this single boundary.
	try {
		launcher::ValidateRendezvousPathLength(sock_path);
	} catch (const std::invalid_argument &e) {
		throw InvalidInputException(e.what());
	}

	auto guard = FlockGuard::Acquire(lock_path, cfg.connect_timeout);

	// Inside the lock: probe for an existing healthy worker, else spawn.
	if (ProbeAlive(sock_path)) {
		return sock_path;
	}
	// Stale socket file (worker crashed or never bound).  Best-effort unlink.
	::unlink(sock_path.c_str());

	// Build final argv with --unix and --idle-timeout appended.
	std::vector<std::string> argv = cfg.worker_argv;
	argv.emplace_back("--unix");
	argv.push_back(sock_path);
	argv.emplace_back("--idle-timeout");
	{
		// Per docs/launcher-protocol.md: decimal seconds in plain notation,
		// no scientific.  ``%g`` switches to ``%e`` past 1e6, which would
		// silently mangle large idle-timeout values; ``%.3f`` keeps
		// millisecond precision and we strip trailing zeros to match the
		// other implementations.
		double sec = static_cast<double>(cfg.idle_timeout.count()) / 1000.0;
		char buf[64];
		std::snprintf(buf, sizeof(buf), "%.3f", sec);
		std::string s(buf);
		auto dot = s.find('.');
		if (dot != std::string::npos) {
			while (s.size() > dot + 1 && s.back() == '0') {
				s.pop_back();
			}
			if (s.back() == '.') {
				s.pop_back();
			}
		}
		argv.emplace_back(std::move(s));
	}

	// Capture stderr while the worker starts, so a failure can say why; a
	// user-chosen stderr file, or no `cat` to drain it afterwards, keeps the
	// old behaviour (file / discard).
	const bool capture_stderr = !cfg.worker_stderr_path.has_value() && !StderrSinkProgram().empty();
	auto spawn = SpawnWorker(argv, cfg.worker_stderr_path, capture_stderr);
	auto deadline = std::chrono::steady_clock::now() + cfg.worker_startup_timeout;
	WaitForReadinessAndDetach(spawn.pid, spawn.stdout_fd, spawn.stderr_fd, cfg.worker_argv, sock_path, deadline);

	// Lock auto-releases as guard goes out of scope on return.
	return sock_path;
}

} // namespace vgi
} // namespace duckdb

#elif defined(_WIN32)

// ===========================================================================
// Windows launcher — named-pipe rendezvous.
//
// CPython has no AF_UNIX on Windows, so the launch: worker is reached via a
// Windows named pipe (\\.\pipe\vgi-rpc-<hash>) served by serve_named_pipe. This
// is a parallel implementation of Launch() that mirrors the POSIX flow: hash →
// rendezvous name, system-wide spawn election (named mutex instead of flock),
// probe-or-spawn, read the PIPE:<name> discovery line, detach. The cross-platform
// hash / argv / discovery-parse helpers are shared (vgi_launcher_internal).
// UNTESTED on macOS CI — validated on the Windows build machine.
// ===========================================================================

#include "vgi_launcher.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_exception.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_missing_program.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <io.h>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace duckdb {
namespace vgi {

namespace {

constexpr std::size_t kDiscoveryBufferLimit = 1u << 20; // 1 MiB

// Read the process environment as a flat (key, value) list (Windows).
std::vector<std::pair<std::string, std::string>> WinEnvSnapshot() {
	std::vector<std::pair<std::string, std::string>> out;
	LPCH env = GetEnvironmentStringsA();
	if (!env) {
		return out;
	}
	for (LPCH p = env; *p;) {
		std::string entry(p);
		p += entry.size() + 1;
		// Skip the magic "=C:=..." drive-cwd entries (key begins with '=').
		if (entry.empty() || entry[0] == '=') {
			continue;
		}
		auto eq = entry.find('=');
		if (eq == std::string::npos) {
			continue;
		}
		out.emplace_back(entry.substr(0, eq), entry.substr(eq + 1));
	}
	FreeEnvironmentStringsA(env);
	return out;
}

std::string WinCwd() {
	DWORD n = GetCurrentDirectoryA(0, nullptr);
	if (n == 0) {
		return std::string();
	}
	std::vector<char> buf(n);
	DWORD got = GetCurrentDirectoryA(n, buf.data());
	return std::string(buf.data(), got);
}

// Quote one argv element for a Windows command line per CommandLineToArgvW rules.
std::string QuoteArg(const std::string &arg) {
	if (!arg.empty() && arg.find_first_of(" \t\"") == std::string::npos) {
		return arg; // no quoting needed
	}
	std::string out = "\"";
	std::size_t backslashes = 0;
	for (char c : arg) {
		if (c == '\\') {
			backslashes++;
			out.push_back(c);
		} else if (c == '"') {
			out.append(backslashes + 1, '\\'); // escape the run of backslashes + the quote
			out.push_back('"');
			backslashes = 0;
		} else {
			backslashes = 0;
			out.push_back(c);
		}
	}
	out.append(backslashes, '\\'); // double trailing backslashes before the closing quote
	out.push_back('"');
	return out;
}

std::string BuildCommandLine(const std::vector<std::string> &argv) {
	std::string cmd;
	for (std::size_t i = 0; i < argv.size(); ++i) {
		if (i) {
			cmd.push_back(' ');
		}
		cmd += QuoteArg(argv[i]);
	}
	return cmd;
}

// RAII named mutex for system-wide spawn election (replaces POSIX flock).
class WinMutexGuard {
public:
	static WinMutexGuard Acquire(const std::string &name, std::chrono::milliseconds timeout) {
		// Prefer the Global namespace (cross-session); fall back to Local if the
		// session can't create global objects.
		HANDLE h = CreateMutexA(nullptr, FALSE, ("Global\\" + name).c_str());
		if (!h) {
			h = CreateMutexA(nullptr, FALSE, ("Local\\" + name).c_str());
		}
		if (!h) {
			throw IOException("vgi launcher: CreateMutex(%s) failed (GetLastError=%lu)", name,
			                  (unsigned long)GetLastError());
		}
		DWORD r = WaitForSingleObject(h, static_cast<DWORD>(timeout.count()));
		if (r != WAIT_OBJECT_0 && r != WAIT_ABANDONED) {
			CloseHandle(h);
			throw IOException(ErrorInfo(error_subtype::kLocalResource).Set(error_key::kTransport, "launch"),
			                  "vgi launcher: timed out acquiring spawn mutex %s after %lldms", name,
			                  static_cast<long long>(timeout.count()));
		}
		return WinMutexGuard(h);
	}
	WinMutexGuard(WinMutexGuard &&o) noexcept : h_(o.h_) {
		o.h_ = nullptr;
	}
	WinMutexGuard(const WinMutexGuard &) = delete;
	WinMutexGuard &operator=(const WinMutexGuard &) = delete;
	~WinMutexGuard() {
		if (h_) {
			ReleaseMutex(h_);
			CloseHandle(h_);
		}
	}

private:
	explicit WinMutexGuard(HANDLE h) : h_(h) {
	}
	HANDLE h_ = nullptr;
};

// Is a worker already serving on `pipe_name`?  A successful connect (or
// ERROR_PIPE_BUSY — all instances momentarily in use) means alive.
bool ProbeAlivePipe(const std::string &pipe_name) {
	HANDLE h = CreateFileA(pipe_name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_EXISTING, 0,
	                       nullptr);
	if (h != INVALID_HANDLE_VALUE) {
		CloseHandle(h);
		return true;
	}
	return GetLastError() == ERROR_PIPE_BUSY;
}

// Spawn the worker detached, read its PIPE:<pipe_name> discovery line, then let
// it run as a daemon (self-shuts-down via --idle-timeout).
void SpawnWorkerWin(const std::vector<std::string> &argv, const std::string &pipe_name,
                    const std::optional<std::string> &stderr_path,
                    std::chrono::milliseconds startup_timeout) {
	SECURITY_ATTRIBUTES sa {};
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;

	HANDLE rd = nullptr, wr = nullptr;
	if (!CreatePipe(&rd, &wr, &sa, 0)) {
		throw IOException("vgi launcher: CreatePipe failed (GetLastError=%lu)", (unsigned long)GetLastError());
	}
	SetHandleInformation(rd, HANDLE_FLAG_INHERIT, 0); // our read end is not inherited
	HANDLE nul_in = CreateFileA("NUL", GENERIC_READ, FILE_SHARE_READ, &sa, OPEN_EXISTING, 0, nullptr);
	HANDLE err_h;
	if (stderr_path.has_value()) {
		err_h = CreateFileA(stderr_path->c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, &sa,
		                    CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
	} else {
		err_h = CreateFileA("NUL", GENERIC_WRITE, FILE_SHARE_WRITE, &sa, OPEN_EXISTING, 0, nullptr);
	}

	STARTUPINFOA si {};
	si.cb = sizeof(si);
	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = nul_in;
	si.hStdOutput = wr;
	si.hStdError = err_h;

	std::string cmdline = BuildCommandLine(argv);
	std::vector<char> mutable_cmd(cmdline.begin(), cmdline.end());
	mutable_cmd.push_back('\0');

	PROCESS_INFORMATION pi {};
	BOOL ok = CreateProcessA(nullptr, mutable_cmd.data(), nullptr, nullptr, /*bInheritHandles=*/TRUE,
	                         CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi);
	CloseHandle(wr);
	if (nul_in) {
		CloseHandle(nul_in);
	}
	if (err_h) {
		CloseHandle(err_h);
	}
	if (!ok) {
		const DWORD err = GetLastError();
		CloseHandle(rd);
		// Name the program and, when it is a well-known one, where to get it.
		const std::string &program = argv[0];
		std::string msg;
		std::string missing;
		const char *subtype = error_subtype::kWorkerNotExecutable;
		if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) {
			subtype = error_subtype::kWorkerNotFound;
			if (program.find_first_of("/\\") == std::string::npos) {
				missing = program;
				msg = "vgi launcher: cannot start worker: '" + program + "' was not found on PATH";
			} else {
				msg = "vgi launcher: cannot start worker: '" + program + "' does not exist";
			}
		} else if (err == ERROR_ACCESS_DENIED) {
			msg = "vgi launcher: cannot start worker: '" + program + "' is not executable (access denied)";
		} else if (err == ERROR_BAD_EXE_FORMAT) {
			msg = "vgi launcher: cannot start worker: '" + program + "' is not a valid executable";
		} else {
			msg = "vgi launcher: cannot start worker '" + program + "': CreateProcess failed (GetLastError=" +
			      std::to_string(static_cast<unsigned long>(err)) + ")";
		}
		std::string hint = MissingProgramHint(missing);
		if (!hint.empty()) {
			msg += "\n" + hint;
		}
		throw IOException(ErrorInfo(subtype)
		                      .Set(error_key::kTransport, "launch")
		                      .Set(error_key::kMissingProgram, missing),
		                  msg);
	}
	CloseHandle(pi.hThread);

	const std::string prefix = launcher::DiscoveryLinePrefix(); // "PIPE:"
	std::string buffer;
	char chunk[4096];
	auto deadline = std::chrono::steady_clock::now() + startup_timeout;
	bool found = false;
	while (std::chrono::steady_clock::now() < deadline) {
		DWORD avail = 0;
		if (!PeekNamedPipe(rd, nullptr, 0, nullptr, &avail, nullptr)) {
			break; // worker closed stdout
		}
		if (avail == 0) {
			if (WaitForSingleObject(pi.hProcess, 0) == WAIT_OBJECT_0) {
				break; // worker exited before announcing
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
			continue;
		}
		DWORD nread = 0;
		if (!ReadFile(rd, chunk, sizeof(chunk), &nread, nullptr) || nread == 0) {
			break;
		}
		buffer.append(chunk, nread);
		auto res = launcher::ParseDiscoveryLine(buffer, pipe_name, prefix);
		if (res == launcher::DiscoveryParseResult::kFound) {
			found = true;
			break;
		}
		if (res == launcher::DiscoveryParseResult::kMismatch) {
			TerminateProcess(pi.hProcess, 1);
			CloseHandle(rd);
			CloseHandle(pi.hProcess);
			throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Set(error_key::kTransport, "launch"),
			                  "vgi launcher: worker announced an unexpected pipe (expected %s)", pipe_name);
		}
		if (buffer.size() > kDiscoveryBufferLimit) {
			break;
		}
	}
	CloseHandle(rd);
	// Detach: closing our process handle leaves the worker running independently;
	// it self-terminates via --idle-timeout. (Windows reclaims an unreferenced
	// detached process automatically — no reaper needed.)
	CloseHandle(pi.hProcess);
	if (!found) {
		throw IOException(ErrorInfo(error_subtype::kWorkerStartTimeout).Set(error_key::kTransport, "launch"),
		                  "vgi launcher: worker did not emit %s%s within %lldms", prefix, pipe_name,
		                  static_cast<long long>(startup_timeout.count()));
	}
}

std::string FormatIdleTimeoutSeconds(std::chrono::milliseconds ms) {
	double sec = static_cast<double>(ms.count()) / 1000.0;
	char buf[64];
	std::snprintf(buf, sizeof(buf), "%.3f", sec);
	std::string s(buf);
	auto dot = s.find('.');
	if (dot != std::string::npos) {
		while (s.size() > dot + 1 && s.back() == '0') {
			s.pop_back();
		}
		if (s.back() == '.') {
			s.pop_back();
		}
	}
	return s;
}

} // namespace

std::string Launch(const LaunchConfig &cfg) {
	if (cfg.worker_argv.empty()) {
		throw InvalidInputException("vgi launcher: worker_argv must be non-empty");
	}

	std::string pipe_name;
	std::string mutex_name;
	if (cfg.socket_path_override.has_value()) {
		pipe_name = *cfg.socket_path_override;
		mutex_name = "vgi-rpc-override-" + std::to_string(std::hash<std::string> {}(pipe_name));
	} else {
		auto env_subset = launcher::FilterVgiRpcEnv(WinEnvSnapshot());
		auto hash = launcher::ComputeLauncherHash(cfg.worker_argv, WinCwd(), env_subset);
		// Pipe name + mutex name both derive from the hash, so every process
		// pointing at the same (argv, cwd, VGI_RPC_*-env) tuple agrees on the
		// rendezvous and serialises spawns against the same mutex.
		pipe_name = "\\\\.\\pipe\\vgi-rpc-" + hash;
		mutex_name = "vgi-rpc-" + hash;
	}

	try {
		launcher::ValidateRendezvousPathLength(pipe_name);
	} catch (const std::invalid_argument &e) {
		throw InvalidInputException(e.what());
	}

	auto guard = WinMutexGuard::Acquire(mutex_name, cfg.connect_timeout);

	if (ProbeAlivePipe(pipe_name)) {
		return pipe_name;
	}

	std::vector<std::string> argv = cfg.worker_argv;
	argv.emplace_back("--unix");
	argv.push_back(pipe_name);
	argv.emplace_back("--idle-timeout");
	argv.push_back(FormatIdleTimeoutSeconds(cfg.idle_timeout));

	SpawnWorkerWin(argv, pipe_name, cfg.worker_stderr_path, cfg.worker_startup_timeout);
	return pipe_name; // mutex auto-releases as guard goes out of scope
}

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT / _WIN32
