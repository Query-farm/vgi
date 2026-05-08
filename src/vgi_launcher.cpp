// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// AF_UNIX worker launcher — POSIX-only.  Under emscripten the entire
// translation unit collapses to nothing; LAUNCH/UNIX LOCATION schemes
// surface a clear "not supported in WASM" error at the dispatch layer
// (see vgi_function_connection.cpp, vgi_unary_rpc.cpp).
#ifndef __EMSCRIPTEN__

#include "vgi_launcher.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_unix_socket.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <fcntl.h>
#include <fstream>
#include <mutex>
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
		static PidReaper inst;
		return inst;
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
			throw IOException("vgi launcher: %s exists but is not a directory", sub);
		}
		throw IOException("vgi launcher: mkdir(%s) failed: %s", sub, std::strerror(errno));
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
			throw IOException("vgi launcher: open(%s) for flock failed: %s", path,
			                   std::strerror(errno));
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
				throw IOException("vgi launcher: flock(%s) failed: %s", path,
				                   std::strerror(saved));
			}
			if (std::chrono::steady_clock::now() >= deadline) {
				::close(fd);
				throw IOException(
				    "vgi launcher: timed out acquiring lock %s after %lldms", path,
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
		throw IOException("vgi launcher: stat(%s) failed: %s", dir, std::strerror(errno));
	}
	if (st.st_uid != ::geteuid()) {
		throw IOException(
		    "vgi launcher: state directory %s is not owned by current user (uid=%u)", dir,
		    static_cast<unsigned>(st.st_uid));
	}
	return dir;
}

// Best-effort connect probe — true iff a worker is currently accepting on path.
bool ProbeAlive(const std::string &path) {
	try {
		auto sock = UnixSocket::Connect(path, std::chrono::milliseconds(2000));
		(void)sock; // immediately closes via RAII
		return true;
	} catch (...) {
		return false;
	}
}

// Spawn the worker.  Returns (pid, stdout_read_fd).  The caller owns the fd
// and is responsible for waitpid'ing the pid (we hand off to a reaper thread).
struct SpawnResult {
	pid_t pid;
	int stdout_fd;
};

SpawnResult SpawnWorker(const std::vector<std::string> &final_argv,
                         const std::optional<std::string> &worker_stderr_path) {
	int pipefd[2];
	if (::pipe(pipefd) != 0) {
		throw IOException("vgi launcher: pipe() failed: %s", std::strerror(errno));
	}
	// CLOEXEC the parent-retained read end so it doesn't leak into any
	// later fork+exec in the host process.  The child clears CLOEXEC on
	// pipefd[1] via dup2() onto STDOUT_FILENO before exec.  macOS lacks
	// pipe2(), so we set both flags explicitly here.
	if (::fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) != 0 || ::fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) != 0) {
		int saved = errno;
		::close(pipefd[0]);
		::close(pipefd[1]);
		throw IOException("vgi launcher: fcntl(FD_CLOEXEC) failed: %s", std::strerror(saved));
	}

	pid_t pid = ::fork();
	if (pid < 0) {
		int saved = errno;
		::close(pipefd[0]);
		::close(pipefd[1]);
		throw IOException("vgi launcher: fork() failed: %s", std::strerror(saved));
	}
	if (pid == 0) {
		// --- Child ---
		// Replace stdout with the write end of the pipe.
		::close(pipefd[0]);
		if (::dup2(pipefd[1], STDOUT_FILENO) < 0) {
			::_exit(126);
		}
		::close(pipefd[1]);

		// stderr → either the user's file or /dev/null.
		int err_fd;
		if (worker_stderr_path.has_value()) {
			err_fd = ::open(worker_stderr_path->c_str(),
			                 O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
		} else {
			err_fd = ::open("/dev/null", O_WRONLY | O_CLOEXEC);
		}
		if (err_fd < 0) {
			::_exit(126);
		}
		if (::dup2(err_fd, STDERR_FILENO) < 0) {
			::_exit(126);
		}
		::close(err_fd);

		// stdin → /dev/null so the worker can't accidentally block on it.
		int in_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
		if (in_fd < 0) {
			::_exit(126);
		}
		if (::dup2(in_fd, STDIN_FILENO) < 0) {
			::_exit(126);
		}
		::close(in_fd);

		// Build argv array; exec it.  We deliberately do NOT setsid() so
		// terminal SIGINT/SIGHUP propagate from the controlling DuckDB
		// process to the worker on POSIX.
		std::vector<char *> raw;
		raw.reserve(final_argv.size() + 1);
		for (const auto &a : final_argv) {
			raw.push_back(const_cast<char *>(a.c_str()));
		}
		raw.push_back(nullptr);
		::execvp(raw[0], raw.data());
		// If we reach here, exec failed.
		::_exit(127);
	}

	// --- Parent ---
	::close(pipefd[1]);
	return {pid, pipefd[0]};
}

// Read from `fd` until either ParseDiscoveryLine returns kFound, the worker
// exits, or `deadline` passes.  On success: closes the pipe (forcing
// SIGPIPE on any contract-violating worker that writes more stdout) and
// hands the worker pid to the singleton reaper.  Throws IOException on
// any failure.
//
// Buffer is hard-capped at ``kDiscoveryBufferLimit`` so a hostile or
// runaway worker can't OOM us with unbounded prefix noise.
void WaitForReadinessAndDetach(pid_t worker_pid, int stdout_fd,
                                const std::string &expected_path,
                                std::chrono::steady_clock::time_point deadline) {
	std::string buffer;
	char chunk[4096];
	auto fail = [&](const std::string &msg) {
		::kill(worker_pid, SIGTERM);
		::close(stdout_fd);
		PidReaper::Instance().Register(worker_pid);
		throw IOException("%s", msg);
	};
	while (true) {
		auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			fail("vgi launcher: worker did not emit UNIX:<path> within startup timeout");
		}
		auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);

		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(stdout_fd, &rfds);
		struct timeval tv;
		tv.tv_sec = static_cast<time_t>(remaining.count() / 1000);
		tv.tv_usec = static_cast<suseconds_t>((remaining.count() % 1000) * 1000);
		int sel = ::select(stdout_fd + 1, &rfds, nullptr, nullptr, &tv);
		if (sel < 0) {
			if (errno == EINTR) {
				continue;
			}
			fail(std::string("vgi launcher: select() failed: ") + std::strerror(errno));
		}
		if (sel == 0) {
			// Loop will hit deadline check.
			continue;
		}
		ssize_t n = ::read(stdout_fd, chunk, sizeof(chunk));
		if (n == 0) {
			// EOF — worker closed stdout (likely exited).
			::close(stdout_fd);
			int status = 0;
			::waitpid(worker_pid, &status, 0);
			throw IOException(
			    "vgi launcher: worker exited before emitting UNIX:<path> (status=%d)", status);
		}
		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			fail(std::string("vgi launcher: read() from worker stdout failed: ") +
			     std::strerror(errno));
		}
		if (buffer.size() + static_cast<size_t>(n) > kDiscoveryBufferLimit) {
			fail("vgi launcher: worker emitted >1 MiB of pre-discovery output without "
			     "the UNIX:<path> line; aborting");
		}
		buffer.append(chunk, static_cast<size_t>(n));
		auto parse = launcher::ParseDiscoveryLine(buffer, expected_path);
		if (parse == launcher::DiscoveryParseResult::kMismatch) {
			fail("vgi launcher: worker bound to a different path than requested");
		}
		if (parse == launcher::DiscoveryParseResult::kFound) {
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
		launcher::ValidateUnixPathLength(sock_path);
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

	auto spawn = SpawnWorker(argv, cfg.worker_stderr_path);
	auto deadline = std::chrono::steady_clock::now() + cfg.worker_startup_timeout;
	WaitForReadinessAndDetach(spawn.pid, spawn.stdout_fd, sock_path, deadline);

	// Lock auto-releases as guard goes out of scope on return.
	return sock_path;
}

} // namespace vgi
} // namespace duckdb

#endif // !__EMSCRIPTEN__
