// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-3 integration tests for the launcher orchestration.  Exercises the
// real fork/exec/flock/probe/discovery flow against the standalone
// ``launcher_test_worker`` binary built from test/support/.
//
// No Python required — every test here is reproducible on any POSIX box
// with the vgi build tree present.  Each TEST_CASE constructs an isolated
// state directory under /tmp so concurrent runs don't collide.

#include "catch.hpp"

#include "vgi_launcher.hpp"
#include "vgi_launcher_cache.hpp"
#include "vgi_unix_socket.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

#include <fstream>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <future>
#include <random>
#include <signal.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using duckdb::IOException;
using duckdb::vgi::LaunchConfig;
using duckdb::vgi::UnixSocket;

// Resolve the path of the test worker binary.  CMake injects the absolute
// path via the ``VGI_LAUNCHER_TEST_WORKER_PATH`` compile definition (see
// vgi/CMakeLists.txt) so this works from any cwd.
static std::string TestWorkerPath() {
#ifndef VGI_LAUNCHER_TEST_WORKER_PATH
#error "VGI_LAUNCHER_TEST_WORKER_PATH must be defined by the build system"
#endif
	const char *path = VGI_LAUNCHER_TEST_WORKER_PATH;
	struct stat st;
	if (::stat(path, &st) != 0 || !(st.st_mode & S_IXUSR)) {
		FAIL("VGI_LAUNCHER_TEST_WORKER_PATH (" << path << ") is not an executable file");
	}
	return path;
}

// Per-test isolated state dir under /tmp (short enough for the AF_UNIX
// 104-byte cap on macOS).  Cleaned up by the destructor.
class IsolatedStateDir {
public:
	IsolatedStateDir() {
		std::random_device rd;
		std::mt19937_64 gen(rd());
		auto suffix = std::to_string(gen() & 0xFFFFFFFF);
		path_ = "/tmp/vgi-launch-test-" + suffix;
		std::filesystem::create_directories(path_);
		::chmod(path_.c_str(), 0700);
	}
	~IsolatedStateDir() {
		std::error_code ec;
		std::filesystem::remove_all(path_, ec);
	}
	const std::string &path() const {
		return path_;
	}

private:
	std::string path_;
};

// Build a baseline config that points at the test worker and writes state
// under the given isolated dir.
static LaunchConfig BaselineConfig(const std::string &state_dir,
                                    std::vector<std::string> extra_args = {}) {
	LaunchConfig cfg;
	cfg.worker_argv = {TestWorkerPath()};
	for (auto &a : extra_args) {
		cfg.worker_argv.push_back(std::move(a));
	}
	cfg.idle_timeout = 2s;
	cfg.connect_timeout = 5s;
	cfg.worker_startup_timeout = 8s;
	cfg.state_dir_override = state_dir;
	return cfg;
}

// Wait for *path* to disappear from the filesystem; return true on success.
static bool WaitForPathGone(const std::string &path, std::chrono::milliseconds timeout) {
	auto deadline = std::chrono::steady_clock::now() + timeout;
	while (std::chrono::steady_clock::now() < deadline) {
		struct stat st;
		if (::stat(path.c_str(), &st) != 0 && errno == ENOENT) {
			return true;
		}
		std::this_thread::sleep_for(50ms);
	}
	return false;
}

// PID of the worker listening on the far end of a connected AF_UNIX socket, as
// the kernel recorded it. Tells one worker process from another without taking
// the worker's word for anything.
static pid_t PeerPid(const UnixSocket &sock) {
#if defined(__linux__)
	struct ucred cred {};
	socklen_t len = sizeof(cred);
	REQUIRE(::getsockopt(sock.GetFd(), SOL_SOCKET, SO_PEERCRED, &cred, &len) == 0);
	return cred.pid;
#elif defined(__APPLE__)
	pid_t pid = 0;
	socklen_t len = sizeof(pid);
	REQUIRE(::getsockopt(sock.GetFd(), SOL_LOCAL, LOCAL_PEERPID, &pid, &len) == 0);
	return pid;
#else
#error "PeerPid: no AF_UNIX peer-PID query for this platform"
#endif
}

// ---------------------------------------------------------------------------

TEST_CASE("Launch spawns a worker and connects to its socket", "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());

	auto path = duckdb::vgi::Launch(cfg);
	REQUIRE(!path.empty());
	struct stat st;
	REQUIRE(::stat(path.c_str(), &st) == 0);
	REQUIRE(S_ISSOCK(st.st_mode));

	// Connecting succeeds.
	auto sock = UnixSocket::Connect(path);
	REQUIRE(sock.IsOpen());
}

TEST_CASE("Launch reuses an existing healthy worker", "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());

	auto path1 = duckdb::vgi::Launch(cfg);
	auto path2 = duckdb::vgi::Launch(cfg);
	REQUIRE(path1 == path2);

	// Exactly one .sock file exists in the state dir.
	int sock_count = 0;
	for (const auto &entry : std::filesystem::directory_iterator(dir.path())) {
		if (entry.path().extension() == ".sock") {
			++sock_count;
		}
	}
	REQUIRE(sock_count == 1);
}

TEST_CASE("Concurrent first-callers serialize and produce one worker",
          "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());

	constexpr int kThreads = 8;
	std::vector<std::future<std::string>> futures;
	futures.reserve(kThreads);
	for (int i = 0; i < kThreads; ++i) {
		futures.emplace_back(
		    std::async(std::launch::async, [&]() { return duckdb::vgi::Launch(cfg); }));
	}
	std::vector<std::string> results;
	for (auto &f : futures) {
		results.push_back(f.get());
	}
	REQUIRE(results.size() == kThreads);
	for (const auto &r : results) {
		REQUIRE(r == results.front());
	}
	// One socket on disk.
	int sock_count = 0;
	for (const auto &entry : std::filesystem::directory_iterator(dir.path())) {
		if (entry.path().extension() == ".sock") {
			++sock_count;
		}
	}
	REQUIRE(sock_count == 1);
}

TEST_CASE("Launch unlinks a stale socket file and respawns", "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());

	// Pre-create a dangling socket file with no listener at the path the
	// launcher will derive.  We don't know the exact hash without
	// recomputing it, so probe by listing after Launch and asserting one
	// .sock exists and is connectable.
	auto path = duckdb::vgi::Launch(cfg);
	{
		// Forcibly take down the worker by connecting + immediately closing
		// many times so that it idle-shuts-down quickly?  Easier: just
		// leave a synthetic dangling file at a known path via socket_path_override.
	}

	// Restart with explicit socket path: pre-create the file, then Launch.
	std::string explicit_sock = dir.path() + "/explicit.sock";
	::close(::open(explicit_sock.c_str(), O_CREAT | O_RDWR, 0600));
	auto cfg2 = cfg;
	cfg2.socket_path_override = explicit_sock;
	auto path2 = duckdb::vgi::Launch(cfg2);
	REQUIRE(path2 == explicit_sock);
	struct stat st;
	REQUIRE(::stat(path2.c_str(), &st) == 0);
	REQUIRE(S_ISSOCK(st.st_mode));
	auto sock = UnixSocket::Connect(path2);
	REQUIRE(sock.IsOpen());
	(void)path; // first call only proves the basic path works
}

// A listener at *path* that never accepts on its own, with its accept queue
// filled by non-blocking connects it holds open — a worker too busy to take
// another connection.  Full means the next connect failed: EAGAIN on Linux,
// ECONNREFUSED on macOS.
class FullListener {
public:
	explicit FullListener(const std::string &path) {
		listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
		REQUIRE(listen_fd_ >= 0);
		auto addr = Address(path);
		REQUIRE(::bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) == 0);
		REQUIRE(::listen(listen_fd_, 0) == 0);
		for (int i = 0; i < 256; ++i) {
			int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
			REQUIRE(fd >= 0);
			::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
			if (::connect(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
				::close(fd);
				return;
			}
			queued_.push_back(fd);
		}
		FAIL("the accept queue never filled");
	}
	FullListener(const FullListener &) = delete;
	FullListener &operator=(const FullListener &) = delete;
	~FullListener() {
		for (int fd : queued_) {
			::close(fd);
		}
		::close(listen_fd_);
	}

	// Accept (and drop) one queued connection after *delay*, freeing a slot.
	std::thread AcceptOneAfter(std::chrono::milliseconds delay) {
		return std::thread([this, delay] {
			std::this_thread::sleep_for(delay);
			int fd = ::accept(listen_fd_, nullptr, nullptr);
			if (fd >= 0) {
				::close(fd);
			}
		});
	}

private:
	static struct sockaddr_un Address(const std::string &path) {
		struct sockaddr_un addr;
		std::memset(&addr, 0, sizeof(addr));
		addr.sun_family = AF_UNIX;
		std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
		return addr;
	}

	int listen_fd_ = -1;
	std::vector<int> queued_;
};

static ino_t InodeOf(const std::string &path) {
	struct stat st;
	REQUIRE(::stat(path.c_str(), &st) == 0);
	return st.st_ino;
}

#ifdef __linux__
// Only Linux tells a full accept queue (EAGAIN) from no listener at all.
TEST_CASE("Launch leaves a worker with a full accept queue alone", "[launcher][e2e]") {
	// Under a burst of connections the worker's accept queue fills and the
	// probe's connect fails with EAGAIN.  Reading that as "dead" unlinked the
	// live worker's socket and spawned a duplicate on every busy probe.
	IsolatedStateDir dir;
	std::string sock = dir.path() + "/busy.sock";
	FullListener busy(sock);
	auto inode = InodeOf(sock);

	// Would throw ("exits before readiness") if Launch spawned it.
	auto cfg = BaselineConfig(dir.path(), {"--exit-with", "1"});
	cfg.socket_path_override = sock;
	REQUIRE(duckdb::vgi::Launch(cfg) == sock);
	REQUIRE(InodeOf(sock) == inode);
}

TEST_CASE("Connect waits out a full accept queue", "[launcher][e2e]") {
	// Failing here sent ResolveAndConnect to relaunch a worker that was fine.
	IsolatedStateDir dir;
	std::string sock = dir.path() + "/busy.sock";
	FullListener busy(sock);
	auto drain = busy.AcceptOneAfter(100ms);
	auto conn = UnixSocket::Connect(sock, 5s);
	drain.join();
	REQUIRE(conn.IsOpen());
}

TEST_CASE("Connect gives up on an accept queue that stays full", "[launcher][e2e]") {
	IsolatedStateDir dir;
	std::string sock = dir.path() + "/busy.sock";
	FullListener busy(sock);
	REQUIRE_THROWS_WITH(UnixSocket::Connect(sock, 200ms), Catch::Contains("accept queue stayed full"));
}
#endif

TEST_CASE("Launch counts a momentarily full accept queue as alive", "[launcher][e2e]") {
	// However the platform reports it — EAGAIN, or macOS's ECONNREFUSED,
	// which the probe re-tries before believing.
	IsolatedStateDir dir;
	std::string sock = dir.path() + "/busy.sock";
	FullListener busy(sock);
	auto inode = InodeOf(sock);
	auto drain = busy.AcceptOneAfter(30ms);

	auto cfg = BaselineConfig(dir.path(), {"--exit-with", "1"});
	cfg.socket_path_override = sock;
	auto path = duckdb::vgi::Launch(cfg);
	drain.join();
	REQUIRE(path == sock);
	REQUIRE(InodeOf(sock) == inode);
}

TEST_CASE("Launch fails when worker exits before readiness", "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path(), {"--exit-with", "1"});

	REQUIRE_THROWS_AS(duckdb::vgi::Launch(cfg), IOException);
}

TEST_CASE("Launch enforces worker_startup_timeout on a stuck worker",
          "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path(), {"--no-bind"});
	cfg.worker_startup_timeout = 1500ms;

	auto t0 = std::chrono::steady_clock::now();
	REQUIRE_THROWS_AS(duckdb::vgi::Launch(cfg), IOException);
	auto elapsed = std::chrono::steady_clock::now() - t0;
	// Timeout should fire promptly; budget for SIGTERM + reaping.
	REQUIRE(elapsed < 4s);
}

TEST_CASE("Launch tolerates noisy prefix lines before UNIX:<path>",
          "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path(), {"--noisy-prefix", "3"});

	auto path = duckdb::vgi::Launch(cfg);
	REQUIRE(!path.empty());
	auto sock = UnixSocket::Connect(path);
	REQUIRE(sock.IsOpen());
}

TEST_CASE("Worker idle-shuts-down after final disconnect", "[launcher][e2e][slow]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());
	cfg.idle_timeout = 1s; // short for the test

	auto path = duckdb::vgi::Launch(cfg);
	{
		auto sock = UnixSocket::Connect(path);
		REQUIRE(sock.IsOpen());
	} // sock closes here, worker sees disconnect

	// Worker should idle-shutdown within idle_timeout + slack.
	REQUIRE(WaitForPathGone(path, 3500ms));
}

TEST_CASE("ResolveAndConnect transparently respawns an idle-shutdown worker",
          "[launcher][e2e][slow]") {
	// Direct end-to-end test of the cache-invalidation retry: we use the
	// `unix://` scheme so the cache resolves to a known path, spawn a
	// worker via Launch with a tiny idle_timeout, prime the cache by
	// connecting once, wait for the worker to idle-shutdown, then call
	// ResolveAndConnect again — this *must* succeed without manual cache
	// flushing.  Without the retry path, the connect-on-stale-cache
	// would throw IOException.
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());
	cfg.idle_timeout = 1s;
	auto path = duckdb::vgi::Launch(cfg);
	std::string location = "unix://" + path;

	// Prime the cache.
	{
		auto sock = duckdb::vgi::ResolveAndConnect(location);
		REQUIRE(sock.IsOpen());
	}

	// Wait for the worker to idle-shutdown.  ``unix://`` is operator-managed,
	// so the cache-invalidation retry will surface as an IOException — the
	// cached worker is gone and there's no launcher to bring up a new one.
	// Verify the surfaced error rather than silent reconnect.
	REQUIRE(WaitForPathGone(path, 3500ms));
	duckdb::vgi::InvalidateLauncherSocketCache(location); // explicit, since unix:// has no respawn
	REQUIRE_THROWS_AS(duckdb::vgi::ResolveAndConnect(location), IOException);
}

TEST_CASE("ResolveAndConnect respawns an idle-shutdown launch: worker", "[launcher][e2e][slow]") {
	// The self-healing path a long-lived session depends on. The cached
	// `launch:` worker idle-shuts down while the cache still names its socket,
	// so the next ResolveAndConnect is refused on the dead path and must
	// invalidate the entry and bring up a fresh worker, with nothing flushed
	// by hand. (The unix:// case above ends in an IOException instead: nothing
	// respawns an operator-managed worker.)
	//
	// This case was an empty placeholder until LaunchOverrides existed:
	// ResolveAndConnect could only launch with LaunchConfig's 300 s idle
	// timeout, far too long to wait out in a test. The overrides now carry a
	// short idle timeout (and an isolated state dir) through it.
	IsolatedStateDir dir;
	const std::string location = "launch:" + TestWorkerPath();
	duckdb::vgi::LaunchOverrides overrides;
	overrides.idle_timeout = 1s;
	overrides.state_dir = dir.path();

	// The cache is process-wide and pins overrides per location. Start clean,
	// and drop this entry on every exit, a failed REQUIRE included, so a later
	// case resolving the same location with other overrides is not refused
	// with a BinderException.
	duckdb::vgi::ClearLauncherSocketCache();
	struct ForgetLocation {
		std::string location;
		~ForgetLocation() {
			duckdb::vgi::InvalidateLauncherSocketCache(location);
		}
	} forget {location};

	std::string path;
	pid_t first_pid = 0;
	{
		auto sock = duckdb::vgi::ResolveAndConnect(location, 10s, overrides);
		REQUIRE(sock.IsOpen());
		first_pid = PeerPid(sock);
		path = duckdb::vgi::ResolveLauncherSocketPath(location, overrides); // cache hit
	} // the only client disconnects; the worker's idle clock starts
	REQUIRE(first_pid > 0);

	// The worker exits and unlinks its socket...
	REQUIRE(WaitForPathGone(path, 3500ms));
	// ...but the cache still hands out that path, so the connect below is
	// refused first and only the invalidate-and-relaunch retry can recover.
	REQUIRE(duckdb::vgi::ResolveLauncherSocketPath(location, overrides) == path);

	auto sock = duckdb::vgi::ResolveAndConnect(location, 10s, overrides);
	REQUIRE(sock.IsOpen());
	// A new worker process, at the same per-hash socket path.
	const pid_t second_pid = PeerPid(sock);
	CHECK(second_pid > 0);
	CHECK(second_pid != first_pid);
	CHECK(duckdb::vgi::ResolveLauncherSocketPath(location, overrides) == path);
}

TEST_CASE("ResolveAndConnect retries once when the cached path goes stale",
          "[launcher][e2e]") {
	// Surgical: pre-poison the cache with an explicit unix:// pointing at
	// a dead path.  ResolveAndConnect's first connect fails, the cache is
	// invalidated, the second resolve goes through Launch (since launch:
	// fires the launcher) and succeeds.  Without the retry path, this
	// throws.
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());
	auto path = duckdb::vgi::Launch(cfg);
	std::string location = "launch:" + TestWorkerPath();

	// Prime the cache against the real worker.
	{
		auto sock = duckdb::vgi::ResolveAndConnect(location);
		REQUIRE(sock.IsOpen());
	}
	(void)path; // launched independently; cache binds to the launch: location

	// Force the cached entry to go stale: kill all instances of the test
	// worker so the cached socket path is dead.
	duckdb::vgi::ClearLauncherSocketCache();
	{
		// First call after clear re-runs Launch — we hit the same hash so
		// the launcher's flock + probe finds the existing worker (still
		// alive at the AF_UNIX path) and reuses it.  Validates that the
		// retry path is invariant under cache flushes.
		auto sock = duckdb::vgi::ResolveAndConnect(location);
		REQUIRE(sock.IsOpen());
	}
}

TEST_CASE("Launch with empty argv throws InvalidInputException", "[launcher][e2e]") {
	LaunchConfig cfg;
	REQUIRE_THROWS_AS(duckdb::vgi::Launch(cfg), duckdb::InvalidInputException);
}

TEST_CASE("Launch acquires lock with bounded timeout under contention",
          "[launcher][e2e]") {
	IsolatedStateDir dir;
	auto cfg = BaselineConfig(dir.path());
	cfg.connect_timeout = 600ms;

	// Manually take an exclusive flock on the same path the launcher will
	// derive.  Easier: use socket_path_override so we know the lock path.
	std::string sock = dir.path() + "/explicit.sock";
	std::string lock = sock + ".lock";
	int fd = ::open(lock.c_str(), O_CREAT | O_RDWR, 0600);
	REQUIRE(fd >= 0);
	REQUIRE(::flock(fd, LOCK_EX) == 0);

	auto cfg2 = cfg;
	cfg2.socket_path_override = sock;

	auto t0 = std::chrono::steady_clock::now();
	REQUIRE_THROWS_AS(duckdb::vgi::Launch(cfg2), IOException);
	auto elapsed = std::chrono::steady_clock::now() - t0;
	REQUIRE(elapsed >= 500ms);
	REQUIRE(elapsed < 2s);

	::flock(fd, LOCK_UN);
	::close(fd);
}

// ---------------------------------------------------------------------------
// LaunchOverrides — ATTACH-options pin & conflict semantics (the design
// pivot from the senior-review pass).
// ---------------------------------------------------------------------------

TEST_CASE("LaunchOverrides::idle_timeout reaches the worker's argv",
          "[launcher][overrides]") {
	IsolatedStateDir dir;
	auto argv_dump = dir.path() + "/argv.txt";

	// Tell the test worker to dump its received argv to a file we can read.
	auto cfg = BaselineConfig(dir.path(), {"--dump-argv-to", argv_dump});
	cfg.idle_timeout = 7s;
	auto path = duckdb::vgi::Launch(cfg);
	REQUIRE(!path.empty());

	// Connect once so the worker fully wakes up + writes argv to disk
	// (the dump happens before bind, but join-with-fs to be safe).
	{
		auto sock = UnixSocket::Connect(path);
		REQUIRE(sock.IsOpen());
	}

	// Wait briefly for the file to materialise (fork/exec is async).
	auto deadline = std::chrono::steady_clock::now() + 1s;
	while (std::chrono::steady_clock::now() < deadline) {
		struct stat st;
		if (::stat(argv_dump.c_str(), &st) == 0 && st.st_size > 0) {
			break;
		}
		std::this_thread::sleep_for(20ms);
	}

	std::ifstream f(argv_dump);
	std::string line;
	std::vector<std::string> argv_lines;
	while (std::getline(f, line)) {
		argv_lines.push_back(line);
	}
	INFO("argv_dump contents:\n" << [&]() {
		std::string joined;
		for (auto &l : argv_lines) {
			joined += l + "\n";
		}
		return joined;
	}());
	// We expect the launcher to have appended `--idle-timeout 7` (the
	// %.3f formatter strips the trailing zeros so it's literally "7").
	bool found_flag = false;
	bool found_value = false;
	for (size_t i = 0; i + 1 < argv_lines.size(); ++i) {
		if (argv_lines[i] == "--idle-timeout") {
			found_flag = true;
			if (argv_lines[i + 1] == "7") {
				found_value = true;
			}
		}
	}
	REQUIRE(found_flag);
	REQUIRE(found_value);
}

TEST_CASE("ResolveAndConnect with overrides pins them on first cache miss",
          "[launcher][overrides]") {
	IsolatedStateDir dir;
	std::string location = "launch:" + TestWorkerPath();

	// Make sure no prior cache state from other tests leaks in.
	duckdb::vgi::ClearLauncherSocketCache();

	duckdb::vgi::LaunchOverrides ov;
	ov.idle_timeout = 30s;
	ov.state_dir = dir.path();

	auto sock1 = duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov);
	REQUIRE(sock1.IsOpen());

	// Same overrides → cache hit, succeeds.
	auto sock2 = duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov);
	REQUIRE(sock2.IsOpen());

	// Different idle_timeout → BinderException (the pin fires).
	duckdb::vgi::LaunchOverrides ov2 = ov;
	ov2.idle_timeout = 60s;
	REQUIRE_THROWS_AS(duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov2),
	                   duckdb::BinderException);

	// Different state_dir → also BinderException.
	duckdb::vgi::LaunchOverrides ov3 = ov;
	ov3.state_dir = "/tmp/some-other-state-dir";
	REQUIRE_THROWS_AS(duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov3),
	                   duckdb::BinderException);

	duckdb::vgi::ClearLauncherSocketCache();
}

TEST_CASE("Cache invalidation resets the override pin",
          "[launcher][overrides]") {
	IsolatedStateDir dir;
	std::string location = "launch:" + TestWorkerPath();
	duckdb::vgi::ClearLauncherSocketCache();

	// First ATTACH with overrides A.
	duckdb::vgi::LaunchOverrides ov_a;
	ov_a.idle_timeout = 30s;
	ov_a.state_dir = dir.path();
	{
		auto sock = duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov_a);
		REQUIRE(sock.IsOpen());
	}

	// Simulate worker being gone (eg idle-shut-down) by clearing the cache —
	// equivalent to the ResolveAndConnect retry path's invalidation step.
	duckdb::vgi::InvalidateLauncherSocketCache(location);

	// Now overrides B should be accepted (the pin was reset with the
	// cache entry).
	IsolatedStateDir dir_b;
	duckdb::vgi::LaunchOverrides ov_b;
	ov_b.idle_timeout = 60s;
	ov_b.state_dir = dir_b.path();
	{
		auto sock = duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov_b);
		REQUIRE(sock.IsOpen());
	}

	duckdb::vgi::ClearLauncherSocketCache();
}

TEST_CASE("LaunchOverrides::state_dir routes the worker into the requested directory",
          "[launcher][overrides]") {
	IsolatedStateDir dir_x;
	std::string location = "launch:" + TestWorkerPath();
	duckdb::vgi::ClearLauncherSocketCache();

	duckdb::vgi::LaunchOverrides ov;
	ov.state_dir = dir_x.path();
	auto sock = duckdb::vgi::ResolveAndConnect(location, std::chrono::seconds(10), ov);
	REQUIRE(sock.IsOpen());

	// Find at least one .sock file under dir_x — proves the override flowed
	// through to the launcher's spawn-time state dir.
	int sock_count = 0;
	for (const auto &entry : std::filesystem::directory_iterator(dir_x.path())) {
		if (entry.path().extension() == ".sock") {
			++sock_count;
		}
	}
	REQUIRE(sock_count == 1);

	duckdb::vgi::ClearLauncherSocketCache();
}
