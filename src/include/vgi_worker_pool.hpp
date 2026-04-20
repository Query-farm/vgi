#pragma once

#include "vgi_stderr_drainer.hpp"
#include "vgi_subprocess.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace duckdb {
namespace vgi {

// Per-path pool configuration, set at ATTACH time via ConfigurePath().
struct PoolSettings {
	size_t max_pool_size = 0;        // 0 = pool disabled
	size_t idle_timeout_seconds = 5; // How long idle workers survive
};

// Represents a pooled worker subprocess ready for reuse.
// Holds a subprocess that has completed an invocation and is waiting for the next one.
//
// The pooled worker owns a live StderrDrainer — its reader thread keeps
// consuming the worker's stderr pipe while the worker sits idle in the
// pool. This prevents the pipe buffer from filling (~16KB on macOS), which
// would block the Python worker mid-log and appear as a stalled RPC on the
// next acquisition. The drainer is handed off on Acquire and moved into a
// fresh drainer on Release — it's always running whenever a worker is idle.
class PooledWorker {
public:
	// Create a pooled worker from an existing subprocess + running drainer.
	// The drainer (if non-null) should already be attached to the worker's
	// stderr fd; it will keep draining while the worker is idle in the pool.
	// Destroying the PooledWorker stops the drainer and closes the fd.
	PooledWorker(std::unique_ptr<SubProcess> proc, const std::string &worker_path,
	             std::unique_ptr<StderrDrainer> drainer);
	~PooledWorker();

	// Move-only (owns subprocess)
	PooledWorker(PooledWorker &&other) noexcept;
	PooledWorker &operator=(PooledWorker &&other) noexcept;
	PooledWorker(const PooledWorker &) = delete;
	PooledWorker &operator=(const PooledWorker &) = delete;

	// Check if the worker process is still alive (non-blocking)
	bool IsAlive() const;

	// Get the subprocess (releases ownership)
	std::unique_ptr<SubProcess> Release();

	// Transfer ownership of the (still-running) stderr drainer. Returns
	// nullptr if none was held. Callers use the returned drainer for their
	// RPC lifetime, then hand it back to a new PooledWorker on release.
	std::unique_ptr<StderrDrainer> ReleaseDrainer();

	// Get worker path
	const std::string &GetWorkerPath() const {
		return worker_path_;
	}

	// Get time when this worker was added to the pool
	std::chrono::steady_clock::time_point GetPooledAt() const {
		return pooled_at_;
	}

	// Get PID of the subprocess
	pid_t GetPid() const;

	// Per-worker idle timeout (set when added to pool)
	void SetIdleTimeout(std::chrono::seconds timeout) {
		idle_timeout_ = timeout;
	}
	std::chrono::seconds GetIdleTimeout() const {
		return idle_timeout_;
	}

private:
	std::unique_ptr<SubProcess> proc_;
	std::string worker_path_;
	std::chrono::steady_clock::time_point pooled_at_;
	// Keeps draining stderr while the worker is idle in the pool — ownership
	// moves back to the RPC caller on Acquire, and a fresh drainer comes back
	// on Release. Never nullptr during a worker's pooled lifetime (unless
	// passthrough mode). Destructor stops the thread and closes the fd.
	std::unique_ptr<StderrDrainer> drainer_;
	std::chrono::seconds idle_timeout_ {5};          // Default 5s, overridden by per-path config
};

// Thread-safe singleton pool for reusing VGI worker subprocesses.
// Workers can be acquired and released. The pool never blocks - if no worker
// is available, TryAcquire returns nullptr and the caller should create a new one.
class VgiWorkerPool {
public:
	// Get the singleton instance
	static VgiWorkerPool &Instance();

	// Acquire a worker from the pool for the given worker_path.
	// Returns nullptr if no worker is available (never blocks).
	// The returned worker is guaranteed to be alive.
	std::unique_ptr<PooledWorker> TryAcquire(const std::string &worker_path);

	// Return a worker to the pool for reuse.
	// Uses per-path config (from ConfigurePath) if available, otherwise default settings.
	void Release(std::unique_ptr<PooledWorker> worker);

	// Set default pool settings for paths without explicit per-path config
	// (e.g., direct vgi_table_function() calls that don't go through ATTACH).
	// Called once at extension load time with values from vgi_worker_pool_max / idle_limit settings.
	void SetDefaultSettings(const PoolSettings &settings);

	// Flush all workers from the pool, returns count flushed
	size_t Flush();

	// Pool entry info for diagnostics
	struct PoolEntry {
		std::string worker_path;
		pid_t pid;
		int64_t age_seconds;
	};

	// Get pool statistics for the vgi_worker_pool() table function
	std::vector<PoolEntry> GetPoolEntries() const;

	// Pool statistics by worker path
	struct PoolStats {
		std::string worker_path;
		uint64_t hits;     // Successfully acquired from pool
		uint64_t misses;   // Pool empty, had to spawn new worker
	};

	// Get hit/miss statistics for each worker path
	std::vector<PoolStats> GetPoolStats() const;

	// Record a pool hit (called internally by TryAcquire)
	void RecordHit(const std::string &worker_path);

	// Record a pool miss (called by callers when TryAcquire returns nullptr)
	void RecordMiss(const std::string &worker_path);

	// Configure per-path pool settings (called at ATTACH time).
	// If a config already exists for this path with different values, it is overwritten with a warning.
	void ConfigurePath(const std::string &path, const PoolSettings &settings);

private:
	VgiWorkerPool();
	~VgiWorkerPool();
	VgiWorkerPool(const VgiWorkerPool &) = delete;
	VgiWorkerPool &operator=(const VgiWorkerPool &) = delete;

	// Cleanup thread function
	void CleanupThread();

	// Remove dead and stale workers (must hold mutex)
	void RemoveStaleWorkersLocked();

	// Get total count of workers across all paths (must hold mutex)
	size_t TotalPoolSizeLocked() const;

	mutable std::mutex mutex_;
	std::map<std::string, std::deque<std::unique_ptr<PooledWorker>>> pools_;

	// Per-path pool configuration (set at ATTACH time)
	std::map<std::string, PoolSettings> path_configs_;

	// Default settings for paths without explicit per-path config
	PoolSettings default_settings_ {256, 5};

	// Hit/miss statistics by worker path
	std::map<std::string, std::pair<uint64_t, uint64_t>> stats_; // {hits, misses}

	// Cleanup thread
	std::thread cleanup_thread_;
	std::atomic<bool> shutdown_ {false};
	std::condition_variable cleanup_cv_;
	std::mutex cleanup_mutex_; // Separate mutex for condition variable
};

} // namespace vgi
} // namespace duckdb
