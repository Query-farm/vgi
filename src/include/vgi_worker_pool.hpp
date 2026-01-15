#pragma once

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

// Represents a pooled worker subprocess ready for reuse.
// Holds a subprocess that has completed an invocation and is waiting for the next one.
class PooledWorker {
public:
	// Create a pooled worker from an existing subprocess.
	// The subprocess should have completed an invocation and be ready for reuse.
	// stderr_fd is the stderr file descriptor (owned by this object, will be closed on destroy)
	PooledWorker(std::unique_ptr<SubProcess> proc, const std::string &worker_path, int stderr_fd = -1);
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

	// Get and release the stderr fd (releases ownership, returns -1 if none)
	int ReleaseStderrFd();

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

private:
	std::unique_ptr<SubProcess> proc_;
	std::string worker_path_;
	std::chrono::steady_clock::time_point pooled_at_;
	int stderr_fd_ = -1; // Stderr file descriptor (owned)
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
	// max_pool_size: 0 = unlimited, >0 = discard if pool would exceed this size
	void Release(std::unique_ptr<PooledWorker> worker, size_t max_pool_size = 0);

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

	// Set the idle timeout for worker cleanup (default 5 seconds)
	void SetIdleTimeout(std::chrono::seconds timeout);

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

	// Cleanup thread
	std::thread cleanup_thread_;
	std::atomic<bool> shutdown_ {false};
	std::condition_variable cleanup_cv_;
	std::mutex cleanup_mutex_; // Separate mutex for condition variable

	// Configuration
	std::chrono::seconds idle_timeout_ {5};
};

} // namespace vgi
} // namespace duckdb
