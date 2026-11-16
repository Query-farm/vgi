// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_settings_defaults.hpp"
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
#include <tuple>
#include <vector>

namespace duckdb {
class ClientContext;

namespace vgi {

// Per-path pool configuration, set at ATTACH time via ConfigurePath().
struct PoolSettings {
	size_t max_pool_size = 0;        // 0 = pool disabled
	size_t idle_timeout_seconds = defaults::WORKER_POOL_IDLE_LIMIT_SECONDS;
};

// The session's vgi_worker_pool_max / vgi_worker_pool_idle_limit_seconds.
PoolSettings PoolSettingsFromSession(ClientContext &context);

// Workers are reused only when all three fields match: a worker spawned for one
// data_version_spec must never serve a differently-versioned attach. Empty
// strings (unconstrained) form their own bucket.
struct PoolKey {
	std::string worker_path;
	std::string data_version_spec;
	std::string implementation_version;

	bool operator<(const PoolKey &other) const {
		return std::tie(worker_path, data_version_spec, implementation_version) <
		       std::tie(other.worker_path, other.data_version_spec, other.implementation_version);
	}

	bool operator==(const PoolKey &other) const {
		return worker_path == other.worker_path && data_version_spec == other.data_version_spec &&
		       implementation_version == other.implementation_version;
	}
};

// An idle worker subprocess awaiting reuse. It owns a running StderrDrainer so
// the stderr pipe keeps draining while idle; a full pipe would block the worker
// mid-log and stall the next RPC. The drainer moves to the acquiring connection
// and back into a new PooledWorker on release.
class PooledWorker {
public:
	// `drainer` (may be null) must already be attached to the worker's stderr fd.
	// Destruction stops the drainer and closes the fd.
	PooledWorker(std::unique_ptr<SubProcess> proc, PoolKey key,
	             std::unique_ptr<StderrDrainer> drainer);
	~PooledWorker();

	PooledWorker(PooledWorker &&other) noexcept;
	PooledWorker &operator=(PooledWorker &&other) noexcept;
	PooledWorker(const PooledWorker &) = delete;
	PooledWorker &operator=(const PooledWorker &) = delete;

	// Non-blocking.
	bool IsAlive() const;

	std::unique_ptr<SubProcess> Release();

	// nullptr if none was held.
	std::unique_ptr<StderrDrainer> ReleaseDrainer();

	const PoolKey &GetKey() const {
		return key_;
	}

	const std::string &GetWorkerPath() const {
		return key_.worker_path;
	}

	std::chrono::steady_clock::time_point GetPooledAt() const {
		return pooled_at_;
	}

	pid_t GetPid() const;

	// Set by VgiWorkerPool::Release from the path's settings.
	void SetIdleTimeout(std::chrono::seconds timeout) {
		idle_timeout_ = timeout;
	}
	std::chrono::seconds GetIdleTimeout() const {
		return idle_timeout_;
	}

private:
	std::unique_ptr<SubProcess> proc_;
	PoolKey key_;
	std::chrono::steady_clock::time_point pooled_at_;
	// Non-null while pooled except in stderr passthrough mode.
	std::unique_ptr<StderrDrainer> drainer_;
	std::chrono::seconds idle_timeout_ {defaults::WORKER_POOL_IDLE_LIMIT_SECONDS};
};

// Thread-safe singleton pool of subprocess workers. Never blocks: when
// TryAcquire returns nullptr the caller spawns a new worker.
class VgiWorkerPool {
public:
	static VgiWorkerPool &Instance();

	// Most-recently-released live worker for exactly `key`, or nullptr.
	std::unique_ptr<PooledWorker> TryAcquire(const PoolKey &key);

	// Lets the caller log the release and resulting pool state in one line.
	struct ReleaseResult {
		bool pooled = false;         // True if the worker was added to the pool
		pid_t pid = -1;              // PID of the released worker (-1 if input was null)
		std::string skip_reason;     // Populated when pooled=false: "dead", "disabled", "path_full"
		size_t pool_size = 0;        // Size of this key's bucket after the release
		size_t total_pool_size = 0;  // Total pooled workers across all buckets after the release
	};

	// Pools into the worker->GetKey() bucket, sized by the worker_path's
	// ConfigurePath settings, else its ConfigureDiscoveryPath settings, else
	// the compile-time defaults.
	ReleaseResult Release(std::unique_ptr<PooledWorker> worker);

	// Returns the number of workers flushed.
	size_t Flush();

	struct PoolEntry {
		std::string worker_path;
		std::string data_version_spec;
		std::string implementation_version;
		pid_t pid;
		int64_t age_seconds;
	};

	// For vgi_worker_pool().
	std::vector<PoolEntry> GetPoolEntries() const;

	// Keyed by worker_path only (all version buckets combined).
	struct PoolStats {
		std::string worker_path;
		uint64_t hits;
		uint64_t misses;
	};

	std::vector<PoolStats> GetPoolStats() const;

	// Called by the caller when TryAcquire returns nullptr.
	void RecordMiss(const std::string &worker_path);

	// Called at ATTACH; a later ATTACH of the same path overwrites (last wins).
	void ConfigurePath(const std::string &path, const PoolSettings &settings);

	// Called by vgi_catalogs() discovery; last wins, but never overrides an
	// ATTACH's ConfigurePath settings for the same path.
	void ConfigureDiscoveryPath(const std::string &path, const PoolSettings &settings);

private:
	VgiWorkerPool();
	~VgiWorkerPool();
	VgiWorkerPool(const VgiWorkerPool &) = delete;
	VgiWorkerPool &operator=(const VgiWorkerPool &) = delete;

	void CleanupThread();

	// Caller holds mutex_.
	void RemoveStaleWorkersLocked();

	// Caller holds mutex_.
	size_t TotalPoolSizeLocked() const;

	mutable std::mutex mutex_;
	std::map<PoolKey, std::deque<std::unique_ptr<PooledWorker>>> pools_;

	std::map<std::string, PoolSettings> path_configs_;      // keyed by worker_path
	std::map<std::string, PoolSettings> discovery_configs_; // keyed by worker_path

	PoolSettings default_settings_ {defaults::WORKER_POOL_MAX, defaults::WORKER_POOL_IDLE_LIMIT_SECONDS};

	std::map<std::string, std::pair<uint64_t, uint64_t>> stats_; // {hits, misses}

	std::thread cleanup_thread_;
	std::atomic<bool> shutdown_ {false};
	std::condition_variable cleanup_cv_;
	std::mutex cleanup_mutex_; // only for cleanup_cv_
};

} // namespace vgi
} // namespace duckdb
