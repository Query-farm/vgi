#include "vgi_worker_pool.hpp"

#include <unistd.h>

namespace duckdb {
namespace vgi {

//===--------------------------------------------------------------------===//
// PooledWorker
//===--------------------------------------------------------------------===//

PooledWorker::PooledWorker(std::unique_ptr<SubProcess> proc, const std::string &worker_path, int stderr_fd)
    : proc_(std::move(proc)), worker_path_(worker_path), pooled_at_(std::chrono::steady_clock::now()),
      stderr_fd_(stderr_fd) {
}

PooledWorker::~PooledWorker() {
	// Close stderr fd if still owned
	if (stderr_fd_ >= 0) {
		close(stderr_fd_);
	}
	// SubProcess destructor handles the rest
}

PooledWorker::PooledWorker(PooledWorker &&other) noexcept
    : proc_(std::move(other.proc_)), worker_path_(std::move(other.worker_path_)), pooled_at_(other.pooled_at_),
      stderr_fd_(other.stderr_fd_) {
	other.stderr_fd_ = -1; // Transfer ownership
}

PooledWorker &PooledWorker::operator=(PooledWorker &&other) noexcept {
	if (this != &other) {
		// Close our existing stderr fd if any
		if (stderr_fd_ >= 0) {
			close(stderr_fd_);
		}
		proc_ = std::move(other.proc_);
		worker_path_ = std::move(other.worker_path_);
		pooled_at_ = other.pooled_at_;
		stderr_fd_ = other.stderr_fd_;
		other.stderr_fd_ = -1; // Transfer ownership
	}
	return *this;
}

bool PooledWorker::IsAlive() const {
	if (!proc_) {
		return false;
	}
	// TryWait returns true if the process has exited
	// So if it returns false, the process is still running
	return !proc_->TryWait();
}

std::unique_ptr<SubProcess> PooledWorker::Release() {
	return std::move(proc_);
}

int PooledWorker::ReleaseStderrFd() {
	int fd = stderr_fd_;
	stderr_fd_ = -1;
	return fd;
}

pid_t PooledWorker::GetPid() const {
	return proc_ ? proc_->GetPid() : -1;
}

//===--------------------------------------------------------------------===//
// VgiWorkerPool
//===--------------------------------------------------------------------===//

VgiWorkerPool &VgiWorkerPool::Instance() {
	static VgiWorkerPool instance;
	return instance;
}

VgiWorkerPool::VgiWorkerPool() {
	// Start the cleanup thread
	cleanup_thread_ = std::thread(&VgiWorkerPool::CleanupThread, this);
}

VgiWorkerPool::~VgiWorkerPool() {
	// Signal shutdown
	shutdown_.store(true);
	cleanup_cv_.notify_all();

	// Wait for cleanup thread
	if (cleanup_thread_.joinable()) {
		cleanup_thread_.join();
	}

	// Clear all pools (workers will be destroyed)
	std::lock_guard<std::mutex> lock(mutex_);
	pools_.clear();
}

std::unique_ptr<PooledWorker> VgiWorkerPool::TryAcquire(const std::string &worker_path) {
	std::lock_guard<std::mutex> lock(mutex_);

	bool debug = getenv("VGI_STDERR_LOG") != nullptr;
	if (debug) {
		size_t total = TotalPoolSizeLocked();
		fprintf(stderr, "[VGI] pool.try_acquire worker_path=%s total_pool_size=%zu\n", worker_path.c_str(), total);
	}

	auto it = pools_.find(worker_path);
	if (it == pools_.end() || it->second.empty()) {
		if (debug) {
			fprintf(stderr, "[VGI] pool.try_acquire path_not_found_or_empty worker_path=%s\n", worker_path.c_str());
		}
		return nullptr;
	}

	// Try to find a live worker (check from front, oldest first)
	auto &pool = it->second;
	while (!pool.empty()) {
		auto worker = std::move(pool.front());
		pool.pop_front();

		if (worker->IsAlive()) {
			// Record hit
			stats_[worker_path].first++;
			if (debug) {
				fprintf(stderr, "[VGI] pool.try_acquire found_alive pid=%d\n", worker->GetPid());
			}
			return worker;
		}
		// Worker died while pooled, discard and try next
		if (debug) {
			fprintf(stderr, "[VGI] pool.try_acquire worker_died pid=%d\n", worker->GetPid());
		}
	}

	return nullptr;
}

void VgiWorkerPool::Release(std::unique_ptr<PooledWorker> worker, size_t max_pool_size) {
	bool debug = getenv("VGI_STDERR_LOG") != nullptr;

	if (!worker) {
		return;
	}
	if (!worker->IsAlive()) {
		if (debug) {
			fprintf(stderr, "[VGI] pool.release worker_not_alive pid=%d\n", worker->GetPid());
		}
		return; // Don't pool dead workers
	}

	// max_pool_size of 0 means pool is disabled
	if (max_pool_size == 0) {
		if (debug) {
			fprintf(stderr, "[VGI] pool.release pool_disabled pid=%d\n", worker->GetPid());
		}
		return; // Pool disabled, discard worker
	}

	std::lock_guard<std::mutex> lock(mutex_);

	// Check if pool is full
	if (TotalPoolSizeLocked() >= max_pool_size) {
		if (debug) {
			fprintf(stderr, "[VGI] pool.release pool_full pid=%d total=%zu max=%zu\n",
			        worker->GetPid(), TotalPoolSizeLocked(), max_pool_size);
		}
		// Pool is full, discard this worker
		return;
	}

	// Add to the pool for this worker path
	const std::string &path = worker->GetWorkerPath();
	pools_[path].push_back(std::move(worker));
	if (debug) {
		fprintf(stderr, "[VGI] pool.release added worker_path=%s pool_size=%zu total=%zu\n",
		        path.c_str(), pools_[path].size(), TotalPoolSizeLocked());
	}
}

size_t VgiWorkerPool::Flush() {
	std::lock_guard<std::mutex> lock(mutex_);

	size_t count = TotalPoolSizeLocked();
	pools_.clear();
	return count;
}

std::vector<VgiWorkerPool::PoolEntry> VgiWorkerPool::GetPoolEntries() const {
	std::lock_guard<std::mutex> lock(mutex_);

	std::vector<PoolEntry> entries;
	auto now = std::chrono::steady_clock::now();

	for (const auto &[path, pool] : pools_) {
		for (const auto &worker : pool) {
			auto age = std::chrono::duration_cast<std::chrono::seconds>(now - worker->GetPooledAt());
			entries.push_back({path, worker->GetPid(), age.count()});
		}
	}

	return entries;
}

void VgiWorkerPool::SetIdleTimeout(std::chrono::seconds timeout) {
	std::lock_guard<std::mutex> lock(mutex_);
	idle_timeout_ = timeout;
}

void VgiWorkerPool::CleanupThread() {
	while (!shutdown_.load()) {
		{
			std::unique_lock<std::mutex> lock(cleanup_mutex_);
			// Wait for 1 second or until shutdown
			cleanup_cv_.wait_for(lock, std::chrono::seconds(1), [this] { return shutdown_.load(); });
		}

		if (shutdown_.load()) {
			break;
		}

		// Remove stale workers
		std::lock_guard<std::mutex> lock(mutex_);
		RemoveStaleWorkersLocked();
	}
}

void VgiWorkerPool::RemoveStaleWorkersLocked() {
	auto now = std::chrono::steady_clock::now();

	for (auto &[path, pool] : pools_) {
		// Remove dead or stale workers
		auto it = pool.begin();
		while (it != pool.end()) {
			auto &worker = *it;

			// Check if dead
			if (!worker->IsAlive()) {
				it = pool.erase(it);
				continue;
			}

			// Check if stale (idle too long)
			auto age = std::chrono::duration_cast<std::chrono::seconds>(now - worker->GetPooledAt());
			if (age >= idle_timeout_) {
				it = pool.erase(it);
				continue;
			}

			++it;
		}
	}
}

size_t VgiWorkerPool::TotalPoolSizeLocked() const {
	size_t total = 0;
	for (const auto &[path, pool] : pools_) {
		total += pool.size();
	}
	return total;
}

std::vector<VgiWorkerPool::PoolStats> VgiWorkerPool::GetPoolStats() const {
	std::lock_guard<std::mutex> lock(mutex_);

	std::vector<PoolStats> result;
	for (const auto &[path, counts] : stats_) {
		result.push_back({path, counts.first, counts.second});
	}
	return result;
}

void VgiWorkerPool::RecordMiss(const std::string &worker_path) {
	std::lock_guard<std::mutex> lock(mutex_);
	stats_[worker_path].second++;
}

} // namespace vgi
} // namespace duckdb
