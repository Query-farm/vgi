#include "vgi_worker_pool.hpp"

#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

//===--------------------------------------------------------------------===//
// PooledWorker
//===--------------------------------------------------------------------===//

PooledWorker::PooledWorker(std::unique_ptr<SubProcess> proc, const std::string &worker_path,
                            std::unique_ptr<StderrDrainer> drainer)
    : proc_(std::move(proc)), worker_path_(worker_path), pooled_at_(std::chrono::steady_clock::now()),
      drainer_(std::move(drainer)) {
}

PooledWorker::~PooledWorker() {
	// drainer_ destructor stops the reader thread and closes the stderr fd.
	// SubProcess destructor handles process cleanup.
}

PooledWorker::PooledWorker(PooledWorker &&other) noexcept
    : proc_(std::move(other.proc_)), worker_path_(std::move(other.worker_path_)), pooled_at_(other.pooled_at_),
      drainer_(std::move(other.drainer_)) {
}

PooledWorker &PooledWorker::operator=(PooledWorker &&other) noexcept {
	if (this != &other) {
		proc_ = std::move(other.proc_);
		worker_path_ = std::move(other.worker_path_);
		pooled_at_ = other.pooled_at_;
		drainer_ = std::move(other.drainer_);
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

std::unique_ptr<StderrDrainer> PooledWorker::ReleaseDrainer() {
	return std::move(drainer_);
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

void VgiWorkerPool::SetDefaultSettings(const PoolSettings &settings) {
	std::lock_guard<std::mutex> lock(mutex_);
	default_settings_ = settings;
}

VgiWorkerPool::VgiWorkerPool() {
#ifndef __EMSCRIPTEN__
	// Start the cleanup thread (not available in WASM single-threaded mode)
	cleanup_thread_ = std::thread(&VgiWorkerPool::CleanupThread, this);
#endif
}

VgiWorkerPool::~VgiWorkerPool() {
	// Signal shutdown
	shutdown_.store(true);
	cleanup_cv_.notify_all();

#ifndef __EMSCRIPTEN__
	// Wait for cleanup thread
	if (cleanup_thread_.joinable()) {
		cleanup_thread_.join();
	}
#endif

	// Clear all pools (workers will be destroyed)
	std::lock_guard<std::mutex> lock(mutex_);
	pools_.clear();
}

std::unique_ptr<PooledWorker> VgiWorkerPool::TryAcquire(const std::string &worker_path) {
	std::lock_guard<std::mutex> lock(mutex_);

	VGI_STDERR_DEBUG("[VGI] pool.try_acquire worker_path=%s total_pool_size=%zu\n",
	                 worker_path.c_str(), TotalPoolSizeLocked());

	auto it = pools_.find(worker_path);
	if (it == pools_.end() || it->second.empty()) {
		VGI_STDERR_DEBUG("[VGI] pool.try_acquire path_not_found_or_empty worker_path=%s\n", worker_path.c_str());
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
			VGI_STDERR_DEBUG("[VGI] pool.try_acquire found_alive pid=%d\n", worker->GetPid());
			return worker;
		}
		// Worker died while pooled, discard and try next
		VGI_STDERR_DEBUG("[VGI] pool.try_acquire worker_died pid=%d\n", worker->GetPid());
	}

	return nullptr;
}

void VgiWorkerPool::Release(std::unique_ptr<PooledWorker> worker) {
	if (!worker) {
		return;
	}
	if (!worker->IsAlive()) {
		VGI_STDERR_DEBUG("[VGI] pool.release worker_not_alive pid=%d\n", worker->GetPid());
		return; // Don't pool dead workers
	}

	std::lock_guard<std::mutex> lock(mutex_);

	const std::string &path = worker->GetWorkerPath();

	// Resolve settings: per-path config if available (from ATTACH), otherwise default settings
	auto config_it = path_configs_.find(path);
	const auto &settings = (config_it != path_configs_.end()) ? config_it->second : default_settings_;

	if (settings.max_pool_size == 0) {
		VGI_STDERR_DEBUG("[VGI] pool.release pool_disabled pid=%d path=%s\n",
		                 worker->GetPid(), path.c_str());
		return;
	}
	if (pools_[path].size() >= settings.max_pool_size) {
		VGI_STDERR_DEBUG("[VGI] pool.release path_pool_full pid=%d path=%s size=%zu max=%zu\n",
		                 worker->GetPid(), path.c_str(), pools_[path].size(), settings.max_pool_size);
		return;
	}

	worker->SetIdleTimeout(std::chrono::seconds(settings.idle_timeout_seconds));
	pools_[path].push_back(std::move(worker));
	VGI_STDERR_DEBUG("[VGI] pool.release added worker_path=%s pool_size=%zu total=%zu\n",
	                 path.c_str(), pools_[path].size(), TotalPoolSizeLocked());
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

void VgiWorkerPool::ConfigurePath(const std::string &path, const PoolSettings &settings) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = path_configs_.find(path);
	if (it != path_configs_.end() && (it->second.max_pool_size != settings.max_pool_size ||
	                                   it->second.idle_timeout_seconds != settings.idle_timeout_seconds)) {
		VGI_STDERR_DEBUG("[VGI] pool.configure_path overwrite path=%s old_max=%zu new_max=%zu old_timeout=%zu new_timeout=%zu\n",
		                 path.c_str(), it->second.max_pool_size, settings.max_pool_size,
		                 it->second.idle_timeout_seconds, settings.idle_timeout_seconds);
	}
	path_configs_[path] = settings;
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

			// Check if stale (idle too long) — uses per-worker timeout
			auto age = std::chrono::duration_cast<std::chrono::seconds>(now - worker->GetPooledAt());
			if (age >= worker->GetIdleTimeout()) {
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
