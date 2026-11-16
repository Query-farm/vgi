// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_worker_pool.hpp"

#include "vgi_logging.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace vgi {

PooledWorker::PooledWorker(std::unique_ptr<SubProcess> proc, PoolKey key,
                            std::unique_ptr<StderrDrainer> drainer)
    : proc_(std::move(proc)), key_(std::move(key)), pooled_at_(std::chrono::steady_clock::now()),
      drainer_(std::move(drainer)) {
}

PooledWorker::~PooledWorker() {
}

PooledWorker::PooledWorker(PooledWorker &&other) noexcept
    : proc_(std::move(other.proc_)), key_(std::move(other.key_)), pooled_at_(other.pooled_at_),
      drainer_(std::move(other.drainer_)) {
}

PooledWorker &PooledWorker::operator=(PooledWorker &&other) noexcept {
	if (this != &other) {
		proc_ = std::move(other.proc_);
		key_ = std::move(other.key_);
		pooled_at_ = other.pooled_at_;
		drainer_ = std::move(other.drainer_);
	}
	return *this;
}

bool PooledWorker::IsAlive() const {
	if (!proc_) {
		return false;
	}
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

VgiWorkerPool &VgiWorkerPool::Instance() {
	// Intentionally leaked: pooled workers' streams and threads must not be
	// destroyed after other statics they depend on at exit.
	static VgiWorkerPool *instance = new VgiWorkerPool();
	return *instance;
}

PoolSettings PoolSettingsFromSession(ClientContext &context) {
	PoolSettings settings;
	settings.max_pool_size = static_cast<size_t>(defaults::WORKER_POOL_MAX);
	Value max_val;
	if (context.TryGetCurrentSetting("vgi_worker_pool_max", max_val)) {
		settings.max_pool_size = static_cast<size_t>(max_val.GetValue<int64_t>());
	}
	Value idle_val;
	if (context.TryGetCurrentSetting("vgi_worker_pool_idle_limit_seconds", idle_val)) {
		settings.idle_timeout_seconds = static_cast<size_t>(idle_val.GetValue<int64_t>());
	}
	return settings;
}

VgiWorkerPool::VgiWorkerPool() {
#if VGI_SUBPROCESS_TRANSPORT
	// No cleanup thread without a subprocess transport (Emscripten): the pool
	// stays empty there.
	cleanup_thread_ = std::thread(&VgiWorkerPool::CleanupThread, this);
#endif
}

VgiWorkerPool::~VgiWorkerPool() {
	shutdown_.store(true);
	cleanup_cv_.notify_all();

#if VGI_SUBPROCESS_TRANSPORT
	if (cleanup_thread_.joinable()) {
		cleanup_thread_.join();
	}
#endif

	std::lock_guard<std::mutex> lock(mutex_);
	pools_.clear();
}

std::unique_ptr<PooledWorker> VgiWorkerPool::TryAcquire(const PoolKey &key) {
	std::lock_guard<std::mutex> lock(mutex_);

	VGI_STDERR_DEBUG("[VGI] pool.try_acquire worker_path=%s data=%s impl=%s total_pool_size=%zu\n",
	                 key.worker_path.c_str(), key.data_version_spec.c_str(),
	                 key.implementation_version.c_str(), TotalPoolSizeLocked());

	auto it = pools_.find(key);
	if (it == pools_.end() || it->second.empty()) {
		VGI_STDERR_DEBUG("[VGI] pool.try_acquire path_not_found_or_empty worker_path=%s\n",
		                 key.worker_path.c_str());
		return nullptr;
	}

	// MRU: reuse the most-recently-released worker first to keep expensive
	// per-process state (connections, clients) warm; skip dead ones.
	auto &pool = it->second;
	while (!pool.empty()) {
		auto worker = std::move(pool.back());
		pool.pop_back();

		if (worker->IsAlive()) {
			stats_[key.worker_path].first++;
			VGI_STDERR_DEBUG("[VGI] pool.try_acquire found_alive pid=%d\n", worker->GetPid());
			return worker;
		}
		VGI_STDERR_DEBUG("[VGI] pool.try_acquire worker_died pid=%d\n", worker->GetPid());
	}

	return nullptr;
}

VgiWorkerPool::ReleaseResult VgiWorkerPool::Release(std::unique_ptr<PooledWorker> worker) {
	ReleaseResult result;
	if (!worker) {
		return result;
	}
	result.pid = worker->GetPid();

	if (!worker->IsAlive()) {
		result.skip_reason = "dead";
		return result;
	}

	std::lock_guard<std::mutex> lock(mutex_);

	const auto &key = worker->GetKey();
	const std::string &path = key.worker_path;

	// Sizing is per worker_path, shared by all version buckets of that path.
	const PoolSettings *settings_ptr = &default_settings_;
	auto config_it = path_configs_.find(path);
	if (config_it != path_configs_.end()) {
		settings_ptr = &config_it->second;
	} else {
		auto discovery_it = discovery_configs_.find(path);
		if (discovery_it != discovery_configs_.end()) {
			settings_ptr = &discovery_it->second;
		}
	}
	const auto &settings = *settings_ptr;

	if (settings.max_pool_size == 0) {
		result.skip_reason = "disabled";
		result.total_pool_size = TotalPoolSizeLocked();
		return result;
	}
	auto &bucket = pools_[key];
	if (bucket.size() >= settings.max_pool_size) {
		result.skip_reason = "path_full";
		result.pool_size = bucket.size();
		result.total_pool_size = TotalPoolSizeLocked();
		return result;
	}

	worker->SetIdleTimeout(std::chrono::seconds(settings.idle_timeout_seconds));
	bucket.push_back(std::move(worker));
	result.pooled = true;
	result.pool_size = bucket.size();
	result.total_pool_size = TotalPoolSizeLocked();
	return result;
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

	for (const auto &[key, pool] : pools_) {
		for (const auto &worker : pool) {
			auto age = std::chrono::duration_cast<std::chrono::seconds>(now - worker->GetPooledAt());
			entries.push_back({key.worker_path, key.data_version_spec, key.implementation_version,
			                   worker->GetPid(), age.count()});
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

void VgiWorkerPool::ConfigureDiscoveryPath(const std::string &path, const PoolSettings &settings) {
	std::lock_guard<std::mutex> lock(mutex_);
	discovery_configs_[path] = settings;
}

void VgiWorkerPool::CleanupThread() {
	while (!shutdown_.load()) {
		{
			std::unique_lock<std::mutex> lock(cleanup_mutex_);
			cleanup_cv_.wait_for(lock, std::chrono::seconds(1), [this] { return shutdown_.load(); });
		}

		if (shutdown_.load()) {
			break;
		}

		std::lock_guard<std::mutex> lock(mutex_);
		RemoveStaleWorkersLocked();
	}
}

void VgiWorkerPool::RemoveStaleWorkersLocked() {
	auto now = std::chrono::steady_clock::now();

	for (auto &[key, pool] : pools_) {
		(void)key;
		auto it = pool.begin();
		while (it != pool.end()) {
			auto &worker = *it;

			if (!worker->IsAlive()) {
				it = pool.erase(it);
				continue;
			}

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
	for (const auto &[key, pool] : pools_) {
		(void)key;
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
