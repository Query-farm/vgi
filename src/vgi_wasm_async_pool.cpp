// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_wasm_async_pool.hpp"

#ifdef __EMSCRIPTEN__

#include <cstdio>

namespace duckdb {
namespace vgi {

VgiWasmAsyncPool &VgiWasmAsyncPool::Instance() {
	static VgiWasmAsyncPool instance;
	return instance;
}

void VgiWasmAsyncPool::EnsureStarted(int num_workers) {
	std::lock_guard<std::mutex> lk(mu_);
	if (started_) {
		return;
	}
	if (num_workers < 1) {
		num_workers = 1;
	}
	workers_.reserve(static_cast<size_t>(num_workers));
	for (int i = 0; i < num_workers; ++i) {
		pthread_t tid = 0;
		int r = pthread_create(&tid, nullptr, &VgiWasmAsyncPool::WorkerEntry, this);
		if (r != 0) {
			continue;
		}
		workers_.push_back(tid);
	}
	started_ = true;
}

void *VgiWasmAsyncPool::WorkerEntry(void *self) {
	static_cast<VgiWasmAsyncPool *>(self)->WorkerLoop();
	return nullptr;
}

void VgiWasmAsyncPool::WorkerLoop() {
	while (true) {
		std::function<void()> task;
		{
			std::unique_lock<std::mutex> lk(mu_);
			cv_.wait(lk, [this] { return shutdown_ || !queue_.empty(); });
			if (shutdown_ && queue_.empty()) {
				return;
			}
			task = std::move(queue_.front());
			queue_.pop();
		}
		try {
			task();
		} catch (...) {
			// std::packaged_task captures exceptions into the future;
			// nothing escapes here under normal use, but swallow defensively.
		}
	}
}

} // namespace vgi
} // namespace duckdb

#endif // __EMSCRIPTEN__
