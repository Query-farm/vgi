// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// WASM-only: a pre-spawned, bounded thread pool for std::async-style work.
//
// In MAIN_MODULE=1 + pthreads Emscripten builds, pthread_create after side
// modules are dlopen'd is unreliable (emscripten #19425, #19199, #13303):
// function tables desync, giving "table index is out of bounds" crashes or
// hangs. So workers are spawned once, at extension load, and live for the
// process; later work goes through the queue. The pool size also caps the
// number of in-flight RPCs.

#ifdef __EMSCRIPTEN__

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <type_traits>
#include <utility>
#include <vector>

namespace duckdb {
namespace vgi {

class VgiWasmAsyncPool {
public:
	static VgiWasmAsyncPool &Instance();

	// Idempotent. Must first be called where pthread_create works (extension
	// load). Failed pthread_create calls are skipped, so the pool may be smaller.
	void EnsureStarted(int num_workers);

	template <typename F>
	auto Submit(F &&task) -> std::future<typename std::invoke_result<F>::type> {
		using R = typename std::invoke_result<F>::type;
		auto packaged = std::make_shared<std::packaged_task<R()>>(std::forward<F>(task));
		auto future = packaged->get_future();
		{
			std::lock_guard<std::mutex> lk(mu_);
			queue_.push([packaged] { (*packaged)(); });
		}
		cv_.notify_one();
		return future;
	}

private:
	VgiWasmAsyncPool() = default;
	VgiWasmAsyncPool(const VgiWasmAsyncPool &) = delete;
	VgiWasmAsyncPool &operator=(const VgiWasmAsyncPool &) = delete;

	static void *WorkerEntry(void *self);
	void WorkerLoop();

	std::mutex mu_;
	std::condition_variable cv_;
	std::queue<std::function<void()>> queue_;
	std::vector<pthread_t> workers_;
	bool started_ = false;
	bool shutdown_ = false;
};

} // namespace vgi
} // namespace duckdb

#endif // __EMSCRIPTEN__
