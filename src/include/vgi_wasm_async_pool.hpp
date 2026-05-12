#pragma once

// WASM-only: a pre-spawned, bounded thread pool for std::async-style work.
//
// Why: in MAIN_MODULE=1 + pthreads Emscripten builds with side modules loaded
// dynamically at runtime, calling pthread_create AFTER side modules are
// dlopen'd is unreliable (upstream emscripten issues #19425, #19199, #13303).
// The function tables get out of sync between main and pthread contexts,
// producing "table index is out of bounds" crashes or silent hangs.
//
// The workaround: spawn worker pthreads ONCE, at extension load time (when
// only the main side module is loaded and pthread_create demonstrably works),
// and keep them alive in a poll loop for the lifetime of the process.
// Subsequent async work is dispatched via a queue, not via new pthread_create.
//
// Also serves a second purpose: bounded concurrency. Per-connection HTTP
// servers don't need to handle unbounded parallel requests — capping the
// pool size caps the in-flight RPC count.

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

	// Idempotent: first call spawns the workers; subsequent calls no-op.
	// Must be called from a context where pthread_create works reliably —
	// in our build that's extension load time.
	void EnsureStarted(int num_workers);

	// Submit a callable that returns R. Returns a std::future<R> that
	// completes when a worker picks up and runs the task.
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
