#include "vgi_cancel_dispatcher.hpp"

#include <chrono>
#include <exception>

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

namespace {

constexpr auto kShutdownJoinDeadline = std::chrono::seconds(2);

} // namespace

VgiCancelDispatcher::VgiCancelDispatcher(DatabaseInstance &db) : db_(db), queue_(1024) {
}

VgiCancelDispatcher::~VgiCancelDispatcher() {
	// Signal the worker (if it ever started) and drop any remaining
	// requests. We intentionally don't try to finish a long queue —
	// HTTP tokens TTL-expire, subprocess workers die with the parent.
	shutdown_.store(true, std::memory_order_release);
	{
		std::lock_guard<std::mutex> lock(cv_mutex_);
		cv_.notify_all();
	}

	if (worker_started_.load(std::memory_order_acquire) && worker_.joinable()) {
		// Detached join with a short deadline: if the worker is stuck
		// on a slow HTTP call we'd rather drop the cancel than hang
		// DatabaseInstance teardown. std::thread doesn't expose a
		// timed join, so we use a helper thread.
		std::atomic<bool> joined{false};
		std::thread joiner([&]() {
			worker_.join();
			joined.store(true, std::memory_order_release);
		});
		auto deadline = std::chrono::steady_clock::now() + kShutdownJoinDeadline;
		while (!joined.load(std::memory_order_acquire) &&
		       std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		if (joined.load(std::memory_order_acquire)) {
			joiner.join();
		} else {
			// Worker still blocked; detach both. Process is shutting
			// down so leaked thread is acceptable.
			worker_.detach();
			joiner.detach();
		}
	}

	// Close bot connection after the worker thread is joined/detached.
	conn_.reset();
}

bool VgiCancelDispatcher::Enqueue(CancelRequest req) noexcept {
	if (shutdown_.load(std::memory_order_acquire)) {
		return false;
	}

	// Spawn the worker on first use. try_enqueue on a ConcurrentQueue
	// is noexcept; EnsureWorkerStarted is the only work that could
	// plausibly throw — wrap in try/catch so Enqueue stays noexcept
	// as promised.
	try {
		EnsureWorkerStarted();
	} catch (...) {
		return false;
	}

	if (!queue_.try_enqueue(std::move(req))) {
		return false;
	}
	pending_count_.fetch_add(1, std::memory_order_relaxed);

	{
		std::lock_guard<std::mutex> lock(cv_mutex_);
		cv_.notify_one();
	}
	return true;
}

void VgiCancelDispatcher::EnsureWorkerStarted() {
	if (worker_started_.load(std::memory_order_acquire)) {
		return;
	}
	std::lock_guard<std::mutex> lock(start_mutex_);
	if (worker_started_.load(std::memory_order_relaxed)) {
		return;
	}
	// Open the bot connection. Kept alive for the dispatcher's
	// lifetime so context.db is always valid on the worker thread.
	conn_ = std::make_unique<duckdb::Connection>(db_);

	worker_ = std::thread([this]() { WorkerLoop(); });
	worker_started_.store(true, std::memory_order_release);
}

void VgiCancelDispatcher::WorkerLoop() {
	while (!shutdown_.load(std::memory_order_acquire)) {
		CancelRequest req;
		if (queue_.try_dequeue(req)) {
			pending_count_.fetch_sub(1, std::memory_order_relaxed);
			ProcessOne(req);
			continue;
		}
		std::unique_lock<std::mutex> lock(cv_mutex_);
		cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {
			return shutdown_.load(std::memory_order_acquire) ||
			       pending_count_.load(std::memory_order_relaxed) > 0;
		});
	}
}

void VgiCancelDispatcher::ProcessOne(CancelRequest &req) noexcept {
	try {
		if (!req.connection) {
			return;
		}
		req.connection->CancelStream(req.state_token);
	} catch (const std::exception &e) {
		// Best-effort; log and move on. Never propagate.
		try {
			VGI_STDERR_DEBUG("[VGI] cancel_dispatcher.error what=%s\n", e.what());
		} catch (...) {
		}
	} catch (...) {
		try {
			VGI_STDERR_DEBUG("[VGI] cancel_dispatcher.error what=unknown\n");
		} catch (...) {
		}
	}
}

void VgiCancelDispatcher::DrainForTesting() {
	CancelRequest req;
	while (queue_.try_dequeue(req)) {
		pending_count_.fetch_sub(1, std::memory_order_relaxed);
		ProcessOne(req);
	}
}

} // namespace vgi
} // namespace duckdb
