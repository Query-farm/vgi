// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_cancel_dispatcher.hpp"

#include <chrono>
#include <exception>

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_aggregate_streaming_impl.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

namespace {

constexpr auto kShutdownJoinDeadline = std::chrono::seconds(2);

} // namespace

VgiCancelDispatcher::VgiCancelDispatcher(DatabaseInstance &db, VgiCancelWorkerStart start)
    : db_(db), start_(start), queue_(1024), streaming_close_queue_(1024) {
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

	bool detached = false;
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
			detached = true;
		}
	}

	std::lock_guard<std::mutex> conn_lock(conn_mutex_);
	if (!detached) {
		// Worker has exited — safe to close the bot connection.
		conn_.reset();
	} else {
		// The detached worker is still inside ProcessOne -> CancelStream ->
		// HttpPostArrowIpc, which dereferences *conn_->context (and the
		// DatabaseInstance that ClientContext pins). Freeing conn_ here would
		// pull that context out from under the running worker — a use-after-free
		// that crashes in DBConfig::GetHTTPUtil. The process is shutting down,
		// so intentionally leak the bot connection to keep its context valid for
		// the detached worker's lifetime. Bounded and benign (matches the
		// already-leaked worker thread).
		(void)conn_.release();
	}
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
		if (!EnsureWorkerStarted()) {
			return false;
		}
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

bool VgiCancelDispatcher::EnqueueStreamingClose(StreamingCloseRequest req) noexcept {
	if (shutdown_.load(std::memory_order_acquire)) {
		return false;
	}
	try {
		if (!EnsureWorkerStarted()) {
			return false;
		}
	} catch (...) {
		return false;
	}
	if (!streaming_close_queue_.try_enqueue(std::move(req))) {
		return false;
	}
	pending_count_.fetch_add(1, std::memory_order_relaxed);
	{
		std::lock_guard<std::mutex> lock(cv_mutex_);
		cv_.notify_one();
	}
	return true;
}

bool VgiCancelDispatcher::EnsureWorkerStarted() {
	if (worker_running_.load(std::memory_order_acquire)) {
		return true;
	}
	if (start_ == VgiCancelWorkerStart::kExplicitOnly) {
		// On WASM the worker is started once, at extension load (StartWorker), and
		// never here. Enqueue runs from destructors, often on a pthread: a table
		// scan's local state is destroyed on whichever thread ran its task. There,
		// pthread_create is a synchronous call proxied to the main runtime thread,
		// and when a query is interrupted that thread is spinning in
		// Executor::CancelTasks, waiting for this very task to unregister. It never
		// services the proxied call, so the two wait on each other forever: in
		// Cupola, the first cancel of a VGI scan after a page load hung the engine
		// in 6 of 10 runs at threads > 1, and in 0 of 10 with cancel disabled.
		// Creating a thread after side modules are dlopen'd is also unreliable on
		// WASM (see vgi_wasm_async_pool.hpp). Until the worker is running the
		// caller drops the request; the server-side stream then expires by TTL.
		// "Running", not "started": a created pthread that never starts would
		// otherwise accept requests nothing drains.
		return false;
	}
	// A request queued before the new thread runs is drained once it does.
	StartWorker();
	return true;
}

void VgiCancelDispatcher::StartWorker() {
	if (worker_started_.load(std::memory_order_acquire)) {
		return;
	}
	std::lock_guard<std::mutex> lock(start_mutex_);
	if (worker_started_.load(std::memory_order_relaxed)) {
		return;
	}
	worker_ = std::thread([this]() { WorkerLoop(); });
	worker_started_.store(true, std::memory_order_release);
}

duckdb::Connection &VgiCancelDispatcher::BotConnection() {
	// Kept alive for the dispatcher's lifetime so context.db is always valid
	// on the worker thread. Opened here rather than in StartWorker, which on
	// WASM runs during extension load, before the database is ready for one.
	// The reference stays valid after the lock is released: only the
	// destructor drops conn_, after joining the worker, or leaks it.
	std::lock_guard<std::mutex> lock(conn_mutex_);
	if (!conn_) {
		conn_ = std::make_unique<duckdb::Connection>(db_);
	}
	return *conn_;
}

void VgiCancelDispatcher::WorkerLoop() {
	worker_running_.store(true, std::memory_order_release);
	while (!shutdown_.load(std::memory_order_acquire)) {
		CancelRequest req;
		if (queue_.try_dequeue(req)) {
			pending_count_.fetch_sub(1, std::memory_order_relaxed);
			ProcessOne(req);
			continue;
		}
		StreamingCloseRequest sreq;
		if (streaming_close_queue_.try_dequeue(sreq)) {
			pending_count_.fetch_sub(1, std::memory_order_relaxed);
			ProcessStreamingClose(sreq);
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
		// Pass the dispatcher's long-lived bot context: the connection's own
		// context_ references the originating query's ClientContext, which is
		// typically destroyed by the time we run here (off-thread). Using it
		// would use-after-free its Logger inside CancelStream's VGI_LOG.
		req.connection->CancelStream(req.state_token, *BotConnection().context);
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

void VgiCancelDispatcher::ProcessStreamingClose(StreamingCloseRequest &req) noexcept {
	try {
		if (!req.attach_params) {
			return;
		}
		auto &bot = BotConnection();
		if (!bot.context) {
			return;
		}
		// Synthesise the minimum bind data InvokeAggregateRpc needs:
		// attach_params (worker_path / debug / pool / version / auth /
		// cookies), function_name, attach_opaque_data. Other fields are unused
		// by the streaming_close path.
		VgiAggregateBindData synth_bind;
		synth_bind.attach_params = req.attach_params;
		synth_bind.attach_opaque_data = req.attach_opaque_data;
		synth_bind.function_name = req.function_name;

		VgiStreamingSession session;
		session.function_name = std::move(req.function_name);
		session.execution_id = std::move(req.execution_id);
		session.attach_opaque_data = std::move(req.attach_opaque_data);

		VgiAggregateStreamingClose(*bot.context, synth_bind, session,
		                            /*enable_logging=*/false);
	} catch (const std::exception &e) {
		try {
			VGI_STDERR_DEBUG("[VGI] cancel_dispatcher.streaming_close.error what=%s\n", e.what());
		} catch (...) {
		}
	} catch (...) {
		try {
			VGI_STDERR_DEBUG("[VGI] cancel_dispatcher.streaming_close.error what=unknown\n");
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
	StreamingCloseRequest sreq;
	while (streaming_close_queue_.try_dequeue(sreq)) {
		pending_count_.fetch_sub(1, std::memory_order_relaxed);
		ProcessStreamingClose(sreq);
	}
}

} // namespace vgi
} // namespace duckdb
