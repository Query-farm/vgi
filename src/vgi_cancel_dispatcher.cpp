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

VgiCancelDispatcher::VgiCancelDispatcher(DatabaseInstance &db, VgiCancelWorkerStart start)
    : db_(db), start_(start), queue_(1024), streaming_close_queue_(1024) {
}

VgiCancelDispatcher::~VgiCancelDispatcher() {
	shutdown_.store(true, std::memory_order_release);
	Suspend();
	cv_.notify_all();
	// Never detach a thread that accesses this dispatcher. In-flight I/O must
	// finish (or observe the bot's interrupt) before its context/queues die.
	if (worker_.joinable()) {
		worker_.join();
	}
}

void VgiCancelDispatcher::Suspend() {
	accepting_.store(false, std::memory_order_release);
	{
		std::lock_guard<std::mutex> lock(conn_mutex_);
		if (context_) {
			context_->Interrupt();
		}
	}
	std::lock_guard<std::mutex> processing_lock(processing_mutex_);
	std::lock_guard<std::mutex> conn_lock(conn_mutex_);
	context_.reset();
}

void VgiCancelDispatcher::Resume() {
	if (!shutdown_.load(std::memory_order_acquire)) {
		accepting_.store(true, std::memory_order_release);
	}
}

bool VgiCancelDispatcher::Enqueue(CancelRequest req) noexcept {
	if (shutdown_.load(std::memory_order_acquire) || !accepting_.load(std::memory_order_acquire)) {
		return false;
	}

	// EnsureWorkerStarted is the only call here that can throw.
	try {
		if (!EnsureWorkerStarted()) {
			return false;
		}
	} catch (...) {
		return false;
	}

	pending_count_.fetch_add(1, std::memory_order_relaxed);
	if (!queue_.try_enqueue(std::move(req))) {
		pending_count_.fetch_sub(1, std::memory_order_relaxed);
		return false;
	}

	{
		std::lock_guard<std::mutex> lock(cv_mutex_);
		cv_.notify_one();
	}
	return true;
}

bool VgiCancelDispatcher::EnqueueStreamingClose(StreamingCloseRequest req) noexcept {
	if (shutdown_.load(std::memory_order_acquire) || !accepting_.load(std::memory_order_acquire)) {
		return false;
	}
	try {
		if (!EnsureWorkerStarted()) {
			return false;
		}
	} catch (...) {
		return false;
	}
	pending_count_.fetch_add(1, std::memory_order_relaxed);
	if (!streaming_close_queue_.try_enqueue(std::move(req))) {
		pending_count_.fetch_sub(1, std::memory_order_relaxed);
		return false;
	}
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
		// On WASM the worker starts only at extension load (StartWorker), never
		// here. Enqueue runs from destructors on arbitrary pthreads, where
		// pthread_create is proxied to the main runtime thread — which, during a
		// cancel, spins in Executor::CancelTasks waiting for this task: deadlock.
		// Thread creation after side modules are dlopen'd is also unreliable (see
		// vgi_wasm_async_pool.hpp). Until the worker is *running* (a created
		// pthread may never start) requests are dropped; the server-side stream
		// then expires by TTL.
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

shared_ptr<ClientContext> VgiCancelDispatcher::BotContext() {
	std::lock_guard<std::mutex> lock(conn_mutex_);
	if (shutdown_.load(std::memory_order_acquire) || !accepting_.load(std::memory_order_acquire)) {
		return nullptr;
	}
	if (!context_) {
		context_ = make_shared_ptr<ClientContext>(db_.shared_from_this());
	}
	return context_;
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
	std::lock_guard<std::mutex> lock(processing_mutex_);
	try {
		if (!req.connection) {
			return;
		}
		// Use the bot context: the originating query's ClientContext is usually
		// gone by now, and CancelStream's VGI_LOG would use-after-free its Logger.
		auto context = BotContext();
		if (context) {
			req.connection->CancelStream(req.state_token, *context);
		}
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
	std::lock_guard<std::mutex> lock(processing_mutex_);
	try {
		if (!req.attach_params) {
			return;
		}
		auto context = BotContext();
		if (!context) {
			return;
		}
		// Only these bind-data fields are used by the streaming_close path.
		VgiAggregateBindData synth_bind;
		synth_bind.attach_params = req.attach_params;
		synth_bind.attach_opaque_data = req.attach_opaque_data;
		synth_bind.function_name = req.function_name;

		VgiStreamingSession session;
		session.function_name = std::move(req.function_name);
		session.execution_id = std::move(req.execution_id);
		session.attach_opaque_data = std::move(req.attach_opaque_data);

		VgiAggregateStreamingClose(*context, synth_bind, session,
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
