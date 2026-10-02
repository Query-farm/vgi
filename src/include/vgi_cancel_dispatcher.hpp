// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"

namespace duckdb {

class DatabaseInstance;

namespace vgi {

class IFunctionConnection;
class CatalogAuth;
class SessionCookieJar;
struct VgiAttachParameters;

// One cancellation request draining through the dispatcher. Built
// inside a noexcept destructor and handed off to the worker thread;
// the dispatcher then calls connection->CancelStream() off-thread.
struct CancelRequest {
	// Transferred ownership: the destructor hands the connection to
	// the dispatcher. Kept alive until the cancel completes, then
	// released (not returned to the pool — a cancelled stream is
	// unsafe to re-use).
	std::unique_ptr<IFunctionConnection> connection;

	// HTTP-only: the most recent state-token seen on the stream. Used
	// to address the right server-side session. Empty on subprocess.
	std::vector<uint8_t> state_token;
};

// One streaming-close request. Used by the streaming-window operator
// state's destructor to surrender a worker-side aggregate_streaming
// session without requiring a ClientContext on the destruction thread.
// The dispatcher synthesises a minimal VgiAggregateBindData from these
// fields and dispatches aggregate_streaming_close on its bot Connection.
struct StreamingCloseRequest {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::string function_name;
	std::vector<uint8_t> execution_id;
	std::vector<uint8_t> attach_opaque_data;
};

// When the dispatcher's worker thread may be created.
enum class VgiCancelWorkerStart {
	// On the first Enqueue, from whatever thread calls it.
	kOnFirstUse,
	// Only by an explicit StartWorker(). Enqueue drops requests until the
	// worker is running. Required on WASM; see EnsureWorkerStarted.
	kExplicitOnly,
};

#ifdef __EMSCRIPTEN__
constexpr VgiCancelWorkerStart kDefaultCancelWorkerStart = VgiCancelWorkerStart::kExplicitOnly;
#else
constexpr VgiCancelWorkerStart kDefaultCancelWorkerStart = VgiCancelWorkerStart::kOnFirstUse;
#endif

// Process-wide (per-DatabaseInstance) background thread that drains
// CancelRequests from destructors. See plan file for rationale: keeps
// std::bad_alloc and TLS/HTTPS exceptions off the destructor stack,
// and owns a bot duckdb::Connection so HTTPUtil::Get(*context.db)
// remains valid for the dispatcher's lifetime.
class VgiCancelDispatcher {
public:
	explicit VgiCancelDispatcher(DatabaseInstance &db,
	                             VgiCancelWorkerStart start = kDefaultCancelWorkerStart);
	~VgiCancelDispatcher();

	VgiCancelDispatcher(const VgiCancelDispatcher &) = delete;
	VgiCancelDispatcher &operator=(const VgiCancelDispatcher &) = delete;

	// Enqueue a cancel request. Safe to call from a destructor — does
	// not allocate in steady state and never throws. Returns false on
	// saturation or after shutdown; caller should not retry.
	bool Enqueue(CancelRequest req) noexcept;

	// Enqueue a streaming-close request. Same noexcept / saturation
	// contract as Enqueue. Used by the streaming-window operator state's
	// destructor to surrender a worker-side session without needing a
	// ClientContext on the destruction thread.
	bool EnqueueStreamingClose(StreamingCloseRequest req) noexcept;

	// Start the worker thread now rather than on first use. Idempotent. Throws
	// std::system_error if the thread cannot be created. With kExplicitOnly
	// (WASM) this is the only way the worker starts, and it must be called from
	// the main runtime thread at extension load (see EnsureWorkerStarted).
	void StartWorker();

	// Test-only: synchronously drain pending work on the caller's
	// thread. Does not interact with the worker thread.
	void DrainForTesting();

	// Test-only: accessor used to confirm destructors enqueue. Combined
	// across both queues; tests inspecting one queue type alone are not
	// supported.
	size_t PendingCountForTesting() const noexcept {
		return pending_count_.load(std::memory_order_relaxed);
	}

	// Test-only: whether the worker thread has started running WorkerLoop.
	bool WorkerRunningForTesting() const noexcept {
		return worker_running_.load(std::memory_order_acquire);
	}

private:
	bool EnsureWorkerStarted();
	duckdb::Connection &BotConnection();
	void WorkerLoop();
	void ProcessOne(CancelRequest &req) noexcept;
	void ProcessStreamingClose(StreamingCloseRequest &req) noexcept;

	DatabaseInstance &db_;
	const VgiCancelWorkerStart start_;

	// Bot connection. Opened lazily by the first request processed, on the
	// thread processing it, so a process that never cancels pays zero
	// startup cost. Guarded by conn_mutex_: the worker opens it while
	// DrainForTesting or the destructor's detach path may touch it.
	std::mutex conn_mutex_;
	std::unique_ptr<duckdb::Connection> conn_;

	duckdb_moodycamel::ConcurrentQueue<CancelRequest> queue_;
	duckdb_moodycamel::ConcurrentQueue<StreamingCloseRequest> streaming_close_queue_;
	std::atomic<size_t> pending_count_{0};

	std::thread worker_;
	std::atomic<bool> worker_started_{false};
	// Set by the worker thread itself once it runs. A started thread is not
	// necessarily running: on WASM a pthread can be created and never start.
	std::atomic<bool> worker_running_{false};
	std::atomic<bool> shutdown_{false};

	std::mutex start_mutex_;
	std::mutex cv_mutex_;
	std::condition_variable cv_;
};

// Look up the VGI cancel dispatcher for a DatabaseInstance. Returns
// nullptr if the VGI storage extension isn't registered on this
// instance (should not happen in practice; defensive).
VgiCancelDispatcher *FindVgiCancelDispatcher(DatabaseInstance &db);

} // namespace vgi
} // namespace duckdb
