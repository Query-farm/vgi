// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// VgiCancelDispatcher's worker-start policy and bot connection.
//
// kExplicitOnly is what WASM builds use: there the worker may only be created
// by StartWorker() at extension load, never from Enqueue, which runs in the
// destructor of an interrupted scan's local state, on whichever thread ran the
// task. Creating a pthread there deadlocked the browser engine. The policy is a
// constructor argument so it can be tested natively.

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_worker_pool.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

struct CancelLog {
	std::atomic<int> cancels {0};
	std::atomic<bool> saw_live_context {false};
};

// Only CancelStream matters to the dispatcher; everything else is unreachable.
class FakeConnection : public IFunctionConnection {
public:
	explicit FakeConnection(std::shared_ptr<CancelLog> log) : log_(std::move(log)) {
	}

	void CancelStream(const std::vector<uint8_t> &, ClientContext &live_context) override {
		log_->saw_live_context.store(live_context.db != nullptr);
		log_->cancels.fetch_add(1);
	}

	void SetTickFilterState(shared_ptr<TickFilterState>) override {
	}
	void ResetForNextSplit() override {
	}
	BindResult PerformBindRpc() override {
		return {};
	}
	void EnsureWorkerSpawned() override {
	}
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &) override {
	}
	void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &) override {
	}
	InitResult PerformInit(const BindResult &, const std::vector<int32_t> &, std::shared_ptr<arrow::Buffer>,
	                       std::vector<std::shared_ptr<arrow::Buffer>>, const std::string &,
	                       const std::optional<OrderByHint> &, const std::optional<TableSampleHint> &,
	                       const std::vector<uint8_t> &, const std::optional<std::vector<uint8_t>> &,
	                       const std::vector<std::string> &) override {
		return {};
	}
	void PerformFinalizeInit(const BindResult &) override {
	}
	void OpenInputWriter() override {
	}
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &) override {
	}
	void CloseInputWriter() override {
	}
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch() override {
		return nullptr;
	}
	std::vector<uint8_t> RpcTableBufferingProcess(const std::string &, const std::vector<uint8_t> &,
	                                              const std::shared_ptr<arrow::RecordBatch> &,
	                                              std::optional<int64_t>) override {
		return {};
	}
	std::vector<std::vector<uint8_t>> RpcTableBufferingCombine(const std::string &, const std::vector<uint8_t> &,
	                                                           const std::vector<std::vector<uint8_t>> &) override {
		return {};
	}
	void RpcTableBufferingDestructor(const std::string &, const std::vector<uint8_t> &) override {
	}
	std::vector<uint8_t> GetLastStateToken() const override {
		return {};
	}
	bool IsTableInOut() const override {
		return false;
	}
	bool IsFinished() const override {
		return false;
	}
	void MarkDataFinished() override {
	}
	std::string GetExecutionIdHex() const override {
		return "";
	}
	std::string GetAttachOpaqueDataDigest() const override {
		return "";
	}
	std::string GetTransactionOpaqueDataDigest() const override {
		return "";
	}
	std::string GetConnIdHex() const override {
		return "";
	}
	int Wait() override {
		return 0;
	}
	std::unique_ptr<PooledWorker> ReleaseForPooling() override {
		return nullptr;
	}

private:
	std::shared_ptr<CancelLog> log_;
};

CancelRequest MakeRequest(const std::shared_ptr<CancelLog> &log) {
	CancelRequest req;
	req.connection = std::make_unique<FakeConnection>(log);
	return req;
}

template <class Predicate>
bool WaitFor(Predicate predicate, std::chrono::milliseconds limit = std::chrono::seconds(5)) {
	auto deadline = std::chrono::steady_clock::now() + limit;
	while (std::chrono::steady_clock::now() < deadline) {
		if (predicate()) {
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	return predicate();
}

} // namespace

TEST_CASE("explicit-only dispatcher drops requests until its worker runs", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kExplicitOnly);
	auto log = std::make_shared<CancelLog>();

	// The caller's thread must not create the worker: Enqueue refuses instead.
	REQUIRE_FALSE(dispatcher.Enqueue(MakeRequest(log)));
	REQUIRE_FALSE(dispatcher.WorkerRunningForTesting());
	REQUIRE(dispatcher.PendingCountForTesting() == 0);

	StreamingCloseRequest close;
	REQUIRE_FALSE(dispatcher.EnqueueStreamingClose(std::move(close)));

	dispatcher.StartWorker();
	REQUIRE(WaitFor([&] { return dispatcher.WorkerRunningForTesting(); }));
	REQUIRE(dispatcher.Enqueue(MakeRequest(log)));
	REQUIRE(WaitFor([&] { return log->cancels.load() == 1; }));
	// The cancel ran against the dispatcher's own bot connection.
	REQUIRE(log->saw_live_context.load());
}

TEST_CASE("on-first-use dispatcher starts its worker from Enqueue", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kOnFirstUse);
	auto log = std::make_shared<CancelLog>();

	// Accepted even before the new thread runs; it drains the queue once it does.
	REQUIRE(dispatcher.Enqueue(MakeRequest(log)));
	REQUIRE(WaitFor([&] { return log->cancels.load() == 1; }));
	REQUIRE(dispatcher.WorkerRunningForTesting());
}

TEST_CASE("StartWorker is idempotent", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kExplicitOnly);
	auto log = std::make_shared<CancelLog>();
	dispatcher.StartWorker();
	dispatcher.StartWorker();
	REQUIRE(WaitFor([&] { return dispatcher.WorkerRunningForTesting(); }));
	REQUIRE(dispatcher.Enqueue(MakeRequest(log)));
	REQUIRE(WaitFor([&] { return log->cancels.load() == 1; }));
}

TEST_CASE("concurrent enqueues are each cancelled once", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kExplicitOnly);
	dispatcher.StartWorker();
	REQUIRE(WaitFor([&] { return dispatcher.WorkerRunningForTesting(); }));
	auto log = std::make_shared<CancelLog>();

	constexpr int kThreads = 8, kEach = 50;
	std::atomic<int> accepted {0};
	std::vector<std::thread> threads;
	for (int t = 0; t < kThreads; t++) {
		threads.emplace_back([&] {
			for (int i = 0; i < kEach; i++) {
				if (dispatcher.Enqueue(MakeRequest(log))) {
					accepted.fetch_add(1);
				}
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	REQUIRE(accepted.load() == kThreads * kEach);
	REQUIRE(WaitFor([&] { return log->cancels.load() == kThreads * kEach; }));
}

TEST_CASE("a streaming close without attach params is dropped harmlessly", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kExplicitOnly);
	dispatcher.StartWorker();
	REQUIRE(WaitFor([&] { return dispatcher.WorkerRunningForTesting(); }));
	StreamingCloseRequest close;
	REQUIRE(dispatcher.EnqueueStreamingClose(std::move(close)));
	REQUIRE(WaitFor([&] { return dispatcher.PendingCountForTesting() == 0; }));
}

TEST_CASE("destroying the dispatcher with requests queued neither hangs nor crashes", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	auto log = std::make_shared<CancelLog>();
	auto started = std::chrono::steady_clock::now();
	{
		VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kExplicitOnly);
		dispatcher.StartWorker();
		REQUIRE(WaitFor([&] { return dispatcher.WorkerRunningForTesting(); }));
		for (int i = 0; i < 100; i++) {
			(void)dispatcher.Enqueue(MakeRequest(log));
		}
	}
	// Well inside the destructor's 2s join deadline: the worker exits promptly.
	REQUIRE(std::chrono::steady_clock::now() - started < std::chrono::seconds(2));
}

TEST_CASE("an unstarted dispatcher is destroyed without a worker or bot connection", "[cancel-dispatcher]") {
	DuckDB db(nullptr);
	{ VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kExplicitOnly); }
	{ VgiCancelDispatcher dispatcher(*db.instance, VgiCancelWorkerStart::kOnFirstUse); }
	SUCCEED();
}
