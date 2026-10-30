// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once
#include <condition_variable>
#include <cstddef>
#include <mutex>

namespace duckdb {

// Counts the background telemetry requests that use one database, so the database's teardown can wait for
// them. A request thread must not outlive the database it borrows: if it did, the host could call exit()
// while the request is still inside curl/OpenSSL, and static destructors would tear down state the request
// is using (seen on macOS as "mutex lock failed: Invalid argument" from the curl share lock).
//
// DuckDB-free so the unit tests can exercise it directly.
class QueryFarmInFlight {
public:
	// Registers one request. Returns false once Drain has started: the database is going away, and the
	// caller must not start a request against it.
	bool TryBegin() {
		std::lock_guard<std::mutex> lk(mu_);
		if (draining_) {
			return false;
		}
		++count_;
		return true;
	}

	// Ends a request that TryBegin registered.
	void End() {
		std::lock_guard<std::mutex> lk(mu_);
		--count_;
		if (count_ == 0) {
			cv_.notify_all();
		}
	}

	// Refuses new requests, then blocks until every registered request has ended.
	void Drain() {
		std::unique_lock<std::mutex> lk(mu_);
		draining_ = true;
		cv_.wait(lk, [this] { return count_ == 0; });
	}

	std::size_t InFlight() const {
		std::lock_guard<std::mutex> lk(mu_);
		return count_;
	}

private:
	mutable std::mutex mu_;
	std::condition_variable cv_;
	std::size_t count_ = 0;
	bool draining_ = false;
};

} // namespace duckdb
