// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Opt-in client-side per-pass timing for the VGI data round trip, symmetric to
// the worker's `[vgi-shm] timeline`. Enabled with VGI_RPC_CLIENT_TIMING; a
// process-wide accumulator prints a breakdown at exit. Single global accumulator
// (atomics) — under threads>1 each worker thread's passes still sum into the same
// buckets, so the proportions hold; the benchmark runs threads=1 for clean
// per-query attribution.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace duckdb {
namespace vgi {

struct ClientTiming {
	std::atomic<uint64_t> convert_in_ns {0};  // DuckDB DataChunk -> Arrow (DataChunkToArrow)
	std::atomic<uint64_t> write_ns {0};       // serialize + send input (WriteInputBatch)
	std::atomic<uint64_t> read_ns {0};        // worker wait + transport + shm resolve (ReadDataBatch)
	std::atomic<uint64_t> schema_ns {0};      // per-batch output-schema derivation (scalar/aggregate)
	std::atomic<uint64_t> convert_out_ns {0}; // Arrow -> DuckDB (LoadBatch + ProduceOutput)
	std::atomic<uint64_t> batches {0};

	static ClientTiming &Instance();
	// Cached getenv check (computed once).
	static bool Enabled();
};

// Adds the elapsed wall time to `target` (relaxed) on scope exit. No-op cost is
// a steady_clock read per construction/destruction; callers guard with
// ClientTiming::Enabled() so the disabled path never constructs one.
class ScopedNs {
public:
	explicit ScopedNs(std::atomic<uint64_t> &target)
	    : target_(target), start_(std::chrono::steady_clock::now()) {
	}
	~ScopedNs() {
		auto end = std::chrono::steady_clock::now();
		target_.fetch_add(
		    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count()),
		    std::memory_order_relaxed);
	}
	ScopedNs(const ScopedNs &) = delete;
	ScopedNs &operator=(const ScopedNs &) = delete;

private:
	std::atomic<uint64_t> &target_;
	std::chrono::steady_clock::time_point start_;
};

} // namespace vgi
} // namespace duckdb
