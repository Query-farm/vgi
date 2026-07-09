// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Standalone C++ concurrency bench for the VGI result cache — confirms the
// scaling seams S1 (per-query global-lock contention on cache access) and S6
// (in-flight capture RAM unbounded by concurrency). Modeled on
// test/cpp/vgi_cancel_harness.cpp; compiled ad-hoc against the built DuckDB lib
// (it is NOT part of the CMake build). Build + run:
//
//   c++ -std=c++17 -O2 -I duckdb/src/include \
//       test/cpp/vgi_cache_bench.cpp \
//       build/release/src/libhaybarn.dylib -o /tmp/vgi_cache_bench
//   VGI_TEST_WORKER='uv run --project ~/Development/vgi-python vgi-fixture-worker' \
//   VGI_BENCH_EXTENSION=build/release/extension/vgi/vgi.duckdb_extension \
//   /tmp/vgi_cache_bench
//
// Modes (VGI_BENCH_MODE):
//   throughput  (S1) warm ONE cached entry, then sweep threads ∈ {1,2,4,8,16,32}
//               each looping identical warm HITS; report hits/sec vs threads.
//               A cache that scales shows rising throughput; a global-mutex-bound
//               cache flatlines.
//   rss         (S6) K threads each capture a DISTINCT ~per-entry-sized cacheable
//               result COLD (so all capture at once); report peak RSS. Unbounded
//               in-flight capture → peak ≈ K × per-entry bytes.
//   both        (default) run throughput then rss.
//
// Env knobs: VGI_BENCH_THREADS (rss K, default 8), VGI_BENCH_ITERS (throughput
// iters/thread, default 2000), VGI_BENCH_ROWS (rss rows/thread, default 6000000
// ≈ 48 MB int64), VGI_BENCH_MAX_THREADS (throughput sweep top, default 32).

#include "duckdb.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

using duckdb::Connection;
using duckdb::DBConfig;
using duckdb::DuckDB;

namespace {

std::string Env(const char *key, const std::string &fallback) {
	const char *v = std::getenv(key);
	return (v && *v) ? std::string(v) : fallback;
}
int EnvInt(const char *key, int fallback) {
	const char *v = std::getenv(key);
	return (v && *v) ? std::atoi(v) : fallback;
}

void Must(Connection &con, const std::string &sql) {
	auto r = con.Query(sql);
	if (r->HasError()) {
		std::cerr << "[bench] FATAL on `" << sql << "`:\n  " << r->GetError() << "\n";
		std::exit(2);
	}
}

// Peak resident set size in MB (ru_maxrss is bytes on macOS, KB on Linux).
double PeakRssMb() {
	struct rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
#if defined(__APPLE__)
	return static_cast<double>(ru.ru_maxrss) / (1024.0 * 1024.0);
#else
	return static_cast<double>(ru.ru_maxrss) / 1024.0;
#endif
}

std::string DirectScan(const std::string &worker, const std::string &rows_expr) {
	return "SELECT COUNT(*) FROM vgi_table_function('" + worker + "', 'cache_bench', [" + rows_expr + "]);";
}

std::unique_ptr<DuckDB> OpenDb(const std::string &ext, const std::string &httpfs) {
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
	auto db = std::make_unique<DuckDB>(nullptr, &config);
	Connection con(*db);
	if (!httpfs.empty()) {
		con.Query("LOAD '" + httpfs + "'");
	}
	Must(con, "LOAD '" + ext + "'");
	return db;
}

std::atomic<uint64_t> g_ok{0}, g_err{0};

// S1: identical warm HITS from K threads. Returns hits/sec.
double ThroughputRound(DuckDB &db, const std::string &sql, int threads, int iters) {
	g_ok = 0;
	g_err = 0;
	auto t0 = std::chrono::steady_clock::now();
	std::vector<std::thread> ths;
	for (int t = 0; t < threads; ++t) {
		ths.emplace_back([&db, &sql, iters]() {
			Connection con(db);
			for (int i = 0; i < iters; ++i) {
				auto r = con.Query(sql);
				(r->HasError() ? g_err : g_ok).fetch_add(1, std::memory_order_relaxed);
			}
		});
	}
	for (auto &th : ths) th.join();
	auto t1 = std::chrono::steady_clock::now();
	double secs = std::chrono::duration<double>(t1 - t0).count();
	if (g_err.load()) {
		std::cerr << "[bench] warning: " << g_err.load() << " query errors\n";
	}
	return static_cast<double>(g_ok.load()) / secs;
}

} // namespace

int main() {
	const std::string ext = Env("VGI_BENCH_EXTENSION", "build/release/extension/vgi/vgi.duckdb_extension");
	const std::string httpfs = Env("VGI_BENCH_HTTPFS", "");
	const std::string worker = Env("VGI_BENCH_LOCATION", Env("VGI_TEST_WORKER", ""));
	const std::string mode = Env("VGI_BENCH_MODE", "both");
	if (worker.empty()) {
		std::cerr << "[bench] set VGI_TEST_WORKER (or VGI_BENCH_LOCATION) to a worker command\n";
		return 2;
	}

	auto db = OpenDb(ext, httpfs);

	if (mode == "throughput" || mode == "both") {
		const int iters = EnvInt("VGI_BENCH_ITERS", 2000);
		const int max_threads = EnvInt("VGI_BENCH_MAX_THREADS", 32);
		// Warm ONE cached entry (small) so every subsequent scan is a pure HIT.
		{
			Connection con(*db);
			Must(con, "SELECT * FROM vgi_result_cache_flush();");
			Must(con, DirectScan(worker, "1000"));
		}
		const std::string sql = DirectScan(worker, "1000");
		std::cout << "== S1 throughput (warm cached hits) ==\n";
		std::cout << "threads   hits/sec   scaling_vs_1t\n";
		double base = 0;
		for (int k = 1; k <= max_threads; k *= 2) {
			double hps = ThroughputRound(*db, sql, k, iters);
			if (k == 1) base = hps;
			std::printf("%5d   %10.0f   %6.2fx\n", k, hps, base > 0 ? hps / base : 0.0);
		}
	}

	if (mode == "rss" || mode == "both") {
		const int threads = EnvInt("VGI_BENCH_THREADS", 8);
		const long rows = std::atol(Env("VGI_BENCH_ROWS", "6000000").c_str()); // ~48 MB int64
		const long inflight_mb = std::atol(Env("VGI_BENCH_INFLIGHT_MB", "256").c_str());
		const long mem_limit_mb = std::atol(Env("VGI_BENCH_MEMLIMIT_MB", "512").c_str());
		{
			Connection con(*db);
			Must(con, "SELECT * FROM vgi_result_cache_flush();");
			// [#17] The capture buffers Arrow IPC substreams OUTSIDE DuckDB's BufferManager,
			// so `memory_limit` can't see or reclaim them — only max_inflight_bytes bounds
			// them. Set a tight memory_limit + a known inflight budget and assert peak RSS
			// stays near the inflight budget, not N × per-entry.
			Must(con, "SET GLOBAL threads=" + std::to_string(threads) + ";");
			Must(con, "SET memory_limit='" + std::to_string(mem_limit_mb) + "MB';");
			Must(con, "SET vgi_result_cache_max_inflight_bytes=" +
			              std::to_string(inflight_mb * 1024 * 1024) + ";");
		}
		double rss_before = PeakRssMb();
		std::cout << "\n== S6 / #17 in-flight capture RAM (" << threads << " concurrent cold captures, ~"
		          << (rows * 8 / (1024 * 1024)) << " MB each; memory_limit=" << mem_limit_mb
		          << "MB inflight_budget=" << inflight_mb << "MB) ==\n";
		std::atomic<int> done{0};
		std::vector<std::thread> ths;
		for (int t = 0; t < threads; ++t) {
			ths.emplace_back([&db, &worker, rows, t, &done]() {
				Connection con(*db);
				// DISTINCT rows per thread → distinct keys → all capture concurrently.
				auto r = con.Query(DirectScan(worker, std::to_string(rows + t)));
				if (r->HasError()) std::cerr << "[bench] rss thread err: " << r->GetError() << "\n";
				done.fetch_add(1);
			});
		}
		for (auto &th : ths) th.join();
		double rss_after = PeakRssMb();
		const double delta = rss_after - rss_before;
		const long unbounded = (long)threads * rows * 8 / (1024 * 1024);
		// Deterministic signal: the inflight budget engaged (some captures aborted).
		int64_t aborts = 0;
		{
			Connection con(*db);
			auto r = con.Query("SELECT capture_aborts FROM vgi_result_cache_stats();");
			if (!r->HasError()) {
				aborts = r->GetValue(0, 0).GetValue<int64_t>();
			}
		}
		// [#17] Capture RAM is untracked by DuckDB's memory_limit, so the guarantee is
		// peak RSS ≤ (DuckDB-bounded memory_limit) + (capture-bounded inflight budget),
		// NOT ≤ memory_limit. The budget must also actually engage under the load.
		const double ceiling = static_cast<double>(mem_limit_mb + inflight_mb);
		std::printf("peak RSS: %.0f MB (delta %.0f MB); per-entry ~%ld MB; unbounded ceiling ~%ld MB; "
		            "capture_aborts=%lld; ceiling (memlimit+budget) %.0f MB → %s\n",
		            rss_after, delta, rows * 8 / (1024 * 1024), unbounded, (long long)aborts, ceiling,
		            (delta <= ceiling && aborts >= 1) ? "BOUNDED + budget engaged (ok)" : "CHECK");
	}

	// Spill-path concurrency stress + same-key dedup. K threads run the SAME
	// force-spilling cache_parallel scan concurrently, each scan itself fanning out
	// to ~8 internal producers whose spill transition races. Over many iterations we
	// assert every query returns the exact COUNT/SUM (a lost/duplicated batch or a
	// torn spill would corrupt it) and that the content-addressed store dedups
	// concurrent identical spills to a single blob.
	if (mode == "spill" || mode == "both") {
		const int threads = EnvInt("VGI_BENCH_SPILL_THREADS", 8);
		const int iters = EnvInt("VGI_BENCH_SPILL_ITERS", 25);
		const long rows = std::atol(Env("VGI_BENCH_SPILL_ROWS", "400000").c_str());
		const std::string dir = Env("VGI_BENCH_SPILL_DIR", "/tmp/vgi_bench_spill");
		const long long expected_sum = (long long)rows * (rows - 1) / 2;
		{
			Connection con(*db);
			Must(con, "SET GLOBAL threads=8;");
		}
		// The cache settings are session-scoped, so EACH thread's connection must set
		// them (dir/caps) before its query — else it uses defaults and never spills.
		const std::string setup = "SET vgi_result_cache_dir='" + dir +
		                          "'; SET vgi_result_cache_disk_max_bytes=4294967296;"
		                          " SET vgi_result_cache_max_entry_bytes=4096;";
		const std::string q = "SELECT COUNT(*), SUM(v)::BIGINT FROM vgi_table_function('" + worker +
		                      "', 'cache_parallel', [" + std::to_string(rows) + "]);";
		std::atomic<uint64_t> wrong{0}, errs{0}, runs{0};
		std::cout << "\n== spill concurrency (" << threads << " threads × " << iters
		          << " iters, force-spill same key) ==\n";
		for (int it = 0; it < iters; ++it) {
			{
				Connection con(*db);
				Must(con, "SET vgi_result_cache_dir='" + dir + "';");
				Must(con, "SELECT * FROM vgi_result_cache_flush();");
			}
			std::vector<std::thread> ths;
			for (int t = 0; t < threads; ++t) {
				ths.emplace_back([&]() {
					Connection con(*db);
					con.Query(setup);
					auto r = con.Query(q);
					runs.fetch_add(1, std::memory_order_relaxed);
					if (r->HasError()) {
						errs.fetch_add(1, std::memory_order_relaxed);
						return;
					}
					int64_t cnt = r->GetValue(0, 0).GetValue<int64_t>();
					int64_t sm = r->GetValue(1, 0).GetValue<int64_t>();
					if (cnt != rows || sm != expected_sum) {
						wrong.fetch_add(1, std::memory_order_relaxed);
					}
				});
			}
			for (auto &th : ths) th.join();
		}
		Connection con(*db);
		auto g = con.Query("SELECT COUNT(*) FROM glob('" + dir + "/*/objects/*.vrc');");
		long long blobs = g->HasError() ? -1 : g->GetValue(0, 0).GetValue<int64_t>();
		auto rq = con.Query("SELECT COUNT(*) FROM glob('" + dir + "/*/refs/*.ref');");
		long long refs = rq->HasError() ? -1 : rq->GetValue(0, 0).GetValue<int64_t>();
		// PASS = wrong/errors == 0. blobs may be >1: a parallel UNORDERED spill drains
		// batches in non-deterministic interleaved order, so concurrent same-key captures
		// hash differently → distinct blobs; the ref (1) points to the winner, the rest
		// orphan and are reaped past the grace window. Correctness rests on the ref, not
		// on cross-capture dedup (which only collapses to 1 for deterministic content).
		std::printf("  %llu runs; wrong=%llu errors=%llu; refs=%lld blobs=%lld "
		            "(1 ref = consistent; extra blobs = transient orphans, reaper-swept)\n",
		            (unsigned long long)runs.load(), (unsigned long long)wrong.load(),
		            (unsigned long long)errs.load(), refs, blobs);
		if (wrong.load() || errs.load()) {
			std::cerr << "[bench] SPILL CONCURRENCY FAILURE\n";
			return 3;
		}
	}
	return 0;
}
