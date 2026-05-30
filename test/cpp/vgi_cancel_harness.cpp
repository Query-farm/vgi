// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Standalone C++ reproduction of /tmp/vgi_crash_repro.py.
//
// The Python repro drives the crash through haybarn (a DuckDB-python wrapper),
// which buries the native stack under CPython frames and makes the
// VgiCancelDispatcher use-after-free hard to attribute. This harness does the
// same thing directly against the embedded DuckDB C++ API so:
//
//   * the crashing stack is pure C++ (no _PyEval frames), and
//   * it can be run straight under ASan / UBSan / TSan (the debug
//     libduckdb_static.a is already built with the sanitizers on).
//
// What it exercises (matching the Python repro line-for-line):
//   * one shared DatabaseInstance + one ATTACH of a VGI catalog,
//   * many short concurrent scans through per-thread connections (== cursors),
//   * every scan is `... LIMIT 1`, which abandons the stream before EOF so the
//     VgiTableFunctionLocalState destructor enqueues a cancel onto the
//     process-wide VgiCancelDispatcher, racing the dispatcher's HTTP/pipe
//     cancel against the still-live query threads.
//
// Extra knob the Python repro can't easily express: VGI_HARNESS_RECREATE_DB
// tears the DatabaseInstance (and therefore the dispatcher) down and back up
// every round, so the dispatcher destructor's "detach the worker after a 2 s
// join deadline, then conn_.reset()" path is hammered while cancels are still
// in flight. That is the prime remaining shutdown-race suspect.
//
// Configuration (all via env, with defaults that match the subprocess fixture
// worker used by test/sql/vgi_cancel_crash.test):
//
//   VGI_HARNESS_EXTENSION   path to vgi.duckdb_extension to LOAD
//                           (default build/debug/extension/vgi/vgi.duckdb_extension)
//   VGI_HARNESS_LOCATION    ATTACH LOCATION — a subprocess worker command, a
//                           launch:/unix:// spec, or an http(s):// URL. Falls
//                           back to $VGI_TEST_WORKER. REQUIRED (one of the two).
//   VGI_HARNESS_ATTACH      full ATTACH statement; overrides LOCATION-based default
//   VGI_HARNESS_SQL         the per-iteration query
//                           (default: SELECT * FROM example.sequence(1000000,
//                            batch_size := 1000) LIMIT 1)
//   VGI_HARNESS_HTTPFS      path to httpfs.duckdb_extension to LOAD first
//                           (needed for http(s):// LOCATIONs; optional otherwise)
//   VGI_HARNESS_THREADS     concurrent threads per round   (default 24)
//   VGI_HARNESS_ITERS       queries per thread per round   (default 60)
//   VGI_HARNESS_ROUNDS      number of rounds               (default 5)
//   VGI_HARNESS_RECREATE_DB if set (non-empty), drop + recreate the DuckDB
//                           instance every round (stresses dispatcher teardown)

#include "duckdb.hpp"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
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

bool EnvSet(const char *key) {
	const char *v = std::getenv(key);
	return v && *v;
}

int EnvInt(const char *key, int fallback) {
	const char *v = std::getenv(key);
	if (!v || !*v) {
		return fallback;
	}
	return std::atoi(v);
}

// Run a statement, abort the harness on error (setup statements must succeed;
// per-query failures during the storm are tolerated separately below).
void MustQuery(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		std::cerr << "[harness] FATAL on `" << sql << "`:\n  " << result->GetError() << "\n";
		std::exit(2);
	}
}

struct Workload {
	std::string sql;
	int threads;
	int iters;
};

std::atomic<uint64_t> g_ok {0};
std::atomic<uint64_t> g_err {0};

// One round: spin up `threads` workers, each with its own Connection (cursor),
// each running `iters` abandoned scans. Per-query errors are counted, not
// fatal — under heavy cancel churn a transport hiccup is expected and is NOT
// the bug we're hunting (a crash is).
void RunRound(DuckDB &db, const Workload &w) {
	std::vector<std::thread> threads;
	threads.reserve(w.threads);
	for (int t = 0; t < w.threads; ++t) {
		threads.emplace_back([&db, &w]() {
			Connection con(db);
			for (int i = 0; i < w.iters; ++i) {
				auto result = con.Query(w.sql);
				if (result->HasError()) {
					g_err.fetch_add(1, std::memory_order_relaxed);
				} else {
					g_ok.fetch_add(1, std::memory_order_relaxed);
				}
			}
		});
	}
	for (auto &th : threads) {
		th.join();
	}
}

std::unique_ptr<DuckDB> OpenDb(const std::string &ext_path, const std::string &httpfs_path,
                               const std::string &attach_sql) {
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
	auto db = std::make_unique<DuckDB>(nullptr, &config);
	Connection con(*db);

	if (!httpfs_path.empty()) {
		// Best-effort: only required for http(s):// LOCATIONs.
		auto r = con.Query("LOAD '" + httpfs_path + "'");
		if (r->HasError()) {
			std::cerr << "[harness] warning: could not LOAD httpfs (" << httpfs_path
			          << "): " << r->GetError() << "\n";
		}
	}

	MustQuery(con, "LOAD '" + ext_path + "'");
	MustQuery(con, attach_sql);
	return db;
}

} // namespace

int main() {
	const std::string ext_path =
	    Env("VGI_HARNESS_EXTENSION", "build/debug/extension/vgi/vgi.duckdb_extension");
	const std::string httpfs_path = Env("VGI_HARNESS_HTTPFS", "");

	std::string location = Env("VGI_HARNESS_LOCATION", Env("VGI_TEST_WORKER", ""));
	std::string attach_sql = Env("VGI_HARNESS_ATTACH", "");
	if (attach_sql.empty()) {
		if (location.empty()) {
			std::cerr << "[harness] need VGI_HARNESS_LOCATION (or VGI_TEST_WORKER), "
			             "or an explicit VGI_HARNESS_ATTACH statement.\n";
			return 2;
		}
		attach_sql = "ATTACH 'example' AS example (TYPE vgi, LOCATION '" + location + "')";
	}

	Workload w;
	w.sql = Env("VGI_HARNESS_SQL",
	            "SELECT * FROM example.sequence(1000000, batch_size := 1000) LIMIT 1");
	w.threads = EnvInt("VGI_HARNESS_THREADS", 24);
	w.iters = EnvInt("VGI_HARNESS_ITERS", 60);
	const int rounds = EnvInt("VGI_HARNESS_ROUNDS", 5);
	const bool recreate = EnvSet("VGI_HARNESS_RECREATE_DB");

	std::cerr << "[harness] extension=" << ext_path << "\n"
	          << "[harness] attach=" << attach_sql << "\n"
	          << "[harness] sql=" << w.sql << "\n"
	          << "[harness] threads=" << w.threads << " iters=" << w.iters
	          << " rounds=" << rounds << " recreate_db=" << (recreate ? "yes" : "no") << "\n";

	if (recreate) {
		for (int r = 0; r < rounds; ++r) {
			auto db = OpenDb(ext_path, httpfs_path, attach_sql);
			RunRound(*db, w);
			// db (and the VgiCancelDispatcher) is destroyed here, while cancels
			// from the just-finished round may still be draining on the
			// dispatcher worker thread.
			std::cerr << "[harness] round " << r << " ok (db recreated)\n";
		}
	} else {
		auto db = OpenDb(ext_path, httpfs_path, attach_sql);
		for (int r = 0; r < rounds; ++r) {
			RunRound(*db, w);
			std::cerr << "[harness] round " << r << " ok\n";
		}
	}

	std::cerr << "[harness] NO CRASH (ok=" << g_ok.load() << " query_errors=" << g_err.load()
	          << ")\n";
	return 0;
}
