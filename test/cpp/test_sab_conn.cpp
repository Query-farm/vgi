// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The Option-A runtime e2e: drive the REAL WebWorkerFunctionConnection (the
// IFunctionConnection DuckDB's scan uses) through a full bind -> init -> producer
// scan against a REAL vgi table-function worker (the vgi crate's `count_to`,
// linked from the sabtable staticlib) over the native SAB ring — no browser.
// Proves the class orchestrates the wire correctly, natively.
#include "catch.hpp"

#include "vgi_arrow_utils.hpp"       // BuildArgumentsFromValues, ArrowArguments
#include "vgi_catalog_metadata.hpp"  // VgiSecretRequirement (complete type for the ctor default)
#include "vgi_sab_abi.hpp"
#include "vgi_webworker_function_connection.hpp"

#include "duckdb.hpp"

#include <arrow/api.h>

#include <memory>
#include <thread>
#include <vector>

using namespace duckdb;
using duckdb::vgi::ArrowArguments;
using duckdb::vgi::BindResult;
using duckdb::vgi::BuildArgumentsFromValues;
using duckdb::vgi::WebWorkerFunctionConnection;

// Rust table-function worker (sabtable staticlib): serve count_to on a slot.
extern "C" void vgi_rust_serve_table_sab_slot(int slot);

TEST_CASE("WebWorkerFunctionConnection drives count_to(5) over the ring", "[sab-conn]") {
	DuckDB db(nullptr);
	Connection con(db);
	ClientContext &context = *con.context;

	// count_to(n=5): a single const i64 argument.
	ArrowArguments args = BuildArgumentsFromValues(context, {Value::BIGINT(5)});

	WebWorkerFunctionConnection conn("worker:test", "count_to", args, {}, {}, context, "TABLE");
	conn.EnsureWorkerSpawned(); // claims slot 0 (only connection in this test)
	std::thread worker([]() { vgi_rust_serve_table_sab_slot(0); });

	// bind -> init -> producer scan through the real IFunctionConnection API.
	BindResult bind_result = conn.PerformBindRpc();
	conn.PerformInit(bind_result);

	std::vector<int64_t> got;
	while (auto batch = conn.ReadDataBatch()) {
		REQUIRE(batch->num_columns() == 1);
		auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
		for (int64_t i = 0; i < col->length(); i++) {
			got.push_back(col->Value(i));
		}
	}

	worker.join();
	CHECK(got == std::vector<int64_t>({0, 1, 2, 3, 4}));
}

TEST_CASE("WebWorkerFunctionConnection streams a multi-batch producer", "[sab-conn]") {
	DuckDB db(nullptr);
	Connection con(db);
	ClientContext &context = *con.context;

	// emit_batches(n_batches=3, rows_per_batch=4) -> 12 rows, values 0..11 across
	// 3 batches (count_to emits a single batch — this exercises the multi-batch path).
	ArrowArguments args = BuildArgumentsFromValues(context, {Value::BIGINT(3), Value::BIGINT(4)});

	WebWorkerFunctionConnection conn("worker:test", "emit_batches", args, {}, {}, context, "TABLE");
	conn.EnsureWorkerSpawned();
	std::thread worker([]() { vgi_rust_serve_table_sab_slot(0); });

	BindResult bind_result = conn.PerformBindRpc();
	conn.PerformInit(bind_result);

	int num_batches = 0;
	std::vector<int64_t> got;
	while (auto batch = conn.ReadDataBatch()) {
		num_batches++;
		auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
		for (int64_t i = 0; i < col->length(); i++) {
			got.push_back(col->Value(i));
		}
	}
	worker.join();

	CHECK(num_batches == 3);
	std::vector<int64_t> want;
	for (int64_t i = 0; i < 12; i++) {
		want.push_back(i);
	}
	CHECK(got == want);
}

TEST_CASE("WebWorkerFunctionConnection surfaces a worker produce error", "[sab-conn]") {
	DuckDB db(nullptr);
	Connection con(db);
	ClientContext &context = *con.context;

	// boom() errors during produce; the client must surface it as a thrown
	// exception (in-band error batch -> IOException), not a hang or a silent
	// empty result.
	ArrowArguments args = BuildArgumentsFromValues(context, {});

	WebWorkerFunctionConnection conn("worker:test", "boom", args, {}, {}, context, "TABLE");
	conn.EnsureWorkerSpawned();
	std::thread worker([]() { vgi_rust_serve_table_sab_slot(0); });

	bool threw = false;
	std::string msg;
	try {
		BindResult bind_result = conn.PerformBindRpc();
		conn.PerformInit(bind_result);
		while (auto batch = conn.ReadDataBatch()) {
		}
	} catch (const std::exception &e) {
		threw = true;
		msg = e.what();
	}
	worker.join();

	CHECK(threw);
	CHECK(msg.find("boom") != std::string::npos);
}

// --- ResetForNextSplit: reuse ONE connection across several splits -----------
//
// Greedy per-split claiming re-inits the SAME connection for each split a reader
// claims, so the reset between them has to leave both rings BETWEEN frames. Get
// it wrong and the next init request lands inside an unterminated tick stream,
// or the next stream header reads the previous split's leftover output — both of
// which hang or mis-frame rather than failing cleanly, and neither of which a
// browser is a pleasant place to debug.
//
// BOTH ARE TAGGED [.] — hidden from a default run, runnable with [sab-split].
// The CLIENT half is implemented (ResetForNextSplit mirrors PerformFinalizeInit,
// which is the tested precedent for re-initializing a live SAB connection). What
// is missing is on the other side of the ring, and these tests pin it precisely:
// a second PerformInit fails with "Stream header EOF", because the worker's
// connection has already ended.
//
// Traced as far as: the ring itself is NOT closed (vgi_sab_native_ring only
// EOFs on close_ring, and only w2c is closed, at the very end of serve). The
// worker exits because vgi-rpc's _serve_one gets None from read_request — it
// reads an end-of-stream where it expects the next request frame. The client
// writes that EOS when it terminates its TICK stream, which on a pipe is just
// framing the next request follows, and on the ring is being read as the end of
// the conversation.
//
// So finishing SAB splits means settling that framing question between the
// client's tick-stream teardown and the worker's request loop — not more work in
// ResetForNextSplit. Turning these on is the acceptance test for that change.

TEST_CASE("WebWorkerFunctionConnection re-inits cleanly after a fully drained split",
          "[.][sab-split]") {
	DuckDB db(nullptr);
	Connection con(db);
	ClientContext &context = *con.context;

	ArrowArguments args = BuildArgumentsFromValues(context, {Value::BIGINT(5)});
	WebWorkerFunctionConnection conn("worker:test", "count_to", args, {}, {}, context, "TABLE");
	conn.EnsureWorkerSpawned();
	std::thread worker([]() { vgi_rust_serve_table_sab_slot(0); });

	BindResult bind_result = conn.PerformBindRpc();

	auto drain = [&conn]() {
		std::vector<int64_t> out;
		while (auto batch = conn.ReadDataBatch()) {
			auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
			for (int64_t i = 0; i < col->length(); i++) {
				out.push_back(col->Value(i));
			}
		}
		return out;
	};

	conn.PerformInit(bind_result);
	auto first = drain();

	conn.ResetForNextSplit();

	conn.PerformInit(bind_result);
	auto second = drain();

	worker.join();

	const std::vector<int64_t> want {0, 1, 2, 3, 4};
	CHECK(first == want);
	CHECK(second == want);
}

TEST_CASE("WebWorkerFunctionConnection re-inits after a split whose output was never read",
          "[.][sab-split]") {
	DuckDB db(nullptr);
	Connection con(db);
	ClientContext &context = *con.context;

	ArrowArguments args = BuildArgumentsFromValues(context, {Value::BIGINT(5)});
	WebWorkerFunctionConnection conn("worker:test", "count_to", args, {}, {}, context, "TABLE");
	conn.EnsureWorkerSpawned();
	std::thread worker([]() { vgi_rust_serve_table_sab_slot(0); });

	BindResult bind_result = conn.PerformBindRpc();

	// Init and abandon WITHOUT reading a batch — the shape the reset exists for:
	// a reader that claimed a split and wanted nothing from it (a LIMIT satisfied,
	// a filter that pruned everything). The worker's output sits unconsumed on the
	// w2c ring and the data reader was never opened, so nothing has drained it.
	conn.PerformInit(bind_result);
	conn.ResetForNextSplit();

	conn.PerformInit(bind_result);
	std::vector<int64_t> got;
	while (auto batch = conn.ReadDataBatch()) {
		auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
		for (int64_t i = 0; i < col->length(); i++) {
			got.push_back(col->Value(i));
		}
	}
	worker.join();

	CHECK(got == std::vector<int64_t>({0, 1, 2, 3, 4}));
}
