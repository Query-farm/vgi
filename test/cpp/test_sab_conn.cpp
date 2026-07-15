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
