// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit test for the SAB transport stream classes (SabInputStream /
// SabOutputStream) over the native in-process ring backend. Proves a real Arrow
// IPC stream round-trips through the duplex ring with blocking flow control — the
// C++ half of the `worker:` transport substrate, testable without a browser.
#include "catch.hpp"

#include "vgi_sab_abi.hpp"
#include "vgi_sab_stream.hpp"

#include <arrow/api.h>
#include <arrow/ipc/api.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

using duckdb::vgi::SabInputStream;
using duckdb::vgi::SabOutputStream;

// Worker-side ring ops provided by the native backend (test/support).
extern "C" {
int vgi_sab_worker_read(int slot, uint8_t *d, int n);
int vgi_sab_worker_write(int slot, const uint8_t *d, int n);
void vgi_sab_worker_close(int slot);
void vgi_sab_native_worker_fail(int region_offset, int slot, int code, int detail);
int vgi_sab_native_slot_claim(int region_offset, int slot);
void vgi_sab_native_worker_fail_claim(int region_offset, int slot, int claim, int code, int detail);
}

TEST_CASE("SabInput/OutputStream round-trip an Arrow IPC stream over the ring", "[sab]") {
	constexpr int region = 0;
	int slot = vgi_wasm_slot_open("test", region);
	REQUIRE(slot >= 0);

	// Echo worker: copy c2w -> w2c until the client closes c2w (EOF), then EOS w2c.
	std::thread worker([slot]() {
		std::vector<uint8_t> buf(8192);
		for (;;) {
			int n = vgi_sab_worker_read(slot, buf.data(), static_cast<int>(buf.size()));
			if (n <= 0) {
				break;
			}
			vgi_sab_worker_write(slot, buf.data(), n);
		}
		vgi_sab_worker_close(slot);
	});

	// Build a tiny batch: one int64 column [10, 20, 30].
	arrow::Int64Builder ib;
	REQUIRE(ib.AppendValues({10, 20, 30}).ok());
	std::shared_ptr<arrow::Array> arr;
	REQUIRE(ib.Finish(&arr).ok());
	auto schema = arrow::schema({arrow::field("v", arrow::int64())});
	auto batch = arrow::RecordBatch::Make(schema, 3, {arr});

	// Write it as an IPC stream to c2w (SabOutputStream), then EOS marker + ring EOF.
	{
		auto out = std::make_shared<SabOutputStream>(region, slot);
		auto writer_res = arrow::ipc::MakeStreamWriter(out, schema);
		REQUIRE(writer_res.ok());
		auto writer = *writer_res;
		REQUIRE(writer->WriteRecordBatch(*batch).ok());
		REQUIRE(writer->Close().ok());
	}
	vgi_wasm_slot_write_eos(region, slot);

	// Read it back from w2c (SabInputStream) and verify byte-for-byte.
	auto in = std::make_shared<SabInputStream>(region, slot);
	auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(in);
	REQUIRE(reader_res.ok());
	auto reader = *reader_res;

	std::shared_ptr<arrow::RecordBatch> got;
	REQUIRE(reader->ReadNext(&got).ok());
	REQUIRE(got != nullptr);
	CHECK(got->num_rows() == 3);
	auto col = std::static_pointer_cast<arrow::Int64Array>(got->column(0));
	CHECK(col->Value(0) == 10);
	CHECK(col->Value(1) == 20);
	CHECK(col->Value(2) == 30);

	// Next read is EOS (null batch).
	REQUIRE(reader->ReadNext(&got).ok());
	CHECK(got == nullptr);

	worker.join();
	vgi_wasm_slot_release(region, slot);
}

TEST_CASE("SAB operations carry an explicit target-region offset", "[sab]") {
	constexpr int first_region = 101;
	constexpr int second_region = 202;
	int first_slot = vgi_wasm_slot_open("first", first_region);
	int second_slot = vgi_wasm_slot_open("second", second_region);
	REQUIRE(first_slot == 0);
	REQUIRE(second_slot == 0);
	std::atomic<int> first_worker_status {0};
	std::atomic<int> second_worker_status {0};

	std::thread first_worker([&]() {
		vgi_wasm_set_channel(first_region);
		uint8_t byte = 0;
		if (vgi_sab_worker_read(first_slot, &byte, 1) != 1) {
			first_worker_status.store(-1);
			return;
		}
		byte += 10;
		if (vgi_sab_worker_write(first_slot, &byte, 1) != 1) {
			first_worker_status.store(-2);
			return;
		}
		vgi_sab_worker_close(first_slot);
		first_worker_status.store(1);
	});
	std::thread second_worker([&]() {
		vgi_wasm_set_channel(second_region);
		uint8_t byte = 0;
		if (vgi_sab_worker_read(second_slot, &byte, 1) != 1) {
			second_worker_status.store(-1);
			return;
		}
		byte += 20;
		if (vgi_sab_worker_write(second_slot, &byte, 1) != 1) {
			second_worker_status.store(-2);
			return;
		}
		vgi_sab_worker_close(second_slot);
		second_worker_status.store(1);
	});

	uint8_t first = 1;
	uint8_t second = 2;
	REQUIRE(vgi_wasm_slot_write(first_region, first_slot, &first, 1) == 1);
	REQUIRE(vgi_wasm_slot_write(second_region, second_slot, &second, 1) == 1);
	vgi_wasm_slot_write_eos(first_region, first_slot);
	vgi_wasm_slot_write_eos(second_region, second_slot);
	REQUIRE(vgi_wasm_slot_read(second_region, second_slot, &second, 1) == 1);
	REQUIRE(vgi_wasm_slot_read(first_region, first_slot, &first, 1) == 1);
	CHECK(first == 11);
	CHECK(second == 22);

	first_worker.join();
	second_worker.join();
	CHECK(first_worker_status.load() == 1);
	CHECK(second_worker_status.load() == 1);
	vgi_wasm_slot_release(first_region, first_slot);
	vgi_wasm_slot_release(second_region, second_slot);
}

TEST_CASE("SAB terminal transport metadata is claim-safe", "[sab]") {
	constexpr int region = 303;
	int slot = vgi_wasm_slot_open("terminal", region);
	REQUIRE(slot == 0);
	const int old_claim = vgi_sab_native_slot_claim(region, slot);
	vgi_sab_native_worker_fail(region, slot, 41, 9001);

	uint8_t byte = 0;
	CHECK(vgi_wasm_slot_read(region, slot, &byte, 1) == duckdb::vgi::sab::kSabTerminalTransportError);
	int code = 0;
	int detail = 0;
	REQUIRE(vgi_wasm_slot_terminal_error(region, slot, &code, &detail) == 1);
	CHECK(code == 41);
	CHECK(detail == 9001);

	vgi_wasm_slot_release(region, slot);
	slot = vgi_wasm_slot_open("terminal", region);
	REQUIRE(slot == 0);
	REQUIRE(vgi_sab_native_slot_claim(region, slot) != old_claim);
	vgi_sab_native_worker_fail_claim(region, slot, old_claim, 99, 7);
	CHECK(vgi_wasm_slot_read(region, slot, &byte, 1) == 0);
	CHECK(vgi_wasm_slot_terminal_error(region, slot, &code, &detail) == 0);
	vgi_wasm_slot_release(region, slot);
}
