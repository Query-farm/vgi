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
#include <chrono>
#include <future>
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
int vgi_sab_native_w2c_closed_claim(int region_offset, int slot);
int vgi_sab_native_slot_reservation(int region_offset, int slot);
int vgi_sab_native_slot_is_reset(int region_offset, int slot);
void vgi_sab_native_pause_open_before_publish(int region_offset);
int vgi_sab_native_wait_open_before_publish(int region_offset);
void vgi_sab_native_resume_open_publish(int region_offset);
void vgi_sab_native_pause_terminal_snapshot(int region_offset);
int vgi_sab_native_wait_terminal_snapshot(int region_offset);
void vgi_sab_native_resume_terminal_snapshot(int region_offset);
void vgi_sab_native_pause_worker_operation(int region_offset);
int vgi_sab_native_wait_worker_operation(int region_offset);
void vgi_sab_native_resume_worker_operation(int region_offset);
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
	CHECK(vgi_sab_native_w2c_closed_claim(region, slot) == old_claim);
	CHECK(vgi_wasm_slot_terminal_error(region, slot, &code, &detail) == 0);
	// A stale close token is ignored. Only this claim's worker can publish EOS.
	vgi_wasm_set_channel(region);
	vgi_sab_worker_close(slot);
	CHECK(vgi_wasm_slot_read(region, slot, &byte, 1) == 0);
	vgi_wasm_slot_release(region, slot);
}

TEST_CASE("SAB slot state is published only after reset", "[sab][race]") {
	constexpr int region = 404;
	int slot = vgi_wasm_slot_open("publication", region);
	REQUIRE(slot == 0);
	uint8_t dirty = 42;
	REQUIRE(vgi_wasm_slot_write(region, slot, &dirty, 1) == 1);
	vgi_wasm_slot_write_eos(region, slot);
	vgi_sab_native_worker_fail(region, slot, 7, 8);
	vgi_wasm_slot_release(region, slot);

	vgi_sab_native_pause_open_before_publish(region);
	std::atomic<int> opened {-99};
	std::thread opener([&]() { opened.store(vgi_wasm_slot_open("publication", region)); });
	const int reached = vgi_sab_native_wait_open_before_publish(region);
	const int state_while_paused = vgi_sab_native_slot_claim(region, slot);
	const int reservation_while_paused = vgi_sab_native_slot_reservation(region, slot);
	const int reset_while_paused = vgi_sab_native_slot_is_reset(region, slot);
	vgi_sab_native_resume_open_publish(region);
	opener.join();

	REQUIRE(reached == 1);
	CHECK(state_while_paused == duckdb::vgi::sab::kSlotFree);
	CHECK(reservation_while_paused != 0);
	CHECK(reset_while_paused == 1);
	CHECK(opened.load() == slot);
	CHECK(vgi_sab_native_slot_claim(region, slot) != duckdb::vgi::sab::kSlotFree);
	CHECK(vgi_sab_native_slot_reservation(region, slot) == 0);
	vgi_wasm_slot_release(region, slot);
}

TEST_CASE("SAB terminal metadata rejects a snapshot torn by reclaim", "[sab][race]") {
	constexpr int region = 405;
	int slot = vgi_wasm_slot_open("terminal-snapshot", region);
	REQUIRE(slot == 0);
	vgi_sab_native_worker_fail(region, slot, 41, 9001);

	vgi_sab_native_pause_terminal_snapshot(region);
	std::atomic<int> result {-1};
	int code = 111;
	int detail = 222;
	std::thread reader(
	    [&]() { result.store(vgi_wasm_slot_terminal_error(region, slot, &code, &detail), std::memory_order_release); });
	const int reached = vgi_sab_native_wait_terminal_snapshot(region);
	vgi_wasm_slot_release(region, slot);
	const int replacement_slot = vgi_wasm_slot_open("terminal-snapshot", region);
	vgi_sab_native_resume_terminal_snapshot(region);
	reader.join();

	REQUIRE(reached == 1);
	REQUIRE(replacement_slot == slot);
	CHECK(result.load(std::memory_order_acquire) == 0);
	CHECK(code == 111);
	CHECK(detail == 222);
	vgi_wasm_slot_release(region, replacement_slot);
}

TEST_CASE("stale native worker cannot consume or publish into a reclaimed claim", "[sab][race]") {
	constexpr int region = 406;
	int slot = vgi_wasm_slot_open("stale-worker", region);
	REQUIRE(slot == 0);
	uint8_t old_request = 3;
	REQUIRE(vgi_wasm_slot_write(region, slot, &old_request, 1) == 1);

	std::promise<void> captured_promise;
	auto captured = captured_promise.get_future();
	std::promise<void> proceed_promise;
	auto proceed = proceed_promise.get_future();
	std::atomic<int> initial_read {-99};
	std::atomic<int> stale_read {-99};
	std::atomic<int> stale_write {-99};
	std::thread stale_worker([&]() {
		vgi_wasm_set_channel(region);
		uint8_t byte = 0;
		initial_read.store(vgi_sab_worker_read(slot, &byte, 1), std::memory_order_release);
		captured_promise.set_value();
		proceed.wait();
		stale_read.store(vgi_sab_worker_read(slot, &byte, 1), std::memory_order_release);
		byte = 99;
		stale_write.store(vgi_sab_worker_write(slot, &byte, 1), std::memory_order_release);
		vgi_sab_worker_close(slot);
	});
	const auto captured_status = captured.wait_for(std::chrono::seconds(5));

	vgi_wasm_slot_release(region, slot);
	slot = vgi_wasm_slot_open("stale-worker", region);
	REQUIRE(slot == 0);
	uint8_t new_request = 7;
	REQUIRE(vgi_wasm_slot_write(region, slot, &new_request, 1) == 1);
	proceed_promise.set_value();
	stale_worker.join();
	REQUIRE(captured_status == std::future_status::ready);
	CHECK(initial_read.load(std::memory_order_acquire) == 1);
	CHECK(stale_read.load(std::memory_order_acquire) == 0);
	CHECK(stale_write.load(std::memory_order_acquire) == 0);
	CHECK(vgi_sab_native_w2c_closed_claim(region, slot) == 0);

	std::atomic<int> current_worker_status {0};
	std::thread current_worker([&]() {
		vgi_wasm_set_channel(region);
		uint8_t byte = 0;
		if (vgi_sab_worker_read(slot, &byte, 1) != 1) {
			current_worker_status.store(-1);
			return;
		}
		byte += 10;
		if (vgi_sab_worker_write(slot, &byte, 1) != 1) {
			current_worker_status.store(-2);
			return;
		}
		vgi_sab_worker_close(slot);
		current_worker_status.store(1);
	});
	uint8_t response = 0;
	REQUIRE(vgi_wasm_slot_read(region, slot, &response, 1) == 1);
	CHECK(response == 17);
	REQUIRE(vgi_wasm_slot_read(region, slot, &response, 1) == 0);
	current_worker.join();
	CHECK(current_worker_status.load() == 1);
	vgi_wasm_slot_release(region, slot);
}

TEST_CASE("release during a native worker ring operation cannot cross into the next claim", "[sab][race]") {
	constexpr int region = 408;
	int slot = vgi_wasm_slot_open("mid-operation-reclaim", region);
	REQUIRE(slot == 0);
	const int old_claim = vgi_sab_native_slot_claim(region, slot);
	std::vector<int> occupied_slots;
	for (int i = 1; i < 16; ++i) {
		const int occupied = vgi_wasm_slot_open("mid-operation-reclaim", region);
		REQUIRE(occupied == i);
		occupied_slots.push_back(occupied);
	}
	uint8_t old_request = 13;
	REQUIRE(vgi_wasm_slot_write(region, slot, &old_request, 1) == 1);

	vgi_sab_native_pause_worker_operation(region);
	std::atomic<int> stale_read {-99};
	std::thread stale_worker([&]() {
		vgi_wasm_set_channel(region);
		uint8_t byte = 0;
		stale_read.store(vgi_sab_worker_read(slot, &byte, 1), std::memory_order_release);
	});
	const int reached = vgi_sab_native_wait_worker_operation(region);
	vgi_wasm_slot_release(region, slot);
	const int state_after_release = vgi_sab_native_slot_claim(region, slot);
	const int reservation_during_release = vgi_sab_native_slot_reservation(region, slot);
	const int open_while_owned = vgi_wasm_slot_open("mid-operation-reclaim", region);
	vgi_sab_native_resume_worker_operation(region);
	stale_worker.join();

	REQUIRE(reached == 1);
	CHECK(state_after_release == duckdb::vgi::sab::kSlotFree);
	CHECK(reservation_during_release == old_claim);
	CHECK(open_while_owned == -1);
	CHECK(stale_read.load(std::memory_order_acquire) == 0);
	CHECK(vgi_sab_native_slot_reservation(region, slot) == 0);

	slot = vgi_wasm_slot_open("mid-operation-reclaim", region);
	REQUIRE(slot == 0);
	CHECK(vgi_sab_native_slot_is_reset(region, slot) == 1);
	CHECK(vgi_sab_native_slot_claim(region, slot) != old_claim);
	vgi_wasm_slot_release(region, slot);
	for (const int occupied : occupied_slots) {
		vgi_wasm_slot_release(region, occupied);
	}
}

TEST_CASE("native client read is released from an abandoned claim", "[sab][race]") {
	constexpr int region = 407;
	const int slot = vgi_wasm_slot_open("cancelled-read", region);
	REQUIRE(slot == 0);
	std::atomic<int> result {-99};
	std::thread reader([&]() {
		uint8_t byte = 0;
		result.store(vgi_wasm_slot_read(region, slot, &byte, 1), std::memory_order_release);
	});
	vgi_wasm_slot_release(region, slot);
	reader.join();
	CHECK(result.load(std::memory_order_acquire) == -1);
}
