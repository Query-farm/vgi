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
}

TEST_CASE("SabInput/OutputStream round-trip an Arrow IPC stream over the ring", "[sab]") {
	int slot = vgi_wasm_slot_open("test");
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
		auto out = std::make_shared<SabOutputStream>(slot);
		auto writer_res = arrow::ipc::MakeStreamWriter(out, schema);
		REQUIRE(writer_res.ok());
		auto writer = *writer_res;
		REQUIRE(writer->WriteRecordBatch(*batch).ok());
		REQUIRE(writer->Close().ok());
	}
	vgi_wasm_slot_write_eos(slot);

	// Read it back from w2c (SabInputStream) and verify byte-for-byte.
	auto in = std::make_shared<SabInputStream>(slot);
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
	vgi_wasm_slot_release(slot);
}
