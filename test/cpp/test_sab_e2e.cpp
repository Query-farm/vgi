// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Cross-language native e2e for the `worker:` SAB transport: a C++ producer
// client (SerializeRpcRequest + the tick loop over SabInputStream/SabOutputStream)
// drives the REAL Rust `vgi-rpc` `server.serve()` (linked from the sabffi
// staticlib) over one native ring — both halves meeting over the ABI in a single
// process, no browser. Proves the C++ stream side and the Rust serve side
// interoperate end-to-end at the protocol level.
#include "catch.hpp"

#include "vgi_rpc_client.hpp" // SerializeRpcRequest
#include "vgi_sab_abi.hpp"
#include "vgi_sab_stream.hpp"

#include <arrow/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <thread>
#include <vector>

using duckdb::vgi::SabInputStream;
using duckdb::vgi::SabOutputStream;
using duckdb::vgi::SerializeRpcRequest;
using duckdb::vgi::VgiProtocolId;

// Rust FFI worker (sabffi staticlib): serve one slot to completion.
extern "C" void vgi_rust_serve_sab_slot(int slot);

// The sabffi worker is a bare vgi-rpc server hosting ONE protocol, "Svc"
// (`RpcServer::builder().protocol_name("Svc")` in test/support/sabffi). vgi-rpc
// requires every request's `vgi_rpc.protocol` to name a hosted protocol, so these
// requests address it rather than SerializeRpcRequest's default — the VGI worker
// protocol, which this server does not host and answers with an error envelope.
// The version is not enforced: the server declares none.
constexpr VgiProtocolId SVC_PROTOCOL {"Svc", "1.0.0"};

// The Rust worker serving one claimed slot on its own thread, owning both the
// thread and the slot. Every exit from a test body passes through the
// destructor, including the exception a failed REQUIRE or FAIL throws, so the
// worker is always stopped and joined and the slot always released.
//
// A bare std::thread did not survive a failing check: unwinding destroyed it
// still joinable, which is std::terminate, so the binary aborted with
// "terminate called without an active exception" (SIGABRT) instead of
// reporting the failure, and no later case ran.
class RustSlotWorker {
public:
	RustSlotWorker(int region, int slot)
	    : region_(region), slot_(slot), thread_([slot]() { vgi_rust_serve_sab_slot(slot); }) {
	}
	RustSlotWorker(const RustSlotWorker &) = delete;
	RustSlotWorker &operator=(const RustSlotWorker &) = delete;

	// The case has finished its conversation and closed c2w: wait for serve() to
	// return on that EOF. Nothing is forced, so a case that gets here still
	// proves the worker stops on the client's end-of-stream by itself.
	void Join() {
		thread_.join();
	}

	~RustSlotWorker() {
		const bool abandoned = thread_.joinable();
		if (abandoned) {
			// The body threw mid-conversation, leaving the worker blocked on the
			// ring. EOF on c2w ends a worker blocked reading; it has to come first,
			// since write_eos is a no-op on a released slot.
			vgi_wasm_slot_write_eos(region_, slot_);
		}
		// Releasing also wakes a worker blocked on a full w2c, whose writes then
		// fail, so serve() returns either way and the join below cannot hang.
		vgi_wasm_slot_release(region_, slot_);
		if (abandoned) {
			thread_.join();
		}
	}

private:
	int region_;
	int slot_;
	std::thread thread_;
};

// Next batch of a worker response. vgi-rpc reports an error in-band, as a 0-row
// batch whose custom metadata carries vgi_rpc.log_level=EXCEPTION; fail with the
// worker's message rather than reading column(0) of an envelope, which for an
// error with an empty schema is a segfault that names nothing.
static std::shared_ptr<arrow::RecordBatch> NextBatch(arrow::ipc::RecordBatchStreamReader &reader) {
	auto next = reader.ReadNext();
	REQUIRE(next.ok());
	const auto &md = next->custom_metadata;
	if (md) {
		auto level = md->Get(duckdb::vgi::RPC_LOG_LEVEL_KEY);
		if (level.ok() && *level == "EXCEPTION") {
			auto message = md->Get(duckdb::vgi::RPC_LOG_MESSAGE_KEY);
			FAIL("worker answered with an error: " << (message.ok() ? *message : std::string("<no message>")));
		}
	}
	return next->batch;
}

TEST_CASE("C++ producer client <-> Rust serve_sab (count_to) over the ring", "[sab-e2e]") {
	constexpr int region = 0;
	int slot = vgi_wasm_slot_open("test", region);
	REQUIRE(slot >= 0);

	// The Rust worker serves this slot on its own thread.
	RustSlotWorker rust_worker(region, slot);

	// --- C++ producer client ------------------------------------------------
	// 1. Invocation request: params batch {total: 3}.
	arrow::Int64Builder tb;
	REQUIRE(tb.Append(3).ok());
	std::shared_ptr<arrow::Array> ta;
	REQUIRE(tb.Finish(&ta).ok());
	// The nullable flag is load-bearing on an RPC params batch: vgi-rpc
	// validates it field-by-field before dispatch, and Arrow's two-argument
	// arrow::field() defaults to NULLABLE while the Rust #[service]-derived
	// schema declares <i64 as VgiArrow>::nullable() == false. A mismatch is
	// answered with an exception envelope, which NextBatch reports.
	auto params = arrow::RecordBatch::Make(arrow::schema({arrow::field("total", arrow::int64(), /*nullable=*/false)}), 1, {ta});
	std::vector<uint8_t> req = SerializeRpcRequest("count_to", params, SVC_PROTOCOL);

	auto out = std::make_shared<SabOutputStream>(region, slot);
	REQUIRE(out->Write(req.data(), static_cast<int64_t>(req.size())).ok());

	// 2. Tick stream (empty schema) on the same c2w + tick #1 (bootstrap).
	auto empty_schema = arrow::schema({});
	auto tick = arrow::RecordBatch::Make(empty_schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
	auto tick_writer_res = arrow::ipc::MakeStreamWriter(out, empty_schema);
	REQUIRE(tick_writer_res.ok());
	auto tick_writer = *tick_writer_res;
	REQUIRE(tick_writer->WriteRecordBatch(*tick).ok());

	// 3. Data reader on the server's output stream (w2c).
	auto in = std::make_shared<SabInputStream>(region, slot);
	auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(in);
	REQUIRE(reader_res.ok());
	auto reader = *reader_res;

	// 4. One-ahead loop: read a batch, then send the next tick.
	std::vector<int64_t> got;
	std::shared_ptr<arrow::RecordBatch> batch;
	for (;;) {
		batch = NextBatch(*reader);
		if (!batch) {
			break; // EOS
		}
		auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
		for (int64_t i = 0; i < col->length(); i++) {
			got.push_back(col->Value(i));
		}
		REQUIRE(tick_writer->WriteRecordBatch(*tick).ok());
	}
	// 5. Close the tick stream (Arrow EOS) + the c2w ring (EOF) so serve() returns.
	REQUIRE(tick_writer->Close().ok());
	vgi_wasm_slot_write_eos(region, slot);

	rust_worker.Join();
	CHECK(got == std::vector<int64_t>({0, 1, 2}));
}

// Prove a UNARY RPC then a STREAMING (producer) RPC succeed on the SAME slot,
// each with fresh stream objects over the one persistent ring — the exact
// PerformBindRpc (unary) → PerformInit (streaming) sequencing WebWorkerFunctionConnection
// relies on. This validates the port's #1 flagged uncertainty (multi-stream
// sequencing / ring position persistence across stream recreation).
TEST_CASE("unary RPC then producer RPC on one slot (bind->init sequencing)", "[sab-e2e]") {
	constexpr int region = 0;
	int slot = vgi_wasm_slot_open("test", region);
	REQUIRE(slot >= 0);
	RustSlotWorker rust_worker(region, slot);

	// --- Phase 1: unary add_one(41) -> 42 (the "bind" analog) ---
	{
		arrow::Int64Builder xb;
		REQUIRE(xb.Append(41).ok());
		std::shared_ptr<arrow::Array> xa;
		REQUIRE(xb.Finish(&xa).ok());
		auto params = arrow::RecordBatch::Make(arrow::schema({arrow::field("x", arrow::int64(), /*nullable=*/false)}), 1, {xa});
		auto req = SerializeRpcRequest("add_one", params, SVC_PROTOCOL);

		auto out = std::make_shared<SabOutputStream>(region, slot);
		REQUIRE(out->Write(req.data(), static_cast<int64_t>(req.size())).ok());

		auto in = std::make_shared<SabInputStream>(region, slot);
		auto reader = *arrow::ipc::RecordBatchStreamReader::Open(in);
		auto resp = NextBatch(*reader);
		REQUIRE(resp != nullptr);
		auto rcol = std::static_pointer_cast<arrow::Int64Array>(resp->column(0));
		CHECK(rcol->Value(0) == 42);
		CHECK(NextBatch(*reader) == nullptr); // drain the response's EOS
	}

	// --- Phase 2: producer count_to(3) -> [0,1,2] on the SAME slot (the "init" analog) ---
	std::vector<int64_t> got;
	{
		arrow::Int64Builder tb;
		REQUIRE(tb.Append(3).ok());
		std::shared_ptr<arrow::Array> ta;
		REQUIRE(tb.Finish(&ta).ok());
		auto params = arrow::RecordBatch::Make(arrow::schema({arrow::field("total", arrow::int64(), /*nullable=*/false)}), 1, {ta});
		auto req = SerializeRpcRequest("count_to", params, SVC_PROTOCOL);

		auto out = std::make_shared<SabOutputStream>(region, slot); // fresh stream, same slot
		REQUIRE(out->Write(req.data(), static_cast<int64_t>(req.size())).ok());

		auto empty_schema = arrow::schema({});
		auto tick = arrow::RecordBatch::Make(empty_schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto tick_writer = *arrow::ipc::MakeStreamWriter(out, empty_schema);
		REQUIRE(tick_writer->WriteRecordBatch(*tick).ok());

		auto in = std::make_shared<SabInputStream>(region, slot); // fresh reader, same slot
		auto reader = *arrow::ipc::RecordBatchStreamReader::Open(in);
		std::shared_ptr<arrow::RecordBatch> batch;
		for (;;) {
			batch = NextBatch(*reader);
			if (!batch) {
				break;
			}
			auto col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
			for (int64_t i = 0; i < col->length(); i++) {
				got.push_back(col->Value(i));
			}
			REQUIRE(tick_writer->WriteRecordBatch(*tick).ok());
		}
		REQUIRE(tick_writer->Close().ok());
		vgi_wasm_slot_write_eos(region, slot);
	}

	rust_worker.Join();
	CHECK(got == std::vector<int64_t>({0, 1, 2}));
}

// Exchange mode (scalar / table-in-out path): the client writes a real INPUT
// batch and reads a 1:1 OUTPUT batch — exercising OpenInputWriter/WriteInputBatch/
// CloseInputWriter over the ring (validates the port's #2 flagged uncertainty:
// CloseInputWriter -> slot_write_eos ordering in exchange mode).
TEST_CASE("exchange RPC (scale) over the ring", "[sab-e2e]") {
	constexpr int region = 0;
	int slot = vgi_wasm_slot_open("test", region);
	REQUIRE(slot >= 0);
	RustSlotWorker rust_worker(region, slot);

	// 1. request scale(factor = 10.0).
	arrow::DoubleBuilder fb;
	REQUIRE(fb.Append(10.0).ok());
	std::shared_ptr<arrow::Array> fa;
	REQUIRE(fb.Finish(&fa).ok());
	auto params = arrow::RecordBatch::Make(arrow::schema({arrow::field("factor", arrow::float64(), /*nullable=*/false)}), 1, {fa});
	auto req = SerializeRpcRequest("scale", params, SVC_PROTOCOL);

	auto out = std::make_shared<SabOutputStream>(region, slot);
	REQUIRE(out->Write(req.data(), static_cast<int64_t>(req.size())).ok());

	// 2. input stream (single "value" f64 column) + one input batch [1,2,3].
	auto input_schema = arrow::schema({arrow::field("value", arrow::float64())});
	arrow::DoubleBuilder ib;
	REQUIRE(ib.AppendValues({1.0, 2.0, 3.0}).ok());
	std::shared_ptr<arrow::Array> ia;
	REQUIRE(ib.Finish(&ia).ok());
	auto input_batch = arrow::RecordBatch::Make(input_schema, 3, {ia});
	auto input_writer = *arrow::ipc::MakeStreamWriter(out, input_schema);
	REQUIRE(input_writer->WriteRecordBatch(*input_batch).ok());

	// 3. read the 1:1 scaled output batch.
	auto in = std::make_shared<SabInputStream>(region, slot);
	auto reader = *arrow::ipc::RecordBatchStreamReader::Open(in);
	auto outb = NextBatch(*reader);
	REQUIRE(outb != nullptr);
	auto ocol = std::static_pointer_cast<arrow::DoubleArray>(outb->column(0));
	std::vector<double> got;
	for (int64_t i = 0; i < ocol->length(); i++) {
		got.push_back(ocol->Value(i));
	}
	CHECK(got == std::vector<double>({10.0, 20.0, 30.0}));

	// 4. close input (Arrow EOS + ring EOF), drain output EOS.
	REQUIRE(input_writer->Close().ok());
	vgi_wasm_slot_write_eos(region, slot);
	CHECK(NextBatch(*reader) == nullptr);

	rust_worker.Join();
}
