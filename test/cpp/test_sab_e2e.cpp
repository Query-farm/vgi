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

#include "vgi_arrow.hpp"
#include "vgi_ipc.hpp"

#include <memory>
#include <thread>
#include <vector>

using duckdb::vgi::SabInputStream;
using duckdb::vgi::SabOutputStream;
using duckdb::vgi::SerializeRpcRequest;
using duckdb::vgi::VgiProtocolId;
using namespace duckdb::vgi;

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
static VgiBatchPtr NextBatch(VgiIpcReader &reader) {
	auto next = reader.ReadNext();
	if (next) {
		const auto &md = *next->metadata();
		auto level = md.Get(duckdb::vgi::RPC_LOG_LEVEL_KEY);
		if (level && *level == "EXCEPTION") {
			auto message = md.Get(duckdb::vgi::RPC_LOG_MESSAGE_KEY);
			FAIL("worker answered with an error: " << (message ? *message : std::string("<no message>")));
		}
	}
	return next;
}

// A one-row, one-column NON-nullable int64 params batch {name: value}.
static VgiBatchPtr Int64Params(const char *name, int64_t value) {
	VgiBatchBuilder b(MakeSchema({type::Int64().Named(name, /*is_nullable=*/false)}));
	append::Int(b.column(0), value);
	b.FinishRow();
	return b.Finish();
}

// The zero-column, zero-row tick batch.
static VgiBatchPtr EmptyTick() {
	return VgiBatch::MakeEmpty(VgiSchema::Empty());
}

TEST_CASE("C++ producer client <-> Rust serve_sab (count_to) over the ring", "[sab-e2e]") {
	constexpr int region = 0;
	int slot = vgi_wasm_slot_open("test", region);
	REQUIRE(slot >= 0);

	// The Rust worker serves this slot on its own thread.
	RustSlotWorker rust_worker(region, slot);

	// --- C++ producer client ------------------------------------------------
	// 1. Invocation request: params batch {total: 3}.
	// The nullable flag is load-bearing on an RPC params batch: vgi-rpc
	// validates it field-by-field before dispatch, and VgiField::Named()
	// defaults to NULLABLE while the Rust #[service]-derived schema declares
	// <i64 as VgiArrow>::nullable() == false. A mismatch is answered with an
	// exception envelope, which NextBatch reports.
	auto params = Int64Params("total", 3);
	std::vector<uint8_t> req = SerializeRpcRequest("count_to", params, SVC_PROTOCOL);

	auto out = std::make_shared<SabOutputStream>(region, slot);
	REQUIRE_NOTHROW(out->Write(req.data(), static_cast<int64_t>(req.size())));

	// 2. Tick stream (empty schema) on the same c2w + tick #1 (bootstrap).
	auto tick = EmptyTick();
	auto tick_writer = VgiIpcWriter::Open(out, tick->schema());
	REQUIRE_NOTHROW(tick_writer->Write(*tick));

	// 3. Data reader on the server's output stream (w2c).
	auto in = std::make_shared<SabInputStream>(region, slot);
	auto reader = VgiIpcReader::Open(in);

	// 4. One-ahead loop: read a batch, then send the next tick.
	std::vector<int64_t> got;
	VgiBatchPtr batch;
	for (;;) {
		batch = NextBatch(*reader);
		if (!batch) {
			break; // EOS
		}
		auto col = batch->column(0);
		for (int64_t i = 0; i < col.length(); i++) {
			got.push_back(col.GetInt(i));
		}
		REQUIRE_NOTHROW(tick_writer->Write(*tick));
	}
	// 5. Close the tick stream (Arrow EOS) + the c2w ring (EOF) so serve() returns.
	REQUIRE_NOTHROW(tick_writer->Close());
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
		auto params = Int64Params("x", 41);
		auto req = SerializeRpcRequest("add_one", params, SVC_PROTOCOL);

		auto out = std::make_shared<SabOutputStream>(region, slot);
		REQUIRE_NOTHROW(out->Write(req.data(), static_cast<int64_t>(req.size())));

		auto in = std::make_shared<SabInputStream>(region, slot);
		auto reader = VgiIpcReader::Open(in);
		auto resp = NextBatch(*reader);
		REQUIRE(resp != nullptr);
		CHECK(resp->column(0).GetInt(0) == 42);
		CHECK(NextBatch(*reader) == nullptr); // drain the response's EOS
	}

	// --- Phase 2: producer count_to(3) -> [0,1,2] on the SAME slot (the "init" analog) ---
	std::vector<int64_t> got;
	{
		auto params = Int64Params("total", 3);
		auto req = SerializeRpcRequest("count_to", params, SVC_PROTOCOL);

		auto out = std::make_shared<SabOutputStream>(region, slot); // fresh stream, same slot
		REQUIRE_NOTHROW(out->Write(req.data(), static_cast<int64_t>(req.size())));

		auto tick = EmptyTick();
		auto tick_writer = VgiIpcWriter::Open(out, tick->schema());
		REQUIRE_NOTHROW(tick_writer->Write(*tick));

		auto in = std::make_shared<SabInputStream>(region, slot); // fresh reader, same slot
		auto reader = VgiIpcReader::Open(in);
		VgiBatchPtr batch;
		for (;;) {
			batch = NextBatch(*reader);
			if (!batch) {
				break;
			}
			auto col = batch->column(0);
			for (int64_t i = 0; i < col.length(); i++) {
				got.push_back(col.GetInt(i));
			}
			REQUIRE_NOTHROW(tick_writer->Write(*tick));
		}
		REQUIRE_NOTHROW(tick_writer->Close());
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
	VgiBatchBuilder fb(MakeSchema({type::Float64().Named("factor", /*is_nullable=*/false)}));
	append::Double(fb.column(0), 10.0);
	fb.FinishRow();
	auto params = fb.Finish();
	auto req = SerializeRpcRequest("scale", params, SVC_PROTOCOL);

	auto out = std::make_shared<SabOutputStream>(region, slot);
	REQUIRE_NOTHROW(out->Write(req.data(), static_cast<int64_t>(req.size())));

	// 2. input stream (single "value" f64 column) + one input batch [1,2,3].
	auto input_schema = MakeSchema({type::Float64().Named("value")});
	VgiBatchBuilder ib(input_schema);
	for (double v : {1.0, 2.0, 3.0}) {
		append::Double(ib.column(0), v);
		ib.FinishRow();
	}
	auto input_batch = ib.Finish();
	auto input_writer = VgiIpcWriter::Open(out, input_schema);
	REQUIRE_NOTHROW(input_writer->Write(*input_batch));

	// 3. read the 1:1 scaled output batch.
	auto in = std::make_shared<SabInputStream>(region, slot);
	auto reader = VgiIpcReader::Open(in);
	auto outb = NextBatch(*reader);
	REQUIRE(outb != nullptr);
	auto ocol = outb->column(0);
	std::vector<double> got;
	for (int64_t i = 0; i < ocol.length(); i++) {
		got.push_back(ocol.GetDouble(i));
	}
	CHECK(got == std::vector<double>({10.0, 20.0, 30.0}));

	// 4. close input (Arrow EOS + ring EOF), drain output EOS.
	REQUIRE_NOTHROW(input_writer->Close());
	vgi_wasm_slot_write_eos(region, slot);
	CHECK(NextBatch(*reader) == nullptr);

	rust_worker.Join();
}
