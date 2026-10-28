// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"
//
// Arrow IPC byte streams over a SAB duplex slot — the transport analog of
// FdInputStream/FdOutputStream (vgi_arrow_ipc.hpp), but backed by the ring stubs
// (vgi_sab_abi.hpp) instead of a pipe fd. A `WebWorkerFunctionConnection` hands
// these to the same Arrow StreamReader/StreamWriter the subprocess path uses, so
// the producer/exchange streaming logic is reused unchanged.
//
// SabOutputStream::Write  -> vgi_wasm_slot_write   (blocking, c2w ring)
// SabInputStream::Read    -> vgi_wasm_slot_read    (blocking, w2c ring; 0 = EOS)

#include <cstdint>
#include <chrono>
#include <memory>
#include <optional>

namespace duckdb {
class ClientContext; // fwd: timeout/cancel polling (parity with FdInputStream)
namespace vgi {

// Non-owning input stream reading the worker->client (w2c) ring of `slot`.
class SabInputStream : public VgiInputStream {
public:
	explicit SabInputStream(int region_offset, int slot, ClientContext *context = nullptr,
	                        std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt);
	~SabInputStream() override;

	void Close() override;
	bool closed() const override;
	int64_t Tell() const {
		return position_;
	}
	int64_t Read(int64_t nbytes, void *out) override;

private:
	int slot_;
	int region_offset_;
	int64_t position_;
	bool is_open_;
	ClientContext *context_;
	std::optional<std::chrono::steady_clock::time_point> deadline_;
};

// Non-owning output stream writing the client->worker (c2w) ring of `slot`.
class SabOutputStream : public VgiOutputStream {
public:
	explicit SabOutputStream(int region_offset, int slot);
	~SabOutputStream() override = default;

	void Close() override;
	bool closed() const override;
	int64_t Tell() const override {
		return position_;
	}
	void Write(const void *data, int64_t nbytes) override;

private:
	int slot_;
	int region_offset_;
	int64_t position_;
	bool is_open_;
};

} // namespace vgi
} // namespace duckdb
