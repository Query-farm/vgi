// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
//
// Arrow IPC byte streams over a SAB duplex slot — the transport analog of
// FdInputStream/FdOutputStream (vgi_arrow_ipc.hpp), but backed by the ring stubs
// (vgi_sab_abi.hpp) instead of a pipe fd. A `WebWorkerFunctionConnection` hands
// these to the same Arrow StreamReader/StreamWriter the subprocess path uses, so
// the producer/exchange streaming logic is reused unchanged.
//
// SabOutputStream::Write  -> vgi_wasm_slot_write   (blocking, c2w ring)
// SabInputStream::Read    -> vgi_wasm_slot_read    (blocking, w2c ring; 0 = EOS)
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdint>
#include <memory>

namespace duckdb {
class ClientContext; // fwd: timeout/cancel polling (parity with FdInputStream)
namespace vgi {

// Non-owning input stream reading the worker->client (w2c) ring of `slot`.
class SabInputStream : public arrow::io::InputStream {
public:
	explicit SabInputStream(int slot, ClientContext *context = nullptr);
	~SabInputStream() override;

	arrow::Status Close() override;
	bool closed() const override;
	arrow::Result<int64_t> Tell() const override;
	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override;
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

private:
	int slot_;
	int64_t position_;
	bool is_open_;
	ClientContext *context_;
};

// Non-owning output stream writing the client->worker (c2w) ring of `slot`.
class SabOutputStream : public arrow::io::OutputStream {
public:
	explicit SabOutputStream(int slot);
	~SabOutputStream() override = default;

	arrow::Status Close() override;
	bool closed() const override;
	arrow::Result<int64_t> Tell() const override;
	arrow::Status Write(const void *data, int64_t nbytes) override;
	arrow::Status Flush() override;

private:
	int slot_;
	int64_t position_;
	bool is_open_;
};

} // namespace vgi
} // namespace duckdb
