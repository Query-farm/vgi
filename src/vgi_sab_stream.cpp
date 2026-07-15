// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_sab_stream.hpp"
#include "vgi_sab_abi.hpp"

#include "duckdb/main/client_context.hpp" // ClientContext::interrupted (cancel polling)

#include <arrow/buffer.h>

namespace duckdb {
namespace vgi {

// Publish the shared channel offset onto the calling pthread's JS realm before ring
// I/O (defined in vgi_webworker_function_connection.cpp). A connection is bound on one
// pthread but DuckDB may run its ring reads/writes on a different pool pthread whose
// realm never had the offset set; without this the JS ring stubs read the wrong
// linear-memory location and block. wasm-only; a no-op on the native test harness.
void VgiSabEnsureChannelOnRealm();

// ---- SabInputStream ---------------------------------------------------------

SabInputStream::SabInputStream(int slot, ClientContext *context)
    : slot_(slot), position_(0), is_open_(true), context_(context) {
}

SabInputStream::~SabInputStream() = default;

arrow::Status SabInputStream::Close() {
	is_open_ = false;
	return arrow::Status::OK();
}

bool SabInputStream::closed() const {
	return !is_open_;
}

arrow::Result<int64_t> SabInputStream::Tell() const {
	return position_;
}

// Blocking read that fills up to `nbytes`, looping over the ring until the
// request is satisfied or the worker signals EOS (slot_read == 0). Mirrors
// FdInputStream::Read's fill-or-EOF loop; the Arrow IPC StreamReader relies on
// this "read as much as asked, short only at end-of-stream" contract.
arrow::Result<int64_t> SabInputStream::Read(int64_t nbytes, void *out) {
	if (!is_open_) {
		return arrow::Status::IOError("SabInputStream: read on closed stream");
	}
	VgiSabEnsureChannelOnRealm(); // this pthread's realm must know the channel offset
	auto *dst = static_cast<uint8_t *>(out);
	int64_t total = 0;
	while (total < nbytes) {
		int want = static_cast<int>(nbytes - total > INT32_MAX ? INT32_MAX : nbytes - total);
		int n = vgi_wasm_slot_read(slot_, dst + total, want);
		if (n == sab::kSabWouldBlock) {
			// The ring was empty for a bounded wait. Poll query cancellation and retry —
			// this is what keeps a read running as a DuckDB async prefetch task
			// interruptible: on an error/cancel DuckDB sets context.interrupted (see
			// Executor::PushError) and busy-waits for all executor tasks to return; an
			// uninterruptible read here would never return and would freeze the whole
			// engine. Mirrors the subprocess WaitForReadableUntilCancel poll.
			if (context_ && context_->interrupted) {
				return arrow::Status::IOError("SabInputStream: read interrupted (query cancelled)");
			}
			continue;
		}
		if (n < 0) {
			return arrow::Status::IOError("SabInputStream: slot_read error/cancel");
		}
		if (n == 0) {
			break; // EOS
		}
		total += n;
	}
	position_ += total;
	return total;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SabInputStream::Read(int64_t nbytes) {
	ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
	ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
	ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, /*shrink_to_fit=*/false));
	return std::move(buffer);
}

// ---- SabOutputStream --------------------------------------------------------

SabOutputStream::SabOutputStream(int slot) : slot_(slot), position_(0), is_open_(true) {
}

arrow::Status SabOutputStream::Close() {
	is_open_ = false;
	return arrow::Status::OK();
}

bool SabOutputStream::closed() const {
	return !is_open_;
}

arrow::Result<int64_t> SabOutputStream::Tell() const {
	return position_;
}

// Blocking write of the whole buffer into the c2w ring. slot_write blocks on
// backpressure and returns n (all bytes) or negative on cancel.
arrow::Status SabOutputStream::Write(const void *data, int64_t nbytes) {
	if (!is_open_) {
		return arrow::Status::IOError("SabOutputStream: write on closed stream");
	}
	VgiSabEnsureChannelOnRealm(); // this pthread's realm must know the channel offset
	const auto *src = static_cast<const uint8_t *>(data);
	int64_t total = 0;
	while (total < nbytes) {
		int want = static_cast<int>(nbytes - total > INT32_MAX ? INT32_MAX : nbytes - total);
		int n = vgi_wasm_slot_write(slot_, src + total, want);
		if (n < 0) {
			return arrow::Status::IOError("SabOutputStream: slot_write error/cancel");
		}
		total += n;
	}
	position_ += total;
	return arrow::Status::OK();
}

arrow::Status SabOutputStream::Flush() {
	return arrow::Status::OK();
}

} // namespace vgi
} // namespace duckdb
