// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_sab_stream.hpp"
#include "vgi_sab_abi.hpp"

#include "duckdb/main/client_context.hpp" // ClientContext::interrupted (cancel polling)

#include <arrow/buffer.h>

namespace duckdb {
namespace vgi {

// Preserve the ABI-v1 worker-side channel selector on the calling pthread's JS
// realm. Client ring operations carry region_offset explicitly and do not rely
// on this mutable selector. wasm-only; a no-op on the native test harness.
void VgiSabEnsureChannelOnRealm(int region_offset);

// ---- SabInputStream ---------------------------------------------------------

SabInputStream::SabInputStream(int region_offset, int slot, ClientContext *context,
                               std::optional<std::chrono::steady_clock::time_point> deadline)
    : slot_(slot), region_offset_(region_offset), position_(0), is_open_(true), context_(context), deadline_(deadline) {
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
	VgiSabEnsureChannelOnRealm(region_offset_);
	auto *dst = static_cast<uint8_t *>(out);
	int64_t total = 0;
	while (total < nbytes) {
		int want = static_cast<int>(nbytes - total > INT32_MAX ? INT32_MAX : nbytes - total);
		int n = vgi_wasm_slot_read(region_offset_, slot_, dst + total, want);
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
			if (deadline_ && std::chrono::steady_clock::now() >= *deadline_) {
				return arrow::Status::IOError("SabInputStream: read timed out");
			}
			continue;
		}
		if (n == sab::kSabTerminalTransportError) {
			int code = 0;
			int detail = 0;
			if (vgi_wasm_slot_terminal_error(region_offset_, slot_, &code, &detail) == 1) {
				return arrow::Status::IOError("SabInputStream: terminal transport error (code=", code,
				                              ", detail=", detail, ")");
			}
			return arrow::Status::IOError("SabInputStream: terminal transport error metadata unavailable");
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

SabOutputStream::SabOutputStream(int region_offset, int slot)
    : slot_(slot), region_offset_(region_offset), position_(0), is_open_(true) {
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
	VgiSabEnsureChannelOnRealm(region_offset_);
	const auto *src = static_cast<const uint8_t *>(data);
	int64_t total = 0;
	while (total < nbytes) {
		int want = static_cast<int>(nbytes - total > INT32_MAX ? INT32_MAX : nbytes - total);
		int n = vgi_wasm_slot_write(region_offset_, slot_, src + total, want);
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
