// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_sab_stream.hpp"
#include "vgi_sab_abi.hpp"

#include "duckdb/main/client_context.hpp" // ClientContext::interrupted (cancel polling)

#include "duckdb/common/exception.hpp"

#include <string>

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

void SabInputStream::Close() {
	is_open_ = false;
}

bool SabInputStream::closed() const {
	return !is_open_;
}

// Blocking read that fills up to `nbytes`, looping over the ring until the
// request is satisfied or the worker signals EOS (slot_read == 0). Mirrors
// FdInputStream::Read's fill-or-EOF loop; the IPC stream reader relies on
// this "read as much as asked, short only at end-of-stream" contract.
int64_t SabInputStream::Read(int64_t nbytes, void *out) {
	if (!is_open_) {
		throw IOException("SabInputStream: read on closed stream");
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
			// interruptible: on an error/cancel DuckDB sets context.IsInterrupted() (see
			// Executor::PushError) and busy-waits for all executor tasks to return; an
			// uninterruptible read here would never return and would freeze the whole
			// engine. Mirrors the subprocess WaitForReadableUntilCancel poll.
			if (context_ && context_->IsInterrupted()) {
				throw IOException("SabInputStream: read interrupted (query cancelled)");
			}
			if (deadline_ && std::chrono::steady_clock::now() >= *deadline_) {
				throw IOException("SabInputStream: read timed out");
			}
			continue;
		}
		if (n == sab::kSabTerminalTransportError) {
			int code = 0;
			int detail = 0;
			if (vgi_wasm_slot_terminal_error(region_offset_, slot_, &code, &detail) == 1) {
				throw IOException("SabInputStream: terminal transport error (code=" + std::to_string(code) +
				                  ", detail=" + std::to_string(detail) + ")");
			}
			throw IOException("SabInputStream: terminal transport error metadata unavailable");
		}
		if (n < 0) {
			throw IOException("SabInputStream: slot_read error/cancel");
		}
		if (n == 0) {
			break; // EOS
		}
		total += n;
	}
	position_ += total;
	return total;
}

// ---- SabOutputStream --------------------------------------------------------

SabOutputStream::SabOutputStream(int region_offset, int slot)
    : slot_(slot), region_offset_(region_offset), position_(0), is_open_(true) {
}

void SabOutputStream::Close() {
	is_open_ = false;
}

bool SabOutputStream::closed() const {
	return !is_open_;
}

// Blocking write of the whole buffer into the c2w ring. slot_write blocks on
// backpressure and returns n (all bytes) or negative on cancel.
void SabOutputStream::Write(const void *data, int64_t nbytes) {
	if (!is_open_) {
		throw IOException("SabOutputStream: write on closed stream");
	}
	VgiSabEnsureChannelOnRealm(region_offset_);
	const auto *src = static_cast<const uint8_t *>(data);
	int64_t total = 0;
	while (total < nbytes) {
		int want = static_cast<int>(nbytes - total > INT32_MAX ? INT32_MAX : nbytes - total);
		int n = vgi_wasm_slot_write(region_offset_, slot_, src + total, want);
		if (n < 0) {
			throw IOException("SabOutputStream: slot_write error/cancel");
		}
		total += n;
	}
	position_ += total;
}

} // namespace vgi
} // namespace duckdb
