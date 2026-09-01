// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Native (non-wasm) implementation of the SAB transport stub contract
// (vgi_sab_abi.hpp), backed by an in-process duplex ring with std mutex/condvar
// blocking — the native analog of the browser's SAB+Atomics backend. Used by the
// C++ unit tests and the native C++<->Rust e2e harness; it lets `SabInputStream`/
// `SabOutputStream` and `WebWorkerFunctionConnection` run and be debugged without
// a browser. NOT compiled into the shipped extension (the wasm build resolves the
// stubs against --js-library instead).
//
// Exposes the 6 client stubs (vgi_wasm_slot_*) plus 3 worker-side ops
// (vgi_sab_worker_*) — the serve() end of each slot, used by the echo test and,
// via FFI, by the Rust serve_sab in the cross-language e2e.
#include "vgi_sab_abi.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

namespace {

// One SPSC byte ring (one direction), monotonic positions, blocking flow control.
struct NativeRing {
	std::mutex m;
	std::condition_variable cv;
	int64_t write_pos = 0;
	int64_t read_pos = 0;
	std::vector<uint8_t> buf;
	int cap;

	explicit NativeRing(int c) : buf(static_cast<size_t>(c)), cap(c) {
	}

	int write_all_claim_safe(const uint8_t *data, int n, const std::atomic<int> &state, int claim) {
		std::unique_lock<std::mutex> lk(m);
		int off = 0;
		while (off < n) {
			if (state.load(std::memory_order_acquire) != claim) {
				return off;
			}
			int free = cap - static_cast<int>(write_pos - read_pos);
			if (free == 0) {
				cv.wait(lk);
				continue;
			}
			int k = std::min(free, n - off);
			int pos = static_cast<int>(write_pos % cap);
			int first = std::min(k, cap - pos);
			std::memcpy(buf.data() + pos, data + off, first);
			if (k > first) {
				std::memcpy(buf.data(), data + off + first, k - first);
			}
			write_pos += k;
			off += k;
			cv.notify_all();
		}
		return n;
	}

	int read_some_claim_safe(uint8_t *out, int n, const std::atomic<int> &state, int claim,
	                         const std::atomic<int> &closed_lane, int expected_closed, int cancelled_result) {
		std::unique_lock<std::mutex> lk(m);
		for (;;) {
			if (state.load(std::memory_order_acquire) != claim) {
				return cancelled_result;
			}
			int avail = static_cast<int>(write_pos - read_pos);
			if (avail == 0) {
				if (closed_lane.load(std::memory_order_acquire) == expected_closed) {
					return 0; // EOS
				}
				cv.wait(lk);
				continue;
			}
			int k = std::min(avail, n);
			int pos = static_cast<int>(read_pos % cap);
			int first = std::min(k, cap - pos);
			std::memcpy(out, buf.data() + pos, first);
			if (k > first) {
				std::memcpy(out + first, buf.data(), k - first);
			}
			read_pos += k;
			cv.notify_all();
			return k;
		}
	}

	void wake_all() {
		std::lock_guard<std::mutex> lk(m);
		cv.notify_all();
	}

	void reset() {
		std::unique_lock<std::mutex> lk(m);
		write_pos = 0;
		read_pos = 0;
	}

	bool is_reset() {
		std::lock_guard<std::mutex> lk(m);
		return write_pos == 0 && read_pos == 0;
	}

	int try_read(uint8_t *out, int n, const std::atomic<int> &closed_lane, int expected_closed) {
		std::lock_guard<std::mutex> lk(m);
		const int avail = static_cast<int>(write_pos - read_pos);
		if (avail == 0) {
			return closed_lane.load(std::memory_order_acquire) == expected_closed ? 0
			                                                                      : duckdb::vgi::sab::kSabWouldBlock;
		}
		const int k = std::min(avail, n);
		const int pos = static_cast<int>(read_pos % cap);
		const int first = std::min(k, cap - pos);
		std::memcpy(out, buf.data() + pos, first);
		if (k > first) {
			std::memcpy(out + first, buf.data(), k - first);
		}
		read_pos += k;
		cv.notify_all();
		return k;
	}

	int try_write(const uint8_t *data, int n) {
		std::lock_guard<std::mutex> lk(m);
		const int free = cap - static_cast<int>(write_pos - read_pos);
		if (free == 0) {
			return 0;
		}
		const int k = std::min(free, n);
		const int pos = static_cast<int>(write_pos % cap);
		const int first = std::min(k, cap - pos);
		std::memcpy(buf.data() + pos, data, first);
		if (k > first) {
			std::memcpy(buf.data(), data + first, k - first);
		}
		write_pos += k;
		cv.notify_all();
		return k;
	}

	void WaitForChange() {
		std::unique_lock<std::mutex> lk(m);
		cv.wait_for(lk, std::chrono::milliseconds(10));
	}
};

struct NativeSlot {
	std::atomic<int> state {0}; // 0 free, 1 claimed
	std::atomic<int> reservation {0};
	std::atomic<int> c2w_closed {0};
	std::atomic<int> w2c_closed_claim {0};
	std::atomic<int> terminal_claim {0};
	std::atomic<int> terminal_code {0};
	std::atomic<int> terminal_detail {0};
	NativeRing c2w;
	NativeRing w2c;
	explicit NativeSlot(int cap) : c2w(cap), w2c(cap) {
	}
};

struct NativeChannel {
	std::atomic<int> claim_seq {0};
	std::vector<std::unique_ptr<NativeSlot>> slots;
	NativeChannel(int n, int cap) {
		for (int i = 0; i < n; i++) {
			slots.push_back(std::make_unique<NativeSlot>(cap));
		}
	}
};

NativeChannel &channel(int region_offset) {
	static std::mutex regions_mutex;
	static std::map<int, std::unique_ptr<NativeChannel>> regions;
	std::lock_guard<std::mutex> guard(regions_mutex);
	auto &region = regions[region_offset];
	if (!region) {
		region = std::make_unique<NativeChannel>(16, duckdb::vgi::sab::kDefaultRingCap);
	}
	return *region;
}

NativeSlot &slot_at(int region_offset, int slot) {
	return *channel(region_offset).slots[static_cast<size_t>(slot)];
}

thread_local int worker_region_offset = 0;
thread_local std::map<std::pair<int, int>, int> served_claims;

int served_claim_for(int slot) {
	auto key = std::make_pair(worker_region_offset, slot);
	auto entry = served_claims.find(key);
	if (entry != served_claims.end()) {
		return entry->second;
	}
	const int claim = slot_at(worker_region_offset, slot).state.load(std::memory_order_acquire);
	served_claims.emplace(key, claim);
	return claim;
}

// Deterministic test gates for the two publication races. Production code only
// pays a locked, disabled check in this native-only test backend.
struct PauseGate {
	std::mutex m;
	std::condition_variable cv;
	int region = 0;
	bool enabled = false;
	bool reached = false;
	bool proceed = false;

	void Arm(int target_region) {
		std::lock_guard<std::mutex> guard(m);
		region = target_region;
		enabled = true;
		reached = false;
		proceed = false;
	}

	void MaybePause(int target_region) {
		std::unique_lock<std::mutex> lock(m);
		if (!enabled || region != target_region) {
			return;
		}
		reached = true;
		cv.notify_all();
		cv.wait(lock, [&]() { return proceed; });
		enabled = false;
	}

	bool WaitUntilReached(int target_region) {
		std::unique_lock<std::mutex> lock(m);
		return cv.wait_for(lock, std::chrono::seconds(5),
		                   [&]() { return enabled && region == target_region && reached; });
	}

	void Resume(int target_region) {
		std::lock_guard<std::mutex> guard(m);
		if (enabled && region == target_region) {
			proceed = true;
			cv.notify_all();
		}
	}
};

PauseGate open_publish_gate;
PauseGate terminal_snapshot_gate;
PauseGate worker_operation_gate;

bool AcquireWorkerReservation(NativeSlot &slot, int served_claim) {
	for (;;) {
		if (slot.state.load(std::memory_order_acquire) != served_claim) {
			return false;
		}
		int expected = 0;
		if (slot.reservation.compare_exchange_weak(expected, served_claim, std::memory_order_acquire,
		                                           std::memory_order_relaxed)) {
			if (slot.state.load(std::memory_order_acquire) == served_claim) {
				return true;
			}
			slot.reservation.store(0, std::memory_order_release);
			return false;
		}
		std::this_thread::yield();
	}
}

void ReleaseWorkerReservation(NativeSlot &slot) {
	slot.reservation.store(0, std::memory_order_release);
}

} // namespace

extern "C" {

// ---- client stubs (vgi_sab_abi.hpp) ----
int vgi_wasm_ensure_worker(const char *, int) {
	return 0;
}
int vgi_wasm_slot_open(const char *, int region_offset) {
	auto &ch = channel(region_offset);
	int claim = ch.claim_seq.fetch_add(1) + 1;
	if (claim == 0) {
		claim = 1;
	}
	for (size_t i = 0; i < ch.slots.size(); i++) {
		int expected = 0;
		if (!ch.slots[i]->reservation.compare_exchange_strong(expected, claim, std::memory_order_acquire,
		                                                      std::memory_order_relaxed)) {
			continue;
		}
		if (ch.slots[i]->state.load(std::memory_order_acquire) != 0) {
			ch.slots[i]->reservation.store(0, std::memory_order_release);
			continue;
		}
		ch.slots[i]->c2w.reset();
		ch.slots[i]->w2c.reset();
		ch.slots[i]->c2w_closed.store(0, std::memory_order_relaxed);
		ch.slots[i]->w2c_closed_claim.store(0, std::memory_order_relaxed);
		ch.slots[i]->terminal_claim.store(0, std::memory_order_relaxed);
		ch.slots[i]->terminal_code.store(0, std::memory_order_relaxed);
		ch.slots[i]->terminal_detail.store(0, std::memory_order_relaxed);
		open_publish_gate.MaybePause(region_offset);
		// STATE is the release-publication barrier. Workers cannot observe this
		// claim until every ring and terminal lane has been reset.
		ch.slots[i]->state.store(claim, std::memory_order_release);
		ch.slots[i]->reservation.store(0, std::memory_order_release);
		return static_cast<int>(i);
	}
	return -1;
}
int vgi_wasm_slot_write(int region_offset, int slot, const uint8_t *d, int n) {
	auto &native_slot = slot_at(region_offset, slot);
	const int claim = native_slot.state.load(std::memory_order_acquire);
	if (claim == 0) {
		return -1;
	}
	return native_slot.c2w.write_all_claim_safe(d, n, native_slot.state, claim) == n ? n : -1;
}
void vgi_wasm_slot_write_eos(int region_offset, int slot) {
	auto &native_slot = slot_at(region_offset, slot);
	if (native_slot.state.load(std::memory_order_acquire) != 0) {
		native_slot.c2w_closed.store(duckdb::vgi::sab::kClosed, std::memory_order_release);
		native_slot.c2w.wake_all();
	}
}
int vgi_wasm_slot_read(int region_offset, int slot, uint8_t *d, int n) {
	auto &native_slot = slot_at(region_offset, slot);
	const int claim = native_slot.state.load(std::memory_order_acquire);
	if (claim == 0) {
		return -1;
	}
	int read =
	    native_slot.w2c.read_some_claim_safe(d, n, native_slot.state, claim, native_slot.w2c_closed_claim, claim, -1);
	if (read == 0 && native_slot.terminal_claim.load(std::memory_order_acquire) == claim) {
		return duckdb::vgi::sab::kSabTerminalTransportError;
	}
	return read;
}
int vgi_wasm_slot_terminal_error(int region_offset, int slot, int *code, int *detail) {
	auto &native_slot = slot_at(region_offset, slot);
	const int claim = native_slot.state.load(std::memory_order_acquire);
	const int terminal_claim = native_slot.terminal_claim.load(std::memory_order_acquire);
	if (claim == 0 || terminal_claim != claim) {
		return 0;
	}
	const int snapshot_code = native_slot.terminal_code.load(std::memory_order_relaxed);
	const int snapshot_detail = native_slot.terminal_detail.load(std::memory_order_relaxed);
	terminal_snapshot_gate.MaybePause(region_offset);
	// Release/reclaim may reset or replace the payload after our first token
	// check. Accept only a stable STATE + terminal-token snapshot.
	if (native_slot.state.load(std::memory_order_acquire) != claim ||
	    native_slot.terminal_claim.load(std::memory_order_acquire) != terminal_claim) {
		return 0;
	}
	if (code) {
		*code = snapshot_code;
	}
	if (detail) {
		*detail = snapshot_detail;
	}
	return 1;
}
void vgi_wasm_slot_release(int region_offset, int slot) {
	auto &native_slot = slot_at(region_offset, slot);
	native_slot.state.store(0, std::memory_order_release);
	// Wake blocked client and stale-worker operations so they observe release
	// rather than waiting forever on an abandoned ring.
	native_slot.c2w.wake_all();
	native_slot.w2c.wake_all();
}
void vgi_wasm_set_channel(int region_offset) {
	worker_region_offset = region_offset;
}

// ---- worker-side ops (the serve() end of a slot) ----
int vgi_sab_worker_read(int slot, uint8_t *d, int n) {
	auto &native_slot = slot_at(worker_region_offset, slot);
	const int served_claim = served_claim_for(slot);
	for (;;) {
		if (!AcquireWorkerReservation(native_slot, served_claim)) {
			return 0;
		}
		worker_operation_gate.MaybePause(worker_region_offset);
		if (native_slot.state.load(std::memory_order_acquire) != served_claim) {
			ReleaseWorkerReservation(native_slot);
			return 0;
		}
		const int result = native_slot.c2w.try_read(d, n, native_slot.c2w_closed, duckdb::vgi::sab::kClosed);
		ReleaseWorkerReservation(native_slot);
		if (result != duckdb::vgi::sab::kSabWouldBlock) {
			return result;
		}
		native_slot.c2w.WaitForChange();
	}
}
int vgi_sab_worker_write(int slot, const uint8_t *d, int n) {
	auto &native_slot = slot_at(worker_region_offset, slot);
	const int served_claim = served_claim_for(slot);
	int offset = 0;
	while (offset < n) {
		if (!AcquireWorkerReservation(native_slot, served_claim)) {
			return offset;
		}
		worker_operation_gate.MaybePause(worker_region_offset);
		if (native_slot.state.load(std::memory_order_acquire) != served_claim) {
			ReleaseWorkerReservation(native_slot);
			return offset;
		}
		const int written = native_slot.w2c.try_write(d + offset, n - offset);
		ReleaseWorkerReservation(native_slot);
		if (written == 0) {
			native_slot.w2c.WaitForChange();
			continue;
		}
		offset += written;
	}
	return n;
}
void vgi_sab_worker_close(int slot) {
	auto &native_slot = slot_at(worker_region_offset, slot);
	const int served_claim = served_claim_for(slot);
	if (!AcquireWorkerReservation(native_slot, served_claim)) {
		return;
	}
	native_slot.w2c_closed_claim.store(served_claim, std::memory_order_release);
	ReleaseWorkerReservation(native_slot);
	native_slot.w2c.wake_all();
}
// Test/adapter seam: publish claim-tokened terminal metadata and wake the
// client. A late failure after release/reclaim is ignored by the reader because
// terminal_claim no longer matches STATE.
void vgi_sab_native_worker_fail(int region_offset, int slot, int code, int detail) {
	auto &native_slot = slot_at(region_offset, slot);
	const int claim = native_slot.state.load(std::memory_order_acquire);
	native_slot.terminal_code.store(code, std::memory_order_relaxed);
	native_slot.terminal_detail.store(detail, std::memory_order_relaxed);
	native_slot.terminal_claim.store(claim, std::memory_order_release);
	native_slot.w2c_closed_claim.store(claim, std::memory_order_release);
	native_slot.w2c.wake_all();
}
int vgi_sab_native_slot_claim(int region_offset, int slot) {
	return slot_at(region_offset, slot).state.load(std::memory_order_acquire);
}
void vgi_sab_native_worker_fail_claim(int region_offset, int slot, int claim, int code, int detail) {
	auto &native_slot = slot_at(region_offset, slot);
	native_slot.terminal_code.store(code, std::memory_order_relaxed);
	native_slot.terminal_detail.store(detail, std::memory_order_relaxed);
	native_slot.terminal_claim.store(claim, std::memory_order_release);
	native_slot.w2c_closed_claim.store(claim, std::memory_order_release);
	native_slot.w2c.wake_all();
}
int vgi_sab_native_w2c_closed_claim(int region_offset, int slot) {
	return slot_at(region_offset, slot).w2c_closed_claim.load(std::memory_order_acquire);
}
int vgi_sab_native_slot_reservation(int region_offset, int slot) {
	return slot_at(region_offset, slot).reservation.load(std::memory_order_acquire);
}
int vgi_sab_native_slot_is_reset(int region_offset, int slot) {
	auto &native_slot = slot_at(region_offset, slot);
	return native_slot.c2w.is_reset() && native_slot.w2c.is_reset() &&
	       native_slot.c2w_closed.load(std::memory_order_acquire) == 0 &&
	       native_slot.w2c_closed_claim.load(std::memory_order_acquire) == 0 &&
	       native_slot.terminal_claim.load(std::memory_order_acquire) == 0 &&
	       native_slot.terminal_code.load(std::memory_order_relaxed) == 0 &&
	       native_slot.terminal_detail.load(std::memory_order_relaxed) == 0;
}
void vgi_sab_native_pause_open_before_publish(int region_offset) {
	open_publish_gate.Arm(region_offset);
}
int vgi_sab_native_wait_open_before_publish(int region_offset) {
	return open_publish_gate.WaitUntilReached(region_offset) ? 1 : 0;
}
void vgi_sab_native_resume_open_publish(int region_offset) {
	open_publish_gate.Resume(region_offset);
}
void vgi_sab_native_pause_terminal_snapshot(int region_offset) {
	terminal_snapshot_gate.Arm(region_offset);
}
int vgi_sab_native_wait_terminal_snapshot(int region_offset) {
	return terminal_snapshot_gate.WaitUntilReached(region_offset) ? 1 : 0;
}
void vgi_sab_native_resume_terminal_snapshot(int region_offset) {
	terminal_snapshot_gate.Resume(region_offset);
}
void vgi_sab_native_pause_worker_operation(int region_offset) {
	worker_operation_gate.Arm(region_offset);
}
int vgi_sab_native_wait_worker_operation(int region_offset) {
	return worker_operation_gate.WaitUntilReached(region_offset) ? 1 : 0;
}
void vgi_sab_native_resume_worker_operation(int region_offset) {
	worker_operation_gate.Resume(region_offset);
}
// Browser pthread-pool dispatcher hook — unused natively (the native tests drive a
// single slot via vgi_rust_serve_table_sab_slot directly, not the multi-thread
// pool). Present so sabtable's serve_slot_loop / serve_pool link in native builds.
void vgi_worker_await_slot(int slot) {
	served_claims[std::make_pair(worker_region_offset, slot)] =
	    slot_at(worker_region_offset, slot).state.load(std::memory_order_acquire);
}
void vgi_worker_await_release(int /*slot*/) {
}
}
