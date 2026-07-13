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
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace {

// One SPSC byte ring (one direction), monotonic positions, blocking flow control.
struct NativeRing {
	std::mutex m;
	std::condition_variable cv;
	int64_t write_pos = 0;
	int64_t read_pos = 0;
	bool closed = false;
	std::vector<uint8_t> buf;
	int cap;

	explicit NativeRing(int c) : buf(static_cast<size_t>(c)), cap(c) {
	}

	void write_all(const uint8_t *data, int n) {
		std::unique_lock<std::mutex> lk(m);
		int off = 0;
		while (off < n) {
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
	}

	int read_some(uint8_t *out, int n) {
		std::unique_lock<std::mutex> lk(m);
		for (;;) {
			int avail = static_cast<int>(write_pos - read_pos);
			if (avail == 0) {
				if (closed) {
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

	void close_ring() {
		std::unique_lock<std::mutex> lk(m);
		closed = true;
		cv.notify_all();
	}

	void reset() {
		std::unique_lock<std::mutex> lk(m);
		write_pos = 0;
		read_pos = 0;
		closed = false;
	}
};

struct NativeSlot {
	std::atomic<int> state{0}; // 0 free, 1 claimed
	NativeRing c2w;
	NativeRing w2c;
	explicit NativeSlot(int cap) : c2w(cap), w2c(cap) {
	}
};

struct NativeChannel {
	std::vector<std::unique_ptr<NativeSlot>> slots;
	NativeChannel(int n, int cap) {
		for (int i = 0; i < n; i++) {
			slots.push_back(std::make_unique<NativeSlot>(cap));
		}
	}
};

NativeChannel &channel() {
	static NativeChannel ch(16, duckdb::vgi::sab::kDefaultRingCap);
	return ch;
}

NativeSlot &slot_at(int slot) {
	return *channel().slots[static_cast<size_t>(slot)];
}

} // namespace

extern "C" {

// ---- client stubs (vgi_sab_abi.hpp) ----
int vgi_wasm_ensure_worker(const char *) {
	return 0;
}
int vgi_wasm_slot_open(const char *) {
	auto &ch = channel();
	for (size_t i = 0; i < ch.slots.size(); i++) {
		int expected = 0;
		if (ch.slots[i]->state.compare_exchange_strong(expected, 1)) {
			ch.slots[i]->c2w.reset();
			ch.slots[i]->w2c.reset();
			return static_cast<int>(i);
		}
	}
	return -1;
}
int vgi_wasm_slot_write(int slot, const uint8_t *d, int n) {
	slot_at(slot).c2w.write_all(d, n);
	return n;
}
void vgi_wasm_slot_write_eos(int slot) {
	slot_at(slot).c2w.close_ring();
}
int vgi_wasm_slot_read(int slot, uint8_t *d, int n) {
	return slot_at(slot).w2c.read_some(d, n);
}
void vgi_wasm_slot_release(int slot) {
	slot_at(slot).state.store(0);
}

// ---- worker-side ops (the serve() end of a slot) ----
int vgi_sab_worker_read(int slot, uint8_t *d, int n) {
	return slot_at(slot).c2w.read_some(d, n);
}
int vgi_sab_worker_write(int slot, const uint8_t *d, int n) {
	slot_at(slot).w2c.write_all(d, n);
	return n;
}
void vgi_sab_worker_close(int slot) {
	slot_at(slot).w2c.close_ring();
}
}
