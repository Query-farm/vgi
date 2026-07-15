// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
//
// VGI `worker:` SAB transport — shared ABI (see docs/sab_transport_abi.md).
// Byte layout + stub contract for the duplex streaming byte channel. Consumed by
// both the wasm build (stubs via --js-library) and the native test harness (stubs
// via POSIX shm + futex). The Rust worker mirrors these constants in sab_abi.rs.
#include <cstdint>

namespace duckdb {
namespace vgi {
namespace sab {

// ---- Channel / region header (i32 lane indices) --------------------------
constexpr uint32_t kMagic = 0x42534756u; // 'VGSB' LE
constexpr uint32_t kVersion = 1;

enum HeaderLane : int32_t {
	HDR_MAGIC = 0,
	HDR_VERSION = 1,
	HDR_N_SLOTS = 2,
	HDR_RING_CAP = 3,     // bytes per ring (per direction)
	HDR_SLOT_STRIDE = 4,  // bytes per slot
	HDR_SLOTS_OFF = 5,    // byte offset of slot[0]
	// lane 7 = ensure-worker ready flag (client-side, js-stubs.js)
	HDR_CLAIM_SEQ = 8,    // monotonic global claim-id counter (Atomics.add on slot_open)
};
constexpr int32_t kHeaderBytes = 64;

// ---- Per-slot control (i32 lane indices within the slot's control block) --
enum SlotLane : int32_t {
	SLOT_STATE = 0,         // 0 = free, nonzero = unique claim id (see below)
	C2W_WRITE_POS = 1,      // client -> worker ring
	C2W_READ_POS = 2,
	C2W_CLOSED = 3,
	W2C_WRITE_POS = 4,      // worker -> client ring
	W2C_READ_POS = 5,
	W2C_CLOSED = 6,
};
constexpr int32_t kSlotControlBytes = 64; // control block, cache-line isolated

// vgi_wasm_slot_read sentinel: ring empty after a bounded wait — poll cancellation + retry.
constexpr int32_t kSabWouldBlock = -2;

// state values. STATE=0 is free; a claim stores a UNIQUE nonzero id (from the
// HDR_CLAIM_SEQ counter) rather than a constant 1, so the per-slot worker
// dispatcher can tell "still my claim" from "already reclaimed by the next scan"
// (STATE 1->0->1 would otherwise be an ABA race that wedges await_release). The
// native harness uses a single serve per slot (await is a no-op there) and may
// still store 1. kSlotClaimed is retained for that native path.
constexpr int32_t kSlotFree = 0;
constexpr int32_t kSlotClaimed = 1;
// closed flag
constexpr int32_t kOpen = 0;
constexpr int32_t kClosed = 1;

// Default ring capacity per direction (bytes). Tunable; the ring is the chunker,
// so correctness does not depend on it — only throughput.
constexpr int32_t kDefaultRingCap = 64 * 1024;

inline int32_t SlotStride(int32_t ring_cap) {
	int32_t raw = kSlotControlBytes + 2 * ring_cap;
	return (raw + 63) & ~63; // align up to 64
}

} // namespace sab
} // namespace vgi
} // namespace duckdb

// ---- Stub contract (the C++ <-> backend seam) ----------------------------
// Native harness and the wasm build each provide an implementation. See
// docs/sab_transport_abi.md for full semantics.
extern "C" {
// Ensure the worker for `location` exists and is wired to the channel. 0 = ok.
int vgi_wasm_ensure_worker(const char *location);
// Claim a free slot (CAS state 0->1, reset rings). >=0 slot id, or <0 on exhaustion.
int vgi_wasm_slot_open(const char *location);
// Blocking write of all n bytes into the slot's c2w ring. Returns n, or <0 on cancel.
int vgi_wasm_slot_write(int slot, const uint8_t *data, int n);
// Signal EOS on the c2w (input) ring (CloseInputWriter).
void vgi_wasm_slot_write_eos(int slot);
// Read up to n bytes from the w2c ring. >0 = bytes read, 0 = EOS (worker closed +
// drained). Two negative sentinels: VGI_SAB_WOULD_BLOCK (-2) = the ring was empty for a
// bounded wait (the caller must poll query-cancellation and retry, so an in-flight
// prefetch read on a DuckDB task thread doesn't block DuckDB's error/cancel busy-wait
// forever — see docs/sab_transport_abi.md "Cancellation"); any other <0 = hard error.
int vgi_wasm_slot_read(int slot, uint8_t *data, int n);
// Release the slot (state -> 0). Idempotent.
void vgi_wasm_slot_release(int slot);
// Push the channel's byte offset in linear memory onto the calling pthread's JS
// runtime (the slot stubs read the channel via Module.HEAP at this offset). C++
// calls this per connection with the shared channel offset. wasm build only; the
// native test uses a static in-process channel and does not call it.
void vgi_wasm_set_channel(int channel_offset);
}
