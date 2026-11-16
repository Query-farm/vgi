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
	HDR_FEATURES = 6,     // additive feature bits understood by both endpoints
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
	// 0 = open; the worker closes by storing its served claim id, and the client
	// sees EOS only when W2C_CLOSED == SLOT_STATE. A stale worker finishing after
	// the slot was released and reclaimed therefore cannot close the new claim's
	// stream. See docs/sab_transport_abi.md.
	W2C_CLOSED = 6,
	// Optional terminal transport error; valid only when SLOT_TERMINAL_CLAIM ==
	// SLOT_STATE (same stale-writer guard as W2C_CLOSED).
	SLOT_TERMINAL_CLAIM = 7,
	SLOT_TERMINAL_CODE = 8,
	SLOT_TERMINAL_DETAIL = 9,
	// Mutation ownership: slot_open CASes 0->claim, resets the slot while STATE is
	// still free, then release-publishes STATE; a worker CASes 0->served claim
	// around each ring mutation. Neither can move positions across a reuse.
	SLOT_RESERVATION = 10,
};
constexpr int32_t kSlotControlBytes = 64; // control block, cache-line isolated

// vgi_wasm_slot_read sentinel: ring empty after a bounded wait — poll cancellation + retry.
constexpr int32_t kSabWouldBlock = -2;
constexpr int32_t kSabTerminalTransportError = -3;

enum Feature : uint32_t {
	kFeatureTerminalTransportError = 1u << 0,
};
constexpr uint32_t kSupportedFeatures = kFeatureTerminalTransportError;

// SLOT_STATE: 0 = free, else a unique claim id from HDR_CLAIM_SEQ. A constant
// "claimed" value would make release+reclaim an ABA race that wedges the worker
// dispatcher. kSlotClaimed is for the native single-serve harness only.
constexpr int32_t kSlotFree = 0;
constexpr int32_t kSlotClaimed = 1;
constexpr int32_t kOpen = 0;
constexpr int32_t kClosed = 1;

// Bytes per direction; affects throughput only (the ring chunks the stream).
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
int vgi_wasm_ensure_worker(const char *location, int region_offset);
// Claim a free slot (fresh claim id, rings reset). >=0 slot id, or <0 on exhaustion.
int vgi_wasm_slot_open(const char *location, int region_offset);
// Blocking write of all n bytes into the slot's c2w ring. Returns n, or <0 on cancel.
int vgi_wasm_slot_write(int region_offset, int slot, const uint8_t *data, int n);
// Signal EOS on the c2w (input) ring; ends the worker's serve loop (teardown only).
void vgi_wasm_slot_write_eos(int region_offset, int slot);
// Read up to n bytes from the w2c ring. >0 = bytes read, 0 = EOS (worker closed +
// drained), kSabWouldBlock = empty after a bounded wait (poll query cancellation and
// retry, so a blocked read can't stall DuckDB's cancel busy-wait),
// kSabTerminalTransportError = see vgi_wasm_slot_terminal_error; other <0 = hard error.
int vgi_wasm_slot_read(int region_offset, int slot, uint8_t *data, int n);
// Read claim-safe metadata after kSabTerminalTransportError. Returns 1 only
// when metadata belongs to the slot's current claim, otherwise 0.
int vgi_wasm_slot_terminal_error(int region_offset, int slot, int *code, int *detail);
// Release the slot (state -> 0). Idempotent.
void vgi_wasm_slot_release(int region_offset, int slot);
// Push the channel's byte offset in linear memory onto the calling pthread's JS
// runtime (the slot stubs read the channel via Module.HEAP at this offset). C++
// calls this per connection with the shared channel offset. wasm build only; the
// native test uses a static in-process channel and does not call it.
void vgi_wasm_set_channel(int channel_offset);
}
