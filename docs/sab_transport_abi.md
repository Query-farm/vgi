# VGI `worker:` SAB transport — ABI contract

Shared source of truth across the three repos (C++ extension `vgi`, worker `vgi-rust`,
host glue `haybarn-wasm`). Pin this first; all three build against it independently.

The transport is a **duplex streaming byte channel per slot** — two single-producer/
single-consumer (SPSC) byte rings with blocking flow control. In the browser the rings
live in DuckDB's wasm linear memory and block via `Atomics.wait`/`notify`; in the native
test harness they live in a POSIX shared-memory segment and block via a futex/condvar.
Both back-ends satisfy the **same byte layout and the same stub semantics** below.

## Channel layout (little-endian; i32 lanes are Atomics-addressable)

```
Channel = [ header(64B) | slot[0] | slot[1] | … | slot[N-1] ]

header (i32 lanes):
  [0] magic          = 0x42534756  ('VGSB' LE)
  [1] version        = 1
  [2] n_slots        = N
  [3] ring_cap       = bytes per ring (per direction)
  [4] slot_stride    = bytes per slot
  [5] slots_off      = byte offset of slot[0]  (= 64)
  [6..15] reserved

slot (at slots_off + i*slot_stride):
  control (i32 lanes, first 64B, cache-line isolated):
    [0] state          0 = free, 1 = claimed        (slot_open CAS target)
    [1] c2w_write_pos  monotonic bytes written (client → worker)
    [2] c2w_read_pos   monotonic bytes read
    [3] c2w_closed     1 = client finished writing (EOS on input)
    [4] w2c_write_pos  monotonic (worker → client)
    [5] w2c_read_pos
    [6] w2c_closed     1 = worker finished streaming (EOS on output)
    [7..15] reserved
  c2w_data: ring_cap bytes   (byte offset control+64)
  w2c_data: ring_cap bytes   (byte offset control+64+ring_cap)

slot_stride = align_up(64 + 2*ring_cap, 64)
```

**SPSC ring semantics** (per direction): positions are **monotonic** (never wrap; index =
`pos % ring_cap`), so `avail = write_pos - read_pos`, `free = ring_cap - avail`. A writer
that finds `free == 0` blocks on `read_pos` (waits for the reader to advance); a reader that
finds `avail == 0` blocks on `write_pos` (unless `*_closed == 1`, then EOS). Each side
`notify`s the word it advanced. This is the SAB analog of an OS pipe buffer — the ring is
the chunker, so payloads larger than `ring_cap` stream through in bounded pieces.

**Multi-worker (Phase 2):** a directory/worker-registry (`url_hash → region offset`) is
prepended so several worker modules share one channel with disjoint slot regions. v1 / the
native harness uses a single region of `n_slots`; the header above is the region header.

## Stub contract (`extern "C"`, the C++↔backend seam)

The C++ `SabInputStream`/`SabOutputStream` and the connection call these. The wasm build
resolves them against `--js-library` (Atomics over linear memory); the native test resolves
them against a POSIX-shm + futex implementation. Same signatures, same semantics.

```c
// Ensure the worker for `location` exists and its region is wired to the channel.
// Returns 0 on success; negative on whitelist-reject / spawn failure.
int  vgi_wasm_ensure_worker(const char *location);

// Claim a free slot in the worker's region (CAS state 0->1). Resets both rings.
// Returns slot id >= 0, or negative on exhaustion.
int  vgi_wasm_slot_open(const char *location);

// Blocking write of all n bytes into the slot's c2w ring (blocks on backpressure).
// Returns n on success; negative on cancel/error.
int  vgi_wasm_slot_write(int slot, const uint8_t *data, int n);

// Signal EOS on the c2w (input) ring — the client is done writing (CloseInputWriter).
void vgi_wasm_slot_write_eos(int slot);

// Blocking read of up to n bytes from the slot's w2c ring.
// Returns bytes read (>0); 0 on EOS (w2c_closed && drained); negative on cancel/error.
int  vgi_wasm_slot_read(int slot, uint8_t *data, int n);

// Release the slot (state -> 0) once the connection is fully done. Idempotent.
void vgi_wasm_slot_release(int slot);
```

### Mapping to `IFunctionConnection`

| Connection op | Stub calls |
|---|---|
| `OpenInputWriter` | (slot already open from bind/init) — no-op or first tick |
| `WriteInputBatch(b)` | serialize `b` to Arrow IPC, `vgi_wasm_slot_write(slot, …)` |
| `CloseInputWriter` | `vgi_wasm_slot_write_eos(slot)` |
| `ReadDataBatch` | `vgi_wasm_slot_read(slot, …)` into the Arrow IPC `StreamReader`; 0 ⇒ EOS ⇒ return null |
| connection dtor / release | `vgi_wasm_slot_release(slot)` |

Errors surface **in-band** as Arrow error batches (reuse `ClassifyBatch`/
`HandleBatchLogMessage`), never as a transport error code. Cancellation: `slot_write`/
`slot_read` block with a bounded timeout and re-check `context.client.interrupted`
(adaptive backoff), mirroring `FdInputStream::Read`.

## Rust worker side

`serve_sab` (in `vgi/src/transport.rs`) is thread-per-slot: each thread parks on its slot's
`state`, wraps the slot as a `SabStream: std::io::Read + Write` (reads w2c-side input via the
c2w ring, writes output via w2c) and calls `server.serve(&mut r, &mut w)` **unchanged**. The
Rust `SabStream` blocks via the same ring rule; natively it futex-waits on the shm segment,
in wasm it calls a JS import (`Atomics.wait` over the foreign buffer).
