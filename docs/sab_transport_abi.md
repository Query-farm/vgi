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
  [6] features       additive feature bitmap (bit 0 = claim-safe terminal transport error)
  [7] ensure-worker ready flag (client-side dedup; 1 = worker booted)
  [8] claim_seq      monotonic global claim-id counter (Atomics.add on slot_open)
  [9..15] reserved

slot (at slots_off + i*slot_stride):
  control (i32 lanes, first 64B, cache-line isolated):
    [0] state          0 = free, nonzero = unique claim id  (slot_open CAS target)
    [1] c2w_write_pos  monotonic bytes written (client → worker)
    [2] c2w_read_pos   monotonic bytes read
    [3] c2w_closed     1 = client finished writing (EOS on input)
    [4] w2c_write_pos  monotonic (worker → client)
    [5] w2c_read_pos
    [6] w2c_closed     0 = open; else the CLOSING WORKER's claim id (a token, not a bare 1)
    [7] terminal_claim  claim id that owns terminal transport metadata
    [8] terminal_code   adapter-defined stable error category
    [9] terminal_detail adapter-defined bounded detail value
    [10..15] reserved
  c2w_data: ring_cap bytes   (byte offset control+64)
  w2c_data: ring_cap bytes   (byte offset control+64+ring_cap)

slot_stride = align_up(64 + 2*ring_cap, 64)
```

**`state` carries a UNIQUE claim id, not a constant 1.** `slot_open` writes
`Atomics.add(header[claim_seq], 1) + 1` (a globally-unique nonzero id) into the slot `state`,
not `1`. The worker's per-slot dispatcher records the id it is serving and, after serving,
waits for `state` to leave *that* id before accepting the next claim. A constant `1` would make
a release+immediate-reclaim (`state 1 → 0 → 1`) an **ABA race** — the worker would read the new
`1` as "my claim not released yet" and block forever. Unique ids make `state != served_id` true
for both the free state and any fresh reclaim, so the handoff is unambiguous. (The native single-
serve harness never reuses a slot mid-dispatch, so it may still store `1`.)

**`w2c_closed` is a claim-id TOKEN, and the worker BAILS from a blocked ring op when its slot
is reclaimed.** These two rules make reusing a not-yet-drained slot race-free. On the error
path the client frees the slot (`state → 0`) *before* the worker's serve finishes — it cannot
drain `w2c` during the C++ exception unwind because the query is already interrupted, so reads
bail. A new scan may therefore reclaim + reset the slot while the old worker is still finishing.
Two guards close the race: **(a)** the worker closes `w2c` by storing *its own* `served` claim
id in `w2c_closed` (not `1`), and a reader treats `w2c` as EOS only when `w2c_closed == state`
(its current claim) — so a stale worker's late close carries the *old* id and the new client
ignores it (no phantom `Stream header EOF (no schema)`); **(b)** the worker's blocking ring
read/write re-checks `state == served` after each bounded wait and, if the slot was
released/reclaimed, ends the serve (read → EOS, write → abort) — so a reclaimed slot's serve
thread can't wedge on a ring the client abandoned (which would leak the slot and later starve
scans). The native single-serve harness stores `1` and never reuses a slot mid-dispatch, so
`w2c_closed == state` reduces to `1 == 1` there.

**SPSC ring semantics** (per direction): positions are **monotonic** (never wrap; index =
`pos % ring_cap`), so `avail = write_pos - read_pos`, `free = ring_cap - avail`. A writer
that finds `free == 0` blocks on `read_pos` (waits for the reader to advance); a reader that
finds `avail == 0` blocks on `write_pos` (EOS when `c2w_closed == 1` for the worker reading
input, or `w2c_closed == state` for the client reading output — the claim-id token above). Each side
`notify`s the word it advanced. This is the SAB analog of an OS pipe buffer — the ring is
the chunker, so payloads larger than `ring_cap` stream through in bounded pieces.

**Multi-target adapters:** every canonical target owns a disjoint region with the exact v1
layout above. The region byte offset is carried on every client-side stub call; there is no
new directory in shared memory and therefore no ABI-version bump. Page glue may register many
target/offset pairs with one transport-adapter worker. Region reclamation is permitted only
when every slot `state` is zero. A plain `worker:` URL remains a one-target adapter and receives
the original `vgi-init` boot message.

An `iroh://<64-lowercase-hex-EndpointId>` location is a browser adapter target,
not a worker-script URL. Haybarn maps every such target to one application-owned
Iroh adapter Worker. The adapter owns one local Iroh endpoint identity and
registers a distinct target/offset region for each remote EndpointId; it must not
create a new local endpoint identity per target.

The default client allocation cap is 32 regions and may be changed at compile time with
`VGI_SAB_MAX_TARGET_REGIONS`; the page bridge independently allows a host to choose a lower
`maxTargetsPerAdapter`. At the default four slots and 64 KiB per directional ring, one region is
524,608 bytes (64-byte header + 4 × 131,136-byte slot), so the default worst-case allocation is
16,787,456 bytes.

If feature bit 0 is set, an adapter may terminate a claim by writing code/detail, publishing
`terminal_claim`, then closing `w2c` with the same claim token. The reader returns `-3` only
when both tokens still match the current slot `state`; a late failure from a released claim is
ignored after slot reuse.

## Stub contract (`extern "C"`, the C++↔backend seam)

The C++ `SabInputStream`/`SabOutputStream` and the connection call these. The wasm build
resolves them against `--js-library` (Atomics over linear memory); the native test resolves
them against a POSIX-shm + futex implementation. Same signatures, same semantics.

```c
// Ensure the worker for `location` exists and its region is wired to the channel.
// Returns 0 on success; negative on whitelist-reject / spawn failure.
int  vgi_wasm_ensure_worker(const char *location, int region_offset);

// Claim a free slot in the worker's region (CAS state 0->1). Resets both rings.
// Returns slot id >= 0, or negative on exhaustion.
int  vgi_wasm_slot_open(const char *location, int region_offset);

// Blocking write of all n bytes into the slot's c2w ring (blocks on backpressure).
// Returns n on success; negative on cancel/error.
int  vgi_wasm_slot_write(int region_offset, int slot, const uint8_t *data, int n);

// Signal EOS on the c2w (input) ring — the client is done writing (CloseInputWriter).
void vgi_wasm_slot_write_eos(int region_offset, int slot);

// Blocking read of up to n bytes from the slot's w2c ring.
// Returns bytes read (>0); 0 on EOS (w2c_closed && drained); negative on cancel/error.
int  vgi_wasm_slot_read(int region_offset, int slot, uint8_t *data, int n);

// Read terminal metadata after -3. Returns 1 iff it belongs to the current claim.
int  vgi_wasm_slot_terminal_error(int region_offset, int slot, int *code, int *detail);

// Release the slot (state -> 0) once the connection is fully done. Idempotent.
void vgi_wasm_slot_release(int region_offset, int slot);
```

### Mapping to `IFunctionConnection`

| Connection op | Stub calls |
|---|---|
| `OpenInputWriter` | (slot already open from bind/init) — no-op or first tick |
| `WriteInputBatch(b)` | serialize `b` to Arrow IPC, `vgi_wasm_slot_write(region, slot, …)` |
| `CloseInputWriter` | `vgi_wasm_slot_write_eos(region, slot)` |
| `ReadDataBatch` | `vgi_wasm_slot_read(region, slot, …)` into the Arrow IPC `StreamReader`; 0 ⇒ EOS ⇒ return null |
| connection dtor / release | `vgi_wasm_slot_release(region, slot)` |

Application errors surface **in-band** as Arrow error batches (reuse `ClassifyBatch`/
`HandleBatchLogMessage`). Adapters that negotiate feature bit 0 may additionally report a
terminal transport failure via `-3` plus claim-safe metadata. Cancellation: `slot_write`/
`slot_read` block with a bounded timeout and re-check `context.client.interrupted`
(adaptive backoff), mirroring `FdInputStream::Read`.

## Browser implementation notes (load-bearing — non-obvious)

Two emscripten-pthread footguns must be handled or the transport hangs **intermittently**
(depending on which pool pthread the scheduler runs a scan on — so it passes in single-thread /
native tests and flakes only in the real threaded browser):

1. **Read the LIVE memory buffer, never the cached `Module.HEAPU8` view.** emscripten's cached
   `Module.HEAP*` typed-array views go **stale** on a pthread that was mid-compute/blocked through
   a memory-growth event — its `Atomics.load` then never observes another thread's stores into the
   *current* buffer, so a ring read sees `avail == 0` forever even though the bytes are there (a
   native C++ pointer, always aliasing live memory, sees them — so the two sides silently diverge).
   The JS ring stubs must derive their views from `wasmMemory.buffer` (a live getter) on every op,
   not `Module.HEAPU8.buffer`. This was the primary cause of the early flaky hangs.

2. **Carry the region offset on every operation.** Per-realm mutable JS state is not a safe client
   selector: DuckDB can schedule ring I/O onto a different pool pthread. The ABI therefore passes
   `region_offset` explicitly to open/read/write/close/release. `vgi_wasm_set_channel` remains only
   as an ABI-v1 worker-side compatibility hook.

The native (POSIX-shm) harness has neither issue (native pointers + a single serve thread), so
**both must be covered by the browser E2E** (`test/support/wasm-worker/browser-e2e/`), not the
native `[sab-conn]` unit tests.

## Rust worker side

`serve_sab` (in `vgi/src/transport.rs`) is thread-per-slot: each thread parks on its slot's
`state`, wraps the slot as a `SabStream: std::io::Read + Write` (reads w2c-side input via the
c2w ring, writes output via w2c) and calls `server.serve(&mut r, &mut w)` **unchanged**. The
Rust `SabStream` blocks via the same ring rule; natively it futex-waits on the shm segment,
in wasm it calls a JS import (`Atomics.wait` over the foreign buffer).
