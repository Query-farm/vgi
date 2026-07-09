# On-Disk Compression for the VGI Result Cache — Strategy

Status: **proposal** (not yet implemented). Author: cache maintainers.

## 1. Motivation

The disk-tier blobs (`objects/<sha>.vrc`) store **uncompressed** Arrow IPC — each cached
batch is a self-contained IPC stream (`SerializeRecordBatch`) concatenated into the blob.
Arrow IPC does not compress by default, so the disk footprint is the raw column bytes plus
framing.

Measured zstd-3 on representative per-batch Arrow IPC (100 batches × 24 000 rows, **distinct
data per batch** so there is no artificial cross-batch dedup):

| data shape | raw | per-batch zstd | ratio | whole-blob zstd | ratio | decompress |
|---|--:|--:|--:|--:|--:|--:|
| `seq int64` (fixture-like) | 19.2 MB | 2.48 MB | **7.7×** | 3.46 MB | 5.6× | 1.2 GB/s |
| `random int64` (worst case) | 19.2 MB | 19.2 MB | **1.0×** | 19.2 MB | 1.0× | 27 GB/s |
| `mixed` (string cat / float / int id) | 52.8 MB | 18.6 MB | **2.85×** | 12.9 MB | 4.10× | 1.2 GB/s |

Compress throughput was ~5 GB/s. So for realistic analytical output (strings, low-cardinality
categoricals, timestamps, monotonic ids) we save **~2–4×** on disk; random/opaque data saves
nothing.

**Why it's worth doing (optionally):**
- A disk cache under a fixed `vgi_result_cache_disk_max_bytes` holds **2–4× more** results.
- On a slow/networked disk, reading 2–4× fewer bytes can *outrun* the raw read even after the
  ~1 GB/s decompress cost.
- Cross-restart / cross-process warm cache stores more history in the same space.

**Why it must be optional (default off):**
- Random/incompressible data pays compress+decompress CPU for ~0 benefit.
- The uncompressed path has zero added latency; some deployments are CPU-bound, not disk-bound.

## 2. Hard constraint from the current architecture: keep per-batch random access

The **streaming disk serve** (S8, for results larger than RAM) is a positioned `pread` of each
batch's bytes on demand — `CachedReplayConnection` reads `disk_ipc_length` bytes at
`disk_ipc_offset` and decodes one batch at a time (`vgi_cached_replay_connection.cpp`;
`LoadFromDiskStreaming` in `vgi_result_cache.cpp` records the per-batch TOC). This is what keeps
serve RAM flat for a >RAM result.

A single **whole-blob** zstd stream (the "cross-batch, more optimal" option) **cannot be
positioned into** — you'd have to decompress from the start to reach batch N. That breaks the
flat-RAM streaming serve. The measured whole-blob win (4.10× vs 2.85× on mixed data — ~44%
better) is real but **not worth losing the streaming serve**.

→ **Compress per batch.** Each batch's IPC bytes are an independent zstd frame; the positioned
read fetches the compressed frame and decompresses just that batch. Random access preserved.

(A zstd **dictionary** recovers most of the cross-batch ratio while keeping per-batch access —
see §6, a future enhancement.)

## 3. Codec: zstd

- **Already linked** (`libzstd.a` via vcpkg — visible in the link line). No new dependency.
- Fast, tunable level, strong ratio, dictionary support. Recommend **zstd level 3** default
  (the sweet spot above), configurable.
- **vs Arrow IPC's built-in codec** (`IpcWriteOptions.codec = ZSTD/LZ4`): Arrow compresses
  per-*buffer* within a batch, transparently on read. But (a) it lives at *serialize* time, so
  it would also compress the **memory tier** (we want disk-only), and (b) it has the same
  per-batch limitation. Applying zstd directly to the already-serialized IPC bytes at the
  disk-write boundary is **disk-only**, needs no Arrow re-encode, and gives us level/dictionary
  control. LZ4 remains an option for a "fast" profile.

## 4. Recommended design — per-batch zstd, disk-only, self-describing

**Setting** (session-scoped, read per-query at InitGlobal like the other cache settings; frozen
per capture):

```
vgi_result_cache_disk_compression = 'none' (default) | 'zstd' | 'zstd:<level>'   [| 'lz4' later]
```

**Blob format** — self-describing via the magic, so a reader auto-detects regardless of the
current setting:
- `VRC1` = uncompressed (today's format), `VRCZ` = per-batch zstd.
- Per batch layout is unchanged **except** the stored `ipc_len`/payload is now the **compressed
  frame**. zstd frames embed the decompressed size (`ZSTD_getFrameContentSize`), so **no new
  field** is needed — the TOC's per-batch length is simply the compressed length, and
  `disk_ipc_offset/length` point at the compressed frame.

**Capture** (`VgiCaptureDiskWriter::AppendBatch` and `DrainSubstreamToWriter`, plus the RAM→disk
`PersistToDisk` for small memory entries):
- `ZSTD_compress(ipc_bytes, level)` → write the frame; feed the **compressed** bytes to the
  incremental SHA (content-addressing over stored bytes — deterministic per codec+level).
- Optional refinement: if `compressed_len >= raw_len` (incompressible batch), store raw and set
  a per-batch flag — avoids the rare expansion. v1 can skip this (zstd expansion is <0.1%).

**Serve**:
- `CachedReplayConnection` (streaming): if `entry.codec == zstd`, positioned-read the compressed
  frame → `ZSTD_decompress` → `arrow::io::BufferReader` → decode. One decompress per batch; RAM
  stays one-batch (decompressed) — same as today.
- `LoadFromDisk` (materialized, ≤`max_entry_bytes`): decompress each batch on load; the (D)
  integrity re-hash runs over the **compressed** blob bytes (unchanged); the streaming path still
  skips re-hash.

**Untouched:** the cache key, eligibility, never-partial invariant, the reaper, identity
sharding, the ref format (a `codec=` field can be added for diagnostics but the magic is
authoritative). The **memory tier stays uncompressed** (disk-only compression, per the ask).

**Format coexistence / migration:** none needed. A dir can hold `VRC1` and `VRCZ` blobs at once;
the reader always supports both. Turning the setting **off** still serves previously-written
`VRCZ` blobs. A key re-stored under a different codec yields a different `content_sha` → a new
blob; the old one orphans out via the reaper.

## 5. Testing plan (when implemented)

- **Round-trip byte-identical**: a `VRCZ` entry serves identical rows/order to the live result on
  BOTH the materialized and streaming paths, **across the spill boundary**, including the
  `batch_index` reorder (reuse `cache_interleaved`) and wide/nested types (STRUCT/LIST/MAP).
- **Incompressible data**: correct + graceful (~1×, no expansion corruption).
- **Mixed-format dir**: hand-write / produce both `VRC1` and `VRCZ` blobs; both serve; toggle the
  setting off and confirm `VRCZ` still reads.
- **Observability**: `vgi_result_cache()` (or the ref) exposes compressed vs logical bytes and the
  codec; `glob()` on-disk size < raw.
- **Perf sanity** (bench mode): capture+serve wall-time and disk bytes with/without compression on
  compressible vs random data.

## 6. Future enhancement — zstd dictionary for cross-batch ratio

Per-batch compression forfeits cross-batch redundancy (the 2.85× vs 4.10× gap on low-cardinality
categoricals). A **zstd dictionary** trained on the first K batches (`ZDICT_trainFromBuffer`),
stored once in the blob header and used to compress/decompress every batch
(`ZSTD_compress_usingDict`), recovers most of that gap **while preserving per-batch random
access**. Costs: dict training at capture, a header section, dict load at serve. Ship plain
per-batch first; add the dictionary if the ratio gap proves material on real workloads.

## 7. Effort

Small–medium (~1 day). Touch points: one setting; `AppendBatch` / `DrainSubstreamToWriter` /
`SerializeEntryBlob`(+`PersistToDisk`) on write; `LoadFromDisk` / `LoadFromDiskStreaming` /
`CachedReplayConnection` on read; a `codec` field on the entry. No change to keys, eligibility,
never-partial, or the reaper. zstd one-shot API (`ZSTD_compress`/`ZSTD_decompress`/
`ZSTD_getFrameContentSize`) is trivial to wire.
```
