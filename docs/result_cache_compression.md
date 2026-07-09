# On-Disk Compression for the VGI Result Cache — Strategy

Status: **analysis complete → recommended for implementation** (Arrow built-in IPC codec,
disk-only, `zstd:1` default). Author: cache maintainers.

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

**Default ON when the disk tier is enabled, but overridable:**
- The disk tier is opt-in already (it exists to persist large / cross-restart results), and for
  that use case denser storage is almost always wanted — so `zstd:1` is the sensible default the
  moment someone turns disk on.
- Kept optional (`= none`, or `= lz4` for a lower-CPU profile) because random/incompressible data
  pays compress+decompress CPU for ~0 benefit, and some deployments are CPU-bound, not disk-bound.
- The **memory-only** tier is never compressed — zero added latency on the hot path.

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

→ **Compress per batch.** Each batch stays an independent, self-contained compressed unit; the
positioned read fetches just that batch's bytes and decompresses only it. Random access preserved.

## 3. Codec: Arrow's built-in IPC compression (`IpcWriteOptions.codec`)

**Decision (revised): use Arrow's built-in IPC buffer compression, not a hand-rolled zstd frame
around the serialized bytes.** Arrow's `RecordBatchStreamWriter` compresses each column *buffer*
inside the batch message and records the codec in the message's `BodyCompression` metadata; the
reader (`RecordBatchStreamReader`) **decompresses transparently** — it detects the codec from the
message itself.

Why this beats compressing the IPC bytes ourselves (the previous draft's plan):
- **Zero custom read code.** `LoadFromDisk`, `LoadFromDiskStreaming`, and
  `CachedReplayConnection` already decode each batch through `RecordBatchStreamReader`. Because
  the codec rides in the message, **all three keep working unchanged** — no decompress call, no
  branch. A hand-rolled frame needs a decompress step wired into every one of those read sites.
- **Seek is preserved for free.** Each batch is still one self-contained IPC stream; the TOC's
  `disk_ipc_offset`/`disk_ipc_length` bracket that (now smaller) stream exactly as today. The
  S8 flat-RAM streaming serve is untouched — this is precisely the "keep the ability to seek
  between batches" property, and Arrow gives it to us without us maintaining a framing format.
- **Self-describing ⇒ toggle-safe, no format version.** A batch says how it's compressed, so a
  blob written under `zstd` still reads after the setting is flipped to `lz4` or `none`, and a
  dir can hold a mix. The `VRC1` magic and the per-batch TOC are **unchanged** (the payload is
  just a compressed IPC stream instead of an uncompressed one) — no `VRCZ` variant needed.
- **No new dependency.** The extension's Arrow is already built with `ARROW_WITH_ZSTD` **and**
  `ARROW_WITH_LZ4` (confirmed in `arrow/util/config.h`), so `arrow::util::Codec::Create(ZSTD|
  LZ4_FRAME)` is available today. Arrow's IPC path only accepts these two codecs.

The one property we give up vs. a hand-rolled codec is a **cross-batch zstd dictionary** (§6):
Arrow's IPC compression is per-buffer with no shared dictionary. Given the ask ("don't spend much
CPU, compression not super high"), the dictionary's marginal ratio gain is explicitly not worth
its cost — so foregoing it is the *right* trade, not a regret.

**Keeping it disk-only.** The stored `cb.ipc` (the memory tier) stays **uncompressed** so hot
memory hits pay zero decompress. Compression is applied at the **disk-write boundary** by
transcoding each batch — decode the uncompressed `cb.ipc` and re-emit it with
`IpcWriteOptions.codec` set (see §4). This resolves the only real objection to Arrow's codec
(that it normally acts at serialize time and would compress memory too): we serialize for memory
uncompressed, and re-serialize for disk compressed.

**Level — keep it cheap.** Default **`zstd` level 1**: on the mixed workload above it lands near
the zstd-3 ratio (~2.5–2.8×) at roughly half the compress CPU, and decompresses at ~1 GB/s (well
under the streaming-serve batch cadence). `lz4` is offered as the minimal-CPU profile
(~2× ratio, multi-GB/s both directions); `none` disables. Compression runs **once**, off the
query hot path (at disk write / spill drain), and *reduces* the bytes the reader later reads —
on a networked/slow disk it can net-*speed-up* serve even after decompression.

## 4. Recommended design — transcode-at-disk-write via Arrow's codec, disk-only

**Setting** (session-scoped, read per-query at InitGlobal like the other cache settings; frozen
per capture). **Default-on when the disk tier is enabled**, per the ask:

```
vgi_result_cache_disk_compression = 'zstd' (default) | 'lz4' | 'none'
vgi_result_cache_disk_compression_level = 1 (default; zstd only, ignored for lz4/none)
```

Only meaningful when `vgi_result_cache_dir` + `disk_max_bytes` are set (disk tier on). The
memory-only tier is never compressed.

**Blob format — unchanged.** `VRC1` magic + per-batch TOC exactly as today. The only difference is
that each batch's stored IPC stream now carries Arrow `BodyCompression`, so the payload is smaller
and self-describing. `disk_ipc_offset/length` still bracket one complete IPC stream. **No format
version, no new per-batch field** — the batch itself records its codec.

**One transcode helper** — `TranscodeIpcWithCodec(uncompressed_ipc, codec, level) ->
compressed_ipc`: `RecordBatchStreamReader` the (uncompressed) `cb.ipc` to recover the batch +
its `custom_metadata`, then re-emit through `MakeStreamWriter(sink, schema, opts)` with
`opts.codec = *Codec::Create(codec, level)` and re-attach the metadata. Used at both disk-write
sites:
- **`PersistToDisk`** (small buffered entries, RAM→disk): transcode each `cb.ipc` before it's
  concatenated into the blob; feed the **compressed** bytes to the SHA (content-addressing over
  stored bytes — deterministic per codec+level+Arrow version).
- **`VgiCaptureDiskWriter`** (streaming spill, the >RAM path): transcode each batch as it drains
  to the blob. Spill is disk-only (no memory copy), so this is the *only* serialization of those
  batches — no double work.

Decode-of-uncompressed-IPC is cheap (framing parse + zero-copy buffer views); the added cost is
the codec's compress, once, off the query hot path. (Alternative considered — serialize twice at
capture, once compressed for disk — rejected: it complicates the flat-RAM spill and duplicates the
memory copy for the common small-entry case. Transcoding from the one uncompressed copy is simpler
and only runs for disk-bound entries.)

**Serve — no code change.** `CachedReplayConnection` (streaming) and `LoadFromDisk`
(materialized ≤ `max_entry_bytes`) already funnel every batch through `RecordBatchStreamReader`,
which decompresses transparently. `LoadFromDiskStreaming`'s TOC walk reads the fixed per-batch
header + `ipc_len` and seeks past the (now compressed) payload — unchanged. The (D) integrity
re-hash runs over the on-disk (compressed) bytes — unchanged; the streaming path still skips it.

**Untouched:** the cache key, eligibility, never-partial invariant, the reaper (its byte-cap
eviction stats real file sizes, so it counts *compressed* disk usage automatically), identity
sharding. The ref's `bytes=` stays the **logical/uncompressed** size (it drives the S8
materialize-vs-stream threshold and `vgi_result_cache()` reporting, both of which want the
in-memory footprint); a new `codec=` line is added for the diagnostic only.

**Format coexistence / migration:** none. A dir can hold uncompressed and zstd/lz4 blobs at once;
the reader always handles all three (the codec is in each message). Flipping the setting **off**
still serves previously-compressed blobs. Re-storing a key under a different codec yields a
different `content_sha` → a new blob; the old one orphans out via the reaper.

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

## 6. Explicitly out of scope — cross-batch zstd dictionary

A **zstd dictionary** trained across batches would recover the cross-batch redundancy that
per-batch compression forfeits (the 2.85× vs 4.10× gap on low-cardinality categoricals), while
keeping per-batch random access. But Arrow's built-in IPC compression has **no dictionary hook** —
adopting it would mean abandoning the transparent-read property and hand-rolling the framing +
decompress in all three read sites. Given the ask (low CPU, ratio "not super high"), the
dictionary's marginal gain does not justify that complexity or the extra training/compress CPU.
**Not planned.** If a real workload ever makes cross-batch ratio critical, this section is the
knowingly-deferred lever, and the cost of switching (custom framing) is documented here.

## 7. Effort & touch points

Small (~half a day). **Write side only** — the read side is free (Arrow decompresses
transparently):
- Two settings (`vgi_result_cache_disk_compression`, `..._level`) + per-query plumbing to the
  singleton (mirror the existing cap settings).
- One `TranscodeIpcWithCodec` helper (Arrow `RecordBatchStreamReader` → `MakeStreamWriter` with
  `IpcWriteOptions.codec`).
- Call it in `PersistToDisk` (buffered) and `VgiCaptureDiskWriter`'s drain (spill).
- A `codec=` line in the ref + a column in `vgi_result_cache(include_disk:=true)` for
  observability.

No change to the cache key, eligibility, never-partial commit, the reaper, identity sharding, or
any read path. No blob-format version bump. No new dependency (Arrow ZSTD/LZ4 already compiled in).
```
