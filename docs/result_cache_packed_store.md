# Packed On-Disk Store for Small Exchange-Mode Cache Entries — Design

Status: **PHASE 1 SHIPPED, DEFAULT ON** (`vgi_result_cache_pack=true`; single-process pack
backend + cross-restart read). **Routing is EXCHANGE-only** — a memo packs when its
`input_hash` is present and it is small; producer entries (few-and-large) always stay in the
loose store, so their `objects/`/`refs/` diagnostics are unchanged. Cross-process
*concurrent*-write coordination (phases 3–4) not yet built — a second live process reading a
shard sees peers' sealed packs on load but not their in-flight appends until a reload, and a
dead writer's packs are reclaimed only on expiry / `vgi_result_cache_flush()`, not compacted.
See §7. Author: cache maintainers.
Supersedes the interim guard `vgi_result_cache_exchange_disk_max_refs` (which bounds file
count by *evicting* small entries; this replaces the storage model so we don't have to).

## 1. Motivation

The disk tier is a **loose-object store**, content-addressed and per-identity sharded:

```
<dir>/<shard=sha256(identity_scope)>/objects/<content_sha>.vrc   # immutable blob
<dir>/<shard>/refs/<key_fingerprint>.ref                          # key -> content + meta
```

That is excellent for the **producer** cache and the **buffered** exchange path — a handful of
*large* whole-result blobs. It is pathological for the **streaming table-in-out** and
**correlated LATERAL** exchange paths, which memoize **per 2048-row input chunk**: one query
can mint thousands of tiny entries, each costing *two* files (a `.vrc` + a `.ref`) where the
filesystem metadata dwarfs the payload. Concretely, the loose store hurts three ways:

1. **Inode / metadata blow-up** — a 100-byte memo consumes an inode + a min block for the
   object *and* a second file for the ref. Thousands per query → hundreds of thousands of files.
2. **Reaper cost is `O(files)`** — `ReapDisk` reads **every** ref and stats **every** object each
   pass (`src/vgi_result_cache.cpp`). File count is the reaper's scaling term.
3. **Per-entry write cost** — create + write + fsync + atomic-rename, twice, per tiny memo.

The interim mitigation (`vgi_result_cache_exchange_disk_max_refs`, an LRU cap on exchange refs)
bounds the *count* but only by throwing entries away — so a workload of many small-but-expensive
memos (an ML inference / slow remote lookup returning a few bytes: exactly the case a warm
cross-process cache helps most) can't actually keep them. **The fix is to stop storing small
entries as loose files.** Every mature HTTP/object cache reached the same conclusion.

## 2. Prior art — nobody uses file-per-object for many small objects

| System | Small-object storage |
|---|---|
| **Chromium Blockfile cache** (desktop HTTP cache) | A few block files (`data_0..3`) with 256 B / 1 K / 4 K slot allocation; only entries over a size threshold get their own `f_*` file. Small responses are *packed*. |
| **Chromium Simple Cache** (Android) | Keeps file-per-entry — but *only* because it pairs it with an **in-memory index** (never stats the dir) + a dedicated index file for fast startup. |
| **Firefox cache2** | File-backed entries plus a compact binary **index** so lookup/eviction never scan `entries/`. |
| **Squid** | Abandoned `ufs`/`aufs` (file-per-object in hashed L1/L2 dirs — inode hell) for **Rock store**: objects packed into fixed slots in one big DB file + an in-memory slot index. |
| **Git** (closest analogy — already content-addressed) | Loose objects (one file each) are packed by `gc` into **packfiles + a pack index**; readers read *all* packs; `gc` compacts loose→packed and drops dead objects. |

The convergent shape: **a few large container files + slot/pack allocation + an index
(key → file/offset/length), with lookup, eviction, and expiry driven by the index — never the
directory.** Our `.vrc` files *are* git loose objects; the mature move is to pack them.

## 3. Design: a size-tiered disk tier

Route by **kind + size**, keeping two storage backends **coexisting** under the same
per-identity shard. As shipped, only **exchange** memos pack (see the routing note below):

- **Loose** — producer results (any size) + large exchange entries (≥ `pack_max_entry_bytes`) +
  buffered exchange results. `objects/<sha>.vrc` + `refs/*.ref`, including the S8 streaming serve
  (positioned per-batch reads for >RAM results). Loose is *ideal* for these — few and large.
- **Packed** — small **exchange** memos (`input_hash` present, `< pack_max_entry_bytes`): the
  streaming / LATERAL per-chunk memos, the actual many-tiny-files case. Appended into pack files +
  an index, described below.

**Routing note (as shipped).** The design originally proposed routing purely by *size* (a small
producer result would also pack — open question #1 below). When flipping the default ON we scoped
it to **exchange memos only** (`!input_hash.empty()`): the file-explosion problem is specific to
per-chunk memoization, producer results are few-and-large (loose is ideal + keeps their
`objects/`/`refs/` diagnostics + the S8 streaming serve), and it kept the producer disk tests
untouched. Packing small *producer* results is a future option if a high-arg-cardinality workload
ever needs it.

Per-shard packing preserves the two invariants the loose store already gives us:
- **Identity security boundary** — a pack never mixes identities (packs live *under* the shard),
  so no cross-tenant leak is possible and `FlushCatalog` stays an `O(1)` shard-subtree removal.
- **Content integrity** — each packed record carries its `content_sha`, re-hashed on load
  (a corrupt record is a clean miss, exactly as today).

### 3.1 Layout

```
<dir>/<shard>/
  objects/                       # large loose blobs         (unchanged)
  refs/                          # large loose refs          (unchanged)
  packs/
    <pid>-<seq>.vpack            # append-only; many small records concatenated
    <pid>-<seq>.vidx             # derived index for that pack (rebuildable from .vpack)
```

**Per-*process* pack files** (namespaced by pid + a monotonic seq). This is the git model and it
sidesteps cross-process write contention entirely: each pack has a **single writer** (append
only), and every process **reads all packs** in the shard (its own + peers') to build a union
view. Cross-process + cross-restart warm cache falls out naturally: process B serves process A's
memos by reading A's packs; a fresh process reads the packs left on disk.

### 3.2 Pack record format (`.vpack` is the source of truth)

Append-only, self-describing so the index is a pure derived optimization (rebuildable by scanning
the pack — the git `.idx`-from-packfile property). Each record:

```
magic "VPK1" | key_fingerprint(32) | flags(u8) | content_sha(32)
             | logical_len(u64) | stored_len(u64)      # uncompressed / on-disk(compressed)
             | expires_unix(i64) | rows(i64)
             | meta_len(u16) | meta(scope,etag,last_modified,codec,catalog,function, exch)
             | blob[stored_len]                          # the codec-compressed IPC bytes
             | record_crc32(u32)                         # framing integrity
```

- `key_fingerprint` = the full-key SHA already used for the loose ref filename
  (`VgiResultCacheKey::Fingerprint()`), re-verified on lookup so an index bug can never cross-serve.
- `content_sha` re-hashed against `blob` on load → tamper/corruption = clean miss (parity with loose).
- `blob` reuses the **same** per-batch Arrow-IPC + codec bytes the loose store writes, so the
  serve path (`BufferReader` → `RecordBatchStreamReader`, transparent decompress) is **unchanged**
  — a packed small entry is materialized + adopted into memory exactly like a small loose entry
  today. (The S8 >RAM streaming serve stays loose-only; packed entries are small by construction.)
- No cross-object dedup for packed entries (loose keeps it). For per-chunk memos dedup value is ~0
  (distinct chunk → distinct output), and dropping it removes the shared-object refcount problem.

### 3.3 Index (`.vidx`, derived + rebuildable)

In-memory per touched shard: `key_fingerprint → { pack_id, offset, stored_len, logical_len,
expires_unix, mtime, dead }`. It is the **only** lookup / eviction / expiry surface — the reaper
never scans a directory again.

- **On-disk `.vidx`** — a compact binary of the same tuples, written by atomic-rename after a batch
  of appends, for fast startup without scanning whole packs. If a `.vidx` is missing/stale/corrupt,
  rebuild it by scanning its `.vpack` (records are self-describing + CRC'd). The pack is truth;
  `.vidx` is a cache. This is crash-safe: append-then-rename-index means a crash between the two
  loses at most the just-appended record (invisible, reclaimed on next compaction/rebuild) —
  **never** corruption.
- **Memory cost** is metadata only (~64–96 B/entry): 100 k entries ≈ 6–9 MB, trivial and separate
  from the existing decoded-batch memory LRU.

### 3.4 Lookup

`Lookup(key)`: memory tier → **packed index** (in-RAM map, `O(1)`) → **loose ref** file. A key is
placed in exactly one backend at insert (decided by size), so the two on-disk probes are ordered,
cheap, and mutually exclusive. Expiry is enforced at read time by **every** reader against the
record's `expires_unix` (a stale packed entry is a miss regardless of which process wrote it) — so
correctness never depends on the owner having compacted yet.

### 3.5 Insert

Size ≤ `pack_max_entry_bytes` → append a record to this process's *current* pack, update the
in-memory index, and (batched) rewrite the `.vidx`. Else → the loose path, unchanged. A pack rolls
to a new `<pid>-<seq+1>.vpack` when it exceeds a target size (e.g. 64 MB) so no single pack grows
unbounded and compaction units stay reasonable.

### 3.6 Eviction, expiry, compaction (the reaper, reworked)

The reaper walks the **in-memory index** (RAM, no I/O) instead of the directory:

- **Expiry** — drop index entries past `expires_unix`; mark the pack slot `dead`.
- **Caps** — the byte cap (`disk_max_bytes`) and the entry-count cap (reuse
  `exchange_disk_max_refs`, now enforced cheaply on the index) LRU-evict oldest → mark `dead`.
- **Compaction (git `gc`)** — when a pack this process *owns* crosses a dead-ratio threshold
  (`pack_compaction_dead_ratio`, default 0.5) or a dead-bytes threshold, rewrite its **live**
  records into a fresh pack, fsync, atomic-rename the new `.vidx`, then unlink the old
  `.vpack`+`.vidx`. Space is reclaimed here, not at eviction.
- **Foreign / orphaned packs** — a process compacts only its **own** packs (the append-only writer
  invariant). Dead space in a *peer's* pack is reclaimed when that peer compacts. A pack whose
  owner pid is **dead** and is past a grace window is claimed by the reaper under a **per-shard
  flock** (we already use flock for the launcher) and compacted-or-deleted. This is the only place
  a lock is taken; the steady state (append + read + own-compaction) is lock-free.

### 3.7 Cross-process freshness (the one real trade-off)

A just-inserted memo becomes visible to **another process** only after this process flushes its
`.vidx` (peers refresh their union index by re-statting `.vidx` mtimes on a cadence). So batching
`.vidx` writes trades a small **cross-process** freshness lag for far fewer fsyncs. Same-process
hits are immediate (in-memory index). Tunable via a flush cadence (`pack_flush_interval` /
flush-every-N-appends); at the extreme "flush per append" it matches loose-store visibility at
loose-store write cost. This lag is **hit-rate only, never correctness** — a missed cross-process
hit just recomputes.

## 4. Effect on hit rate

Adapting storage moves hit rate the **right** way, and leaves hit *semantics* untouched:

- **Key granularity is unchanged** — packing is purely internal; the per-chunk key is identical, so
  *what* hits is identical. Independent per-entry eviction/expiry still works via the index (mark
  dead, reclaim on compaction). **Do not** coarsen keys to cut file count — that would trade hit
  precision for fewer entries, the wrong lever.
- **Higher achievable hit rate** — today the retention ceiling is *filesystem tolerance* (the
  ref-count cap exists to protect the FS, not because we're out of bytes). Packed, the ceiling is
  just bytes, so we can retain far more memos → more hits. The `exchange_disk_max_refs` cap can be
  raised (or retired) once file count is decoupled from entry count.
- **Minor downward caveat** — the cross-process freshness lag (§3.7): a brand-new memo may miss for
  a peer until the next `.vidx` flush. Bounded and tunable; in-process unaffected.

Net: strictly more entries retained for a given disk budget, same hit semantics, at the cost of a
tunable cross-process visibility lag.

## 5. Coexistence & migration

- Additive: a new `packs/` subdir beside `objects/`+`refs/`. Old loose entries stay readable
  (lookup falls back to the loose ref), so a mixed dir just works — no format bump for loose, no
  migration step. New small entries write packed; existing loose small entries age out normally.
- **Feature-gated** — `vgi_result_cache_pack` (BOOLEAN). Ship **off**, enable after soak. With it
  off, everything is exactly today's loose store.
- `FlushAll` / `FlushCatalog` already operate on shard subtrees (`packs/` included) → unchanged.

## 6. Proposed settings

| Setting | Default | Meaning |
|---|--:|---|
| `vgi_result_cache_pack` | `true` | Master switch for the packed small-entry backend (default ON; routes small **exchange** memos). |
| `vgi_result_cache_pack_max_entry_bytes` | `262144` (256 KB) | Route threshold: entries below this pack; at/above go loose. |
| `vgi_result_cache_pack_target_bytes` | `67108864` (64 MB) | Roll to a new pack past this size (compaction-unit size). |
| `vgi_result_cache_pack_compaction_dead_ratio` | `0.5` | Compact an owned pack when this fraction is dead. |
| `vgi_result_cache_pack_flush_interval_ms` | `1000` | Max `.vidx` flush lag (cross-process visibility knob). |

`vgi_result_cache_exchange_disk_max_refs` stays as the index entry-count cap (now cheap to enforce)
and can default much higher once packed.

## 7. Phased implementation plan

1. **Format + single-process pack read/write** — ✅ **DONE**. `.vpack` record framing (per-record
   magic + `content_sha`; a torn append tail is detected by length-bounds on scan, so no CRC is
   needed for an append-only single-writer file), append, in-memory index, `.vidx` write/rebuild
   (trusted when its recorded `pack_size` matches the file, else rebuilt from the pack). Small
   entries route here; loose unchanged. Serve reuses the positioned-read → `BufferReader` path.
   Reaper (expiry / global byte-cap evict / own-pack compaction over the dead-ratio threshold) runs
   over the in-memory index via `vgi_result_cache_reap()` + the background thread — no directory
   scan for lookups. Cross-**restart** read works (a new process reads prior packs as foreign +
   compacts only its own). **Reap hardening (shipped):** a fully-dead pack is *deleted* (not
   compacted to an empty pack); the active writer is *sealed* first when it crosses the dead
   threshold so a never-rolling writer can't pin dead space open; shard state is keyed by
   **(dir, shard_hash)** so a mid-process `vgi_result_cache_dir` change can't reuse a writer
   pointing at the old dir. **Default flipped ON**, routing scoped to exchange memos. Tests:
   `test/sql/integration/cache/pack.test` (structural file-count, cross-serve, routing threshold,
   expiry reclamation + seal + fresh-writer, byte-cap eviction, flush); concurrent soak (8 threads,
   tiny pack target forcing rolls, interleaved reap/flush) validated on release + debug.
2. *(folded into 1)* Reaper on the index.
3. **Cross-process union read (live)** — refresh peers' pack indexes on a cadence
   (`pack_flush_interval`) so a running process sees another running process's *new* appends without
   a restart. Test with two concurrent processes over one dir.
4. **Orphaned-pack reclaim** — dead-owner detection + per-shard flock so a crashed writer's dead
   space is compacted, not just left to expire. Test with a killed writer leaving packs behind.
5. **Enable path** — flip `vgi_result_cache_pack` default after soak.

## 8. Open questions for review

- **Threshold vs. mode** — route purely by *size* (simplest; a small producer result also packs) or
  additionally by *mode* (only exchange entries pack, producer always loose)? Size-only is simpler
  and self-correcting; mode-gating is more predictable. Leaning size-only.
- **Do we even want per-chunk memos on disk long-term?** The memory tier already serves same-process
  repeats; the disk win is cross-process/cross-restart identical chunks (dashboards, repeated LATERAL
  over stable dimension tables). If real workloads don't show that pattern, the packed store may be
  better scoped to *buffered* whole-input results only — but those are already large and loose-happy,
  which would make this whole effort unnecessary. **We should confirm the target workload before
  building phases 3–4.**
- **Reuse an embedded KV (LMDB / SQLite) instead of hand-rolling packs?** Less code we own, but a new
  dependency and the mmap-lifetime concerns the S8 serve deliberately avoided (leaked-singleton Arrow
  static-destruction ordering). Hand-rolled packs keep the positioned-pread model we already trust.
