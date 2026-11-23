# Large values in protocol 3.4: Phase 0 measurements

Measured 2026-11-22/23 on the shared aarch64 build box (48 cores) against the
3.4 extension at `5b8b7a2` (haybarn v2.0.0-dev), each SDK at its
`protocol-3.4` SHA (see [the checklist](large-values-3.4-sdk-checklist.md); C++
used a binary from `vgi-cpp@23577fb`, whose diff to `4b472b4` touches no token
or stream code). HTTP transport, loopback. Measurement only; nothing in the
SDKs was changed. Spec: [`large-values-3.4.md`](large-values-3.4.md).

## Recommendations

1. **`VGI_RPC_STATE_REF_INLINE_BYTES` default: keep 16 KiB.** A plain scan's
   init is 7.3–8.4 KB raw → 2.0–2.1 KB compressed; filters, a 41-key join and a
   1000-value regular IN list stay at 3–5 KB; 500 columns + filters + a
   1000-value IN list is ~12 KB, still inline. 16 KiB is crossed at about 1,300
   output columns or about 1,400 random int64 IN/join keys (2,000 keys measured
   18.9 KB), and `vgi_join_keys_max_bytes` defaults to 64 MiB, so the stored
   form is needed and the threshold sits where it should. With a small request
   cap the `turn_budget / 4` term governs (≈5 KB at
   `VGI_RPC_MAX_REQUEST_BYTES=32768`).
2. **No per-turn parse memo.** For a realistic producer (64K rows, one SHA-256
   per row) the per-turn open + init parse (+ re-mint where the init rides the
   cursor) is ≤ 1.6% of turn time in Go, TypeScript and Rust, including
   500-column scans, and < 1% for a plain Python scan. It is material only for
   the trivial `sequence()`-style producer (25–35%) and for Python with very
   wide schemas and light per-batch work (4.7–11%, 2.6–4.5 ms per turn). That
   does not meet decision 3's bar. If Python wide inits matter later, make
   Python's init decode cheaper first (it carries the output schema twice).

Other observations that matter for Phase 2/4:
- Worst-case per-turn token bytes are already 22–24 KB (TS, Rust: cursor + call
  token at 500 columns), so the turn budget and the call token's schemas going
  behind a `StateRef` both matter.
- Go and Rust re-compress the whole cursor every turn, which costs as much as
  the parse; inline `StateRef` payloads are already compressed and should not be
  run through zstd again (the token codec's "raw when not smaller" rule handles
  this, at the cost of one failed compression attempt; SDKs may skip it for a
  payload they know is compressed).
- C#, C and C++ echo no init at all: their example workers keep HTTP streams in
  process memory (24–75-byte handles), so those streams are not cross-replica.
- Filter evaluation dwarfs any token cost: a 1000-value IN list costs per
  64K-row turn Java 8.6 s, C# ~35 s, TS 1.4 s, C 1.5 s, Go (worst) 1.5 s,
  C++ 375 ms, Rust 128 ms (Rust's `in_list` is O(rows × values)). Out of scope
  here; worth its own issue.

## Method

- **Token sizes.** `tokproxy.py` is an aiohttp reverse proxy sitting between haybarn and each worker on loopback.
  - It undoes the request's `Content-Encoding` (zstd), parses the Arrow IPC body with pyarrow, and logs, per request: the path, the wire and decoded body sizes, and the length of every custom-metadata value. For `#b64` keys it logs both the base64 length and the decoded ("raw", i.e. sealed) length.
  - It forwards the body re-compressed with zstd, so the worker sees an ordinary request.
  - "Turn time" is the proxy's upstream time: request sent to full response received, over loopback. It is the worker's server time plus loopback.
- **Bench workers.** One was written per SDK (Python, Go, TS, Rust) with the same contract: `bench(count, batch_size := 65536, cols := 4, work := 1)`.
  - Output columns: `n, k = n % 1000, h = sha256_hex('row-' || n), c3..c{cols-1}`. Projection and filter pushdown are on, auto-apply filters is on, and cardinality is set so DuckDB pushes join filters.
  - "Realistic" means a 64K-row batch with one SHA-256 per row. "Trivial" means `work := 0` with 2048-row batches, the `sequence()`-style case.
- **Java, C#, C and C++** use their shared `sequence` fixture (`scen_seq.sh`), which has no wide scenario. The Python fixture was run the same way as an anchor.
- **Scenarios** (`scen.sh`; each produces 60 batches, so 60 continuations):

  | Scenario | What it does |
  |---|---|
  | `plain` | `SELECT count(*), max(h) FROM b.bench(65536*60)` |
  | `filters` | `k IN (50 values) AND n >= 10 AND n < 1e8 AND h <> 'zz' AND c3 IS NOT NULL AND (n % 2 = 0 OR k > 5)` |
  | `join` | 41 sparse keys in a temp table, `JOIN ... ON keys.id = s.n`. EXPLAIN confirms a dynamic `optional: n IN (...)` filter pushed into the scan. |
  | `bigin` | `n IN (1000 values)`, which is over the 4 KiB inline-set limit, so the set travels as an external `join_keys` batch |
  | `wide` / `wide500` | 300 / 500 columns, 4096-row batches, query touches every column |
  | `worst` | `join` + `filters` + `NOT IN (1000)` + 500 columns |
  | `trivial` | 2048-row batches, `work := 0` |
  | `randinN` (Python only) | `n IN (N random int64)`, N = 1000 / 2000 / 5000 |

- **Parse cost.**
  - Python: microbenchmarked the SDK's own functions (`_open_call_token` and `VgiCallState.deserialize_from_bytes`) over the captured tokens, opened with the known key (`decode_py.py`). Also ran a **live A/B**: the same launcher with vgi-rpc's `_CallStateCache.get` monkeypatched to always miss, against an unpatched control (`serve_nocache.py`, `P0_NOCACHE=0/1`).
  - Go: pprof CPU profile of the live bench worker (`prof_go.sh`).
  - Rust: `perf record --call-graph dwarf` of the live worker (`prof_perf.sh`).
  - TS: microbenchmarked the SDK's own functions over the captured `/init` `request` payloads (`ts-bench/microbench.ts`), because TS already memoizes the parse per process.

## A. Token sizes per SDK × scenario (continuation requests, max over 60 turns)

`raw` = sealed bytes after base64 decoding; `b64` = as sent. "Body" = decoded continuation request body; "wire" = zstd'd on the wire.

| SDK | scenario | /init body | cursor raw / b64 | call raw / b64 | cont. body / wire | turn p50 / p95 ms |
|---|---|---|---|---|---|---|
| Python | plain | 7808 | 384 / 512 | 2254 / 3008 | 3816 / 2979 | 73.2 / 75.0 |
| Python | filters | 10416 | 384 / 512 | 3018 / 4024 | 4832 / 3740 | 140.8 / 143.4 |
| Python | join | 9208 | 386 / 516 | 2897 / 3864 | 4672 / 3616 | 116.5 / 119.0 |
| Python | bigin | 17176 | 386 / 516 | 5536 / 7384 | 8192 / 6275 | 66.4 / 71.8 |
| Python | wide (300) | 23056 | 386 / 516 | 6674 / 8900 | 9712 / 7450 | 54.5 / 57.3 |
| Python | wide500 | 33456 | 385 / 516 | 9570 / 12760 | 13568 / 10357 | 90.3 / 92.0 |
| Python | worst | 45504 | 386 / 516 | 12079 / 16108 | 16920 / 12920 | 35.5 / 36.9 |
| Python | trivial | 8040 | 384 / 512 | 2319 / 3092 | 3896 / 3013 | 1.72 / 1.79 |
| Go | plain | 7792 | 1433 / 1912 | 306 / 408 | 2616 / 2037 | 28.6 / 33.7 |
| Go | filters | 10400 | 2249 / 3000 | 377 / 504 | 3800 / 2957 | 239.1 / 245.8 |
| Go | join | 9192 | 2110 / 2816 | 342 / 456 | 3568 / 2788 | 66.9 / 70.1 |
| Go | bigin | 17160 | 4576 / 6104 | 342 / 456 | 6856 / 5265 | 93.4 / 97.6 |
| Go | wide (300) | 23040 | 4629 / 6172 | 2713 / 3620 | 10088 / 7729 | 31.6 / 32.4 |
| Go | wide500 | 33440 | 6919 / 9228 | 4438 / 5920 | 15440 / 11794 | 52.2 / 57.5 |
| Go | worst | 45488 | 10930 / 14576 | 4404 / 5872 | 20744 / 15791 | 1545 / 1576 |
| Go | trivial | 8024 | 1510 / 2016 | 320 / 428 | 2736 / 2135 | 1.17 / 1.23 |
| TypeScript | plain | 7808 | 2487 / 3316 | 191 / 256 | 3864 / 3006 | 189.8 / 199.5 |
| TypeScript | filters | 10416 | 3300 / 4400 | 238 / 320 | 5016 / 3865 | 371.7 / 383.0 |
| TypeScript | join | 9208 | 3141 / 4188 | 205 / 276 | 4760 / 3675 | 247.3 / 257.1 |
| TypeScript | bigin | 17176 | 5752 / 7672 | 205 / 276 | 8240 / 6316 | 1443 / 1458 |
| TypeScript | wide (300) | 23056 | 5685 / 7580 | 2622 / 3496 | 11368 / 8695 | 151.0 / 158.7 |
| TypeScript | wide500 | 33456 | 7775 / 10368 | 4347 / 5796 | 16456 / 12581 | 243.4 / 252.0 |
| TypeScript | worst | 45504 | 12424 / 16568 | 4338 / 5784 | 22648 / 17219 | 261.1 / 266.7 |
| TypeScript | trivial | 8040 | 2553 / 3404 | 183 / 244 | 3944 / 3064 | 1.66 / 1.77 |
| Rust | plain | 7840 | 545 / 728 | 253 / 340 | 1360 / 1078 | 45.4 / 47.4 |
| Rust | filters | 10448 | 1416 / 1888 | 329 / 440 | 2624 / 2048 | 41.1 / 41.5 |
| Rust | join | 9240 | 1207 / 1612 | 279 / 372 | 2280 / 1789 | 38.7 / 39.2 |
| Rust | bigin | 17208 | 3580 / 4776 | 282 / 376 | 5448 / 4199 | 128.5 / 131.4 |
| Rust | wide (300) | 23088 | 5503 / 7340 | 2928 / 3904 | 11536 / 8823 | 50.7 / 60.4 |
| Rust | wide500 | 33488 | 9069 / 12092 | 4866 / 6488 | 18872 / 14361 | 87.5 / 95.8 |
| Rust | worst | 45536 | 13282 / 17712 | 4337 / 5784 | 23792 / 18118 | 13.5 / 13.6 |
| Rust | trivial | 8072 | 623 / 832 | 242 / 324 | 1448 / 1148 | 0.61 / 0.64 |
| Python fixture `sequence` | plain | 9880 | 384 / 512 | 3869 / 5160 | 5968 / 4602 | 4.1 / 4.5 |
| Python fixture | filters | 11664 | 384 / 512 | 4510 / 6016 | 6824 / 5266 | 73.0 / 75.4 |
| Python fixture | join | 11592 | 383 / 512 | 4557 / 6076 | 6880 / 5263 | 61.9 / 63.2 |
| Java `sequence` | plain | 8616 | 440 / 588 | 207 / 276 | 1160 / 927 | 5.8 / 9.3 |
| Java | filters | 10400 | 1122 / 1496 | 209 / 280 | 2072 / 1642 | 374.8 / 376.1 |
| Java | join | 9568 | 896 / 1196 | 207 / 276 | 1768 / 1397 | 19.0 / 20.3 |
| Java | bigin | 17976 | 4195 / 5596 | 208 / 280 | 6168 / 4762 | **8610** / 8620 |
| Java | trivial | 8616 | 437 / 584 | 206 / 276 | 1152 / 919 | 1.55 / 1.82 |
| C# `sequence` | all four | 8648–18008 | 45 / 60 | 45 / 60 | 416 / 416 | n/a |
| C `sequence` | all four | 7920–17280 | 48 / 64 | ~74 / 99 | 456 / 456 | n/a |
| C++ `sequence` | all five | 7896–17256 | 24 / 32 | 75 / 103 | 424 / 424 | n/a |

Random-key IN lists (Python bench, call token = compressed init):

| IN-list size | init plain (VgiCallState) | init zstd-3 | call raw / b64 |
|---|---|---|---|
| 1000 random int64 | 17648 | 10955 | 11127 / 14836 |
| 2000 random int64 | 25648 | **18937** | 19109 / 25480 |
| 5000 random int64 | 49648 | **42988** | 43163 / 57552 |

### Carried init: plaintext vs compressed (what the 16 KiB threshold sees)

Python is exact: the token was opened with the key and the init is the `VgiCallState` bytes (`InitRequest` + `GlobalInitResponse`). TS is exact: its own `packInitRequest` applied to the captured `request` IPC. Go and Rust can't be opened without porting their token formats, so the "compressed" figure for them is the cursor raw size minus ~60 B of envelope.

| scenario | Py init plain → zstd | TS init IPC → packed | Go cursor (≈ compressed state + init) | Rust cursor (≈) | Python schema in call token (plain) |
|---|---|---|---|---|---|
| plain | 8448 → 2106 | 7256 → 1965 | ~1.37 K | ~0.49 K | 120 |
| filters | 11016 → 2869 | 9864 → 2779 | ~2.19 K | ~1.36 K | 272 |
| join | 9808 → 2734 | 8656 → 2627 | ~2.05 K | ~1.15 K | 176 |
| bigin (1000 regular) | 17648 → 5368 | 16624 → 5228 | ~4.5 K | ~3.5 K | 176 |
| wide 300 | 26064 → 5325 | 22504 → 5164 | ~4.6 K | ~5.4 K | 15184 |
| wide 500 | 38064 → 7482 | 32904 → 7251 | ~6.9 K | ~9.0 K | 25584 |
| worst | 49920 → 11935 | 44952 → 11901 | ~10.9 K | ~13.2 K | 25680 |

Growth rates:
- about 11 B per output column, compressed (the init carries `output_schema`);
- about 3.3 B per IN value for a regular progression, and about 8 B per value for random 64-bit keys.

So 16 KiB of compressed init is crossed at about 1,300 columns, or about 1,400 random int64 IN/join keys. `vgi_join_keys_max_bytes` defaults to 64 MiB, so nothing upstream stops a much larger init.

### Observed:  where each SDK puts the init

- **Python:** the init is in the **call** token (`VgiCallState`); the cursor stays ~384 B raw. The call token also carries the output schema a second time (~51 B/column plain). For wide500 the call-token plaintext is 63.8 KB, of which 25.6 KB is the schema and 38 KB the init, which itself contains `output_schema` again. Compressed together: 9.6 KB.
- **Go and TS:** the init is in the **cursor**. Their call tokens carry only schemas: about 0.2–0.4 KB, or 4.3–4.4 KB at 500 columns. TS's cursor is the packed init (compressed once, then re-sealed **uncompressed** every turn). Go compresses the whole cursor every turn.
- **Rust and Java:** a destructured blob in the **cursor** (arguments, settings, secrets, filters, join keys, schemas), not the raw `InitRequest`. It is smaller for plain scans (Rust 545 B, Java 440 B) but grows the same way with columns and filters.
- **C#, C and C++:** with these example workers, **no init is echoed at all**. The cursor and call tokens are 24–75 B random handles. The stream lives in an in-process registry (vgi-rpc-c `src/http.c:1514-1540`: `stateless = serialize && VgiRpcCanResume`, otherwise "local-call" plus a random cursor). No `VGI-Session` header was involved; checked with the proxy. So these streams are not cross-replica today.
  - When vgi-rpc-c does run a stream stateless, it seals the **whole request IPC into the CALL token** (`http.c:896-905`), like Python, not the cursor. That contradicts plan §1 cause 2's grouping of C with the cursor SDKs.
  - We didn't force stateless mode. It needs a fixture with `serialize`/resume, or `require_stateless_streams`.

## B. Per-turn worker time and init-parse share

"Parse" means the per-turn work that exists only because the init rides the token:
- opening the token (AEAD, and zstd where the SDK uses it);
- deserializing the init/state, and rebuilding params, filters and schemas;
- where the init is in the cursor, re-sealing it into the next cursor ("re-mint").

### Python (call-state cache = today's memo; A/B run live)

| scenario | turn p50, control (cache on) | turn p50, cache disabled | delta | share of a no-memo turn | microbench open+parse+schemas |
|---|---|---|---|---|---|
| trivial (2048 rows, no work) | 1.73 ms | 2.66 ms | **+0.93 ms** | **35%** | 48+599+10 = 657 µs |
| plain realistic (64K sha256) | 74.4 ms | 73.8 ms | ≈0 (noise) | <1% | 48+570+11 = 628 µs (0.8%) |
| wide500 (4096 rows × 500 cols) | 90.5 ms | 95.0 ms | +4.5 ms | **4.7%** | 162+2187+250 = 2.6 ms |
| worst (500 cols + filters + 1000-IN) | 36.1 ms | 40.6 ms | +4.5 ms | **11%** | 185+2281+261 = 2.7 ms |

- Python parse cost is roughly flat in IN-list size: 0.62–0.66 ms for 1000–5000 values, because the join-key batch stays as bytes. Token open grows with size: 115 µs at 1000 values, 374 µs at 5000.
- It scales with column count: 1.6 ms at 300 columns, 2.2 ms at 500.
- Each A/B row is the median over 2–3 interleaved runs of 60 turns. An earlier comparison against a separately launched `vgi-serve` process showed a spurious +4 ms on plain. The A/A control (same launcher, cache on) removed it, so only the control numbers above count.

### Go (no memo; pprof of the live worker; µs CPU per turn)

| scenario (turns) | turn p50 | open cursor (AEAD+zstd+gob) | rehydrate (params, filters) | re-mint cursor | total | share of turn |
|---|---|---|---|---|---|---|
| trivial (4000) | 1.15 ms | 128 | 60 | 125 | **313 µs** | ~27% of wall |
| plain realistic (300) | 27.0 ms | ~200* | ~33* | ~200* | ~0.3–0.4 ms | **~1.2–1.6%** |
| wide500, 64-row (3000) | 4.3 ms | 103 | 133 | 140 | 376 µs | 8.7% |
| wide500 realistic (150) | 46.8 ms | ~270* | ~270* | ~530* | ~0.4–1.1 ms | **~1–2%** |
| bigin realistic (100) | 94.2 ms | ~200* | ~300* | ~100* | ~0.6 ms | <1% |

\* Under 10 samples, so only order-of-magnitude. The trivial and wide500-64-row rows are statistically solid; apply their absolute µs to the realistic turn.

### TypeScript (already memoizes the parse per process; microbench of the SDK's own functions; µs)

| scenario | open token | parse init, memo miss | parse, memo hit | re-mint cursor | no-memo total | share of realistic turn |
|---|---|---|---|---|---|---|
| trivial | 66 | 438 | 4 | 164 | 668 | ~29% of a 1.66 ms turn (if the memo were removed) |
| plain | 84 | 541 | 9 | 207 | 832 | 0.4% of 190 ms |
| filters | 96 | 528 | 6 | 231 | 855 | 0.2% |
| bigin | 118 | 507 | 11 | 374 | 999 | <0.1% |
| wide500 | 134 | 2488 | 15 | 511 | 3133 | 1.3% of 243 ms |
| worst | 204 | 2606 | 24 | 808 | 3618 | 1.4% of 261 ms |

The microbench state omits user state, so its token is about 500 B smaller than live.

### Rust (no memo; perf of the live worker; µs CPU per turn)

| scenario (turns) | turn p50 | open token (AEAD+zstd) | bincode decode of `ExchangeBlob` | re-mint cursor (bincode+zstd+seal) | total | share |
|---|---|---|---|---|---|---|
| trivial (3000) | 0.77 ms | 22 | 27 | 144 | **194 µs** | ~25% of wall |
| plain realistic (150) | 47.9 ms | <0.05% | <0.05% | 57 | ~0.1 ms | **~0.2%** |
| wide500, 64-row (3000) | 4.8 ms | 131 | 263 | 599 | 993 µs | 21% |
| wide500 realistic (100) | 94.7 ms | 63 | 251 | 610 | 924 µs | **1.0%** |
| bigin 2048-row (3000) | 5.0 ms | 53 | 74 | 262 | 389 µs | 7.8% |

In Go and Rust, the per-turn **re-mint** (re-serializing and re-compressing the cursor that carries the init) costs as much as or more than the open and parse. A `StateRef` design that carries already-compressed inline bytes should not run them through zstd again every turn.

### Side findings (not part of the question, but large)

- **Filter evaluation is slow across SDKs.** On 64K-row batches the filter-heavy turns cost far more than the parse ever does:
  - `n IN (1000)` costs Java 8.6 s/turn, C# ~35 s/turn, TS 1.44 s, C 1.47 s, Go `worst` 1.5 s, C++ 375 ms, Rust 128 ms.
  - Rust's `vgi::pushdown::in_list` is O(rows × values): 87% of CPU in `bigin`.
  - Go's auto-apply goes through `exprduckdb`; its `filters` turn is 239 ms against 28 ms for `plain`.
- The plan's "~7 KB raw, ~2–3 KB compressed" for a small init holds: 7.3–8.4 KB → 2.0–2.1 KB. The Python fixture catalog, with more settings, is 10.5 KB → 3.7 KB.

## Couldn't measure / caveats

- **C#, C, C++:** no init travels in their tokens with these fixtures (in-process streams), so no init parse was measured for them. C# `filters` and `bigin` produced only 1 and 9 turns before being stopped (113 s and ~35 s per turn), but the token sizes are constant per turn.
- **C++:** used the older `34-vgi-cpp` binary read-only (see the setup table). We didn't build p0-vgi-cpp.
- **Java, C#, C, C++:** sequence fixture only, so no wide-schema scenario, and no per-turn profile.
- **Go and Rust compressed-init figures** are inferred from cursor size, not decrypted.
- **Profiles and microbenches are CPU on a shared box.** Low-sample profile rows are flagged.

