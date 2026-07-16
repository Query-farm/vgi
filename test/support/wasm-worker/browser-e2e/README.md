# Browser e2e for the VGI `worker:` SAB transport

End-to-end test of the in-browser `worker:` transport in a real cross-origin-isolated
headless Chromium. Exercises the full stack — the haybarn DuckDB-WASM engine (with the
VGI slot-stub js-library), the VGI wasm extension (`WebWorkerFunctionConnection` +
`WebWorkerInvokeUnary`), and the VGI worker module (real Rust `serve()` over the SAB
duplex-ring channel) — asserting:

1. `LOAD vgi` — the extension's `vgi_wasm_slot_*` imports resolve against the engine at `dlopen`.
2. **direct** — `vgi_table_function('worker:...', 'count_to', [5])` ⇒ `[0..4]`.
3. **multi-batch** — `emit_batches(3, 4)` streams 3 batches (12 rows) over the tick loop.
4. **error propagation** — `boom()` (worker errors mid-produce) surfaces as a thrown query
   error (not a hang or a silent empty result).
5. **ATTACH** — `ATTACH 'worker:...'` → catalog discovery → `wcat.main.count_to(3)` ⇒ `[0..2]`
   (catalog RPCs over the unary-over-SAB path).
6. **concurrent** — 4 concurrent `worker:` scans on separate connections (`SET threads=4`).
7. **PARALLEL SERVE PROOF** — a **single** scan of `parallel_probe` (a producer that declares
   `max_workers=4`) with `SET threads=4`. DuckDB fans one scan out across scan threads, each
   acquiring its own `worker:` connection = its own SAB slot = its own worker serve pthread, so N
   serves run **at once**. Each `parallel_probe` producer holds a process-global `ConcGuard`
   (shared across serve threads via the worker module's linear memory), and `peek_max_concurrency()`
   reads back the peak simultaneously-active serves. Observed **maxConcurrency = 4** — genuine
   parallel multi-thread serve. (This is the workload the older `slow_count` "diagnostic, not a
   gate" note called for: separate concurrent queries serialize at `AsyncDuckDB`'s single-threaded
   boundary and only ever showed a peak of 1; ONE parallelized scan does not, so it exercises the
   multi-thread serve for real.)

The fixtures (`count_to`, `emit_batches`, `boom`, `parallel_probe`, `peek_max_concurrency`, plus
`slow_count`) live in `../../sabtable/src/lib.rs`; cases 2–4 are also covered natively by the
`[sab-conn]` unit test.

### Resolved: stale pthread heap views (was an intermittent hang)

Earlier this suite hung intermittently whenever a scan reused a SAB slot (streaming
`count_to`→`emit_batches`, catalog discovery's rapid unary RPCs, etc.). Root cause — found via the
worker-side + client-side channel observability: the JS ring stubs read the channel through
emscripten's cached `Module.HEAPU8` **view**, which goes **stale on a pthread that was
mid-compute/blocked through a memory-growth event** — so its `Atomics.load` never observed another
thread's stores into the *current* buffer, and the ring read saw `avail=0` forever (native C++
pointers, always aliasing live memory, saw the data fine, so the two sides diverged). It was flaky
because it depended on which pool pthread DuckDB scheduled the scan onto. Fixes (all landed):

- **`js-stubs.js` reads the live buffer** (`$vgiSab.buf()` → `wasmMemory.buffer`, not
  `Module.HEAPU8.buffer`) for every ring op. This is the primary fix.
- **`VgiSabEnsureChannelOnRealm()`** (`vgi_sab_stream.cpp` `Read`/`Write`) re-publishes the channel
  offset onto the executing pthread's realm, since `vgiSab.base` is per-realm and the scan can run
  on a different pthread than the one that bound the connection.
- **Unique claim-id STATE handoff** (`vgi_sab_abi.hpp` `HDR_CLAIM_SEQ`) fixes the ABA in the
  worker's `await_release`; proactively freeing an errored slot fixes "channel exhausted".

With these, the full E2E is **reliably green** across repeated fresh loads (`maxConcurrency = 4`).

### Resolved: worker-error deadlock under `threads > 1` (a DuckDB-WASM engine bug)

Case 5 (the worker-error case) now runs under `SET threads=4`. It previously (≈2/3 of fresh loads)
**deadlocked the whole engine**: a worker throw under `threads>1` left the DuckDB-WASM worker thread
frozen *inside* the query C++ call (a trivial `SELECT 42` on a *fresh* connection also hung), so the
query promise never settled — even though the SAB transport had already torn down cleanly (all slots
freed, both rings closed, worker idle). It was characterized as **not a transport bug** (making the
transport error path fully non-blocking did not fix it; a generic non-VGI parallel throw settled
fine), then root-caused to the engine:

- On a query error the DuckDB-WASM runtime **main thread** busy-spins `Executor::CancelTasks`
  (`while (executor_tasks > 0) WorkOnTasks()`). When its own producer queue is empty that loop
  pure-spins on the atomic without hitting any cancellation point, **starving the main thread's
  proxied-call queue** (emscripten issue [#3495](https://github.com/emscripten-core/emscripten/issues/3495)).
- A worker **pthread** whose in-flight task makes a main-thread-proxied call during teardown then
  blocks forever; its `ExecutorTask` never destructs, `executor_tasks` sticks (observed wedged at
  `1`), and the drain never converges → the engine deadlocks.
- **Fix** (haybarn DuckDB fork, `src/parallel/executor.cpp`): pump
  `emscripten_main_thread_process_queued_calls()` inside that spin (the documented workaround for
  exactly this lock-free-loop pattern) so the proxied call completes and the task finishes.

Verified across many fresh loads with the race provably occurring (`executor_tasks` briefly `1`) and
**draining every time**; the full suite — including this error case and the parallel-serve proof
(`maxConcurrency=4`) — is green under `threads=4`. `repro-threads-deadlock.mjs` is the
reproduction / regression harness (it loops the trigger and asserts no engine freeze); the
`probe-*.mjs` files were the isolation harness used to characterize the bug.

## Prerequisites (build these first)

- **Engine + JS package** — build the haybarn COI-loadable engine and re-bundle:
  ```bash
  cd <haybarn-wasm>
  DUCKDB_WASM_LOADABLE_EXTENSIONS=1 USE_GENERATED_EXPORTED_LIST=no \
    ./scripts/wasm_build_lib.sh relperf coi
  (cd packages/duckdb-wasm && node bundle.mjs release)
  ```
- **VGI wasm extension** — deployed under `<haybarn-wasm>/extensions/<ver>/wasm_threads/`
  (see `../../../build-wasm-coi.sh`, pointed at the haybarn engine's Arrow).
- **Worker module** — `../vgi_worker.{js,wasm}`, built via `../build.sh` (needs sabtable's
  wasm staticlib: `cd ../sabtable && RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-args=-pthread' cargo +nightly build --target wasm32-unknown-emscripten -Z build-std=std,panic_abort --release`
  — the `RUSTFLAGS` are required so `-Z build-std`'s recompiled `compiler_builtins` supports
  `--shared-memory`, else wasm-ld rejects the link).
- **puppeteer + esbuild** resolvable from `<haybarn-wasm>/node_modules` (used headless).

## Run

```bash
node run.mjs
```
Exit 0 = pass, 1 = fail, 2 = skipped (a prerequisite is missing — the runner prints which).

Env overrides: `HAYBARN_WASM` (default `~/Development/haybarn/haybarn-wasm`), `VGI_EXT_WASM`,
`VGI_ENGINE_VERSION_DIR` (default `unknown` — the version dir a `DUCKDB_WASM_VERSION`-unset
dev engine requests; set to the engine's real version for a release build).

Not wired into the default `make test` suite (it needs the three sibling wasm builds); run it
directly after a wasm build.

### Function-type coverage (`test-features.mjs`)

`test-entry.mjs` covers producer-mode tables. `test-features.mjs` broadens coverage to the other
function-type protocol paths over the SAB channel, all under `SET threads=4`:

- **scalar** (`sab_double`, exchange 1:1, null passthrough),
- **streaming table-in-out** (`sab_echo`, classic `TABLE`/subquery input → passthrough),
- **aggregate** (`sab_sum`, exchange update + combine + finalize; ungrouped + grouped),
- **large payload** (`sab_big`, ~50 MB streamed over the 64 KiB ring — the chunker),
- **LIMIT early-abandon** (client stops mid-stream → slot teardown/bail; a following scan works),
- **result-cache hit** (`sab_cached` advertises `vgi.cache.ttl` + stamps a per-run nonce; two
  identical scans return the SAME nonce ⇒ the 2nd was served from cache, and
  `vgi_result_cache_stats().hits ≥ 1`).

Fixtures live in `../../sabtable/src/lib.rs` (rebuild the worker via `../build.sh`). Run:

```bash
VGI_ENTRY=test-features.mjs node serve.mjs 8799   # then open the page in a COI browser
# ?only=<caseName> runs one case in isolation (a hung query blocks the single DuckDB-WASM
# worker thread, so isolate to localize a hang). window.__result.pass is the gate.
```

**Result cache on WASM (resolved).** The cache was briefly defaulted off on WASM because its keys +
identity scope are SHA-256 hashes, and the engine's mbedtls (`MbedTlsWrapper::ComputeSha256Hash`) is
not resolvable from the dlopen'd extension side-module on emscripten (→ "`_ is not a function`",
which for the exchange operators manifested as a hang). Fixed: the extension now computes SHA-256 via
`VgiSha256Hex` (`src/vgi_sha256.cpp`), which on emscripten calls the engine's **exported**
`duckdb_wasm_sha256` primitive (mbedtls runs inside the main module, where it *is* available — the
same primitive `vgi_oauth` uses) and on native uses mbedtls directly. The cache is **on by default**
again on WASM; `cacheHitOk` proves a real hit. (The on-disk cache tier's incremental hashing stays
mbedtls, but the disk tier is off on WASM — no persistent FS.)

### TypeScript worker parity (`test-ts-worker.mjs`)

The `worker:` transport is worker-language-agnostic. `test-ts-worker.mjs` proves a
**TypeScript** worker (vgi-typescript) serves over the same SAB channel at parity with
the Rust `sabtable` worker: a direct `vgi_table_function('worker:ts-worker-boot.js',
'ts_count', [5])` (producer mode), `ATTACH` + discovery + `tcat.main.ts_count(3)`, a
`ts_double` scalar (exchange mode), and a **true-parallelism** proof (`peakConcurrency=4`
— `ts_probe`/`ts_peek`). The worker is `vgi-typescript/examples/sab-worker/boot.ts` — it
drives vgi-rpc's `serveStream` over a browser `ByteSink` writer and is **truly parallel**:
one dedicated sub-Worker per SAB slot (real threads sharing the SAB), matching the Rust
worker's emscripten thread-per-slot.

```bash
# build the TS worker bundle (from vgi-typescript), copy to ts-worker-mod.js here:
(cd <vgi-typescript> && bun build examples/sab-worker/boot.ts --outdir examples/sab-worker/dist --target browser --format esm)
cp <vgi-typescript>/examples/sab-worker/dist/boot.js test/support/wasm-worker/ts-worker-mod.js
VGI_ENTRY=test-ts-worker.mjs node serve.mjs 8799   # then open in a COI browser
```

`ts-worker-boot.js` (a hand-written classic shim, tracked) dynamic-imports the ESM
`ts-worker-mod.js` bundle (a build artifact, gitignored). Needs vgi-rpc-typescript ≥ the
`ByteSink` serve writable and vgi-typescript's `serveStream` widened to accept it.

**Reliability.** The transport's *logic* is covered reliably + fast by the native
`[sab]/[sab-e2e]/[sab-conn]` unit tests (count_to + multi-batch + error over the in-process ring
analog) — those are the gate. This browser runner is the only test of the *real* browser stack
(engine + wasm extension + worker module + bridge + actual SAB/Atomics), so it's valuable, but
headless Chromium under puppeteer can occasionally session-close under the nested-worker load —
re-run, or drive a real browser (it passes cleanly there). It's best-effort integration coverage,
not a flake-free CI gate.
