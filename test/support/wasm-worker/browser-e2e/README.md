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

### Known limitation: flaky worker-error propagation under `threads > 1` + heavy load

Case 5 (the worker-error case) runs under `SET threads=1`. This is **not** a transport bug — it's
a narrowly-scoped, flaky DuckDB-WASM-side error-propagation race, characterized carefully:

- The SAB transport tears down **cleanly** on a worker error under `threads=4` (channel
  diagnostics show all slots freed, both rings closed, worker idle) — the exception *is* delivered.
- A worker throw under `threads=4` propagates fine **in isolation** — even after a warm scan +
  ATTACH + a concurrent 4-connection case (see `probe-vgi-boom.mjs`).
- A generic (non-VGI) parallel throw propagates fine under `threads=4` too (see `probe-throw.mjs`).
- **Only** under the *full* suite's concurrency/timing does a worker throw under `threads>1`
  *occasionally* (≈2/3 of fresh loads) hang. When it does, the diagnostics show the **entire engine
  is frozen**: a trivial `SELECT 42` on a *fresh* connection also times out, so DuckDB-WASM's worker
  thread is deadlocked **inside** the boom query's C++ call and never returns to settle the promise —
  after the transport already tore down cleanly. It is a DuckDB-WASM parallel-error-handling deadlock,
  exercised by (not caused by) the error path. **Confirmed not the transport:** making the transport's
  error path fully non-blocking (no `Atomics.wait` during the C++ exception unwind) did **not** fix it,
  and it does not reproduce in the focused `probe-vgi-boom.mjs` (same prefix + boom in isolation).
  Fixing it is DuckDB-WASM-internals work, not transport work.

`threads=1` makes the error path deterministic. Everything else — including the parallel-serve proof
(case 7, `maxConcurrency=4`) and all the happy-path streaming/catalog/concurrent cases — runs green
under `threads=4`. The `probe-*.mjs` files (run via `VGI_ENTRY=probe-*.mjs node serve.mjs`) are the
isolation harness used to characterize this.

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

**Reliability.** The transport's *logic* is covered reliably + fast by the native
`[sab]/[sab-e2e]/[sab-conn]` unit tests (count_to + multi-batch + error over the in-process ring
analog) — those are the gate. This browser runner is the only test of the *real* browser stack
(engine + wasm extension + worker module + bridge + actual SAB/Atomics), so it's valuable, but
headless Chromium under puppeteer can occasionally session-close under the nested-worker load —
re-run, or drive a real browser (it passes cleanly there). It's best-effort integration coverage,
not a flake-free CI gate.
