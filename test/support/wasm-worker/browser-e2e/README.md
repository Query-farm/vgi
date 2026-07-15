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
7. **concurrency diagnostic (not a gate)** — 4 concurrent `slow_count` scans, then
   `peek_max_concurrency()` reads the process-global peak number of simultaneously-active serves
   (tracked by a Drop guard in shared worker memory). This is **reported, not asserted**: with
   these single-producer fixtures + DuckDB-WASM's `AsyncDuckDB` serializing multi-connection
   queries at its single-threaded worker boundary, the observed peak is **1** — the "concurrent"
   scans serialize, so the multi-thread serve (correct, one pthread per slot) is not exercised in
   parallel here. Exercising it in parallel needs a workload DuckDB parallelizes across scan
   threads (one scan → >1 slot at once); the diagnostic is emitted so that can be checked later.

The fixtures (`count_to`, `emit_batches`, `boom`, `slow_count`, `peek_max_concurrency`) live in
`../../sabtable/src/lib.rs`; cases 2–4 are also covered natively by the `[sab-conn]` unit test.

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
  wasm staticlib: `cd ../sabtable && cargo +nightly build --target wasm32-unknown-emscripten
  -Z build-std=std,panic_abort --release`).
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
