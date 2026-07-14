# Browser e2e for the VGI `worker:` SAB transport

End-to-end test of the in-browser `worker:` transport in a real cross-origin-isolated
headless Chromium. Exercises the full stack — the haybarn DuckDB-WASM engine (with the
VGI slot-stub js-library), the VGI wasm extension (`WebWorkerFunctionConnection` +
`WebWorkerInvokeUnary`), and the VGI worker module (real Rust `serve()` over the SAB
duplex-ring channel) — asserting:

1. `LOAD vgi` — the extension's `vgi_wasm_slot_*` imports resolve against the engine at `dlopen`.
2. **direct** — `vgi_table_function('worker:...', 'count_to', [5])` ⇒ `[0..4]`.
3. **ATTACH** — `ATTACH 'worker:...'` → catalog discovery → `wcat.main.count_to(3)` ⇒ `[0..2]`
   (catalog RPCs over the unary-over-SAB path).
4. **multi-threaded** — 4 concurrent `worker:` scans on separate connections (`SET threads=4`),
   served in parallel by one serve pthread per channel slot.

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
directly after a wasm build. The transport's native half is covered by the `[sab]/[sab-e2e]/
[sab-conn]` unit tests.
