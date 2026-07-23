# In-browser `worker:` (SAB) transport

Run a VGI worker **entirely in the browser** — in a Web Worker, exchanging Arrow batches with
the DuckDB-WASM extension over a **SharedArrayBuffer duplex-ring channel** — with **no server**.
This is the WASM-only sibling of the subprocess transport: same streaming `IFunctionConnection`
paths, only the byte transport differs (SAB rings + `Atomics` instead of an OS pipe).

```sql
ATTACH 'worker:/workers/example.js' AS w (TYPE vgi);
SELECT * FROM w.main.count_to(5);
SELECT * FROM w.main.some_table;
```

`LOCATION 'worker:<url>'` is any URL `new Worker(url)` accepts (same-origin by default; the
host bridge gates which URLs SQL may spawn). The worker's JS entry is either a Rust
`-pthread`/emscripten worker or a plain JS/TS worker that speaks the slot protocol.

## How it works (one paragraph)

The channel is `malloc`'d **inside DuckDB's own wasm linear memory** (a `SharedArrayBuffer` under
`-pthread`), so every DuckDB scan pthread reaches it natively and the VGI worker gets a view of it
via one `postMessage` at spawn. It holds a header + N **slots**; each slot is a duplex pair of
SPSC byte rings (client→worker, worker→client) with blocking `Atomics.wait`/`notify` flow control
— the ring *is* the chunker, so batches larger than a ring stream through in bounded pieces. A
connection claims a slot for its lifetime; the worker runs one serve thread per slot. Full wire
contract: [sab_transport_abi.md](sab_transport_abi.md).

## Multithreading

Parallel table-function scans work: a scan whose worker advertises `max_workers > 1` fans out
across DuckDB scan threads, each acquiring its own slot + its own worker serve pthread, so N
serves run **concurrently**. The browser E2E proves this — a single `parallel_probe(max_workers=4)`
scan under `SET threads=4` reaches an observed **peak concurrency of 4** (a process-global guard in
the worker's shared memory counts simultaneously-active serves). Note: multiple *separate* queries
serialize at DuckDB-WASM's single-threaded `AsyncDuckDB` worker boundary — parallelism comes from
*one* fanned-out scan, not concurrent connections.

## Build

Three independently-built repos against the shared ABI:

- **Extension** (`vgi`): built into the DuckDB-WASM COI build — `build-wasm-coi.sh` (SAB files are
  `#if defined(__EMSCRIPTEN__)`). Native `[sab-conn]`/`[sab-e2e]` unit tests link the same code
  with `-DVGI_SAB_NATIVE_TEST` against a POSIX-shm backend.
- **Host glue** (`haybarn-wasm`): `lib/js-stubs.js` (the `--js-library` ring stubs) +
  `packages/duckdb-wasm-app/src/lib/vgi-webworker-bridge.ts` (main-thread spawn bridge). Editing
  `js-stubs.js` needs an engine re-link (`rm build/.../duckdb_wasm.{wasm,js}` — CMake misses
  `--js-library` deps).
- **Worker** (`vgi-rust` → `test/support/sabtable` + `test/support/wasm-worker/build.sh`): the Rust
  serve framework built for `wasm32-unknown-emscripten -pthread`. `-Z build-std` needs
  `RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-args=-pthread'` or
  wasm-ld rejects `--shared-memory`.

## Testing

- **Native (the fast gate):** `[sab-conn]`/`[sab-e2e]` in `vgi_unit_tests` cover the transport
  logic (bind→init→stream, multi-batch, worker error) over the in-process ring backend — but
  single-threaded, so they cannot exercise the browser threading/heap-view paths.
- **Browser E2E** (`test/support/wasm-worker/browser-e2e/`): the only test of the real stack
  (engine + wasm extension + worker module + bridge + actual SAB/Atomics). Reliably green across
  fresh loads: `LOAD` → direct scan → multi-batch → ATTACH+discovery → concurrent → worker-error →
  parallel-serve proof (`maxConcurrency=4`). Headless Chromium under puppeteer can session-close
  under nested-worker load; MCP Playwright / a real browser is stable. `probe-*.mjs` are isolation
  harnesses (`VGI_ENTRY=probe-throw.mjs node serve.mjs`).

## Reliability notes

The transport is reliable on the happy path (streaming, catalog, concurrent scans, parallel
serve) **and** on the worker-error path under `threads > 1` (see the fix below). Two
emscripten-pthread footguns are handled and documented in
[sab_transport_abi.md](sab_transport_abi.md) → *Browser implementation notes* (stale cached heap
views → read `wasmMemory.buffer`; per-realm channel offset → re-publish before each ring op) —
getting either wrong causes intermittent, scheduler-dependent hangs.

### Fixed: worker-error deadlock under `threads > 1` (a DuckDB-WASM engine bug)

When a worker **threw** under `threads > 1` amid heavy concurrent load, DuckDB-WASM *occasionally*
(≈2/3 of fresh loads) **deadlocked the whole engine** — the runtime worker thread stayed frozen
inside the query C++ call (even a fresh-connection `SELECT 42` hung), so the query promise never
settled. This was **not** a transport bug: the transport tears down cleanly on the error (all slots
freed, rings closed, worker idle — verified), and a generic non-VGI parallel throw settles fine.

Root cause: on a query error the DuckDB-WASM runtime **main thread** busy-spins
`Executor::CancelTasks` (`while (executor_tasks > 0) WorkOnTasks()`). With its own producer queue
empty, that loop pure-spins on the atomic without hitting any cancellation point, **starving the
main thread's proxied-call queue** (emscripten issue
[#3495](https://github.com/emscripten-core/emscripten/issues/3495)). A worker pthread whose
in-flight task makes a main-thread-proxied call during teardown then blocks forever; its
`ExecutorTask` never destructs, `executor_tasks` sticks (observed wedged at `1`), and the drain
never converges.

**Fix** (haybarn DuckDB fork, `src/parallel/executor.cpp`): pump
`emscripten_main_thread_process_queued_calls()` inside that spin — the documented workaround for
exactly this lock-free-loop pattern — so the proxied call completes, the pthread task finishes, and
the drain converges. Verified across many fresh loads with the race provably occurring
(`executor_tasks` briefly `1`) and draining every time. The E2E now runs the worker-error case
under `threads=4`; `test/support/wasm-worker/browser-e2e/repro-threads-deadlock.mjs` is the
regression harness.
