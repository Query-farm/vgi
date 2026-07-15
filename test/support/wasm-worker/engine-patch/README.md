# Engine patch — DuckDB-WASM `CancelTasks` proxied-call pump

The VGI `worker:` SAB transport runs client-side workers on emscripten pthreads and
exchanges Arrow batches over SharedArrayBuffer duplex rings. Under `SET threads > 1`, a
worker that **throws** mid-scan used to *occasionally* (~2/3 of fresh loads) deadlock the
whole DuckDB-WASM engine — the runtime worker thread stayed frozen inside the query C++
call (even a fresh-connection `SELECT 42` hung), so the query promise never settled, even
though the transport had already torn down cleanly.

## Root cause (DuckDB-WASM engine, not the transport)

On a query error the DuckDB-WASM runtime **main thread** busy-spins `Executor::CancelTasks`:

```cpp
while (executor_tasks > 0) {
    WorkOnTasks();
}
```

With this thread's own producer queue empty, `WorkOnTasks()` returns instantly and the loop
pure-spins on the atomic without hitting any cancellation point — which **starves the main
thread's proxied-call queue** (emscripten issue
[#3495](https://github.com/emscripten-core/emscripten/issues/3495)). A worker pthread whose
in-flight task makes a main-thread-proxied call during teardown then blocks forever, its
`ExecutorTask` never destructs, `executor_tasks` sticks (observed wedged at `1`), and the
drain never converges.

## The fix

`executor-cancel-tasks-emscripten-pump.patch` pumps
`emscripten_main_thread_process_queued_calls()` inside that spin (the documented workaround
for exactly this lock-free-loop pattern) so the proxied call completes and the drain
converges.

## Applying it

The patch targets the **haybarn DuckDB fork** used by the DuckDB-WASM (haybarn-wasm) build —
`submodules/duckdb/src/parallel/executor.cpp`. The haybarn build carries its DuckDB patches
as working-tree modifications on a pinned upstream commit; this is one of them. Apply from
that submodule root:

```bash
cd <haybarn-wasm>/submodules/duckdb
git apply <this-dir>/executor-cancel-tasks-emscripten-pump.patch
```

Then rebuild the engine + re-bundle (see `../browser-e2e/README.md` → Prerequisites).

## Verifying

`../browser-e2e/repro-threads-deadlock.mjs` is the regression harness — it loops the trigger
and asserts the engine never freezes. Without the patch it hangs ~2/3 of loads; with it, the
race still occurs (`executor_tasks` briefly `1`) but drains every time.
