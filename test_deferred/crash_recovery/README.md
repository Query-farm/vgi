# Crash-recovery tests (disabled)

These two sqllogictest files used to live in `test/sql/integration/table_in_out/`:

- `table_buffering_worker_crash.test`: the worker SIGKILLs itself inside
  `table_buffering.process`; the query must fail with `RPC response stream EOF`
  and the next query on the same catalog must succeed on a fresh worker.
- `table_buffering_pool_recovery.test`: two such crashes in a row; the worker
  pool must drop the dead workers (pool `misses` grows) and keep serving.

They were taken out of the default suite, and out of every SDK's CI, because
they cost minutes per lane and caught little. Their `crash_on_process` fixture
kills the process serving it, so they can only run over the subprocess
transport (one private worker per DuckDB process). That forced each SDK
harness to run them in a separate step. The fast error-propagation tests
(`table_buffering_combine_crash.test`, `table_buffering_finalize_crash.test`)
stay in the integration suite.

The SDK example workers still register `crash_on_process`, so the files can be
run by hand.

## Running them

The unittest binary only discovers files under `<test-dir>/test/`, so stage
them first. `VGI_TEST_WORKER` must be a bare worker command (subprocess
transport), never `launch:`, `http://`, `https://` or `unix://`: on those the
SIGKILL takes down a shared worker. `VGI_TEST_DEDICATED_WORKER=1` confirms you
have done that; without it the files skip.

```sh
stage="$(mktemp -d)"; mkdir -p "$stage/test"
cp test_deferred/crash_recovery/*.test "$stage/test/"
cd "$stage"
VGI_TEST_WORKER="/path/to/vgi-fixture-worker" \
VGI_TEST_DEDICATED_WORKER=1 \
  /path/to/unittest --test-dir "$stage" "test/*"
```

Use `build/release/test/unittest` from a local build, or a native runner's
`unittest`. Any SDK example worker works, for example vgi-python's
`.venv/bin/vgi-fixture-worker` or vgi-rust's `target/debug/vgi-example-worker`.
