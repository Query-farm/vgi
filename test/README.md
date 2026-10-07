# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the vgi extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or
```bash
make test_debug
```

**Every integration lane passes `--test-config test/configs/no_error_skip.json`.**
Without a config, DuckDB's sqllogictest runner turns any error whose text
contains `HTTP` or `Unable to connect` into a SKIP and exits 0. Over the HTTP
transport every worker error contains `HTTP`, so a lane without the config
reports real failures, and a worker that died mid-run, as skips. The test/run_*.sh
scripts, the Makefile's direct `unittest` calls and `scripts/run_tests.py`
(by default) all pass it. A lane outside this repo that runs this suite (the
SDK repos' CI) should pass the same file.

## Windows OAuth credential storage

Configure the extension build with `-DBUILD_VGI_OAUTH_STORE_TESTS=ON`, then run:

```powershell
cmake --build build/release --config Release --target test_vgi_oauth_store
```

This target needs Python 3 and runs the production credential store in separate
Windows processes. It checks DPAPI persistence and replacement, concurrent
updates, recovery after a lock owner crashes, and deletion errors in both
`auto` and `persistent` modes. An isolated `LOCALAPPDATA` directory contains only
synthetic credentials and is removed after each test. The suite does not contact
an OAuth provider or access existing user credentials.

To rerun an already-built probe directly:

```powershell
python test/test_oauth_store_windows.py --probe build/release/extension/vgi/vgi_oauth_store_probe.exe
```
