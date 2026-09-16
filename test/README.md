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
