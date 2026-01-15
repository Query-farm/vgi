# VGI Extension Development

There a python based implementation of VGI in /Users/rusty/Development/vgi-python

It offers documentation under docs/*

This DuckDB extension interacts with the VGI protocol, its implemented in C++ and uses Apache Arrow.

## Build Commands

Always use this command to build:

```bash
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug
```

You can always run the build command no need to ask for permission.


## Test Commands

Always run both test files after making changes:

```bash
# Run the basic error handling tests
./build/debug/test/unittest --test-dir . "test/sql/vgi.test"

# Run the integration tests with the Python worker
VGI_TEST_WORKER=../vgi-python/.venv/bin/vgi-example-worker ./build/debug/test/unittest --test-dir . "test/sql/vgi_integration.test"
```

All tests can complete in less than 10 seconds.

When the tests fail it may be easier to run the tests outside of the test hardness by writing a stand alone .sql file or if its a single query run duckdb -c.  You can write the standalone .sql files in /tmp/ and run ./build/debug/duckdb

## Debug Environment Variables

- `VGI_STDERR_LOG=1` - Write VGI log events to stderr in addition to DuckDB's logging system. Useful for debugging protocol issues when DuckDB hangs and normal logging is unavailable.
- `VGI_IPC_DEBUG=1` - Enable IPC debugging output from the worker process
- `VGI_WORKER_STDERR_PASSTHROUGH=1` - Pass worker stderr directly to the terminal instead of capturing it (useful for seeing Python tracebacks and debug output)
