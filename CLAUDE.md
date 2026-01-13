# VGI Extension Development

## Build Commands

Always use this command to build:

```bash
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug
```

## Test Commands

Always run both test files after making changes:

```bash
# Run the basic error handling tests
./build/debug/test/unittest --test-dir . "test/sql/vgi.test"

# Run the integration tests with the Python worker
VGI_TEST_WORKER=../vgi-python/.venv/bin/vgi-example-worker ./build/debug/test/unittest --test-dir . "test/sql/vgi_integration.test"
```

All tests can complete in less than 10 seconds.

## Debug Environment Variables

- `VGI_IPC_DEBUG=1` - Enable IPC debugging output from the worker process
- `VGI_WORKER_STDERR_PASSTHROUGH=1` - Pass worker stderr directly to the terminal instead of capturing it (useful for seeing Python tracebacks and debug output)
