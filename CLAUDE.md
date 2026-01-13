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
