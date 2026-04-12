PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=vgi
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Auto-detect vcpkg toolchain if present in the project tree
VCPKG_TOOLCHAIN_PATH ?= $(wildcard ${PROJ_DIR}vcpkg/scripts/buildsystems/vcpkg.cmake)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# VGI test worker (subprocess transport)
VGI_TEST_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-example-worker

# Subprocess transport tests
.PHONY: test_subprocess test_subprocess_debug test_http test_http_debug test_all test_all_debug

test_subprocess:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" VGI_TEST_WORKER="$(VGI_TEST_WORKER)" ./build/release/test/unittest "test/*"

test_subprocess_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" VGI_TEST_WORKER="$(VGI_TEST_WORKER)" ./build/debug/test/unittest "test/*"

# HTTP transport tests (uses test/run_http_integration.sh)
test_http:
	./test/run_http_integration.sh "test/sql/integration/*"

test_http_debug:
	BUILD_DIR=debug ./test/run_http_integration.sh "test/sql/integration/*"

# HTTP bearer auth tests
test_http_bearer:
	./test/run_http_bearer_integration.sh "test/sql/integration/bearer_auth/*"

test_http_bearer_debug:
	BUILD_DIR=debug ./test/run_http_bearer_integration.sh "test/sql/integration/bearer_auth/*"

# Run all transports
test_all: test_subprocess test_http test_http_bearer

test_all_debug: test_subprocess_debug test_http_debug test_http_bearer_debug