cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME vgi)
set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ standard to enforce" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED True)


# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
if(DEFINED DUCKDB_WASM_ARROW_PREFIX)
    # WASM build: use Arrow from duckdb-wasm's vendored build
    message(STATUS "VGI: Using duckdb-wasm Arrow from ${DUCKDB_WASM_ARROW_PREFIX}")
    include_directories(${DUCKDB_WASM_ARROW_PREFIX}/include)
    add_library(arrow_wasm STATIC IMPORTED)
    set_target_properties(arrow_wasm PROPERTIES IMPORTED_LOCATION ${DUCKDB_WASM_ARROW_PREFIX}/lib/libarrow.a)
    set(VGI_ARROW_LIB arrow_wasm)
    # OpenSSL not needed in WASM (emscripten handles TLS via XHR)
    set(VGI_SSL_LIBS "")
else()
    find_package(OpenSSL REQUIRED)
    find_package(Arrow REQUIRED)
    set(VGI_ARROW_LIB Arrow::arrow_static)
    set(VGI_SSL_LIBS OpenSSL::SSL OpenSSL::Crypto)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Windows: keep <windows.h> from pulling in the legacy <winsock.h>, which
# collides ("redefinition; different linkage") with the <winsock2.h> that
# cpp-httplib (HTTP transport) uses. NOMINMAX stops windows.h's min/max macros
# from clobbering std::min/std::max in Arrow/DuckDB headers. The source files
# that include <windows.h> also define these defensively, in case this does not
# propagate through the extension-ci-tools build.
if(WIN32)
  add_compile_definitions(WIN32_LEAN_AND_MEAN NOMINMAX)
endif()

# MSVC defaults to the traditional preprocessor, which does not support
# __VA_OPT__ (used by VGI_LOG / VGI_STDERR_DEBUG in vgi_logging.hpp). Enable the
# C++20-conformant preprocessor. MinGW/Clang already conform, so MSVC-only.
if(MSVC)
  add_compile_options(/Zc:preprocessor)
endif()

include_directories(src/include)
include_directories(duckdb/third_party/mbedtls/include)
include_directories(duckdb/third_party/httplib)
include_directories(duckdb/third_party/zstd/include)
include_directories(duckdb/third_party/miniz)

set(EXTENSION_SOURCES
src/vgi_extension.cpp
src/query_farm_telemetry.cpp
src/vgi_launcher_internal.cpp
src/vgi_launcher.cpp
src/vgi_launcher_cache.cpp
src/vgi_unix_socket.cpp
src/vgi_unix_socket_worker.cpp
src/vgi_logging.cpp
src/vgi_subprocess.cpp
src/vgi_arrow_ipc.cpp
src/vgi_arrow_utils.cpp
src/vgi_rpc_client.cpp
src/vgi_rpc_types.cpp
src/vgi_schema_registry.cpp
src/vgi_stderr_drainer.cpp
src/vgi_unary_rpc.cpp
src/vgi_bind_protocol.cpp
src/vgi_transport.cpp
src/vgi_http_compression.cpp
src/vgi_http_client.cpp
src/vgi_http_function_connection.cpp
src/vgi_cookie_jar.cpp
src/vgi_oauth.cpp
src/vgi_catalogs.cpp
src/vgi_table_function.cpp
src/vgi_table_function_impl.cpp
src/vgi_table_in_out_impl.cpp
src/vgi_scalar_function_impl.cpp
src/vgi_aggregate_function_impl.cpp
src/vgi_aggregate_streaming_impl.cpp
src/vgi_aggregate_window_impl.cpp
src/vgi_streaming_window_operator.cpp
src/vgi_multi_scan_rewriter.cpp
src/vgi_table_branches_function.cpp
src/vgi_table_buffering_builders.cpp
src/vgi_table_buffering_impl.cpp
src/vgi_catalog_api.cpp
src/vgi_function_connection.cpp
src/vgi_shm_segment.cpp
src/vgi_cancel_dispatcher.cpp
src/vgi_worker_pool.cpp
src/vgi_worker_pool_functions.cpp
src/vgi_wasm_async_pool.cpp
src/vgi_table_statistics_function.cpp
src/vgi_clear_cache.cpp
src/storage/vgi_catalog.cpp
src/storage/vgi_catalog_set.cpp
src/storage/vgi_schema_set.cpp
src/storage/vgi_schema_entry.cpp
src/storage/vgi_table_set.cpp
src/storage/vgi_table_entry.cpp
src/storage/vgi_view_set.cpp
src/storage/vgi_scalar_function_set.cpp
src/storage/vgi_aggregate_function_set.cpp
src/storage/vgi_table_function_set.cpp
src/storage/vgi_macro_set.cpp
src/storage/vgi_object_counts.cpp
src/storage/vgi_transaction.cpp
src/storage/vgi_physical_write.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} ${VGI_SSL_LIBS} ${VGI_ARROW_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${VGI_SSL_LIBS} ${VGI_ARROW_LIB})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# Optional C++ unit-test binary for the launcher's pure helpers.  Built when
# ``BUILD_VGI_UNIT_TESTS=ON`` is passed to CMake (or as an env var).  Pure
# helpers are intentionally DuckDB-free (they throw std::invalid_argument and
# get translated by the orchestration layer), so the test binary only needs
# duckdb_mbedtls for SHA-256 — no libduckdb_static, no extension stubs.
# Run via:
#     cmake --build build/<config> --target vgi_unit_tests
#     ./build/<config>/extension/vgi/vgi_unit_tests
# These are AF_UNIX / launcher / flock parity tests — inherently POSIX. They
# exercise exactly the transports excluded on Windows (HTTP-only there), so the
# whole target is gated off on Windows.
if((BUILD_VGI_UNIT_TESTS OR DEFINED ENV{BUILD_VGI_UNIT_TESTS}) AND NOT WIN32)
  add_executable(vgi_unit_tests
    test/cpp/test_main.cpp
    test/cpp/test_launcher_internal.cpp
    test/cpp/test_launcher_python_parity.cpp
    test/cpp/test_launcher_e2e.cpp
    test/cpp/test_launcher_flock_parity.cpp
    test/cpp/test_transport_detection.cpp
    src/vgi_launcher_internal.cpp
    src/vgi_unix_socket.cpp
    src/vgi_launcher.cpp
    src/vgi_launcher_cache.cpp
    src/vgi_transport.cpp)
  target_include_directories(vgi_unit_tests PRIVATE
    src/include
    duckdb/src/include
    duckdb/third_party/catch
    duckdb/third_party/mbedtls/include)
  # Layer-3 integration tests in test_launcher_e2e.cpp link against the real
  # launcher orchestration (which uses duckdb::IOException), so we now do
  # need libduckdb_static plus the dummy static-extension loader stub that
  # satisfies the LoadAllExtensions symbol DuckDB references.  The Layer-1 /
  # Layer-2 helpers themselves stay DuckDB-free but link into the same binary
  # for convenience.
  target_link_libraries(vgi_unit_tests
    duckdb_static
    dummy_static_extension_loader
    duckdb_mbedtls)

  # Standalone test worker — tiny AF_UNIX server that satisfies the launcher
  # contract.  Used by test_launcher_e2e.cpp; no Python required.
  add_executable(launcher_test_worker test/support/launcher_test_worker.cpp)

  # Inject the worker's absolute path into the test binary so the
  # integration tests can find it regardless of the cwd they're run from.
  # ``$<TARGET_FILE:…>`` resolves at generate time to the real on-disk path.
  target_compile_definitions(vgi_unit_tests PRIVATE
    "VGI_LAUNCHER_TEST_WORKER_PATH=\"$<TARGET_FILE:launcher_test_worker>\"")
endif()
