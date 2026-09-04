cmake_minimum_required(VERSION 3.5)

# Honor CMAKE_MSVC_RUNTIME_LIBRARY (set below for MSVC). Must be NEW *before*
# project() enables CXX; our cmake_minimum_required(3.5) would otherwise pin it
# OLD in this scope, leaving our objects on the static CRT (/MT) while the
# x64-windows-static-md vcpkg deps use the dynamic CRT (/MD) — an LNK2038 clash.
if(POLICY CMP0091)
  cmake_policy(SET CMP0091 NEW)
endif()

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

# Native Iroh is compiled into the DuckDB extension as a Rust static library.
# Emscripten keeps using the application-owned browser adapter. Release builds
# build from pinned source; local developers may point at a sibling checkout or
# disable the native transport for C++-only iteration.
if(EMSCRIPTEN)
  set(VGI_ENABLE_IROH OFF CACHE BOOL "Embed native Iroh transport" FORCE)
else()
  option(VGI_ENABLE_IROH "Embed native Iroh transport" ON)
endif()

if(VGI_ENABLE_IROH)
  set(VGI_IROH_SOURCE_DIR "" CACHE PATH
      "Optional path to a local vgi-rpc-rust source tree")
  if(NOT VGI_IROH_SOURCE_DIR)
    include(FetchContent)
    FetchContent_Declare(vgi_iroh_source
      GIT_REPOSITORY https://github.com/Query-farm/vgi-rpc-rust.git
      GIT_TAG 9e7dfb59686682be0ff4a46183988e2724416393
      GIT_SHALLOW FALSE)
    FetchContent_GetProperties(vgi_iroh_source)
    if(NOT vgi_iroh_source_POPULATED)
      FetchContent_Populate(vgi_iroh_source)
    endif()
    set(VGI_IROH_SOURCE_DIR "${vgi_iroh_source_SOURCE_DIR}")
  endif()
  if(NOT EXISTS "${VGI_IROH_SOURCE_DIR}/vgi-iroh-cabi/Cargo.toml")
    message(FATAL_ERROR
      "VGI_ENABLE_IROH requires vgi-iroh-cabi source. Set VGI_IROH_SOURCE_DIR "
      "to a pinned vgi-rpc-rust checkout or configure -DVGI_ENABLE_IROH=OFF.")
  endif()

  set(VGI_IROH_CARGO_ENV)
  if(APPLE)
    # rustc otherwise defaults native objects to the host OS version, which can
    # make a distributable extension unloadable on older supported macOS hosts.
    # Apple Silicon starts at macOS 11; use an explicitly configured higher
    # deployment target when the enclosing DuckDB build supplies one.
    set(VGI_IROH_MACOS_DEPLOYMENT_TARGET "${CMAKE_OSX_DEPLOYMENT_TARGET}")
    if(NOT VGI_IROH_MACOS_DEPLOYMENT_TARGET)
      set(VGI_IROH_MACOS_DEPLOYMENT_TARGET "11.0")
    endif()
    list(APPEND VGI_IROH_CARGO_ENV
      "MACOSX_DEPLOYMENT_TARGET=${VGI_IROH_MACOS_DEPLOYMENT_TARGET}")
  endif()
  find_program(VGI_CARGO_EXECUTABLE cargo REQUIRED)

  if(CMAKE_CROSSCOMPILING)
    message(FATAL_ERROR
      "Native VGI Iroh currently requires a native Rust build host; CMake cross-compilation "
      "is not yet supported. Configure with -DVGI_ENABLE_IROH=OFF for unsupported builders.")
  elseif(APPLE)
    if(CMAKE_OSX_ARCHITECTURES MATCHES "x86_64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
      set(VGI_IROH_RUST_TARGET x86_64-apple-darwin)
    elseif(CMAKE_OSX_ARCHITECTURES MATCHES "arm64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
      set(VGI_IROH_RUST_TARGET aarch64-apple-darwin)
    else()
      message(FATAL_ERROR "Native VGI Iroh supports only x86_64 and arm64 macOS")
    endif()
  elseif(WIN32)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "ARM|arm|aarch64")
      message(FATAL_ERROR "Native VGI Iroh does not yet support Windows ARM")
    endif()
    if(MINGW)
      set(VGI_IROH_RUST_TARGET x86_64-pc-windows-gnu)
      set(VGI_IROH_MINGW_LINKER "" CACHE FILEPATH
          "GCC linker for the embedded Rust x86_64-pc-windows-gnu target")
      set(VGI_IROH_MINGW_AR "" CACHE FILEPATH
          "Archiver for Rust C dependencies on x86_64-pc-windows-gnu")

      # The extension's MinGW build may intentionally use RTools, whose copied
      # target-prefixed GCC does not provide libgcc_eh. Rust's GNU target needs
      # that library, so select its linker independently and validate the
      # runtime rather than relying on PATH ordering.
      set(VGI_IROH_MINGW_LINKER_CANDIDATES)
      if(VGI_IROH_MINGW_LINKER)
        list(APPEND VGI_IROH_MINGW_LINKER_CANDIDATES
             "${VGI_IROH_MINGW_LINKER}")
      else()
        list(APPEND VGI_IROH_MINGW_LINKER_CANDIDATES
          "C:/mingw64/bin/gcc.exe"
          "C:/msys64/mingw64/bin/gcc.exe"
          "C:/msys64/ucrt64/bin/gcc.exe"
          "${CMAKE_C_COMPILER}")
        find_program(VGI_IROH_MINGW_PATH_LINKER
          NAMES x86_64-w64-mingw32-gcc gcc)
        if(VGI_IROH_MINGW_PATH_LINKER)
          list(APPEND VGI_IROH_MINGW_LINKER_CANDIDATES
               "${VGI_IROH_MINGW_PATH_LINKER}")
        endif()
      endif()

      set(VGI_IROH_MINGW_VALID_LINKER "")
      foreach(VGI_IROH_MINGW_LINKER_CANDIDATE IN LISTS VGI_IROH_MINGW_LINKER_CANDIDATES)
        if(NOT EXISTS "${VGI_IROH_MINGW_LINKER_CANDIDATE}")
          continue()
        endif()
        execute_process(
          COMMAND "${VGI_IROH_MINGW_LINKER_CANDIDATE}"
                  -print-file-name=libgcc_eh.a
          RESULT_VARIABLE VGI_IROH_MINGW_LINKER_RESULT
          OUTPUT_VARIABLE VGI_IROH_MINGW_LIBGCC_EH
          ERROR_QUIET
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(VGI_IROH_MINGW_LINKER_RESULT EQUAL 0
           AND NOT VGI_IROH_MINGW_LIBGCC_EH STREQUAL "libgcc_eh.a"
           AND EXISTS "${VGI_IROH_MINGW_LIBGCC_EH}")
          set(VGI_IROH_MINGW_VALID_LINKER
              "${VGI_IROH_MINGW_LINKER_CANDIDATE}")
          break()
        endif()
      endforeach()
      if(NOT VGI_IROH_MINGW_VALID_LINKER)
        message(FATAL_ERROR
          "Native VGI Iroh requires a MinGW GCC with libgcc_eh for Rust's "
          "x86_64-pc-windows-gnu target. Set VGI_IROH_MINGW_LINKER to a "
          "compatible gcc executable (for example C:/mingw64/bin/gcc.exe).")
      endif()
      set(VGI_IROH_MINGW_LINKER "${VGI_IROH_MINGW_VALID_LINKER}"
          CACHE FILEPATH
          "GCC linker for the embedded Rust x86_64-pc-windows-gnu target"
          FORCE)

      if(NOT VGI_IROH_MINGW_AR)
        get_filename_component(VGI_IROH_MINGW_TOOLCHAIN_DIR
                               "${VGI_IROH_MINGW_LINKER}" DIRECTORY)
        foreach(VGI_IROH_MINGW_AR_NAME
                ar.exe x86_64-w64-mingw32-ar.exe)
          if(EXISTS "${VGI_IROH_MINGW_TOOLCHAIN_DIR}/${VGI_IROH_MINGW_AR_NAME}")
            set(VGI_IROH_MINGW_AR
                "${VGI_IROH_MINGW_TOOLCHAIN_DIR}/${VGI_IROH_MINGW_AR_NAME}")
            break()
          endif()
        endforeach()
      endif()
      if(NOT EXISTS "${VGI_IROH_MINGW_AR}")
        message(FATAL_ERROR
          "Could not find the archiver matching ${VGI_IROH_MINGW_LINKER}. "
          "Set VGI_IROH_MINGW_AR to its ar executable.")
      endif()
      execute_process(
        COMMAND "${VGI_IROH_MINGW_AR}" --version
        RESULT_VARIABLE VGI_IROH_MINGW_AR_RESULT
        OUTPUT_QUIET ERROR_QUIET)
      if(NOT VGI_IROH_MINGW_AR_RESULT EQUAL 0)
        message(FATAL_ERROR
          "VGI_IROH_MINGW_AR is not executable: ${VGI_IROH_MINGW_AR}")
      endif()
      set(VGI_IROH_MINGW_AR "${VGI_IROH_MINGW_AR}"
          CACHE FILEPATH
          "Archiver for Rust C dependencies on x86_64-pc-windows-gnu"
          FORCE)
      list(APPEND VGI_IROH_CARGO_ENV
        "CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=${VGI_IROH_MINGW_LINKER}"
        "CC_x86_64_pc_windows_gnu=${VGI_IROH_MINGW_LINKER}"
        "AR_x86_64_pc_windows_gnu=${VGI_IROH_MINGW_AR}")
    else()
      set(VGI_IROH_RUST_TARGET x86_64-pc-windows-msvc)
      # The enclosing DuckDB/Haybarn build uses /MT. Keep Rust on the same
      # static CRT so one extension never crosses allocator/CRT boundaries.
      # Preserve any target-specific caller flags rather than replacing them.
      set(VGI_IROH_MSVC_RUSTFLAGS "$ENV{CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_RUSTFLAGS}")
      string(STRIP "${VGI_IROH_MSVC_RUSTFLAGS} -Ctarget-feature=+crt-static"
             VGI_IROH_MSVC_RUSTFLAGS)
      list(APPEND VGI_IROH_CARGO_ENV
        "CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_RUSTFLAGS=${VGI_IROH_MSVC_RUSTFLAGS}")
    endif()
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(CMAKE_CXX_COMPILER_TARGET MATCHES "musl")
      message(FATAL_ERROR "Native VGI Iroh does not yet support musl targets")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
      set(VGI_IROH_RUST_TARGET aarch64-unknown-linux-gnu)
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
      set(VGI_IROH_RUST_TARGET x86_64-unknown-linux-gnu)
    else()
      message(FATAL_ERROR "Native VGI Iroh supports only x86_64 and arm64 glibc Linux")
    endif()
  else()
    message(FATAL_ERROR "Native VGI Iroh is currently supported on macOS, Windows x86_64, and glibc Linux")
  endif()

  set(VGI_IROH_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/vgi-iroh-cargo")
  if(WIN32 AND NOT MINGW)
    set(VGI_IROH_STATIC_LIB
      "${VGI_IROH_CARGO_TARGET_DIR}/${VGI_IROH_RUST_TARGET}/release/vgi_iroh_cabi.lib")
  else()
    set(VGI_IROH_STATIC_LIB
      "${VGI_IROH_CARGO_TARGET_DIR}/${VGI_IROH_RUST_TARGET}/release/libvgi_iroh_cabi.a")
  endif()
  file(GLOB_RECURSE VGI_IROH_RUST_INPUTS CONFIGURE_DEPENDS
    "${VGI_IROH_SOURCE_DIR}/vgi-iroh-cabi/*"
    "${VGI_IROH_SOURCE_DIR}/vgi-iroh-transport/*")
  list(APPEND VGI_IROH_RUST_INPUTS
    "${VGI_IROH_SOURCE_DIR}/Cargo.toml"
    "${VGI_IROH_SOURCE_DIR}/Cargo.lock")
  add_custom_command(
    OUTPUT "${VGI_IROH_STATIC_LIB}"
    COMMAND "${CMAKE_COMMAND}" -E env ${VGI_IROH_CARGO_ENV}
            "${VGI_CARGO_EXECUTABLE}" build --locked --release
            --manifest-path "${VGI_IROH_SOURCE_DIR}/Cargo.toml"
            --package vgi-iroh-cabi --target "${VGI_IROH_RUST_TARGET}"
            --target-dir "${VGI_IROH_CARGO_TARGET_DIR}"
    DEPENDS ${VGI_IROH_RUST_INPUTS}
    WORKING_DIRECTORY "${VGI_IROH_SOURCE_DIR}"
    COMMENT "Building embedded VGI Iroh transport (${VGI_IROH_RUST_TARGET})"
    VERBATIM)
  add_custom_target(vgi_iroh_rust DEPENDS "${VGI_IROH_STATIC_LIB}")
  add_library(vgi_iroh_cabi STATIC IMPORTED GLOBAL)
  set_target_properties(vgi_iroh_cabi PROPERTIES IMPORTED_LOCATION "${VGI_IROH_STATIC_LIB}")
  add_dependencies(vgi_iroh_cabi vgi_iroh_rust)
  include_directories("${VGI_IROH_SOURCE_DIR}/vgi-iroh-cabi/include")
  add_compile_definitions(VGI_ENABLE_IROH=1)
endif()

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
  # Use the *static* CRT (/MT), matching duckdb (duckdb/CMakeLists.txt sets the
  # same value) and the static vcpkg deps. The extension links both duckdb_static
  # and arrow/openssl/boost into one loadable module, so all three must share a
  # CRT; LNK2038 otherwise. This requires the vcpkg deps to be built with a
  # static-CRT triplet — x64-windows-static (NOT x64-windows-static-md, whose -md
  # forces the dynamic CRT). Requires CMP0091=NEW (set above).
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

include_directories(src/include)
include_directories(duckdb/third_party/mbedtls/include)
include_directories(duckdb/third_party/httplib)
include_directories(duckdb/third_party/zstd/include)
include_directories(duckdb/third_party/miniz)

set(EXTENSION_SOURCES
src/vgi_extension.cpp
src/query_farm_telemetry.cpp
src/vgi_telemetry.cpp
src/vgi_telemetry_util.cpp
src/vgi_launcher_internal.cpp
src/vgi_launcher.cpp
src/vgi_launcher_cache.cpp
src/vgi_unix_socket.cpp
src/vgi_unix_socket_worker.cpp
src/vgi_named_pipe.cpp
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
src/vgi_container_runtime.cpp
src/vgi_http_compression.cpp
src/vgi_http_client.cpp
src/vgi_http_function_connection.cpp
src/vgi_iroh_config.cpp
src/vgi_iroh_native.cpp
src/vgi_cookie_jar.cpp
src/vgi_oauth.cpp
src/vgi_oauth_env.cpp
src/vgi_oauth_store.cpp
src/vgi_catalogs.cpp
src/vgi_table_function_impl.cpp
src/vgi_table_in_out_impl.cpp
src/vgi_scalar_function_impl.cpp
src/vgi_aggregate_function_impl.cpp
src/vgi_aggregate_streaming_impl.cpp
src/vgi_aggregate_window_impl.cpp
src/vgi_streaming_window_operator.cpp
src/vgi_lateral_batch_operator.cpp
src/vgi_multi_scan_rewriter.cpp
src/vgi_table_branches_function.cpp
src/vgi_function_arguments_function.cpp
src/vgi_copy_from_impl.cpp
src/vgi_copy_to_impl.cpp
src/vgi_table_buffering_builders.cpp
src/vgi_table_buffering_impl.cpp
src/vgi_catalog_api.cpp
src/vgi_function_connection.cpp
src/vgi_client_timing.cpp
src/vgi_shm_segment.cpp
src/vgi_cancel_dispatcher.cpp
src/vgi_worker_pool.cpp
src/vgi_worker_pool_functions.cpp
src/vgi_result_cache.cpp
src/vgi_exchange_cache_key.cpp
src/vgi_memo_arena.cpp
src/vgi_sqlite_backend.cpp
src/vgi_sha256.cpp
src/vgi_cache_identity.cpp
src/vgi_input_dedup.cpp
src/vgi_result_cache_functions.cpp
src/vgi_cached_replay_connection.cpp
src/vgi_github.cpp
src/vgi_database_worker.cpp
src/vgi_worker_package_functions.cpp
src/vgi_global_functions.cpp
src/vgi_github_functions.cpp
src/vgi_wasm_async_pool.cpp
src/vgi_table_statistics_function.cpp
src/vgi_clear_cache.cpp
src/vgi_secret_storage.cpp
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

# The browser `worker:` SAB transport (WebWorkerFunctionConnection +
# SabInputStream/SabOutputStream) is emscripten-only: it resolves the
# vgi_wasm_slot_* extern "C" stubs against the engine's --js-library at dlopen.
# On native builds these files live only in the vgi_unit_tests target (compiled
# with -DVGI_SAB_NATIVE_TEST against the in-process ring backend); the shipped
# native/loadable extension must not pull them in (undefined stubs). See
# docs/sab_transport_abi.md and the WASM worker-transport design.
if(EMSCRIPTEN)
  list(APPEND EXTENSION_SOURCES
    src/vgi_webworker_function_connection.cpp
    src/vgi_sab_stream.cpp)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} ${VGI_SSL_LIBS} ${VGI_ARROW_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${VGI_SSL_LIBS} ${VGI_ARROW_LIB})

if(VGI_ENABLE_IROH)
  add_dependencies(${EXTENSION_NAME} vgi_iroh_rust)
  add_dependencies(${LOADABLE_EXTENSION_NAME} vgi_iroh_rust)
  target_link_libraries(${EXTENSION_NAME} vgi_iroh_cabi)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} vgi_iroh_cabi)
  if(APPLE)
    target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation"
                          "-framework SystemConfiguration")
    target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework Security" "-framework CoreFoundation"
                          "-framework SystemConfiguration")
  elseif(WIN32)
    # Rust's stdlib and Tokio use the Windows profile and native APIs directly.
    # These dependencies are not emitted transitively by a Rust staticlib.
    target_link_libraries(${EXTENSION_NAME} Bcrypt Crypt32 Userenv Ntdll)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} Bcrypt Crypt32 Userenv Ntdll)
  endif()
endif()

# SQLite-backed per-value memo disk tier (same-host multi-process + cross-restart).
# sqlite3 comes from vcpkg (see vcpkg.json), like every other native dependency, so it
# builds identically on macOS/Linux/Windows. Native only: WASM has no filesystem tier
# (the cache dir is forced empty), so the backend compiles to a null stub there.
if(NOT EMSCRIPTEN)
  find_package(unofficial-sqlite3 CONFIG REQUIRED)
  foreach(tgt ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
    target_compile_definitions(${tgt} PRIVATE VGI_ENABLE_SQLITE)
    target_link_libraries(${tgt} unofficial::sqlite3::sqlite3)
  endforeach()
endif()

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
    test/cpp/test_container_runtime.cpp
    test/cpp/test_oauth.cpp
    test/cpp/test_telemetry.cpp
    test/cpp/test_request_schema_parity.cpp
    test/cpp/test_sab_stream.cpp
    test/cpp/test_sab_e2e.cpp
    test/cpp/test_sab_conn.cpp
    test/support/vgi_sab_native_ring.cpp
    test/support/vgi_sab_test_stubs.cpp
    test/support/vgi_database_worker_test_stubs.cpp
    src/vgi_sab_stream.cpp
    src/vgi_webworker_function_connection.cpp
    src/vgi_rpc_client.cpp
    src/vgi_rpc_types.cpp
    src/vgi_bind_protocol.cpp
    src/vgi_arrow_utils.cpp
    src/vgi_result_cache.cpp
    src/vgi_sha256.cpp
    src/vgi_cache_identity.cpp
    src/vgi_table_buffering_builders.cpp
    src/vgi_schema_registry.cpp
    src/vgi_arrow_ipc.cpp
    src/vgi_logging.cpp
    src/vgi_telemetry_util.cpp
    src/vgi_oauth_env.cpp
    src/vgi_launcher_internal.cpp
    src/vgi_unix_socket.cpp
    src/vgi_launcher.cpp
    src/vgi_launcher_cache.cpp
    src/vgi_transport.cpp
    src/vgi_container_runtime.cpp
    src/vgi_subprocess.cpp
    src/vgi_unix_socket_worker.cpp)
  target_include_directories(vgi_unit_tests PRIVATE
    src/include
    duckdb/src/include
    duckdb/third_party/catch
    duckdb/third_party/mbedtls/include)
  # Compile WebWorkerFunctionConnection (SAB transport) natively against the
  # in-process ring backend for the runtime e2e; its production form is
  # emscripten-only.
  target_compile_definitions(vgi_unit_tests PRIVATE VGI_SAB_NATIVE_TEST)
  # Layer-3 integration tests in test_launcher_e2e.cpp link against the real
  # launcher orchestration (which uses duckdb::IOException), so we now do
  # need libduckdb_static plus the dummy static-extension loader stub that
  # satisfies the LoadAllExtensions symbol DuckDB references.  The Layer-1 /
  # Layer-2 helpers themselves stay DuckDB-free but link into the same binary
  # for convenience.
  # Rust FFI worker staticlib for the cross-language SAB e2e (test_sab_e2e.cpp).
  # Built out-of-band via `cargo build --release` in test/support/sabffi.
  set(SABFFI_LIB
    ${CMAKE_CURRENT_SOURCE_DIR}/test/support/sabffi/target/release/libsabffi.a)
  # Table-function worker staticlib (vgi crate) for the WebWorkerFunctionConnection
  # runtime e2e. Built via `cargo build --release` in test/support/sabtable.
  set(SABTABLE_LIB
    ${CMAKE_CURRENT_SOURCE_DIR}/test/support/sabtable/target/release/libsabtable.a)
  target_link_libraries(vgi_unit_tests
    duckdb_static
    dummy_static_extension_loader
    duckdb_mbedtls
    ${VGI_ARROW_LIB}
    ${SABFFI_LIB}
    ${SABTABLE_LIB})
  if(APPLE)
    # Rust std / chrono's iana_time_zone dep in libsabffi references CoreFoundation.
    target_link_libraries(vgi_unit_tests "-framework CoreFoundation")
  endif()

  # Standalone test worker — tiny AF_UNIX server that satisfies the launcher
  # contract.  Used by test_launcher_e2e.cpp; no Python required.
  add_executable(launcher_test_worker test/support/launcher_test_worker.cpp)

  # Inject the worker's absolute path into the test binary so the
  # integration tests can find it regardless of the cwd they're run from.
  # ``$<TARGET_FILE:…>`` resolves at generate time to the real on-disk path.
  target_compile_definitions(vgi_unit_tests PRIVATE
    "VGI_LAUNCHER_TEST_WORKER_PATH=\"$<TARGET_FILE:launcher_test_worker>\"")
endif()
