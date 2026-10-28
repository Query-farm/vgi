# Vendored nanoarrow bundle (see NANOARROW_SOURCE): Arrow IPC encode/decode for the
# wire (src/vgi_nanoarrow_ipc.cpp).
#
# Compiled as C11 so shared buffers use stdatomic refcounts: decoded batches are
# released on DuckDB threads. Its own directory scope keeps the include path to
# exactly this bundle plus vcpkg's zstd/lz4 -- DuckDB's vendored zstd.h (C++,
# namespaced) is a directory-level -I in the parent scopes and would otherwise
# outrank vcpkg's -isystem.
enable_language(C)
# IPC body compression (zstd, lz4-frame) comes from vcpkg on native builds. The WASM
# build has no vcpkg dependencies, so it reads and writes uncompressed bodies only.
if(NOT EMSCRIPTEN)
  find_package(zstd CONFIG REQUIRED)
  find_package(lz4 CONFIG REQUIRED)
  if(TARGET zstd::libzstd_static)
    set(VGI_ZSTD_LIB zstd::libzstd_static)
  else()
    set(VGI_ZSTD_LIB zstd::libzstd)
  endif()
endif()

add_library(vgi_nanoarrow STATIC src/nanoarrow.c src/nanoarrow_ipc.c src/flatcc.c)
set_target_properties(vgi_nanoarrow PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  POSITION_INDEPENDENT_CODE ON
  INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/include
  INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(EMSCRIPTEN AND USE_WASM_THREADS)
  target_compile_options(vgi_nanoarrow PRIVATE -pthread)
endif()
if(NOT EMSCRIPTEN)
  target_compile_definitions(vgi_nanoarrow PRIVATE NANOARROW_IPC_WITH_ZSTD NANOARROW_IPC_WITH_LZ4)
  target_link_libraries(vgi_nanoarrow PRIVATE ${VGI_ZSTD_LIB} lz4::lz4)
endif()
# Shared buffers are released on DuckDB worker threads: refcounts must be atomic.
# MSVC defines __STDC_NO_ATOMICS__ (so nanoarrow would fall back to plain counters)
# unless C11 atomics are enabled explicitly.
if(MSVC)
  target_compile_options(vgi_nanoarrow PRIVATE /experimental:c11atomics)
endif()
