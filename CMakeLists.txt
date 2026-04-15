cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME vgi)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED 1)


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
include_directories(src/include)
include_directories(duckdb/third_party/mbedtls/include)
include_directories(duckdb/third_party/httplib)
include_directories(duckdb/third_party/zstd/include)

set(EXTENSION_SOURCES
src/vgi_extension.cpp
src/query_farm_telemetry.cpp
src/vgi_logging.cpp
src/vgi_subprocess.cpp
src/vgi_arrow_ipc.cpp
src/vgi_arrow_utils.cpp
src/vgi_rpc_client.cpp
src/vgi_rpc_types.cpp
src/vgi_bind_protocol.cpp
src/vgi_transport.cpp
src/vgi_http_client.cpp
src/vgi_http_function_connection.cpp
src/vgi_oauth.cpp
src/vgi_catalogs.cpp
src/vgi_table_function.cpp
src/vgi_table_function_impl.cpp
src/vgi_table_in_out_impl.cpp
src/vgi_scalar_function_impl.cpp
src/vgi_aggregate_function_impl.cpp
src/vgi_catalog_api.cpp
src/vgi_function_connection.cpp
src/vgi_worker_pool.cpp
src/vgi_worker_pool_functions.cpp
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
