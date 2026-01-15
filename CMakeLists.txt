cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME vgi)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED 1)


# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
src/vgi_extension.cpp
src/query_farm_telemetry.cpp
src/vgi_logging.cpp
src/vgi_subprocess.cpp
src/vgi_arrow_ipc.cpp
src/vgi_arrow_utils.cpp
src/vgi_protocol.cpp
src/vgi_catalogs.cpp
src/vgi_table_function.cpp
src/vgi_table_function_impl.cpp
src/vgi_catalog_api.cpp
src/vgi_worker_pool.cpp
src/vgi_worker_pool_functions.cpp
src/storage/vgi_catalog.cpp
src/storage/vgi_catalog_set.cpp
src/storage/vgi_schema_set.cpp
src/storage/vgi_schema_entry.cpp
src/storage/vgi_table_set.cpp
src/storage/vgi_table_entry.cpp
src/storage/vgi_view_set.cpp
src/storage/vgi_scalar_function_set.cpp
src/storage/vgi_table_function_set.cpp
src/storage/vgi_transaction.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Arrow::arrow_static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Arrow::arrow_static)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
