# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(vgi
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# DuckDB 2.0's duckdb_extension_load() only builds an extension; link it into
# the shell and unittest as 1.5 did, so scripts can SET vgi_* settings before
# the first ATTACH. Distribution builds pass STATICALLY_LINK_EXTENSIONS, which
# overrides this list.
duckdb_extension_statically_link(vgi)

# The v2.0 branch builds against Haybarn's DuckDB 2.0 engine. Pin its matching
# HTTP stack so the extension and httpfs share the same engine APIs.
set(HTTPFS_GIT_URL https://github.com/Query-farm-haybarn/haybarn-httpfs)
set(HTTPFS_GIT_TAG d312888956c2edef09c68a42f3fdb28e1dd55acf)

# httpfs is curl-based. On the MinGW toolchain
# (x64-mingw-static, rtools GCC) it fails to link against static curl with
# `undefined reference to __imp_curl_*` — curl's headers emit DLL-import symbols
# unless CURL_STATICLIB is defined when httpfs compiles, and that flag can't be
# injected into httpfs's separate CMake subtree from here. MSVC / Linux / WASM
# link fine. So skip httpfs on MinGW; the `require httpfs` tests simply skip there.
if(NOT MINGW)
    duckdb_extension_load(httpfs
        LOAD_TESTS
        GIT_URL ${HTTPFS_GIT_URL}
        GIT_TAG ${HTTPFS_GIT_TAG}
    )
    duckdb_extension_statically_link(httpfs)
endif()

# Any extra extensions that should be built
duckdb_extension_load(json)
#duckdb_extension_load(icu)


#duckdb_extension_load(ducklake
#    GIT_URL https://github.com/Query-farm/ducklake
#    GIT_TAG e2ee16d580403eb2fd23ca277566776f27bdbf68
#    INCLUDE_DIR src/ducklake
#    LOAD_TESTS
#)



#duckdb_extension_load(spatial
#    GIT_URL https://github.com/duckdb/duckdb_spatial
#    GIT_TAG 4295b9b9a1b5a16b0a6c07880356ff3c4a21e676
#    INCLUDE_DIR src/spatial
#    LOAD_TESTS
#)
