# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(vgi
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# httpfs source depends on the engine we're building against. The Haybarn
# engine fork defines HAYBARN_VERSION_STRING (a top-level CMake var, set before
# extension configs are included); stock DuckDB never does. Build against the
# matching httpfs in each case — they have genuinely different sources.
if(DEFINED HAYBARN_VERSION_STRING)
    set(HTTPFS_GIT_URL https://github.com/Query-farm-haybarn/haybarn-httpfs)
    set(HTTPFS_GIT_TAG 1f23e5bd8f7a50253c08b00bb7a88ecfa15862df)
else()
    set(HTTPFS_GIT_URL https://github.com/duckdb/duckdb-httpfs)
    set(HTTPFS_GIT_TAG 52afb4204a3238d6ee132e83340f8d68c40ee91c)
endif()

# DuckDB 1.5.3's httpfs (52afb42) is curl-based. On the MinGW toolchain
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
