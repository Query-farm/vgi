# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(vgi
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 0de1997810f22888511efd277ed4775ece295561
)

# Any extra extensions that should be built
duckdb_extension_load(json)
duckdb_extension_load(icu)


duckdb_extension_load(ducklake
    GIT_URL https://github.com/Query-farm/ducklake
    GIT_TAG e2ee16d580403eb2fd23ca277566776f27bdbf68
#    INCLUDE_DIR src/ducklake
    LOAD_TESTS
)



#duckdb_extension_load(spatial
#    GIT_URL https://github.com/duckdb/duckdb_spatial
#    GIT_TAG 4295b9b9a1b5a16b0a6c07880356ff3c4a21e676
#    INCLUDE_DIR src/spatial
#    LOAD_TESTS
#)
