# WASM extension config
# - Arrow headers/lib come from duckdb-wasm's vendored build, not vcpkg
#   Set DUCKDB_WASM_ARROW_PREFIX to the duckdb-wasm arrow install dir

duckdb_extension_load(vgi
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

duckdb_extension_load(ducklake
    # Use local checkout (branch feature/vgi-metadata-manager) — it has the
    # patches needed to support catalog version 1.0 while still building
    # cleanly against DuckDB v1.5.1 (no table_index.hpp dep, no duplicate
    # AddFilterToPushdownInfo).
    SOURCE_DIR /Users/rusty/Development/ducklake
    LOAD_TESTS
)

duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    # v1.5.1-aligned: "Bump duckdb to v1.5.1" commit (2026-03-25). This matches
    # the DuckDB version our duckdb-wasm wrapper (lib/) was authored against
    # during the emsdk 5.0.4 upgrades.
    GIT_TAG 0de1997
)

duckdb_extension_load(parquet)
duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(autocomplete)
