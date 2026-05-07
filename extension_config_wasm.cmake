# WASM extension config
# - Skip httpfs (DuckDB-WASM has its own HTTP layer)
# - Arrow headers/lib come from duckdb-wasm's vendored build, not vcpkg
#   Set DUCKDB_WASM_ARROW_PREFIX to the duckdb-wasm arrow install dir

duckdb_extension_load(vgi
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(autocomplete)
