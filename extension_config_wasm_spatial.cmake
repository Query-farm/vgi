# Optional spatial extension entry — opt in via build-wasm-coi.sh --with-spatial.
duckdb_extension_load(spatial
    GIT_URL https://github.com/duckdb/duckdb_spatial
    GIT_TAG b5ea138
)
