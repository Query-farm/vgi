# Compile inventory only. The runner verifies that this checkout's duckdb path
# resolves to the selected 2.0 engine, including third-party include directories.
# The full extension_config.cmake also loads the pinned 2.0 httpfs; this lane
# deliberately excludes it to isolate VGI translation-unit diagnostics.
duckdb_extension_load(vgi SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR})
