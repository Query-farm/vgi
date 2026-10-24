# Compile inventory only. The runner verifies that this checkout's duckdb path
# resolves to the selected 2.0 engine, including third-party include directories.
# The default extension_config.cmake and its 1.5 dependencies remain unchanged.
duckdb_extension_load(vgi SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR})
