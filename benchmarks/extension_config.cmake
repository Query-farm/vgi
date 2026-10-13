# The subprocess/SHM benchmark needs VGI and core SQL only. A separate config
# avoids downloading unrelated extensions; the normal production config stays intact.
duckdb_extension_load(vgi SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/.. DONT_LINK)
duckdb_extension_load(json)
