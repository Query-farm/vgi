# Vendored SQLite amalgamation

`sqlite3.c` / `sqlite3.h` are the official SQLite **amalgamation**, version
**3.46.1** (2024-08-13), downloaded verbatim from
<https://sqlite.org/2024/sqlite-amalgamation-3460100.zip>.

## Why vendored

The per-value memo **disk tier** (`src/vgi_sqlite_backend.cpp`) needs an embedded,
same-host multi-process, crash-safe KV. SQLite in WAL mode is that. Vendoring the
single-file amalgamation makes the disk tier build identically on macOS, Linux, and
**Windows** — none of which can be relied on to ship a linkable system `libsqlite3`
(Windows has none). It also pins the exact version so a system-library upgrade can't
silently change on-disk behaviour.

Compiled by `CMakeLists.txt` into the `vgi_sqlite3` static library and linked into
both extension targets whenever `NOT EMSCRIPTEN` (WASM has no filesystem tier, so it
falls back to the memory-only null stub). Build options are set in `CMakeLists.txt`
(`SQLITE_THREADSAFE=1`, WAL defaults, `OMIT_LOAD_EXTENSION`, `DQS=0`).

## Updating

Download a newer amalgamation zip from <https://sqlite.org/download.html>, replace
`sqlite3.c` and `sqlite3.h` (drop `shell.c` and `sqlite3ext.h` — unused), and bump
the version noted above. Nothing else references the tree layout.
