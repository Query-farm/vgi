# VGI — Vector Gateway Interface for DuckDB

**VGI** (Vector Gateway Interface) is an Apache Arrow–based protocol for extending DuckDB
using *any* language — no C++/C/Rust/Zig and no compilation or linking required.

This repository contains the **DuckDB extension** (the C++ side of VGI). It loads into
DuckDB and lets you `ATTACH` a *worker* — a program written in Python, TypeScript, Go, or
anything that speaks the protocol — and then call the scalar, table, and aggregate
functions that worker exposes as if they were native DuckDB functions. Data moves between
DuckDB and the worker over Apache Arrow IPC, across a subprocess pipe, an HTTP connection,
or a Unix domain socket.

The reference worker SDK is **vgi-python** — `pip install vgi`.

Created by [Query.Farm](https://query.farm).

## Why VGI?

| Traditional DuckDB extensions | VGI workers |
|-------------------------------|-------------|
| C/C++ compilation required | Any language — write a script or ship a binary |
| Tied to a specific DuckDB version | Version independent |
| Complex build / release cycle | Ship a script or executable |
| Runs in-process | Process isolation |
| Single-threaded by default | Parallel pooled workers |

**Use cases:** call REST APIs from SQL, run ML inference (PyTorch, scikit-learn),
process data with Python/pandas/numpy, build custom ETL transforms, or expose external
data sources as queryable tables and views.

## Quick Start

Install the extension and attach a worker:

```sql
-- First time only
INSTALL vgi FROM community;
LOAD vgi;

-- Attach a worker as a catalog
ATTACH 'my_funcs' (TYPE vgi, LOCATION './my_worker.py');

-- Call the functions it exposes
SELECT upper_case(name) FROM users;
SELECT * FROM my_table_function('arg');
```

A minimal Python worker (using `vgi-python`):

```python
# my_worker.py
from typing import Annotated
import pyarrow as pa
import pyarrow.compute as pc
from vgi import ScalarFunction, Param, Returns, Worker


class UpperCase(ScalarFunction):
    """Convert string values to uppercase."""

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="String value to uppercase")],
    ) -> Annotated[pa.StringArray, Returns()]:
        return pc.utf8_upper(value)


class MyWorker(Worker):
    catalog_name = "my_funcs"
    functions = [UpperCase]


if __name__ == "__main__":
    MyWorker().run()
```

## Features

- **Function shapes** — scalar, table, table-in-out (streaming), buffered table
  (see-every-row-before-output), aggregate, and windowed aggregate functions, all defined
  in the worker and surfaced as native DuckDB functions.
- **Full catalog integration** — workers expose schemas, tables, views, and functions;
  DuckDB lazily loads catalog metadata, column statistics, and supports multi-branch
  (UNION-ALL) tables.
- **Multiple transports** — `LOCATION` accepts a bare command (subprocess, pooled per
  DuckDB process), `http(s)://` (HTTP), `unix:///path/to.sock` (AF_UNIX), or
  `launch:<argv>` (launcher-managed shared worker).
- **Pushdown** — projection, filter, `ORDER BY` + `LIMIT`, and join-key pushdown to
  workers that opt in.
- **Worker pooling** — subprocess workers are pooled and reused across queries, with
  diagnostics (`vgi_worker_pool()`, `vgi_worker_pool_stats()`).
- **Performance** — optional POSIX shared-memory transport for zero-copy batch transfer.
- **Auth** — per-catalog OAuth / bearer tokens; OIDC identity introspection via
  `vgi_catalog_identity()`.

See the [`docs/`](.) directory for deep dives on
[multi-branch tables](multi_branch.md),
[the launcher protocol](launcher-protocol.md),
[catalog profiling](catalog_profiling.md), and more.

## Building

This extension uses VCPKG for dependency management and is built with multiple modules,
so set `USE_MERGED_VCPKG_MANIFEST=1`. The Makefile auto-detects the VCPKG toolchain from
`vcpkg/` in the project tree.

```sh
git clone --recurse-submodules https://github.com/Query-farm/vgi.git
cd vgi

# Debug build
USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make debug

# Release build
USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make release
```

Installing [ccache](https://ccache.dev/) and [ninja](https://ninja-build.org/) is
strongly recommended for fast incremental rebuilds.

The build produces:

```sh
./build/release/duckdb                                    # DuckDB shell with the extension preloaded
./build/release/test/unittest                             # test runner
./build/release/extension/vgi/vgi.duckdb_extension        # the loadable extension
```

## Testing

The extension supports two transports; subprocess is the faster default. Tests prefer the
release build:

```sh
# Subprocess transport (default, faster)
make test_subprocess

# HTTP transport
make test_http

# Both
make test_all
```

The `VGI_TEST_WORKER` environment variable controls which worker is used and defaults to
the `vgi-python` fixture worker. See the project root `CLAUDE.md` for the full matrix of
test targets, debug builds, and environment variables.

## License

This project is licensed under the **Query Farm Source-Available License, Version 1.0**
(Licensor: Query Farm LLC). Non-production use is freely permitted, as is production use —
except for offering a Competing Offering or operating a Commercial Marketplace built on
the Licensed Work, which require a separate commercial license. Each version converts to
**Apache 2.0** on its Change Date (the tenth anniversary of that version's first public
release). See [`LICENSE`](../LICENSE) for the full terms.

For commercial or custom licensing, contact hello@query.farm.
