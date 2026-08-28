# Database-backed worker packages (`database://`)

`database://` stores a VGI worker executable or archive in a DuckDB table. At
`ATTACH`, VGI reads exactly one platform/version row, verifies its SHA-256,
installs it in an immutable local cache, and runs its entrypoint through the
ordinary subprocess transport.

```sql
CREATE TABLE worker_packages AS
SELECT * FROM vgi_worker_package(
    '/build/acme-worker.tar.gz',
    'acme-worker',
    '1.4.0',
    entrypoint := 'bin/acme-worker');

ALTER TABLE worker_packages ADD PRIMARY KEY
    (worker_name, platform, package_version);

ATTACH 'acme' AS acme (
    TYPE vgi,
    LOCATION 'database://memory/main/worker_packages/acme-worker?package_version=1.4.0');
```

The URI shape is:

```text
database://catalog/schema/table/worker?package_version=version[#sha256=64-hex-digest]
```

All four path components and the version are percent-decoded. Use percent
encoding when an identifier or worker name contains `/`, `?`, `#`, or `%`.
Catalog, schema, and table names are quoted as identifiers; worker, platform,
and version are bound parameters, so user-provided coordinates are not pasted
into predicates.

## Package table contract

The selected table must expose these columns:

| Column | Type | Meaning |
|---|---|---|
| `worker_name` | `VARCHAR` | Stable package name. |
| `platform` | `VARCHAR` | Exact value returned by `pragma_platform()`. |
| `package_version` | `VARCHAR` | Publisher-selected immutable package version. |
| `package_format` | `VARCHAR` | `executable`, `zip`, `tar`, `tar.gz`, `tar.zst`, `gzip`, or `zstd`. Aliases `raw`, `binary`, `tgz`, `gz`, and `zst` are accepted. |
| `entrypoint` | `VARCHAR` | Relative executable path after extraction. |
| `contents` | `BLOB` | Complete executable/archive bytes. |
| `sha256` | `VARCHAR` | Lower- or upper-case 64-character SHA-256 of `contents`. |

`created_at` is useful publisher metadata and is emitted by the packaging
macro, but the resolver does not require it. A primary key or unique constraint
on `(worker_name, platform, package_version)` is strongly recommended. VGI
counts every matching row and fails if the lookup is ambiguous.

The resolver uses a dedicated connection to avoid recursively executing SQL on
the active `ATTACH` connection. Consequently, the registry must be a committed,
persistent table; temporary tables and uncommitted changes are intentionally
not visible.

## Packaging macro

`vgi_worker_package(path, worker_name, package_version, package_format :=
'auto', entrypoint := NULL, platform := NULL)` is a table macro over DuckDB's
`read_blob()` and `pragma_platform()` primitives. Insert or materialize its
single row using normal SQL:

```sql
INSERT INTO worker_packages
SELECT * FROM vgi_worker_package(
    'dist/acme-worker-${VERSION}.zip',
    'acme-worker',
    '${VERSION}',
    entrypoint := 'acme-worker.exe');
```

The path must resolve to exactly one non-empty file. `auto` recognizes `.zip`,
`.tar`, `.tar.gz`, `.tar.zst`, `.gz`, and `.zst`; everything else is a raw
executable. Archives require an explicit entrypoint. `platform` defaults to the
current DuckDB platform and can be overridden when cross-packaging.

## Cache and cleanup

The cache identity is SHA-256 over the verified content digest, canonical
format, and entrypoint. Installation uses a private staging directory followed
by an atomic rename, so concurrent processes converge on one immutable copy.

Each attached catalog and every running or pooled subprocess holds a shared
cross-process lease. Cleanup first requests the exclusive lease and skips an
artifact if any process still uses it. This prevents `DETACH`, explicit cleanup,
or another DuckDB process from deleting an executing worker.

VGI updates `last_used` on cache hits and performs a best-effort prune after
each database package resolution. Entries older than the TTL are removed;
size-based cleanup evicts least-recently-used entries until the cache falls
below 80% of its configured maximum. Operators can inspect or trigger the same
logic explicitly:

```sql
SELECT * FROM vgi_worker_cache();
SELECT * FROM vgi_worker_cache_prune();
SELECT * FROM vgi_worker_cache_flush();
```

`vgi_worker_cache_flush()` is lease-aware: it reports `skipped_in_use` rather
than killing or racing a worker. Flush the subprocess pool or detach catalogs
before retrying when immediate deletion is required.

| Setting | Default | Meaning |
|---|---:|---|
| `vgi_worker_cache_dir` | empty | Empty follows `vgi_github_cache_dir`, then `${XDG_CACHE_HOME:-~/.cache}/vgi/releases`. The filesystem must permit execution. |
| `vgi_worker_cache_max_bytes` | 5 GiB | Managed artifact budget; `0` disables size eviction. |
| `vgi_worker_cache_ttl_seconds` | 30 days | Unused-age limit; `0` disables TTL eviction. |
| `vgi_worker_package_max_bytes` | 512 MiB | Maximum stored/compressed `contents` size. |
| `vgi_worker_package_max_extracted_bytes` | 1 GiB | Maximum decompressed package size. |
| `vgi_worker_package_max_files` | 10,000 | Maximum archive entries. |

## Security properties

- The row digest is always recomputed before cache lookup. An optional URI
  `#sha256=` pin makes the expected artifact part of the caller's configuration.
- Cached entrypoints are executed directly, never through a command shell.
- Archive paths are relative, normalized, and cannot contain traversal,
  backslashes, drive prefixes, or links. Extraction size and file count are
  bounded before installation.
- Cache directories and files are private to the current OS user on POSIX.

This feature executes native code stored in the selected database. Only attach
package registries whose writers you trust, and use the digest pin when the
expected build is known out of band.
