# `COPY ... TO` custom formats (VGI worker writers)

A VGI catalog can advertise custom **`COPY ... TO` formats**, turning a worker into a
remote sink. DuckDB streams the source rows out to the worker, which writes them to a
destination (a proprietary format, a remote API/object store, a custom sink), and
DuckDB reports the row count:

```sql
ATTACH 'acme' AS acme (TYPE vgi, LOCATION 'launch:acme-worker');

COPY (SELECT * FROM events) TO 's3://bucket/out.weird' (FORMAT 'acme.weird_out', codec 'zstd');
-- worker `acme` encodes/writes the rows to the destination; DuckDB reports the count.
```

Scope: **`COPY ... TO` only** (the read direction is [`docs/copy_from.md`](copy_from.md)).
A single format may support **both** directions (see *Direction* below).

## Naming, discovery, options

Identical to COPY FROM: format names are **scoped by the attach alias**
(`<alias>.<format>`, collision-free since aliases are unique per DB), discovered at
ATTACH via the same RPC, and introspected with `vgi_copy_formats()` — filter
`WHERE direction = 'to'`. Options are the writer's `Arg`-annotated arguments; the
extension's bind rejects unknown options and coerces types, while the worker enforces
required/choices/ranges. The destination `path` is supplied by the COPY statement, not
an option.

**Ordering.** By default the sink is parallel/sharded and rows arrive in no particular order. A
writer that needs rows in **source order** declares `Meta.sink_order_dependent = True`; the extension
then uses a single-threaded sink (`REGULAR_COPY_TO_FILE`), so one worker receives every batch in
source order (when `preserve_insertion_order` is on, the default), trading write parallelism for
order. The `ordered` column of `vgi_copy_formats()` reports which formats are single-thread/ordered.

**Unsupported write modes.** `PARTITION_BY`, `PER_THREAD_OUTPUT`, and file rotation
(`FILE_SIZE_BYTES`) are rejected with a clear error — VGI writes a single destination.

**Row count.** DuckDB reports `rows_copied` = rows *fed to* the sink (not rows the
worker confirms persisted); a filtering writer's reported count may differ from what it
actually wrote.

## How it works: a writer is a buffered (Sink+Combine) function

COPY TO reuses the buffered-table machinery with **no Source phase**. DuckDB's
`PhysicalCopyToFile` drives the extension's `copy_to_*` callbacks, which map to the
worker's existing `table_buffering_process` / `table_buffering_combine` RPCs:

| DuckDB callback | maps to |
|---|---|
| `copy_to_initialize_global` (once) | acquire one worker, `PerformInit` → mint `execution_id` (retained as the combine worker) |
| `copy_to_sink` (per chunk, parallel) | per-thread worker → `table_buffering_process(batch)` → shard `state_id` |
| `copy_to_combine` (per thread) | hand the worker + `state_id` to the global state (no RPC) |
| `copy_to_finalize` (once) | `table_buffering_combine(all state_ids)` on the retained worker → worker's `combine()` does the terminal merge + write + close |

`initialize_global` runs once before any sink, so the `execution_id` is published
race-free — no init handshake is needed. Empty-input COPY still calls combine (with an
empty `state_id` list) on the retained init worker, so an empty/header file is written.

**Cross-process invariant.** `process()` (write) and `combine()` (close) may run on
different worker processes (pool rotation / HTTP). The writer MUST keep shards in
`execution_id`-scoped storage (`params.storage`) — or write to a concurrency-tolerant
destination — so `combine()` can read every shard. Buffering on `self` breaks under
rotation (identical to `TableBufferingFunction`).

## Authoring a format (vgi-python)

Subclass `CopyToFunction`, set `COPY_TO_FORMAT`, declare options as `Arg`-annotated
arguments, and implement `write` (per shard) + `close` (terminal write):

```python
from vgi.copy_to_function import CopyToFunction

class WeirdWriter(CopyToFunction[WeirdArgs]):
    COPY_TO_FORMAT = "weird_out"
    COPY_TO_COMMENT = "Writes the .weird format"  # optional

    class Meta:
        name = "weird_writer"            # the handler function name
        tags = {"category": "copy_to"}

    @classmethod
    def write(cls, *, batch, options, file_path, params):
        # buffer the shard (execution_id-scoped) — race-safe across sink threads
        params.storage.state_append(b"shard", b"", serialize(batch))

    @classmethod
    def close(cls, *, options, file_path, params) -> int:
        # read every shard and write + close the destination, once
        ...
        return rows_written
```

Register the class in the catalog's function list;
`ReadOnlyCatalogInterface.copy_from_formats` advertises registered `CopyToFunction`
subclasses automatically (with `direction='to'`).

## Forwarding `CREATE SECRET` credentials (secret-backed cloud writes)

A writer that targets cloud storage (S3/GCS/HTTP/…) needs the caller's credentials.
Forward them by overriding **`on_secrets`** — the secret-bind hook on `CopyToFunction`.
`on_bind` is `@final` for a writer (no output schema to compute), so `on_secrets` is the
seam the framework calls during bind:

```python
class WeirdWriter(CopyToFunction[WeirdArgs]):
    COPY_TO_FORMAT = "weird_out"

    class Meta:
        name = "weird_writer"

    @classmethod
    def on_secrets(cls, params):
        # Request an s3 secret scoped to the destination path; the framework's
        # two-phase secret bind resolves the longest-prefix-matching CREATE SECRET.
        cf = params.bind_call.copy_to
        params.secrets.get("s3", scope=cf.file_path if cf else None)

    @classmethod
    def write(cls, *, batch, options, file_path, params):
        params.storage.state_append(b"shard", b"", serialize(batch))

    @classmethod
    def close(cls, *, options, file_path, params) -> int:
        creds = params.secrets.for_scope_of_type(file_path, "s3")  # resolved at bind
        ...  # use creds to authenticate the destination write
```

Mechanics: `on_secrets` calls `params.secrets.get(secret_type, scope=…, name=…)`. Each
`get()` for a not-yet-resolved secret registers a pending lookup; the framework then
issues a **two-phase bind retry** — the extension (C++) resolves every requested secret
from the caller's `SecretManager` and re-binds with the resolved values, exactly like a
plain table function's `on_bind` secret request. The resolved values ride the bind into
init and are surfaced on `params.secrets` (a `ResolvedSecrets`) at `write` / `close`
time. A genuinely missing secret resolves to "not found" (silent miss); pass
`required=True` to `get()` to make a missing secret fail the bind. No `CREATE SECRET` is
needed inside the worker — the credential is brokered from the calling DuckDB session,
reaching parity with the writable-table (`write_fixed`) path. This needs **no extra COPY
options or wiring** on the C++ side: the COPY-TO bind already drives the two-phase secret
protocol (`PerformBindProtocol`). Covered by `test/sql/integration/copy_to/secrets.test`.

`CopyFromFunction` has the symmetric `on_secrets` hook for secret-backed cloud *sources*
(scope by `params.bind_call.copy_from.file_path`); see [docs/copy_from.md](copy_from.md).

## Direction (`from` / `to` / `both`)

The discovery metadata's `direction` field drives registration: the extension wires the
FROM callbacks for `from`/`both` and the TO callbacks for `to`/`both` onto one
alias-scoped `CopyFunction` (the two sides are independent on the DuckDB object). A
format that supports both read and write is advertised once with `direction='both'`.

## Transports & limitations

- **Subprocess / launcher / unix:** fully supported.
- **HTTP:** works (the buffering RPCs are unary), but COPY TO is the buffering path,
  which CLAUDE.md notes has divergent semantics under HTTP worker pooling — treat as
  best-effort.
- **DETACH** does not unregister the format (same lifecycle gap as COPY FROM / the
  secret provider); the entry persists for the DB's lifetime.

## Temp-file overwrite (`use_tmp_file`)

When the destination is a **local file that already exists**, DuckDB performs an atomic
overwrite: `PhysicalCopyToFile` writes to a `tmp_<name>` sibling and renames it onto the
final name at finalize (`MoveTmpFile`). The path it hands `copy_to_initialize_global` is
that **temp** path — distinct from the bind-time destination. The extension detects the
mismatch and **re-binds the worker with the temp path** (`RebindCopyToForPath` in
`vgi_copy_to_impl.cpp`), republishing the bind result onto the bind data so every
sink-thread secondary init (and the combine worker, whichever worker it lands on) agrees
on where to write. Without this the worker would write the final path, the temp file would
never be created, and DuckDB's rename would throw `Could not rename ... No such file or
directory`. `initialize_global` runs once before any sink, so mutating the bind data there
is race-free. Covered by `test/sql/integration/copy_to/tmp_file.test`.

`use_tmp_file` is **off** for remote destinations (DuckDB forces it false for `s3://` /
`http(s)://`) and for first-time (fresh-file) writes, so the worker receives the real
destination in those cases. Consequence: a worker that inspects the destination (e.g. an
`on_exists='error'` check) only sees the real path when DuckDB writes in place — under the
default temp-file overwrite it is handed a fresh temp path and cannot observe the existing
file. Pass `use_tmp_file false` in the COPY options to force the worker to receive the
actual destination.

## How it works (C++)

`src/vgi_copy_to_impl.cpp` — `VgiCopyToFunctionInfo` carrier (self-contained, no
`Catalog&`; rides `CopyFunction::function_info`, which `copy_to_bind` receives
directly), the `copy_to_*` callbacks, and the gstate/lstate with cancel-dispatch
teardown (re-implemented on DuckDB's `GlobalFunctionData`/`LocalFunctionData`, since the
buffered operator's machinery lives on different base classes). The `copy_to` bind
context is threaded through `BuildBindRequest` / `PerformBindProtocol` / both function
connections exactly like `copy_from`. Registration (direction-gated) +
`vgi_copy_formats()` live in `vgi_extension.cpp`.

Tests: `test/sql/integration/copy_to/*` (subprocess + HTTP),
`vgi-python/tests/test_copy_to_function.py`.
