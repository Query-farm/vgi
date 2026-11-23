# `blob_test`: the blob-vendor fixture contract

`test/sql/external_storage/` checks blob-vendor externalization end to end:
results over a threshold fetched from vendor pointers, requests over a cap
uploaded through vendor URLs, and function-storage values over a threshold
kept in the vendor. The tests are SQL only. Every SDK's HTTP fixture worker can
run them if it serves the `blob_test` catalog described here and honours the
environment below. The Python fixture worker
(`vgi-python/vgi/_test_fixtures/blob_catalog.py`) is the reference
implementation.

The fake vendor is vgi-rpc-python's `vgi-rpc-fake-vendor`. Lanes start it with
`test/start_fake_blob_vendor.sh`, which runs it with `uvx` from a pinned git
commit, so it needs no SDK checkout. The `fetch_*.test` files are different.
They test the extension's own download client against
`test/support/external_location_probe_worker.py` (`VGI_EXTERNAL_PROBE_WORKER`)
and are not part of this contract.

## Running a lane

```bash
source test/start_fake_blob_vendor.sh     # starts the vendor, exports the variables below
trap stop_fake_blob_vendor EXIT
# start your SDK's HTTP fixture worker with this environment, then:
VGI_TEST_WORKER=http://127.0.0.1:<port> \
    python3 scripts/run_tests.py --build release "test/sql/external_storage/*" -j 1
```

Run the files **one at a time** (`-j 1`). They compare the vendor's lane-wide
counters before and after each query, and they arm faults that hit the next
download of a purpose. Concurrent files would disturb both. Each file starts
with `GET /__admin__/reset`, so a file that stopped half way through cannot
leave a fault armed for the next. The vendor lives
for the whole lane, so give each test file its own worker, as the Python lane
does, or one long-lived worker.

The vendor is started as:

```bash
uvx --from "git+https://github.com/Query-farm/vgi-rpc-python@${VGI_RPC_FAKE_VENDOR_REF}" \
    vgi-rpc-fake-vendor --test-routes --token "${VGI_BLOB_VENDOR_TOKEN}=tenant-a"
```

It prints `PORT:<n>` and then `URL:<base>`. Set `VGI_RPC_FAKE_VENDOR_FROM` to
replace the whole `--from` spec, for example with a local checkout or
`vgi-rpc==X.Y.Z`.

If the vendor dies mid-lane, every later test fails with "connection refused".
To find out why, the script keeps the vendor's output in
`$VGI_FAKE_BLOB_VENDOR_LOG`, in `VGI_FAKE_BLOB_VENDOR_LOG_DIR` (default
`${TMPDIR:-/tmp}`). It prints the path at start and keeps the file after
`stop_fake_blob_vendor`. The vendor logs each `SIGTERM`, `SIGINT` or `SIGHUP`
it receives, with the sender's pid and command line on Linux, and then its
final counters. The script appends the time the process disappeared.
`stop_fake_blob_vendor` reports whether the vendor was still alive; if it had
died, it prints the exit status or signal and the last 40 log lines. Stop a
vendor with `kill $VGI_FAKE_BLOB_VENDOR_PID`, never with `pkill -f` on its
command line, which every lane on the machine shares.

## Environment

`test/start_fake_blob_vendor.sh` exports these. A value the caller already set
is kept. The worker must honour every variable in the first group.

| Variable | Lane value | Worker obligation |
|---|---|---|
| `VGI_BLOB_VENDOR_URL` | `http://127.0.0.1:<port>` | The vendor's base URL. With it set, the HTTP server externalizes results, offers upload URLs and (with `VGI_STORAGE_EXTERNALIZE`) keeps large storage values in the vendor. `http://` is allowed only for loopback. |
| `VGI_BLOB_VENDOR_TOKEN` | `test-token` | Bearer token for every `/v1` call. Never put it on a signed-URL request. |
| `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` | `4096` | A result batch whose Arrow buffers total at least this many bytes is uploaded (purpose `rpc`) and replaced by a pointer. The pointer **must** carry `vgi_rpc.location.sha256` (`result_integrity.test`). |
| `VGI_RPC_MAX_REQUEST_BYTES` | `32768` | Advertise `VGI-Max-Request-Bytes` and `VGI-Upload-URL-Support: 2` (upload URLs v2, protocol 3.4). Serve `__upload_url__/init` (`uploads = [{size, sha256}]`) from the vendor's sized `POST /v1/blobs/put` (purpose `rpc`, with `origin: "client_upload"`, which the tests count as `puts_upload`), returning its `required_headers` as `upload_headers`. Accept requests sent as upload pointers, require their `vgi_rpc.location.sha256`, and verify the downloaded bytes against it. |
| `VGI_RPC_MAX_UPLOAD_BYTES` | `268435456` | Advertise `VGI-Max-Upload-Bytes`. |
| `VGI_STORAGE_EXTERNALIZE` | `1` | Wrap the function-storage backend (e.g. SQLite) in the envelope described below. |
| `VGI_STORAGE_EXTERNALIZE_BYTES` | `4096` | A storage value of **at least** this many bytes is a vendor blob (purpose `fs`). A smaller one is stored inline. The tests use it as `${VGI_STORAGE_EXTERNALIZE_BYTES}`. |

For the extension only:

| Variable | Lane value | Meaning |
|---|---|---|
| `VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS` | `1` | Lets the extension fetch and upload the fake's `http://127.0.0.1` signed URLs. |
| `VGI_TEST_ARGUMENTS_REF_FAULTS` | `1` | Enables the extension's malformed-`BindRequest` test hook (`SET VARIABLE vgi_test_arguments_ref_fault`), see *Protocol 3.4 large values*. |
| `VGI_TEST_WORKER` | your worker | Set by the lane, not by the script. |
| `VGI_RPC_FAKE_VENDOR_REF` / `VGI_RPC_FAKE_VENDOR_FROM` | pinned commit | The vendor build to run (see above). |

## The catalog

`ATTACH 'blob_test' AS bt (TYPE vgi, LOCATION '${VGI_TEST_WORKER}')`.
`blob_test` is its own catalog, with one schema `main`, so the example
catalog's function counts do not change. It has exactly these seven functions.

| Function | Kind | Arguments | Output |
|---|---|---|---|
| `blob_repeat(s, n)` | scalar | `s VARCHAR`, `n BIGINT` | `VARCHAR` (Arrow `large_utf8` in Python; any string type) |
| `blob_len(s)` | scalar | `s VARCHAR` | `BIGINT` |
| `blob_rows(rows, width)` | table | `rows BIGINT >= 0`, `width BIGINT >= 0` (positional) | `i BIGINT, payload VARCHAR` |
| `blob_param_echo(payload)` | table | `payload VARCHAR` (positional) | `payload_len BIGINT, payload_sha256 VARCHAR` |
| `blob_buffer_echo(t)` | table-buffering | one table input | every input column, then `payload_len BIGINT, payload_sha256 VARCHAR` |
| `blob_tio(t)` | table-in-out | one table input | every input column, then `payload_len BIGINT, payload_sha256 VARCHAR` |
| `blob_store_values(t)` | table-in-out | one table input | input columns **2..n**, then `payload_len BIGINT, payload_sha256 VARCHAR` |

**Checksum columns.** `payload_len` is the UTF-8 byte length and
`payload_sha256` the lowercase hex SHA-256 of a value as text. They match
DuckDB's `strlen(x)` and `sha256(x)`. A NULL value gives NULL in both.

Behaviour:

- **`blob_repeat`**: `s` repeated `n` times (`n < 0` counts as 0). NULL in
  either argument gives NULL. It produces large scalar outputs.
- **`blob_len`**: `strlen(s)` in UTF-8 bytes. It takes large scalar inputs
  (the requests exceed the request cap).
- **`blob_rows`**: row `i` (0-based) is `repeat(chr(97 + i % 26), width)`,
  for `rows` rows in order of `i`. Emit them in batches (Python uses 2,048
  rows). Cardinality is exactly `rows`. It produces large table outputs.
- **`blob_param_echo`**: one row with the checksum columns of `payload`. It
  takes large bind parameters, which ride the bind and init requests.
- **`blob_buffer_echo`**: *process* stores each input batch, serialized as an
  Arrow IPC stream, as **one** storage value appended to the execution's log
  (`state_append`). *Finalize* scans the log and emits each stored batch with
  the checksum columns of its **first** column (cast to text), one batch per
  tick. Every row must come back with its other columns intact.
- **`blob_tio`**: for each input batch, store it (IPC stream) as **one**
  storage value under a fresh key, read it back immediately, delete it, and
  emit the batch **as read back** with the checksum columns of its first
  column. A value that is missing straight after the write is an error.
- **`blob_store_values`**: for each input batch, store the UTF-8 bytes of each
  non-NULL value of the first column (cast to text) as **its own storage value
  of exactly that size** (an empty string is a zero-byte value; NULL stores
  nothing). Read all of them back in one call, delete them, and emit input
  columns 2..n with the checksum columns **of the bytes read back**. Each value
  at or over the threshold costs exactly one vendor `put` and, on the read,
  exactly one vendor `get` (`storage_values.test` counts them).

The functions in each storage group (`blob_buffer_echo`, `blob_tio`,
`blob_store_values`) must compute their outputs from what storage returns, not
from the input they were given. Otherwise the round trip goes untested.

## Storage-error contract

When a storage blob cannot be read, the query must fail. The error message
that reaches DuckDB must match these SDK-neutral patterns (RE2,
`.` matches newline):

| Failure | Pattern | Python message |
|---|---|---|
| Downloaded bytes differ from the pointer's SHA-256 or size | `.*storage blob.*(sha256\|size) mismatch.*` | `storage blob <id> failed its integrity check: sha256 mismatch` |
| Blob gone (vendor `get` 404 `not_found`/`not_uploaded`, or the signed URL 404s) | `.*storage blob.*(missing\|not found).*` | `storage blob <id> is missing: not found in the blob vendor (expired or deleted?)` |
| Anything else | starts `storage blob <id>` | `storage blob <id> could not be read: <detail>` |

The words are lowercase. Verify against the **pointer's** size and SHA-256,
not the vendor's own report. An SDK that skips verification feeds other bytes
to the function. That gives wrong rows or a decode error, never these
messages, so `storage_integrity.test` fails.

Result downloads are checked by the extension, not the worker. A result
pointer without `vgi_rpc.location.sha256` cannot be verified: since protocol
3.4 the extension refuses it ("missing vgi_rpc.location.sha256"), and a
mismatch is `CHECKSUM_MISMATCH`, "VGI external location SHA-256 checksum
mismatch" (`result_integrity.test`).

Client uploads (protocol 3.4, upload URLs v2) are checked by storage: the
extension hashes the request, asks for a URL signed for that size and
SHA-256, and PUTs exactly those bytes with the returned `upload_headers`. A
body that does not match is refused by the vendor (`400 BadDigest`), and the
query fails with `CHECKSUM_MISMATCH`, "storage checksum mismatch"
(`upload_integrity.test`).

## Storage value envelope (v1)

Every value the externalizing wrapper writes is enveloped, including
work-queue items, `state_*` values and log items:

```
inline:  0x00 || raw
pointer: 0x01 || u64be(size) || sha256(raw) (32 bytes) || object_id (ASCII)
```

- A value with `len(raw) >= VGI_STORAGE_EXTERNALIZE_BYTES` (with a vendor) is
  uploaded **before** the backend write and stored as a pointer, about 73
  bytes. Everything else is inline.
- `object_id` matches `[A-Za-z0-9._:-]{1,128}`. The fixed part is 41 bytes.
- Decoding is strict, and each of these is an error: an empty value, an
  unknown tag, a pointer of 41 bytes or fewer, or an object id outside the
  charset. A pointer read with no vendor configured is an error too. User
  bytes that begin `0x01` are safe because every value is enveloped.
- Counters (`state_counter_*`) are not enveloped.

Test vectors: [`test/vectors/storage_envelope_v1.json`](../../test/vectors/storage_envelope_v1.json).
They contain inline values, pointers, malformed cases, and the scope and
shard names below. They are generated by
`vgi-python/scripts/gen_storage_envelope_vectors.py`, and vgi-python's
`tests/test_storage_envelope_vectors.py` checks this copy byte for byte when
the two checkouts are siblings.

## Vendor scope and idempotency conventions

Uploads carry `scope = {shard, execution_id}` so clean-up can delete by scope:

- **`shard`**: the storage shard key (`"att-" + hex(attach uuid)`). An empty
  key becomes `loc-anon`. A key that does not match `[A-Za-z0-9._-]{1,128}`
  becomes `h-` plus the hex SHA-256 of its UTF-8 bytes. The vendor itself also
  accepts `:`, but the wrapper's charset does not.
- **`execution_id`**: `q.<hex execution id>` for queue items and
  `s.<hex scope id>` for state and log values. If the result is longer than
  128 characters, use `<kind>.h<hex sha256(ident)>` instead.
- **`idempotency_key`**: `<per-call uuid hex>.<index of the value in the
  call>`, so a retried logical write reuses the object.
- Clean-up runs in the background and is best effort. `queue_clear` and
  `execution_clear` delete their scope, `state_drain` deletes the blobs it
  drained, and `queue_pop` deletes the blob it claimed. `state_delete` leaves
  blobs until the scope is cleared.

## Vendor HTTP API

These are the `/v1` routes, mirroring the staging Worker
(`blobs.staging.queryfarm.cloud`). Every `/v1` route needs
`Authorization: Bearer <token>`. Errors are `{error, message, ...}`. Times are
ISO-8601 UTC. Object ids are 32 lowercase hex characters.

| Route | Request | Response |
|---|---|---|
| `POST /v1/blobs/put` | `{size, sha256_b64, purpose: "rpc"\|"fs", scope?: {shard, execution_id}, content_encoding?, idempotency_key?, origin?: "client_upload"}` | `{object_id, region, upload_url, upload_method: "PUT", required_headers, expires_at}` plus `download_url, download_expires_at` for purpose `rpc`. A repeated idempotency key with the same bytes returns the same object; with other bytes, 409 `idempotency_conflict`. 413 `object_too_large` / `quota_exceeded`. `origin: "client_upload"` is advisory: a server sends it on grants it mints for a client to upload a request (`__upload_url__`), never on its own writes. The real vendor ignores it; the fake counts and faults client uploads by it. |
| `POST /v1/blobs/get` | `{object_id}` | `{object_id, download_url, size, sha256_b64, content_encoding, expires_at}`. 404 `not_uploaded` / `not_found`. |
| `POST /v1/blobs/delete` | `{object_ids: [...]}` (at most 1,000) | `{deleted}` |
| `POST /v1/blobs/delete_scope` | `{shard, execution_id}` | `{deleted}` |
| ~~`POST /v1/upload_urls`~~ | removed | **Removed in protocol 3.4** from the vendor and the fake vendor (vgi-rpc STATE_REF_V1.md §13): servers serve `__upload_url__` from sized `/v1/blobs/put`. The route now answers 404 like any unknown path. The fake vendor keeps its `upload_urls` stats key, always 0, and the tests still assert it never moves. |
| `GET /v1/info` | none | `{tenant, region, jurisdiction, upload_hosts, download_hosts, url_prefixes, limits, ttl_seconds}`. Pin signed URLs to these hosts and prefixes. |

Signed URLs behave like R2's. A PUT must send exactly the `required_headers`
(`content-length`, `x-amz-checksum-sha256`, and `content-encoding` when one was
declared). A body of another length gets `400 IncompleteBody`, a missing or
different checksum header `403 SignatureDoesNotMatch`, and a body that does
not hash to the declared digest `400 BadDigest`. Downloads support `HEAD` and single `Range` requests. A URL
whose query has been stripped fails `403`.

### Test routes (`--test-routes`)

These are unauthenticated, take `GET` only and have side effects only on
`GET`. A `HEAD` returns the same headers with no effect. Bodies are JSON padded
with spaces to exactly 1,024 bytes. They support `Range` and carry
`Cache-Control: no-store`, so DuckDB's `read_json_auto` can read them over
httpfs.

| Route | Effect / body |
|---|---|
| `GET /__stats__` | `{puts, gets, deletes, delete_scopes, upload_urls, storage_puts, storage_gets, objects, bytes, bytes_uploaded, bytes_downloaded, upload_refusals, upload_bad_digest, upload_incomplete_body, upload_signature_mismatch, uploads_corrupted, puts_rpc, puts_fs, puts_upload, client_uploads, client_upload_bytes, client_upload_refusals, puts_state_ref, gets_state_ref, gets_upload}` (the last three from protocol 3.4, STATE_REF_V1.md §10.3: grants with `origin: "state_ref"`, and `/v1/blobs/get` calls on objects granted with `origin` `state_ref` and `client_upload`). These are calls to the five `/v1` routes in order; signed-URL PUTs and GET/HEADs; live objects and their bytes; cumulative bytes accepted by signed PUTs and served by signed GETs; signed PUTs the store refused (the sum, then by S3 code); and PUT bodies an upload fault damaged. Then by purpose: `/v1/blobs/put` grants issued for the server's rpc writes (results), for function storage (`fs`), and for client uploads (`origin: "client_upload"`; counted only here), which sum to the grants issued (`puts` also counts refused calls); and signed PUTs to client-upload objects: accepted, their bytes, and refused. All but `objects` and `bytes` only grow, so tests compare deltas. |
| `GET /__admin__/fault?purpose=fs\|rpc&action=corrupt\|missing&count=N` | Arms a fault for the next `N` **objects** of that purpose that are downloaded. The first signed `GET` of such an object marks it. After that, every `GET` of it is faulted until reset: `corrupt` XORs every byte with `0xFF` (same length) and `missing` answers `404 NoSuchKey`. So a probe `GET`, ranged reads and retries all see the same damage. Arming replaces the fault armed for that purpose, so repeating the request is harmless. `count=0` disarms. Body: `{ok, purpose, action, count}`. |
| `GET /__admin__/fault?purpose=upload&action=corrupt&count=N` | Corrupts the next `N` client-upload `PUT` bodies in transit (objects granted with `origin: "client_upload"`), so the store's checksum check refuses them with `400 BadDigest` and stores nothing. Server writes (results, storage values) are never corrupted, so a test may return large results while it is armed. `corrupt` is the only upload action; arming replaces the armed upload fault. Body: `{ok, purpose, action, count}`. |
| `GET /__admin__/reset` | Clears armed and applied download and upload faults. Body: `{ok}`. |

From SQL:

```sql
SELECT puts FROM read_json_auto('${VGI_BLOB_VENDOR_URL}/__stats__');
SELECT ok FROM read_json_auto('${VGI_BLOB_VENDOR_URL}/__admin__/fault?purpose=fs&action=corrupt&count=1');
```

## Protocol 3.4 large values

`arguments_ref*.test` check argument references and `init: StateRef`
(`docs/large-values-3.4.md`). They need more from the worker, the fixture
server and the fake vendor than the files above, so each declares what it
needs with `require-env`, and a lane sets the variable once its worker or
vendor provides it. Until then the file skips with that reason, and the lane
lists the reason as expected (`--allow-skip`), so the skip is visible and a
new, unlisted one still fails the lane.

| Variable | Set by the lane when | Files |
|---|---|---|
| `VGI_WORKER_ARGUMENTS_REF` | the worker implements argument references (sends `VGI-Arguments-Ref-Bytes`, resolves `arguments_location`, refuses bad ones with the spec's kinds) **and** serves the four fixtures below | `arguments_ref.test`, `_streams`, `_prepared`, `_errors` |
| `VGI_WORKER_INIT_STATE_REF` | the worker carries its init request as a vgi-rpc `StateRef` (vgi spec §6) and the fake vendor has `puts_state_ref` | `arguments_ref_state_ref.test` |
| `VGI_FAKE_VENDOR_DELETE_ROUTE` | the fake vendor serves `GET /__admin__/delete?origin=client_upload` (below) | `arguments_ref_prepared.test` |
| `VGI_TEST_ARGUMENTS_REF_FAULTS` | always, on the extension's vendor lane (`test/start_fake_blob_vendor.sh` exports it): enables the extension's malformed-`BindRequest` hook | `arguments_ref_errors.test` |

`arguments_ref_fallback.test` and `arguments_ref_aggregate.test` need none of
these and run against any worker.

**Fixture server switches**, set per file with the test runner's
`# server-env: NAME=VALUE` directive (`scripts/run_tests.py`, applied to that
file's own worker server only):

- `VGI_TEST_DISABLE_ARGUMENTS_REF=1`: the HTTP fixture server does not send
  `VGI-Arguments-Ref-Bytes` (so it behaves like a worker without argument
  references, while still offering upload URLs v2). A worker without
  argument-reference support ignores it. Used by `arguments_ref_fallback.test`.
- `VGI_RPC_STATE_REF_INLINE_BYTES=2048`: vgi-rpc's `StateRef` inline limit (STATE_REF_V1.md §9), so a ~5 KB init takes the stored form whatever the turn budget. Used by `arguments_ref_state_ref.test`.
- `VGI_RPC_OBJECT_CACHE_BYTES=1`: vgi-rpc's object-cache bound
  (STATE_REF_V1.md §11), so every resolve is a vendor GET (a cold cache). Used
  by `arguments_ref_prepared.test`.

**Fake vendor route** (vgi-rpc-python `fake_vendor.py`, test routes):

| Route | Effect / body |
|---|---|
| `GET /__admin__/delete?origin=client_upload` | Deletes every live object that was granted with that `origin` (standing in for the bucket lifecycle rule): its signed GET and `/v1/blobs/get` then answer 404 `not_found`. `origin` is required (`client_upload` or `state_ref`). Body: `{ok, deleted}`. |

**Fixtures** in `blob_test`, besides the seven above (protocol 3.4; the
SDK-neutral contract every SDK's fixture worker set serves):

| Function | Kind | Arguments | Output |
|---|---|---|---|
| `blob_param_batches(payload, batches)` | table | `payload VARCHAR`, `batches BIGINT >= 0` (positional) | `i BIGINT, payload_len BIGINT, payload_sha256 VARCHAR` |
| `blob_param_streams(payload, streams)` | table | `payload VARCHAR`, `streams BIGINT >= 1` (positional) | `stream BIGINT, payload_len BIGINT, payload_sha256 VARCHAR` |
| `blob_param_scalar(i, payload)` | scalar | `i BIGINT` (per row), `payload VARCHAR` (a **constant** parameter, bound once) | `VARCHAR` |
| `blob_param_table` | catalog table | none | `payload_len BIGINT, payload_sha256 VARCHAR` |

- **`blob_param_batches`**: a producer that emits `batches` batches of one row
  each, row `i` = `(i, strlen(payload), sha256(payload))`, one batch per
  stream turn (so a 60-batch call is at least 60 continuations).
- **`blob_param_streams`**: one bind, planned as `streams` parallel streams
  (one split each, so DuckDB can run them on several threads); each stream
  emits one row `(stream index, strlen(payload), sha256(payload))`. Every
  stream's init embeds the same `bind_call`, so with argument references the
  worker process resolves the argument from the vendor once.
- **`blob_param_scalar`**: `sha256(payload)` for every row whose `i` is not
  NULL (NULL otherwise). `payload` must be a constant parameter (Python
  `ConstParam`), so it rides the scalar's bind, not its input batches.
- **`blob_param_table`**: a table whose scan binds `blob_param_echo` with the
  fixed argument `repeat('t', 65536)`, reports repeatability
  `STABLE_WITHIN_QUERY`, and does **not** inline its bind result in the
  catalog (so every scan binds over RPC). Its one row is
  `(65536, sha256(repeat('t', 65536)))`.

| File | What it proves |
|---|---|
| `arguments_ref.test` | An 8 MiB argument goes up once (`client_uploads` +1, of about the argument's size) and travels by reference: no whole-request upload and every request under 16 KiB (`vgi_http_request_stats`); a small argument stays inline. A 60-batch producer on it never needs a whole-request upload and keeps every turn under 16 KiB. A scalar with a large constant parameter uploads it once. A serialized-plan rebind (`blob_param_table` under `debug_verify_serializer`, which replaces each plan with its deserialized copy) reuses the reference (`arguments_ref_reuses`) and never trips "Repeatable VGI source bind state changed"; a recursive CTE over it uploads once. |
| `arguments_ref_streams.test` | Eight parallel streams on one 8 MiB argument: one client upload and one worker GET (`gets_upload` +1). |
| `arguments_ref_state_ref.test` | A 1000-value IN list over `example.echo` makes the init request larger than the inline limit (pinned to 2 KiB with `VGI_RPC_STATE_REF_INLINE_BYTES`): the worker stores it once (`puts_state_ref` +1) and no turn needs a whole-request upload. |
| `arguments_ref_prepared.test` | With the object deleted and a cold worker cache, `EXECUTE` of a prepared statement fails with "this prepared statement's large argument has expired; prepare it again", without uploading again; preparing it again works. |
| `arguments_ref_errors.test` | A tampered `ref` fails with `invalid_arguments_ref`; both or neither of `arguments` / `arguments_location` fail with "BindRequest must set exactly one of 'arguments' and 'arguments_location'". |
| `arguments_ref_fallback.test` | A worker that does not send `VGI-Arguments-Ref-Bytes` gets the arguments inline; an over-cap bind takes the whole-request upload, as before 3.4. |
| `arguments_ref_aggregate.test` | Aggregates never use argument references; an aggregate with an 80 KiB input batch still works through the whole-request upload. |

## The tests

| File | What it proves |
|---|---|
| `scalar.test` | Large scalar outputs come back exactly from vendor pointers (`puts_rpc`, `storage_gets` move). Large scalar inputs are uploaded (`puts_upload`, `client_uploads`, `client_upload_bytes` move; `upload_urls` does not). |
| `table.test` | Large table outputs come back exactly (`puts_rpc`). Large bind parameters are uploaded (`puts_upload`, `client_uploads`), and a small one is not. |
| `buffered.test` | Buffered input, storage and output are all externalized, with 1 and 4 threads: the client uploads its input (`puts_upload`, `client_uploads`), the worker stores it (`puts_fs`, `gets`), and the client fetches the output. |
| `table_in_out.test` | Table-in-out input, per-batch state and output are externalized (`puts_upload`, `client_uploads`, `puts_fs`, `gets`). |
| `storage_integrity.test` | A corrupted storage blob fails `blob_tio`, `blob_buffer_echo` and `blob_store_values` with the integrity pattern. A missing one fails `blob_tio` and `blob_buffer_echo` with the missing pattern. The same queries pass after reset. |
| `storage_values.test` | Values one byte below, at and one byte above the threshold round-trip, and exactly the last two become blobs (`puts` +2, `gets` +2, bytes exact). The threshold counts UTF-8 bytes. Empty values, NULLs and empty inputs work, and store nothing. 25 batches and 100 vendored values out of 3,000 in one execution round-trip exactly. |
| `result_integrity.test` | A corrupted result (table and scalar) fails with `CHECKSUM_MISMATCH`, and a missing one fails with a 404. Neither ever returns rows. Its faults use `count=100`, which damages every `rpc` download until the reset. The first query after an error may re-fetch the catalog, which is itself an externalized result and then fails with the same error. Each faulted query uses fresh arguments, so no cached result can stand in. |
| `catalog_integrity.test` | The `blob_test` catalog's `catalog_contents` is a vendor object. Corrupted, it fails the enumeration with `CHECKSUM_MISMATCH` (`delivery = external_location`); missing, with a 404. Neither falls back to `catalog_schemas` (logged `outcome=error`, never `outcome=fallback`). The catalog loads again after the reset. |
| `upload_integrity.test` | An oversized request (scalar input and table bind parameter) round-trips through a sized upload: `__upload_url__` is served from sized client-upload grants (`puts_upload`), the signed PUT carries the bytes (`client_uploads`, `client_upload_bytes`), and the result is exact. The corrupt-upload fault makes storage refuse the PUT (`client_upload_refusals`), and the query fails with `CHECKSUM_MISMATCH`, "storage checksum mismatch", while a large result (a server write) still comes back intact. It passes again after the reset. |
| `redaction.test` | Errors for a missing or corrupted result object and for a refused upload name the URL as `scheme://host/path?<redacted>`, in the message and in `errors_as_json`, and never contain `X-Amz-Signature`, `X-Amz-Credential` or `X-Amz-Algorithm`. |
