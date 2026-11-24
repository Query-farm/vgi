# External locations (externalized batches)

vgi-rpc moves Arrow batches that are too large to send inline through an
object store. A pointer is a zero-row batch whose custom metadata carries
`vgi_rpc.location` (a URL) and `vgi_rpc.location.sha256` (the lowercase hex
SHA-256 of the stored bytes; mandatory in both directions since protocol
3.4.0).

## Results (worker → client)

A worker's `ExternalLocationConfig` replaces any result batch over its
threshold with a pointer: unary results, stream headers and data batches, on
every transport (HTTP and the byte transports alike). The client resolves
them in `ResolveExternalLocation` (`src/vgi_http_client.cpp`):

- **Scheme.** `https://` is fetched on any host (AWS, R2, GCS, or a custom
  S3 endpoint such as MinIO, Ceph, Wasabi or B2). Plain `http://` is
  accepted on any host only after `SET vgi_storage_allow_plain_http = true`
  (an in-network MinIO; the pre-signed query, which is the credential, then
  crosses the network in the clear). Without it, plain `http://` is accepted
  only for a loopback host (`127.0.0.0/8`, `[::1]`, `localhost`, parsed from
  the authority, so `http://127.0.0.1@evil/` is refused) and only with
  `VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1` in the process environment
  (always in the browser, which has no environment and blocks mixed content
  itself). The same rule covers vended upload and download URLs.
  `httpi://` is refused. Refusals are `UNSUPPORTED` and happen before any
  request.
- **Timeouts and retries.** Every storage transfer (result and catalog
  fetches, argument and request uploads) has a connect timeout and a stall
  timeout, both `vgi_storage_stall_timeout_seconds` (default 30, the value
  vgi-rpc's storage clients use, `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS` on
  workers), and no whole-request deadline: a 1 GiB transfer over a slow link
  runs as long as it keeps moving (more than 1 KiB/s over the window). A
  connect error, a stall, 408, 429 or 5xx is retried, at most 3 attempts in
  all (fewer when `http_retries` is lower), with 0.25 s x 2^n backoff plus
  jitter. A retried GET starts its body over; a retried PUT resends its body,
  since a pre-signed PUT cannot resume. `vgi_http_timeout_seconds` does not
  apply to storage transfers.
- **Size.** `vgi_external_location_max_bytes` (default 1 GiB) caps both the
  transferred body and its decoded size: a `Content-Length` over the cap
  stops before the body is read, a body that grows past it stops at the cap,
  and a `zstd`/`gzip` body that inflates past it fails in the codec.
  `RESPONSE_TOO_LARGE`.
- **Integrity.** A pointer without `vgi_rpc.location.sha256` is refused
  before anything is fetched (`PROTOCOL_VIOLATION`, "missing
  vgi_rpc.location.sha256"). The body must hash to it (`CHECKSUM_MISMATCH`).
  A resolved batch that is itself a pointer is a protocol violation.
- **Failures are tagged.** Every error raised while resolving a pointer carries
  `delivery = external_location` in `extra_info`, so callers can tell a damaged
  or missing object from the worker's own answer. `catalog_contents` uses it:
  a worker that raises still falls back to the per-schema RPCs, a snapshot
  that arrives corrupted, missing or over a size cap fails the statement
  (`docs/catalog_contents.md`).
- **Redaction.** A pre-signed URL's query is its credential. Every message,
  `extra_info` field and log line that names an external-location, upload or
  download URL shows only `scheme://host/path?<redacted>`, including errors the
  engine's HTTP client raises for it. `RedactCredentials` also masks
  `X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`,
  `X-Goog-Signature` and SAS `sig` values wherever else a URL is reported.
- No credentials are sent: pre-signed URLs reject an extra `Authorization`.

The fetched host is not pinned to the worker's store: that would need the
server to advertise its storage hosts, which the protocol does not carry yet.

## Requests (client → worker, HTTP): upload URLs v2

A server advertising `VGI-Max-Request-Bytes` and `VGI-Upload-URL-Support: 2`
takes larger requests by reference. For any request body over the cap (unary
RPCs such as `bind`, catalog calls and `table_buffering_process`, the `/init`
POST, and exchange input batches), `MaybeExternalizeRequestBody`:

1. refuses a server that advertises only v1 (`VGI-Upload-URL-Support: true`,
   whose blank URLs accepted any bytes): `SERVER_TOO_OLD`, "upload protocol
   is too old". It never falls back to the retired `count` shape;
2. refuses a body over `VGI-Max-Upload-Bytes` too (`LIMIT_EXCEEDED`);
3. hashes the body and asks `{base}/__upload_url__/init` for one URL with
   `uploads = [{size, sha256}]` (`list<struct<size: int64 not null, sha256:
   utf8 not null>> not null`; the upload-URL request itself is never
   externalized), applying the scheme rule above to both returned URLs;
4. PUTs exactly those bytes, uncompressed, with a plain `Content-Length` and
   every `upload_headers` entry verbatim (e.g. `x-amz-checksum-sha256`). The
   URL is signed for that size and digest, so storage refuses anything else:
   a refusal whose S3 `<Code>` is `BadDigest` (or `IncompleteBody`) is
   `CHECKSUM_MISMATCH`, "storage checksum mismatch" ("storage size mismatch");
5. sends a zero-row pointer batch with the request's schema and **all** of its
   metadata (method, protocol, state tokens, validators) plus
   `vgi_rpc.location` = the download URL and `vgi_rpc.location.sha256` = the
   body's digest, which the server verifies after downloading.

Without upload support the body is sent inline and the server's 413 explains.

The pointer keeps the request's schema (as vgi-rpc-python's client does), so
a cap below the widest request schema still 413s: echo over the fixture's
`all_types` table has a ~20 KiB schema, which is why the vendor lane uses a
32 KiB cap rather than 16 KiB.

The body is read, hashed and PUT in place: no copy of a large request is
made on the way up.

## Arguments by reference (protocol 3.4)

Normative spec: [`large-values-3.4.md`](large-values-3.4.md) §2-§5. A large
argument would otherwise ride the `bind` request and every request that embeds
it (cardinality, statistics, init, dynamic-to-string, plan, projection),
uploaded whole each time. Instead, `HttpFunctionConnection::PerformBindRpc`
(HTTP and Iroh; never pipe, stdio or unix):

1. discovers the worker's capabilities **before** building the bind request,
   and hands that snapshot to the bind's `HttpInvokeUnary` (no second probe);
2. serializes the arguments IPC once and decides once
   (`ShouldExternalizeArguments`): by reference iff the worker sends a valid
   `VGI-Arguments-Ref-Bytes` (A), advertises `VGI-Upload-URL-Support: 2`, the
   size is over `min(A, VGI-Max-Request-Bytes / 4)` (an unset cap counts as
   infinite) and within any `VGI-Max-Upload-Bytes`. Without the header the
   arguments go inline and an over-cap bind takes the whole-request upload
   above, so old workers keep working;
3. hashes the arguments buffer, asks `__upload_url__/init` for one URL
   (`[(size, sha256)]`) and PUTs straight from that buffer; the grant's `ref`
   column (sealed by the worker) becomes `BindRequest.arguments_location =
   {ref, size, sha256}` with `arguments` null;
4. reuses that location for the scoped-secret retry, and records it in a
   per-query memo (keyed by worker URL, ATTACH and the argument's size and
   SHA-256, cleared at query end). Every other bind in the query with the same
   argument (several scans, scalar connections, the plan rebind in
   `VgiTableScanDeserialize`) reuses it, so it uploads once and rebuilds
   byte-identical `bind_request_bytes`.

A missing object (a prepared statement past the bucket lifecycle) is the
worker's `arguments_ref_missing`, "this prepared statement's large argument
has expired; prepare it again", surfaced unchanged; there is no transparent
rebind. Aggregates never use references.

Telemetry: the `arguments_ref.upload` log event (size, ms), and
`vgi_http_request_stats([reset := true])`: per worker URL, the request count,
the largest and total request body (uncompressed IPC, after any upload replaced
it with a pointer), whole-request uploads, and argument-reference uploads and
memo reuses.

Test hook: with `VGI_TEST_ARGUMENTS_REF_FAULTS=1` in the environment,
`SET VARIABLE vgi_test_arguments_ref_fault = 'tamper' | 'both_set' |
'both_null'` rewrites the next binds' `BindRequest` so the worker's refusals can
be tested (`arguments_ref_errors.test`). It is ignored without the variable.

## Tests

- `make test_http_vendor` runs `test/sql/external_storage/` and the
  integration suite against the fixture HTTP server and a fake blob vendor,
  with results over 4 KiB externalized, requests over 32 KiB uploaded and
  worker storage values over 4 KiB kept in the vendor
  (`VGI_STORAGE_EXTERNALIZE=1`). The vendor is vgi-rpc's
  `vgi-rpc-fake-vendor --test-routes`, started by
  `test/start_fake_blob_vendor.sh` with `uvx` from a pinned vgi-rpc-python
  commit (`VGI_RPC_FAKE_VENDOR_REF`); any SDK lane can source that script.
  The targeted tests check values exactly, read the vendor's counters straight
  from it (`read_json_auto('${VGI_BLOB_VENDOR_URL}/__stats__')`), and use its
  fault routes to corrupt or lose storage blobs and result objects
  (`storage_integrity.test`, `result_integrity.test`,
  `catalog_integrity.test`), corrupt client uploads in transit
  (`upload_integrity.test`), check that no error prints a signature
  (`redaction.test`), plus threshold, empty and NULL cases
  (`storage_values.test`). The worker contract (the
  `blob_test` catalog, environment, storage-error messages, the value
  envelope and vendor conventions) is `docs/testing/blob-test-catalog.md`;
  envelope vectors are `test/vectors/storage_envelope_v1.json`.
  `make test_http_vendor_external` runs only the targeted files.
- `test/sql/external_storage/fetch_*.test` use
  `test/support/external_location_probe_worker.py`
  (`VGI_EXTERNAL_PROBE_WORKER`), which points at objects it serves itself:
  bad digests, missing digests, altered objects, unsized and gzip bodies,
  arbitrary URLs.
- `make test_launcher_external_storage` and `make test_http_demo_storage`
  externalize every result to loopback stores and set the loopback hook.
