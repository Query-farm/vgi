# External locations (externalized batches)

vgi-rpc moves Arrow batches that are too large to send inline through an
object store. A pointer is a zero-row batch whose custom metadata carries
`vgi_rpc.location` (a URL) and, optionally, `vgi_rpc.location.sha256`.

## Results (worker → client)

A worker's `ExternalLocationConfig` replaces any result batch over its
threshold with a pointer: unary results, stream headers and data batches, on
every transport (HTTP and the byte transports alike). The client resolves
them in `ResolveExternalLocation` (`src/vgi_http_client.cpp`):

- **Scheme.** Only `https://` is fetched. Plain `http://` is accepted only
  for a loopback host (`127.0.0.0/8`, `[::1]`, `localhost`, parsed from the
  authority, so `http://127.0.0.1@evil/` is refused) and only with
  `VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1` in the process environment
  (always in the browser, which has no environment and blocks mixed content
  itself). `httpi://` is refused. Refusals are `UNSUPPORTED` and happen
  before any request.
- **Size.** `vgi_external_location_max_bytes` (default 1 GiB) caps both the
  transferred body and its decoded size: a `Content-Length` over the cap
  stops before the body is read, a body that grows past it stops at the cap,
  and a `zstd`/`gzip` body that inflates past it fails in the codec.
  `RESPONSE_TOO_LARGE`.
- **Integrity.** With `vgi_rpc.location.sha256`, the decoded body must hash
  to it (`CHECKSUM_MISMATCH`). A resolved batch that is itself a pointer is a
  protocol violation.
- No credentials are sent: pre-signed URLs reject an extra `Authorization`.

The fetched host is not pinned to the worker's store: that would need the
server to advertise its storage hosts, which the protocol does not carry yet.

## Requests (client → worker, HTTP)

A server advertising `VGI-Max-Request-Bytes` and `VGI-Upload-URL-Support:
true` takes larger requests by reference. For any request body over the cap
(unary RPCs such as `bind`, catalog calls and `table_buffering_process`, the
`/init` POST, and exchange input batches), `MaybeExternalizeRequestBody`:

1. refuses a body over `VGI-Max-Upload-Bytes` too (`LIMIT_EXCEEDED`);
2. asks `{base}/__upload_url__/init` for one URL pair (the upload-URL request
   itself is never externalized), applying the scheme rule above to both URLs;
3. PUTs the serialized request to the upload URL;
4. sends a zero-row pointer batch with the request's schema and **all** of its
   metadata (method, protocol, state tokens, validators) plus
   `vgi_rpc.location` = the download URL.

Without upload support the body is sent inline and the server's 413 explains.

The pointer keeps the request's schema (as vgi-rpc-python's client does), so
a cap below the widest request schema still 413s: echo over the fixture's
`all_types` table has a ~20 KiB schema, which is why the vendor lane uses a
32 KiB cap rather than 16 KiB.

## Tests

- `make test_http_vendor` runs `test/sql/external_storage/` and the
  integration suite against the fixture HTTP server and a fake blob vendor
  (`vgi._test_fixtures.fake_blob_vendor`), with results over 4 KiB
  externalized, requests over 32 KiB uploaded and worker storage values over
  4 KiB kept in the vendor (`VGI_STORAGE_EXTERNALIZE=1`). The targeted tests
  check values exactly and that the vendor's counters
  (`blob_test.vgi_test_blob_stats()`) moved.
  `make test_http_vendor_external` runs only the targeted files.
- `test/sql/external_storage/fetch_*.test` use
  `test/support/external_location_probe_worker.py`
  (`VGI_EXTERNAL_PROBE_WORKER`), which points at objects it serves itself:
  bad digests, altered objects, unsized and gzip bodies, arbitrary URLs.
- `make test_launcher_external_storage` and `make test_http_demo_storage`
  externalize every result to loopback stores and set the loopback hook.
