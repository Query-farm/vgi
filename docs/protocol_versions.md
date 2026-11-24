# Protocol version negotiation (extension side)

Protocol version negotiation v1 lets a client learn which versions of each
protocol a worker speaks, and speak a version both support. In 3.4 every
worker and the extension support exactly `3.4-3.4` for `vgi.v3`, so 3.4 to 3.4
behaves as before. What 3.4 adds is the advertisement and the client-side
machinery for future versions.

## Wire

| Direction | HTTP | Byte transports (stdio, launch, unix, tcp, iroh) |
|---|---|---|
| Server ranges | `VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0` on every response | `vgi_rpc.protocol_versions` on the reflection (`vgi_rpc.Reflection.v1/list_protocols`) answer |
| Client ranges (informational) | `VGI-Client-Protocol-Versions` on every request | `vgi_rpc.protocol_versions_client` request metadata |
| Version spoken | `vgi_rpc.protocol_version` request metadata (negotiated major.minor, client patch) | same |
| Refusal | `protocol_version_unsupported` error, with `vgi_rpc.protocol_versions` in its metadata and the header on the response | same, without the header |

Names are the protocol routing names (`vgi.v3`, `vgi.secret.v1`,
`vgi.attach_tickets.v1`). Only major.minor is negotiated.

## In the extension

- **Window**: `ClientProtocolWindows()` (`src/include/vgi_protocol_window.hpp`)
  lists every protocol the extension versions, with its `min..max` range.
  `src/vgi_protocol_window.cpp` binds it to the generated constants
  (`VGI_PROTOCOL_VERSION_MIN/MAX`, `VGI_SECRET_PROTOCOL_VERSION_MIN/MAX`);
  `vgi.attach_tickets.v1` has no generated window yet and is exactly its
  declared version. For 3.4 every window is one version.
- **ProtocolSession** (`src/include/vgi_protocol_session.hpp`): one per
  endpoint, created at ATTACH and kept on the catalog's
  `VgiAttachParameters::protocol_session()`, shared with its unary RPCs and
  function connections. It holds the server's ranges, the version negotiated
  per protocol and the capability snapshot (`VGI-Upload-URL-Support`,
  `VGI-Arguments-Ref-Bytes`, `VGI-Max-Request-Bytes`, `VGI-Max-Response-Bytes`,
  `VGI-Max-Upload-Bytes`, `VGI-Supported-Encodings`,
  `VGI-Accept-Max-Response-Bytes-Support`). Capability decisions go through
  `ProtocolSession::Supports(ProtocolFeature)` (or `CapabilitiesSupport` on a
  snapshot the HTTP path threads through a call), and the version a request
  stamps through `NegotiatedProtocol(session, protocol)`.
- **Codecs** (`src/include/vgi_protocol_codec.hpp`): requests pass through
  `EncodeRpcRequest` (used by both `WriteRpcRequest` and `SerializeRpcRequest`),
  which stamps the version from the codec registered for (protocol,
  major.minor) and lets it translate the parameters. Unary catalog replies
  pass through the codec's `DecodeResponse`. The current version's codec is
  the identity. Supporting an older version means adding its codec next to the
  identity in `vgi_protocol_session.cpp` and widening the window. A unit test
  checks that every version in a window has a codec.

## ATTACH

1. Byte transports: when the extension window for `vgi.v3` spans more than one
   version, call `list_protocols` before anything else and use its
   `vgi_rpc.protocol_versions` (or, from a worker released before negotiation,
   the exact versions its listing declares). A single-version window (3.4)
   skips this probe: there is nothing to choose, so 3.4 to 3.4 traffic has no
   extra round trip (as in the reference client). The worker's ranges then
   come from a refusal.
2. HTTP: the capability probe and every response carry `VGI-Protocol-Versions`.
   The session stores it, even from a failed response.
3. Negotiate `vgi.v3`. If the ranges are disjoint, fail ATTACH with:
   `protocol_version_unsupported: the VGI worker supports vgi.v3 3.3-3.3; this vgi extension supports vgi.v3 3.4-3.4. The worker is too old: upgrade the worker to one that supports vgi.v3 3.4 or later.`
   (or `... is too new for this extension: upgrade the vgi extension ...`).
   `extra_info` carries `client_protocol_versions` and
   `server_protocol_versions`.
4. If `catalog_attach` is refused, take the server's ranges from the refusal
   and negotiate again. A refusal is `protocol_version_unsupported`, carrying
   `vgi_rpc.protocol_versions` in its error batch. A server released before
   negotiation answers `protocol_version_mismatch`, read with
   `(?:Server:\s*|server requires\s+)(\d+\.\d+)`. Disjoint ranges give the
   error above. Otherwise retry once at the highest common version, which is
   safe because `catalog_attach` is idempotent. No other request is retried.
5. Unknown ranges: speak the client's current version, as before.

Advertisement rules (from the reference's Decisions):
- An absent header or key is a server released before negotiation.
- A present but empty value means the server negotiates but gates nothing.
- Entries that do not parse are skipped, and a repeated name keeps its first
  window.
- A negotiated version carries the client's own patch (`3.5.2` becomes
  `3.4.2`).
- Servers also send the ranges on the `describe` and `__transport_options__`
  replies; the extension reads them from `list_protocols`, HTTP headers and
  refusals.

## Tests

- `test/cpp/test_protocol_session.cpp` (`make test_http_interrupt`): parsing,
  the window and codec registry, negotiation (including a 3.4-3.5 client
  against a 3.4 server), refusal parsing (new and legacy), request metadata,
  the session, and ATTACH over HTTP against a server advertising disjoint,
  overlapping or no ranges.
- `test/sql/protocol_negotiation/attach_negotiated.test`: attach and query.
- `test/sql/protocol_negotiation/range_disjoint.test`: the vgi-python fixture
  started with `VGI_TEST_PROTOCOL_RANGE=3.3-3.3` (`VGI_PROTOCOL_RANGE_WORKER`,
  which the Makefile defines only when the vgi-python checkout has the switch).
- `test/sql/integration/protocol_version/version_mismatch.test`: a worker
  declaring a version outside the window fails ATTACH with the message above.
