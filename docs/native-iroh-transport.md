# Native Iroh transport

Native VGI embeds its Iroh client into the DuckDB extension. There is no runtime
download, connector process, localhost proxy, or Rust dependency for extension
users. Rust is a build-time toolchain requirement only.

Raw, connection-stateful VGI uses the canonical `vgi-rpc/arrow-mux/1` ALPN:

```sql
ATTACH 'remote' AS remote (
  TYPE vgi,
  LOCATION 'iroh://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
);
```

HTTP-state VGI uses `iroh-http/2` while retaining the existing HTTP continuation,
capability, compression, response-budget, authentication, cookie, and error logic:

```sql
ATTACH 'remote' AS remote (
  TYPE vgi,
  LOCATION 'httpi://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/vgi'
);
```

The client authenticates the remote endpoint ID cryptographically in both modes.
That proves possession of the endpoint key; worker authorization remains an
operator policy.

By default the client creates one process-stable ephemeral identity. A stable
identity can be scoped to a worker without placing key material in SQL history:

```sql
CREATE SECRET worker_iroh_identity (
  TYPE iroh,
  SECRET_KEY '…',
  SCOPE 'iroh://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
);
```

An `iroh_secret_key` ATTACH option takes precedence over a scoped secret. Invalid
configured keys fail closed; they never downgrade to an ephemeral identity.
Although VGI redacts that option from catalog diagnostics after binding, key
text written directly in SQL may already have been captured by the caller's
SQL history or logging. Use a scoped secret for persistent identities.
`iroh_relay_urls` selects custom relays and `iroh_no_relay` disables relays; the
two options are mutually exclusive. Connect and I/O budgets are controlled by
`vgi_iroh_connect_timeout_seconds` and `vgi_iroh_io_timeout_seconds`.

Private or relay-disabled workers can supply the remote address information
that an EndpointId alone cannot discover: `iroh_remote_relay_url` accepts the
worker's relay URL and `iroh_direct_addresses` accepts a `VARCHAR[]` of Iroh
socket-address strings. These are public routing hints, not identities; the
authenticated EndpointId remains authoritative.

Externalized HTTP response pointers must still be absolute HTTP(S) URLs. An Iroh
worker that cannot expose such object storage should use HTTP continuations
within the advertised response budget instead.
