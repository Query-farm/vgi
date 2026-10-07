# Catalog discovery and authentication

`vgi_catalogs(location)` lists catalogs before attaching one, and
`vgi_protocols(location)` lists the protocols its worker hosts (see
[Hosted protocols](#hosted-protocols)). For HTTP/HTTPS
and `httpi://` services both use the same authentication handlers as `ATTACH`,
including OAuth challenges, refresh and the `vgi_oauth_enabled` setting.
Public services and non-HTTP discovery continue to work without credentials.

The optional named arguments are:

| Argument | Behavior |
| --- | --- |
| `bearer_token` | Static bearer credential; a rejected token fails without interactive sign-in. |
| `oauth_refresh_token` | Seeds the existing OAuth refresh flow. Mutually exclusive with `bearer_token`. |
| `oauth_profile` | OAuth session profile, default `default` because discovery has no attached alias. Use the same explicit profile on discovery and `ATTACH` to share a session. Session keys also bind the issuer, client and resource. |
| `oauth_cache` | `auto`, `persistent`, `memory` or `none`; defaults to the `vgi_oauth_cache` setting. |

Without explicit credentials, an OAuth challenge can use an existing profile
session or start the normal sign-in flow. Applications that manage sign-in
externally can disable it with `SET vgi_oauth_enabled = false` during an
unauthenticated probe, then retry with their in-memory credential. Enable OAuth
when supplying a refresh token so the existing refresh handler can run.

```sql
SELECT catalog FROM vgi_catalogs('https://service.example/vgi',
    oauth_profile := 'business', oauth_cache := 'memory');
ATTACH 'chosen_catalog' AS business_data
    (TYPE vgi, LOCATION 'https://service.example/vgi',
     oauth_profile 'business', oauth_cache 'memory');
```

Pass credentials through bound SQL parameters where supported. Do not save
credential-bearing SQL in workbooks or query logs. Discovery's EXPLAIN output
contains the worker address but no authentication options or tokens. The
extension cannot redact SQL captured by a client or an external query logger.

## Hosted protocols

Every VGI worker hosts `vgi.v2`, and every worker built on vgi-rpc 0.46 or later
also hosts `vgi_rpc.Reflection.v1` on every transport. Optional capabilities
(attach tickets, report services, identity) are separate vgi-rpc protocols, so
"does this worker support X" means "does its reflection list X".
`vgi_protocols()` shows that list:

```sql
-- By LOCATION, with the same LOCATION schemes, auth and iroh_* options as vgi_catalogs()
SELECT * FROM vgi_protocols('https://service.example/vgi', oauth_profile := 'business');

-- By attached catalog: its connection, its auth, its cached answer
SELECT * FROM vgi_protocols(catalog := 'business_data');
```

| Column | Type | Meaning |
| --- | --- | --- |
| `position` | `INTEGER` | 0-based order as the worker listed it: `vgi.v2` first, then the worker's own protocols, then framework ones such as reflection. |
| `protocol_name` | `VARCHAR` | Wire name, the routing key every request carries. The major version is part of it. |
| `protocol_version` | `VARCHAR` | Declared semver. `NULL` when the protocol declares none (reflection itself). |
| `protocol_hash` | `VARCHAR` | 64 lowercase hex: a fingerprint of the protocol's wire surface, identical in every SDK that hosts the same protocol. |

The reserved `features` list on the wire is always empty and is not shown.

A worker that predates reflection answers `protocol_not_supported` (or, before
routing keys, `method_not_implemented`). By LOCATION that is an error naming
the cause. By catalog it is not: the worker is taken to host exactly `vgi.v2`,
reported as one row with `NULL` version and hash. The catalog form fails with a
binder error for an alias that is not attached or not `TYPE vgi`, and rejects
the LOCATION-only options (auth, `iroh_*`), since the catalog's own are used.

**Caching.** The catalog form, and the extension's internal capability check
(`vgi::HostsProtocol` in `src/include/vgi_reflection.hpp`), share one cached
answer per attached catalog. It is fetched on first use, then reused until
`DETACH`, `vgi_clear_cache()`, or the extension re-establishes the connection
to that LOCATION's worker (a stale pooled subprocess replaced, a launcher
worker relaunched, a shared container restarted), since the new process may be
an upgraded build. HTTP has no connection to re-establish; re-ATTACH or call
`vgi_clear_cache()` after redeploying a worker. The LOCATION form is never
cached.

**DuckDB-WASM.** Both forms use the ordinary unary RPC path, so they work on
every transport the build has: `http(s)://` and `httpi://` through the
browser's fetch, and `worker:` / browser `iroh://` through the SAB transport.

Validation:

```sh
make test_http_bearer 2>&1 | tee /tmp/vgi-bearer.log
make test_catalog_discovery_auth 2>&1 | tee /tmp/vgi-discovery-auth.log
```

The second command uses a loopback OAuth fixture and the release Haybarn binary
to test refresh, discovery-to-ATTACH session reuse, failure handling and EXPLAIN
redaction without signing into a real service or writing a persistent credential.
It requires the sibling `vgi-python` development fixture (override
`VGI_PYTHON_DIR` if needed).
