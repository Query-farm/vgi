# VGI Telemetry

The VGI extension sends a small amount of anonymous usage telemetry to Query.Farm so we
can understand adoption — which transports, server/worker types, and auth methods are used
in the wild. It contains **no personal data and no persistent identifier**, and it is easy
to turn off.

## Opting out

Set the environment variable to any non-empty value before starting DuckDB:

```bash
export QUERY_FARM_TELEMETRY_OPT_OUT=1
```

This disables **all** telemetry (both events below), including the per-process session id.

## What is sent, and when

Telemetry is a fire-and-forget HTTPS `POST` of a small JSON body. It is asynchronous and
best-effort: failures are ignored and never affect your query. It requires the `httpfs`
extension (auto-loaded); if `httpfs` is unavailable, nothing is sent.

| Event | When | Endpoint |
|-------|------|----------|
| `load`   | Once, when the extension loads | `https://duckdb-in.query-farm.services/` |
| `attach` | Once per **successful** `ATTACH ... (TYPE vgi)` | `https://vgi-in.query-farm.services/` |

The `load` event is the long-standing 7-field extension-load ping (extension/DuckDB
versions + platform). The `attach` event is described below. Failed attaches send nothing.

## Privacy stance

- **No PII.** No user email, no OAuth subject, no query text, no table/column data.
- **No persistent identifier.** `session_id` is a random UUID minted once per process, in
  memory — it is never written to disk and cannot correlate you across restarts. There is no
  install id or machine id.
- **Credential scrubbing.** Any worker location that is sent is scrubbed first: URL userinfo
  (`user:pass@`), auth-bearing query params (`token`, `sig`, `X-Amz-*`, …), and token-shaped
  flags in a `launch:` argv (`--bearer-token`, `--password`, …) are replaced with `REDACTED`.
  Auth **tokens themselves are never included** — only the auth *mode*.
- **Federation is summarized, not enumerated.** Companion (DuckLake/Iceberg/…) catalogs are
  reported as a count plus their engine `db_type`s only — never their targets/DSNs.

## The `attach` event

Top-level keys are flat, typed scalars; the `catalog` / `transport` / `auth` / `options`
groups are sent as **embedded JSON strings** (so they map 1:1 to `JSON` columns on ingest).
Example (pretty-printed; the group values are strings on the wire):

```json
{
  "schema_version": 2,
  "event": "attach",
  "event_ts": "2026-07-11T14:22:08.412Z",
  "session_id": "5f2c9e1a-7b04-4e3d-9a11-0c6d2f8e4b71",
  "attach_seq": 3,
  "extension_name": "vgi",
  "extension_version": "2026011201",
  "user_agent": "query-farm/20260201",
  "host_kind": "cli",
  "duckdb_platform": "osx_arm64",
  "duckdb_version": "1.1.3",
  "duckdb_release_codename": "Eatoni",
  "duckdb_source_id": "19864453f7",
  "attach_duration_ms": 842,
  "catalog":   "{\"name\":\"sales_warehouse\",\"version\":42,\"impl_version\":\"0.9.2\",\"data_date\":\"2026-06-01\",\"secret_service_url\":\"https://secrets.example/orchard\",\"companion_count\":1,\"companion_types\":[\"ducklake\"]}",
  "transport": "{\"type\":\"container\",\"scheme\":\"oci\",\"location\":\"oci://ghcr.io/query-farm/vgi-sklearn:latest\",\"container\":{\"runtime\":\"docker\",\"connection\":\"tcp\",\"shared\":true}}",
  "auth":      "{\"mode\":\"oauth\",\"interactive\":false,\"oauth_issuer\":\"https://accounts.google.com\"}",
  "options":   "{\"pool\":true,\"pool_max\":256,\"pool_timeout\":5,\"secrets\":true,\"cache\":true,\"attach_companions\":true,\"attach_companion_secrets\":false}"
}
```

### Field reference

**Envelope / scalars**

| Field | Type | Meaning |
|-------|------|---------|
| `schema_version` | int | Payload schema version (currently `2`). Additive within a major; bumped only on a breaking rename/retype. |
| `event` | string | Always `"attach"`. |
| `event_ts` | timestamp | Client wall-clock (UTC) at attach completion. |
| `session_id` | uuid | Random per-process id (in-memory; not persisted). |
| `attach_seq` | int | 1-based counter of attaches within this process. |
| `extension_name` / `extension_version` | string | The VGI extension. |
| `user_agent` | string | Query.Farm telemetry client tag. |
| `host_kind` | string | DuckDB client surface: `cli` / `python` / `node` / `jdbc` / `capi` / `cpp` / `wasm` / `other` (from the `duckdb_api` setting; `cpp` = a C++ embedder *or* a client that didn't self-identify). |
| `duckdb_platform` / `duckdb_version` / `duckdb_release_codename` / `duckdb_source_id` | string | DuckDB build info. |
| `attach_duration_ms` | int | Wall-clock time for the successful attach. |

**`catalog`** (JSON) — facts resolved from the worker's `catalog_attach` response

| Key | Meaning |
|-----|---------|
| `name` | The attached catalog name (`?`-query connection strings are redacted). |
| `version` | Catalog data version (int), or null. |
| `impl_version` / `data_date` | Worker-resolved implementation / data version, or null. |
| `secret_service_url` | Advertised Orchard secret-service URL (scrubbed), or null. |
| `companion_count` | Number of companion catalogs the worker advertised (federation). |
| `companion_types` | Their `db_type`s (e.g. `["ducklake"]`) — engine kinds only, never targets. |

**`transport`** (JSON)

| Key | Meaning |
|-----|---------|
| `type` | Normalized transport: `subprocess` / `http` / `tcp` / `unix` / `launch` / `container` / `github`. |
| `scheme` | Exact scheme: `http`/`https`/`oci`/`docker`/`github`/`github-auto`/… |
| `location` | The scrubbed worker location. |
| `container` | For container transports: `{runtime, connection, shared}`; otherwise null. |

**`auth`** (JSON)

| Key | Meaning |
|-----|---------|
| `mode` | `none` / `oauth` / `bearer` / `oauth_refresh_token`. Never includes the token. |
| `interactive` | Whether a human device-code/browser flow ran for this catalog. |
| `oauth_issuer` | The OIDC issuer URL when available (best-effort: present for interactive OAuth that returned an id_token; often null for headless refresh, always null for bearer / non-HTTP), or null. |

**`options`** (JSON) — the ATTACH options the user set: `pool`, `pool_max`, `pool_timeout`,
`secrets`, `cache`, `attach_companions`, `attach_companion_secrets`.

## Parquet / warehouse mapping

Every top-level key is one column: scalars are typed (`event_ts` TIMESTAMP, `attach_seq` /
`attach_duration_ms` / `schema_version` INT, the rest VARCHAR), and `catalog` / `transport` /
`auth` / `options` are `JSON` columns queried with e.g. `catalog->>'$.name'`,
`transport->>'$.type'`, `auth->>'$.mode'`.

## Implementation notes

- Extension side: `src/vgi_telemetry.cpp` (event build + send), `src/vgi_telemetry_util.cpp`
  (pure scrub/classify/host_kind helpers, unit-tested in `test/cpp/test_telemetry.cpp`),
  emit site in `VgiCatalogAttach` (`src/vgi_extension.cpp`), shared transport in
  `src/query_farm_telemetry.cpp`.
- The `attach` event is received by a dedicated ingestion worker (separate repo / subdomain);
  the shared `load`-ping infrastructure is left untouched.
