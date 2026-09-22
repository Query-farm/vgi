# LOCATION transport policy (`vgi_allowed_transports`)

A service that lets users run SQL, including `ATTACH`, usually must not let them make VGI
start local processes. A bare LOCATION is a shell command, so `ATTACH 'sh -c "…"' (TYPE vgi)`
runs whatever it is given. `vgi_allowed_transports` restricts which worker transports a LOCATION
may use. Like DuckDB's own lockdown settings it can only be narrowed: once a database instance
is restricted, no connection on it can widen the setting again. A new database instance is needed.

## Operator flow

```sql
LOAD vgi;
-- Optional: attach trusted local workers first; they keep working afterwards.
ATTACH 'trusted' AS trusted (TYPE vgi, LOCATION '/opt/workers/trusted-worker');
SET vgi_allowed_transports = 'https';
-- Optional, and recommended when handing SQL to users:
SET enable_external_access = false;
SET lock_configuration = true;
```

The value cannot be passed when the database is opened (`duckdb.connect(config={...})`). DuckDB
only autoloads startup options for extensions in its built-in entries table, and it does not list
vgi, so the open fails with an unrecognized-option error. Run `SET` after `LOAD` instead.

## Tokens

Comma-separated and case-insensitive. The default is `all`; `none` refuses every transport.

| Token | LOCATIONs | Local? |
|---|---|---|
| `subprocess` | bare command, including the haybarn `vgi:` form | yes |
| `launch` | `launch:<argv>` | yes |
| `unix` | `unix:///path` (a named pipe on Windows) | yes |
| `oci` | `oci://`, `docker://` | yes |
| `github` | `github://`, `github-auto://` | yes |
| `database` | `database://` | yes |
| `http` / `https` | `http://` / `https://` (separate, so plain http can be forbidden) | no |
| `tcp` | `tcp://host:port` | no |
| `httpi` | `httpi://<EndpointId>` | no |
| `iroh` | `iroh://<EndpointId>` | no |
| `worker` | `worker:<url>` (DuckDB-WASM only) | no |

An unknown or empty token is an error, so a typo can never loosen the policy.

## Rules

- **Narrow-only.** A `SET` may only remove transports. Adding one back, or `RESET` (which
  restores `all`), is refused with "Cannot widen vgi_allowed_transports while the database is running".
- **Global only.** `SET SESSION` is refused. The enforced value lives in extension-owned,
  per-database state and never in the setting itself, so no session value or other route into
  the option can change what is enforced. Narrowing uses compare-and-swap, so two concurrent SETs
  can't combine into a widening.
- **`enable_external_access = false`** also refuses every *local* transport, whatever the
  setting says. DuckDB already prevents turning external access back on.
- **Where it applies.** The check runs when a LOCATION enters VGI: `ATTACH`, and `vgi_catalogs()`
  both at bind and again at execution, so a statement prepared before narrowing is re-checked.
  It runs before any I/O: container inspection, package resolution, connecting or spawning.
- **Existing catalogs keep working.** Catalogs attached before the setting was narrowed keep
  running, spawning and pooling their workers. This is the same model as extensions loaded before
  `enable_external_access=false`. A DETACH followed by a new ATTACH counts as a new ATTACH.

## Classification follows dispatch

A LOCATION is classified as the transport VGI's dispatch will actually use on this build, not by
what it looks like. Anything that no other transport claims falls through to `subprocess`, as it
does in dispatch:

- On native builds `worker:x` is a command, because only DuckDB-WASM has a `worker:` transport.
- `iroh://` and `httpi://` are matched case-sensitively, so `IROH://x` is a command.
- A leading space defeats every scheme prefix, so `' https://…'` is a command.

Some LOCATIONs are refused whatever the policy says:

- The internal tokens `vgi-artifact:` and `container-shared:`, which ATTACH creates itself and
  users never type.
- `database://` given to `vgi_catalogs()`, which never resolves it. There the raw string would
  otherwise go to the shell.

## Limitations

- **Transport, not destination.** `tcp`, `http(s)` and `iroh` (through `iroh_direct_addresses` /
  `iroh_relay_urls`) can still reach services listening on loopback.
- **An ATTACH already running** when the setting is narrowed will finish.
- **Worker cache directories.** The directory settings (`vgi_result_cache_dir`,
  `vgi_github_cache_dir`, `vgi_worker_cache_dir`) are not governed by this policy. Use
  `lock_configuration` to freeze them.

Refusals raise `PermissionException` and log a `location_policy.refused` event (`entry`,
`transport`, `reason` ∈ `not_allowed` / `external_access_disabled` / `internal`). Files:
`src/vgi_location_policy.{cpp,hpp}`. Tests: `test/sql/integration/location_policy/*`.
