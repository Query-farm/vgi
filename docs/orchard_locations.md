# `orchard://` locations

```sql
ATTACH 'hp' AS hp (TYPE vgi, LOCATION 'orchard://orchard-e2e/hosted-python');
SELECT hp.main.pass_claims(1);
```

`orchard://<workspace>/<deployment>` names a deployment hosted on Query.Farm
Orchard. ATTACH signs in at the Orchard **Front Desk** and then talks directly to
the deployment's hosted worker at its **Field Address**. The design is
[location-delegation.md](location-delegation.md); this page describes what the
extension implements today (the MVP).

## Parsing

Both parts are slugs: 1–63 lowercase letters, digits and dashes, starting and
ending with a letter or digit (`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`). The
scheme is case-insensitive. Anything else (a missing or extra part, a query,
fragment or userinfo) is a binder error before any network I/O.

The bare form works too: `ATTACH 'orchard://acme/tiles' (TYPE vgi)` discovers the
catalog name from the worker.

Only ATTACH resolves `orchard://`. `vgi_catalogs()` and `vgi_protocols()` refuse
it. The location policy classifies it as `https`, and the Field Address is
checked again after sign-in.

## Sign-in (at ATTACH, before any RPC)

The issuer is fixed: `https://auth.queryfarm.cloud`, with no discovery. The
RFC 8707 `resource` is `https://desk.queryfarm.cloud/<workspace>/<deployment>`
and the client is `vgi-duckdb` (public, no secret, no scope).

- **`oauth_refresh_token`** given: one `refresh_token` grant with `resource`.
  If it is refused, ATTACH fails; it never falls back to a browser.
- Otherwise, interactive, chosen like the existing OAuth flows
  (`vgi_oauth_flow`, default `auto`):
  - **headless** (SSH, `CI=true`, Docker, Kubernetes, Linux without a display,
    Colab) or `vgi_oauth_flow='device_code'`: RFC 8628 device code. The prompt
    (URL `https://auth.queryfarm.cloud/device` and an 8-character code) is
    printed on stderr and to the DuckDB log.
  - **desktop**: browser PKCE with a loopback redirect to exactly
    `http://127.0.0.1:<port>/oauth-callback.html`. The callback's `state` and
    RFC 9207 `iss` are checked. If the port can't be bound, device code.
  - Both wait at most `vgi_oauth_timeout_seconds` (default 120); Ctrl-C cancels.

The token response carries `access_token` (the **Pass**, a 15-minute ES256 JWT),
`refresh_token` (the **Renewal Token**, `ort1.…`, rotated on every refresh; the
extension always keeps the newest) and `vgi_location`, the Field Address.

## Field Address

`vgi_location` must be `https://` with an optional port, no path, query,
fragment or userinfo, not an IP literal, and a host under
`*.field.queryfarm.cloud`. Otherwise ATTACH is refused. It is the catalog's HTTP
worker for the life of the attach; a different `vgi_location` on a later refresh
is logged and ignored (re-attach to move).

`duckdb_databases()` keeps showing the `orchard://` LOCATION; `vgi_oauth_tokens()`
shows the Field Address origin.

## Refresh and 401s

- A Pass with under 5 minutes left is refreshed by one thread while the others
  keep using it; an expired Pass makes callers wait for that one refresh.
- A 401 from the Field Worker: its challenge is ignored. The extension refreshes
  once, then retries the request; a second 401 fails. Parallel 401s cause one
  refresh: a request whose rejected Pass has already been replaced just retries
  with the new one.
- Refresh errors: `invalid_grant` fails with "Sign in again: DETACH and re-run
  ATTACH" (and every later query on the catalog fails the same way; no browser is
  ever opened after ATTACH); 5xx, 429 and `temporarily_unavailable` are retried
  after 1, 2 and 4 s; `invalid_target` names the deployment.

The Renewal Token lives only in memory: it is not persisted across processes
(out of scope for the MVP). `oauth_refresh_token` is redacted in
`duckdb_databases()` like `bearer_token`; Passes and Renewal Tokens never appear
in errors.

## Test hooks (environment, not settings)

| Variable | Effect |
| --- | --- |
| `VGI_ORCHARD_ISSUER` | Replace `https://auth.queryfarm.cloud` (https or loopback http). |
| `VGI_ORCHARD_DESK_ORIGIN` | Replace `https://desk.queryfarm.cloud` in the `resource`. |
| `VGI_TEST_ALLOW_LOOPBACK_DELEGATION=1` | Also accept a loopback `http://127.0.0.1:<port>` / `localhost` Field Address. |
| `VGI_TEST_ORCHARD_TAMPER_FIRST_PASS=1` | Spoil the first Pass so the first RPC draws a 401 (exercises refresh-and-retry). |

## Testing

- Offline: `test/sql/orchard/orchard_location.test` (parsing, policy, option
  conflicts, redaction).
- Live, manual, not in CI: `scripts/orchard_e2e.sh` attaches
  `orchard://orchard-e2e/hosted-python` with a seeded Renewal Token read from a
  file (default `~/orchard-e2e/ext-renewal.txt`), calls `pass_claims`, and
  checks one forced 401 → one refresh → retry. Each run spends the token.

## Not yet

WASM sign-in, Workloads, persisting the Renewal Token, generic https delegation
(spec §3), the per-connection effective base URL (spec §9), and a result-cache
scope by `(iss, Desk Address, sub)`: today the cache scope is the Pass
fingerprint, so it resets every 15 minutes.
