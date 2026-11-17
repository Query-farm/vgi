# `orchard://` locations

```sql
ATTACH 'functions' AS hp (TYPE vgi, LOCATION 'orchard://orchard-e2e/hosted-python');
SELECT hp.main.pass_claims(1);
```

As with every vgi location, the first argument is the catalog to attach (a
worker may host several) and `LOCATION` names where the worker is. The location
names the deployment, never a catalog.

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

Only ATTACH resolves `orchard://`. `vgi_catalogs()` and `vgi_protocols()` refuse
it. The location policy classifies it as `https`, and the Field Address is
checked again after sign-in.

## Sign-in (at ATTACH, before any RPC)

The issuer is fixed: `https://auth.queryfarm.cloud`, with no discovery. The
RFC 8707 `resource` is `https://desk.queryfarm.cloud/<workspace>/<deployment>`
and the client is `vgi-duckdb` (public, no secret, no scope).

In order:

1. **`oauth_refresh_token`** given: one `refresh_token` grant with `resource`,
   for this attach only. It is never read from or written to the credential
   store, so a test token can't overwrite a stored sign-in. If it is refused,
   ATTACH fails; it never falls back to a browser.
2. **The stored sign-in** (see below), when there is one: refreshed under the
   store's lock. If the Front Desk answers `invalid_grant` (revoked, signed
   out, the 30-day limit, reuse detected, membership removed), the entry is
   deleted, a notice is printed, and ATTACH goes on to step 3.
3. **Interactive**, chosen like the existing OAuth flows (`vgi_oauth_flow`,
   default `auto`):
   - **headless** (SSH, `CI=true`, Docker, Kubernetes, Linux without a display,
     Colab) or `vgi_oauth_flow='device_code'`: RFC 8628 device code. The prompt
     (URL `https://auth.queryfarm.cloud/device` and an 8-character code) is
     printed on stderr right away, with a line saying where the sign-in will be
     saved (or why it won't be), and to the DuckDB log at INFO.
   - **desktop**: browser PKCE with a loopback redirect to exactly
     `http://127.0.0.1:<port>/oauth-callback.html`. The callback's `state` and
     RFC 9207 `iss` are checked. If the port can't be bound, device code.
   - Both wait at most `vgi_oauth_timeout_seconds` (default 120; set it higher
     for a person typing a code); Ctrl-C cancels.
   The result is saved to the store.

Interactive sign-in only ever happens during ATTACH. After ATTACH, a failed
refresh fails the query.

The token response carries `access_token` (the **Pass**, a 15-minute ES256 JWT),
`refresh_token` (the **Renewal Token**, `ort1.…`, rotated on every refresh; the
extension always keeps the newest) and `vgi_location`, the Field Address.

## Persistence across processes

Only the **Renewal Token** is stored, never the Pass or the Field Address
(every refresh returns the current Field Address). The store is the extension's
OAuth credential store (`src/vgi_oauth_store.cpp`):

| Platform | Backend |
| --- | --- |
| macOS | the login Keychain (service `com.queryfarm.vgi.oauth`) |
| Windows | a DPAPI-protected file under `%LOCALAPPDATA%\QueryFarm\VGI\oauth` |
| Linux | the Secret Service through `libsecret-1.so.0`, loaded at run time with `dlopen` (no build dependency), when a D-Bus session is present and a Secret Service answers; otherwise a mode-0600 file in `$XDG_STATE_HOME/queryfarm-vgi` (default `~/.local/state/queryfarm-vgi`), a 0700 directory this user owns. A directory or file that is not owner-only, is a symlink or belongs to someone else is refused. If neither backend works, the sign-in stays in memory and the sign-in prompt says why. |
| DuckDB-WASM | never stored |

On Linux, only Orchard sign-ins use these backends; other OAuth sign-ins stay
memory-only there, as before.

**Key:** `oauth_profile` (default `default`, not the catalog alias), the issuer,
the Desk resource and the client. Every catalog that attaches the same
deployment shares one stored sign-in; different deployments, issuers or profiles
stay separate. `oauth_cache` applies: `auto` (default) stores when it can,
`persistent` fails ATTACH when it can't, `memory` / `none` never store.

**Shared rotation.** Every refresh rotates the Renewal Token, and the Front
Desk kills the whole family if a rotated token is presented more than 30 s
after its rotation. So every refresh, at ATTACH and later, in every process:

1. takes the store's cross-process lock: `flock` on
   `/tmp/queryfarm-vgi-oauth-<uid>/<sha256>.lock` (macOS and Linux; a named
   mutex on Windows). The directory must be a 0700 directory owned by this
   user, not a symlink; otherwise the sign-in is not stored (`auto`) or ATTACH
   fails (`persistent`);
2. re-reads the stored token and uses it if another process rotated it;
3. refreshes, writes the successor back, and releases the lock.

If a write fails, the entry is deleted and that session continues in memory,
so no stale token is left for another process to present. If the entry
disappears while a session is running (signed out in another process), that
session keeps its in-memory sign-in but does not write it back.

**Forgetting it:** `SELECT * FROM vgi_oauth_logout()` (or
`vgi_oauth_logout(catalog := 'hp')`) deletes the stored entry for that
deployment. `DETACH` does not: the sign-in is kept for the next ATTACH.

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
- Refresh errors: `invalid_grant` deletes the stored entry and fails with
  "Sign in again: DETACH and re-run ATTACH" (every later query on the catalog
  fails the same way; no browser is ever opened after ATTACH); 5xx, 429 and
  `temporarily_unavailable` are retried after 1, 2 and 4 s; `invalid_target`
  names the deployment.

`oauth_refresh_token` is redacted in
`duckdb_databases()` like `bearer_token`; Passes and Renewal Tokens never appear
in errors.

## Test hooks (environment, not settings)

| Variable | Effect |
| --- | --- |
| `VGI_ORCHARD_ISSUER` | Replace `https://auth.queryfarm.cloud` (https or loopback http). |
| `VGI_ORCHARD_DESK_ORIGIN` | Replace `https://desk.queryfarm.cloud` in the `resource`. |
| `VGI_TEST_ALLOW_LOOPBACK_DELEGATION=1` | Also accept a loopback `http://127.0.0.1:<port>` / `localhost` Field Address. |
| `VGI_TEST_ORCHARD_TAMPER_FIRST_PASS=1` | Spoil the first Pass so the first RPC draws a 401 (exercises refresh-and-retry). |
| `VGI_TEST_ORCHARD_SEED_TOKEN_FILE=<path>` | Stand in for the interactive sign-in with a Renewal Token read from the file; the result is stored like an interactive sign-in. |
| `VGI_TEST_ORCHARD_PASS_TTL_SECONDS=<n>` | Cap the Pass lifetime, so refreshes happen mid-session. |
| `VGI_OAUTH_LINUX_STORE=file\|secret-service\|none` | Force the Linux credential backend. |

## Testing

- Offline: `test/sql/orchard/orchard_location.test` (parsing, policy, option
  conflicts, redaction).
- Live, manual, not in CI: `scripts/orchard_e2e.sh` attaches
  `orchard://orchard-e2e/hosted-python` with a seeded Renewal Token read from a
  file (default `~/orchard-e2e/ext-renewal.txt`), calls `pass_claims`, and
  checks one forced 401 → one refresh → retry. Each run spends the token.
- Live, manual: `scripts/orchard_persist_e2e.sh` (Linux file backend, private
  `XDG_STATE_HOME`): a saved sign-in reused by a second process, four
  concurrent processes each refreshing twice, two long-lived processes whose
  refreshes interleave across the 30 s window, the family still alive after
  all of it, and `invalid_grant` deleting the entry and starting device code.

## Not yet

WASM sign-in, Workloads, generic https delegation
(spec §3), the per-connection effective base URL (spec §9), and a result-cache
scope by `(iss, Desk Address, sub)`: today the cache scope is the Pass
fingerprint, so it resets every 15 minutes.
