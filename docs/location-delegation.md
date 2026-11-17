# Location delegation: sign in once, then connect straight to the worker

Status: **draft for review** (2026-11-17).

The domain is **`queryfarm.cloud`**, registered through Cloudflare (2026-11-17)
in its **own Cloudflare account, "Orchard"**, separate from query.farm's.

| Host | Serves |
| --- | --- |
| `auth.queryfarm.cloud` | The Front Desk: issuer, `/authorize`, `/device`, `/token`, consent and confirmation pages, Key Set, Admin API |
| `desk.queryfarm.cloud` | Desk Addresses, `https://desk.queryfarm.cloud/d/<deployment>` |
| `<label>.field.queryfarm.cloud` | Field Addresses, CNAMEs to Fly apps |

`.cloud` isn't on browsers' built-in HTTPS-only list the way `.app` and `.dev`
are, so the zone sends `Strict-Transport-Security: max-age=63072000;
includeSubDomains; preload` from day one and is submitted to hstspreload.org.

## Orchard locations (what customers type)

```sql
ATTACH 'mvt' AS mvt (TYPE vgi, LOCATION 'orchard://acme/vector-tiles');
```

`orchard://<workspace>/<deployment>` is the customer-facing location. Both parts
are slugs: lowercase letters, digits and dashes, 1–63 characters, starting and
ending with a letter or digit. A workspace slug is globally unique; a deployment
slug is unique within its workspace.

- **The scheme fixes the issuer.** For `orchard://` the extension and other
  clients use `https://auth.queryfarm.cloud` without discovery. No server can
  claim to delegate or steer the client to another authorization server.
- **Resource and audience (option B).** The location maps to the pretty Desk
  Address `https://desk.queryfarm.cloud/<workspace>/<deployment>`, which clients
  send as the RFC 8707 `resource`. The Front Desk resolves the name to the
  deployment's stable id **after sign-in** (unknown and forbidden look the same,
  T11). Every Pass's `aud` is the id form `https://desk.queryfarm.cloud/d/<id>`,
  so renaming a deployment never invalidates Passes or worker configuration.
- **No Desk round trip.** A client given an `orchard://` location goes straight
  to the Front Desk. The https Desk Addresses, by id or by name, still answer
  the probe and challenge, for clients that only speak plain RFC 9728 OAuth.
- **Generic https delegation** (a server *claiming* delegation through its
  metadata, §3) is **deferred**. The Front Desk keeps supporting it, but the
  extension implements `orchard://` first.

## Components

| Name | What it is | Runs where | Wire / code name |
| --- | --- | --- | --- |
| **Front Desk** | The Orchard OAuth authorization server. Signs users in (via AuthKit), owns the Registry, issues Passes and Renewal Tokens, tells clients the Field Address. Speaks only enough VGI to answer the health probe and the challenge. | Its own Worker in its own Cloudflare account | issuer `https://auth.queryfarm.cloud` |
| **Desk Address** | What the customer types in `ATTACH … LOCATION`. Stable. Answers the VGI health probe and an OAuth challenge, nothing else. | Front Desk | `https://desk.queryfarm.cloud/d/<deployment>`; also the OAuth `resource` and the Pass `aud` |
| **AuthKit** | Where people log in (Query.Farm accounts). The Front Desk is its confidential client; AuthKit tokens never leave the Front Desk. | WorkOS | — |
| **Pass** | The access token: a JWT naming one principal (a user or a Workload) and one Desk Address, and the workspace that owns it. | Issued by the Front Desk; checked by the Field Worker | RFC 9068 `typ: at+jwt`, ES256 |
| **Renewal Token** | The refresh token, rotated on every use. | Front Desk; kept by the Pass Holder | `refresh_token`, `ort1.…` |
| **Signing Key / Key Set** | The Front Desk's private ES256 key, and its public half. | Front Desk | `https://auth.queryfarm.cloud/.well-known/jwks.json` |
| **Registry** | The source of truth for access and routing: workspaces, members (bound to WorkOS `sub`), deployments, Desk Addresses, Field Addresses, Renewal Token families, sign-out times. **Written only by the Front Desk.** | D1 in the Front Desk's account | D1 `orchard-registry` |
| **Field Worker** | The customer's hosted VGI worker, running the SDK's **hosted mode**. One process. | Fly.io, in the customer's region | any vgi SDK worker |
| **Field Address** | The Field Worker's address on **our** domain, a CNAME to its Fly app. Created by the Front Desk. | DNS in the Front Desk's account → Fly.io | `https://<label>.field.queryfarm.cloud`; token response `vgi_location` |
| **Pass Holder** | The extension's delegated mode of its existing OAuth client: sends `resource`, keeps Pass and Renewal Token, routes to the Field Address. | Inside the vgi extension (native and WASM) | state in `OAuthCatalogAuth` / `AuthState` |
| **Workload** | A non-human principal registered in a workspace: a job, a service, a scheduled task, a CI pipeline. Gets Passes without a person present. | Registry; runs anywhere | Pass `sub` = its opaque Registry id |
| **Workload Key** | A public key registered for a Workload. The Workload keeps the private key and signs short assertions with it. The Front Desk never holds a secret for it. | Public half in the Registry | RFC 7523 `private_key_jwt` |
| **Trust Rule** | Lets a platform identity (GitHub Actions, Google Cloud, Fly Machines, Kubernetes, Azure managed identity, AWS IAM role) act as a Workload, with no stored key at all. | Registry | RFC 7523 JWT-bearer grant |
| **Console / Tracker** | query.farm pages. A UI only: every membership and routing change is a call to the Front Desk's Admin API made **as the signed-in user**, through the dedicated Console client (see The Admin API). Changes that create lasting access are confirmed on a Front Desk page, not in the Console. Workspace admins register Workloads, Workload Keys and Trust Rules here too. Billing, requests and leads stay in the site's own D1. | query.farm (Pages, query.farm Cloudflare account) | `/products/orchard/console/…` |


```
 ┌──────── Cloudflare account "Orchard" (queryfarm.cloud) — Front Desk Worker ──────────┐
 │ desk.queryfarm.cloud/d/<id>  HEAD /health (VGI caps) · 401 + resource metadata       │
 │ auth.queryfarm.cloud         /authorize · /device · /token · consent · JWKS · Admin  │
 │ Registry (D1)                members(sub) · deployments · Field Addresses · families │
 │ DNS                          <label>.field.queryfarm.cloud  CNAME → Fly app          │
 └──────▲───────────────────────▲──────────────────────────────────▲────────────────────┘
        │ OAuth (browser,       │ Admin API, as the signed-in       │ JWKS (cached ≤10 min)
        │ device code, WASM)    │ user, Console client only         │
 ┌──────┴──────┐        ┌───────┴────────────────────┐              │
 │ DuckDB +    │        │ query.farm Console/Tracker │              │
 │ vgi ext.    │        │ (query.farm account: UI,   │              │
 │ Pass Holder │        │  billing)                  │              │
 └──────┬──────┘        └────────────────────────────┘              │
        │ VGI over HTTPS, Bearer <Pass>, to the Field Address       │
        ▼                                                           │
 ┌────────── Fly.io, customer region ───────────────────────────────┴───┐
 │ Field Worker (SDK hosted mode) at <label>.field.queryfarm.cloud      │
 └──────────────────────────────────────────────────────────────────────┘
```

## Goals

1. A Desk Address the customer types and we own, stable across moves and
   re-regioning.
2. Sign-in with a Query.Farm account; access only for workspace members.
3. Query traffic goes **directly** to the Field Worker, never through a
   Query.Farm proxy.
4. Field Workers check access themselves, without calling home on every
   request.
5. Field Workers know who is calling (user id, email, name, workspace),
   signed, so it can't vary between requests or be edited by the
   client.
6. It works from native DuckDB (desktop and headless) and from duckdb-wasm.
7. Unattended jobs and services can connect without a person, using a key
   pair or their platform's own identity token, never a shared secret.

## Flow (native, interactive)

```
Pass Holder                         Front Desk                               Field Worker
───────────                         ──────────                               ────────────
ATTACH … LOCATION 'https://desk.queryfarm.cloud/d/dep_123'
0 ── HEAD /health ─────────────────▶ 200 + VGI capability headers
1 ── first VGI request ────────────▶ 401 + VGI capability headers +
                                      WWW-Authenticate: Bearer resource_metadata="…"
2 ── GET resource metadata ────────▶ { resource: <Desk Address>, authorization_servers: [https://auth.queryfarm.cloud] }
   check: canonical(resource) == canonical(LOCATION); AS metadata has resource_indicators_supported
3 ── browser: /authorize?resource=…&code_challenge=…&client_id=vgi-duckdb&redirect_uri=http://127.0.0.1:<port>/oauth-callback.html
                                      login at AuthKit → member? → consent (always, public client)
   ◀── code + iss (RFC 9207)          check iss == AS
4 ── POST /token (code, verifier, resource) ─▶
   ◀── { access_token: <Pass>, refresh_token, expires_in: 900, vgi_location: https://k7f2….field.queryfarm.cloud }
   check vgi_location against the allowed delegation hosts for this issuer
5 ── HEAD /health on the Field Address (re-probe capabilities) ─────────────▶ 200
6 ── VGI requests, Bearer <Pass> ───────────────────────────────────────────▶ verify locally
7 ── at < 5 min left (single flight): POST /token grant_type=refresh_token, resource=… ─▶ Registry re-checked
   ◀── new Pass + new Renewal Token + vgi_location
```

## Protocol

### 1. Canonical resource form

Every comparison uses one canonical form. That covers the extension's check,
the Front Desk lookup, the Pass `aud` and `VGI_JWT_AUDIENCE`.

- Scheme `https`, lowercase.
- Host lowercase, IDNA A-label form.
- No port, or a non-default port written as-is; `:443` is removed.
- The path as registered, with no trailing slash, no dot segments and no
  percent-encoding changes.
- No query, fragment or userinfo.

Comparisons are exact string matches after canonicalisation. Anything that
canonicalises differently is a different resource.

### 2. The Desk Address

- `HEAD /health` → `200` with the VGI capability headers the extension
  requires (`vgi_http_client.cpp:1269-1276`, including
  `accept_max_response_bytes_support`).
- Any other request → `401` with the same capability headers and
  `WWW-Authenticate: Bearer resource_metadata="https://desk.queryfarm.cloud/.well-known/oauth-protected-resource/d/<id>"`
  (the RFC 9728 path-suffixed form).
- The metadata:

  ```json
  { "resource": "https://desk.queryfarm.cloud/d/dep_123",
    "authorization_servers": ["https://auth.queryfarm.cloud"],
    "resource_name": "Acme Analytics · Vector tiles (MVT)" }
  ```

  It carries no `token_endpoint`, `client_id`, `client_secret` or
  `use_id_token` overrides. In delegated mode the extension ignores those
  overrides even if a server sends them.
- The capabilities a Desk Address advertises describe only the Desk Address.
  The extension re-probes at the Field Address once routed.

### 3. Authorization server metadata

`https://auth.queryfarm.cloud/.well-known/oauth-authorization-server` (RFC 8414) lists:
- `authorization_endpoint`, `token_endpoint`, `device_authorization_endpoint`
  and `jwks_uri`, all on `https://auth.queryfarm.cloud`;
- `code_challenge_methods_supported: ["S256"]`;
- `authorization_response_iss_parameter_supported: true` (RFC 9207);
- `resource_indicators_supported: true`;
- `grant_types_supported`: `authorization_code`, `refresh_token`,
  `urn:ietf:params:oauth:grant-type:device_code`, `client_credentials`,
  `urn:ietf:params:oauth:grant-type:jwt-bearer`, and the AWS grant
  `urn:query-farm:params:oauth:grant-type:aws-sts` (§10c);
- `token_endpoint_auth_methods_supported`: `none` (public clients),
  `private_key_jwt` (Workloads), with
  `token_endpoint_auth_signing_alg_values_supported: ["ES256"]`.

The last field is the extension's switch for sending `resource`. **Delegated
mode** is on when the resource metadata's authorization server advertises
`resource_indicators_supported: true` and the metadata `resource` equals
`LOCATION`. Every other server (Entra included) keeps today's behaviour: no
`resource` (`vgi_oauth.cpp:1685`) and no metadata `resource` check.

In delegated mode:
- **Built-in client ids.** The extension uses `vgi-duckdb` (native) or
  `vgi-duckdb-web` (WASM). Today it only takes `client_id` from the resource
  metadata or the challenge and throws without one (`vgi_oauth.cpp:964,
  1213-1215, 1529-1531, 1596-1598, 2234-2237`). Delegated mode must not.
- **No `scope` parameter.** Today the extension falls back to `scope=openid`
  on authorize, device code and refresh (`BuildScopeString`,
  `vgi_oauth.cpp:490-500`). Delegated mode sends none. The Front Desk ignores
  `scope` if a client sends it anyway, and the session key no longer includes
  it.

### 4. Clients and redirects

| Client | `client_id` | Redirect | Consent |
| --- | --- | --- | --- |
| Native, interactive | `vgi-duckdb` | `http://127.0.0.1:<any port>/oauth-callback.html`. The extension binds and redirects to the same literal address, **not** `localhost` (fixes the `[::1]` co-bind). | Always shown (public client). |
| Native, headless (SSH, Docker, CI: the extension's existing auto-detection, `vgi_oauth.cpp:1262-1266`) | `vgi-duckdb` | Device code, RFC 8628 | On the approval page. |
| WASM | `vgi-duckdb-web` | `<page origin>/oauth-callback.html`, any `https` origin (or `http://localhost` for development) | Always shown, naming the requesting origin. |

**Consent page:**
- **Shows:** the workspace, the deployment, the client, and where the request
  came from: for WASM the **requesting origin**, prominently (e.g.
  "**app.example** wants to use *Acme · Vector tiles* as you"); for loopback
  the port.
- **Requires** an explicit click.
- **Never remembered** for public clients. Every new sign-in prompts. A
  native sign-in lasts up to 30 days through refresh; a WASM sign-in up to 12
  hours.
- **Can't be framed.** The consent and device pages send
  `Content-Security-Policy: frame-ancestors 'none'`, so a site can't overlay
  them to trick the click.
- **The origin is displayed safely:** in punycode with its port, the
  registrable domain emphasised, HTML-escaped (as is `resource_name`).

**Device code:**
- **Bound to the request:** the device authorization request carries
  `resource`, and the device code is bound to it.
- **Approval page:** at `https://auth.queryfarm.cloud/device`. It shows the same consent
  details, prominently names the deployment and workspace, and warns "only
  approve if you started this sign-in".
- **User code:** 8 characters from an unambiguous alphabet.
- **Timing:** expires after 10 minutes; polling interval 5 s with
  `slow_down`.
- **Rate limits** per IP and per user code.

**WASM origins:**
- **No registration.** Any web app embedding duckdb-wasm can sign users in.
- **Redirect validation.** The `redirect_uri` is parsed with the WHATWG URL
  parser, and its serialized form must equal `url.origin +
  "/oauth-callback.html"`. That rejects userinfo (`https://app.example@evil.com`),
  queries, fragments, dot segments and backslashes. The scheme is `https`, or
  `http` only when the host is exactly `localhost`. A `redirect_uri` that
  fails to parse is an error page, never a redirect.
- **The code is only ever delivered to that origin**, and the consent page
  names it, so a user always sees which site is asking.
- **State and `iss`.** The duckdb-wasm callback stub must return `state` and
  `iss` (RFC 9207) as well as the code, and the extension checks both. Today
  the WASM path generates `state` but only gets the code back
  (`vgi_oauth.cpp:1539-1580`).
- **Accepted risk: consent phishing.** A malicious site can ask a user to
  approve access, and gets that user's access to the deployment if they click
  Approve. This is easier to pull off than device-code phishing: one click if
  the user is already logged in, no code to type, and the user really did
  start the flow. Limits on the damage:
  - **12 hours.** A WASM sign-in's Renewal Token family expires 12 hours after
    sign-in, regardless of use. Tokens are memory-only in the browser anyway,
    so honest users rarely notice.
  - **First-use email.** The first sign-in from a new origin emails the user
    ("You signed in to *Acme · Vector tiles* from app.example").
  - **Revocable.** The Console lists the user's sessions with their origins,
    and each can be revoked. Sign-out everywhere ends all of them.
  - **No admin access.** WASM sign-ins can never reach the Admin API (see its section).
- **CORS is open** (`Access-Control-Allow-Origin: *`) on `/token`, on Desk
  Addresses and on Field Workers. Requests carry the Pass explicitly as a
  bearer token, and the browser never sends it automatically. A code is
  useless without its PKCE verifier. So origin checks would add nothing.
- WASM keeps the Pass and Renewal Token in memory only, so they're lost when
  the page closes. A persistent browser store can come later.

### 5. Token response

```json
{
  "access_token":  "<Pass>",
  "token_type":    "Bearer",
  "expires_in":    900,
  "refresh_token": "ort1.…",
  "vgi_location":  "https://k7f2q9xw.field.queryfarm.cloud"
}
```

`vgi_location` must:
- be `https://` with no path, query or userinfo;
- match the **allowed delegation hosts for this issuer** (§6);
- not be an IP literal (and so never private, loopback or link-local).

It is re-checked on every refresh. It also goes through the existing
`CheckLocationPolicy`.

### 6. Allowed delegation hosts (extension setting)

`vgi_allowed_delegation_hosts` maps an issuer to host patterns:

```
https://auth.queryfarm.cloud => *.field.queryfarm.cloud
```

- **This is the built-in default,** and the only entry. For any other issuer
  the default is empty, so delegation is refused.
- **Narrow-only:** once set, a new value must be a literal subset of the
  current entries (same issuers, pattern lists that are subsets). It is
  compared as text, not by evaluating the globs.
- **Test escape hatch:** loopback `http` is allowed only when the extension
  runs with `VGI_TEST_ALLOW_LOOPBACK_DELEGATION=1` in the environment. No SQL
  setting can enable it.
- **It also gates Workload credentials (§10d).** The issuer must be an entry
  here **before** the extension sends any Workload credential, and the token
  endpoint must be on that issuer's origin.

### 7. Refresh

- `grant_type=refresh_token`, with `resource` required and equal to the
  family's Desk Address. A missing or different `resource` → `invalid_target`.
- **Rotation is atomic.** It is one D1 statement:
  `UPDATE … SET state='rotated', successor=? WHERE hash=? AND state='active'`.
  The Registry has no read replicas.
- **Grace window:** if the same old token is presented again within 30 s of
  rotating, the Front Desk returns the **same** successor (so a lost response
  or a 5xx retry doesn't sign the user out). After 30 s, presenting a rotated
  token kills the whole family (theft detection).
- **The Registry is re-checked on every refresh:** membership (by `sub`),
  deployment status, and the user's sign-out time compared with the
  **family's original sign-in time**, not the token's issue time. A
  stopped deployment answers `invalid_grant`, "deployment is not running".

### 8. Errors seen by the Pass Holder

| Situation | Native | WASM |
| --- | --- | --- |
| Refresh `invalid_grant` | Fail the query with "sign in again: `CALL vgi_oauth_login('<alias>')`" or re-ATTACH. **Never** opens a browser after a failed refresh, keeping today's rule that batch jobs never hang (`vgi_oauth.cpp:2003-2016`). | Starts the browser flow (today's WASM behaviour). |
| Refresh 5xx / `temporarily_unavailable` | Backoff (1 s, 2 s, 4 s), honouring `Retry-After`; then fail. | Same. |
| `invalid_target` | Fail: not a Desk Address. | Same. |
| A 401 from the Field Worker | Refresh once (single flight, §9), then retry the request. A second 401 fails. **Any challenge on the Field Worker's 401 is ignored**: no discovery, no new authorization server. | Same. |

Workload sign-in (§10) never opens a browser or starts device code. A refused
assertion or platform token (`invalid_client`, `invalid_grant`) fails the
query with the Front Desk's message. Today a 401 with no Renewal Token falls
into full sign-in (`RefreshOrFullAuth`), which natively can open a browser or
start device code. `CI=true`, Kubernetes and Docker all count as headless
(`vgi_oauth.cpp:879+, 1257-1266`). So Workload state takes an explicit branch
ahead of that choice.

### 9. The Pass Holder (extension design)

File references are to the current extension code.

- **State, not a new class.** Delegation is state inside the existing
  `OAuthCatalogAuth`/`AuthState`, beside `OAuthTokenSet`: the Field Address,
  the issuer, the token endpoint fixed at sign-in, the Renewal Token. The auth
  object is created at ATTACH and shared by attach params, remote secret
  storage, connections and cancel (`vgi_extension.cpp:2003-2021, 2464`), so it
  can't be swapped. `AuthState` is shared across catalogs through the session
  registry (`vgi_oauth.cpp:1813-1848`), so routing lives there too.
- **Effective base URL.** A new abstraction replaces building URLs from
  `worker_path` at the 19 call sites (`vgi_http_client.cpp`,
  `vgi_http_function_connection.cpp` `base_url_` at `:164`,
  `vgi_unary_rpc.cpp`):
  - The base and the path are separate, and the base is resolved per
    connection from the catalog's auth state.
  - The retry after authentication **resolves the base again**. Today it reuses
    `url` (`vgi_http_client.cpp:752`), which would send the first
    post-sign-in request back to the Desk Address.
  - When the base host changes:
    - the reused `HTTPClient` (`client_holder`, `:415`) is reset;
    - the cookie jar is cleared (it ignores Domain, `vgi_cookie_jar.cpp:130`);
    - `server_caps_` is re-probed (`vgi_attach_parameters.hpp:197-249`).
  - A connection keeps its Field Address for its lifetime, so streams stay on
    one worker. A Field Address that changes on refresh applies to new
    connections.
- **Proactive single-flight refresh.**
  - `GetToken` gains a `ClientContext&` overload. Its hot caller is
    `vgi_http_client.cpp:693`; the other caller is the session status row at
    `vgi_extension.cpp:2899`.
  - With under 5 minutes left, one thread refreshes. The others keep using the
    still-valid Pass and don't wait.
  - At expiry (today's 45 s margin), callers wait on the single refresh.
- **Stale-401 dedupe.** `HandleUnauthorized` takes the token the request was
  rejected with. If the current Pass differs, it returns the current one
  without refreshing. Today N parallel 401s cause N refreshes
  (`vgi_oauth.cpp:2140-2150`), which would trip reuse detection.
- **A 401 without a challenge.** In delegated mode, a successful refresh
  followed by one retry is the whole handling. Today's "no challenge" branch
  throws even when refresh worked (`vgi_http_client.cpp:740-743`); delegated
  mode fixes that.
- **Result-cache scope.**
  - Today the scope is the credential fingerprint, which empties on every
    refresh and deliberately ignores id_token claims
    (`ComputeCatalogIdentityFingerprint`, `:2321-2322`).
  - Delegated catalogs are scoped by `(iss, canonical Desk Address,
    sub)`, read from the Pass. The Desk Address also stands in as
    `worker_path` in cache keys (`vgi_exchange_cache_key.cpp:288,306`).
  - **Why `sub` is acceptable here, unverified:** the Pass came straight from
    an issuer the extension checked (RFC 9207 `iss`, allowed delegation
    hosts), over TLS, as the response to its own PKCE or refresh exchange. The
    user can only mislead their own local cache.
  - The existing fail-closed rule stays: no currently valid Pass → `""` → no
    cache use.
  - Accepted: a removed member's own machine keeps results it already
    fetched.
- **Remote secret storage.**
  - `VgiRemoteSecretStorage` reuses the catalog auth (`:2432-2464`).
  - In delegated mode the Pass is sent only to the current Field Address's
    origin.
  - A remote secret service on any other origin is not called; the error
    names the restriction.
- **httpfs parameters.** These are resolved once, from the Desk Address URL,
  at ATTACH (`vgi_catalog_api.cpp:157-170`). Field Workers inherit them
  (proxy, CA bundle). Documented, not changed (duckdb#22258).
- **Field Addresses aren't secret.** They're on our domain and carry random
  labels, but security doesn't depend on hiding them, so they may appear in
  errors and `duckdb_logs`. The Pass is redacted like `bearer_token`.
- **Credential storage:**
  - macOS uses the Keychain; Windows uses DPAPI (`vgi_oauth_store.cpp:83,239`).
  - **Linux gains persistence** through the Secret Service (libsecret) when
    available. Otherwise it stays memory-only, with a clear message.
  - The lock directory `/tmp/queryfarm-vgi-oauth-<uid>` gets an
    owner-and-mode check. Today it accepts `EEXIST` blindly.
- **Identity display.** Delegated mode requests no `openid` scope, so there
  is no `id_token`. `vgi_oauth_identity()` shows the Pass's claims (including
  `kind`), labelled as unverified and for display only.

### 10. Workloads

A Workload is created by a workspace admin in the Console (Admin API) with a
name chosen by the admin ("nightly ETL", "billing export job"). The Registry
gives it an **opaque, stable id**. Nothing in the id says what kind of thing
it is. A Workload belongs to one workspace and, like a member, may use that
workspace's deployments. Optionally the admin limits it to a list of
deployments.

It authenticates in one of three ways (§10a–c). All happen at `/token`, with
`resource=<Desk Address>`, and both return **a Pass only**: no Renewal Token.
A Workload simply asks again before its Pass expires, so there is no token
family, no rotation and no reuse detection to manage.

#### 10a. Workload Key (`client_credentials` + `private_key_jwt`)

1. The admin registers one or more **public** keys for the Workload: a P-256
   JWK, or a PEM the Console converts. Each key gets a `kid`. Several keys can
   be active at once, which is how rotation works: add the new key, switch the
   job over, delete the old one.
2. The job signs a client assertion (RFC 7523 §2.2) with its private key and
   posts it:

   ```
   POST https://auth.queryfarm.cloud/token
   grant_type=client_credentials
   &client_id=<workload id>
   &client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer
   &client_assertion=<JWT: alg ES256, kid; iss = sub = <workload id>,
                      aud = <issuer> or <issuer>/token, iat, exp ≤ iat + 300 s, jti>
   &resource=https://desk.queryfarm.cloud/d/dep_123
   ```

3. The Front Desk checks:
   - the signature against the Workload's registered keys;
   - `iss` = `sub` = `client_id`;
   - `aud` is its own issuer or its token endpoint (both accepted, following
     the RFC 7523bis direction);
   - lifetime ≤ 5 minutes, with 60 s skew, and `iat` not in the future;
   - `(client_id, jti)` not seen before. It is recorded with a D1 unique
     insert (strongly consistent, never KV) until the assertion's `exp`, so an
     assertion can't be replayed. A retry after an error always uses a fresh
     assertion and `jti`;
   - the Workload exists, isn't disabled, and may use that deployment.

The private key never leaves the job's environment, and the Front Desk stores
nothing that could be used to impersonate it.

#### 10b. Trust Rule (JWT-bearer grant, workload identity federation)

For platforms that give each job its own short-lived OIDC identity token:

1. The admin adds a Trust Rule to a Workload with three parts:
   - **issuer**, chosen from the issuers the Front Desk supports;
   - **subject**, matched exactly (e.g.
     `repo:acme/etl:ref:refs/heads/main` for GitHub Actions);
   - **extra claim conditions**, required for some issuers (below).
2. The job fetches its platform token **with the audience set to the
   checked issuer** (`https://auth.queryfarm.cloud`) and posts it:

   ```
   POST https://auth.queryfarm.cloud/token
   grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
   &assertion=<platform OIDC token>
   &resource=https://desk.queryfarm.cloud/d/dep_123
   ```

3. The Front Desk:
   - verifies the token against that issuer's published keys;
   - requires `aud` = its issuer (Azure: see below), so a token minted for
     another service can't be replayed here;
   - checks `exp`;
   - matches exactly one Trust Rule **among the rules of the workspace that
     owns `resource`**. Rules in other workspaces are never considered, so
     another workspace can't make your rule ambiguous by copying your
     subject;
   - issues a Pass for that rule's Workload.

**Supported issuers.** The Front Desk keeps a built-in list, each with a fixed
issuer URL and the subject format documented for admins:

| Issuer | Token source |
| --- | --- |
| GitHub Actions | `https://token.actions.githubusercontent.com` |
| Google Cloud | `https://accounts.google.com` (service-account identity tokens) |
| Fly.io Machines | `https://oidc.fly.io/<org>` |
| Kubernetes | A cluster's service-account issuer, registered per workspace (below) |
| Azure (Entra ID managed identities) | `https://login.microsoftonline.com/<tenant>/v2.0`, for the customer's tenant |

**Per-issuer matching rules:**
- **GitHub Actions.** The rule records `repository_id` and
  `repository_owner_id` (the Console resolves them when the rule is created)
  and requires both, so a renamed or deleted repo or org can't be
  re-registered by someone else.
  - Branch subjects (`ref:refs/heads/main`) are also issued to
    `pull_request_target` runs, which can run fork code. So rules exclude
    `event_name: pull_request_target` by default and can add conditions on
    `event_name` and `job_workflow_ref`.
  - `pull_request` subjects are refused.
  - Environment subjects are allowed. The Console notes they're only as
    strong as the environment's protection rules.
- **Google Cloud.** Match the service account's numeric `sub`, never its
  email: emails can be reused after an account is deleted.
- **Fly.io.** The issuer is per Fly org, and machine ids change on every
  deploy. Rules match app-level claims (org and app name), not the machine
  `sub`.
- **Kubernetes.** A workspace admin registers the cluster's issuer:
  - **By URL:** `https` on a public host, with size and time limits on the
    discovery and JWKS fetches, and `jwks_uri` on the issuer's own origin.
  - **Or by uploading** a static JWKS, for clusters whose issuer isn't publicly
    reachable.

  The issuer is scoped to the workspace that registered it.
- **Azure (Entra ID managed identities).** The customer registers a small
  **audience app in their own tenant**. Entra only issues tokens for an
  audience that has a service principal in the caller's tenant
  (`AADSTS500011`), and since March 2026 it refuses third-party multi-tenant
  app-only tokens without one. So a Query.Farm-owned multi-tenant app would
  need every customer's admin to consent first. The customer runs, once per
  tenant:

  ```bash
  APP_ID=$(az ad app create --display-name orchard-federation --query appId -o tsv)
  az ad app update --id "$APP_ID" --identifier-uris "api://$APP_ID"
  az rest --method PATCH \
    --uri "https://graph.microsoft.com/v1.0/applications/$(az ad app show --id "$APP_ID" --query id -o tsv)" \
    --body '{"api":{"requestedAccessTokenVersion":2}}'
  az ad sp create --id "$APP_ID"
  ```

  (Azure CLI 2.90 rejects the shorter `az ad app update … --set
  api.requestedAccessTokenVersion=2`, so the version is set through Microsoft
  Graph.)

  **Verified in the Query.Farm tenant (2026-11-17)** with an app-only service
  principal token for such an app: `ver 2.0`, `aud` = the app's client-id
  GUID, `iss = https://login.microsoftonline.com/<tid>/v2.0`, `tid` correct,
  `oid` = the calling service principal. No `idtyp`, `scp` or `roles` were
  present, so `oid` matching is the check.

  Their managed identities then request tokens for `api://<APP_ID>`.
  - **The token:** v2 (the resource app decides the version, so v1 is never
    issued). `iss = https://login.microsoftonline.com/<tid>/v2.0`, `aud =
    <APP_ID>` (the GUID), `oid`/`sub` = the managed identity, `ver = 2.0`.
  - **The Trust Rule** records the tenant (`tid`), the audience `APP_ID`, and
    the managed identity's object id (`oid`), all matched exactly. The Front
    Desk fetches keys for that tenant and requires `iss` to match `tid` and
    `ver = 2.0`.
  - **User tokens can't pass,** because a user's `oid` never equals the
    managed identity's. No `idtyp` check is needed.
  - `xms_mirid`, the Azure resource id, is shown in the Console for the
    admin's reference.
  - **The audience isn't ours,** so this token isn't bound to the Front Desk the
    way other platforms' are. A token for that app is only meaningful to
    Orchard because the customer created the app for Orchard alone. The
    Console says so. The extension's issuer pinning (§10d) still ensures it is
    only ever sent to the real Front Desk.

**Rules for the Console:** no wildcard subjects in the first version.

#### 10c. AWS (presigned `sts:GetCallerIdentity`)

AWS has no general way for an IAM role to get an OIDC token for a third party,
so AWS uses the established pattern instead: the job proves who it is by
handing the Front Desk a request to AWS STS that **it signed but did not
send**. The Front Desk sends it, and AWS answers with the caller's identity.

1. **The Trust Rule** names an AWS account id and an IAM role name, matched
   exactly (e.g. `123456789012` / `orchard-etl`).
   - **Role id binding.** On the first successful sign-in, the rule records
     the role's unique id (`AROA…`, the first part of GetCallerIdentity's
     `UserId`) and requires it from then on. A deleted and re-created role
     with the same name doesn't inherit the trust; the Console shows the bound
     id and lets an admin re-bind deliberately.
   - **No session-name rules.** The role session name is chosen by whoever
     assumes the role, so it's not a security boundary and rules can't use
     it.
2. **The job builds and SigV4-signs** this request with its normal AWS
   credentials, without sending it:
   - `POST https://sts.amazonaws.com/`, or a regional STS endpoint;
   - body `Action=GetCallerIdentity&Version=2011-06-15`;
   - signed headers including `host`, `x-amz-date` (plus
     `x-amz-security-token` for temporary credentials) and
     **`x-orchard-server-id: <checked issuer>`** (`https://auth.queryfarm.cloud`).

   It posts the signed parts to `/token`:

   ```
   POST https://auth.queryfarm.cloud/token
   grant_type=urn:query-farm:params:oauth:grant-type:aws-sts
   &sts_url=<endpoint>&sts_headers=<base64 JSON of the signed headers>
   &sts_body=<base64 body>&resource=https://desk.queryfarm.cloud/d/dep_123
   ```

3. **The Front Desk:**
   - accepts only an STS endpoint from a fixed list of `sts[.<region>].amazonaws.com`
     hosts (no other URL is ever fetched);
   - requires the exact body above;
   - requires `x-orchard-server-id` to be present, equal to its own issuer and
     covered by the signature, so a request signed for another service can't
     be used here, and one signed for us can't be used elsewhere;
   - requires `x-amz-date` within 5 minutes of now;
   - keeps a replay cache of signature values for that window;
   - **builds the outgoing request from scratch** (this is the pattern behind
     Vault's AWS-auth bypass, CVE-2020-16250/16251):
     - a fixed method, the allowlisted URL and the fixed body;
     - only the headers named in the `SignedHeaders` list are copied, and
       duplicates are refused;
     - redirects are never followed;
   - sends it to STS. AWS verifies the signature and returns the caller's
     identity, which must be a 200 with a strictly parsed XML body.
4. **Matching:** an assumed-role ARN
   (`arn:aws:sts::<account>:assumed-role/<role>/<session>`) is matched by
   account, role name and bound role id, among the rules of the workspace that
   owns `resource`. IAM user ARNs are refused; roles only.
   `federated_iss` = `https://sts.amazonaws.com`, and `federated_sub` = the
   ARN.

The Front Desk calls STS once per Pass, at most every 10 minutes per job. A
replayed signed request is only good for its 5-minute window and only at our
Front Desk, and it gives the same identity again.

If AWS's newer outbound identity tokens prove a good fit, they would use the
ordinary Trust Rule path (§10b) instead. That's a later evaluation, not a
launch requirement.

#### 10d. Workloads in the extension

No DuckDB secret type. A Workload is configured through the environment the
job already runs in, or through matching ATTACH options:

| Environment | ATTACH option | Meaning |
| --- | --- | --- |
| `VGI_WORKLOAD_ID` | `workload_id` | The Workload's id. Required with a key; an error with a federation source (federation identifies the Workload through its Trust Rule). |
| `VGI_WORKLOAD_KEY_FILE` | `workload_key_file` | Path to the private key: P-256, PEM, as PKCS#8 (`BEGIN PRIVATE KEY`) or SEC1 (`BEGIN EC PRIVATE KEY`, what `openssl ecparam -genkey` writes). The path, never the key, appears in SQL. |
| `VGI_WORKLOAD_IDENTITY` | `workload_identity` | Federation source instead of a key: `github-actions`, `gcp`, `fly`, `azure`, `aws`, or `file:<path>` (a token file, e.g. a Kubernetes projected service-account token). `none` turns Workload sign-in off for that ATTACH. |
| `VGI_WORKLOAD_AZURE_AUDIENCE` | `workload_azure_audience` | Azure only: the customer's audience app, `api://<APP_ID>` (§10b). |

When delegated mode starts (§3) and a Workload is configured, the Pass Holder
uses §10a, §10b or §10c instead of the browser or device code.

**Issuer pinning.** Before sending **any** Workload credential (an assertion, a
platform token or a signed STS request), the extension requires two things:
- the authorization server's issuer is an entry in
  `vgi_allowed_delegation_hosts` (§6);
- its token endpoint is on that issuer's origin.

Every audience it uses comes from that checked issuer, never from a constant:
- the assertion `aud`;
- the audience it asks the platform for (except Azure, whose audience is the
  customer's own app, §10b);
- `x-orchard-server-id`.

Otherwise a hostile `LOCATION` could point at a fake Front Desk and collect a
platform token, or a signed STS request, addressed to the real one, then
replay it.

**Precedence:**
- An ATTACH option beats the environment.
- A configured Workload beats a saved user sign-in. The environment is how
  jobs say what they are.
- `workload_identity := 'none'` (or `VGI_WORKLOAD_IDENTITY=none`) opts out,
  e.g. for a developer whose shell happens to export Workload variables.
- `vgi_oauth_identity()` always shows whether a catalog is signed in as a user
  or a Workload.

**Using these from SQL:**
- `workload_key_file` and `file:<path>` are read through DuckDB's
  `FileSystem`, so `enable_external_access` and `allowed_directories` apply.
- `workload_identity := 'aws'` or `'azure'` given in SQL is refused when
  external access is disabled. Otherwise any SQL author could borrow the
  host's cloud identity.
- `AWS_CONTAINER_CREDENTIALS_FULL_URI` is only honoured for loopback and the
  ECS/EKS link-local addresses, as in the AWS SDKs.

**Key-file permissions:**
- **Refused:** a file owned by another non-root user, or writable by group or
  others. Someone else could substitute the key.
- **Warned, not refused:** a file readable by others. Kubernetes Secret
  mounts default to `0644` and Docker secrets are root-owned `0444`, and both
  are normal in single-purpose containers. On a shared host the warning tells
  the user to `chmod 600`.
- **Windows** has no mode bits, so there's no check there.

**The options exist on ATTACH and on `vgi_catalogs()`** (`vgi_catalogs.cpp:219-289`).
- They join the conflict checks: a Workload against `bearer_token` or
  `oauth_refresh_token` (`vgi_extension.cpp:1755`), and `workload_key_file`
  against `workload_identity`.
- **Changelog note:** the extension now claims the `workload_*` option names.
  Today unknown options are forwarded to the worker
  (`vgi_extension.cpp:1665-1670`), so a worker that already declared such a
  name loses it.

**Sessions:**
- The Workload identity (the id, or the federation source) is part of the
  session key, so two catalogs with different Workloads never share an
  `AuthState`.
- Workload Passes are never persisted.

**How each federation source gets its token:**
- `github-actions`: from `ACTIONS_ID_TOKEN_REQUEST_URL` with
  `ACTIONS_ID_TOKEN_REQUEST_TOKEN`, `audience=<checked issuer>`.
- `gcp`: from the metadata server's identity endpoint, with the same audience.
- `fly`: from the Machine's local API, `/v1/tokens/oidc`, with the same
  audience.
- `file:<path>`: read fresh before each request.
- `azure`: a token for the customer's audience app, named by
  `VGI_WORKLOAD_AZURE_AUDIENCE` (or the `workload_azure_audience` option), e.g.
  `api://<APP_ID>`, from whichever managed identity source is present (IMDS
  and App Service take `resource=api://<APP_ID>`; the AKS exchange at Entra v2
  takes `scope=api://<APP_ID>/.default`):
  - **App Service, Functions, Container Apps:** `IDENTITY_ENDPOINT` with
    `IDENTITY_HEADER`.
  - **AKS workload identity:** `AZURE_FEDERATED_TOKEN_FILE` with
    `AZURE_CLIENT_ID` and `AZURE_TENANT_ID`, exchanged at Entra's token
    endpoint.
  - **VMs and VM scale sets:** the instance metadata service
    (`169.254.169.254`, header `Metadata: true`).
  - `AZURE_CLIENT_ID` picks a user-assigned identity.
- `aws`: SigV4-signs the request in §10c with the standard credential sources,
  tried in this order:
  1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`;
  2. `AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN` (EKS IRSA), via STS
     `AssumeRoleWithWebIdentity`;
  3. ECS and EKS Pod Identity container credentials
     (`AWS_CONTAINER_CREDENTIALS_FULL_URI` / `…_RELATIVE_URI`);
  4. EC2 instance metadata (IMDSv2).

  **Region:** `AWS_REGION`, else `AWS_DEFAULT_REGION`, picks a regional STS
  endpoint. The global endpoint signs for `us-east-1`.

  **Other details:**
  - EKS Pod Identity also needs `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`.
  - `AssumeRoleWithWebIdentity` is requested with `Accept: application/json`,
    because there's no XML parser in the extension.
  - Shared-config profiles and SSO aren't supported at launch.
  - There's no AWS SDK dependency.

Renewal reuses the same single-flight, before-expiry logic as for users, but
by repeating the grant instead of refreshing.

**Crypto:** OpenSSL, which the extension already links natively
(`CMakeLists.txt:20-25, 449-451`; `vgi_oauth.cpp` already uses EVP and RAND).
DuckDB's bundled mbedtls is trimmed: no ECP or ECDSA, and DuckDB's patches
forbid enabling them.
- **ES256 assertions:** `PEM_read_bio_PrivateKey`, a P-256 check, then
  `EVP_DigestSign`.
- **The DER-to-raw signature conversion:** OpenSSL returns DER signatures,
  and JWS needs the raw 64-byte R‖S (RFC 7518 §3.4). Converting with
  `d2i_ECDSA_SIG` and `BN_bn2binpad` is a classic bug site, so it gets unit
  test vectors.
- **SigV4:** HMAC-SHA256 through OpenSSL `HMAC` (or DuckDB's
  `MbedTlsWrapper::Hmac256`), tested against AWS's published SigV4 vectors.
- `vgi_unit_tests` adds `OpenSSL::Crypto` to its link.

WASM has no OpenSSL and doesn't support Workloads.

**Implementation notes:**
- **Metadata requests:** the credential-source fetchers need custom headers
  and methods (`Metadata: true`, `Metadata-Flavor: Google`,
  `X-IDENTITY-HEADER`, and a PUT for the IMDSv2 token); today `HttpGet` takes
  neither (`vgi_oauth.cpp:445-470`). They bypass `http_proxy`, since
  link-local endpoints can't be proxied, and they're exempt from
  `CheckLocationPolicy`.
- **Fly:** its token API is a Unix socket (`/.fly/api`), which needs a small
  HTTP/1.1-over-Unix-socket client.
- **Transactions:** fetches and renewals use `OAuthHttpParams`, which already
  switches to secret-free parameters inside a transaction start
  (duckdb#22258).
- **Where the code lives:** the dependency-free pieces (JWS assembly, the
  SigV4 canonical request, choosing the credential source) go in a new
  `src/vgi_workload.cpp`, so `vgi_unit_tests` can cover them.
- **Tests:** the Python harness fakes each metadata service through its
  standard override:
  - `ACTIONS_ID_TOKEN_REQUEST_URL` (GitHub);
  - `GCE_METADATA_HOST` (Google Cloud);
  - `AWS_EC2_METADATA_SERVICE_ENDPOINT`, `AWS_ENDPOINT_URL_STS` and a loopback
    `AWS_CONTAINER_CREDENTIALS_FULL_URI` (AWS);
  - `IDENTITY_ENDPOINT` and `AZURE_AUTHORITY_HOST` (Azure).

  Test-only overrides cover the rest (Azure VM IMDS, the Fly socket), gated
  like `VGI_TEST_ALLOW_LOOPBACK_DELEGATION`.

**Revocation:**
- Disabling or deleting a Workload, Workload Key or Trust Rule stops new
  Passes at once.
- Outstanding Passes expire within 15 minutes.

## The Pass

RFC 9068 JWT access token: header `{"alg":"ES256","typ":"at+jwt","kid":…}`.

| Claim | Example | Meaning |
| --- | --- | --- |
| `iss` | `https://auth.queryfarm.cloud` | The Front Desk. |
| `aud` | `https://desk.queryfarm.cloud/d/dep_123` | Exactly one canonical Desk Address. |
| `sub` | `user_01M7KNJYC18T…` | Users: the WorkOS user id. Workloads: the Workload's opaque Registry id. Becomes `AuthContext.principal`. |
| `kind` | `user` / `workload` | Whether a person or a Workload is calling. |
| `email` / `email_verified` | `ada@example.com` / `true` | Users only. A user's Pass is only issued when `email_verified` is true. |
| `name` | `Ada Lovelace` / `nightly ETL` | The user's name, or the name the admin gave the Workload. |
| `federated_iss` / `federated_sub` | `https://token.actions.githubusercontent.com` / `repo:acme/etl:ref:refs/heads/main` | Workloads signed in through a Trust Rule only: the platform identity that was exchanged, for the worker's own logs. |
| `org_id` / `org_name` | `org_01M7…` / `Acme Analytics` | The workspace that owns the deployment. |
| `client_id` | `vgi-duckdb` / the Workload id | |
| `iat` / `nbf` / `exp` / `jti` | | 900 s lifetime. |

Identity is fixed for the life of a Pass. Claims are passed to worker code
**with their JSON types** (booleans stay booleans; this needs a Rust fix).

Worker code that authorizes by `sub` or `name` must also check `kind`. A
Workload's id is opaque and its name is chosen by an admin, so either could
look like a user's.

A valid Pass means "a member, or a Workload, of the workspace that owns this
deployment", and
that is the only authorization the platform does. Anything finer (read-only
users, per-function rules) is the worker's own decision, made from the
identity claims above.

## The Field Worker: SDK hosted mode

`VGI_ORCHARD_HOSTED=1` is baked into the hosted base image, not set per deployment.
In hosted mode, in all **seven** SDKs (Python, Go, Rust, TypeScript, Java,
C#, C++):

1. **Fail closed at startup** unless `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE`
   (exactly one canonical value, no comma splitting) and `VGI_JWT_JWKS_URI`
   are all set. An unset variable is an error, the same as an empty one.
2. **Every request without a valid Pass gets 401**, `WWW-Authenticate: Bearer
   error="invalid_token"`, with VGI capability headers, and no
   `resource_metadata`. `HEAD /health` is the only exception. This fixes
   Rust's anonymous path for a missing `Authorization` (`jwt.rs:168`).
3. **No other credentials:** `VGI_BEARER_TOKENS`, grant keys
   (`VGI_RPC_GRANT_KEYS`, `issue_grant`) and `resolve_token` refuse to start.
4. **Verification:**
   - algorithm `ES256` only (`VGI_JWT_ALGORITHMS`, default `ES256` in hosted
     mode);
   - `typ` must be `at+jwt`, which needs a Java type verifier (Nimbus rejects
     it by default);
   - `iss` and `aud` must match exactly;
   - `exp` required (Python doesn't require it today) and checked, as is
     `nbf`, with 60 s skew.
5. **Key Set rules (exact):**
   - Cache for at most 10 minutes; honour a shorter `Cache-Control: max-age`.
   - On an unknown `kid`, re-fetch at most once per 60 s per issuer; otherwise
     return 401 straight away.
   - **Fail closed:** the cached Key Set expires hard at 10 minutes, even if
     re-fetching fails. Stale keys are never served. That makes the 10-minute
     cap a real revocation guarantee: a key removed from the Key Set stops
     being trusted everywhere within 10 minutes.
   - Today's behaviour, to be fixed:
     - Python caches forever and re-fetches on every unknown `kid`
       (`_oauth_jwt.py:102-114,173-175`).
     - Rust has no expiry (`jwt.rs:53`) and returns anonymous on a missing
       header (`jwt.rs:168`).
     - Go uses `keyfunc` defaults: a 1 h refresh and 5 min unknown-`kid` limit
       (`jwtauth.go:58-66`).
     - Java uses Nimbus refresh-ahead caching (`JwtAuthenticator.java:67-71`,
       300 s), whose behaviour when fetches fail must be checked.
     - TypeScript relies on oauth4webapi's internal cache.
     - Go and Java also accept RS* and PS* algorithms.
6. **Authenticate before reading the body.**
7. **Never log `Authorization`**, including in `--access-log-debug`.
8. **CORS** open (`*`); authentication is by bearer Pass only.

Each item gets a conformance test that runs against every SDK. The Front Desk
also probes a new deployment's Field Address unauthenticated and requires a
401 (and a 200 `HEAD /health`) before it will route to it.

## The Front Desk

- **Isolation.**
  - A separate Cloudflare account, holding the `queryfarm.cloud` zone, the Front Desk
    Worker, the Registry D1 and its secrets.
  - query.farm's account, its people and its CI have no access.
  - Deploys come from its own repo (`vgi-orchard-auth`) and a protected
    branch, with an API token for that account only.
  - Secrets are set by hand.
- **Registry ownership.** Only the Front Desk writes it, through the Admin
  API (below). Every write is recorded in an append-only audit table.
- **Field Addresses.**
  - Created by the Front Desk: a random 8-character label, the DNS CNAME
    `<label>.field.queryfarm.cloud` → `<app>.fly.dev`, and `fly certs add`.
  - Labels and Fly app names are never reused.
  - On delete, the DNS record goes first and the Fly app second.
- **Membership.**
  - Bound to the WorkOS `sub` when an invite is accepted (the invite's email
    must match a verified AuthKit email).
  - After that, `sub` is the key and email is display only.
- **Sign-out everywhere** (self, or by a workspace admin for a member):
  - sets `signed_out_at`;
  - revokes the user's WorkOS sessions;
  - clears the Front Desk's own browser session.

  Refresh compares it with each family's original sign-in time. There is no
  remembered consent to clear (§4).
- **Logging.** Workers Logs are off for `/token`, `/device` and the Admin API,
  or scrubbed: no codes, tokens or `Authorization`. No `wrangler tail` in
  production.
- **Rate limits.** `/authorize`, `/device`, `/token` and the Admin API, per IP
  and per user. The same `access_denied` answer for an unknown deployment and
  a non-member.
- **Signing Key.**
  - ES256, held as a secret and used through WebCrypto.
  - **Rotation:**
    1. Publish the new public key.
    2. Wait more than 10 minutes (the SDK cache maximum).
    3. Sign with the new key.
    4. Remove the old public key after 15 minutes more.
  - **Emergency (key leak):**
    1. Publish a Key Set containing only a new key, so the leaked key is
       untrusted within 10 minutes (the SDK cache cap).
    2. Kill every Renewal Token family, so everyone signs in again.

    Exposure window: at most 10 minutes; lower the cache cap to shorten it.
  - KMS signing stays optional.

## The Admin API

The Admin API changes the Registry. A Pass for it would let an attacker add
their own Workload Key or Trust Rule, or invite themselves, and that access
outlives sign-out everywhere. So it's protected separately from deployment
access:

- **Only the Console can get admin Passes.** The admin resource
  `https://auth.queryfarm.cloud/admin` is issued only to `orchard-console`, a
  confidential client with one exact redirect URI on query.farm. Its secret
  lives in the site's server-side Functions.
  - It is refused for `vgi-duckdb`, `vgi-duckdb-web`, device code and
    Workloads.
  - A phished deployment sign-in, from any origin or device code, can never
    reach the Admin API.
- **Login freshness travels with the session.**
  - At sign-in, the Front Desk verifies AuthKit's token (RS256) against the
    WorkOS JWKS and reads `auth_time`, the time of the user's last active
    login.
    - **Confirmed on the staging environment (2026-11-17):** access tokens
      carry `auth_time`, and a refresh keeps it unchanged while `iat`
      advances.
    - The issuer depends on how the token was obtained: the AuthKit domain
      for the hosted sign-in flow, `https://api.workos.com/user_management/<client
      id>` for direct API authentication. The Front Desk accepts exactly the
      issuer of the flow it uses.
  - It stores that time on the Renewal Token family. Refresh never advances
    it.
  - It copies it into admin Passes as `auth_time`.
  - If WorkOS doesn't supply `auth_time`, the Front Desk forces a fresh login
    (`max_age=0`) for admin sign-ins and records its own timestamp when it
    receives the code for that `state`.
- **Changes that create lasting access are confirmed on the Front Desk.**
  These are:
  - adding a Workload, Workload Key or Trust Rule;
  - registering a Kubernetes issuer;
  - inviting a member;
  - re-binding an AWS role id.

  Each needs two things:
  - a login within the last 10 minutes; a staler `auth_time` gets an RFC 9470
    `insufficient_user_authentication` challenge, and the Console sends the
    user back through AuthKit with `max_age`;
  - **a confirmation page on `auth.queryfarm.cloud`.** The Console creates a pending
    change and redirects the user there. The page shows exactly what will
    change ("Add Trust Rule: GitHub `acme/etl` on `main` → Workload *nightly
    ETL*"), can't be framed, and the change applies only when the user clicks
    Confirm.

  So a compromised query.farm Console, or script running in it, can't
  silently create access. The worst it can do is propose a change the user
  must read and approve on a page it doesn't control.
- **Other customer actions** (removing a member, sign-out everywhere,
  disabling or deleting a Workload, key or rule) need the workspace `admin`
  role and an admin Pass, without the confirmation page. They only reduce
  access.
- **Staff actions** (mark live, set a Field Address, create its DNS) need a
  staff account (flagged in the Registry), a login within the last 10
  minutes, and the same confirmation page.

## What changes, by repo

| Repo | Change |
| --- | --- |
| vgi (extension) | Delegated mode in `OAuthCatalogAuth`/`AuthState`; effective-base-URL abstraction (19 call sites, retry re-resolution, client, cookie and capability reset); `GetToken(ClientContext&)` with single-flight refresh; stale-401 dedupe; conditional `resource`; RFC 9207 `iss` check; canonical resource compare; `vgi_location` handling and `vgi_allowed_delegation_hosts`; ignoring challenges and overrides in delegated mode; built-in client ids and no `scope` in delegated mode; Workload sign-in (§10d: issuer pinning, OpenSSL ES256 assertions and SigV4, federation sources including Azure managed identity and the AWS credential chain, env and ATTACH and `vgi_catalogs()` options, precedence and opt-out, FileSystem-gated files, explicit non-interactive branch); WASM `state`/`iss` checks; result-cache scope; remote-secret restriction; Linux Secret Service store and lock-dir check; `127.0.0.1` redirect; WASM path (`vgi-duckdb-web`, CORS). |
| vgi-python (SDK) | Hosted mode; Key Set fixes (fail-closed 10-minute expiry, refresh limit); require `exp`. |
| SDKs (Go, Rust, TypeScript, Java, C#, C++) | Hosted mode items 1–8, each with conformance tests. Fixes: Rust (anonymous path, claim types, Key Set expiry), Java (`at+jwt`, Key Set behaviour on fetch failure), Go (env wiring, cache settings), TypeScript (Key Set cache), all (ES256 pinning, fail-closed Key Set expiry). |
| vgi-orchard-auth (new repo, separate CF account) | Front Desk: OAuth AS (authorization code + PKCE, device code, open WASM redirects with strict validation, 12-hour WASM families and first-use email, consent showing the requesting origin with `frame-ancestors 'none'`, RFC 9207), Desk Address health and challenge, Key Set, Registry, Workloads (`client_credentials` + `private_key_jwt`, JWT-bearer federation with the supported-issuer list including Azure, the AWS STS grant with its endpoint allowlist, D1 `jti` replay cache, workspace-scoped Trust Rule matching, per-issuer matching rules, hardened STS proxying), Admin API (The Admin API: Console-only admin resource, `auth_time` on families, confirmation pages) with audit, DNS and Fly certificate automation, rate limits, logging policy. |
| query-farm-astro (site) | Console and Tracker become Admin API clients through the confidential `orchard-console` client, redirecting to Front Desk confirmation pages for changes that create access; Workload, Workload Key and Trust Rule management; a session list with per-origin revoke; billing, requests and leads stay. |

## Build order

1. **Extension groundwork, with no Orchard dependency:** effective base URL,
   single-flight and proactive refresh, stale-401 dedupe, conditional
   `resource`, canonical compare, `iss` check, `127.0.0.1` redirect, Linux
   credential store.
2. **Delegated mode** in the extension, plus allowed delegation hosts and the
   remote-secret restriction.
3. **Test harness:** a Python fake Front Desk (the
   `test/catalog_discovery_auth.py` pattern) with a seeded refresh token and a
   JWT-configured Python worker on loopback (behind the test escape hatch).
   sqllogictest covers settings and refusals; the Python harness covers flows.
4. **SDK hosted mode** and the conformance suite, all seven SDKs.
5. **Front Desk** (`vgi-orchard-auth`) in the new account, then the site's
   Console and Tracker moved onto its Admin API.
6. **Workloads**: Workload Keys first, then Trust Rules. GitHub Actions is
   tested from a real Actions run; AWS from an EC2 role and an ECS task; Azure
   from a VM managed identity and an App Service, in a tenant where we can
   register the audience app. Then Google Cloud, Fly and Kubernetes.
7. **WASM** (open redirects with origin shown on consent, open CORS, memory-only tokens).
8. **End to end on EC2:**
   - real WorkOS staging;
   - two Field Workers on `*.field.queryfarm.cloud` standing in for two regions;
   - sign-in by browser, by device code, from WASM, with a Workload Key, and
     through GitHub Actions, AWS and Azure Trust Rules;
   - then remove a member, sign out everywhere, delete a Workload Key, stop the
     deployment and rotate the Signing Key, checking that each ends access as specified.

## Open questions

1. **Lifetimes:** Pass 15 minutes; Renewal Token 7 days idle / 30 days
   absolute; refresh grace 30 s?
2. **Generic VGI feature:** document delegated mode as generic? The extension
   side already is. Third-party issuers would need their own
   `vgi_allowed_delegation_hosts` entry.
3. **WASM token persistence:** memory-only to start; is a session-scoped store
   wanted later?
4. **KMS signing:** now or later?
5. **Workload scope:** whole workspace by default, with an optional
   deployment list, or deployment-specific by default?
