# Proposal: credentials for ATTACH (secret attach options + `vgi_attach` secrets)

**Status:** implemented in the extension (Part A extension side, Part B). The decisions that
closed the open questions, and the places the implementation differs from the text below, are at
the end: [Decisions](#decisions) and [Implementation notes](#implementation-notes).

## Context

A catalog can need a credential before it can attach at all: an API key that picks the tenant,
or a token without which `catalog_attach` refuses. Today such a credential can reach the worker
two ways:

1. **OAuth** (`bearer_token` / `oauth_refresh_token`, or the extension's own PKCE flow).
2. **A declared attach option.** The worker declares it in `AttachOptionSpec`, and the user passes
   it in plain text in the ATTACH statement. vgi-go's own example does exactly this:
   `{Name: "api_key", Type: String, Required: true}` (`examples/attach_options/attach_options.go`).

The second route has no protection at all. The value sits in the ATTACH text, so it ends up in
shell history, client logs and share links. The extension also treats it as ordinary
configuration (see below). The protocol has no way to say "this option is a credential", and no
way to supply one from a DuckDB secret.

Cupola (the browser frontend) is moving to workspaces of several catalogs. These have to be shared
as links and exported as files, without the credentials. That needs both pieces below.

## How it works today (verified)

- **Secret lookups never feed the main catalog's ATTACH.** Every `SecretManager` call in the
  extension is either:
  - function-bind time (`VgiSecretRequirement`, `vgi_catalog_api.cpp:3339/3351`)
  - companion catalogs only, opt-in (`InjectCompanionSecret`, `vgi_extension.cpp:1229-1269`)
  - the iroh transport identity (`LookupSecret(scope, "iroh")`, `vgi_iroh_config.cpp:63`)
  - the remote secret storage after attach (`vgi_secret_storage.cpp`)
- **A worker's own secret types are registered after it attaches** (`vgi_extension.cpp:2255-2290`).
  So `CREATE SECRET (TYPE <worker type>, …)` can't run before the first ATTACH even if it were
  consulted. The comment at `vgi_extension.cpp:1796` recommends `CREATE SECRET` for
  non-ephemeral tokens, but nothing implements that for ATTACH.
- **Declared options are cast to their declared types and forwarded** (`vgi_extension.cpp:2165-2213`).
  Undeclared options are rejected with "Unknown ATTACH option".
- **Every option except a fixed list goes into the result-cache key in plain text.** The list is
  `type`, `location`, `path`, `bearer_token`, `oauth_refresh_token`, `iroh_secret_key` and
  STRUCT-valued options (`vgi_extension.cpp:1676-1684`). The key is serialized into
  `attach_options_canonical` (`:2357-2368`) and from there into the on-disk cache digest
  (`vgi_exchange_cache_key.cpp:346`, `vgi_result_cache.cpp:345`). So an `api_key` option is
  written to disk.
- **Only the same three tokens are redacted from `info.options`** (`vgi_extension.cpp:1807-1814`),
  which is what `duckdb_databases()` and friends expose.
- **DuckDB secret scope matching is a plain string prefix** (`BaseSecret::MatchScore`,
  `duckdb/src/main/secret/secret.cpp:13-31`). A scope of `https://sales.example.com` matches
  `https://sales.example.com.evil.net`. A secret with **no** scope matches every path, at score 0.
- **The spec wire format** is the shared four columns (`name`, `description`, `type`,
  `default_value`) plus extras appended per spec. `required` was appended as a nullable column that
  older peers ignore (`vgi-python/vgi/catalog/attach_option.py:51-58`; `VgiAttachOptionSpec`,
  `src/include/vgi_catalog_metadata.hpp:59-68`).

## Goals

1. A worker can mark an attach option as a credential, and every client and the extension handle
   it accordingly.
2. A credential needed for ATTACH can come from a DuckDB secret, so the ATTACH statement carries
   none: `CREATE SECRET …; ATTACH …` works the same in the CLI, Python and DuckDB-WASM.
3. A worker can never choose which of the user's secrets it receives.
4. Credentials never reach the result-cache key in plain text, `duckdb_databases()`, telemetry or
   logs.

## Non-goals

- Changing OAuth. `bearer_token` / `oauth_refresh_token` keep working as they do now. A secret
  can supply them (below), but the OAuth flow is untouched.
- Function-bind secrets (`VgiSecretRequirement`): unchanged.
- Secret storage on the server (Orchard): unchanged. That covers post-attach data credentials.

## Design

### Part A: `secret` flag on `AttachOptionSpec`

Append a nullable `secret: bool` column to the spec, in the same way as `required`:

| Repo | Change |
|---|---|
| vgi-python | `AttachOptionSpec.secret: bool = False`; add it to `ARROW_SCHEMA` and `_extra_row` |
| vgi-typescript | `AttachOptionSpec.secret?: boolean`; (de)serialize it by column name |
| vgi-go | `Secret bool` on the option definition and the spec |
| extension | `VgiAttachOptionSpec::secret`, read by column name, default `false` |

A missing column reads as `false`, so older peers interoperate unchanged.

What the extension does with a declared secret option:

- **Cache key.** Never put it in `key_options` in plain text. It can't simply be left out either: if
  the key selects which tenant's data a user sees, two users with different keys would share cached
  results. Instead, add `name=h:<hex>` to the key, where `<hex>` is HMAC-SHA256 of the value under a
  random per-cache-directory salt (created on first use, alongside the cache). This keeps results
  separate per credential, and digests don't match across machines. It won't protect a low-entropy
  credential from someone who holds the cache directory, salt included. Document that.
- **`info.options`.** Redact it to `<redacted>`, as is done for the three tokens now.
- **Logs and telemetry.** Never emit the value. `VGI_LOG` attach events list secret option names
  only.

Note on ordering: the spec is only known after the discovery RPC (`InvokeCatalogs`, `:2121`), but
`key_options` is filled earlier, in `apply_option`. Hold plain-text cache-key entries for
worker-declared options back until validation (`:2190-2212`), then add each one in plain text or as
a hash according to its spec. The extension's built-in options (`pool`, `cache`, …) are unaffected.

Clients use the flag to mask the field, keep the value out of exported or shared configuration, and
offer to store it as a `vgi_attach` secret (Part B).

### Part B: `vgi_attach` secrets resolved at ATTACH

The extension registers a secret type of its own, **`vgi_attach`**, at LOAD time next to `iroh`
(`vgi_extension.cpp:3428`). It doesn't depend on any worker. It is key-value with the `config`
provider. **Every key is redacted** in `duckdb_secrets()`: the type exists only to hold
credentials.

DuckDB's `CREATE SECRET` only accepts parameters the provider declares, so a worker's option names
can't be top-level keys (no secret type can accept arbitrary ones). The type therefore has three
parameters: `BEARER_TOKEN`, `OAUTH_REFRESH_TOKEN`, and `OPTIONS`, a `MAP(VARCHAR, VARCHAR)` of
worker attach options (see [Implementation notes](#implementation-notes)).

```sql
-- Matched by scope:
CREATE SECRET sales_creds (
    TYPE vgi_attach,
    SCOPE 'https://sales.example.com',
    OPTIONS MAP {'api_key': 'sk-…'}
);
ATTACH 'sales' (TYPE vgi, LOCATION 'https://sales.example.com');

-- Or named explicitly:
ATTACH 'sales' (TYPE vgi, LOCATION 'https://sales.example.com', attach_secret 'sales_creds');

-- Persistent secrets work the same way in the CLI:
CREATE PERSISTENT SECRET sales_creds (TYPE vgi_attach, SCOPE 'https://sales.example.com',
                                      OPTIONS MAP {'api_key': getenv('SALES_API_KEY')});

-- Which secret would an ATTACH of this LOCATION use? (names only, never values)
SELECT * FROM vgi_which_attach_secret('https://sales.example.com');
```

**Which secret is used**, decided once per ATTACH:

1. `attach_secret '<name>'` set: `GetSecretByName`. It must exist and be of type `vgi_attach`,
   otherwise it's a binder error. Scope isn't checked: the user named it.
2. Otherwise: `LookupSecret(LOCATION, "vgi_attach")`, then apply the **boundary rule** below.
3. `attach_secret ''` (empty) turns lookup off for this ATTACH.

**Boundary rule.** DuckDB's prefix match is not enough for credentials:

- A secret **without a scope never matches** in lookup. A user's one unscoped secret must not be
  sent to every server they attach.
- The match must end at a URL boundary: the scope equals the LOCATION, the scope ends in `/`, or
  the next character of the LOCATION after the scope is `/`, `?`, `#` or `:`. This rejects
  `https://sales.example.com.evil.net`.
- Compare against the raw LOCATION the user wrote, before any rewrite, as `CheckLocationPolicy`
  does (`:1862`).

**How the secret's keys are applied.** For each key in the secret:

| Key | Effect |
|---|---|
| `bearer_token`, `oauth_refresh_token` | Fed into the existing handling (mutual exclusion still applies, `:1827`) |
| An `OPTIONS` entry the catalog declares (after discovery) | Supplied as that option and cast by its spec, like an explicit value |
| Any other `OPTIONS` entry | Ignored, and logged by **name** (never value) at debug level |

Two more rules:

- **Explicit wins.** An option given in the ATTACH clause (or its connection-string query)
  overrides the same key from the secret, matching `InjectCompanionSecret`'s "don't overwrite".
- **Secret-sourced values are secret.** Whether or not the spec sets `secret`, they get Part A's
  treatment: hashed in the cache key, never written to `info.options`, never logged. They are
  injected through the `apply_option` path, not by changing `info.options`, so they never appear
  there.

**Discovery.** If the secret supplies a declared option, the explicit form needs the
`InvokeCatalogs` round trip it already makes for attach options (`:2121`). If the secret holds only
`bearer_token` / `oauth_refresh_token`, no extra RPC is needed. The discovery RPC itself is
authenticated with the secret's token if it carries one, so a worker that won't list catalogs
without auth still works.

**Why the worker can't choose a secret.** The only inputs that pick a secret are the user's
`attach_secret` and the user's LOCATION. Nothing in a worker response (catalog info, attach result,
`secret_ref`) is consulted. A worker learns only the keys it declared, and only from the secret the
user scoped to that worker. This is the same fail-closed position as `InjectCompanionSecret`.

### Interaction with `vgi_allowed_transports` and companions

- The location policy check runs first, unchanged. A refused LOCATION never resolves a secret.
- Companion catalogs keep their own `secret_ref` / `attach_companion_secrets` path. A `vgi_attach`
  secret applies only to the catalog it was resolved for, never to its companions.

### Side note: iroh lookup

`SecretKeyFromScope` (`vgi_iroh_config.cpp:60-81`) has the same prefix behavior, unscoped secrets
included. The exposure there is lower (it's the client's own identity key, not a credential the
worker receives), but applying the same boundary rule would be consistent.

## What clients get

- **DuckDB CLI / Python:** credentials leave the ATTACH text. A `.sql` setup script can be committed
  with `getenv()` values.
- **Cupola:**
  - **Credential store.** Workspaces keep credentials in a local store, never in the workspace file.
  - **At boot.** Cupola runs `CREATE SECRET (TYPE vgi_attach, SCOPE '<url>', …)` for each
    catalog, then a clean ATTACH.
  - **Specs.** `secret` drives the options editor, and `required` + `secret` lets it ask for a
    credential before attaching.
  - **Workspace export.** "Export as DuckDB script" writes `CREATE SECRET … getenv(…)` followed by
    the ATTACH.
  - **Feature detection.** If the loaded extension has no `vgi_attach` type
    (`SELECT 1 FROM duckdb_secret_types() WHERE type = 'vgi_attach'`), Cupola falls back to inline
    options, masked and kept out of exports.

## Tests

- `test/sql/integration/attach_secrets/` (sqllogictest), driven by `run_http_attach_options_integration.sh`
  with the vgi-go attach-options worker:
  - a scoped secret supplies a `required` `api_key`, so ATTACH succeeds with no options
  - an explicit option overrides the secret
  - `attach_secret` by name; a wrong name or wrong type gives an error
  - `attach_secret ''` turns lookup off
  - **boundary:** `https://host` must not match `https://host.evil.net` or `https://hostx`; it must
    match `https://host/`, `https://host:443` and `https://host?x`
  - **an unscoped `vgi_attach` secret is never used by lookup**
  - `duckdb_databases()` and `duckdb_secrets()` show no secret values
  - a secret holding `bearer_token` authenticates the discovery RPC
- **Cache key unit tests (C++):** the same secret gives the same key; different secrets give
  different keys; the plain-text value never appears in `attach_options_canonical`.
- **SDK round trip:** spec with `secret` across Python ↔ TS ↔ Go ↔ C++, plus a spec without the
  column (an older peer) reading as `false`.

## Rollout

1. **Part A** in all four repos. It's backward compatible both ways. Change the vgi-go example's
   `api_key` to `Secret: true`.
2. **Part B** in the extension: the `vgi_attach` type, lookup, the boundary rule, hashing and
   redaction.
3. **Docs:** a "Credentials at ATTACH" section in the user docs, and SDK docs saying credential
   options must be declared `secret`.
4. **Cupola** feature-detects both and keeps its fallback.

## Decisions

The open questions were settled as follows.

1. **Name:** `vgi_attach`. The name is reserved, along with `iroh`: a worker advertising a secret
   type with either name is refused, and the refusal is logged (`attach.secret_type_refused`)
   instead of being skipped silently. (A worker type that merely collides with another worker's
   earlier registration is now logged too, as `attach.secret_type_exists`.)
2. **Lookup is on by default**, with the boundary rule and the no-unscoped rule.
   `attach_secret ''` turns it off for one ATTACH; `attach_secret 'name'` selects a secret by
   name.
3. **The iroh lookup follows the same rules.** `SecretKeyFromScope` uses the same boundary-rule
   lookup: an unscoped `iroh` secret no longer matches every Iroh LOCATION, and `iroh://abc` no
   longer matches `iroh://abcdef`.
4. **Grainlift** is handled separately, in the grainlift repository.
5. **The OAuth tokens stay out of the cache key**, as before.

## Implementation notes

Where the extension differs from, or adds to, the design above:

- **`OPTIONS` map instead of top-level option keys.** DuckDB's `BindCreateSecret` rejects any
  parameter the provider did not declare (`Unknown parameter 'api_key' for secret type …`), and
  there is no wildcard. Worker attach options therefore go in `OPTIONS MAP {'name': 'value'}`;
  values are `VARCHAR` and are cast by the catalog's spec at ATTACH, like an explicit value.
  `bearer_token` / `oauth_refresh_token` are refused inside `OPTIONS` (they're top-level), and a
  secret can't hold both tokens.
- **Only the local secret storages are consulted** (`memory`, `local_file`), for lookup and for
  `attach_secret`. The Orchard remote storage a worker can register after attach is worker-backed,
  so it can neither supply an ATTACH credential nor be shown a LOCATION. Lookup scans
  `AllSecrets` (cache-only for the remote storage, no network) rather than calling
  `LookupSecret`, because DuckDB's best prefix match can fail the boundary rule while a shorter
  scope passes. Ties on scope length prefer a temporary secret, then the smaller name, as DuckDB
  does.
- **Explicit auth wins as a whole.** If the ATTACH gives `bearer_token` or `oauth_refresh_token`,
  neither token from the secret is used (taking the other one would only trip the mutual-exclusion
  error).
- **Salt file.** `<vgi_result_cache_dir>/attach_option_key.salt`, 32 random bytes as hex, created
  on first use (hard-linked into place so concurrent processes agree) and read by every process
  using that directory. Without a cache directory (memory-only cache, WASM, or an unwritable
  directory) the salt is random per process. The salt is chosen when the catalog is attached; a
  later `SET vgi_result_cache_dir` doesn't re-key it. Like any salted hash, it does not protect a
  low-entropy credential from someone holding the cache directory, salt included.
- **Cache-key entries for worker-declared options are deferred** until the spec is known. A
  non-secret option keeps exactly the plain-text entry it had before (lowercased name, the value
  as written), so existing cache entries stay valid; a secret one (declared `secret`, or sourced
  from a `vgi_attach` secret) becomes `name=h:<hmac>`, where the HMAC covers name and value.
  `attach_secret` never enters the key.
- **Redaction** covers declared-secret options in the options clause (`<redacted>` in
  `duckdb_databases().options`) and in the connection-string form (the path's whole `?query`
  becomes `?<redacted>`, as it already did for the tokens). A cast failure for a secret value
  names the option and type but doesn't quote the value.
- **`duckdb_databases().options` reads `AttachOptions`, not `info.options`.** The attach callback
  receives both, and only `info.options` was redacted, so the three tokens given in the options
  clause were visible in `duckdb_databases()` all along. Credentials (the tokens and declared-secret
  options) are now redacted in both.
- **`enable_external_access = false`.** DuckDB enumerates every secret storage at once, and the
  persistent one can't be read with the file system disabled. Implicit lookup then finds nothing
  instead of failing the ATTACH; `attach_secret '<name>'` still reaches a temporary secret.
- **Logs** record which secret was used and the names of the options it offered
  (`attach.secret_resolved`), never values.
- **`vgi_which_attach_secret(location)`** is a table function naming the secret scope lookup would
  pick (`name`, `persistent`, `storage`), the boundary-rule counterpart of DuckDB's
  `which_secret`.
- **Discovery** with a secret: the `InvokeCatalogs` round trip runs whenever the secret carries
  `OPTIONS` (to learn which are declared), authenticated with the secret's token. A token-only
  secret needs no extra RPC.
- **Options clause wins over the connection-string query.** Worker options from the path's
  `?query` were collected with `emplace`, so the query value won over the clause, contrary to the
  documented order. They are now assigned, so the clause wins.
- **`vgi_catalogs()`** exposes the flag as `attach_options[].secret`, next to `required`.
- **Tests:** `test/sql/integration/attach_secrets/`, run by
  `test/run_http_attach_options_integration.sh` (`make test_http_attach_options`) against the
  vgi-python attach-options fixture, plus a bearer-auth instance of it.
  `VGI_ATTACH_OPTIONS_WORKER_CMD` runs the same files against another SDK's worker (e.g. vgi-go's,
  which doesn't send the `secret` column); the declared-secret cases are skipped there. The pure
  helpers (boundary rule, HMAC, salt, canonical key) have C++ unit tests in
  `test/cpp/test_attach_credentials.cpp`.
