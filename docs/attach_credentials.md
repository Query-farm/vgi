# Credentials passed as ATTACH options

A catalog can need a credential before it can attach at all: an API key that picks the tenant, or
a token without which `catalog_attach` refuses. Besides OAuth (`bearer_token`,
`oauth_refresh_token`, or the extension's own PKCE flow), the way to pass one is a **declared
attach option**: the worker lists it in its `AttachOptionSpec`s and the user supplies it in the
ATTACH statement. vgi-python's fixture does exactly this:

```python
api_key: Annotated[str, AttachOption(desc="API key", required=True, secret=True)]
```

This document describes how the extension treats such options, the recommended way to supply them,
three related fixes, and one design that was considered and rejected.

## The `secret` flag on `AttachOptionSpec`

`AttachOptionSpec` carries a nullable boolean column `secret`, appended after `required`. It is
identical across the SDKs:

| SDK | Declares it from |
|---|---|
| vgi-python | 0.38.0 |
| vgi-go | v0.28.0 |
| vgi-typescript | 0.37.0 |
| vgi-java | 0.34.1 |
| vgi-rust | 0.37.0 |
| vgi-csharp | 0.10.0 |

The extension reads it **by column name** (`ParseAttachOptionSpec`, `src/vgi_catalog_api.cpp`). An
absent column (a worker older than the flag) or a null reads as `false`, so old and new peers
interoperate both ways. `VgiAttachOptionSpec::secret` (`src/include/vgi_catalog_metadata.hpp`)
holds it, and `vgi_catalogs()` exposes it as `attach_options[].secret`, next to `required`.

What the extension does with an option the spec marks `secret`:

- **Result-cache key.** Every ATTACH option except the tokens and LOCATION goes into the
  result-cache key (`attach_options_canonical`), which reaches the on-disk cache digest. A secret
  option can't simply be left out: if it selects which tenant's data a user sees, two users with
  different keys would share cached results. So it enters the key as `name=h:<hex>`, where `<hex>`
  is HMAC-SHA256 over the name and value under a random salt
  (`src/vgi_attach_credentials.cpp`). Results stay separate per credential, and digests don't
  match across machines.
  - The salt is per cache directory: `<vgi_result_cache_dir>/attach_option_key.salt`, 32 random
    bytes as hex, created on first use and hard-linked into place so concurrent processes agree,
    then read by every process that uses the directory. Without a cache directory (memory-only
    cache, WASM, or an unwritable directory) it is random per process.
  - The salt is chosen when the catalog attaches; a later `SET vgi_result_cache_dir` doesn't re-key
    it.
  - Like any salted hash, it doesn't protect a low-entropy credential from someone who holds the
    cache directory, salt included.
- **Ordering.** The spec is only known after the discovery RPC, but the cache key is collected while
  the options are parsed. So cache-key entries for worker-declared options are held back until the
  spec is validated, then added as plain text or as an HMAC. A non-secret option keeps exactly the
  plain-text entry it always had (lowercased name, the value as written), so existing cache entries
  stay valid.
- **`duckdb_databases()`.** The value is shown as `<redacted>` in `options`. Given in the
  connection-string form (`'cat?location=…&api_key=…'`), the whole `?query` is redacted from the
  path, as it already was for the tokens.
- **Errors and logs.** A failed cast names the option and its type but doesn't quote the value. No
  `VGI_LOG` event or telemetry field carries an attach option's value.

## Supplying a credential

Pass it inline, as the option the catalog declares. An ATTACH option is an expression, so the value
doesn't have to appear in the SQL text:

```sql
-- In the CLI, read it from the environment:
ATTACH 'sales' (TYPE vgi, LOCATION 'https://sales.example.com', api_key getenv('SALES_API_KEY'));

-- Any constant expression works:
ATTACH 'sales' (TYPE vgi, LOCATION 'https://sales.example.com', api_key 'sk-' || 'abc');
```

`getenv()` is the CLI's function (DuckDB's shell registers it). With it, a setup script can be
committed without the credential in it. The value is evaluated before the extension sees it, so it
gets the same treatment as a literal: forwarded to the worker, redacted from `duckdb_databases()`,
hashed into the cache key, never logged.

`getenv()` of an unset variable returns an empty string, not an error. A catalog that treats an
empty key as missing should say so in its `catalog_attach` validation.

Clients use the flag to mask the field in their UIs and keep the value out of exported or shared
configuration.

## Fixes made alongside

- **The tokens were visible in `duckdb_databases()`.** The attach callback receives the options
  twice: `info.options`, and DuckDB's own `AttachOptions` copy, which is what `duckdb_databases()`
  reads. Only `info.options` was redacted, so `bearer_token`, `oauth_refresh_token` and
  `iroh_secret_key` given in the options clause appeared there in plain text. Both copies are
  redacted now, for the tokens and for secret options.
- **The options clause now wins over the path's `?query`.** Worker options from the
  connection-string query were collected with `emplace`, so on a conflict the query's value won,
  contrary to the documented order (the query is applied first so that the clause overrides it).
  They are assigned now.
- **The `iroh` identity-secret lookup respects a URL boundary.** `SecretKeyFromScope`
  (`src/vgi_iroh_config.cpp`) used DuckDB's `LookupSecret`, a plain string prefix match where a
  secret with no scope matches everything. It now applies a boundary rule
  (`ScopeMatchesAtBoundary` in `src/vgi_attach_credentials.cpp`):
  - a secret with no scope never matches;
  - the match must end at a boundary: the scope equals the LOCATION, the scope ends in `/`, or the
    next character of the LOCATION is `/`, `?`, `#` or `:`. So `iroh://abc` no longer matches
    `iroh://abcdef`;
  - only the local secret storages (`memory`, `local_file`) are consulted, never a worker-backed
    one;
  - ties on scope length prefer a temporary secret, then the smaller name, as DuckDB does.

  A worker can't register a secret type named `iroh` (refused and logged), and a worker secret type
  that collides with an earlier worker's registration is now logged instead of being skipped
  silently.

## Considered and rejected: `vgi_attach` secrets

The original proposal also had the extension register a `vgi_attach` DuckDB secret type. ATTACH
would have looked one up by LOCATION scope (or by name, through an `attach_secret` option) and
taken attach options and OAuth tokens from it, so `CREATE SECRET …; ATTACH …` would carry no
credential. It was implemented, then removed before anyone relied on it.

- **It was redundant.** The flag already keeps an inline credential out of `duckdb_databases()`, the
  cache key and the logs. An expression such as `getenv()` keeps it out of the SQL text. That
  covers what the secret type was for.
- **It was extra surface area.** It added:
  - a secret type;
  - an ATTACH option, `attach_secret`;
  - a diagnostic function;
  - a lookup over every user secret at every ATTACH;
  - a boundary rule for credentials sent to workers;
  - precedence rules between explicit and secret-sourced values;
  - a special case for `enable_external_access = false`, where DuckDB can't list the persistent
    secrets.

  Each was a place for a credential to reach the wrong worker.
- **It couldn't look like the design.** DuckDB's `CREATE SECRET` accepts only the parameters a
  type declares, so worker option names couldn't be top-level keys. They had to go in an
  `OPTIONS MAP {...}`, which is clumsier than writing the option in the ATTACH.

## Tests

- `test/sql/integration/attach/attach_options_required.test`: the flag in `vgi_catalogs()`; the
  inline value forwarded (the gated catalog refuses without it); redaction in
  `duckdb_databases()`, including for a constant expression and the connection-string form.
- `test/sql/integration/attach/attach_options_echo.test`: `secret` reads false for the ordinary
  catalog, and the clause wins over the path's `?query`.

  Both files run against every SDK's attach-options worker, the same files and gates as before. A
  worker that stops advertising the flag fails them.
- `test/sql/integration/bearer_auth/bearer_token.test`: the bearer token is redacted from
  `duckdb_databases()`.
- `test/cpp/test_attach_credentials.cpp` (`vgi_unit_tests`, tag `[attach-credentials]`):
  - HMAC against the RFC 4231 vectors;
  - the hash is stable for one value, differs across values, salts and names, and the plain text
    never appears in the canonical key;
  - the salt file;
  - the boundary rule.
- `test/run_http_attach_options_integration.sh` (`make test_http_attach_options`) runs the
  attach-options files over HTTP. Set `VGI_ATTACH_OPTIONS_WORKER_CMD` to run them against another
  SDK's worker.
