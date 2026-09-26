# Catalog discovery and authentication

`vgi_catalogs(location)` lists catalogs before attaching one. For HTTP/HTTPS
and `httpi://` services it uses the same authentication handlers as `ATTACH`,
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
