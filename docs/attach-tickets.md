# Attach tickets and `vgi_export_session()`

An attach ticket lets an unattended runner (a scheduler, an alert host)
reattach a user's catalog later, as that user, without ever seeing the user's
attach options. The wire format and the worker's side are specified in
vgi-python's `docs/protocol/vgi-attach-tickets.md`; this page is the
extension's side.

Two credentials, minted together while the user is attached and logged in:

| | From | Says |
| --- | --- | --- |
| **grant** | `vgi_rpc.Identity.v1` `issue_grant` | *who* attaches. Presented later as `bearer_token`. |
| **ticket** | `vgi.attach_tickets.v1` `seal_attach` | *what* to attach: the catalog, its options (secret ones included) and version specs, sealed so only that worker can open them, and only for the same principal. |

```sql
-- While the user is attached and logged in:
SELECT alias, status, expires_at FROM vgi_export_session();

-- Later, anywhere:
ATTACH 'sales' AS sales (TYPE vgi, LOCATION 'https://sales.example/vgi',
    bearer_token '<grant>', attach_ticket '<ticket>');
```

## `vgi_export_session(aliases := NULL, ttl_seconds := NULL)`

One row per attached catalog (sorted by alias), or per alias in `aliases`:

| Column | Meaning |
| --- | --- |
| `alias` | Local catalog alias. |
| `location` | `LOCATION` as typed at ATTACH. |
| `catalog_name` | The worker's catalog name (the ATTACH path). |
| `grant` | The grant token, for `bearer_token`. |
| `ticket` | The ticket, for `attach_ticket`. |
| `expires_at` | The earlier of the two expiries; NULL when neither expires. |
| `status` | See below. |
| `message` | Why, for every status but `ok`. |

| `status` | When |
| --- | --- |
| `ok` | Both halves minted. |
| `not_vgi` | The catalog is not `TYPE vgi`. |
| `not_supported` | The worker's reflection lists no `vgi.attach_tickets.v1` or no `vgi_rpc.Identity.v1` (tickets need an HTTP worker with a configured signing key and grant keys), `issue_grant` is not implemented, or the catalog was itself attached with a ticket (its original options were never in this session). |
| `stale_login` | `issue_grant` answered `stale_auth`: the session is not a fresh login (for example it authenticated with a grant; grants never mint grants). Re-authenticate and export again. |
| `refused` | The worker refused `issue_grant` or `seal_attach`. |
| `error` | Anything else for that row: an unknown alias, an unreachable worker. |

Per-row failures never abort the export. `ttl_seconds` (positive) asks both
halves for that lifetime; the worker caps it at its grant maximum. NULL asks
for the longest the worker allows. The result holds credentials: store it as a
secret.

To seal the original options, the extension keeps each catalog's validated
ATTACH options, **secret ones included**, and the version specs as typed, in
memory on the catalog's connection parameters for the life of the attach
(`VgiRetainedAttach`). They are never logged or persisted, and are dropped at
DETACH. There is no reconnect path that re-runs `catalog_attach`, so the
retained values are always the ones the catalog was attached with.

## The `attach_ticket` ATTACH option

- A secret, like `bearer_token`: redacted from `duckdb_databases()` (options
  clause and ATTACH path query), never logged, and folded into the result-cache
  key only as a salted hash.
- Before `catalog_attach`, the extension asks the worker's reflection
  (`vgi::HostsProtocol`) whether it hosts `vgi.attach_tickets.v1`, and fails
  clearly if not. The worker would otherwise see an unknown option.
- It is sent as the only catalog option, `vgi_attach_ticket`. Catalog options,
  `data_version_spec` and `implementation_version` beside it are refused before
  any I/O: the ticket carries the originals. Extension options (`LOCATION`,
  `bearer_token`, `pool`, ...) are fine.
- The worker refuses a ticket sealed for another principal, tampered with, from
  another worker's key (`attach_ticket_invalid`), or expired
  (`attach_ticket_expired`).

## Tests

`test/sql/integration/attach_ticket/`: `unsupported.test` runs on every lane
against `${VGI_TEST_WORKER}`; `round_trip.test` needs two ticket-hosting HTTP
workers and runs via `make test_http_attach_ticket`
(`test/run_http_attach_ticket_integration.sh`).
