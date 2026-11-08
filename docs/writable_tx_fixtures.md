# Writable-table and transaction-hook fixtures

The files `test/sql/integration/writable_tx/*.test` check, against a fixture worker each SDK
provides, that a worker built with the SDK's **public** API can serve an UPDATE-able catalog table
and take part in DuckDB transactions. This page is the contract those files hold each fixture to.
An SDK implements the catalog below in a fixture worker and then opts in.

## Why these exist

Porting one worker (a Philips Hue worker: an UPDATE-able `lights` table whose changes are staged in
DuckDB transactions and sent at COMMIT) to every SDK found that only vgi-typescript could express
it. The others:

1. **Had no transaction hooks.** vgi-go, vgi-csharp, vgi-rust and vgi-java gave the worker no way to
   run code at transaction begin, commit or rollback.
2. **Hard-coded `supports_transactions`.** vgi-csharp turned it on only for a catalog named
   `example`, vgi-rust always, vgi-java per catalog kind, vgi-go worker-wide. It belongs to the
   attach result, so a worker can decide **per attach** (here: from an ATTACH option).
3. **Hid the transaction id from functions.** Functions, and in particular the table in-out function
   behind UPDATE, could not see the transaction id at process time, so they could not stage work
   under it. vgi-go handed commit/rollback the *sealed* id over HTTP, which never equals what a
   function (or the begin hook) saw.
4. **Could not declare a writable catalog table.** vgi-go, vgi-rust and vgi-java table descriptors
   had no update function or write result modes.
5. **Never negotiated `vgi.constraint.enforce.v1`.** UPDATE on a table with constraints failed with
   "constraint enforcement profile was not negotiated" (in vgi-rust even on a read-only constrained
   table, instead of "does not support UPDATE").
6. **Dropped secret ATTACH options.** vgi-rust and vgi-java did not pass options declared `secret`
   on to functions.

## Opting in

Every file is gated on two variables:

```
require-env VGI_WRITABLE_TX_FIXTURES
require-env VGI_WRITABLE_TX_WORKER
```

`VGI_WRITABLE_TX_WORKER` is the fixture worker's LOCATION, set per transport the way
`VGI_SIMPLE_WRITABLE_WORKER` is: the plain command for stdio, `launch:<command>` for the launcher,
`http://127.0.0.1:<port>` for HTTP. Set `VGI_WRITABLE_TX_FIXTURES=1` in the SDK's
`ci/run-integration.sh` once the fixture implements this page, on every lane it can serve. Until
then the files skip with the reason `require-env VGI_WRITABLE_TX_FIXTURES` (or
`require-env VGI_WRITABLE_TX_WORKER`); harnesses that enforce a skip allow-list
(`EXPECTED_SKIP_REASONS`) must list that reason, with a comment, until the fixture lands, and must
remove it in the commit that sets the variables. The vgi `Makefile` exports
`VGI_WRITABLE_TX_FIXTURES` (empty by default: the vgi-python fixture does not exist yet) and its
skip allow-list and the vgi `integration.yml` suite step allow both reasons.

The harness SHOULD point `VGI_TXFIX_STATE_DIR` at a directory private to the lane (for example a
`mktemp -d`), so lanes running at the same time on one host do not share fixture state.

Status:

| SDK | Fixture | From |
|---|---|---|
| vgi-typescript | `examples/writable-tx-worker.ts` (`bin/vgi-example-writable-tx-worker`, `--http`) | reference fixture (vgi-typescript main) |
| vgi-python | — | pending |
| vgi-go | — | pending |
| vgi-rust | — | pending |
| vgi-java | — | pending |
| vgi-csharp | — | pending |

## Common rules

- **Public, documented API only**: catalog-table descriptors, attach hooks, transaction hooks,
  write-result helpers. No reflection, proxies, internal namespaces or hand-built `TableInfo`. If
  the SDK cannot express something publicly, that is the SDK bug to fix.
- A dedicated worker (not the example worker), serving one catalog `txfix` with one schema `main`.
  It must serve stdio and the launcher (`--unix`), and HTTP if the SDK can serve HTTP.
- The tests attach the catalog twice with the same token, once with hooks:

  ```sql
  ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '${VGI_WRITABLE_TX_WORKER}', token 'tok-123');
  ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '${VGI_WRITABLE_TX_WORKER}', token 'tok-123', transactional true);
  ```

- Transaction ids are rendered as **lowercase hex** wherever they appear in output.
- Errors the worker raises must **contain** the quoted text; the tests match by substring
  (`<REGEX>:.*text.*`), so the SDK may add its own prefix or suffix.

### ATTACH options

| Name | Type | Notes |
|---|---|---|
| `transactional` | BOOLEAN, default false | The attach result's `supports_transactions` is **true only when this is true**, per attach. |
| `token` | VARCHAR, **secret**, optional | DuckDB redacts it. Its value MUST reach every function of the attach (bind and process), the update function, and the transaction hooks. |

### State

Shared across processes: the stdio, launcher and HTTP lanes may route statements of one test, even
of one transaction, to different worker processes. Keep it in a file under `$VGI_TXFIX_STATE_DIR`
(default: `<system temp dir>/vgi-txfix`), guarded by a file lock (or a database with its own
locking, e.g. SQLite).

State is **partitioned by the `token` value** (a NULL token is its own partition). Each test file
uses its own token, so files that run in parallel do not see each other's rows; and the partition a
function or hook works on is the observable proof that the secret reached it. A partition holds the
committed rows of both tables, the staged changes per transaction id, and the event log. It starts
out as the initial rows below. Every test file starts with `SELECT * FROM t.main.reset()`.

## Tables

### `main.items`

| Column | Type | Constraints |
|---|---|---|
| `id` | INTEGER | NOT NULL, PRIMARY KEY |
| `name` | VARCHAR | NOT NULL |
| `qty` | BIGINT | |

Initial rows: `(1, 'a', 10)`, `(2, 'b', 20)`, `(3, 'c', 30)`. Scans return rows ordered by `id`.

- **Row id**: a BIGINT row-id virtual column whose value is `id`. The scan supports projection
  pushdown (DuckDB reads `rowid` only through it).
- **Writable** through an **update function** declared on the table descriptor (`items_update`, a
  table in-out function). Write result modes for update: advertise the highest mode the SDK can
  produce, at least `rows` (`changes` if supported; see [Write result modes](#write-result-modes)).
  No insert or delete function.
- **Updatable**: `name`, `qty`. DuckDB sends the SET columns plus `rowid` (every column with
  RETURNING), so compare with the row's current value: a column equal to its current value is not
  a change. A row whose `id` differs from its current value fails the statement with
  `txfix: id is not updatable`. Validate every row before applying any.
- **Non-transactional attach**: changes apply immediately. A `qty < 0` fails the statement with
  `txfix: qty must be >= 0`.
- **Transactional attach**: changes are **staged** under the plaintext transaction id the function
  sees. Scans of `items` on that attach within that transaction show the staged values on top of
  the committed rows (read-your-writes, including a computed `SET qty = qty + 1` over a staged
  value); other attaches see committed state only. A negative `qty` is accepted at UPDATE time and
  refused by the commit hook. (A transactional attach always has a transaction id: DuckDB wraps
  every statement in one.)
- Write results: `count` mode returns the number of rows updated; `rows` mode returns the new row
  values (user columns only).

### `main.labels`

`id INTEGER NOT NULL PRIMARY KEY`, `label VARCHAR`; rows `(1, 'x')`, `(2, 'y')`. Read-only: no
insert, update or delete function, but the same constraint declarations as `items`.
`UPDATE t.main.labels ...` must fail with the engine's `does not support UPDATE`, not with a
constraint-profile error. (The SDK must negotiate `vgi.constraint.enforce.v1` because `items` is a
writable table with constraints; the extension checks that negotiation before the table's write
modes, so a missing profile shows up on `labels` too.)

## Transaction hooks (transactional attaches only)

- **begin**: mint the transaction id: 16 random bytes. Record event `begin`.
- **commit**: if any row staged by the transaction has `qty < 0`, record event `commit` with
  `ok = false`, discard the staged changes, and FAIL the commit with
  `txfix: commit refused: qty <n> for id <id>` (the first such row by `id`). Otherwise apply the
  staged changes to the committed rows and record `commit` with `ok = true`.
- **rollback**: discard the staged changes, record event `rollback`.
- The hooks must receive the same **plaintext** id the functions see, on every transport
  (including HTTP, where the SDK seals opaque data on the wire).

## Table functions

All VOLATILE, no arguments, in schema `main`.

| Function | Output | Meaning |
|---|---|---|
| `reset()` | `ok BOOLEAN` (one row, `true`) | Restore the partition's initial rows; clear its staged changes and events. |
| `whoami()` | `token VARCHAR, transactional BOOLEAN` (one row) | The attach's options as the function sees them (`token` NULL when absent). |
| `current_tx()` | `in_tx BOOLEAN, tx_id VARCHAR` (one row) | The plaintext transaction id this call received, lowercase hex; `false`/NULL when none. Read it at **process time** (not only at bind). |
| `events()` | `seq BIGINT, kind VARCHAR, tx_id VARCHAR, ok BOOLEAN` | The partition's event log in order (`seq` increasing). `kind` is `begin`, `commit` or `rollback`; `tx_id` lowercase hex; `ok` is `false` only for a refused commit, `true` otherwise. |

## Table in-out function

`tag_tx(TABLE)`: every input column passed through unchanged, plus a last column
`tx_id VARCHAR`: the lowercase hex of the transaction id seen **at process time**, NULL when none.

## Write result modes

DuckDB asks the update function for `count` (plain UPDATE) or `rows` (UPDATE ... RETURNING), never
`changes`: the extension sends only those two (`BuildWriteOptions` in
`src/storage/vgi_physical_write.cpp`). An advertised `changes` is a maximum and satisfies both.
The SQL that would need the OLD row image `changes` carries, `UPDATE ... RETURNING OLD.x`, does not
bind in this engine at all (Binder Error `Referenced table "OLD" not found`, for native tables
too), and the extension would refuse it anyway (`VgiCatalog::PlanUpdate` throws on
`capture_old_rows`). `returning_old.test` pins this, so it fails, and should be turned into real
coverage, once the engine grows OLD/NEW RETURNING.

## What the files cover

| File | Cases |
|---|---|
| `attach_options.test` | `token` redacted in `duckdb_databases()` (literal and expression); `whoami()` sees it on both attaches, in and out of a transaction, `transactional` per attach, NULL token without the option; the token partitions state (write through `t` visible through `ttx`, not through another token) |
| `update.test` | rowid = id (BIGINT, in WHERE); plain UPDATE counts; multi-column/multi-row; no-match; computed SET; SET NULL; RETURNING `*` and expressions; `id` not updatable; `qty >= 0`; refused statements change nothing; `SET id = id` is not a change; INSERT/DELETE not supported on `items`; `labels` "does not support UPDATE", not a constraint-profile error; `reset()` |
| `transactions.test` | BEGIN/UPDATE/RETURNING/read-your-writes/computed SET on staged value/COMMIT, other attach sees committed only; ROLLBACK; refused COMMIT (`commit refused: qty -5 for id 2`) applies nothing and the session recovers; autocommit UPDATE (plain and RETURNING) applies; refused autocommit commit; event log order, `ok` values, distinct lowercase-hex ids, begin first per id |
| `transaction_ids.test` | non-transactional attach: no id in or out of BEGIN, no events, in-out `tx_id` NULL; autocommit statements each get an id that begin minted and commit received; in one transaction `current_tx()` (twice), `tag_tx()` over several rows and the update function share one id, which the commit event carries; the rollback hook gets the id; consecutive transactions differ; both attaches in one statement |
| `returning_old.test` | `RETURNING OLD.x` is a Binder Error on a native table and on both attaches, and changes nothing; with `changes` advertised, RETURNING gets `rows` and plain UPDATE `count` (both attaches) |
