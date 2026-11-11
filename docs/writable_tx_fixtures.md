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

**How the token reaches the fixture.** The fixture obtains `token` (and `transactional`) **only
through the SDK's public attach-options accessor**, the same one that returns non-secret options,
in bind, process and the transaction hooks. It never packs the token into attach bytes of its own.
The SDK, not the author, carries secret option values from ATTACH to the functions: it encrypts
them inside `attach_opaque_data` with a key the client never has (HKDF-derived from the HTTP
signing key, or from a host-local key file on stdio/unix/launch), bound to the attach's id, so the
value never appears in plaintext in any opaque value on any transport (vgi-python
`docs/protocol/vgi-opaque-data-sealing.md` rule 5). `whoami().token_in_attach_data` checks this.

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
| `qty` | BIGINT | DEFAULT `0` |

Initial rows: `(1, 'a', 10)`, `(2, 'b', 20)`, `(3, 'c', 30)`. Scans return rows ordered by `id`.
The `qty` default is declared through the SDK's public column-default API (a constant `0`, served
as `vgi.column.default.v1` metadata); DuckDB fills it in for an omitted column before the insert
function sees the row.

- **Scan function**: `items_scan`, left **hidden**: the table descriptor does not set
  `expose_scan` (the SDK default), so the SDK lists it with visibility `table_only`
  ([function visibility](function_visibility.md)). It is absent from `duckdb_functions()` and
  `SELECT * FROM t.main.items_scan()` fails, yet the table's scans keep its capabilities.
- **Scans read a snapshot**: a scan reads the rows it returns (committed rows plus, on `ttx`, the
  transaction's staged changes) **once, when it starts**, and never re-reads state between the
  batches it emits. `INSERT INTO t.main.items SELECT id + 100, ... FROM t.main.items` and a MERGE
  must not see the rows their own write calls add while the scan runs.
- **Row id**: a BIGINT row-id virtual column whose value is `id`. The scan supports projection
  pushdown (DuckDB reads `rowid` only through it, so a scan whose listing entry were missing
  would fail with "Virtual columns require projection pushdown").
- **Write functions**, all declared on the table descriptor, all table in-out functions, all with
  **`has_finalize`** (see [Write calls and finalize](#write-calls-and-finalize)):
  `items_insert`, `items_update`, `items_delete`. Write functions are always `table_only`: they are
  listed but absent from `duckdb_functions()`. Write result modes for each: advertise the highest
  mode the SDK can produce, at least `rows` (`changes` if supported; see
  [Write result modes](#write-result-modes)).
- **Validation is per batch, before any change**: a write function checks every row of a batch
  before it applies (or stages) any row of that batch. Rows are checked in batch order and the
  first failing row's error is raised. Batches already applied by the same call stay applied on
  the non-transactional attach (the extension sends a large INSERT as several batches of one call).

#### INSERT (`items_insert`)

DuckDB sends every user column (`id`, `name`, `qty`; never `rowid`), with defaults filled in.

- `id` NULL fails with `txfix: id required`; `name` NULL fails with `txfix: name required`.
  (The table declares NOT NULL; the worker, which negotiated `vgi.constraint.enforce.v1`, enforces
  it.) `qty` may be NULL.
- An `id` that already exists fails with `txfix: duplicate id <id>`: an id among the rows the call
  can see (committed rows, plus on `ttx` the transaction's staged inserts minus its staged
  deletes), an id inserted by an earlier batch of the same call, or an id repeated within the
  batch.
- `qty < 0` is refused like UPDATE: immediately on the non-transactional attach
  (`txfix: qty must be >= 0`), at commit on the transactional one.
- Write results: `count` returns the number of rows inserted; `rows` returns the inserted rows
  (user columns only).

#### UPDATE (`items_update`)

- **Updatable**: `name`, `qty`. DuckDB sends the SET columns plus `rowid` (every column with
  RETURNING), so compare with the row's current value: a column equal to its current value is not
  a change. A row whose `id` differs from its current value fails the statement with
  `txfix: id is not updatable`.
- A `qty < 0` fails the statement with `txfix: qty must be >= 0` on the non-transactional attach;
  on the transactional one it is accepted at UPDATE time and refused by the commit hook.
- Write results: `count` mode returns the number of rows updated; `rows` mode returns the new row
  values (user columns only).

#### DELETE (`items_delete`)

DuckDB sends `rowid` only, and rowid = `id`.

- A rowid with no visible row is a no-op and is not counted. A rowid repeated within a call (the
  extension de-duplicates per call, but a fixture must not rely on it) is deleted and counted once.
- Write results: `count` returns the number of rows deleted; `rows` returns the deleted rows as
  they were (user columns only).

#### Transactions

- **Non-transactional attach (`t`)**: every change applies immediately.
- **Transactional attach (`ttx`)**: inserts, updates and deletes are **staged** under the
  plaintext transaction id the function sees. Scans of `items` on that attach within that
  transaction show the staged changes on top of the committed rows (read-your-writes: staged
  inserts are visible, staged deletes hidden, a computed `SET qty = qty + 1` reads a staged
  value); other attaches see committed state only. Updating or deleting a row the same
  transaction inserted edits (or removes) the staged insert; inserting an id the transaction
  deleted is allowed. A negative `qty` (inserted or updated) is accepted at write time and refused
  by the commit hook. Rollback, and a refused commit, discard everything staged. (A transactional
  attach always has a transaction id: DuckDB wraps every statement in one, so an autocommit
  statement is staged and committed by its own transaction.) Conflicts with rows another
  transaction committed after this one staged its changes are not tested; commit may apply the
  staged row images as they are.

### Write calls and finalize

A **write call** is one invocation of a write function: the extension opens one per INSERT,
UPDATE or DELETE statement, one per INSERT/UPDATE/DELETE action of a MERGE (whether or not any
row reaches it), streams zero or more batches into it, and then runs its finalize stage
(`has_finalize`). Finalize emits **no** batches (the per-batch results already carry every count
and row) and records that the call finished: it is what makes the call's `writes()` row
`finalized = true`. A call whose statement failed before finalize never gets `finalized = true`.

`writes()` (see [Table functions](#table-functions)) returns one row per write call of the
partition:

| Column | Meaning |
|---|---|
| `seq` | BIGINT, increasing in the order calls were recorded |
| `op` | `insert`, `update` or `delete` |
| `tx_id` | the call's plaintext transaction id, lowercase hex; NULL on the non-transactional attach |
| `rows` | the number of rows the call reported (the sum of its `count` results, or of the rows it returned in `rows` mode): rows applied on `t`, rows staged on `ttx` |
| `finalized` | true once the call's finalize stage ran |

A fixture records the row when the call starts (bind or first batch) and updates it, or only at
finalize; the tests read only `finalized` rows, so both are conforming. The SDK's per-call state
(not the batch) carries the running count to finalize. `writes()` is not transactional: a rolled
back or refused transaction's calls stay listed. `reset()` clears it.

### `main.labels`

`id INTEGER NOT NULL PRIMARY KEY`, `label VARCHAR`; rows `(1, 'x')`, `(2, 'y')`. Read-only: no
insert, update or delete function, but the same constraint declarations as `items`. Its scan
function is `labels_scan`, with **`expose_scan` set** on the table descriptor (`exposeScan` /
`ExposeScan` in SDKs that spell it so), so it is `public`: listed in `duckdb_functions()` and
callable as `SELECT * FROM t.main.labels_scan()` (no arguments; it returns the table's rows). SDKs whose fixture used other names rename to
these.
`UPDATE t.main.labels ...` must fail with the engine's `does not support UPDATE`, not with a
constraint-profile error; `INSERT` and `DELETE` with `does not support INSERT` /
`does not support DELETE` (also `INSERT ... ON CONFLICT`, which DuckDB rewrites to a MERGE). (The SDK must still negotiate `vgi.constraint.enforce.v1`, because `items`
is a writable table with constraints. Before vgi v2.0 commit "check the declared write before the
constraint-enforcement profile", the extension checked that negotiation first, so a missing profile
showed up on `labels` too; it now reports the undeclared operation first.)

## Transaction hooks (transactional attaches only)

- **begin**: mint the transaction id: 16 random bytes. Record event `begin`.
- **commit**: if any row staged by the transaction (inserted or updated) has `qty < 0`, record event `commit` with
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
| `reset()` | `ok BOOLEAN` (one row, `true`) | Restore the partition's initial rows; clear its staged changes, events and write calls. |
| `whoami()` | `token VARCHAR, transactional BOOLEAN, token_in_attach_data BOOLEAN` (one row) | The attach's options as the function sees them through the SDK's accessor (`token` NULL when absent). `token_in_attach_data`: true iff the token's UTF-8 bytes occur anywhere in the attach opaque bytes the SDK hands the function (the opened inner bytes on HTTP, the raw bytes on stdio/unix); false when there is no token. Tests require it to be false. |
| `current_tx()` | `in_tx BOOLEAN, tx_id VARCHAR` (one row) | The plaintext transaction id this call received, lowercase hex; `false`/NULL when none. Read it at **process time** (not only at bind). |
| `writes()` | `seq BIGINT, op VARCHAR, tx_id VARCHAR, rows BIGINT, finalized BOOLEAN` | One row per write call, see [Write calls and finalize](#write-calls-and-finalize). |
| `events()` | `seq BIGINT, kind VARCHAR, tx_id VARCHAR, ok BOOLEAN` | The partition's event log in order (`seq` increasing). `kind` is `begin`, `commit` or `rollback`; `tx_id` lowercase hex; `ok` is `false` only for a refused commit, `true` otherwise. |

## Table in-out function

`tag_tx(TABLE)`: every input column passed through unchanged, plus a last column
`tx_id VARCHAR`: the lowercase hex of the transaction id seen **at process time**, NULL when none.

## Write result modes

DuckDB asks a write function for `count` (no RETURNING) or `rows` (RETURNING), never
`changes`: the extension sends only those two (`BuildWriteOptions` in
`src/storage/vgi_physical_write.cpp`). An advertised `changes` is a maximum and satisfies both.
Since protocol 3.3 the write-options batch carries only `result_mode`; the former `on_conflict`
and `on_conflict_columns` fields are gone (they could only ever say `throw` / empty, see below).
The SQL that would need the OLD row image `changes` carries, `UPDATE ... RETURNING OLD.x`, does not
bind in this engine at all (Binder Error `Referenced table "OLD" not found`, for native tables
too), and the extension would refuse it anyway (`VgiCatalog::PlanUpdate` throws on
`capture_old_rows`). `returning_old.test` pins this, so it fails, and should be turned into real
coverage, once the engine grows OLD/NEW RETURNING.

## MERGE and INSERT ... ON CONFLICT

DuckDB's binder rewrites every `INSERT ... ON CONFLICT` (DO NOTHING, DO UPDATE, `INSERT OR
REPLACE`) into a MERGE before any catalog hook runs, so a VGI worker never sees a conflict
clause. The extension **emulates** MERGE with the table's ordinary write functions: one write
call per INSERT/UPDATE/DELETE action of the MERGE (a MERGE with `WHEN MATCHED THEN UPDATE` and
`WHEN NOT MATCHED THEN INSERT` makes two calls), fed from a scan of the target joined with the
source. Consequences a fixture (and a user) sees:

- MERGE needs a row id: on a table without one it is a Binder Error at planning
  (`merge_no_rowid.test`). Conflict targets need the table's declared PRIMARY KEY / UNIQUE.
- The calls are not atomic together. On a non-transactional attach a failure in one action
  (for example a `WHEN ... THEN ERROR` clause, or a refused row) leaves what earlier calls and
  batches applied; on a transactional attach the whole MERGE is staged in the statement's
  transaction and discarded with it.
- `UPDATE` actions send their computed SET values (already evaluated and cast to the column
  types) plus `rowid`, exactly like a plain UPDATE; `INSERT` actions send full rows, exactly like
  a plain INSERT; `DELETE` actions send `rowid`.

## `simple_writable`: created tables (CTAS)

`simple_writable/ctas*.test` hold each SDK's `simple_writable` fixture (the per-attach in-memory
writable worker, `VGI_SIMPLE_WRITABLE_WORKER`) to this addition:

- `catalog_table_create` in schema `main` creates a table with the request's columns (at least
  INTEGER, BIGINT, VARCHAR, DOUBLE, BOOLEAN), in the attach's state next to the fixed tables. The
  created table is scannable (its scan `table_only`), has an **insert** function (result modes up
  to `rows`), and needs no row id, update or delete function.
- The reply's `created` (protocol 3.3, `bool not null`): **true** when this call created the
  table (including `REPLACE`, which drops the old table and its rows first), **false** when
  `IGNORE` (`IF NOT EXISTS`) found an existing table, which stays exactly as it was. `ERROR` on an
  existing name fails with a message containing `already exists`.
- `catalog_table_drop` drops a created table (`ignore_not_found` honoured).

The extension uses `created`: CTAS inserts nothing when it is false, and when a CTAS fails after
creating the table on a non-transactional attach, the extension drops the table again (best
effort, logged as `vgi.ctas.drop_after_failure`). `CREATE OR REPLACE TABLE ... AS` cannot restore
the replaced table if the statement then fails: the old table is already gone.

## What the files cover

| File | Cases |
|---|---|
| `attach_options.test` | `token` redacted in `duckdb_databases()` (literal and expression); `whoami()` sees it on both attaches, never in plaintext in the attach bytes (`token_in_attach_data` false), in and out of a transaction, `transactional` per attach, NULL token without the option; the token partitions state (write through `t` visible through `ttx`, not through another token) |
| `update.test` | rowid = id (BIGINT, in WHERE); plain UPDATE counts; multi-column/multi-row; no-match; computed SET; SET NULL; RETURNING `*` and expressions; `id` not updatable; `qty >= 0`; refused statements change nothing; `SET id = id` is not a change; `SET qty = DEFAULT`; `labels` "does not support INSERT/UPDATE/DELETE", not a constraint-profile error; `reset()` |
| `transactions.test` | BEGIN/UPDATE/RETURNING/read-your-writes/computed SET on staged value/COMMIT, other attach sees committed only; ROLLBACK; refused COMMIT (`commit refused: qty -5 for id 2`) applies nothing and the session recovers; autocommit UPDATE (plain and RETURNING) applies; refused autocommit commit; event log order, `ok` values, distinct lowercase-hex ids, begin first per id |
| `transaction_ids.test` | non-transactional attach: no id in or out of BEGIN, no events, in-out `tx_id` NULL; autocommit statements each get an id that begin minted and commit received; in one transaction `current_tx()` (twice), `tag_tx()` over several rows and the update function share one id, which the commit event carries; the rollback hook gets the id; consecutive transactions differ; both attaches in one statement |
| `scan_function_listing.test` | protocol 3.3 visibility: `items_scan` and `items_update` absent from `duckdb_functions()`; `SELECT * FROM t.main.items_scan()` fails; `rowid`, `count(*)` and `UPDATE ... RETURNING` still work on `items` (the hidden entries keep their capabilities); `labels_scan` listed and callable |
| `returning_old.test` | `RETURNING OLD.x` is a Binder Error on a native table and on both attaches, and changes nothing; with `changes` advertised, RETURNING gets `rows` and plain UPDATE `count` (both attaches) |
| `insert.test` | VALUES, multi-row, omitted `qty` takes the default, explicit NULL `qty`; RETURNING `*` and expressions; duplicate id (committed, within the statement, within a batch) and NULL `id`/`name` fail and insert nothing; `qty >= 0`; inserted rows have rowid = id and can be updated and deleted |
| `insert_select.test` | `INSERT ... SELECT` from `range(5000)` (several batches, one write call, one `writes()` row with `rows = 5000`); a self-referencing `INSERT INTO items SELECT ... FROM items` sees only the snapshot; a duplicate in a later batch fails the statement: earlier batches stay on `t`, nothing on `ttx` |
| `delete.test` | DELETE by id, by predicate, no match (count 0), all rows; deleted rows gone from scans; re-insert of a deleted id; `writes()` counts |
| `delete_returning.test` | DELETE ... RETURNING `*` and expressions on both attaches (gated on `VGI_ENGINE_DELETE_RETURNING_VIRTUAL_COLUMNS`) |
| `delete_using_dupes.test` | `DELETE ... USING` where a target row matches several source rows: each rowid is sent once and counted once, RETURNING lists each deleted row once |
| `insert_delete_tx.test` | `ttx`: staged INSERT/DELETE with read-your-writes, other attach sees committed only; UPDATE and DELETE of a staged insert; delete then re-insert of an id; ROLLBACK discards; refused COMMIT (inserted `qty < 0`) discards everything; autocommit INSERT/DELETE apply; duplicate against a staged insert |
| `merge.test` | `t`: every MERGE clause kind (MATCHED UPDATE/DELETE, with `AND` conditions and computed SET, NOT MATCHED INSERT, NOT MATCHED BY SOURCE UPDATE/DELETE, DO NOTHING, ERROR); stored values checked after each; `writes()` shows one call per action; ERROR clause: the statement fails, earlier calls of the MERGE stay applied |
| `merge_tx.test` | the same MERGE shapes on `ttx`: staged, read-your-writes, ROLLBACK and a failed MERGE discard everything, COMMIT applies |
| `merge_returning.test` | MERGE ... RETURNING `merge_action` and columns, for INSERT, UPDATE and DELETE actions |
| `on_conflict.test` | `INSERT ... ON CONFLICT DO NOTHING` with and without a target; `DO UPDATE SET qty = excluded.qty`; `DO UPDATE SET qty = items.qty + excluded.qty`; `DO UPDATE ... WHERE`; `INSERT OR REPLACE`; RETURNING; stored values asserted after every statement (regression test for the MERGE value-mapping bug); on `labels` "does not support INSERT" |
| `finalize.test` | every write call (INSERT, UPDATE, DELETE, each MERGE action, empty calls) is finalized exactly once, with the right `op`, `rows` and `tx_id`; a failed statement adds no finalized call |
