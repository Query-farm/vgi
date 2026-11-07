# Secret-lookup conformance fixtures

The files `test/sql/integration/secret/secret_lookup_*.test` check, against every SDK's example
worker, that a secret created with `CREATE SECRET` reaches the worker function that declared it.
This page is the contract those files hold each example worker to. An SDK implements the five
table functions below in its example worker (the one `VGI_TEST_WORKER` points at) and then opts in.

## Why these exist

At bind the extension sends the resolved secrets as a one-row Arrow batch with one struct column per
secret, **named by the secret's unique DuckDB name**, not its type (`ExtractVgiSecrets` /
`BuildSecretsBatch`; layout in vgi-python `docs/protocol/vgi-secrets.md`). An unnamed
`CREATE SECRET (TYPE brave, ...)` is named `__default_brave`. Two SDK bugs left twelve fleet workers
without credentials:

1. **By-type lookup done as a column-name lookup.** `get("brave")` looked for a column called
   `brave`, so it found the secret only when the user happened to name it after its type.
2. **Silently dropped declarations.** A requirement written in a form the SDK did not recognise
   (vgi-python: `required_secrets = ["brave"]`) was discarded, so the function never asked for the
   secret and the client never sent it.

The fixtures pin both down, plus same-type scope selection, multi-type delivery, required/optional
absence and redaction.

## Opting in

Every file is gated on two variables:

```
require-env VGI_TEST_WORKER
require-env VGI_SECRET_LOOKUP_FIXTURES
```

Set `VGI_SECRET_LOOKUP_FIXTURES=1` in the SDK's `ci/run-integration.sh` once its example worker
implements all five functions. Until then the files skip with the reason
`require-env VGI_SECRET_LOOKUP_FIXTURES`; harnesses that enforce a skip allow-list
(`EXPECTED_SKIP_REASONS`) must list that reason, with a comment, until the fixtures land, and must
remove it in the commit that sets the variable. The vgi `Makefile` exports it (default `1`, for the
vgi-python worker) and so does the vgi `integration.yml` suite step.

Status: every SDK implements the fixtures and opts in.

| SDK | Fixtures | From |
|---|---|---|
| vgi-python | `vgi/_test_fixtures/secret_lookup.py` | 0.49.2 |
| vgi-typescript | `examples/secret_lookup.ts` | 0.48.3 |
| vgi-go | `examples/table/secret_lookup.go` | v0.40.2 |
| vgi-rust | `vgi-example-worker/src/secret_lookup.rs` | 0.49.4 |
| vgi-java | `vgi-example-worker/.../table/SecretLookupFixtures.java` | 0.46.2 |
| vgi-csharp | `fixtures/QueryFarm.Vgi.ExampleWorker/Table/SecretLookupFixtures.cs` | 0.24.1 |

## Common rules

- All five are **table functions** in the example worker's default catalog (called as
  `example.<name>(...)` after `ATTACH ... AS example`).
- They read secrets of type **`vgi_example`**, the type the example worker already registers
  (fields `secret_string` and `api_key` marked redacted, `port`, `use_ssl`, `timeout`).
  `secret_types_seen` also requires DuckDB's built-in **`s3`** type (from httpfs).
- **Requirements are declared statically** (in function metadata, `required_secrets`) wherever this
  page says *static*. Those functions MUST NOT also request secrets dynamically (two-phase bind):
  the static declaration is what is under test, and a dynamic request would mask a dropped one.
- Where this page says *simplest form*, declare the requirement in the shortest form the SDK offers
  for "a secret of type T" (vgi-python: the bare string `"vgi_example"`; TypeScript:
  `requiredSecrets: ["vgi_example"]`). If the SDK has only one form, use it.
- Select secrets with the SDK's public selection API (by type / by name / by scope), not by indexing
  the batch: the point is to exercise what worker authors call.
- **Value functions** return exactly **one row**, always, with columns

  | Column | Type | Content |
  |---|---|---|
  | `found` | BOOLEAN | Whether a secret was selected. |
  | `secret_name` | VARCHAR | The selected secret's `name` field; NULL when not found. |
  | `value` | VARCHAR | The selected secret's `field` rendered as text; NULL when not found or when the secret has no such field. |

  Render strings as-is, booleans as `true`/`false`, numbers in plain decimal. The tests only read
  VARCHAR fields, so other renderings are not asserted.
- A requirement that resolves nothing is not an error (except for `secret_value_required`): the
  function returns `found = false`.

## The functions

### `secret_value_by_type(field VARCHAR)`

- Requirement (static, simplest form): type `vgi_example`, no name, no scope.
- Selection: by **type** `vgi_example` — an unscoped secret of that type, else the first one.
- Output: one value row for `field`.
- Pins: an unnamed `__default_vgi_example` secret and a secret named `conformance_creds` are both
  found by type; with neither, `found = false`; a scoped-only secret does not satisfy it.

### `secret_value_required(field VARCHAR)`

- Requirement and selection: as `secret_value_by_type`.
- When no `vgi_example` secret was resolved, the **bind fails**. Use the SDK's own "required"
  mechanism if it has one (vgi-python `secrets.get(type, required=True)`, Go `Secrets.Require`);
  otherwise raise from bind. The error text MUST contain `vgi_example` and MUST NOT contain any
  secret value.
- Otherwise: one value row for `field`.

### `secret_value_by_name(name VARCHAR, field VARCHAR)`

- Requirement: dynamic (two-phase bind) — at bind, request type `vgi_example` with secret name
  `name`, no scope.
- Selection: by **name** `name`, and only if that secret's `type` is `vgi_example`.
- Output: one value row for `field`. A missing name, or a name whose secret has another type, is
  `found = false`.

### `secret_value_for_scope(path VARCHAR, field VARCHAR)`

- Requirements (static), all of type `vgi_example`:
  1. no scope (simplest form),
  2. scope `vgi-conformance://a/`,
  3. scope `vgi-conformance://a/b/`.

  The client resolves each to the best-matching secret, so up to three secrets of one type arrive
  together, each in its own column.
- Selection: worker-side, among the delivered `vgi_example` secrets, the one whose `scope` field
  (newline-separated prefixes) has the **longest prefix of `path`**; failing that, one with an empty
  scope; failing that, none. (vgi-python `ResolvedSecrets.for_scope_of_type`.)
- Output: one value row for `field`.

### `secret_types_seen()`

- Requirements (static): type `vgi_example` with no scope (simplest form), and type `s3` with scope
  `s3://vgi-conformance/`. (DuckDB's `s3` secrets default to the scope `s3://`, so an unscoped `s3`
  lookup would never match one.)
- Output: **one row per secret delivered** (zero rows when none), columns `secret_name VARCHAR`
  (the column/secret name), `secret_type VARCHAR` (its `type` field), `scope VARCHAR` (its `scope`
  field, empty string when unscoped). Row order is free; the tests sort.

## What the files cover

| File | Cases |
|---|---|
| `secret_lookup_by_type.test` | optional absent → `found=false`; required absent → error naming the type; unnamed `__default_vgi_example` by type; named-unlike-type by type; missing field → NULL; drop → absent again; scoped-only secret does not satisfy an unscoped requirement |
| `secret_lookup_by_name.test` | two same-type secrets, each by its name; unnamed secret by its default name; missing name; name of another type |
| `secret_lookup_scopes.test` | nested scopes, longest prefix wins; unscoped fallback; no match without fallback; fall to outer scope when inner dropped |
| `secret_lookup_multi_type.test` | none → 0 rows; one type; both types delivered; an out-of-scope `s3` secret is not delivered |
| `secret_lookup_redaction.test` | the worker receives the values; redacted fields are redacted in `duckdb_secrets()`; no value in `EXPLAIN`, in the required-secret error, or in any non-QueryLog `duckdb_logs()` record |
