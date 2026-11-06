# Building against upstream DuckDB 2.0

VGI's `v2.0` branch builds against two engines from the same source:

| Engine | Source | httpfs | Shell |
|--------|--------|--------|-------|
| **Haybarn** (default) | the committed `duckdb` submodule pin | `Query-farm-haybarn/haybarn-httpfs` | `build/<mode>/haybarn` |
| **Upstream DuckDB v2.0.0** | `duckdb/duckdb` at `d54cbbe3df17` | `duckdb/duckdb-httpfs` at `77392a72` (upstream's own v2.0.0 pin) | `build/<mode>/duckdb` |

## Selecting the engine

Like any DuckDB extension, VGI builds against whatever the `duckdb` submodule has
checked out:

```bash
scripts/use_duckdb_engine.sh duckdb      # upstream v2.0.0 (prints the OVERRIDE_GIT_DESCRIBE to use)
rm -rf build
CC=clang CXX=clang++ USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja OVERRIDE_GIT_DESCRIBE=v2.0.0 make release

scripts/use_duckdb_engine.sh haybarn     # back to the committed pin
scripts/use_duckdb_engine.sh status
```

Selecting upstream leaves the submodule modified in `git status`. Don't commit
that change. The CMake cache records the engine it was configured against, so
remove `build/` after switching. Better still, keep a separate checkout or
worktree for each engine.

The upstream build's merged vcpkg manifest uses a newer `builtin-baseline`
(`cd61e1e2…`) than the Haybarn build, so its vcpkg checkout must contain that
commit.

## How the selection works

- **CMake:** `extension_config.cmake` checks for `src/include/duckdb/haybarn.h`
  in the engine tree, which only Haybarn ships. It then pins the matching httpfs
  fork and logs `VGI: building against the <haybarn|duckdb> engine`.
- **C++:** `src/include/vgi_engine_compat.hpp` includes `duckdb/haybarn.h` when
  `__has_include` finds it. It defines one `VGI_ENGINE_HAS_*` macro per engine
  divergence. Code gates on those macros, never on `HAYBARN` directly. A Haybarn
  engine with `HAYBARN_ABI_VERSION < 2` is a compile error. CMake passes its own
  decision down as `VGI_ENGINE_FLAVOR_HAYBARN=0|1`, and the header fails the
  build if the two disagree, e.g. when a stray `duckdb/haybarn.h` on another
  include path shadows the engine tree's.
- **SQL:** `vgi_engine_features()` reports what the build was compiled with.
- **Tests:** standard sqllogictest directives. A file that needs a feature
  has `require-env VGI_ENGINE_<FEATURE> 1`. A file that covers the behavior
  *without* it has `require-env-not VGI_ENGINE_<FEATURE>`, with no value: the
  form with a value skips when the variable is unset. The top-level Makefile
  sets each variable to `1` for a Haybarn engine tree, using the same
  `haybarn.h` check, and leaves it unset otherwise. It exports them, so every
  make target sees them, including extension-ci-tools' `make test`, whose
  runner passes its environment through to `unittest`. Anything that runs
  the tests outside make loads them from `make -s --no-print-directory
  engine_test_env`, which prints `NAME=1` lines (nothing on upstream):
  - In a shell: `set -a; eval "$(make -s --no-print-directory engine_test_env)"; set +a`.
  - `test/run_http_integration.sh` and `test/run_unix_integration.sh` load
    them themselves, so a direct run matches `make test_http` / `make test_unix`.
  - `integration.yml` appends them to `$GITHUB_ENV` right after checkout. Its
    direct `run_tests.py` steps and shell scripts don't go through make.
  - `scripts/ci_fetch_native_runner.py` records them as `engine_test_env` in the
    native runner's `manifest.json`, and exports them when an SDK job fetches
    that runner. The manifest also records the `engine` (`haybarn` or `duckdb`)
    and the revision actually checked out in `duckdb/`.

  Each variable is `1` or unset. Any other value, empty included, would make
  both sides of a split skip, so the Makefile stops on it.
- **File layout:** a test file whose coverage needs a feature is split. Its
  engine-independent cases stay in the ungated file, and only the cases that
  need the feature go in a gated sibling (`*_named_args.test`,
  `delete_returning.test`). The ungated file reaches the same feature by other
  means where it can, e.g. a correlated call that relies on a named argument's
  default.
- **Allow-lists and floors:** `make` lanes that pass `--allow-skip` add
  `VGI_ENGINE_EXPECTED_SKIPS`: the `require-env-not` reasons on Haybarn, the
  `require-env … 1` reasons on upstream. On upstream, `--min-executed` floors
  are lowered by the number of gated files (`VGI_ENGINE_FLOOR_SLACK`). SDK
  repositories that run these files with their own allow-lists need the same
  reasons. They pin `VGI_REF` to an exact commit, so nothing changes for them
  until that pin moves. The change that bumps `VGI_REF` past this feature must
  add `--allow-skip 'require-env-not VGI_ENGINE_CORRELATED_NAMED_ARGS'` and
  `--allow-skip 'require-env-not VGI_ENGINE_DELETE_RETURNING_VIRTUAL_COLUMNS'`
  (and the matching `EXPECTED_SKIP_REASONS` entries in `ci/run-integration.sh`).
- **Drift guards:** `test/sql/engine_features/` holds one file per feature and
  state. Each checks that the variable matches `vgi_engine_features()`. They
  need no worker, so they run in every lane, including `make test_http` and
  `make test_unix`. A missing variable on Haybarn, or
  a stray `=1` on upstream, fails there instead of silently skipping (or wrongly
  running) the gated files.

| Variable | Feature |
|---|---|
| `VGI_ENGINE_CORRELATED_NAMED_ARGS` | `correlated_named_args` |
| `VGI_ENGINE_DELETE_RETURNING_VIRTUAL_COLUMNS` | `delete_returning_virtual_columns` |

## What differs on upstream DuckDB v2.0.0

| Feature (`vgi_engine_features()`) | Upstream change | Without it |
|---|---|---|
| `http_cancellation` | Haybarn `HTTPParams::interrupt_context` (no upstream equivalent) | An interrupted query waits for its in-flight HTTP request to finish or time out, then reports the interrupt. |
| `http_params_clone` | duckdb/duckdb#28088 | Requests use the catalog's cached `HTTPParams` directly and never mutate them. Every single-attempt (non-idempotent) method is a catalog RPC, which gets the catalog's dedicated `retries = 0` snapshot. A single-attempt request arriving with a retrying snapshot is a caller bug and fails with an internal error before dispatch, rather than rebuilding parameters (that would re-enter the secret manager, duckdb#22258). |
| `delete_returning_virtual_columns` | duckdb/duckdb#28089 | `DELETE … RETURNING` is refused at plan time, before any row is deleted: the engine binds a `rowid` the worker's response never carries. Plain `DELETE` works. |
| `runtime_filter_pushdown` | duckdb/duckdb#28090 | Results are unchanged. A hash join that picks a Bloom or prefix-range filter pushes no min/max bounds to a VGI scan, so the worker can fetch more rows (duckdb/duckdb#28091). |
| `correlated_named_args` | duckdb/duckdb#27833 (merged to `main` after v2.0.0) | Named arguments in correlated calls (`FROM t, LATERAL f(t.x, opt := 1)`) fail to bind ("No function matches"). The literal form `f(1, opt := 1)` works. |

## C++ unit tests

- `vgi_http_params_clone_tests` is configured only against Haybarn; it tests
  `HTTPParams::Clone` and `interrupt_context`, which upstream lacks.
- `vgi_http_interrupt_tests` builds on both. Its two in-flight-abort cases need
  `VGI_ENGINE_HAS_HTTP_CANCELLATION` and are compiled out on upstream. Its
  no-replay case expects one dispatch on Haybarn (a per-request clone with
  `retries = 0`) and none on upstream (refused before dispatch).

## Publishing to the Query.Farm repository

`.github/workflows/QueryFarmDeploy.yml` builds this source against upstream
DuckDB v2.0.0 (linux_amd64, linux_arm64, osx_arm64, windows_amd64), runs the
extension tests, then signs the binaries and uploads them to
`https://duckdb-extensions.query.farm`. Like the other Query.Farm extensions, it
publishes on every push to `v2.0` once the build and tests pass; a manual
dispatch republishes the branch head (`-f dry_run=true` signs and verifies but
uploads nothing). Users install it with:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
```

The DuckDB community repository carries only a placeholder `vgi` (branch
`v2.0-community-stub`) that points users here, or loads this build once it is
installed. Haybarn builds come from `MainDistributionPipeline.yml`, which does
not publish to this repository.

## When upstream catches up

When an upstream release ships one of these, switch its macro in
`vgi_engine_compat.hpp` to a version check, and update the matching Makefile
variable. Remove both once both engines have the feature.

For correlated named arguments, `test/sql/integration/table_in_out/correlated_named_args_unsupported.test`
is the canary: it runs only without the feature and asserts that the binder
still rejects such a call. Once upstream ships duckdb/duckdb#27833 it fails,
so the `*_named_args.test` files don't keep skipping unnoticed.
