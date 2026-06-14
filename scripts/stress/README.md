# VGI stress harness

Concurrency stress for the two parts of VGI that the standard test suite can't
exercise under real load:

- the **per-process subprocess worker pool** (acquire/release/cleanup races,
  EPIPE stale-connection recovery, `pool_max` enforcement), and
- the **launcher** (`launch:` cross-process coordination — one warm worker
  shared system-wide via per-hash flock + AF_UNIX socket).

DuckDB's `.test` format has no multi-process / multi-connection primitive, so
this lives outside the sqllogittest suite. It's driven by
[`haybarn-python`](/Users/rusty/Development/haybarn-python) (the Query Farm
DuckDB distribution that ships VGI as a signed community extension) and loads the
real production artifact via `INSTALL vgi FROM community; LOAD vgi`.

## Running

Use the **haybarn venv python**, from the repo root:

```bash
PY=/Users/rusty/Development/haybarn-python/.venv/bin/python

# Correctness + fault injection (the bug-finding mode)
$PY -m scripts.stress --target both --mode correctness --concurrency 8 --queries 50

# Throughput ramp -> CSV
$PY -m scripts.stress --target both --mode throughput --concurrency 1,2,4,8,16 --csv /tmp/vgi-stress.csv

# Soak (watch for FD/worker/zombie leaks; one-worker-per-hash invariant)
$PY -m scripts.stress --target both --mode soak --duration 120 --concurrency 8
```

Exit code is non-zero if any assertion fails, so it composes with `&&` / CI.

## What each mode asserts

| mode | pool | launcher |
|------|------|----------|
| `correctness` | N threads (own connections) + parallel `SET threads` scans + attach churn all return the deterministic result; pool size never exceeds `pool_max`. **Faults:** Phase A kills every worker then proves a clean query recovers and `misses` grows (EPIPE retry); Phase B sustains load under a periodic kill storm and requires every thread to finish. | N spawned **processes** race the same `launch:` LOCATION and must coordinate to **exactly one** socket/worker; divergent `launcher_idle_timeout` in one process raises `BinderException`. **Faults:** SIGKILL the bound listener mid-flight (recover via cache invalidation), unlink the socket (reprobe + respawn), and idle-shutdown + re-attach. |
| `throughput` | qps, p50/p95/p99 latency, and pool hit-rate vs concurrency. | cold-vs-warm attach cost and query-phase qps (measured inside the child, excluding process spawn / haybarn startup). |
| `soak` | sustained load with a small `pool_timeout` so the cleanup thread constantly evicts; samples FD count / worker count / zombies and flags unbounded growth. | sustained multi-process load; asserts at most one socket per LOCATION hash and reaps workers on teardown. |

## Notes

- **Fault injection is external SIGKILL.** The fixture `crash_on_process` hook
  doesn't cleanly kill against the community extension, so faults kill worker
  PIDs directly (`vgi_worker_pool()` for subprocess; `lsof -t
  <hash>.sock` for launcher). Transport-agnostic and more robust.
- **Isolation.** Launcher runs use a private temp `launcher_state_dir`, and
  every worker bound to it is killed on teardown — the harness never touches a
  real warm worker. Subprocess workers self-reap when the harness exits.
- **Knobs.** `--worker '<cmd>'` to point at a different VGI worker;
  `VGI_STRESS_EXTENSION_PATH=<.duckdb_extension>` to `LOAD` a local build
  instead of the community artifact; `VGI_STRESS_VERBOSE=1` to log
  retry-exhaustion detail; `--no-faults` for happy-path load only.
- No third-party deps beyond haybarn — resource probes shell out to
  `pgrep` / `lsof` / `ps`.
