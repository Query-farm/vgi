# Launcher ATTACH-Option Reference

Per-LOCATION ATTACH options for the AF_UNIX worker launcher.  Pairs
with [`docs/launcher-tutorial.md`](launcher-tutorial.md) (the
introductory walkthrough) and [`docs/launcher-protocol.md`](launcher-protocol.md)
(the cross-language wire contract).

All options apply only to `launch:` LOCATIONs.  ATTACHing with these
options on `http://`, `unix://`, or bare-subprocess LOCATIONs raises
`BinderException` at parse time.

## Options

### `launcher_idle_timeout`

| Property | Value |
|---|---|
| **Type** | `BIGINT` (seconds) |
| **Range** | `>= 0` |
| **Default** | 300 (uses `LaunchConfig::idle_timeout` baked into the launcher binary) |
| **Special value** | `0` = run forever (worker never self-shuts-down) |

Forwarded into the worker's `--idle-timeout` argv.  Workers
self-shutdown when zero clients have been connected for the requested
period.  Setting this short (e.g. 30) is good for development /
short-lived deployments; setting it long (e.g. 3600) keeps a JVM warm
across an entire workday.

```sql
ATTACH 'mycat' AS svc (TYPE vgi,
    LOCATION 'launch:uv run --project /opt/my-worker my-worker-cli',
    launcher_idle_timeout 60);
```

### `launcher_state_dir`

| Property | Value |
|---|---|
| **Type** | `VARCHAR` (filesystem path) |
| **Range** | non-empty |
| **Default** | OS-derived: `$XDG_RUNTIME_DIR/vgi-rpc` on Linux when set, else `$TMPDIR/vgi-rpc-$UID/` |

Override where the launcher places lockfiles, sockets, and `.meta`
files.  The directory is created mode 0700 if missing.

**This is an escape valve, not an isolation primitive.**  Two
ATTACHes with the same `launch:` argv but different
`launcher_state_dir` values DO spawn duplicate workers (no flock
cross-coordination because their lockfiles live in different
directories).  If you need per-tenant isolation, differentiate the
worker argv (or `VGI_RPC_*` env, which IS part of the launcher hash),
not the state dir.

Use cases:

- **Sandboxing** — point at a tmpfs that's wiped between test runs.
- **Non-default `$TMPDIR`** — avoid colliding with a system-wide
  /tmp on a shared host.
- **Filesystem boundary** — keep launcher state on a different mount.

```sql
ATTACH 'mycat' AS svc (TYPE vgi,
    LOCATION 'launch:uv run --project /opt/my-worker my-worker-cli',
    launcher_state_dir '/srv/myapp/launcher-state');
```

## Validation rules

| Rule | Error |
|---|---|
| `launcher_idle_timeout < 0` | `BinderException: launcher_idle_timeout must be >= 0` |
| `launcher_state_dir = ''` (empty string) | `BinderException: launcher_state_dir, if set, must not be empty` |
| Either option used with non-`launch:` LOCATION | `BinderException: launcher_idle_timeout / launcher_state_dir are only valid for ``launch:`` LOCATIONs` |

## Conflict semantics

The launcher caches the resolved socket path per-process keyed on the
LOCATION string.  Each cache entry also records the `LaunchOverrides`
that were used at first spawn.  A subsequent ATTACH that hits the
same cache entry with *different* overrides fails at parse time:

```
Binder Error: vgi launcher: this LOCATION is already attached with
launcher overrides [idle_timeout=60000ms]; cannot reattach with
[idle_timeout=120000ms].  Detach the existing catalog first, or wait
for the worker's idle-shutdown so a fresh ATTACH can install new
overrides.
```

The pin is reset when the cache entry is invalidated (worker
idle-shut-down, manual `vgi_clear_cache()`, or connect-failure
retry).  The next ATTACH after invalidation can install fresh
overrides.

This is the design choice the senior-review pass landed on: "first
ATTACH wins" is fail-loud rather than silent — overrides are a
property of the worker's identity, not of the connection, so two
clients can't disagree.

## Inspection

### Where the state lives

| Platform | Default | Override |
|---|---|---|
| Linux with `$XDG_RUNTIME_DIR` set | `$XDG_RUNTIME_DIR/vgi-rpc/` | `launcher_state_dir` ATTACH option |
| Linux without `$XDG_RUNTIME_DIR` | `$TMPDIR/vgi-rpc-$UID/` (or `/tmp/vgi-rpc-$UID/`) | same |
| macOS | `$TMPDIR/vgi-rpc-$UID/` | same |
| Windows / WASM | unsupported | — |

### Files per worker

| File | Contents | Mode |
|---|---|---|
| `<hash>.lock` | Empty file used as `flock(2)` target | 0600 |
| `<hash>.sock` | AF_UNIX SOCK_STREAM the worker listens on | 0600 |
| `<hash>.meta` | JSON: `{cmd, cwd, started_at, launcher_pid, socket}` | 0600 |

`<hash>` is the first 16 hex chars of `sha256(canonical_json({cmd,
cwd, env_subset}))`.  See `docs/launcher-protocol.md` for the exact
canonicalisation rules.

### `.meta` example

```json
{
  "cmd": ["uv", "run", "--project", "/opt/my-worker", "my-worker-cli"],
  "cwd": "/home/me",
  "started_at": 1714940000.0,
  "launcher_pid": 12345,
  "socket": "/run/user/1000/vgi-rpc/abc1234567890.sock"
}
```

`started_at` is unix epoch seconds; subtract from `now()` to learn
the worker's age.

### Listing live workers

There's no SQL function for this today — the launcher's pool is
process-local plus disk-backed.  For ops dashboards, walk the state
directory:

```bash
# How many warm workers are out there?
$ ls $XDG_RUNTIME_DIR/vgi-rpc/*.sock | wc -l

# What are they?
$ for m in $XDG_RUNTIME_DIR/vgi-rpc/*.meta ; do
    cat "$m" | jq '.cmd | join(" ")'
  done

# Who's using them?
$ for s in $XDG_RUNTIME_DIR/vgi-rpc/*.sock ; do
    echo "$s →"
    lsof -t "$s" 2>/dev/null | head
  done
```

`vgi_worker_subprocess_pool()` deliberately returns no rows for these workers —
see [`CLAUDE.md`](../CLAUDE.md) "Transports" section for why.

## Common pitfalls

- **`launch:` is rejected at ATTACH** — confirm `LOAD vgi;` succeeded
  and the build isn't WASM (where launchers are unsupported by design).
- **Worker startup timeout** — JVM cold-start over 60 s will trip the
  launcher's worker-startup-timeout (currently hardcoded to 60 s in
  the launcher binary, not user-tunable yet).  Workaround: use
  `unix://` and start the JVM out-of-band.
- **Worker stderr is currently discarded.**  No ATTACH option for
  capture yet — the senior-review pass dropped `launcher_worker_stderr`
  because per-spawn-shared workers can't honour per-connection
  stderr destinations without breaking on respawn.  For debugging,
  use `VGI_WORKER_DEBUG=1` (env var; passes through to the launched
  worker via `VGI_RPC_*` env hashing — but be aware this puts you on
  a *different* worker hash from non-debug callers).

## See also

- [`docs/launcher-tutorial.md`](launcher-tutorial.md) — narrative walkthrough
- [`docs/launcher-protocol.md`](launcher-protocol.md) — cross-language wire contract
- `CLAUDE.md` — `vgi_worker_subprocess_pool()` caveats, ATTACH option summary table
