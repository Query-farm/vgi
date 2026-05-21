# Using the Launcher: a tutorial

This is the user-facing tutorial.  If you're porting the launcher to
another language, see `docs/launcher-protocol.md` instead.  If you're
looking for the per-option reference, see `docs/launcher-options.md`.

## What it is, in one sentence

The launcher lets a single long-running Python (or JVM, or Go) worker
process serve every DuckDB instance that points at it, so cold-start
costs are paid once per worker — not once per query, not once per
DuckDB process.

## Why you might want it

- **JVM workers cost 2–5 seconds to start.** Spawning one per query is
  unaffordable. A pool of one-per-DuckDB-process workers helps within
  a session, but each new DuckDB instance pays the cost again.  The
  launcher amortises across DuckDB instances.
- **Python workers** with heavy imports (numpy/pyarrow/torch) take
  hundreds of milliseconds to start.  Same story.
- **One shared interpreter** uses far less memory than N copies — the
  test suite proved this with a measured **~5× wall-clock speedup**
  over the per-process subprocess pool (138/138 integration tests in
  ~38 s instead of ~187 s).

## The four LOCATION schemes

The vgi extension picks a transport from the prefix of `LOCATION`:

| LOCATION | Transport | When to use |
|---|---|---|
| `/path/to/worker` | **subprocess** | One worker per DuckDB process, pooled. The original transport.  Good for one-shot CLIs and scripts. |
| `http://host:port/` | **HTTP** | Worker serves on TCP. Operator-managed. Cross-host, multi-tenant. |
| `unix:///path/to/sock` | **AF_UNIX** | Connect to an externally-managed worker (systemd unit, daemon).  Operator owns the worker's lifetime. |
| `launch:<argv>` | **launcher** | Spawn-or-reuse via the launcher.  One warm worker shared across DuckDB processes.  This tutorial. |

## Quick start

```sql
LOAD vgi;
ATTACH 'mycat' AS svc (TYPE vgi,
    LOCATION 'launch:uv run --project /opt/my-worker my-worker-cli');

-- First query in this DuckDB process: spawns the worker (or reuses an
-- existing warm one from a sibling DuckDB process).  Subsequent queries
-- are sub-millisecond to dispatch.
SELECT * FROM svc.data.events LIMIT 10;
```

The first call pays whatever the worker's startup cost is.  Every
subsequent call — across this DuckDB process, every other DuckDB
process pointed at the same `launch:` LOCATION, even other unrelated
tools using the cross-language launcher contract — connects to the
same warm worker via `AF_UNIX`.

The worker self-shuts-down after a configurable idle period
(default: 300 seconds) with zero connected clients, so forgotten
workers reclaim themselves.

## `launch:` argv quoting

The argv after `launch:` is parsed with shlex-style semantics:

```
launch:python -m my_worker
launch:java -jar /opt/foo/worker.jar
launch:"/path with spaces/worker" --quiet
launch:'python -c print("x")'
launch:python -m my_worker --my-flag a\ b
```

- Tokens separated by whitespace
- `"…"` for tokens containing whitespace; `\"`, `\\`, `\$`, `\``, `\<newline>` are escapes
- `'…'` for raw tokens (no escapes; literal contents)
- Bare `\<char>` outside quotes inserts `<char>` literally

Anything else passes through unchanged.

The launcher appends two args to whatever you wrote: `--unix /path/to/socket`
and `--idle-timeout 300`.  If your worker doesn't accept those flags,
it'll fail at startup and the ATTACH will throw.  Workers built on the
vgi-rpc Python framework's `run_server()` honour them out of the box;
ports in other languages must do the same.

## Warm-worker sharing in practice

The launcher computes a SHA-256 hash of `(argv, cwd, VGI_RPC_*-env)` and
uses it to derive a deterministic AF_UNIX socket path under
`$XDG_RUNTIME_DIR/vgi-rpc/<hash>.sock` (or `$TMPDIR/vgi-rpc-$UID/` on
macOS).  Two callers with the same tuple resolve to the same socket
file; the first one to arrive spawns the worker, subsequent callers
just connect.

To check that sharing is working:

```bash
# After a few ATTACHes, count the worker processes.
$ pgrep -f vgi-fixture-worker | wc -l
1

# Inspect what's running.
$ ls $XDG_RUNTIME_DIR/vgi-rpc/  # or $TMPDIR/vgi-rpc-$(id -u)/ on macOS
abc1234567890.lock  abc1234567890.meta  abc1234567890.sock

$ cat $XDG_RUNTIME_DIR/vgi-rpc/abc1234567890.meta
{
  "cmd": ["uv", "run", "--project", "/opt/my-worker", "my-worker-cli"],
  "cwd": "/home/me",
  "started_at": 1714940000.0,
  "launcher_pid": 12345,
  "socket": "/run/user/1000/vgi-rpc/abc1234567890.sock"
}
```

The `.meta` file is JSON, intended for ops scripts.  The `.sock` is
the AF_UNIX socket.  The `.lock` is a flock target — its body is
unused.

## Tuning per-LOCATION knobs

Two ATTACH options expose launcher knobs.  See
`docs/launcher-options.md` for the full reference; the highlights:

```sql
-- Self-shutdown after 60 s idle instead of the default 300 s:
ATTACH 'mycat' AS svc (TYPE vgi,
    LOCATION 'launch:uv run --project /opt/my-worker my-worker-cli',
    launcher_idle_timeout 60);

-- Use an alternate state directory (override of $XDG_RUNTIME_DIR/vgi-rpc):
ATTACH 'mycat' AS svc (TYPE vgi,
    LOCATION 'launch:uv run --project /opt/my-worker my-worker-cli',
    launcher_state_dir '/srv/myapp/launcher-state');
```

**Important conflict semantic:** when a worker is already running for a
LOCATION, a second ATTACH that requests *different* values for these
options will fail at parse time with a clean error:

```
Binder Error: vgi launcher: this LOCATION is already attached with launcher
overrides [defaults]; cannot reattach with [idle_timeout=60000ms].  Detach
the existing catalog first, or wait for the worker's idle-shutdown so a
fresh ATTACH can install new overrides.
```

This is intentional.  Spawn-time options are a property of the worker,
not the connection — so two callers can't disagree about them.  Detach
the existing catalog (or wait for idle-shutdown to invalidate the cache)
to install new values.

## When to pick `unix://` instead

If you're running the worker as a separately-supervised process (a
systemd unit, a Kubernetes pod with a sidecar, a manual `python -m
my_worker --unix /run/foo.sock --idle-timeout 0`), use `unix://` and
let the operator own the worker's lifetime:

```sql
ATTACH 'mycat' AS svc (TYPE vgi, LOCATION 'unix:///run/foo.sock');
```

The `launcher_*` ATTACH options don't apply here — they're spawn-time
knobs, and `unix://` doesn't spawn.  vgi rejects them at parse time
with `BinderException`.

## When NOT to use the launcher

- **HTTP** if the worker isn't local (different host, container, etc.).
- **Plain subprocess** if your workload is one-shot scripts running on
  cold caches anyway — the launcher's setup overhead doesn't amortise.
- **WASM builds**: the launcher is POSIX-only.  In WASM, `launch:` /
  `unix://` LOCATIONs throw `BinderException` at ATTACH; only `http://`
  works.

## Common pitfalls

- **`vgi_worker_subprocess_pool()` returns no rows.**  That's expected for
  `launch:` and `unix://` LOCATIONs — workers there are pooled by the
  OS-level AF_UNIX socket, not by DuckDB's per-process subprocess
  pool.  See `CLAUDE.md` for the full caveat.
- **Different `launcher_idle_timeout` per ATTACH conflicts.**  See the
  fail-loud explanation above.  If you genuinely need different idle
  timeouts for different services, use different worker argv
  (different `launch:` LOCATION strings) so they hash to distinct
  workers.
- **Worker not appearing under `$XDG_RUNTIME_DIR/vgi-rpc/`.**  On
  systems without `$XDG_RUNTIME_DIR`, look under
  `$TMPDIR/vgi-rpc-<uid>/` (macOS, BSDs).
- **Stale `.sock` file.**  Should self-clean on idle-shutdown.  If a
  worker was killed hard (SIGKILL), it may leave the file; the
  launcher unlinks it on next attach, no manual cleanup needed.

## See also

- [`docs/launcher-options.md`](launcher-options.md) — full ATTACH-option reference
- [`docs/launcher-protocol.md`](launcher-protocol.md) — cross-language wire contract
- `CLAUDE.md` — extension-level developer notes including transport caveats
