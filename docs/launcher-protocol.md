# AF_UNIX Worker Launcher Protocol

This document is the **single source of truth** for the cross-language
launcher contract — the on-disk layout, the wire bytes that get hashed,
the worker CLI surface — that the Python reference launcher
(`vgi-rpc/vgi_rpc/launcher.py`) and the C++ launcher inside this extension
must agree on.

If you're adding a launcher in Go/Rust/Java/etc., conform to this
document.  If you're modifying either existing implementation, update this
document and regenerate the parity vectors.

## Why a launcher

The vgi C++ extension talks to long-running Python/JVM workers.  Workers
that are expensive to start (JVM cold-start ≈ 2–5 s) are unaffordable to
spawn per query.  The launcher amortises startup across:

1. every RPC inside one DuckDB process (per-process socket-path cache), and
2. every DuckDB process pointed at the same worker tuple (per-hash flock +
   AF_UNIX socket).

End-to-end this is ~5× faster than the per-process subprocess pool for
real test workloads.

## Lifecycle in one paragraph

The first DuckDB process to use a `launch:<argv>` LOCATION acquires a
per-tuple flock, probes for an existing AF_UNIX listener, and — finding
none — spawns the worker with `--unix PATH --idle-timeout SEC`.  The
worker binds, prints `UNIX:<absolute-path>\n` to stdout, then serves RPCs
until idle for `SEC` seconds with no connected clients.  Subsequent
DuckDB processes hitting the same tuple acquire the same flock, probe,
find the listener, and connect — sharing one Python interpreter (or one
JVM) across the cluster.  Each DuckDB process caches the resolved socket
path in-process; on a cache miss (worker idle-shut-down) the cache
invalidates and the launcher fires fresh.

## State directory

Resolved per-user, never shared across UIDs:

| Platform | Path |
|---|---|
| Linux with `$XDG_RUNTIME_DIR` set | `$XDG_RUNTIME_DIR/vgi-rpc/` |
| Linux without `$XDG_RUNTIME_DIR` | `$TMPDIR/vgi-rpc-$EUID/` (or `/tmp/vgi-rpc-$EUID/`) |
| macOS | `$TMPDIR/vgi-rpc-$EUID/` |
| Windows | currently unsupported — POSIX-only POC |

The directory is created mode `0700`; a launcher refuses to operate if it
exists with `st_uid != geteuid()`.

## Per-tuple files

For each `(argv, cwd, env)` tuple, the launcher creates three files in
the state directory, named by a 16-hex-char SHA-256 prefix of the
canonical-JSON tuple bytes (see *Hashing* below):

| File | Purpose | Format |
|---|---|---|
| `<hash>.lock` | flock target.  Body unused. | empty file (mode 0600) |
| `<hash>.sock` | AF_UNIX SOCK_STREAM socket the worker binds | socket file |
| `<hash>.meta` | human-readable metadata for `vgi_worker_subprocess_pool()`-style debugging | JSON: `{cmd, cwd, started_at, launcher_pid, socket}` |

**The body of `<hash>.lock` is unused.**  Do not put data there — atomicity
of the lockfile body is genuinely hard (see Python launcher
`vgi_rpc/launcher.py` commit log for why we backed off the
"path-in-lockfile" design).  Path discovery is purely deterministic from
the hash.

## Hashing

The launcher computes:

```
hash = sha256(canonical_json({
    "cmd": [argv0, argv1, …],
    "cwd": getcwd(),
    "env": {k: v for k, v in os.environ if k.startswith("VGI_RPC_")},
})).hexdigest()[:16]
```

`canonical_json` matches Python's
`json.dumps(payload, sort_keys=True, separators=(",", ":"))` — i.e. **no
whitespace**, **`,` and `:` separators only**, **object keys sorted
ASCII-lex**.

### Encoding rules for strings

- Standard JSON escapes for `"`, `\`, control characters `\b`, `\f`,
  `\n`, `\r`, `\t`.
- Other control characters `\x00..\x1f` use `\u00xx` (lowercase hex).
- Bytes ≥ `0x7F`: **currently passed through as raw UTF-8 by the C++
  implementation, escaped as `\uXXXX` by the Python one.**  Hashes will
  diverge for non-ASCII inputs.  The `VGI_RPC_*` env namespace is
  conventionally ASCII-only; `cwd` may contain non-ASCII on some
  systems and is the most likely divergence trigger in practice.  See
  the open issue in `~/.claude/plans/i-know-we-have-scalable-mountain.md`
  for the resolution plan (Python-side `ensure_ascii=False` or C++-side
  decode-and-escape).

### What `VGI_RPC_*` env means

Only environment variables whose names start with `VGI_RPC_` go into the
hash.  This is the agreed-upon namespace for "settings the worker reads
out-of-band that affect its behaviour" — e.g. `VGI_RPC_ACCESS_LOG`,
`VGI_RPC_SHM_SIZE_BYTES`.  Two callers passing different values for any
of these intentionally get different workers.  Variables outside the
namespace (`PATH`, `HOME`, …) do not affect the hash.

When duplicates exist in `environ` (rare; possible after `setenv` /
fork-exec injection), **last-wins**.  Both implementations must agree.

## Worker CLI surface

The launcher invokes the worker as:

```
<worker_argv...>  --unix <abs_socket_path>  --idle-timeout <seconds>
```

`<seconds>` is a **decimal floating-point number in plain notation** (no
scientific notation, no thousands separators) — chosen so every parser
on every platform agrees byte-for-byte.  Values:

| Value | Meaning |
|---|---|
| any positive number | self-shutdown after this many seconds idle |
| `0` | no idle timeout (worker runs until killed) |
| negative | reserved; current implementations treat as `0` |

The worker MUST:

1. bind its rendezvous endpoint — an AF_UNIX SOCK_STREAM socket at
   `<abs_socket_path>` on POSIX, or a Windows **named pipe** at the
   `\\.\pipe\…` name on Windows (see *Platform: Windows* below),
2. emit exactly one discovery line on **stdout** (not stderr), flushed:
   `UNIX:<abs_socket_path>\n` on POSIX, or `PIPE:<pipe_name>\n` on Windows.
   The scheme prefix (`UNIX:` / `PIPE:`) is a protocol constant both the
   launcher and the worker select by platform,
3. emit nothing further on stdout for the rest of its lifetime
   (anything else triggers SIGPIPE on the launcher's now-closed read
   end, which kills the worker — this is intentional),
4. listen for connections, dispatching each to its own thread
   (or coroutine — the launcher requires no specific concurrency model),
5. self-shutdown when zero clients have been connected for the requested
   idle period.

The worker MAY emit log noise on stdout *before* the discovery line
— the launcher skips lines not matching the platform prefix for
resilience.  A hard cap of 1 MiB of pre-discovery noise applies; beyond
that the launcher SIGTERMs the worker and reports an error.  Workers
SHOULD direct logs to stderr instead.

### Platform: Windows

CPython does not expose `socket.AF_UNIX` on Windows, so the Windows variant
uses **named pipes** for the rendezvous instead of AF_UNIX sockets. The
transport-agnostic machinery is unchanged: the hash, state-dir resolution,
discovery line, and idle-timeout are identical; only three things differ:

- **Rendezvous address**: a named pipe `\\.\pipe\vgi-rpc-<hash>` rather than
  a filesystem socket path. The discovery line uses the `PIPE:` prefix.
- **Singleton election**: a named mutex (`CreateMutex`, `Global\…`) replaces
  the POSIX `flock` on the lockfile. (The Python reference launcher already
  uses the cross-platform `filelock` library.)
- **State dir**: `%TEMP%\vgi-rpc` (no `$XDG_RUNTIME_DIR`, no euid suffix).

The named pipe MUST be created with `PIPE_UNLIMITED_INSTANCES` so multiple
concurrent clients can connect, mirroring an AF_UNIX listen backlog.

## Idle-timeout encoding — single source of truth

The idle timeout has three representations along the pipeline:

| Layer | Type | Range |
|---|---|---|
| C++ `LaunchConfig::idle_timeout` | `std::chrono::milliseconds` | `0` = unbounded; default `300 000 ms` |
| Wire (worker argv) | decimal seconds, e.g. `"300"` or `"1.5"` | non-negative; `"0"` = unbounded |
| Python worker `--idle-timeout` typer flag | `float` seconds | `0` = unbounded; default `300.0` |

**Conversion contracts:**

- C++ → wire: `snprintf("%.3f", milliseconds / 1000.0)` then strip trailing zeros
  past the decimal point.  Never use `%g` (loses precision on large values
  via scientific notation).
- Python worker → wire: `idle_timeout` argv value is parsed as `float`; pass
  through to `serve_unix(idle_timeout=value if value > 0 else None)`.
  Negative or `0` collapses to `None` (no timeout).

Adding a fourth representation (Go/Rust port) MUST conform to the wire
format in column 2.

## Worker shutdown

When the idle timer fires, the worker MUST:

1. close its listening socket (so subsequent `connect()` calls get
   `ECONNREFUSED`),
2. drain in-flight connections to natural end,
3. unlink `<hash>.sock` (best-effort; not a correctness requirement —
   stale socket files are detected via the connect-probe in step 4
   below).

Launcher invocations encountering a `<hash>.sock` that exists but
refuses connect MUST `unlink()` it before binding.

## Lock semantics

The flock primitive is `flock(2)` (POSIX advisory file lock by open file
description).  This is **not the same syscall** as `fcntl(F_SETLK)` and
the two **do not interlock** with each other.

Both reference implementations (Python `filelock` and C++ direct call)
use `flock(2)`.  Verified by the cross-language parity tests in
`vgi/test/cpp/test_launcher_flock_parity.cpp`.  Any new port MUST also
use `flock(2)` (not `fcntl`-based locking) to interlock correctly.

## Reaping

The launcher does NOT use `setsid()` or daemonize.  Spawned workers
inherit the launcher's session and process group; they reparent to init
when the launcher exits — which it does immediately after observing
the `UNIX:<path>` line.

Each launcher process maintains a single shared reaper thread that polls
its registered worker pids via `waitpid(pid, WNOHANG)` every ~500 ms.
Hosts that install `signal(SIGCHLD, SIG_IGN)` (Python embedded, etc.)
auto-reap; the polling thread sees `ECHILD`, drops the entry, and moves
on without conflict.

## Verifiable contract

The C++ implementation ships `vgi/test/cpp/launcher_parity_vectors.hpp`
— a generated header of golden `(input, expected_hash)` pairs produced
by the Python reference (see
`vgi-rpc/scripts/regenerate_launcher_parity_vectors.py`).  Add new
ports' hash implementations against this file.  Re-generate after any
change to the canonical-JSON encoding or the hash domain.

## Files

- `vgi-rpc/vgi_rpc/launcher.py` — Python reference implementation.
- `vgi/src/include/vgi_launcher_internal.hpp` + `.cpp` — C++ pure helpers
  (hashing, canonical JSON, state-dir resolution, scheme detection).
- `vgi/src/include/vgi_launcher.hpp` + `.cpp` — C++ orchestration (flock,
  fork/exec, discovery, reaper).
- `vgi/src/include/vgi_launcher_cache.hpp` + `.cpp` — per-process
  resolved-path cache + invalidation-retry helper.
- `vgi/src/include/vgi_unix_socket_worker.hpp` + `.cpp` — `SubProcess`
  subclass adapting an AF_UNIX fd to the FunctionConnection
  wire-protocol surface.
- `vgi-rpc/scripts/regenerate_launcher_parity_vectors.py` — vector
  generator.
- `vgi/test/cpp/test_launcher_python_parity.cpp` — C++ side of the
  parity test.
- `vgi/test/cpp/test_launcher_flock_parity.cpp` — cross-language flock
  parity test.
