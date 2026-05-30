"""Shared plumbing for the VGI stress harness.

Connection factory, ATTACH helpers, the deterministic correctness probe, a
dependency-free latency accumulator, and resource probes (worker PIDs, open FDs,
state-dir census, zombies) built on ``pgrep`` / ``lsof`` / ``ps`` so the harness
has no third-party dependencies beyond haybarn itself.

Everything here is import-safe under ``multiprocessing`` spawn: no module-level
side effects, no open handles.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field

# Lazy import so `python -m scripts.stress --help` works even outside the
# haybarn venv (the actual run will fail loudly with a clear message instead).
try:  # pragma: no cover - import guard
    import haybarn  # type: ignore
except Exception:  # pragma: no cover
    haybarn = None  # resolved at connect() time with a helpful error


# ---------------------------------------------------------------------------
# Fixed facts about the fixture worker (verified live, see plan file).
# ---------------------------------------------------------------------------

DEFAULT_WORKER = (
    "uv run --project /Users/rusty/Development/vgi-python vgi-fixture-worker"
)
# The fixture worker only answers to the catalog name 'example'.
CATALOG = "example"
# Deterministic, cheap probe: first_ten yields rows 0..9.
DET_QUERY = "SELECT sum(n)::BIGINT, count(*)::BIGINT FROM {alias}.main.first_ten"
DET_EXPECTED = (45, 10)
# A heavier scan that DuckDB parallelizes across threads, driving concurrent
# pool acquires inside a single query. funny_numbers has ~123k rows.
BIG_QUERY = "SELECT count(*)::BIGINT FROM {alias}.data.funny_numbers"


def haybarn_python() -> str:
    """Best-effort path to the haybarn venv interpreter, for the --help hint."""
    return "/Users/rusty/Development/haybarn-python/.venv/bin/python"


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

_installed = False


def ensure_installed() -> None:
    """Install the vgi community extension once per process (cached globally).

    Skipped entirely when VGI_STRESS_EXTENSION_PATH points at a local build.
    """
    global _installed
    if _installed or os.environ.get("VGI_STRESS_EXTENSION_PATH"):
        return
    if haybarn is None:
        raise RuntimeError(
            "haybarn is not importable. Run this harness with the haybarn venv "
            f"python, e.g.\n  {haybarn_python()} -m scripts.stress ..."
        )
    c = haybarn.connect(config={"allow_unsigned_extensions": "true"})
    c.execute("INSTALL vgi FROM community")
    c.close()
    _installed = True


def connect():
    """Open a haybarn connection with vgi loaded.

    Honors VGI_STRESS_EXTENSION_PATH (LOAD a local .duckdb_extension) over the
    community build. Safe to call from many threads/processes — each call is an
    independent connection (sharing one across threads is unsafe in haybarn).
    """
    if haybarn is None:
        raise RuntimeError(
            "haybarn is not importable. Run with the haybarn venv python:\n"
            f"  {haybarn_python()} -m scripts.stress ..."
        )
    c = haybarn.connect(config={"allow_unsigned_extensions": "true"})
    local = os.environ.get("VGI_STRESS_EXTENSION_PATH")
    if local:
        c.execute(f"LOAD '{local}'")
    else:
        c.execute("LOAD vgi")
    return c


def attach(
    conn,
    alias: str,
    location: str,
    *,
    state_dir: str | None = None,
    idle_timeout: int | None = None,
    pool: bool | None = None,
    pool_max: int | None = None,
    pool_timeout: int | None = None,
) -> None:
    """ATTACH the fixture catalog under ``alias`` with the given options."""
    opts = ["TYPE vgi", f"LOCATION '{location}'"]
    if state_dir is not None:
        opts.append(f"launcher_state_dir '{state_dir}'")
    if idle_timeout is not None:
        opts.append(f"launcher_idle_timeout {idle_timeout}")
    if pool is not None:
        opts.append(f"pool {str(pool).lower()}")
    if pool_max is not None:
        opts.append(f"pool_max {pool_max}")
    if pool_timeout is not None:
        opts.append(f"pool_timeout {pool_timeout}")
    conn.execute(f"ATTACH '{CATALOG}' AS {alias} ({', '.join(opts)})")


def attach_with_retry(conn, alias, location, *, retries: int = 3, **kw) -> bool:
    """ATTACH, retrying if a worker dies during the catalog discovery RPCs.

    Catalog/bind unary RPCs are not transparently retried the way the data-scan
    path is, so an aggressive kill storm landing mid-ATTACH can throw. Re-running
    the ATTACH acquires a fresh worker. Returns True on success.
    """
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            attach(conn, alias, location, **kw)
            return True
        except Exception as e:  # noqa: BLE001
            last = e
            # If a half-attach left the alias around, drop it before retrying.
            try:
                conn.execute(f"DETACH {alias}")
            except Exception:  # noqa: BLE001
                pass
            if attempt < retries:
                time.sleep(0.2)
    if last is not None and os.environ.get("VGI_STRESS_VERBOSE"):
        print(f"  [attach-retry-exhausted] {type(last).__name__}: {str(last)[:120]}")
    return False


def subprocess_location(worker: str) -> str:
    return worker


def launch_location(worker: str) -> str:
    return f"launch:{worker}"


def run_det(conn, alias: str) -> tuple[int, int]:
    """Run the deterministic probe and return its (sum, count) result."""
    row = conn.execute(DET_QUERY.format(alias=alias)).fetchone()
    return (int(row[0]), int(row[1]))


def run_det_checked(conn, alias: str, *, retries: int = 1, backoff: float = 0.2) -> bool:
    """Run the probe, retrying transient worker-death errors.

    Returns True iff a (possibly retried) run produced the expected result.
    A mid-flight worker kill surfaces as an IOException/EOF to the in-flight
    scan; the retry re-acquires a worker via the pool's EPIPE path, spawning a
    fresh one if the pool is empty. Respawn costs a few hundred ms, so retries
    back off to span that window rather than burning all attempts instantly.
    """
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            if run_det(conn, alias) == DET_EXPECTED:
                return True
            return False  # wrong result is never acceptable, don't retry
        except Exception as e:  # noqa: BLE001 - transient transport failure
            last = e
            if attempt < retries:
                time.sleep(backoff)
    if last is not None and os.environ.get("VGI_STRESS_VERBOSE"):
        print(f"  [retry-exhausted] {type(last).__name__}: {str(last)[:120]}")
    return False


# ---------------------------------------------------------------------------
# Latency accumulation (dependency-free percentiles)
# ---------------------------------------------------------------------------


class LatencyStats:
    __slots__ = ("samples",)

    def __init__(self) -> None:
        self.samples: list[float] = []

    def add(self, seconds: float) -> None:
        self.samples.append(seconds)

    def merge(self, other: "LatencyStats") -> None:
        self.samples.extend(other.samples)

    def summary(self) -> dict[str, float]:
        s = sorted(self.samples)
        n = len(s)
        if n == 0:
            return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "mean": 0.0}

        def q(p: float) -> float:
            return s[min(n - 1, int(p * n))]

        return {
            "count": n,
            "p50": q(0.50),
            "p95": q(0.95),
            "p99": q(0.99),
            "max": s[-1],
            "mean": sum(s) / n,
        }


# ---------------------------------------------------------------------------
# Resource probes (no psutil dependency — pgrep / lsof / ps)
# ---------------------------------------------------------------------------


def _run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout
    except Exception:  # noqa: BLE001
        return ""


def worker_pids(pattern: str = "vgi-fixture-worker") -> list[int]:
    """All live PIDs whose argv matches ``pattern`` (wrappers + workers)."""
    out = _run(["pgrep", "-f", pattern])
    return [int(p) for p in out.split() if p.strip().isdigit()]


def listener_pid_for_socket(sock_path: str) -> int | None:
    """PID of the process bound to an AF_UNIX socket file (the launcher worker)."""
    out = _run(["lsof", "-t", sock_path]).split()
    pids = [int(p) for p in out if p.strip().isdigit()]
    return pids[0] if pids else None


def open_fd_count(pid: int | None = None) -> int:
    """Open file-descriptor count for ``pid`` (default: this process)."""
    pid = pid or os.getpid()
    out = _run(["lsof", "-p", str(pid)])
    # subtract the header line
    lines = [ln for ln in out.splitlines() if ln.strip()]
    return max(0, len(lines) - 1)


def zombie_count(pattern: str = "vgi-fixture-worker") -> int:
    """Count zombie processes matching ``pattern`` (defunct, unreaped)."""
    out = _run(["ps", "-axo", "pid,stat,command"])
    n = 0
    for ln in out.splitlines():
        if pattern in ln and ("Z" in ln.split()[1:2][0] if len(ln.split()) > 1 else False):
            n += 1
    # robust fallback: explicit <defunct> marker
    n += sum(1 for ln in out.splitlines() if pattern in ln and "<defunct>" in ln)
    return n


def statedir_census(state_dir: str) -> dict[str, int]:
    """Count .sock / .lock / .meta entries in a launcher state dir."""
    if not state_dir or not os.path.isdir(state_dir):
        return {"sock": 0, "lock": 0, "meta": 0}
    names = os.listdir(state_dir)
    return {
        "sock": sum(1 for n in names if n.endswith(".sock")),
        "lock": sum(1 for n in names if n.endswith(".lock")),
        "meta": sum(1 for n in names if n.endswith(".meta")),
    }


def sock_paths(state_dir: str) -> list[str]:
    if not state_dir or not os.path.isdir(state_dir):
        return []
    return [
        os.path.join(state_dir, n)
        for n in os.listdir(state_dir)
        if n.endswith(".sock")
    ]


def kill_state_dir_workers(state_dir: str) -> int:
    """SIGKILL every launcher worker bound to this (private) state dir.

    Launcher workers self-shutdown only after their idle_timeout (default 300s,
    30s here), so without this a finished run leaves them lingering. The worker
    argv carries ``--unix <state_dir>/<hash>.sock``, so matching the state dir
    path targets exactly this run's workers and nothing else.
    """
    if not state_dir:
        return 0
    killed = 0
    out = _run(["pgrep", "-f", state_dir])
    for tok in out.split():
        if tok.strip().isdigit():
            try:
                os.kill(int(tok), 9)
                killed += 1
            except ProcessLookupError:
                pass
            except Exception:  # noqa: BLE001
                pass
    return killed


@contextlib.contextmanager
def temp_state_dir():
    """A private launcher state dir; workers reaped + dir rm -rf'd on exit."""
    d = tempfile.mkdtemp(prefix="vgi-stress-")
    try:
        yield d
    finally:
        kill_state_dir_workers(d)
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Shared config passed to per-target runners (picklable for spawn children)
# ---------------------------------------------------------------------------


@dataclass
class StressConfig:
    target: str = "both"
    mode: str = "correctness"
    concurrency: list[int] = field(default_factory=lambda: [8])
    queries: int = 50
    duration: float = 60.0
    worker: str = DEFAULT_WORKER
    state_dir: str | None = None
    idle_timeout: int = 30
    faults: bool = True
    csv: str | None = None
    pool_max: int = 32
    pool_timeout: int = 1
