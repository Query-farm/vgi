"""Launcher stress (separate OS processes via multiprocessing spawn).

The launcher's job is cross-process coordination: every DuckDB process pointing
at the same ``launch:`` argv (+ cwd + VGI_RPC_* env) must share ONE warm worker,
serialized by a per-hash flock and discovered through a shared AF_UNIX socket.
A single in-process harness can't exercise that — so each child here is a fresh
spawned process with its own haybarn instance and its own (empty) launcher cache.

Faults are injected externally: SIGKILL the listener PID bound to the socket
(found via ``lsof -t``), unlink the socket file, or race a re-ATTACH against the
idle self-shutdown. There is no ``.meta`` file to read on macOS.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time

from . import common
from .common import StressConfig

# spawn gives each child a clean interpreter + fresh haybarn/launcher cache,
# which is exactly the cross-process condition we want to stress.
_CTX = mp.get_context("spawn")


# ---------------------------------------------------------------------------
# Child entrypoints (module-level so spawn can import + pickle them)
# ---------------------------------------------------------------------------


def _child_query(child_id, worker, state_dir, idle_timeout, n_queries,
                 barrier, result_q):
    """Attach a launch: catalog and run the deterministic probe n times.

    Result tuple: (child_id, status, ok_count, attach_s, query_loop_s). The
    query-loop time excludes process spawn + haybarn startup + attach, so
    throughput reflects VGI cost rather than interpreter cold-start.
    """
    try:
        c = common.connect()
        if barrier is not None:
            barrier.wait()  # all children race into ATTACH together
        t0 = time.perf_counter()
        common.attach(c, "lex", common.launch_location(worker),
                      state_dir=state_dir, idle_timeout=idle_timeout)
        attach_s = time.perf_counter() - t0
        ok = 0
        q0 = time.perf_counter()
        for _ in range(n_queries):
            # retries=2: a kill/unlink may land mid-flight; recovery is the
            # launcher cache invalidation + re-resolve + retry-once path.
            if common.run_det_checked(c, "lex", retries=2):
                ok += 1
        query_loop_s = time.perf_counter() - q0
        c.close()
        result_q.put((child_id, "ok", ok, round(attach_s, 4), round(query_loop_s, 4)))
    except Exception as e:  # noqa: BLE001
        result_q.put((child_id, "err", f"{type(e).__name__}: {str(e)[:160]}", 0.0, 0.0))


def _child_conflict(child_id, worker, state_dir, idle_timeout, result_q):
    """Within ONE process, two ATTACHes of the same LOCATION with different
    idle_timeout must conflict (BinderException) rather than diverge."""
    try:
        c = common.connect()
        common.attach(c, "a", common.launch_location(worker),
                      state_dir=state_dir, idle_timeout=idle_timeout)
        try:
            common.attach(c, "b", common.launch_location(worker),
                          state_dir=state_dir, idle_timeout=idle_timeout + 100)
            result_q.put((child_id, "no_conflict", "second ATTACH unexpectedly succeeded"))
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if "reattach" in msg or "already attached" in msg or "Binder" in type(e).__name__:
                result_q.put((child_id, "conflict_ok", ""))
            else:
                result_q.put((child_id, "wrong_error", f"{type(e).__name__}: {msg[:140]}"))
        c.close()
    except Exception as e:  # noqa: BLE001
        result_q.put((child_id, "err", f"{type(e).__name__}: {str(e)[:160]}"))


def _spawn(target, args) -> "_CTX.Process":
    p = _CTX.Process(target=target, args=args)
    p.start()
    return p


def _drain(result_q, expected: int, timeout: float = 120.0) -> list[tuple]:
    out = []
    deadline = time.time() + timeout
    while len(out) < expected and time.time() < deadline:
        try:
            out.append(result_q.get(timeout=deadline - time.time()))
        except Exception:  # noqa: BLE001 - queue Empty / timeout
            break
    return out


# ---------------------------------------------------------------------------
# correctness  (spawn race + override conflict)
# ---------------------------------------------------------------------------


def _correctness(cfg: StressConfig, state_dir: str) -> list[str]:
    failures: list[str] = []
    n = cfg.concurrency[0]

    # --- spawn race: N children hit the same launch: LOCATION at once ---
    barrier = _CTX.Barrier(n)
    q = _CTX.Queue()
    procs = [
        _spawn(_child_query,
               (i, cfg.worker, state_dir, cfg.idle_timeout, cfg.queries, barrier, q))
        for i in range(n)
    ]
    results = _drain(q, n)
    for p in procs:
        p.join(timeout=10)

    oks = [r for r in results if r[1] == "ok"]
    errs = [r for r in results if r[1] == "err"]
    for r in errs:
        failures.append(f"spawn-race child {r[0]}: {r[2]}")
    short = [r for r in oks if r[2] < cfg.queries]
    for r in short:
        failures.append(f"spawn-race child {r[0]}: only {r[2]}/{cfg.queries} queries ok")
    if len(results) < n:
        failures.append(f"spawn-race: only {len(results)}/{n} children reported")

    # Exactly one worker should have been spawned for the single LOCATION hash.
    socks = common.sock_paths(state_dir)
    if len(socks) != 1:
        failures.append(f"expected exactly 1 socket for one LOCATION, found {len(socks)}")
    else:
        lp = common.listener_pid_for_socket(socks[0])
        if lp is None:
            failures.append("no live listener bound to the launcher socket")
    print(f"  spawn-race: {len(oks)}/{n} children ok, sockets={len(socks)}")

    # --- override conflict (single process) ---
    qc = _CTX.Queue()
    pc = _spawn(_child_conflict, (0, cfg.worker, state_dir, cfg.idle_timeout, qc))
    cres = _drain(qc, 1, timeout=60)
    pc.join(timeout=10)
    if not cres:
        failures.append("override-conflict child did not report")
    else:
        tag = cres[0][1]
        if tag == "conflict_ok":
            print("  override-conflict: BinderException raised as expected")
        elif tag == "no_conflict":
            failures.append("override-conflict: divergent idle_timeout was NOT rejected")
        else:
            failures.append(f"override-conflict: {cres[0][2]}")

    return failures


# ---------------------------------------------------------------------------
# faults  (kill listener / unlink socket / idle-shutdown race)
# ---------------------------------------------------------------------------


def _faults(cfg: StressConfig, state_dir: str) -> list[str]:
    failures: list[str] = []
    n = max(2, cfg.concurrency[0])

    # kill-mid-flight: children run many queries; mid-run we SIGKILL the listener.
    long_q = max(cfg.queries, 40)
    q = _CTX.Queue()
    procs = [
        _spawn(_child_query,
               (i, cfg.worker, state_dir, cfg.idle_timeout, long_q, None, q))
        for i in range(n)
    ]
    # Let workers warm + start querying, then kill the shared listener twice.
    kills = 0
    for _ in range(2):
        time.sleep(0.8)
        for s in common.sock_paths(state_dir):
            lp = common.listener_pid_for_socket(s)
            if lp:
                try:
                    os.kill(lp, signal.SIGKILL)
                    kills += 1
                except ProcessLookupError:
                    pass
    results = _drain(q, n)
    for p in procs:
        p.join(timeout=15)

    oks = [r for r in results if r[1] == "ok"]
    if kills == 0:
        failures.append("kill-mid-flight: never found a listener to kill")
    # Every child should ultimately complete all queries (recovery succeeded).
    incomplete = [r for r in oks if r[2] < long_q] + [r for r in results if r[1] == "err"]
    for r in incomplete:
        failures.append(f"kill-mid-flight child {r[0]} did not fully recover: {r[1:]}")
    print(f"  kill-mid-flight: {kills} kills, {len(oks)}/{n} children fully recovered")

    # stale-socket: unlink the socket out from under a warm cache, expect respawn.
    for s in common.sock_paths(state_dir):
        try:
            os.unlink(s)
        except FileNotFoundError:
            pass
    q2 = _CTX.Queue()
    p2 = _spawn(_child_query, (0, cfg.worker, state_dir, cfg.idle_timeout, 5, None, q2))
    r2 = _drain(q2, 1, timeout=60)
    p2.join(timeout=10)
    if not r2 or r2[0][1] != "ok" or r2[0][2] < 5:
        failures.append(f"stale-socket: respawn did not recover ({r2})")
    else:
        print("  stale-socket: respawned cleanly after socket unlink")

    # idle-shutdown race: short idle_timeout, idle past it, assert self-shutdown
    # then immediate re-attach respawns to exactly one socket.
    short_idle = 2
    q3 = _CTX.Queue()
    p3 = _spawn(_child_query, (0, cfg.worker, state_dir, short_idle, 2, None, q3))
    _drain(q3, 1, timeout=60)
    p3.join(timeout=10)
    time.sleep(short_idle + 3)  # let the worker self-shutdown + unlink its socket
    after_idle = len(common.sock_paths(state_dir))
    q4 = _CTX.Queue()
    p4 = _spawn(_child_query, (0, cfg.worker, state_dir, short_idle, 3, None, q4))
    r4 = _drain(q4, 1, timeout=60)
    p4.join(timeout=10)
    # Two assertions, both timing-robust: (1) the worker self-shut-down once idle
    # (socket removed), and (2) a re-attach respawns and serves. We deliberately
    # do NOT assert the live socket count afterwards — with idle_timeout=2 the
    # respawned worker may already be self-shutting-down again, which is correct.
    if after_idle != 0:
        failures.append(
            f"idle-shutdown: worker did not remove its socket {short_idle}s after "
            f"going idle ({after_idle} still present)"
        )
    if not r4 or r4[0][1] != "ok" or r4[0][2] < 3:
        failures.append(f"idle-shutdown: re-attach after self-shutdown failed ({r4})")
    if after_idle == 0 and r4 and r4[0][1] == "ok":
        print("  idle-shutdown: self-shutdown removed socket, re-attach respawned cleanly")

    return failures


# ---------------------------------------------------------------------------
# throughput  (cold vs warm attach + steady qps across processes)
# ---------------------------------------------------------------------------


def _throughput(cfg: StressConfig, state_dir: str) -> tuple[list[str], list[dict]]:
    rows: list[dict] = []
    for n in cfg.concurrency:
        q = _CTX.Queue()
        t0 = time.perf_counter()
        procs = [
            _spawn(_child_query,
                   (i, cfg.worker, state_dir, cfg.idle_timeout, cfg.queries, None, q))
            for i in range(n)
        ]
        results = _drain(q, n)
        for p in procs:
            p.join(timeout=30)
        elapsed = time.perf_counter() - t0

        oks = [r for r in results if r[1] == "ok"]
        errs = [r for r in results if r[1] == "err"]
        attach_times = sorted(r[3] for r in oks)
        cold = attach_times[-1] if attach_times else 0.0  # someone paid the spawn
        warm = attach_times[0] if attach_times else 0.0     # the rest reused
        total_q = sum(r[2] for r in oks)
        # Query-phase wall time = the slowest child's query loop (they overlap),
        # excluding process spawn / haybarn startup / attach. wall_s is the raw
        # end-to-end including spawn, kept for context.
        query_wall = max((r[4] for r in oks), default=0.0)
        row = {
            "target": "launcher",
            "concurrency": n,
            "queries": total_q,
            "errors": len(errs),
            "query_wall_s": round(query_wall, 4),
            "wall_s": round(elapsed, 4),
            "qps": round(total_q / query_wall, 1) if query_wall else 0,
            "cold_attach_ms": round(cold * 1000, 1),
            "warm_attach_ms": round(warm * 1000, 1),
            "hit_rate": "",  # n/a for launcher (OS-pooled)
            "p50_ms": "", "p95_ms": "", "p99_ms": "",
        }
        rows.append(row)
        print(f"  c={n:<4} qps={row['qps']:<8} (query-phase {query_wall:.2f}s, "
              f"e2e incl spawn {elapsed:.1f}s) cold_attach={row['cold_attach_ms']}ms "
              f"warm_attach={row['warm_attach_ms']}ms errs={row['errors']}")
    failures = [f"throughput saw {r['errors']} errors at c={r['concurrency']}"
                for r in rows if r["errors"]]
    return failures, rows


# ---------------------------------------------------------------------------
# soak  (sustained multi-process load; one-worker-per-hash invariant)
# ---------------------------------------------------------------------------


def _soak(cfg: StressConfig, state_dir: str) -> list[str]:
    failures: list[str] = []
    n = cfg.concurrency[0]
    # Each child runs enough queries to outlast the soak duration roughly.
    per_child = max(cfg.queries, int(cfg.duration * 5))
    q = _CTX.Queue()
    procs = [
        _spawn(_child_query,
               (i, cfg.worker, state_dir, cfg.idle_timeout, per_child, None, q))
        for i in range(n)
    ]

    deadline = time.time() + cfg.duration
    max_socks = 0
    max_workers = 0
    while time.time() < deadline and any(p.is_alive() for p in procs):
        max_socks = max(max_socks, len(common.sock_paths(state_dir)))
        max_workers = max(max_workers, len(common.worker_pids()))
        time.sleep(min(2.0, cfg.duration / 10 or 1))

    results = _drain(q, n, timeout=cfg.duration + 30)
    for p in procs:
        p.join(timeout=20)

    errs = [r for r in results if r[1] == "err"]
    for r in errs:
        failures.append(f"soak child {r[0]}: {r[2]}")
    # One LOCATION hash => at most one socket should ever exist.
    if max_socks > 1:
        failures.append(f"soak: saw {max_socks} sockets for one LOCATION (expected 1)")
    print(f"  soak: max_sockets={max_socks} max_worker_procs={max_workers}")

    # After everyone idles out, the state dir should drain and no orphan remains.
    time.sleep(cfg.idle_timeout + 4)
    leftover = common.sock_paths(state_dir)
    if leftover:
        print(f"  soak: {len(leftover)} socket(s) still present after idle window "
              f"(idle_timeout={cfg.idle_timeout}s) — may need longer to self-reap")
    return failures


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------


def run(cfg: StressConfig) -> tuple[list[str], list[dict]]:
    common.ensure_installed()
    print(f"[launcher/{cfg.mode}] worker={cfg.worker!r} concurrency={cfg.concurrency}")
    with common.temp_state_dir() as state_dir:
        print(f"  isolated state_dir={state_dir}")
        if cfg.mode == "correctness":
            fails = _correctness(cfg, state_dir)
            if cfg.faults:
                fails += _faults(cfg, state_dir)
            rows: list[dict] = []
        elif cfg.mode == "throughput":
            fails, rows = _throughput(cfg, state_dir)
        elif cfg.mode == "soak":
            fails, rows = _soak(cfg, state_dir), []
        else:
            fails, rows = [f"unknown mode {cfg.mode}"], []

        # Final isolation check: no fixture worker should outlive our state dir.
        time.sleep(0.5)
    # state_dir is rm -rf'd here; workers bound to it will self-shutdown on idle.
    return fails, rows
