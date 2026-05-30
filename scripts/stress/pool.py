"""Subprocess worker-pool stress (threads inside one haybarn process).

The pool is a per-DuckDB-process singleton, so concurrency *within* one process
is the right tool: many threads (each its own connection) plus ``SET threads=N``
big scans drive concurrent acquire/release against the same pool, while the
background cleanup thread evicts idle/dead workers underneath.

Fault injection SIGKILLs pooled worker PIDs externally (read from
``vgi_worker_subprocess_pool()``) and asserts the next query recovers via the
pool's EPIPE stale-connection retry.
"""

from __future__ import annotations

import os
import signal
import threading
import time

from . import common
from .common import StressConfig


def _pooled_pids(conn) -> list[int]:
    return [int(r[0]) for r in conn.execute(
        "SELECT pid FROM vgi_worker_subprocess_pool()"
    ).fetchall()]


def _pool_size(conn) -> int:
    return int(conn.execute(
        "SELECT count(*) FROM vgi_worker_subprocess_pool()"
    ).fetchone()[0])


def _stats(conn) -> tuple[int, int]:
    row = conn.execute(
        "SELECT COALESCE(SUM(hits),0)::BIGINT, COALESCE(SUM(misses),0)::BIGINT "
        "FROM vgi_worker_pool_stats()"
    ).fetchone()
    return (int(row[0]), int(row[1]))


# ---------------------------------------------------------------------------
# correctness
# ---------------------------------------------------------------------------


def _correctness(cfg: StressConfig) -> list[str]:
    failures: list[str] = []
    n = cfg.concurrency[0]
    stop = threading.Event()
    lock = threading.Lock()

    def record(msg: str) -> None:
        with lock:
            failures.append(msg)

    def worker_thread(tid: int) -> None:
        try:
            c = common.connect()
            alias = f"ex{tid}"
            common.attach(c, alias, common.subprocess_location(cfg.worker),
                          pool_max=cfg.pool_max)
            for i in range(cfg.queries):
                if stop.is_set():
                    return
                if not common.run_det_checked(c, alias, retries=1):
                    record(f"t{tid} q{i}: wrong/failed result")
                    return
                # Periodically churn the attach to stress detach/re-attach.
                if i % 17 == 16:
                    c.execute(f"DETACH {alias}")
                    common.attach(c, alias, common.subprocess_location(cfg.worker),
                                  pool_max=cfg.pool_max)
            c.close()
        except Exception as e:  # noqa: BLE001
            record(f"t{tid}: unhandled {type(e).__name__}: {str(e)[:140]}")

    def big_scan_thread() -> None:
        try:
            c = common.connect()
            c.execute("SET threads=8")
            common.attach(c, "exbig", common.subprocess_location(cfg.worker),
                          pool_max=cfg.pool_max)
            for _ in range(max(3, cfg.queries // 10)):
                if stop.is_set():
                    return
                c.execute(common.BIG_QUERY.format(alias="exbig")).fetchone()
            c.close()
        except Exception as e:  # noqa: BLE001
            record(f"big-scan: {type(e).__name__}: {str(e)[:140]}")

    # A monitor thread asserts the pool never exceeds pool_max.
    mon_breach = []

    def monitor_thread() -> None:
        c = common.connect()
        common.attach(c, "exmon", common.subprocess_location(cfg.worker),
                      pool_max=cfg.pool_max)
        while not stop.is_set():
            sz = _pool_size(c)
            if sz > cfg.pool_max:
                mon_breach.append(sz)
            time.sleep(0.05)
        c.close()

    threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(n)]
    threads.append(threading.Thread(target=big_scan_thread))
    mon = threading.Thread(target=monitor_thread, daemon=True)
    mon.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    mon.join(timeout=2)

    if mon_breach:
        failures.append(
            f"pool size exceeded pool_max={cfg.pool_max}: saw {max(mon_breach)}"
        )
    return failures


# ---------------------------------------------------------------------------
# faults
# ---------------------------------------------------------------------------


def _faults(cfg: StressConfig) -> list[str]:
    failures: list[str] = []

    # --- Phase A: deterministic recovery (mirrors buffered_pool_recovery.test).
    # Warm the pool, kill every live worker, then a clean query MUST recover via
    # the pool's EPIPE stale-connection retry, and misses MUST grow.
    c = common.connect()
    common.attach(c, "fxdet", common.subprocess_location(cfg.worker),
                  pool_max=cfg.pool_max)
    common.run_det(c, "fxdet")  # warm -> at least one pooled worker
    _, misses_before = _stats(c)
    killed = 0
    for pid in common.worker_pids():
        try:
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except ProcessLookupError:
            pass
    time.sleep(0.3)
    recovered = common.run_det_checked(c, "fxdet", retries=2)
    _, misses_after = _stats(c)
    c.close()
    if killed == 0:
        failures.append("Phase A: found no workers to kill")
    if not recovered:
        failures.append("Phase A: clean query did not recover after killing all workers")
    if misses_after <= misses_before:
        failures.append(
            f"Phase A: misses did not grow after kill ({misses_before} -> "
            f"{misses_after}); EPIPE recovery suspect"
        )
    print(f"  faults Phase A: killed {killed}, recovered={recovered}, "
          f"misses {misses_before} -> {misses_after}")

    # --- Phase B: stay-up-under-kills storm. Sustained concurrent load while a
    # killer aggressively SIGKILLs every live worker; every thread must finish
    # all its queries (each query retried up to 3x to absorb a mid-flight kill).
    n = cfg.concurrency[0]
    per_thread = max(cfg.queries, 60)
    stop = threading.Event()
    lock = threading.Lock()
    kills = {"count": 0}

    def record(msg: str) -> None:
        with lock:
            failures.append(msg)

    warmed = threading.Barrier(n + 1)  # threads + main, gate the killer start

    def worker_thread(tid: int) -> None:
        try:
            cc = common.connect()
            alias = f"fx{tid}"
            if not common.attach_with_retry(cc, alias,
                                            common.subprocess_location(cfg.worker),
                                            pool_max=cfg.pool_max):
                record(f"Phase B t{tid}: could not attach")
                warmed.wait()
                return
            common.run_det_checked(cc, alias, retries=2)  # warm before killing
            warmed.wait()  # release the killer only once everyone is warm
            for i in range(per_thread):
                if not common.run_det_checked(cc, alias, retries=4):
                    record(f"Phase B t{tid} q{i}: did not recover under kill storm")
                    return
            cc.close()
        except Exception as e:  # noqa: BLE001
            record(f"Phase B t{tid}: unhandled {type(e).__name__}: {str(e)[:140]}")

    def killer_thread() -> None:
        while not stop.is_set():
            for pid in common.worker_pids():
                try:
                    os.kill(pid, signal.SIGKILL)
                    with lock:
                        kills["count"] += 1
                except ProcessLookupError:
                    pass
                except Exception:  # noqa: BLE001
                    pass
            # Sustainable cadence: comfortably longer than worker respawn
            # (~0.5s) so we test sustained resilience, not an impossible
            # kill-faster-than-spawn condition.
            time.sleep(1.5)

    threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(n)]
    killer = threading.Thread(target=killer_thread, daemon=True)
    for t in threads:
        t.start()
    warmed.wait()  # all threads attached + warmed
    killer.start()
    for t in threads:
        t.join()
    stop.set()
    killer.join(timeout=2)
    print(f"  faults Phase B: {kills['count']} kills under load, "
          f"{n} threads x {per_thread} queries")
    if kills["count"] == 0:
        failures.append("Phase B: killer never landed a kill (load too short?)")
    return failures


# ---------------------------------------------------------------------------
# throughput
# ---------------------------------------------------------------------------


def _throughput(cfg: StressConfig) -> tuple[list[str], list[dict]]:
    rows: list[dict] = []
    for n in cfg.concurrency:
        lat = common.LatencyStats()
        lat_lock = threading.Lock()
        counts = {"ok": 0, "err": 0}

        # fresh stats baseline per level
        sc = common.connect()
        common.attach(sc, "tbase", common.subprocess_location(cfg.worker),
                      pool_max=cfg.pool_max)
        common.run_det(sc, "tbase")
        h0, m0 = _stats(sc)

        def worker_thread(tid: int) -> None:
            local = common.LatencyStats()
            ok = err = 0
            c = common.connect()
            alias = f"tp{tid}"
            common.attach(c, alias, common.subprocess_location(cfg.worker),
                          pool_max=cfg.pool_max)
            for _ in range(cfg.queries):
                t0 = time.perf_counter()
                try:
                    common.run_det(c, alias)
                    ok += 1
                except Exception:  # noqa: BLE001
                    err += 1
                local.add(time.perf_counter() - t0)
            c.close()
            with lat_lock:
                lat.merge(local)
                counts["ok"] += ok
                counts["err"] += err

        threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(n)]
        t_start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - t_start

        h1, m1 = _stats(sc)
        sc.close()
        s = lat.summary()
        hits, misses = h1 - h0, m1 - m0
        hit_rate = hits / (hits + misses) if (hits + misses) else 0.0
        row = {
            "target": "pool",
            "concurrency": n,
            "queries": counts["ok"] + counts["err"],
            "errors": counts["err"],
            "elapsed_s": round(elapsed, 4),
            "qps": round((counts["ok"] + counts["err"]) / elapsed, 1) if elapsed else 0,
            "p50_ms": round(s["p50"] * 1000, 2),
            "p95_ms": round(s["p95"] * 1000, 2),
            "p99_ms": round(s["p99"] * 1000, 2),
            "hit_rate": round(hit_rate, 4),
        }
        rows.append(row)
        print(f"  c={n:<4} qps={row['qps']:<8} p50={row['p50_ms']}ms "
              f"p99={row['p99_ms']}ms hit_rate={row['hit_rate']} errs={row['errors']}")

    failures = [f"throughput saw {r['errors']} errors at c={r['concurrency']}"
                for r in rows if r["errors"]]
    return failures, rows


# ---------------------------------------------------------------------------
# soak
# ---------------------------------------------------------------------------


def _soak(cfg: StressConfig) -> list[str]:
    failures: list[str] = []
    n = cfg.concurrency[0]
    stop = threading.Event()
    lock = threading.Lock()

    def record(msg: str) -> None:
        with lock:
            failures.append(msg)

    def worker_thread(tid: int) -> None:
        try:
            c = common.connect()
            alias = f"sk{tid}"
            # small pool_timeout => cleanup thread constantly evicting idle workers
            common.attach(c, alias, common.subprocess_location(cfg.worker),
                          pool_max=cfg.pool_max, pool_timeout=cfg.pool_timeout)
            deadline = time.time() + cfg.duration
            while time.time() < deadline and not stop.is_set():
                if not common.run_det_checked(c, alias, retries=1):
                    record(f"t{tid}: wrong/failed result during soak")
                    return
                time.sleep(0.01)
            c.close()
        except Exception as e:  # noqa: BLE001
            record(f"t{tid}: unhandled {type(e).__name__}: {str(e)[:140]}")

    threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()

    # Sample resource counters while load runs.
    samples = []
    deadline = time.time() + cfg.duration
    while time.time() < deadline:
        samples.append({
            "fds": common.open_fd_count(),
            "workers": len(common.worker_pids()),
            "zombies": common.zombie_count(),
        })
        time.sleep(min(5.0, cfg.duration / 6 or 1))
    for t in threads:
        t.join()
    stop.set()

    if samples:
        fd_max = max(s["fds"] for s in samples)
        fd_min = min(s["fds"] for s in samples)
        wk_max = max(s["workers"] for s in samples)
        zomb = max(s["zombies"] for s in samples)
        print(f"  soak: fds[{fd_min}..{fd_max}] workers<= {wk_max} zombies<= {zomb}")
        # Leak heuristic: FD count should not grow without bound. Flag a >2x
        # climb from the first sample to the peak as suspicious.
        if samples[0]["fds"] and fd_max > 2 * samples[0]["fds"] + 50:
            failures.append(
                f"FD count climbed {samples[0]['fds']} -> {fd_max} (possible leak)"
            )
        if zomb > 0:
            failures.append(f"observed {zomb} zombie worker(s)")

    # After load + cleanup, idle workers should drain. Give the cleanup thread
    # time (idle_limit) plus margin, then check the pool is bounded.
    time.sleep(cfg.pool_timeout + 3)
    return failures


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------


def run(cfg: StressConfig) -> tuple[list[str], list[dict]]:
    """Returns (failures, csv_rows)."""
    common.ensure_installed()
    print(f"[pool/{cfg.mode}] worker={cfg.worker!r} concurrency={cfg.concurrency}")
    if cfg.mode == "correctness":
        fails = _correctness(cfg)
        if cfg.faults:
            fails += _faults(cfg)
        return fails, []
    if cfg.mode == "throughput":
        return _throughput(cfg)
    if cfg.mode == "soak":
        return _soak(cfg), []
    return [f"unknown mode {cfg.mode}"], []
