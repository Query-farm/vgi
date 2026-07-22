#!/usr/bin/env python3
# Per-value memo arena: reproducible correctness tests that a single sqllogictest
# file cannot express — cross-restart reuse, concurrent same-host multi-process
# sharing, tricky-type disk round-trips, and scale/eviction correctness under churn.
#
# These exercise the columnar arena + its SQLite disk backend. Single-process,
# deterministic per-value behaviour is covered by test/sql/integration/cache/*.test;
# this harness owns the multi-process and scale dimensions.
#
# Run:   python3 test/per_value_disk_test.py
# Env:   VGI_HAYBARN        path to haybarn (default build/release/haybarn)
#        VGI_TEST_WORKER    per-value fixture worker command (must advertise
#                           vgi.cache.per_value; the vgi-python fixture does)
#        VGI_EXTENSION      path to vgi.duckdb_extension (default build/release/...)
#        VGI_PV_SCALE       distinct-value count for the scale test (default 500_000)
#        VGI_PV_THREADS     threads for the scale test (default 4)
#
# Scale it up:  VGI_PV_SCALE=5000000 python3 test/per_value_disk_test.py
# Exit code is non-zero if any assertion fails (CI-friendly).

import os
import re
import subprocess
import sys
import tempfile
import shutil
import time

_ANSI = re.compile(r"\x1b\[[0-9;]*m")  # enable_logging colours stdout; strip before parsing

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
HAYBARN = os.environ.get("VGI_HAYBARN", os.path.join(ROOT, "build/release/haybarn"))
EXTENSION = os.environ.get(
    "VGI_EXTENSION", os.path.join(ROOT, "build/release/extension/vgi/vgi.duckdb_extension")
)
WORKER = os.environ.get(
    "VGI_TEST_WORKER",
    f"uv run --project {os.path.expanduser('~/Development/vgi-python')} vgi-fixture-worker",
)
SCALE = int(os.environ.get("VGI_PV_SCALE", "500000"))
THREADS = int(os.environ.get("VGI_PV_THREADS", "4"))

PRELUDE = f"LOAD '{EXTENSION}';\nATTACH 'example' AS ex (TYPE vgi, LOCATION '{WORKER}');\n"

_failures = []


def run(sql, env=None):
    """Run one haybarn process on `sql`, return (stdout, rc). CSV mode for easy parsing."""
    script = ".mode csv\n.headers off\n" + PRELUDE + sql
    e = dict(os.environ)
    if env:
        e.update(env)
    p = subprocess.run(
        [HAYBARN, "-unsigned", "-init", "/dev/null"],
        input=script,
        env=e,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    return _ANSI.sub("", p.stdout), p.stderr, p.returncode


def run_bg(sql, env=None):
    """Launch a haybarn process in the background (for concurrency tests)."""
    script = ".mode csv\n.headers off\n" + PRELUDE + sql
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.Popen(
        [HAYBARN, "-unsigned", "-init", "/dev/null"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=e,
        text=True,
    ), script


def tagged(out, tag):
    """Return the CSV fields of the row whose first column is `tag` (or None)."""
    for line in out.splitlines():
        parts = [c.strip().strip('"') for c in line.split(",")]
        if parts and parts[0] == tag:
            return parts
    return None


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        _failures.append(name)


# ---------------------------------------------------------------------------
# 1. Cross-restart reuse: process 1 warms the memo; a FRESH process 2 (cold
#    memory) must serve it from disk with the worker NEVER called.
# ---------------------------------------------------------------------------
def test_cross_restart(dirroot):
    d = os.path.join(dirroot, "restart")
    os.makedirs(d)
    warm = f"SET vgi_result_cache_dir='{d}'; SET threads=1;\n"
    warm += "SELECT 'w', count(ex.cached_double_scalar(x % 5)) FROM range(20) g(x);\n"
    run(warm)

    cold = f"SET vgi_result_cache_dir='{d}'; SET threads=1;\n"
    cold += "CALL enable_logging(level := 'debug');\n"
    cold += "SELECT 'r', sum(ex.cached_double_scalar(x % 5))::BIGINT FROM range(20) g(x);\n"
    cold += (
        "SELECT 'wc', count(*) FROM duckdb_logs() WHERE type='VGI' "
        "AND regexp_extract(message, '''event'': ([a-z_.]+)', 1)='scalar.write_input' "
        "AND message LIKE '%cached_double_scalar%';\n"
    )
    out, _, _ = run(cold)
    # (0%5)*2 + ... over 20 rows: each of {0,2,4,6,8} appears 4x -> sum = 4*(0+2+4+6+8)=80
    r = tagged(out, "r")
    wc = tagged(out, "wc")
    check("cross_restart: value correct", r and r[1] == "80", f"got {r}")
    check("cross_restart: worker not called in fresh process", wc and wc[1] == "0", f"write_input={wc}")


# ---------------------------------------------------------------------------
# 2. Tricky-type disk round-trip: VARCHAR + NULL (cached_label) and lateral
#    1:N (cached_double) must hydrate exactly, worker never called.
# ---------------------------------------------------------------------------
def test_tricky_types(dirroot):
    d = os.path.join(dirroot, "tricky")
    os.makedirs(d)
    # '|' separator (not ',') so the aggregated string stays one CSV field.
    lbl = "string_agg(coalesce(ex.cached_label(x),'NULL'), '|' ORDER BY x)"
    warm = f"SET vgi_result_cache_dir='{d}'; SET threads=1;\n"
    warm += f"SELECT 'a', {lbl} FROM (VALUES (1),(2),(-3),(4),(-5)) t(x);\n"
    warm += "SELECT 'b', count(*), sum(doubled)::BIGINT FROM range(9) t(x), LATERAL ex.cached_double(x % 3);\n"
    run(warm)

    cold = f"SET vgi_result_cache_dir='{d}'; SET threads=1;\n"
    cold += "CALL enable_logging(level := 'debug');\n"
    cold += f"SELECT 'a', {lbl} FROM (VALUES (1),(2),(-3),(4),(-5)) t(x);\n"
    cold += "SELECT 'b', count(*), sum(doubled)::BIGINT FROM range(9) t(x), LATERAL ex.cached_double(x % 3);\n"
    cold += (
        "SELECT 'wc', count(*) FROM duckdb_logs() WHERE type='VGI' "
        "AND regexp_extract(message, '''event'': ([a-z_.]+)', 1) "
        "IN ('scalar.write_input','table_in_out.write_input');\n"
    )
    out, _, _ = run(cold)
    a = tagged(out, "a")
    b = tagged(out, "b")
    wc = tagged(out, "wc")
    check("tricky: VARCHAR+NULL round-trip", a and a[1] == "NULL|NULL|lbl-1|lbl-2|lbl-4", f"got {a}")
    check("tricky: lateral 1:N round-trip", b and b[1] == "9" and b[2] == "18", f"got {b}")
    check("tricky: worker not called in fresh process", wc and wc[1] == "0", f"write_input={wc}")


# ---------------------------------------------------------------------------
# 3. Concurrent same-host multi-process: two processes hammer the SAME sqlite
#    dir with overlapping value sets; both must return the correct sum with no
#    lock/corruption errors (the WAL claim).
# ---------------------------------------------------------------------------
def test_multiprocess(dirroot):
    d = os.path.join(dirroot, "mproc")
    os.makedirs(d)
    # 3000 distinct (0..2999), each *2, appearing 200x over 600k rows.
    expect = 200 * 2 * (2999 * 3000 // 2)
    sql = f"SET vgi_result_cache_dir='{d}'; SET threads=2;\n"
    sql += "CREATE TABLE t AS SELECT (i % 3000)::BIGINT v FROM range(600000) g(i);\n"
    sql += "SELECT 's', sum(ex.cached_double_scalar(v))::BIGINT FROM t;\n"
    sql += "SELECT 's', sum(ex.cached_double_scalar(v))::BIGINT FROM t;\n"
    pa, sa = run_bg(sql)
    pb, sb = run_bg(sql)
    oa, ea = pa.communicate(sa)
    ob, eb = pb.communicate(sb)
    ra, rb = tagged(oa, "s"), tagged(ob, "s")
    errs = [w for w in ("error", "malformed", "database is locked") if w in (ea + eb).lower()]
    check("multiprocess: proc A sum correct", ra and int(ra[1]) == expect, f"got {ra}, want {expect}")
    check("multiprocess: proc B sum correct", rb and int(rb[1]) == expect, f"got {rb}, want {expect}")
    check("multiprocess: no lock/corruption errors", not errs, f"{errs}")


# ---------------------------------------------------------------------------
# 4. Scale + eviction correctness: high cardinality with a TINY byte cap forces
#    arena eviction + compaction + reclamation churn across threads. The
#    per-value-ON result must EXACTLY equal per-value-OFF (no corruption), and
#    RSS must stay bounded. Scales with VGI_PV_SCALE.
# ---------------------------------------------------------------------------
def test_scale_eviction():
    n = SCALE
    rows = n * 2
    sql = f"SET threads={THREADS};\n"
    sql += f"CREATE TABLE t AS SELECT (i % {n})::BIGINT v FROM range({rows}) g(i);\n"
    sql += "SET vgi_result_cache_per_value=false;\n"
    sql += "SELECT 'off', count(*), sum(ex.cached_double_scalar(v))::HUGEINT FROM t;\n"
    sql += "SET vgi_result_cache_per_value=true;\n"
    sql += "SET vgi_result_cache_max_bytes=2000000;\n"  # 2 MB << N values -> heavy eviction
    sql += "SELECT 'on1', count(*), sum(ex.cached_double_scalar(v))::HUGEINT FROM t;\n"
    sql += "SELECT 'on2', count(*), sum(ex.cached_double_scalar(v))::HUGEINT FROM t;\n"
    t0 = time.time()
    out, err, rc = run(sql)
    dt = time.time() - t0
    off, on1, on2 = tagged(out, "off"), tagged(out, "on1"), tagged(out, "on2")
    ok_crash = rc == 0 and "error" not in err.lower()
    check(f"scale({n}): no crash", ok_crash, err.strip()[:120])
    same = off and on1 and on2 and off[1:] == on1[1:] == on2[1:]
    check(
        f"scale({n}): per-value ON == OFF under eviction",
        bool(same),
        f"off={off[1:] if off else None} on1={on1[1:] if on1 else None} on2={on2[1:] if on2 else None}",
    )
    print(f"       (scale={n}, threads={THREADS}, {dt:.1f}s)")


# ---------------------------------------------------------------------------
# 5. Soak: for VGI_PV_SOAK_SECONDS, hammer the disk-backed per-value tier with a
#    mix of workloads — concurrent multi-process writers/readers over one dir,
#    eviction pressure, cross-"restart" (a fresh process each iteration) — and
#    assert correctness holds EVERY iteration and the SQLite WAL stays bounded.
#    Off by default (0); a nightly/soak run sets e.g. VGI_PV_SOAK_SECONDS=600.
# ---------------------------------------------------------------------------
def test_soak(dirroot):
    secs = int(os.environ.get("VGI_PV_SOAK_SECONDS", "0"))
    if secs <= 0:
        return
    d = os.path.join(dirroot, "soak")
    os.makedirs(d)
    wal = os.path.join(d, "vgi_per_value.sqlite-wal")
    # Each iteration: N distinct over 2N rows (some reuse), 2 threads, disk on, a tiny
    # byte cap to force in-memory eviction (disk still holds everything). Result must
    # equal the analytic sum(2*(i%N)) for i in [0,2N).
    n = 20000
    rows = 2 * n
    analytic = 2 * sum(i % n for i in range(rows))
    sql = (
        f"SET vgi_result_cache_dir='{d}'; SET threads=2;\n"
        "SET vgi_result_cache_per_value_max_stores_per_chunk=0;\n"
        "SET vgi_result_cache_max_bytes=1000000;\n"
        f"CREATE TABLE t AS SELECT (i % {n})::BIGINT v FROM range({rows}) g(i);\n"
        "SELECT 's', sum(ex.cached_double_scalar(v))::HUGEINT FROM t;\n"
    )
    t_end = time.time() + secs
    it = 0
    max_wal = 0
    bad = 0
    concurrent = 0
    while time.time() < t_end:
        it += 1
        if it % 3 == 0:
            # concurrent pair on the same dir
            pa, sa = run_bg(sql)
            pb, sb = run_bg(sql)
            oa, _ = pa.communicate(sa)
            ob, _ = pb.communicate(sb)
            for o in (oa, ob):
                r = tagged(o, "s")
                if not (r and int(r[1]) == analytic):
                    bad += 1
            concurrent += 1
        else:
            out, _, _ = run(sql)
            r = tagged(out, "s")
            if not (r and int(r[1]) == analytic):
                bad += 1
        if os.path.exists(wal):
            max_wal = max(max_wal, os.path.getsize(wal))
    check(f"soak: correctness held all {it} iterations", bad == 0, f"{bad} bad")
    # WAL must stay bounded (autocheckpoint ~4 MB); allow generous 32 MB headroom.
    check(f"soak: WAL bounded (max {max_wal//1024} KB over {it} iters, {concurrent} concurrent)",
          max_wal < 32 * 1024 * 1024, f"max_wal={max_wal}")


# ---------------------------------------------------------------------------
# 6. Disk size cap + LRU eviction: with a small vgi_result_cache_per_value_disk_max_bytes,
#    memoize FAR more distinct values than fit. The SQLite store must stay under the cap
#    (LRU-evicting cold entries) and the result must stay exactly correct (evicted values
#    recompute).
# ---------------------------------------------------------------------------
def _sqlite_used_bytes(db):
    def pragma(p):
        try:
            return int(subprocess.run(["sqlite3", db, f"PRAGMA {p}"], capture_output=True, text=True).stdout or 0)
        except Exception:
            return 0
    return (pragma("page_count") - pragma("freelist_count")) * pragma("page_size")

def test_disk_cap(dirroot):
    d = os.path.join(dirroot, "cap")
    os.makedirs(d)
    cap = 3_000_000
    n = 300_000
    analytic = 2 * sum(range(n))
    sql = (
        f"SET vgi_result_cache_dir='{d}'; SET threads=1;\n"
        f"SET vgi_result_cache_per_value_disk_max_bytes={cap};\n"
        "SET vgi_result_cache_per_value_max_stores_per_chunk=0;\n"
        f"SELECT 's', sum(ex.cached_double_scalar(v))::HUGEINT FROM range({n}) g(v);\n"
    )
    out, _, _ = run(sql)
    r = tagged(out, "s")
    used = _sqlite_used_bytes(os.path.join(d, "vgi_per_value.sqlite"))
    check(f"disk_cap: correct under LRU eviction ({n} distinct)", r and int(r[1]) == analytic, f"got {r}")
    # Allow one eviction batch of headroom over the cap.
    check(f"disk_cap: SQLite bounded ({used//1024} KB <= {cap//1024} KB cap)", 0 < used <= cap + 512 * 1024,
          f"used={used} cap={cap}")


def main():
    if not os.path.exists(HAYBARN):
        print(f"haybarn not found at {HAYBARN} (set VGI_HAYBARN)", file=sys.stderr)
        return 2
    print(f"per-value disk/scale tests  (haybarn={HAYBARN})")
    print(f"  worker={WORKER}")
    dirroot = tempfile.mkdtemp(prefix="vgi_pv_")
    try:
        print("- cross-restart reuse")
        test_cross_restart(dirroot)
        print("- tricky-type disk round-trip")
        test_tricky_types(dirroot)
        print("- concurrent multi-process")
        test_multiprocess(dirroot)
        print(f"- scale + eviction correctness (VGI_PV_SCALE={SCALE})")
        test_scale_eviction()
        print("- disk size cap + LRU eviction")
        test_disk_cap(dirroot)
        soak = int(os.environ.get("VGI_PV_SOAK_SECONDS", "0"))
        if soak > 0:
            print(f"- soak ({soak}s: concurrent multi-process + eviction + disk)")
            test_soak(dirroot)
    finally:
        shutil.rmtree(dirroot, ignore_errors=True)
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {', '.join(_failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
