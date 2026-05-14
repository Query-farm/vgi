#!/usr/bin/env python3
"""Microbenchmark for the buffered_table operator.

Times `SELECT count(*) FROM example.buffer_input(...)` across row counts,
thread settings, and value sizes, and compares against the streaming
`echo` shape on the same input. The buffered/streaming delta is the
per-chunk RPC roundtrip overhead.

Characterization only — no regression gates. Writes a CSV row per run.

Usage:
    python3 scripts/bench_buffered_table.py [--build release|debug] \\
        [--out /tmp/bench.csv] [--quick]

`--quick` runs a smaller matrix (~30 s total). Without it the full matrix
runs ~5–10 min.
"""

from __future__ import annotations

import argparse
import csv
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def cell(label: str, value: float) -> str:
    return f"{label}={value:.3f}s"


def run_one(
    duckdb: Path, ext: Path, worker: str, *,
    shape: str, rows: int, threads: int, value_size_bytes: int,
) -> float:
    """Run one timed query; return wall time in seconds."""
    # We build an input row of approximately `value_size_bytes` by repeating
    # a string and casting to BLOB-equivalent. Simpler: a repeat() of length B.
    # Exception: sum_all_columns only accepts numeric columns, so for the
    # buffered_agg shape the value column is always BIGINT regardless of the
    # value_size axis. That axis is meaningful for streaming + buffered_pass
    # only — measuring IPC payload size impact.
    if value_size_bytes <= 8:
        value_expr = "i::BIGINT AS v"
    else:
        # repeat() makes a STRING of N chars; works in input via VARCHAR.
        value_expr = f"repeat('x', {value_size_bytes}) AS v"

    # Three shapes:
    #   streaming     — echo (passthrough TableInOut)
    #   buffered_pass — buffer_input (TIO + per-batch IPC serialization into
    #                   Python bytes; dominates measurements at large value
    #                   sizes — that's fixture cost, not operator cost)
    #   buffered_agg  — sum_all_columns (constant-size state; the operator-
    #                   only signal). The buffered_agg ↔ streaming delta is
    #                   the cleanest measure of per-chunk RPC overhead.
    func_map = {
        "streaming":     "echo",
        "buffered_pass": "buffer_input",
        "buffered_agg":  "sum_all_columns",
    }
    func = func_map[shape]
    # sum_all_columns can't process string values; override to numeric input
    # for the agg shape regardless of value_size_bytes. (The buffered_agg
    # measurement is operator-overhead-only and intentionally doesn't scale
    # with payload size.)
    if shape == "buffered_agg":
        value_expr = "i::BIGINT AS v"

    sql = f"""
LOAD '{ext}';
ATTACH 'example' AS example (TYPE vgi, LOCATION getenv('VGI_TEST_WORKER'));
SET threads = {threads};
SELECT count(*) FROM example.{func}(
    (SELECT {value_expr} FROM range({rows}) AS t(i))
);
"""

    env = os.environ.copy()
    env["VGI_TEST_WORKER"] = worker

    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(sql)
        sqlfile = f.name

    try:
        t0 = time.perf_counter()
        result = subprocess.run(
            [str(duckdb), "-unsigned", "-f", sqlfile],
            capture_output=True, text=True, env=env, timeout=600,
        )
        elapsed = time.perf_counter() - t0
        if result.returncode != 0:
            print(f"  FAIL ({shape} N={rows} threads={threads} value={value_size_bytes}B):", file=sys.stderr)
            print(f"    stdout: {result.stdout[-400:]}", file=sys.stderr)
            print(f"    stderr: {result.stderr[-400:]}", file=sys.stderr)
            return float("nan")
        return elapsed
    finally:
        os.unlink(sqlfile)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default="release", choices=["release", "debug"])
    ap.add_argument("--out", default="/tmp/bench_buffered_table.csv")
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()

    repo = Path(__file__).resolve().parent.parent
    duckdb = repo / "build" / args.build / "duckdb"
    ext = repo / "build" / args.build / "extension" / "vgi" / "vgi.duckdb_extension"
    if not duckdb.exists() or not ext.exists():
        print(f"FAIL: duckdb or extension not built under build/{args.build}/", file=sys.stderr)
        return 1

    worker = os.environ.get(
        "VGI_TEST_WORKER",
        "uv run --project " + os.path.expanduser("~/Development/vgi-python") + " vgi-fixture-worker",
    )

    if args.quick:
        rows_set = [1_000, 100_000]
        threads_set = [1, 4]
        value_sizes = [8, 1024]
    else:
        rows_set = [1_000, 100_000, 1_000_000]
        threads_set = [1, 4, 8]
        value_sizes = [8, 1024, 65536]
    shapes = ["streaming", "buffered_pass", "buffered_agg"]

    rows_out: list[dict] = []
    for rows in rows_set:
        for threads in threads_set:
            for vsize in value_sizes:
                row: dict = {"rows": rows, "threads": threads, "value_size_bytes": vsize}
                for shape in shapes:
                    # Warm pool (one run discarded) so the first cold-spawn
                    # doesn't taint the measurement.
                    run_one(duckdb, ext, worker, shape=shape, rows=rows,
                            threads=threads, value_size_bytes=vsize)
                    # Time-of-record: median of 3.
                    timings = sorted(
                        run_one(duckdb, ext, worker, shape=shape, rows=rows,
                                threads=threads, value_size_bytes=vsize)
                        for _ in range(3)
                    )
                    row[f"{shape}_sec"] = timings[1]
                # Two ratios — pass exposes fixture cost, agg the operator only.
                def _safe(num: float, den: float) -> float:
                    if num != num or den != den or den == 0:
                        return float("nan")
                    return num / den
                row["pass_ratio"] = _safe(row["buffered_pass_sec"], row["streaming_sec"])
                row["agg_ratio"]  = _safe(row["buffered_agg_sec"],  row["streaming_sec"])
                rows_out.append(row)
                print(f"rows={rows:>9} threads={threads} value={vsize:>5}B  "
                      f"stream={row['streaming_sec']:.3f}s  "
                      f"buf_pass={row['buffered_pass_sec']:.3f}s({row['pass_ratio']:.2f}x)  "
                      f"buf_agg={row['buffered_agg_sec']:.3f}s({row['agg_ratio']:.2f}x)")

    cols = ["rows", "threads", "value_size_bytes",
            "streaming_sec", "buffered_pass_sec", "buffered_agg_sec",
            "pass_ratio", "agg_ratio"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in rows_out:
            w.writerow(row)
    print(f"\nWrote {len(rows_out)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
