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
    if value_size_bytes <= 8:
        value_expr = "i::BIGINT AS v"
    else:
        # repeat() makes a STRING of N chars; works in input via VARCHAR.
        value_expr = f"repeat('x', {value_size_bytes}) AS v"

    # The buffered shape reads `count` from buffer_input output (rows match
    # input rows). The streaming shape reads `count` from echo output.
    func = "buffer_input" if shape == "buffered" else "echo"

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
    shapes = ["streaming", "buffered"]

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
                if not (row["streaming_sec"] != row["streaming_sec"]  # nan check
                        or row["buffered_sec"] != row["buffered_sec"]):
                    row["overhead_ratio"] = row["buffered_sec"] / row["streaming_sec"]
                else:
                    row["overhead_ratio"] = float("nan")
                rows_out.append(row)
                print(f"rows={rows:>9} threads={threads} value={vsize:>5}B  "
                      f"streaming={row['streaming_sec']:.3f}s  "
                      f"buffered={row['buffered_sec']:.3f}s  "
                      f"ratio={row['overhead_ratio']:.2f}x")

    cols = ["rows", "threads", "value_size_bytes", "streaming_sec", "buffered_sec", "overhead_ratio"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in rows_out:
            w.writerow(row)
    print(f"\nWrote {len(rows_out)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
