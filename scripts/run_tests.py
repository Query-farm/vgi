#!/usr/bin/env python3
"""Parallel test runner for the VGI DuckDB extension.

Discovers ``.test`` files matching DuckDB-unittest-style glob patterns and
runs each one in its own ``./build/<build>/test/unittest <file>`` subprocess,
N at a time. Designed for workloads where unittest is mostly waiting on
external I/O (Cloudflare DO storage, HTTP transport) so serial execution
wastes wall-clock time.

Pattern semantics match the way the Makefile targets pass arguments to
``unittest``:

  - ``test/*`` and ``test/sql/foo/*`` are recursive — ``*`` matches any
    characters including ``/``.
  - A leading ``~`` excludes any path matching that pattern. Excludes
    are applied after includes.
  - All matching files must end in ``.test``.

Env vars (``VGI_TEST_WORKER``, ``VGI_WORKER_SHARED_STORAGE``,
``VGI_CF_DO_URL``, ``VGI_CF_DO_TOKEN``, etc.) are read from the current
process environment and propagated to every child unittest. The runner
itself does not interpret them.

Exit code is 0 iff every test passed.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import os
import re
import statistics
import subprocess
import sys
import time
from collections.abc import Iterable
from pathlib import Path


# --- ANSI colours (auto-disabled when stdout is not a terminal) ---

_USE_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


GREEN = lambda s: _c("32", s)  # noqa: E731
RED = lambda s: _c("31", s)  # noqa: E731
YELLOW = lambda s: _c("33", s)  # noqa: E731
DIM = lambda s: _c("2", s)  # noqa: E731


# --- Pattern matching ---


def _pattern_to_regex(pattern: str) -> re.Pattern[str]:
    """Convert a DuckDB-unittest-style glob to a regex.

    ``*`` matches any character sequence *including* path separators.
    Everything else is literal. ``test/*`` therefore matches every
    descendant under ``test/``, the way the Makefile targets expect.
    """
    return re.compile("^" + re.escape(pattern).replace(r"\*", ".*") + "$")


def discover(patterns: list[str], root: Path) -> list[Path]:
    """Walk ``root`` for ``.test`` files matching the include/exclude patterns."""
    includes = [p for p in patterns if not p.startswith("~")]
    excludes = [p[1:] for p in patterns if p.startswith("~")]

    if not includes:
        raise SystemExit("at least one include pattern is required")

    include_rxs = [_pattern_to_regex(p) for p in includes]
    exclude_rxs = [_pattern_to_regex(p) for p in excludes]

    matched: set[Path] = set()
    for p in root.rglob("*.test"):
        rel = str(p.relative_to(root))
        if not any(rx.match(rel) for rx in include_rxs):
            continue
        if any(rx.match(rel) for rx in exclude_rxs):
            continue
        matched.add(p.relative_to(root))
    return sorted(matched)


# --- Execution ---


def run_one(test_path: Path, unittest: Path, env: dict[str, str]) -> dict:
    """Invoke ``unittest <test_path>`` and capture everything."""
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            [str(unittest), str(test_path)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        exit_code = proc.returncode
        stdout = proc.stdout
        stderr = proc.stderr
        error = None
    except (OSError, subprocess.SubprocessError) as exc:
        # Treat infrastructural failures (binary missing, exec failure, etc.)
        # as test failures so we surface them in the summary rather than
        # crashing the runner.
        exit_code = -1
        stdout = ""
        stderr = ""
        error = repr(exc)
    return {
        "path": test_path,
        "exit": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "duration": time.monotonic() - t0,
        "error": error,
    }


def _format_result_line(idx: int, total: int, result: dict) -> str:
    """One-line live progress message."""
    status = (
        GREEN("PASS") if result["exit"] == 0 else RED("FAIL")
    )
    dur = f"{result['duration']:6.2f}s"
    return f"[{idx:>3}/{total}] {status} {result['path']} {DIM(f'({dur})')}"


def _print_failure(result: dict, idx: int, total: int) -> None:
    """Full inline output for a failed test."""
    print()
    print(RED("=" * 70))
    print(RED(f"FAIL [{idx}/{total}]: {result['path']} "
              f"(exit={result['exit']}, {result['duration']:.2f}s)"))
    if result.get("error"):
        print(RED(f"runner error: {result['error']}"))
    print(RED("=" * 70))
    if result["stdout"]:
        print(DIM("--- stdout ---"))
        print(result["stdout"].rstrip())
    if result["stderr"]:
        print(DIM("--- stderr ---"))
        print(result["stderr"].rstrip())
    print()


# --- Summary ---


def _print_summary(
    results: list[dict],
    elapsed: float,
    total_serial: float,
    *,
    top_n: int,
    show_all_timings: bool,
) -> None:
    """Pass/fail counts, slowest tests, duration distribution, speedup ratio."""
    passes = [r for r in results if r["exit"] == 0]
    fails = [r for r in results if r["exit"] != 0]
    durations = [r["duration"] for r in results]

    print()
    print("─" * 70)
    speedup = total_serial / elapsed if elapsed > 0 else 0.0
    print(
        f"Ran {len(results)} tests in {elapsed:.2f}s "
        f"(serial would be ~{total_serial:.2f}s, speedup {speedup:.1f}×)"
    )
    print(f"  {GREEN(f'passed: {len(passes)}')}")
    if fails:
        print(f"  {RED(f'failed: {len(fails)}')}")

    # Duration distribution — picks up patterns (one slow outlier vs the
    # whole suite being slow) at a glance.
    if durations:
        ds = sorted(durations)
        n = len(ds)
        p50 = statistics.median(ds)
        p95 = ds[min(int(n * 0.95), n - 1)]
        p99 = ds[min(int(n * 0.99), n - 1)]
        mean = statistics.fmean(ds)
        total_cpu = sum(ds)
        print(
            f"  {DIM(f'duration: mean={mean:.2f}s p50={p50:.2f}s '
                     f'p95={p95:.2f}s p99={p99:.2f}s max={ds[-1]:.2f}s '
                     f'sum={total_cpu:.1f}s')}"
        )

    # Slowest tests — what to attack to shrink wall-clock. With per-test
    # cold-start in the hundreds of ms, the long tail usually dominates.
    if show_all_timings:
        ordered = sorted(results, key=lambda r: -r["duration"])
        print()
        print(DIM(f"all tests by duration ({len(ordered)}):"))
        for r in ordered:
            mark = GREEN("✓") if r["exit"] == 0 else RED("✗")
            print(DIM(f"  {mark} {r['duration']:6.2f}s  {r['path']}"))
    elif results:
        slowest = sorted(results, key=lambda r: -r["duration"])[:top_n]
        print()
        print(DIM(f"top {len(slowest)} slowest:"))
        for r in slowest:
            mark = GREEN("✓") if r["exit"] == 0 else RED("✗")
            print(DIM(f"  {mark} {r['duration']:6.2f}s  {r['path']}"))


def _write_timings_csv(results: list[dict], path: Path) -> None:
    """Dump every test's duration + status to CSV for trend analysis."""
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["test_path", "duration_seconds", "exit_code", "status"])
        for r in results:
            w.writerow([
                str(r["path"]),
                f"{r['duration']:.6f}",
                r["exit"],
                "PASS" if r["exit"] == 0 else "FAIL",
            ])


# --- CLI ---


def _find_unittest(root: Path, build: str) -> Path:
    p = root / "build" / build / "test" / "unittest"
    if not p.exists():
        raise SystemExit(
            f"unittest binary not found at {p}\n"
            f"Run `make {build}` first."
        )
    return p


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "patterns",
        nargs="+",
        help="DuckDB-unittest-style glob patterns. Prefix with ~ to exclude.",
    )
    parser.add_argument(
        "-j", "--jobs",
        type=int,
        default=int(os.environ.get("VGI_RUN_TESTS_JOBS", "8")),
        help="Number of tests to run concurrently (default: 8, "
        "override with VGI_RUN_TESTS_JOBS).",
    )
    parser.add_argument(
        "--build",
        default=os.environ.get("VGI_RUN_TESTS_BUILD", "release"),
        choices=("release", "debug"),
        help="Which build's unittest binary to invoke (default: release).",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Project root (defaults to the repo containing this script).",
    )
    parser.add_argument(
        "--show-passes",
        action="store_true",
        help="Print live PASS lines as tests complete (default: true if TTY).",
    )
    parser.add_argument(
        "--no-show-passes",
        dest="show_passes",
        action="store_false",
        help="Suppress PASS lines; only show FAILs and the summary.",
    )
    parser.set_defaults(show_passes=sys.stdout.isatty())
    parser.add_argument(
        "--top",
        type=int,
        default=15,
        help="How many slowest tests to list in the summary (default: 15).",
    )
    parser.add_argument(
        "--all-timings",
        action="store_true",
        help="List every test sorted by duration, not just the top N.",
    )
    parser.add_argument(
        "--timings-csv",
        type=Path,
        default=None,
        help="Also write all per-test durations to this CSV file "
        "(columns: test_path, duration_seconds, exit_code, status).",
    )
    args = parser.parse_args(argv)

    unittest = _find_unittest(args.root, args.build)
    tests = discover(args.patterns, args.root)
    if not tests:
        print(YELLOW("no tests matched the given patterns"), file=sys.stderr)
        return 1

    env = os.environ.copy()
    print(
        f"Discovered {len(tests)} tests under {args.root} — "
        f"running with -j{args.jobs} against {unittest.name} ({args.build})",
        file=sys.stderr,
    )

    results: list[dict] = []
    total_serial = 0.0  # sum of per-test durations — what a serial run would take

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = {pool.submit(run_one, t, unittest, env): t for t in tests}
        completed = 0
        for fut in concurrent.futures.as_completed(futures):
            r = fut.result()
            results.append(r)
            total_serial += r["duration"]
            completed += 1
            if r["exit"] == 0:
                if args.show_passes:
                    print(_format_result_line(completed, len(tests), r))
            else:
                _print_failure(r, completed, len(tests))
                print(_format_result_line(completed, len(tests), r))

    _print_summary(
        results,
        time.monotonic() - t0,
        total_serial,
        top_n=args.top,
        show_all_timings=args.all_timings,
    )
    if args.timings_csv is not None:
        _write_timings_csv(results, args.timings_csv)
        print(DIM(f"wrote timings to {args.timings_csv}"), file=sys.stderr)
    return 1 if any(r["exit"] != 0 for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
