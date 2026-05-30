"""CLI entrypoint for the VGI stress harness.

    <haybarn-venv-python> -m scripts.stress --target both --mode correctness

Exit code is non-zero if any correctness/soak assertion failed, so it composes
with CI or a shell `&&` chain.
"""

from __future__ import annotations

import argparse
import csv
import sys

from .common import DEFAULT_WORKER, StressConfig, haybarn_python


def _parse_concurrency(spec: str) -> list[int]:
    return [int(x) for x in spec.split(",") if x.strip()]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m scripts.stress",
        description="Stress the VGI worker pool and launcher via haybarn-python. "
                    f"Run with the haybarn venv python:\n  {haybarn_python()} -m scripts.stress ...",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--target", choices=["pool", "launcher", "both"], default="both")
    p.add_argument("--mode", choices=["correctness", "throughput", "soak"],
                   default="correctness")
    p.add_argument("--concurrency", default="8",
                   help="Comma-separated levels (throughput ramps over all; other "
                        "modes use the first). Default: 8")
    p.add_argument("--queries", type=int, default=50,
                   help="Queries per worker/connection (default 50)")
    p.add_argument("--duration", type=float, default=60.0,
                   help="Soak duration in seconds (default 60)")
    p.add_argument("--worker", default=DEFAULT_WORKER,
                   help="VGI worker command (default: the fixture worker)")
    p.add_argument("--idle-timeout", type=int, default=30,
                   help="launcher_idle_timeout seconds (default 30)")
    p.add_argument("--pool-max", type=int, default=32,
                   help="Per-path pooled-worker cap asserted by pool mode (default 32)")
    p.add_argument("--pool-timeout", type=int, default=1,
                   help="pool_timeout seconds used by pool soak (default 1)")
    p.add_argument("--no-faults", dest="faults", action="store_false",
                   help="Disable fault injection (happy-path load only)")
    p.add_argument("--csv", default=None, help="Write throughput rows to this CSV path")
    return p


def _write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    # union of keys preserves both pool and launcher columns
    fields: list[str] = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"wrote {len(rows)} rows -> {path}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = StressConfig(
        target=args.target,
        mode=args.mode,
        concurrency=_parse_concurrency(args.concurrency),
        queries=args.queries,
        duration=args.duration,
        worker=args.worker,
        idle_timeout=args.idle_timeout,
        faults=args.faults,
        csv=args.csv,
        pool_max=args.pool_max,
        pool_timeout=args.pool_timeout,
    )

    # Imported lazily so --help works without haybarn on the path.
    from . import launcher, pool

    all_failures: list[str] = []
    all_rows: list[dict] = []
    targets = ["pool", "launcher"] if cfg.target == "both" else [cfg.target]
    for t in targets:
        mod = pool if t == "pool" else launcher
        fails, rows = mod.run(cfg)
        all_failures += [f"[{t}] {m}" for m in fails]
        all_rows += rows

    if cfg.csv:
        _write_csv(cfg.csv, all_rows)

    print("\n" + "=" * 60)
    if all_failures:
        print(f"FAILED: {len(all_failures)} assertion(s)")
        for m in all_failures:
            print(f"  - {m}")
        return 1
    print("PASSED: no assertion failures")
    return 0


if __name__ == "__main__":
    sys.exit(main())
