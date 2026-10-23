#!/usr/bin/env python3
"""Verify that a raw producer transport EOF cannot become a partial SQL result.

The private Python worker uses the real nested_sequence generator and framework.
It dies on its second tick, after the first output batch has reached the client.
The empty case filters that first batch away; the nonempty case exposes its 100
rows. Both queries require 10000 generated rows and must report failure on EOF.
Healthy controls traverse 99 empty batches before the final 10 matching rows,
complete an empty result normally, and reuse a connection across paginated splits.
"""

import argparse
import json
import os
import shlex
import subprocess
from pathlib import Path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-root", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument(
        "--expect-truncation",
        action="store_true",
        help="Capture the pre-fix launcher accepting incomplete output",
    )
    parser.add_argument(
        "--transport", choices=("launcher", "subprocess"), default="launcher"
    )
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    build = args.build.resolve()
    cli = next(path for path in (build / "haybarn", build / "duckdb") if path.is_file())
    extension = build / "extension/vgi/vgi.duckdb_extension"
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(
            None,
            [
                str(args.python_root.resolve()),
                environment.get("PYTHONPATH"),
            ],
        )
    )
    cases = [
        (
            "empty_boundary",
            "drop",
            "SELECT count(*) AS n FROM w.main.nested_sequence(10000, batch_size := 100) WHERE n >= 9990",
            0,
        ),
        (
            "nonempty_boundary",
            "drop",
            "SELECT count(*) AS n FROM w.main.nested_sequence(10000, batch_size := 100)",
            100,
        ),
        (
            "empty_batches",
            "healthy",
            "SELECT count(*) AS n FROM w.main.nested_sequence(10000, batch_size := 100) WHERE n >= 9990",
            10,
        ),
        (
            "empty_result",
            "healthy",
            "SELECT count(*) AS n FROM w.main.nested_sequence(0, batch_size := 100)",
            0,
        ),
        (
            "paginated_splits",
            "healthy",
            "SELECT count(*) AS n FROM w.main.split_paginated(n := 200, splits := 12)",
            200,
        ),
        (
            "split_boundary",
            "drop_split",
            "SELECT count(*) AS n FROM w.main.split_paginated(n := 10, splits := 2)",
            None,
        ),
    ]
    results = []
    for name, mode, query, expected_count in cases:
        drop = mode != "healthy"
        case_dir = artifacts / name
        case_dir.mkdir()
        event = case_dir / "dropped.txt"
        command = shlex.join(
            [
                "env",
                f"VGI_SCAN_TRUNCATION_MODE={mode}",
                f"VGI_SCAN_TRUNCATION_EVENT={event}",
                str(args.python.absolute()),
                "-m",
                "tests.scan_truncation_probe",
            ]
        )
        worker = "launch:" + command if args.transport == "launcher" else command
        script = (
            f"LOAD {quote(extension)};\n"
            f"ATTACH 'example' AS w (TYPE vgi, LOCATION {quote(worker)});\n"
            "SET threads=1;\n"
            f"{query};\n"
        )
        sql_path = case_dir / "query.sql"
        sql_path.write_text(script, encoding="utf-8")
        completed = subprocess.run(
            [
                str(cli),
                "-unsigned",
                "-batch",
                "-bail",
                "-json",
                "-init",
                os.devnull,
                ":memory:",
                "-f",
                str(sql_path),
            ],
            cwd=native,
            env=environment,
            text=True,
            capture_output=True,
            timeout=45,
            check=False,
        )
        (case_dir / "stdout.log").write_text(completed.stdout, encoding="utf-8")
        (case_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
        should_fail = drop and (not args.expect_truncation or mode == "drop_split")
        checks = [event.exists() == drop, (completed.returncode != 0) == should_fail]
        if should_fail:
            expected_error = (
                "Failed to close input writer between splits"
                if args.expect_truncation
                else "before Arrow EOS"
            )
            checks.extend(
                [not completed.stdout.strip(), expected_error in completed.stderr]
            )
        else:
            rows = json.loads(completed.stdout) if completed.stdout.strip() else None
            checks.append(rows == [{"n": expected_count}])
        result = {
            "case": name,
            "transport": args.transport,
            "passed": all(checks),
            "expected_baseline_truncation": args.expect_truncation,
            "returncode": completed.returncode,
            "injected_drop": event.exists(),
            "checks": checks,
        }
        results.append(result)
        print(
            f"{'PASS' if result['passed'] else 'FAIL'} {name}: {json.dumps(result)}",
            flush=True,
        )
    (artifacts / "results.json").write_text(
        json.dumps(results, indent=2) + "\n", encoding="utf-8"
    )
    raise SystemExit(0 if all(result["passed"] for result in results) else 1)


if __name__ == "__main__":
    main()
