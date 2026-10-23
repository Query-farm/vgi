#!/usr/bin/env python3
"""Observe native 1.5 INSERT/finalize behavior using an opt-in diagnostic sink.

This is a focused research runner, including an expected unsupported deferred
RETURNING case. The provider does not implement storage or transactions; these
checks establish callback/result/error behavior, never rollback or atomicity.
Run on the build host. Every case gets fresh diagnostic/framework storage.
"""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def json_rows(output):
    rows = []
    decoder = json.JSONDecoder()
    while output.strip():
        batch, end = decoder.raw_decode(output.strip())
        if not isinstance(batch, list):
            raise AssertionError(f"Expected JSON row array: {batch!r}")
        rows.extend(batch)
        output = output.strip()[end:]
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    build = args.build.resolve() if args.build else native / "build/release"
    python_root = args.python.resolve()
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    cli = next(path for path in (build / "haybarn", build / "duckdb") if path.is_file())
    extension = build / "extension/vgi/vgi.duckdb_extension"
    pair = "VALUES (1, 'one'), (2, 'two')"
    cases = [
        ("input_returning", "input_result", pair, True, 2, False, None),
        ("emit_then_raise", "emit_then_raise", pair, True, 2, False,
         "write_finalize_probe: process raised after buffered emit"),
        ("finalize_count", "finalize_count", pair, False, 2, True, None),
        ("finalize_multiple_batches", "finalize_count",
         "SELECT i, 'row' FROM range(5000) AS t(i)", False, 5000, True, None),
        ("finalize_zero_input", "finalize_count",
         "SELECT i, 'row' FROM range(0) AS t(i)", False, 0, True, None),
        ("finalize_raise", "finalize_raise", pair, False, 2, True,
         "write_finalize_probe: finalize raised"),
        ("deferred_returning", "deferred_returning", pair, True, 2, True,
         "incompatible count result"),
    ]
    results = []
    for name, table, source, returning, expected_input, finalized, error in cases:
        case_dir = artifacts / name
        case_dir.mkdir()  # Refuse to mix evidence from different executions.
        worker = shlex.join([
            "env", f"VGI_WRITE_FINALIZE_PROBE_DIR={case_dir}",
            "uv", "--directory", str(python_root), "run", "--no-sync",
            "python", "-m", "tests.catalog.write_finalize_probe",
        ])
        script = (
            f"LOAD {quote(extension)};\n"
            f"ATTACH 'write_finalize_probe' AS w (TYPE vgi, LOCATION {quote(worker)});\n"
            f"INSERT INTO w.main.{table} {source}"
            + (" RETURNING *" if returning else "") + ";\n"
        )
        sql_path = case_dir / "query.sql"
        sql_path.write_text(script)
        completed = subprocess.run(
            [str(cli), "-unsigned", "-batch", "-bail", "-json", "-init",
             os.devnull, ":memory:", "-f", str(sql_path)],
            cwd=native, text=True, capture_output=True, timeout=45,
        )
        (case_dir / "stdout.log").write_text(completed.stdout)
        (case_dir / "stderr.log").write_text(completed.stderr)
        event_path = case_dir / "events.jsonl"
        events = [json.loads(line) for line in event_path.read_text().splitlines()] if event_path.exists() else []
        checks = []
        checks.append((completed.returncode != 0) if error else completed.returncode == 0)
        if error:
            checks.append(error in completed.stderr)
            checks.append(not completed.stdout.strip())
        elif returning:
            checks.append(json_rows(completed.stdout) == [
                {"id": 1, "label": "one"}, {"id": 2, "label": "two"},
            ])
        inputs = [event for event in events if event["phase"] == "input"]
        final = [event for event in events if event["phase"] == "finalize"]
        checks.append(sum(event["rows"] for event in inputs) == expected_input)
        checks.append(len(final) == (1 if finalized else 0))
        if finalized and final:
            checks.append(final[0]["received_rows"] == expected_input)
            checks.append(all(event["execution_id"] == final[0]["execution_id"] for event in inputs))
        if name == "finalize_multiple_batches":
            checks.append(len(inputs) > 1)
        if name == "emit_then_raise":
            checks.append(any(event["phase"] == "buffered_emit" for event in events))
            checks.append(any(event["phase"] == "input_raise" for event in events))
        if name == "finalize_raise":
            checks.append(any(event["phase"] == "finalize_raise" for event in events))
        if name == "deferred_returning":
            checks.append(any(event["phase"] == "finalize_return" and event["rows"] == 2
                              and event["columns"] == ["id", "label"] for event in events))
        passed = all(checks)
        result = {
            "case": name, "passed": passed, "returncode": completed.returncode,
            "expected_error": error, "input_batches": len(inputs),
            "input_rows": sum(event["rows"] for event in inputs),
            "finalize_callbacks": len(final), "checks": checks,
        }
        results.append(result)
        print(f"{'PASS' if passed else 'FAIL'} {name}: {json.dumps(result)}", flush=True)
    (artifacts / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    if not all(result["passed"] for result in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
