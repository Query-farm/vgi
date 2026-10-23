#!/usr/bin/env python3
"""Prove native BEGIN does not repeat allocation after losing its response.

The diagnostic subprocess allocates a token and exits before replying once.
An unpatched pooled client retries, allocates a second token, and succeeds.
The fixed client must surface an error after exactly one allocation. The first
token remains a provider-owned orphan in either case; no recovery is claimed.
Use --expect-retry only to capture the pre-fix regression on the build host.
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
            raise ValueError("Expected a JSON row array")
        rows.extend(batch)
        output = output.strip()[end:]
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-root", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--expect-retry", action="store_true")
    parser.add_argument("--expect-fresh-begin", action="store_true",
                        help="Capture the intermediate regression that disables pool reuse for BEGIN")
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    build = args.build.resolve()
    cli = next(path for path in (build / "haybarn", build / "duckdb") if path.is_file())
    extension = build / "extension/vgi/vgi.duckdb_extension"
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, [
        str(args.python_root.resolve()), environment.get("PYTHONPATH"),
    ]))
    results = []
    for lose_response in (False, True):
        name = "lost_begin_response" if lose_response else "normal_begin"
        case_dir = artifacts / name
        case_dir.mkdir()
        worker = shlex.join([
            "env", f"VGI_TRANSACTION_CLEANUP_PROBE_DIR={case_dir}",
            f"VGI_TRANSACTION_BEGIN_PROBE_DROP_REPLY={int(lose_response)}",
            str(args.python.absolute()), "-m", "tests.catalog.transaction_begin_probe",
        ])
        query_count = 1 if lose_response else 3
        script = (
            f"LOAD {quote(extension)};\n"
            f"ATTACH 'transaction_cleanup_probe' AS w (TYPE vgi, LOCATION {quote(worker)}, pool true);\n"
            + "SELECT id, label FROM w.main.items;\n" * query_count
        )
        sql_path = case_dir / "query.sql"
        sql_path.write_text(script, encoding="utf-8")
        completed = subprocess.run(
            [str(cli), "-unsigned", "-batch", "-bail", "-json", "-init", os.devnull,
             ":memory:", "-f", str(sql_path)],
            cwd=native, env=environment, text=True, capture_output=True, timeout=45, check=False,
        )
        (case_dir / "stdout.log").write_text(completed.stdout, encoding="utf-8")
        (case_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
        event_path = case_dir / "events.jsonl"
        events = [json.loads(line) for line in event_path.read_text().splitlines()] if event_path.exists() else []
        allocations = [event for event in events if event["event"] == "begin" and event["phase"] == "completed"]
        lost = [event for event in events if event["event"] == "begin" and event["phase"] == "response_lost"]
        completions = [event for event in events if event["event"] in {"commit", "rollback"}]
        active = {path.stem.removeprefix("active-") for path in case_dir.glob("active-*.json")}
        should_fail = lose_response and not args.expect_retry
        expected_allocations = 2 if lose_response and args.expect_retry else query_count
        seen_pids = set()
        reused_workers = []
        for event in events:
            if event["event"] == "begin" and event["phase"] == "completed":
                reused_workers.append(event["pid"] in seen_pids)
            seen_pids.add(event["pid"])
        checks = [
            (completed.returncode != 0) == should_fail,
            len(allocations) == expected_allocations,
            len({event["token"] for event in allocations}) == expected_allocations,
            len(lost) == int(lose_response),
            active == {event["token"] for event in lost},
            not any(event["token"] in active for event in completions),
        ]
        if should_fail:
            checks.extend([bool(completed.stderr.strip()), not completed.stdout.strip(), not completions])
        else:
            rows = json_rows(completed.stdout)
            returned_tokens = [event["token"] for event in allocations if event["token"] not in active]
            checks.extend([
                rows == [{"id": 1, "label": "tx:" + token} for token in returned_tokens],
                [(event["event"], event["phase"], event["token"]) for event in completions]
                == [("commit", phase, token) for token in returned_tokens for phase in ("attempt", "completed")],
            ])
        if not lose_response:
            checks.append(reused_workers == [not args.expect_fresh_begin] * query_count)
        result = {
            "case": name, "passed": all(checks), "expected_baseline_retry": args.expect_retry,
            "returncode": completed.returncode, "allocations": len(allocations),
            "begin_worker_pids": [event["pid"] for event in allocations],
            "begin_reused_worker": reused_workers,
            "abandoned_allocations": len(active), "checks": checks,
        }
        results.append(result)
        print(f"{'PASS' if result['passed'] else 'FAIL'} {name}: {json.dumps(result)}", flush=True)
    (artifacts / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    if not all(result["passed"] for result in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
