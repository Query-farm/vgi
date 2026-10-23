#!/usr/bin/env python3
"""Run native transaction cleanup tests and verify isolated provider traces.

This checks callback counts, transaction ownership and native lifetime recovery.
The provider is diagnostic: no user data atomicity or durable recovery claim is
made. Run on the build host with the existing vgi_catalog_tests binary.
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

CASES = [
    (operation, mode)
    for operation in ("commit", "rollback")
    for mode in ("success", "reject", "after_completion_error")
]
TOKEN = re.compile(r"[0-9a-f]{32}\Z")


def read_jsonl(path):
    records = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise TypeError(f"{path.name}:{number}: expected an object")
        records.append(value)
    return records


def validate_case(case_dir, operation, mode):
    errors = []

    def check(condition, message):
        if not condition:
            errors.append(message)

    expected = read_jsonl(case_dir / "expected.jsonl")
    events = read_jsonl(case_dir / "events.jsonl")
    check(len(expected) == 3, f"expected three tested cycles, got {len(expected)}")
    expected_tokens = set()
    for record in expected:
        token = record.get("token")
        if not isinstance(token, str) or not TOKEN.fullmatch(token):
            raise ValueError(f"Invalid expected token: {token!r}")
        check(token not in expected_tokens, f"Repeated expected token {token}")
        expected_tokens.add(token)
        check(record.get("operation") == operation, f"Wrong expected operation for {token}: {record!r}")
        check(record.get("mode") == mode, f"Wrong expected mode for {token}: {record!r}")

    by_token = {}
    for index, event in enumerate(events):
        token = event.get("token")
        attach = event.get("attach")
        if not isinstance(attach, str) or not TOKEN.fullmatch(attach):
            raise ValueError(f"Event {index} has invalid attachment: {event!r}")
        if token is None:
            check(event.get("event") == "attach", f"Tokenless non-attach event: {event!r}")
            continue
        if not isinstance(token, str) or not TOKEN.fullmatch(token):
            raise ValueError(f"Event {index} has invalid token: {event!r}")
        by_token.setdefault(token, []).append((index, event))

    check(expected_tokens <= by_token.keys(), "Some expected tokens have no provider events")
    remaining = set()
    for path in case_dir.glob("active-*.json"):
        token = path.stem.removeprefix("active-")
        if not TOKEN.fullmatch(token):
            raise ValueError(f"Invalid active-state filename: {path.name}")
        remaining.add(token)
        active = json.loads(path.read_text(encoding="utf-8"))
        owners = {event["attach"] for _, event in by_token.get(token, [])}
        check(owners == {active.get("attach")}, f"Active-file owner differs from events for {token}")

    rejected_tokens = expected_tokens if mode == "reject" else set()
    check(remaining == rejected_tokens, f"Active tokens: expected {sorted(rejected_tokens)}, got {sorted(remaining)}")
    spans = {}
    for token, records in by_token.items():
        begins = [(index, event) for index, event in records if event.get("event") == "begin"]
        check(len(begins) == 1, f"Expected one begin for {token}, got {len(begins)}")
        if begins:
            check(begins[0][1].get("phase") == "completed", f"Begin did not complete for {token}")
            check(begins[0][0] == records[0][0], f"Events precede begin for {token}")
        owners = {event["attach"] for _, event in records}
        check(len(owners) == 1, f"Token crossed attachment owners: {token}")
        completions = [(index, event) for index, event in records if event.get("event") in {"commit", "rollback"}]
        attempts = [(index, event) for index, event in completions if event.get("phase") == "attempt"]
        check(len(attempts) == 1, f"Expected exactly one completion attempt for {token}, got {len(attempts)}")

        tested = token in expected_tokens
        expected_operation = operation if tested else (attempts[0][1]["event"] if attempts else None)
        token_mode = mode if tested else "success"
        phases = {
            "success": ["attempt", "completed"],
            "reject": ["attempt", "rejected"],
            "after_completion_error": ["attempt", "completed", "response_error"],
        }[token_mode]
        actual_phases = [event.get("phase") for _, event in completions]
        check(actual_phases == phases, f"Completion phases for {token}: expected {phases}, got {actual_phases}")
        check(
            all(event.get("event") == expected_operation for _, event in completions),
            f"Opposite or compensating completion operation for {token}",
        )
        check(
            all(event.get("mode") == token_mode for _, event in completions if event.get("phase") != "attempt"),
            f"Wrong terminal mode for {token}",
        )
        if attempts:
            check(
                not any(index > attempts[0][0] and event.get("event") == "scan" for index, event in records),
                f"Token scanned after completion was attempted: {token}",
            )
        if tested:
            check(any(event.get("event") == "scan" for _, event in records), f"No real scan for tested token {token}")
        if begins and completions:
            spans[token] = (begins[0][0], completions[-1][0], expected_operation)

    other_tokens = set(by_token) - expected_tokens
    check(len(other_tokens) >= 4, "Missing three recovery transactions and the second-connection transaction")
    if expected_tokens <= spans.keys() and expected_tokens:
        first_begin = min(spans[token][0] for token in expected_tokens)
        last_finish = max(spans[token][1] for token in expected_tokens)
        surviving = [
            token for token in other_tokens & spans.keys()
            if spans[token][0] < first_begin and spans[token][1] > last_finish and spans[token][2] == "rollback"
        ]
        check(bool(surviving), "No second-connection transaction survived all three tested cycles")
        ordered = sorted(expected_tokens, key=lambda token: spans[token][0])
        for index, token in enumerate(ordered):
            boundary = spans[ordered[index + 1]][0] if index + 1 < len(ordered) else len(events)
            recoveries = [
                other for other in other_tokens & spans.keys()
                if spans[token][1] < spans[other][0] < spans[other][1] < boundary
                and spans[other][2] == "rollback"
            ]
            check(bool(recoveries), f"No independent recovery transaction rolled back after cycle token {token}")

    control_path = case_dir / "control.json"
    if control_path.exists():
        controls = json.loads(control_path.read_text(encoding="utf-8"))
        check(not controls, f"Unconsumed failure controls: {controls!r}")
    return {
        "case": case_dir.name,
        "passed": not errors,
        "operation": operation,
        "mode": mode,
        "expected_transactions": len(expected_tokens),
        "observed_transactions": len(by_token),
        "remaining_active": sorted(remaining),
        "errors": errors,
    }


def output_text(value):
    return value.decode(errors="replace") if isinstance(value, bytes) else (value or "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--python-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=240)
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cases_dir = output_dir / "cases"
    cases_dir.mkdir()  # Never mix callbacks or active state from different runs.
    environment = os.environ.copy()
    environment["VGI_TRANSACTION_CLEANUP_TEST_WORKER"] = shlex.join([
        # Preserve a virtualenv's interpreter symlink: resolve() loses its prefix.
        str(args.python.absolute()), "-m", "tests.catalog.transaction_cleanup_probe",
    ])
    environment["VGI_TRANSACTION_CLEANUP_TEST_DIR"] = str(cases_dir)
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, [
        str(args.python_root.resolve()), environment.get("PYTHONPATH"),
    ]))
    command = [str(args.binary.resolve()), "[transaction-cleanup]"]
    binary_result = {"command": command, "returncode": None, "timed_out": False, "error": None}
    stdout = stderr = ""
    try:
        completed = subprocess.run(
            command, cwd=Path(__file__).resolve().parents[1], env=environment,
            text=True, capture_output=True, timeout=args.timeout, check=False,
        )
        binary_result["returncode"] = completed.returncode
        stdout, stderr = completed.stdout, completed.stderr
    except subprocess.TimeoutExpired as exc:
        binary_result["timed_out"] = True
        stdout, stderr = output_text(exc.stdout), output_text(exc.stderr)
    except OSError as exc:
        binary_result["error"] = str(exc)
    (output_dir / "stdout.log").write_text(stdout, encoding="utf-8")
    (output_dir / "stderr.log").write_text(stderr, encoding="utf-8")

    cases = []
    for operation, mode in CASES:
        case_dir = cases_dir / f"{operation}_{mode}"
        try:
            result = validate_case(case_dir, operation, mode)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result = {"case": case_dir.name, "passed": False, "errors": [str(exc)]}
        cases.append(result)
        print(f"{'PASS' if result['passed'] else 'FAIL'} {case_dir.name}: {json.dumps(result)}", flush=True)
    passed = binary_result["returncode"] == 0 and all(case["passed"] for case in cases)
    results = {"passed": passed, "binary": binary_result, "cases": cases}
    (output_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
