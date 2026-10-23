#!/usr/bin/env python3
"""Verify native write failures against real, independently read SQLite data.

The single-process HTTP reference uses ordinary provider transactions. It never
rolls back merely because a write callback fails: the native client must issue
rollback. Each case has a fresh server, data file, controls and captured logs.
This qualifies the bounded reference, not the unavailable private DuckDB Python
transactor or every production backend.
"""

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path


def quote(value):
    """Quote a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def rows_from_json(output):
    """Read consecutive native JSON result arrays."""
    rows = []
    decoder = json.JSONDecoder()
    while output.strip():
        value, end = decoder.raw_decode(output.strip())
        if not isinstance(value, list):
            raise TypeError("Expected native JSON row array")
        rows.extend(value)
        output = output.strip()[end:]
    return rows


@contextmanager
def server(python_root, case_dir):
    """Own one isolated loopback provider and its captured output."""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, [str(python_root), environment.get("PYTHONPATH")]))
    environment["VGI_TRANSACTIONAL_WRITE_DIR"] = str(case_dir)
    log_path = case_dir / "server.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.catalog.transactional_write_reference", "--port", "0"],
            env=environment, cwd=python_root, stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            port = None
            while time.monotonic() < deadline:
                content = log_path.read_text()
                match = re.search(r"PORT:(\d+)", content)
                if match:
                    port = int(match.group(1))
                    break
                if process.poll() is not None:
                    raise RuntimeError(f"Reference server exited: {content}")
                time.sleep(0.05)
            if port is None:
                raise TimeoutError(f"Reference server did not start: {log_path.read_text()}")
            yield f"http://127.0.0.1:{port}"
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def stored_state(path):
    """Read persisted tables independently of the provider's transactions."""
    # Independent SQLite connection reads committed data, without consulting
    # provider metadata caches or the transaction-under-test's connection.
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        return {
            name: [list(row) for row in connection.execute(
                'SELECT id,label FROM "' + name.replace('"', '""') + '" ORDER BY id,label'
            )]
            for name in tables
        }
    finally:
        connection.close()


SOURCE = "SELECT i::BIGINT AS id, 'row'::VARCHAR AS label FROM range(5000) t(i)"
BASELINE = [[-1, "baseline"]]
RECOVERED = [[-1, "baseline"], [9000, "recovered"]]


def scenarios():
    """Yield bounded success and failure cases with exact committed outcomes."""
    yield "count_success", None, None, False, "INSERT INTO w.main.items " + SOURCE + ";", {
        "items": BASELINE + [[i, "row"] for i in range(5000)]
    }, []
    yield "rows_success", None, None, False, "INSERT INTO w.main.items VALUES (1,'one'),(2,'two') RETURNING *;", {
        "items": BASELINE + [[1, "one"], [2, "two"]]
    }, [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}]
    yield "explicit_rollback", None, None, False, (
        "BEGIN; INSERT INTO w.main.items VALUES (99,'earlier'); ROLLBACK;"
    ), {"items": BASELINE}, []
    yield "zero_finalize", None, None, False, (
        "INSERT INTO w.main.items SELECT i::BIGINT,'row'::VARCHAR FROM range(0) t(i);"
    ), {"items": BASELINE}, []
    yield "ctas_success", None, "ctas", False, "CREATE TABLE w.main.created AS " + SOURCE + ";", {
        "items": BASELINE, "created": [[i, "row"] for i in range(5000)]
    }, []
    for explicit in (False, True):
        for operation in ("insert", "ctas"):
            for phase in ("init", "input", "finalize"):
                name = f"{'explicit' if explicit else 'autocommit'}_{operation}_{phase}_failure"
                setup = "CREATE TABLE w.main.target(id BIGINT,label VARCHAR);\n" if operation == "insert" else ""
                before = "BEGIN; INSERT INTO w.main.items VALUES (99,'earlier');\n" if explicit else ""
                failing = (
                    "INSERT INTO w.main.target " + SOURCE + " RETURNING *;\n"
                    if operation == "insert" else "CREATE TABLE w.main.created AS " + SOURCE + ";\n"
                )
                # COMMIT after the error must use DuckDB's invalidated-tx
                # rollback path, including the earlier successful write.
                after = "COMMIT;\n" if explicit else ""
                after += "INSERT INTO w.main.items VALUES (9000,'recovered');\n"
                after += "SELECT id,label FROM w.main.items ORDER BY id;\n"
                expected = {"items": RECOVERED}
                if operation == "insert":
                    expected["target"] = []
                yield name, phase, operation, explicit, setup + before + failing + after, expected, [
                    {"id": value, "label": label} for value, label in RECOVERED
                ]


def verify_events(events, phase, operation, explicit, name):
    """Check that persistence is explained by real callback/transaction events."""
    failures = [event for event in events if event["event"] == "failure"]
    if phase is None:
        assert not failures, failures
    else:
        assert len(failures) == 1 and failures[0]["phase"] == phase, failures
        failed = failures[0]
        token = failed["token"]
        same_transaction = [event for event in events if event.get("token") == token]
        assert len([event for event in same_transaction if event["event"] == "rollback"]) == 1, same_transaction
        assert not any(event["event"] == "commit" for event in same_transaction), same_transaction
        writes = [
            event for event in same_transaction if event["event"] == "input" and event["table"] == failed["table"]
        ]
        if phase == "init":
            assert not writes, writes
        elif phase == "input":
            assert len(writes) == 2 and sum(event["rows"] for event in writes) > 0, writes
        else:
            assert len(writes) > 1 and sum(event["rows"] for event in writes) == 5000, writes
        if explicit:
            assert any(
                event["event"] == "input" and event["table"] == "items" for event in same_transaction
            ), same_transaction
        if operation == "ctas":
            assert any(
                event["event"] == "table_create" and event["table"] == "created" for event in same_transaction
            ), same_transaction
    begins = [event["token"] for event in events if event["event"] == "begin"]
    ends = [event["token"] for event in events if event["event"] in {"commit", "rollback"}]
    assert len(set(begins)) == len(begins), begins
    assert sorted(begins) == sorted(ends), {"begins": begins, "ends": ends}
    if name == "zero_finalize":
        assert not any(event["event"] == "input" for event in events), events
        assert len([event for event in events if event["event"] == "finalize"]) == 1, events


def main():
    """Run native SQL, inspect committed storage, and save each result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    build = args.build.resolve() if args.build else native / "build/release"
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    cli = build / "haybarn"
    extension = build / "extension/vgi/vgi.duckdb_extension"
    results = []
    for name, phase, operation, explicit, body, expected_state, expected_rows in scenarios():
        if args.case and name not in args.case:
            continue
        directory = artifacts / name
        directory.mkdir()
        if phase:
            (directory / "control.json").write_text(json.dumps({
                "phase": phase, "table": "target" if operation == "insert" else "created", "after_batches": 2
            }))
        result = {"case": name, "passed": False, "error": None}
        try:
            with server(args.python.resolve(), directory) as location:
                script = (
                    f"LOAD {quote(extension)};\nSET threads=1;\n"
                    f"ATTACH 'transactional_writes' AS w (TYPE vgi, LOCATION {quote(location)});\n"
                    + body + "\nDETACH w;\n"
                )
                (directory / "query.sql").write_text(script)
                completed = subprocess.run(
                    [str(cli), "-unsigned", "-batch", "-json", "-init", os.devnull, ":memory:"],
                    input=script, cwd=native, text=True, capture_output=True, timeout=90, check=False,
                )
                (directory / "stdout.log").write_text(completed.stdout)
                (directory / "stderr.log").write_text(completed.stderr)
                result["returncode"] = completed.returncode
                actual_state = stored_state(directory / "data.sqlite")
                (directory / "stored-state.json").write_text(json.dumps(actual_state, indent=2) + "\n")
                events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
                assert (completed.returncode != 0) == (phase is not None), completed.stderr
                if phase:
                    assert f"transactional_write_reference: {phase}" in completed.stderr, completed.stderr
                actual_rows = rows_from_json(completed.stdout)
                assert actual_rows == expected_rows, {"actual_rows": actual_rows, "expected_rows": expected_rows}
                assert actual_state == expected_state, "Committed data differs; inspect stored-state.json"
                verify_events(events, phase, operation, explicit, name)
                result.update(passed=True, events=len(events), stored_rows={k: len(v) for k, v in actual_state.items()})
        except (AssertionError, OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
            result["error"] = str(exc)
        results.append(result)
        print(f"{'PASS' if result['passed'] else 'FAIL'} {name}: {json.dumps(result)}", flush=True)
    assert results, "No cases selected"
    (artifacts / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
