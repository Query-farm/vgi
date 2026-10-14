#!/usr/bin/env python3
"""Exercise native explicit queries and inspect the durable provider's audit.

Uses the exact native build's CLI rather than a separately installed DuckDB
Python module. Each case has its own SQLite authority and attachment.
"""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def query(sql, parameters=None, mode=None):
    arguments = [quote("w"), quote(sql)]
    if parameters is not None:
        arguments.append("parameters := " + parameters)
    if mode is not None:
        arguments.append("parameter_mode := " + quote(mode))
    return "SELECT * FROM vgi_query(" + ", ".join(arguments) + ")"


def rows(output):
    """Read consecutive CLI JSON arrays, including empty result sets."""
    decoder = json.JSONDecoder()
    result = []
    remaining = output.strip()
    while remaining:
        value, end = decoder.raw_decode(remaining)
        assert isinstance(value, list), output
        result.extend(value)
        remaining = remaining[end:].strip()
    return result


def events(database):
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute("SELECT * FROM native_query_events ORDER BY id")]


def assert_clean(database):
    with sqlite3.connect(database) as connection:
        counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("query_preparations", "query_executions", "query_fragments", "query_attachments")
        }
        assert not any(counts.values()), f"retained query resources in {database}: {counts}"


class Runner:
    def __init__(self, native, python, build, directory):
        self.native = native
        self.python = python
        self.build = build
        self.directory = Path(directory)
        candidates = [build / "haybarn", build / "duckdb", build / "tools/shell/duckdb"]
        self.cli = next((path for path in candidates if path.is_file()), None)
        if self.cli is None:
            raise FileNotFoundError("build the pinned shell target (expected build/release/haybarn)")
        self.extension = build / "extension/vgi/vgi.duckdb_extension"

    def run(self, name, sql, *, expected=(), starts=0, mode="", enabled=True, error=None, settings="", cancel=False):
        database = self.directory / (name + ".sqlite")
        marker = self.directory / (name + ".read")
        worker = shlex.join([
            "env", f"VGI_QUERY_REFERENCE_DB={database}", f"VGI_QUERY_NATIVE_MODE={mode}",
            f"VGI_QUERY_READ_MARKER={marker}",
            "uv", "--directory", str(self.python), "run", "--no-sync",
            "python", "-m", "tests.catalog.query_native",
        ])
        script = (
            f"LOAD {quote(self.extension)};\n"
            f"SET vgi_query_enabled={'true' if enabled else 'false'};\n"
            f"{settings}\n"
            f"ATTACH 'queries' AS w (TYPE vgi, LOCATION {quote(worker)});\n"
            f"{sql}\nDETACH w;\n"
        )
        (self.directory / (name + ".sql")).write_text(script)
        command = [str(self.cli), "-unsigned", "-batch", "-json", "-init", os.devnull, ":memory:"]
        if cancel:
            # CLI -batch deliberately stops input and skips trailing DETACH
            # after SIGINT. Exercise the engine API while keeping its database
            # and connection alive, then inspect the same durable audit below.
            binary = self.build / "extension/vgi/vgi_catalog_tests"
            environment = dict(os.environ)
            environment["VGI_QUERY_TEST_WORKER"] = worker
            environment["VGI_QUERY_READ_MARKER"] = str(marker)
            command = [str(binary), "[query-interrupt]"]
            result = subprocess.run(command, cwd=self.native, env=environment, text=True, capture_output=True, timeout=90)
            stdout, stderr, returncode = result.stdout, result.stderr, result.returncode
        else:
            result = subprocess.run(command, cwd=self.native, input=script, text=True, capture_output=True, timeout=90)
            stdout, stderr, returncode = result.stdout, result.stderr, result.returncode
        (self.directory / (name + ".stdout.txt")).write_text(stdout)
        (self.directory / (name + ".stderr.txt")).write_text(stderr)
        diagnostic = f"{name}: exit={returncode}\nSQL:\n{sql}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        if error is None:
            assert returncode == 0, diagnostic
        else:
            assert returncode != 0, diagnostic
            if error:
                assert error.casefold() in stderr.casefold(), diagnostic
        if not cancel:
            assert rows(stdout) == list(expected), diagnostic
        audit = events(database)
        attempts = [event for event in audit if event["event"] == "start_attempt"]
        with sqlite3.connect(database) as connection:
            actual = connection.execute("SELECT starts FROM query_audit WHERE id=1").fetchone()[0]
        assert actual == starts, f"{diagnostic}\nExpected {starts} executions, got {actual}"
        assert len(attempts) == starts, f"{diagnostic}\nUnexpected execution retry: {attempts}"
        assert_clean(database)
        print(f"PASS {name}: {starts} executions; no retained query resources", flush=True)
        return audit


def run_cases(runner, selected):
    def wanted(name):
        return not selected or name in selected

    if wanted("values"):
        named_null = query("SELECT $value AS result", "{'value': NULL::INTEGER}")
        positional_null = query("SELECT $1 AS result", "row(NULL::INTEGER)", "positional")
        named = query("SELECT $value AS result", "{'value': 'hello λ'::VARCHAR}")
        repeated = query("SELECT $value + $value AS result", "{'value': 6::INTEGER}")
        audit = runner.run(
            "values",
            ";\n".join([named_null, positional_null, named, repeated]) + ";",
            expected=[{"result": None}, {"result": None}, {"result": "hello λ"}, {"result": 12}], starts=4,
        )
        values = [json.loads(event["detail"]) for event in audit if event["event"] == "start_attempt"]
        assert [value["names"] for value in values] == [["value"], ["1"], ["value"], ["value"]]
        assert [value["types"] for value in values] == [["int32"], ["int32"], ["string"], ["int32"]]
        assert [value["values"] for value in values] == [[None], [None], ["hello λ"], [6]]

    if wanted("ordered_empty"):
        audit = runner.run(
            "ordered_empty",
            query("SELECT i AS result FROM numbers WHERE false") + ";\n"
            + query("SELECT i AS result FROM numbers ORDER BY i") + ";",
            expected=[{"result": i} for i in range(7)], starts=2,
        )
        handles = [event["handle"] for event in audit if event["event"] == "start"]
        positions = [
            json.loads(event["detail"])["position"] for event in audit
            if event["event"] == "read" and event["handle"] == handles[-1]
        ]
        assert positions == [0, 1, 2, 3, 4], positions
        assert sum(event["event"] == "close" and json.loads(event["detail"])["reason"] == "eos" for event in audit) == 2

    for explicit in (False, True):
        name = "prepared_transaction" if explicit else "prepared_autocommit"
        if not wanted(name):
            continue
        sql = ("BEGIN;\n" if explicit else "") + "PREPARE q AS " + query(
            "SELECT $value AS result", "{'value': $1::INTEGER}"
        ) + ";\nEXECUTE q(11);\nEXECUTE q(22);\nDEALLOCATE q;\n" + ("COMMIT;" if explicit else "")
        audit = runner.run(name, sql, expected=[{"result": 11}, {"result": 22}], starts=2)
        executions = [event for event in audit if event["event"] == "start_attempt"]
        assert [json.loads(event["detail"])["values"] for event in executions] == [[11], [22]]
        assert all(event["tx"] for event in executions), executions
        assert (executions[0]["tx"] == executions[1]["tx"]) == explicit, executions
        assert len({json.loads(event["detail"])["execution_id"] for event in executions}) == 2
        assert executions[0]["attach"] == executions[1]["attach"]

    if wanted("prepare_only"):
        audit = runner.run(
            "prepare_only",
            "BEGIN;\nPREPARE q AS " + query("SELECT $value AS result", "{'value': 7::INTEGER}")
            + ";\nDEALLOCATE q;\nCOMMIT;",
        )
        assert any(event["event"] == "prepare" for event in audit), audit
        assert any(event["event"] == "release" for event in audit), audit

    if wanted("not_offered"):
        audit = runner.run("not_offered", query("SELECT i AS result FROM numbers ORDER BY i") + ";", enabled=False, error="")
        assert not any(event["event"] in ("capabilities", "prepare_attempt") for event in audit), audit

    if wanted("provider_disabled"):
        audit = runner.run(
            "provider_disabled", query("SELECT i AS result FROM numbers ORDER BY i") + ";", mode="disabled", error=""
        )
        assert not any(event["event"] == "prepare_attempt" for event in audit), audit

    failures = [
        ("declined", query("SELECT 42 AS result"), "", 0, "", ""),
        ("missing_parameter", query("SELECT $value AS result"), "", 0, "placeholder", ""),
        ("unsupported_parameter", query("SELECT $value AS result", "{'value': 1.5::DOUBLE}"), "", 0, "", ""),
        ("prepare_error", query("SELECT $value AS result", "{'value': 1::INTEGER}"), "prepare_error", 0, "preparation failure", ""),
        ("prepare_disconnect", query("SELECT $value AS result", "{'value': 1::INTEGER}"), "prepare_disconnect", 0, "", ""),
        ("start_error", query("SELECT $value AS result", "{'value': 1::INTEGER}"), "start_error", 1, "ambiguous start failure", ""),
        ("read_error", query("SELECT i AS result FROM numbers ORDER BY i"), "read_error", 1, "read failure", ""),
        # The Python worker also validates schemas: this is an end-to-end
        # failure test, not an isolated native decoder test.
        ("output_schema", query("SELECT i AS result FROM numbers ORDER BY i"), "read_wrong_type", 1, "", "SET vgi_validate_worker_batches='none';"),
    ]
    for name, sql, mode, starts, error, settings in failures:
        if wanted(name):
            audit = runner.run(name, sql + ";", mode=mode, starts=starts, error=error, settings=settings)
            if name == "prepare_disconnect":
                assert sum(event["event"] == "prepare_attempt" for event in audit) == 1, audit
                assert sum(event["event"] == "prepare" for event in audit) == 1, audit

    if wanted("rollback"):
        audit = runner.run(
            "rollback", "BEGIN;\n" + query("SELECT $value AS result", "{'value': 9::INTEGER}") + ";\nROLLBACK;",
            expected=[{"result": 9}], starts=1,
        )
        assert any(
            event["event"] == "transaction_finish" and json.loads(event["detail"])["status"] == "rolled_back"
            for event in audit
        ), audit

    if wanted("early_limit"):
        runner.run(
            "early_limit", query("SELECT i AS result FROM numbers ORDER BY i") + " LIMIT 1;",
            expected=[{"result": 0}], starts=1,
        )

    if wanted("interrupt"):
        runner.run(
            "interrupt", query("SELECT i AS result FROM numbers ORDER BY i") + ";", starts=1,
            mode="slow_read", cancel=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case", action="append", default=[], help="run only this named case; repeatable",
        choices=[
            "values", "ordered_empty", "prepared_transaction", "prepared_autocommit", "prepare_only",
            "not_offered", "provider_disabled", "declined", "missing_parameter", "unsupported_parameter",
            "prepare_error", "prepare_disconnect", "start_error", "read_error", "output_schema", "rollback",
            "early_limit", "interrupt",
        ],
    )
    parser.add_argument(
        "--keep-on-failure", action=argparse.BooleanOptionalAction, default=True,
        help="retain the SQLite audit, SQL and CLI output after a failure (default: enabled)",
    )
    arguments = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    python = Path(os.environ.get("VGI_PYTHON_DIR") or native.parent / "vgi-python").resolve()
    build = Path(os.environ.get("VGI_BUILD_DIR") or native / "build/release").resolve()
    state = build / "query-reference"
    state.mkdir(parents=True, exist_ok=True)
    directory = tempfile.mkdtemp(prefix="run-", dir=state)
    try:
        run_cases(Runner(native, python, build, directory), set(arguments.case))
    except BaseException:
        if arguments.keep_on_failure:
            print(f"Native query failure diagnostics retained at: {directory}", file=sys.stderr, flush=True)
        else:
            shutil.rmtree(directory)
        raise
    else:
        shutil.rmtree(directory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
