#!/usr/bin/env python3
"""Prove optional native scalar projection acceptance and unchanged local fallback."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def json_rows(output):
    rows = []
    decoder = json.JSONDecoder()
    while output.strip():
        value, end = decoder.raw_decode(output.strip())
        rows.extend(value)
        output = output.strip()[end:]
    return rows


class Runner:
    def __init__(self, python, build, artifacts):
        self.native = Path(__file__).resolve().parents[1]
        self.python, self.build, self.artifacts = python, build, artifacts
        self.cli = next(p for p in (build / "haybarn", build / "duckdb") if p.is_file())
        self.extension = build / "extension/vgi/vgi.duckdb_extension"
        self.results = []

    def run(
        self,
        name,
        sql,
        *,
        table,
        expected=None,
        error=None,
        proposals=None,
        accepted=None,
        inits=None,
    ):
        trace = self.artifacts / f"{name}.jsonl"
        command = [
            "env",
            f"PYTHONPATH={self.python}",
            f"VGI_PROJECTION_TRACE={trace}",
            sys.executable,
            "-m",
            "tests.projection_reference",
        ]
        script = (
            f"LOAD {quote(self.extension)};\n"
            "SET default_collation='binary'; SET default_order='asc';\n"
            "SET default_null_order='nulls_last'; SET preserve_insertion_order=true;\n"
            f"ATTACH 'projection_reference' AS w (TYPE vgi, LOCATION {quote(shlex.join(command))});\n"
            f"{sql}\n"
        )
        path = self.artifacts / f"{name}.sql"
        path.write_text(script)
        result = subprocess.run(
            [
                str(self.cli),
                "-unsigned",
                "-batch",
                "-bail",
                "-json",
                "-init",
                os.devnull,
                ":memory:",
                "-f",
                str(path),
            ],
            cwd=self.native,
            text=True,
            capture_output=True,
            timeout=120,
            check=False,
        )
        (self.artifacts / f"{name}.stdout.txt").write_text(result.stdout)
        (self.artifacts / f"{name}.stderr.txt").write_text(result.stderr)
        diagnostic = (
            f"{name}: exit={result.returncode}\n{result.stdout}\n{result.stderr}"
        )
        if error is None:
            assert result.returncode == 0, diagnostic
            if expected is not None:
                assert json_rows(result.stdout) == expected, diagnostic
        else:
            assert result.returncode != 0 and error.lower() in result.stderr.lower(), (
                diagnostic
            )
        events = (
            [json.loads(line) for line in trace.read_text().splitlines()]
            if trace.exists()
            else []
        )
        events = [
            event for event in events if event["function"] == f"projection_{table}"
        ]
        counts = {
            kind: sum(event["event"] == kind for event in events)
            for kind in ("proposal", "accepted", "init", "rows")
        }
        for kind, count in (
            ("proposal", proposals),
            ("accepted", accepted),
            ("init", inits),
        ):
            if count is not None:
                assert counts[kind] == count, (name, counts, events)
        self.results.append({"name": name, "counts": counts, "passed": True})
        print(f"PASS {name}: {counts}", flush=True)
        return events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-repo", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    runner = Runner(
        args.python_repo.resolve(), args.build_dir.resolve(), args.artifacts.resolve()
    )
    expected = [
        {"wide": -2147483648, "wide_other": 2, "text": "min"},
        {"wide": 0, "wide_other": 3, "text": "zero"},
        {"wide": 2147483647, "wide_other": -1, "text": "max"},
        {"wide": None, "wide_other": None, "text": None},
    ]
    for table in ("accepted", "declined", "partial", "unoffered"):
        events = runner.run(
            table,
            f"SELECT CAST(n AS BIGINT) AS wide, CAST(other AS BIGINT) AS wide_other, text "
            f"FROM w.main.{table} ORDER BY n NULLS LAST;",
            table=table,
            expected=expected,
            proposals=0 if table == "unoffered" else 1,
            accepted=1 if table in {"accepted", "partial"} else 0,
            inits=1,
        )
        for event in events:
            if event["event"] == "accepted":
                assert event["accepted_indices"] == (
                    [0] if table == "partial" else [0, 1]
                ), event
                assert len(event["output_fields"]) == (
                    4 if table == "partial" else 5
                ), event
    runner.run(
        "residual_filter",
        "SELECT CAST(n AS BIGINT) AS wide, length(text) AS len "
        "FROM w.main.accepted WHERE other > 0 ORDER BY n;",
        table="accepted",
        expected=[{"wide": -2147483648, "len": 3}, {"wide": 0, "len": 4}],
        proposals=1,
        accepted=1,
        inits=1,
    )
    runner.run(
        "reordered",
        "SELECT CAST(other AS BIGINT) AS other_wide, text, CAST(n AS BIGINT) AS n_wide "
        "FROM w.main.accepted WHERE text IS NOT NULL ORDER BY n;",
        table="accepted",
        expected=[
            {
                "other_wide": row["wide_other"],
                "text": row["text"],
                "n_wide": row["wide"],
            }
            for row in expected
            if row["text"] is not None
        ],
        proposals=1,
        accepted=1,
        inits=1,
    )
    runner.run(
        "throwing_cast_local",
        "SELECT CAST(n AS SMALLINT) AS small FROM w.main.accepted WHERE n = 0;",
        table="accepted",
        expected=[{"small": 0}],
        proposals=0,
        accepted=0,
        inits=1,
    )
    runner.run(
        "unsupported_context_local",
        "SET default_order='desc'; SELECT CAST(n AS BIGINT) AS wide "
        "FROM w.main.accepted WHERE n=0;",
        table="accepted",
        expected=[{"wide": 0}],
        proposals=0,
        accepted=0,
        inits=1,
    )
    runner.run(
        "malformed",
        "SELECT CAST(n AS BIGINT) FROM w.main.malformed;",
        table="malformed",
        error="schema",
        proposals=1,
        accepted=1,
        inits=0,
    )
    runner.run(
        "execution_error",
        "SELECT CAST(n AS BIGINT) FROM w.main.execution_error;",
        table="execution_error",
        error="execution failed after acceptance",
        proposals=1,
        accepted=1,
        inits=1,
    )
    runner.run(
        "prepared_rebind",
        "PREPARE p AS SELECT CAST(n AS BIGINT) AS wide FROM w.main.accepted WHERE n=0; "
        "EXECUTE p; EXECUTE p;",
        table="accepted",
        expected=[{"wide": 0}, {"wide": 0}],
        proposals=2,
        accepted=2,
        inits=2,
    )
    runner.run(
        "explain_no_execution",
        "EXPLAIN SELECT CAST(n AS BIGINT) FROM w.main.accepted;",
        table="accepted",
        proposals=1,
        accepted=1,
        inits=0,
    )
    (args.artifacts / "verified.json").write_text(
        json.dumps({"checks": runner.results}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
