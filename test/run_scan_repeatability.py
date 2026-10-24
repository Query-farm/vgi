#!/usr/bin/env python3
"""Qualify query-scoped bind snapshots against the mutable reference provider."""

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-repo", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    python = args.python_repo.resolve()
    build = args.build_dir.resolve()
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(python))
    from tests.repeatability_reference import seed_database  # noqa: PLC0415

    cli = next(p for p in (build / "haybarn", build / "duckdb") if p.is_file())
    extension = build / "extension/vgi/vgi.duckdb_extension"
    mutator = artifacts / "mutate.py"
    mutator.write_text(
        "from pathlib import Path\n"
        "import sys\n"
        "from tests.repeatability_reference import replace_rows\n"
        "assert replace_rows(Path(sys.argv[1]), [(20, 'twenty'), (None, 'new-null')]) == 2\n"
    )
    baseline = [
        {"n": 1, "label": "one", "generation": 1},
        {"n": 3, "label": "three", "generation": 1},
        {"n": 4, "label": "four", "generation": 1},
        {"n": None, "label": "null", "generation": 1},
    ]
    changed = [
        {"n": 20, "label": "twenty", "generation": 2},
        {"n": None, "label": "new-null", "generation": 2},
    ]
    results = []

    def run(name, table, *, prepared=False, projected=False, recursive=False):
        database = artifacts / f"{name}.sqlite"
        trace = artifacts / f"{name}.jsonl"
        seed_database(database)
        command = shlex.join(
            [
                "env",
                f"PYTHONPATH={python}",
                f"VGI_REPEATABILITY_DB={database}",
                f"VGI_REPEATABILITY_TRACE={trace}",
                sys.executable,
                "-m",
                "tests.repeatability_reference",
            ]
        )
        mutate = shlex.join(
            ["env", f"PYTHONPATH={python}", sys.executable, str(mutator), str(database)]
        )
        select = "CAST(n AS BIGINT) AS n" if projected else "n"
        query = (
            f"SELECT {select}, label, generation FROM w.main.{table} "
            "ORDER BY n NULLS LAST;"
        )
        if recursive:
            body = (
                "WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL "
                "SELECT r.i+1 FROM r CROSS JOIN w.main.stable_unoffered s "
                "WHERE r.i<2 AND s.n=1) SELECT i FROM r ORDER BY i;\n"
            )
            expected = [{"i": 0}, {"i": 1}, {"i": 2}]
        else:
            if prepared:
                body = f"PREPARE repeated AS {query}\nEXECUTE repeated;\n"
                body += f".shell {mutate}\nEXECUTE repeated;\n"
            else:
                body = f"{query}\n.shell {mutate}\n{query}\n"
            expected = baseline + changed
            body += (
                "SELECT count(*) AS cached_snapshots FROM vgi_result_cache() "
                "WHERE function LIKE 'repeatability_%';\n"
            )
            expected += [{"cached_snapshots": 0}]
        script = (
            f"LOAD {quote(extension)};\n"
            "SET default_collation='binary'; SET default_order='asc';\n"
            "SET default_null_order='nulls_last'; SET vgi_result_cache=true;\n"
            f"ATTACH 'repeatability_reference' AS w (TYPE vgi, LOCATION {quote(command)});\n"
            + body
        )
        sql_file = artifacts / f"{name}.sql"
        sql_file.write_text(script)
        result = subprocess.run(
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
                str(sql_file),
            ],
            cwd=Path(__file__).resolve().parents[1],
            env={**os.environ, "VGI_STDERR_LOG": "1"},
            text=True,
            capture_output=True,
            timeout=120,
            check=False,
        )
        (artifacts / f"{name}.stdout.txt").write_text(result.stdout)
        (artifacts / f"{name}.stderr.txt").write_text(result.stderr)
        diagnostic = (
            f"{name}: exit={result.returncode}\n{result.stdout}\n{result.stderr}"
        )
        assert result.returncode == 0, diagnostic
        assert json_rows(result.stdout) == expected, diagnostic
        events = [json.loads(line) for line in trace.read_text().splitlines()]
        initializations = [event for event in events if event["event"] == "init"]
        generations = [event["generation"] for event in initializations]
        if recursive:
            assert len(generations) >= 2 and set(generations) == {1}, events
        else:
            assert generations == [1, 2], events
        assert "query_scoped_repeatability" in result.stderr, diagnostic
        if table == "stable_inline":
            assert "inline_bind_query_snapshot_bypassed" in result.stderr, diagnostic
        if projected:
            assert sum(event["event"] == "proposal" for event in events) == 2, events
        results.append(
            {
                "case": name,
                "init_generations": generations,
                "binds": sum(event["event"] == "bind" for event in events),
                "passed": True,
            }
        )
        print(f"PASS {name}: init generations {generations}", flush=True)

    run("prepared_fresh_snapshot", "stable_unoffered", prepared=True)
    run("result_cache_exclusion", "stable_unoffered")
    run("inline_snapshot_bypass", "stable_inline")
    run(
        "prepared_projection_preserves_snapshot",
        "stable",
        prepared=True,
        projected=True,
    )
    run("recursive_same_bind_rescans", "stable_unoffered", recursive=True)
    (artifacts / "verified.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
