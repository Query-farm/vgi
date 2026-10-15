#!/usr/bin/env python3
"""Exercise typed index DDL/discovery against the pinned native binary and SQLite authority."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_json(output):
    values = []
    decoder = json.JSONDecoder()
    while output.strip():
        batch, end = decoder.raw_decode(output.strip())
        values.extend(batch)
        output = output.strip()[end:]
    return values


class Runner:
    def __init__(self, native, python, build, artifacts):
        self.native, self.python, self.artifacts = native, python, artifacts
        self.cli = next(path for path in (build / "haybarn", build / "duckdb") if path.is_file())
        self.extension = build / "extension/vgi/vgi.duckdb_extension"

    def execute(self, name, sql, *, database=None, error=None, json_output=True):
        database = database or self.artifacts / (name + ".sqlite")
        worker = shlex.join(["env", f"VGI_INDEX_DATABASE={database}", "uv", "--directory", str(self.python),
                             "run", "--no-sync", "python", "-m", "tests.catalog.index_protocol_reference"])
        script = (f"LOAD {quote(self.extension)};\n"
                  f"ATTACH 'indexes' AS w (TYPE vgi, LOCATION {quote(worker)});\n"
                  f"USE w.main;\n{sql}\nUSE memory.main;\nDETACH w;\n")
        (self.artifacts / (name + ".sql")).write_text(script)
        result = subprocess.run([str(self.cli), "-unsigned", "-batch", "-json", "-init", os.devnull, ":memory:"],
                                cwd=self.native, input=script, text=True, capture_output=True, timeout=120)
        (self.artifacts / (name + ".stdout.txt")).write_text(result.stdout)
        (self.artifacts / (name + ".stderr.txt")).write_text(result.stderr)
        diagnostic = f"{name}: exit={result.returncode}\n{result.stdout}\n{result.stderr}"
        if error is None:
            assert result.returncode == 0, diagnostic
        else:
            assert result.returncode != 0 and error.casefold() in result.stderr.casefold(), diagnostic
        return database, read_json(result.stdout) if json_output else []


QUERY = "SELECT index_name,is_unique,expressions,sql FROM duckdb_indexes() WHERE database_name='w' ORDER BY index_name;"


def cases(runner, selected):
    def wanted(name):
        return not selected or name in selected

    if wanted("create_discovery_drop"):
        database, rows = runner.execute("create_discovery_drop", '''
CREATE INDEX "raw.index" ON items USING "SQLITE.BTREE.V1" (label);
CREATE UNIQUE INDEX unique_id ON items USING "SQLITE.BTREE.V1" (id);
''' + QUERY)
        assert [(row["index_name"], row["is_unique"]) for row in rows] == [("raw.index", False), ("unique_id", True)], rows
        assert all("SQLITE.BTREE.V1" in row["sql"] for row in rows), rows
        _, rows = runner.execute("reconnect_and_drop", QUERY + '''
DROP INDEX "raw.index";
DROP INDEX UNIQUE_ID;
DROP INDEX IF EXISTS unique_id;
''' + QUERY, database=database)
        assert len(rows) == 2, rows
        _, rows = runner.execute("empty_after_drop", QUERY, database=database)
        assert rows == [], rows
        print("PASS create_discovery_drop", flush=True)

    if wanted("ddl_transactions"):
        _, rows = runner.execute("ddl_transactions", '''
BEGIN;
CREATE INDEX rolled_back ON items USING "SQLITE.BTREE.V1" (id);
ROLLBACK;
''' + QUERY + '''
BEGIN;
CREATE UNIQUE INDEX committed ON items USING "SQLITE.BTREE.V1" (id);
COMMIT;
''' + QUERY + '''
BEGIN;
DROP INDEX committed;
ROLLBACK;
''' + QUERY)
        assert [row["index_name"] for row in rows] == ["committed", "committed"], rows
        print("PASS ddl_transactions", flush=True)

    if wanted("explain_no_effect"):
        database, _ = runner.execute("explain_no_effect", '''
EXPLAIN CREATE INDEX explained_index ON items USING "SQLITE.BTREE.V1" (label);
''', json_output=False)
        _, rows = runner.execute("explain_no_effect_readback", QUERY, database=database)
        assert rows == [], rows
        print("PASS explain_no_effect", flush=True)

    for name, sql, error in [
        ("reject_expression", 'CREATE INDEX bad ON items USING "SQLITE.BTREE.V1" ((id + 1));', "expression keys"),
        ("reject_options", 'CREATE INDEX bad ON items USING "SQLITE.BTREE.V1" (id) WITH (fillfactor=80);', "options"),
        ("reject_art", 'CREATE INDEX bad ON items (id);', "algorithm"),
        ("reject_collation", 'SET default_collation=nocase; CREATE INDEX bad ON items USING "SQLITE.BTREE.V1" (label);', "collation"),
    ]:
        if wanted(name):
            database, _ = runner.execute(name, sql, error=error)
            _, rows = runner.execute(name + "_no_effect", QUERY, database=database)
            assert rows == [], rows
            print("PASS " + name, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    native = Path(__file__).resolve().parent.parent
    build = args.build or native / "build/release"
    artifacts = args.artifacts or Path(tempfile.mkdtemp(prefix="vgi-indexes-"))
    artifacts.mkdir(parents=True, exist_ok=True)
    cases(Runner(native, args.python.resolve(), build, artifacts), args.case)


if __name__ == "__main__":
    main()
