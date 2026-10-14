#!/usr/bin/env python3
"""Probe canonical constraints through the exact native build's SQL/IPC path.

The durable reference has no scan or DML adapter. These checks exercise catalog
binding, canonical discovery, stable mutation identity/revisions, and DDL
transactions; provider row-enforcement tests run in the Python suite.
"""

import argparse
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import tempfile


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_json(output):
    decoder = json.JSONDecoder()
    result = []
    remaining = output.strip()
    while remaining:
        value, end = decoder.raw_decode(remaining)
        assert isinstance(value, list), output
        result.extend(value)
        remaining = remaining[end:].strip()
    return result


def state(database):
    with sqlite3.connect(database) as connection:
        version, document = connection.execute("SELECT version, data FROM catalog WHERE id=1").fetchone()
        return version, json.loads(document)


def table_state(database, name):
    _, document = state(database)
    return document[json.dumps([["main"], name])]


class Runner:
    def __init__(self, native, python, build, artifacts):
        self.native = native
        self.python = python
        self.artifacts = artifacts
        self.cli = next((path for path in (build / "haybarn", build / "duckdb", build / "tools/shell/duckdb") if path.is_file()), None)
        if self.cli is None:
            raise FileNotFoundError("build the pinned CLI first")
        self.extension = build / "extension/vgi/vgi.duckdb_extension"

    def execute(self, name, sql, *, database=None, error=None):
        database = database or self.artifacts / (name + ".sqlite")
        worker = shlex.join([
            "env", f"VGI_CONSTRAINT_DATABASE={database}", "uv", "--directory", str(self.python),
            "run", "--no-sync", "python", "-m", "tests.catalog.constraints_protocol_reference",
        ])
        script = (
            f"LOAD {quote(self.extension)};\n"
            f"ATTACH 'constraints' AS w (TYPE vgi, LOCATION {quote(worker)});\n"
            "USE w.main;\n"
            f"{sql}\nUSE memory.main;\nDETACH w;\n"
        )
        (self.artifacts / (name + ".sql")).write_text(script)
        result = subprocess.run(
            [str(self.cli), "-unsigned", "-batch", "-json", "-init", os.devnull, ":memory:"],
            cwd=self.native, input=script, text=True, capture_output=True, timeout=120,
        )
        (self.artifacts / (name + ".stdout.txt")).write_text(result.stdout)
        (self.artifacts / (name + ".stderr.txt")).write_text(result.stderr)
        diagnostic = f"{name}: exit={result.returncode}\nSQL:\n{sql}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        if error is None:
            assert result.returncode == 0, diagnostic
        else:
            assert result.returncode != 0 and error.casefold() in result.stderr.casefold(), diagnostic
        return database, read_json(result.stdout)


def cases(runner, selected):
    def wanted(name):
        return not selected or name in selected

    if wanted("create_discovery"):
        database, rows = runner.execute("create_discovery", '''
CREATE TABLE w.parent (id INTEGER PRIMARY KEY, code INTEGER UNIQUE);
CREATE TABLE w.child (id INTEGER UNIQUE, parent_id INTEGER REFERENCES parent(id));
CREATE TABLE w.raw ("a.b" INTEGER NOT NULL);
SELECT table_name, constraint_type, constraint_column_names
FROM duckdb_constraints() WHERE database_name='w'
ORDER BY table_name, constraint_type;
''')
        kinds = {(row["table_name"], row["constraint_type"]) for row in rows}
        assert {("parent", "PRIMARY KEY"), ("parent", "UNIQUE"), ("child", "UNIQUE"), ("child", "FOREIGN KEY"), ("raw", "NOT NULL")} <= kinds, rows
        raw = next(row for row in rows if row["table_name"] == "raw")
        assert raw["constraint_column_names"] == ["a.b"], rows
        assert all(table_state(database, name)["revision"] for name in ("parent", "child", "raw"))
        print("PASS create_discovery: PK/UNIQUE/FK and literal dotted column", flush=True)

    if wanted("not_null_mutation"):
        database, _ = runner.execute("not_null_create", "CREATE TABLE w.t (a INTEGER, b INTEGER UNIQUE);")
        before = table_state(database, "t")
        _, rows = runner.execute("not_null_set", '''
ALTER TABLE w.t ALTER COLUMN A SET NOT NULL;
SELECT constraint_type FROM duckdb_constraints()
WHERE database_name='w' AND table_name='t' ORDER BY constraint_type;
''', database=database)
        added = table_state(database, "t")
        assert rows == [{"constraint_type": "NOT NULL"}, {"constraint_type": "UNIQUE"}], rows
        assert added["revision"] != before["revision"]
        assert set(before["constraints"]) < set(added["constraints"]), "existing stable records changed"
        _, rows = runner.execute("not_null_drop", '''
ALTER TABLE w.t ALTER COLUMN a DROP NOT NULL;
SELECT constraint_type FROM duckdb_constraints()
WHERE database_name='w' AND table_name='t' ORDER BY constraint_type;
''', database=database)
        dropped = table_state(database, "t")
        assert rows == [{"constraint_type": "UNIQUE"}], rows
        assert dropped["revision"] not in (before["revision"], added["revision"])
        assert dropped["constraints"] == before["constraints"], "drop changed an unrelated stable key"
        print("PASS not_null_mutation: public apply, stable records, rotating binary revisions", flush=True)

    if wanted("ddl_transactions"):
        database, _ = runner.execute("ddl_transaction_create", "CREATE TABLE w.t (a INTEGER);")
        before = table_state(database, "t")
        _, rows = runner.execute("ddl_transaction_rollback", '''
BEGIN;
ALTER TABLE w.t ALTER COLUMN A SET NOT NULL;
SELECT count(*) AS n FROM duckdb_constraints() WHERE database_name='w' AND table_name='t';
ROLLBACK;
SELECT count(*) AS n FROM duckdb_constraints() WHERE database_name='w' AND table_name='t';
''', database=database)
        assert rows == [{"n": 1}, {"n": 0}], rows
        assert table_state(database, "t") == before, "rollback changed durable table definition"
        _, rows = runner.execute("ddl_transaction_commit", '''
BEGIN;
ALTER TABLE w.t ALTER COLUMN A SET NOT NULL;
COMMIT;
SELECT count(*) AS n FROM duckdb_constraints() WHERE database_name='w' AND table_name='t';
''', database=database)
        assert rows == [{"n": 1}], rows
        assert table_state(database, "t")["revision"] != before["revision"]
        print("PASS ddl_transactions: read-your-writes, rollback, commit", flush=True)

    if wanted("reject_pk_drop"):
        database, _ = runner.execute("reject_pk_drop", '''
CREATE TABLE w.t (id INTEGER PRIMARY KEY);
ALTER TABLE w.t ALTER COLUMN id DROP NOT NULL;
''', error="PRIMARY KEY implied NOT NULL")
        assert table_state(database, "t")["constraints"], "rejected mutation removed primary key"
        print("PASS reject_pk_drop: primary-key nullability retained", flush=True)

    if wanted("reject_check"):
        database, _ = runner.execute("reject_check", "CREATE TABLE w.t (id INTEGER CHECK (id > 0));", error="canonical CHECK")
        assert not state(database)[1], "unsupported CHECK created a partial table"
        print("PASS reject_check: unsupported expression rejected before effects", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    native = args.native.resolve()
    build = args.build.resolve() if args.build else native / "build/release"
    artifacts = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="vgi-constraints-native-"))
    artifacts.mkdir(parents=True, exist_ok=True)
    print(f"Artifacts: {artifacts}", flush=True)
    cases(Runner(native, args.python.resolve(), build, artifacts), set(args.case))


if __name__ == "__main__":
    main()
