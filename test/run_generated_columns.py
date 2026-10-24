#!/usr/bin/env python3
"""Probe canonical VIRTUAL metadata and physical-only native scans on pinned 1.5."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_json(output):
    decoder, rows = json.JSONDecoder(), []
    while output.strip():
        batch, end = decoder.raw_decode(output.strip())
        rows.extend(batch)
        output = output.strip()[end:]
    return rows


class Runner:
    def __init__(self, python, build, artifacts):
        self.native = Path(__file__).resolve().parents[1]
        self.artifacts, self.database = artifacts, artifacts / "generated.sqlite"
        self.cli = next(
            (p for p in (build / "haybarn", build / "duckdb") if p.is_file()), None
        )
        if self.cli is None:
            raise FileNotFoundError("build the pinned native CLI first")
        self.extension = build / "extension/vgi/vgi.duckdb_extension"
        sys.path.insert(0, str(python))
        self.command = [
            "env",
            f"PYTHONPATH={python}",
            f"VGI_GENERATED_REFERENCE_DB={self.database}",
            sys.executable,
            "-m",
            "tests.catalog.generated_columns_reference",
        ]
        from tests.catalog.generated_columns_reference import GeneratedColumnsCatalog

        GeneratedColumnsCatalog.database_path = str(self.database)
        self.provider = GeneratedColumnsCatalog()
        self.raw_token = self.provider.catalog_attach(
            name="generated_columns",
            options={},
            data_version_spec=None,
            implementation_version=None,
        ).attach_opaque_data

    def execute(self, name, sql, error=None):
        script = (
            f"LOAD {quote(self.extension)};\n"
            "SET default_collation='binary'; SET default_order='asc';\n"
            "SET default_null_order='nulls_last'; SET preserve_insertion_order=true;\n"
            f"ATTACH 'generated_columns' AS w (TYPE vgi, LOCATION {quote(shlex.join(self.command))});\n"
            f"USE w.main;\n{sql}\n"
        )
        filename = self.artifacts / f"{name}.sql"
        filename.write_text(script)
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
                str(filename),
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
        else:
            messages = (error,) if isinstance(error, str) else error
            assert result.returncode != 0 and any(
                m.casefold() in result.stderr.casefold() for m in messages
            ), diagnostic
        return read_json(result.stdout)

    def public(self, callback):
        from vgi.client import Client

        with Client(self.command, pool=None) as client:
            token = client.catalog_attach(
                name="generated_columns",
                data_version_spec=None,
                implementation_version=None,
            ).attach_opaque_data
            return callback(client, token)

    def table(self, name):
        return self.public(
            lambda c, token: c.table_get(
                attach_opaque_data=token, schema_path=["main"], name=name
            )
        )

    def create(self, name, schema):
        self.public(
            lambda c, token: c.table_create(
                attach_opaque_data=token,
                schema_path=["main"],
                name=name,
                columns=schema.serialize().to_pybytes(),
            )
        )

    def seed(self, name, rows):
        self.provider.seed_rows(
            attach_opaque_data=self.raw_token,
            transaction_opaque_data=None,
            schema_path=["main"],
            name=name,
            rows=rows,
        )

    def state(self):
        with sqlite3.connect(self.database) as db:
            version, data = db.execute(
                "SELECT version,data FROM catalog WHERE id=1"
            ).fetchone()
        return {"version": version, "data": json.loads(data)}

    def reject(self, name, sql, error):
        before = self.state()
        self.execute(name, sql, error)
        assert self.state() == before, f"{name} changed persistent state"


def cases(runner):
    import pyarrow as pa
    from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
    from vgi.catalog.evaluation_context import QueryEvaluationContext
    from vgi.catalog.sql import (
        ColumnExpressions,
        SqlExpression,
        column_expressions,
        with_column_expressions,
    )

    def generated(name, sql, dtype=None):
        return with_column_expressions(
            pa.field(name, dtype or pa.int64()),
            ColumnExpressions(
                generated=SqlExpression(
                    sql=sql,
                    profile=ROW_SCALAR_EXPRESSION,
                    schema_path=["main"],
                    evaluation_context=QueryEvaluationContext(),
                ),
                generation="virtual",
            ),
        )

    runner.execute(
        "native_create",
        """
CREATE TABLE native_created (
    doubled BIGINT AS (n * 2) VIRTUAL,
    n BIGINT,
    label VARCHAR AS ('item_' || CAST(n AS VARCHAR)) VIRTUAL,
    text VARCHAR,
    chained BIGINT AS (doubled + 1) VIRTUAL,
    tail BIGINT
);
""",
    )
    schema = pa.ipc.read_schema(pa.py_buffer(runner.table("native_created").columns))
    assert schema.names == ["doubled", "n", "label", "text", "chained", "tail"]
    for name in ["doubled", "label", "chained"]:
        expressions = column_expressions(schema.field(name))
        assert (
            expressions.generation == "virtual"
            and expressions.generated.schema_path == ["main"]
        )
        assert expressions.generated.evaluation_context == QueryEvaluationContext()
    for name in ["n", "text", "tail"]:
        assert column_expressions(schema.field(name)) == ColumnExpressions()
    runner.seed(
        "native_created",
        [
            {"n": 3, "text": "three", "tail": 30},
            {"n": None, "text": "null", "tail": 40},
            {"n": -2, "text": "negative", "tail": 50},
        ],
    )
    expected = [
        {
            "doubled": -4,
            "n": -2,
            "label": "item_-2",
            "text": "negative",
            "chained": -3,
            "tail": 50,
        },
        {
            "doubled": 6,
            "n": 3,
            "label": "item_3",
            "text": "three",
            "chained": 7,
            "tail": 30,
        },
        {
            "doubled": None,
            "n": None,
            "label": None,
            "text": "null",
            "chained": None,
            "tail": 40,
        },
    ]
    assert (
        runner.execute("read_interleaved", "SELECT * FROM native_created ORDER BY n;")
        == expected
    )
    assert runner.execute(
        "count_rows", "SELECT count(*) AS total FROM native_created;"
    ) == [{"total": 3}]
    assert (
        runner.execute(
            "constant_projection", "SELECT 42 AS answer FROM native_created;"
        )
        == [{"answer": 42}] * 3
    )
    assert runner.execute(
        "project_filter",
        "SELECT label,chained,tail FROM native_created WHERE doubled > 0;",
    ) == [{"label": "item_3", "chained": 7, "tail": 30}]
    assert (
        runner.execute("reconnect", "SELECT * FROM native_created ORDER BY n;")
        == expected
    )
    discovery = runner.execute(
        "discovery",
        "SELECT column_name,column_default FROM duckdb_columns() "
        "WHERE database_name='w' AND table_name='native_created' ORDER BY column_index;",
    )
    assert [row["column_name"] for row in discovery] == schema.names
    assert [row["column_default"] is not None for row in discovery] == [
        True,
        False,
        True,
        False,
        True,
        False,
    ]
    print(
        "PASS native CREATE, public readback, interleaved scans, NULLs, projection/filter and reconnect",
        flush=True,
    )

    runner.create(
        "rowids",
        pa.schema(
            [
                generated("g", "n * 2"),
                pa.field("rid", pa.int64(), metadata={b"is_row_id": b""}),
                pa.field("n", pa.int64()),
                pa.field("text", pa.string()),
            ]
        ),
    )
    runner.seed("rowids", [{"rid": 101, "n": 10, "text": "x"}])
    assert runner.execute("rowid_mapping", "SELECT rowid,n,g,text FROM rowids;") == [
        {"rowid": 101, "n": 10, "g": 20, "text": "x"}
    ]
    runner.create(
        "literal",
        pa.schema([generated("abs_value", 'abs("a.b")'), pa.field("a.b", pa.int64())]),
    )
    runner.seed("literal", [{"a.b": -7}])
    assert runner.execute(
        "builtin_shadow",
        "USE memory.main; CREATE MACRO abs(x) AS 999; USE w.main; "
        "SELECT abs_value FROM literal;",
    ) == [{"abs_value": 7}]
    print(
        "PASS provider CREATE, literal dotted dependency, hidden rowid mapping and builtin shadow isolation",
        flush=True,
    )

    for name, sql, error in [
        (
            "unknown",
            "CREATE TABLE bad_unknown(n BIGINT,g BIGINT AS (missing + 1));",
            ("does not exist", "not found"),
        ),
        (
            "type",
            "CREATE TABLE bad_type(n BIGINT,g BIGINT AS (lower(n)));",
            ("No function matches", "type"),
        ),
        (
            "cycle",
            "CREATE TABLE bad_cycle(n BIGINT,a BIGINT AS (b + 1),b BIGINT AS (a + 1));",
            ("circular", "cycle"),
        ),
        (
            "stored",
            "CREATE TABLE bad_stored(n BIGINT,g BIGINT AS (n + 1) STORED);",
            "STORED",
        ),
        (
            "context_create",
            "SET default_order='desc'; CREATE TABLE bad_context(n BIGINT,g BIGINT AS (n + 1));",
            "context",
        ),
        (
            "context_cached",
            "BEGIN; SELECT doubled FROM native_created ORDER BY n; SET default_order='desc'; SELECT doubled FROM native_created;",
            "context",
        ),
        (
            "rename",
            "ALTER TABLE native_created RENAME COLUMN n TO renamed;",
            ("dependent", "semantic column"),
        ),
        (
            "drop",
            "ALTER TABLE native_created DROP COLUMN n CASCADE;",
            ("dependent", "semantic column"),
        ),
        (
            "type_alter",
            "ALTER TABLE native_created ALTER COLUMN n TYPE INTEGER;",
            ("dependent", "semantic column"),
        ),
        (
            "add",
            "ALTER TABLE native_created ADD COLUMN extra BIGINT AS (n + 1);",
            ("generated", "adding"),
        ),
        (
            "insert",
            "INSERT INTO native_created(n,text,tail) VALUES (9,'bad',9);",
            ("generated", "does not support INSERT"),
        ),
        (
            "update",
            "UPDATE native_created SET n=9;",
            ("generated", "does not support UPDATE"),
        ),
        (
            "delete",
            "DELETE FROM native_created;",
            ("generated", "does not support DELETE"),
        ),
    ]:
        runner.reject(name, sql, error)
    assert (
        runner.execute("after_rejections", "SELECT * FROM native_created ORDER BY n;")
        == expected
    )
    runner.execute(
        "rollback",
        "BEGIN; CREATE TABLE rolled_back(n BIGINT,g BIGINT AS (n + 1)); ROLLBACK;",
    )
    assert runner.table("rolled_back") is None
    print(
        "PASS invalid definitions, context changes, protected ALTER/DML and CREATE rollback preserve state",
        flush=True,
    )
    (runner.artifacts / "verified.json").write_text(
        json.dumps({"status": "passed", "state": runner.state()}, indent=2) + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    artifacts = (
        args.artifacts.resolve()
        if args.artifacts
        else Path(tempfile.mkdtemp(prefix="vgi-generated-"))
    )
    artifacts.mkdir(parents=True, exist_ok=True)
    if (artifacts / "generated.sqlite").exists():
        raise FileExistsError("use a fresh artifact directory")
    print(f"Artifacts: {artifacts}", flush=True)
    cases(
        Runner(
            args.python.resolve(),
            args.build.resolve() if args.build else native / "build/release",
            artifacts,
        )
    )


if __name__ == "__main__":
    main()
