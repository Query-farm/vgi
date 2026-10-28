#!/usr/bin/env python3
"""Probe native semantic ALTER with authoritative public dependency readback.

The durable metadata-only provider is seeded through its public HTTP RPC so
views need no native scan adapter. Generated-column metadata remains an explicit
native negotiation/materialization rejection; its dependency semantics are
covered by public Python tests.
The pinned parser synthesizes CAST for TYPE without USING; both native TYPE
forms therefore exercise rejected conversion semantics rather than silent casts.
"""

import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_json(output):
    decoder = json.JSONDecoder()
    rows = []
    while output.strip():
        batch, end = decoder.raw_decode(output.strip())
        assert isinstance(batch, list), output
        rows.extend(batch)
        output = output.strip()[end:]
    return rows


class Runner:
    def __init__(self, native, python, build, artifacts):
        self.native, self.python, self.artifacts = native, python, artifacts
        self.cli = next(
            (path for path in (build / "haybarn", build / "duckdb") if path.is_file()),
            None,
        )
        if self.cli is None:
            raise FileNotFoundError("build the pinned native CLI first")
        self.extension = build / "extension/vgi/vgi.duckdb_extension"
        # The EC2 task Python environment supplies PyArrow and the public SDK.
        # Import the exact synced source checkout, without installing or editing it.
        sys.path.insert(0, str(python))

    @contextmanager
    def client(self, database):
        from vgi.client import Client
        from vgi.protocol import ClientCapabilities
        from vgi.serve import create_app
        from vgi_rpc.http._testing import _SyncTestClient
        from tests.catalog.dependencies_reference import (
            DEPENDENCY_PROFILES,
            DependencyCatalog,
            DependencyWorker,
        )

        DependencyCatalog.database_path = str(database)
        offer = ClientCapabilities(
            engine="native-alter-probe",
            native_formats=[],
            catalogs=[],
            can_stream=False,
            filter_encodings=[],
            catalog=DEPENDENCY_PROFILES,
        )
        with Client.from_http(
            "http://vgi.test",
            httpx_client=_SyncTestClient(
                create_app(
                    DependencyWorker,
                    describe=False,
                    signing_key=b"native-semantic-alter-probe",
                )
            ),
        ) as client:
            token = client.catalog_attach(
                name="dependencies",
                data_version_spec=None,
                implementation_version=None,
                client_capabilities=offer,
            ).attach_opaque_data
            yield client, token

    def seed(self, name, *, views=False, generated=False):
        import pyarrow as pa
        from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
        from vgi.catalog.evaluation_context import QueryEvaluationContext
        from vgi.catalog.sql import (
            ColumnExpressions,
            SqlExpression,
            with_column_expressions,
        )

        database = self.artifacts / (name + ".sqlite")
        if database.exists():
            raise FileExistsError(f"use a fresh artifact directory: {database}")
        fields = [
            pa.field("id", pa.int64()),
            pa.field("label", pa.string()),
            pa.field("keep", pa.int64()),
        ]
        if generated:
            fields.append(
                with_column_expressions(
                    pa.field("derived", pa.int64()),
                    ColumnExpressions(
                        generated=SqlExpression(
                            sql="id + 1",
                            profile=ROW_SCALAR_EXPRESSION,
                            schema_path=["main"],
                            evaluation_context=QueryEvaluationContext(),
                        ),
                        generation="virtual",
                    ),
                )
            )
        with self.client(database) as (client, token):
            client.table_create(
                attach_opaque_data=token,
                schema_path=["main"],
                name="items",
                columns=pa.schema(fields).serialize().to_pybytes(),
            )
            if views:
                client.view_create(
                    attach_opaque_data=token,
                    schema_path=["main"],
                    name="v",
                    definition="SELECT * FROM items",
                )
                client.view_create(
                    attach_opaque_data=token,
                    schema_path=["main"],
                    name="v2",
                    definition="SELECT * FROM v",
                )
        return database

    def state(self, database, label):
        """Read metadata and the graph through public, independently attached RPCs."""
        import pyarrow as pa
        from vgi.catalog import SchemaObjectType

        with self.client(database) as (client, token):
            graph = client.catalog_dependencies(attach_opaque_data=token)
            tables = list(
                client.schema_contents(
                    attach_opaque_data=token, path=["main"], type=SchemaObjectType.TABLE
                )
            )
            views = list(
                client.schema_contents(
                    attach_opaque_data=token, path=["main"], type=SchemaObjectType.VIEW
                )
            )
        result = {
            "version": graph.catalog_version,
            "graph": base64.b64encode(graph.serialize_to_bytes()).decode(),
            "column_dependency_count": sum(
                edge.dependent_column_path is not None or edge.subject_column_paths is not None
                for edge in graph.edges()
            ),
            "tables": {
                info.name: base64.b64encode(info.serialize_to_bytes()).decode()
                for info in tables
            },
            "views": {
                info.name: base64.b64encode(info.serialize_to_bytes()).decode()
                for info in views
            },
            "columns": {
                info.name: pa.ipc.read_schema(pa.BufferReader(info.columns)).names
                for info in tables
            },
            "column_types": {
                info.name: [str(field.type) for field in pa.ipc.read_schema(pa.BufferReader(info.columns))]
                for info in tables
            },
            "comments": {info.name: info.comment for info in tables},
        }
        (self.artifacts / (label + ".state.json")).write_text(
            json.dumps(result, indent=2) + "\n"
        )
        return result

    def execute(self, name, database, sql, *, error=None):
        worker = shlex.join(
            [
                "env",
                f"VGI_DEPENDENCY_REFERENCE_DB={database}",
                "uv",
                "--directory",
                str(self.python),
                "run",
                "--no-sync",
                "python",
                "-m",
                "tests.catalog.dependencies_native",
            ]
        )
        script = (
            f"LOAD {quote(self.extension)};\n"
            f"ATTACH 'dependencies' AS w (TYPE vgi, LOCATION {quote(worker)});\n"
            f"USE w.main;\n{sql}\nUSE memory.main;\nDETACH w;\n"
        )
        (self.artifacts / (name + ".sql")).write_text(script)
        result = subprocess.run(
            [
                str(self.cli),
                "-unsigned",
                "-batch",
                "-json",
                "-init",
                os.devnull,
                ":memory:",
            ],
            cwd=self.native,
            input=script,
            text=True,
            capture_output=True,
            timeout=120,
        )
        (self.artifacts / (name + ".stdout.txt")).write_text(result.stdout)
        (self.artifacts / (name + ".stderr.txt")).write_text(result.stderr)
        diagnostic = f"{name}: exit={result.returncode}\nSQL:\n{sql}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        if error is None:
            assert result.returncode == 0, diagnostic
        else:
            expected = (error,) if isinstance(error, str) else error
            assert (
                result.returncode != 0
                and any(message.casefold() in result.stderr.casefold() for message in expected)
            ), diagnostic
        return read_json(result.stdout)


COLUMNS = "SELECT table_name,column_name FROM duckdb_columns() WHERE database_name='w' ORDER BY table_name,column_index;"
VIEWS = (
    "SELECT view_name FROM duckdb_views() WHERE database_name='w' ORDER BY view_name;"
)


def cases(runner, selected):
    def wanted(name):
        return not selected or name in selected

    if wanted("rename_drop_comments"):
        database = runner.seed("rename_drop_comments")
        rows = runner.execute(
            "rename_drop_comments",
            database,
            """
ALTER TABLE items RENAME COLUMN label TO "label.raw";
COMMENT ON TABLE items IS 'safe metadata';
COMMENT ON COLUMN items."label.raw" IS 'safe column metadata';
ALTER TABLE items DROP COLUMN "label.raw";
ALTER TABLE items RENAME TO "renamed.table";
"""
            + COLUMNS,
        )
        assert rows == [
            {"table_name": "renamed.table", "column_name": "id"},
            {"table_name": "renamed.table", "column_name": "keep"},
        ], rows
        current = runner.state(database, "rename_drop_comments_after")
        assert current["columns"] == {"renamed.table": ["id", "keep"]}
        assert current["comments"] == {"renamed.table": "safe metadata"}
        assert runner.execute("rename_drop_reconnect", database, COLUMNS) == rows
        print(
            "PASS rename_drop_comments: raw names, native cache refresh, authoritative reconnect",
            flush=True,
        )

    if wanted("view_blockers"):
        database = runner.seed("view_blockers", views=True)
        before = runner.state(database, "view_blockers_before")
        for suffix, statement in [
            ("table_rename", "ALTER TABLE items RENAME TO renamed;"),
            ("column_rename", "ALTER TABLE items RENAME COLUMN label TO renamed;"),
            ("column_drop", "ALTER TABLE items DROP COLUMN label;"),
            ("column_type", "ALTER TABLE items ALTER COLUMN id TYPE INTEGER;"),
        ]:
            runner.execute(
                "view_blockers_" + suffix, database, statement, error="dependent"
            )
            assert (
                runner.state(database, "view_blockers_" + suffix + "_after") == before
            )
        runner.execute(
            "view_blockers_comments",
            database,
            "COMMENT ON TABLE items IS 'allowed'; COMMENT ON COLUMN items.label IS 'allowed';",
        )
        after = runner.state(database, "view_blockers_comments_after")
        assert set(after["views"]) == {"v", "v2"}
        assert after["comments"] == {"items": "allowed"}
        print(
            "PASS view_blockers: rename/type/drop denied atomically; comments admitted",
            flush=True,
        )

    if wanted("view_cascade"):
        database = runner.seed("view_cascade", views=True)
        rows = runner.execute(
            "view_cascade",
            database,
            VIEWS + "ALTER TABLE items DROP COLUMN label CASCADE;\n" + VIEWS + COLUMNS,
        )
        assert rows[:2] == [{"view_name": "v"}, {"view_name": "v2"}], rows
        assert rows[2:] == [
            {"table_name": "items", "column_name": "id"},
            {"table_name": "items", "column_name": "keep"},
        ], rows
        after = runner.state(database, "view_cascade_after")
        assert after["columns"] == {"items": ["id", "keep"]} and after["views"] == {}
        print(
            "PASS view_cascade: full view closure removed, table retained", flush=True
        )

    if wanted("ddl_transactions"):
        database = runner.seed("ddl_transactions")
        before = runner.state(database, "ddl_transactions_before")
        rows = runner.execute(
            "ddl_transactions_rollback",
            database,
            """
BEGIN;
ALTER TABLE items RENAME COLUMN label TO pending;
ALTER TABLE items DROP COLUMN keep;
"""
            + COLUMNS
            + "ROLLBACK;\n"
            + COLUMNS,
        )
        assert [row["column_name"] for row in rows] == [
            "id",
            "pending",
            "id",
            "label",
            "keep",
        ], rows
        assert runner.state(database, "ddl_transactions_rollback_after") == before
        runner.execute(
            "ddl_transactions_commit",
            database,
            """
BEGIN;
ALTER TABLE items RENAME COLUMN label TO committed;
ALTER TABLE items DROP COLUMN keep;
COMMIT;
""",
        )
        assert runner.state(database, "ddl_transactions_commit_after")["columns"] == {
            "items": ["id", "committed"]
        }
        rows = runner.execute("ddl_transactions_reconnect", database, COLUMNS)
        assert [row["column_name"] for row in rows] == ["id", "committed"], rows
        print(
            "PASS ddl_transactions: pending visibility, rollback, commit, reconnect",
            flush=True,
        )

    if wanted("native_type_conversion"):
        # Canonical DEFAULT/USING support now admits ordinary casts and the
        # equivalent explicit CAST. This metadata-only provider still declines
        # computed USING expressions. Read back the authoritative schema.
        for suffix, statement in [
            ("implicit", "ALTER TABLE items ALTER COLUMN id TYPE INTEGER;"),
            (
                "explicit",
                "ALTER TABLE items ALTER COLUMN id TYPE INTEGER USING CAST(id AS INTEGER);",
            ),
        ]:
            name = "native_type_" + suffix
            database = runner.seed(name)
            before = runner.state(database, name + "_before")
            assert before["column_types"]["items"] == ["int64", "string", "int64"]
            runner.execute(name, database, statement)
            after = runner.state(database, name + "_after")
            assert after["version"] > before["version"]
            assert after["columns"] == before["columns"]
            assert after["column_types"]["items"] == ["int32", "string", "int64"]
            runner.execute(
                name + "_computed",
                database,
                "ALTER TABLE items ALTER COLUMN id TYPE BIGINT USING id + 1;",
                error="metadata-only reference does not execute USING conversions",
            )
            assert runner.state(database, name + "_computed_after") == after
            runner.execute(
                name + "_unsupported",
                database,
                "ALTER TABLE items ALTER COLUMN id TYPE BIGINT USING random();",
                error="unsupported",
            )
            assert runner.state(database, name + "_unsupported_after") == after
        print(
            "PASS native_type_conversion: ordinary implicit/explicit CAST updates durable types; declined expressions have no effects",
            flush=True,
        )

    if wanted("generated_dependency_cascade"):
        database = runner.seed("generated_dependency_cascade", generated=True)
        before = runner.state(database, "generated_dependency_before")
        assert before["columns"]["items"] == ["id", "label", "keep", "derived"]
        assert before["column_dependency_count"] > 0
        runner.execute(
            "generated_dependency_restrict",
            database,
            "ALTER TABLE items DROP COLUMN id;",
            error="depend",
        )
        assert runner.state(database, "generated_dependency_restrict_after") == before
        runner.execute(
            "generated_dependency_cascade",
            database,
            "ALTER TABLE items DROP COLUMN id CASCADE;",
        )
        after = runner.state(database, "generated_dependency_cascade_after")
        assert after["version"] > before["version"]
        assert after["columns"]["items"] == ["label", "keep"]
        assert after["column_types"]["items"] == ["string", "int64"]
        assert after["column_dependency_count"] == 0
        print(
            "PASS generated_dependency_cascade: RESTRICT preserves state; CASCADE removes the dependent column and graph edge",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    build = args.build.resolve() if args.build else native / "build/release"
    artifacts = (
        args.artifacts.resolve()
        if args.artifacts
        else Path(tempfile.mkdtemp(prefix="vgi-semantic-alter-"))
    )
    artifacts.mkdir(parents=True, exist_ok=True)
    print(f"Artifacts: {artifacts}", flush=True)
    cases(Runner(native, args.python.resolve(), build, artifacts), set(args.case))


if __name__ == "__main__":
    main()
