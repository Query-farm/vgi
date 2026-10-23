#!/usr/bin/env python3
"""Verify public Arrow metadata, native CREATE/ADD and both discovery paths."""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    """Exercise metadata transport and persisted public readback on native 1.5."""
    native = Path(__file__).resolve().parents[1]
    python = Path(os.environ.get("VGI_PYTHON_DIR") or native.parent / "vgi-python").resolve()
    build = Path(os.environ.get("VGI_BUILD_DIR") or native / "build/release").resolve()
    sys.path.insert(0, str(python))
    import pyarrow as pa

    from vgi.catalog.column_metadata import (
        ColumnMetadata,
        column_metadata,
        with_column_metadata,
    )
    from vgi.client import Client

    with tempfile.TemporaryDirectory(prefix="vgi-column-metadata-") as directory:
        command = [
            "env",
            f"PYTHONPATH={python}",
            f"VGI_COLUMN_REFERENCE_DB={directory}/columns.sqlite",
            sys.executable,
            "-m",
            "tests.catalog.column_native_reference",
        ]
        fields = [
            pa.field("absent", pa.int32()),
            with_column_metadata(pa.field("empty", pa.int32()), ColumnMetadata(comment="", tags={})),
            with_column_metadata(
                pa.field("a.b", pa.int32(), metadata={b"opaque": b"\xff\0"}),
                ColumnMetadata(comment="snow 雪", tags={"a.b": "snow 雪", "": "", "\0": "x\0y"}),
            ),
            pa.field(
                "outer",
                pa.struct(
                    [
                        with_column_metadata(
                            pa.field("leaf.with.dot", pa.int32()),
                            ColumnMetadata(comment="nested", tags={"raw/key": "value"}),
                        )
                    ]
                ),
            ),
        ]
        with Client(command, pool=None) as client:
            token = client.catalog_attach(
                name="columns", data_version_spec=None, implementation_version=None
            ).attach_opaque_data
            client.table_create(
                attach_opaque_data=token,
                schema_path=["main"],
                name="seeded",
                columns=pa.schema(fields).serialize().to_pybytes(),
            )
        corpus = json.loads((python / "conformance/catalog-contracts.json").read_text())["column_metadata_schemas"]
        manifest = []
        for expected, vectors in corpus.items():
            for index, (name, encoded) in enumerate(vectors.items()):
                filename = f"{expected}-{index}.ipc"
                (Path(directory) / filename).write_bytes(base64.b64decode(encoded))
                manifest.append(f"{expected} {filename} {name}")
        (Path(directory) / "vectors.txt").write_text("\n".join(manifest) + "\n")
        env = dict(os.environ)
        env["VGI_COLUMN_METADATA_VECTORS"] = directory
        env["VGI_COLUMN_METADATA_WORKER"] = shlex.join(command)
        result = subprocess.call(
            [
                str(build / "extension/vgi/vgi_catalog_tests"),
                "[column-metadata],[column-metadata-native],[column-metadata-corpus]",
            ],
            cwd=native,
            env=env,
        )
        if result:
            return result
        # Reopen via the public client: verify native writes persisted their
        # explicit empty comments and left absent comments absent.
        with Client(command, pool=None) as client:
            token = client.catalog_attach(
                name="columns", data_version_spec=None, implementation_version=None
            ).attach_opaque_data
            for name, expected in (
                ("created", ["", None, "", "snow 雪"]),
                ("copied", ["", "copy 雪"]),
            ):
                table = client.table_get(attach_opaque_data=token, schema_path=["main"], name=name)
                schema = pa.ipc.read_schema(pa.BufferReader(table.columns))
                assert [column_metadata(field).comment for field in schema] == expected
                assert all(column_metadata(field).tags is None for field in schema)
            table = client.table_get(attach_opaque_data=token, schema_path=["main"], name="seeded")
            schema = pa.ipc.read_schema(pa.BufferReader(table.columns))
            assert schema.equals(pa.schema(fields), check_metadata=True)
        print("Public readback preserved native comments and authoritative tagged schemas.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
