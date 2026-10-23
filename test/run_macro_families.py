#!/usr/bin/env python3
"""Create durable families through the public client, then execute in native 1.5."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    native = Path(__file__).resolve().parents[1]
    python = Path(
        os.environ.get("VGI_PYTHON_DIR") or native.parent / "vgi-python"
    ).resolve()
    build = Path(os.environ.get("VGI_BUILD_DIR") or native / "build/release").resolve()
    sys.path.insert(0, str(python))
    import pyarrow as pa
    from vgi.catalog import MacroOverload, MacroParameter, MacroType
    from vgi.catalog.evaluation_context import QueryEvaluationContext
    from vgi.catalog.layout import SqlExpression
    from vgi.client import Client
    from vgi.typed_values import TypedValueBatch

    def parameter(name="x", kind=None, *, default=None, doc=None):
        return MacroParameter(
            name=name,
            declared_type=pa.schema([pa.field(name, kind)])
            if kind is not None
            else None,
            default_value=default,
            documentation=doc,
        )

    def overload(body, params, kind="scalar"):
        return MacroOverload(
            parameters=params,
            definition=body,
            profile="duckdb.macro.v1",
            schema_path=["main"],
            macro_type=kind,
            evaluation_context=QueryEvaluationContext(),
        )

    with tempfile.TemporaryDirectory(prefix="vgi-native-macro-") as directory:
        command = [
            "env",
            f"PYTHONPATH={python}",
            f"VGI_MACRO_DB={directory}/macros.sqlite",
            sys.executable,
            str(native / "test/support/macro_family_worker.py"),
        ]
        with Client(command, pool=None) as client:
            token = client.catalog_attach(
                name="options", data_version_spec=None, implementation_version=None
            ).attach_opaque_data
            families = {
                "family": [
                    overload("x + 1", [parameter(kind=pa.int32(), doc="")]),
                    overload("upper(x)", [parameter(kind=pa.string())]),
                ],
                "defaults": [
                    overload(
                        "x + 1",
                        [
                            MacroParameter(
                                name="x",
                                default_expression=SqlExpression(
                                    sql="40 + 2",
                                    profile="duckdb.macro.default.v1",
                                    schema_path=["main"],
                                    evaluation_context=QueryEvaluationContext(),
                                ),
                            )
                        ],
                    )
                ],
                "typed_null": [
                    overload(
                        "x",
                        [
                            parameter(
                                default=TypedValueBatch(
                                    pa.record_batch(
                                        [pa.array([None], type=pa.int32())], names=["x"]
                                    )
                                )
                            )
                        ],
                    )
                ],
            }
            for name, family in families.items():
                client.macro_create(
                    attach_opaque_data=token,
                    schema_path=["main"],
                    name=name,
                    macro_type=MacroType.SCALAR,
                    overloads=family,
                )
                assert len(
                    client.macro_get(
                        attach_opaque_data=token, schema_path=["main"], name=name
                    ).overloads
                ) == len(family)
            client.macro_create(
                attach_opaque_data=token,
                schema_path=["main"],
                name="table_family",
                macro_type=MacroType.TABLE,
                overloads=[
                    overload("SELECT x AS value", [parameter(kind=kind)], "table")
                    for kind in (pa.int32(), pa.string())
                ],
            )
        env = dict(os.environ)
        env["VGI_MACRO_FAMILY_WORKER"] = shlex.join(command)
        env["VGI_MACRO_BAD_FAMILY_WORKER"] = shlex.join(
            ["env", "VGI_MACRO_BAD=1", *command]
        )
        for mode in ("window", "foreign", "unknown"):
            env[f"VGI_MACRO_{mode.upper()}_FAMILY_WORKER"] = shlex.join(
                ["env", f"VGI_MACRO_BAD={mode}", *command]
            )
        return subprocess.call(
            [
                str(build / "extension/vgi/vgi_catalog_tests"),
                "[macro-contract],[macro-family-native]",
            ],
            cwd=native,
            env=env,
        )


if __name__ == "__main__":
    raise SystemExit(main())
