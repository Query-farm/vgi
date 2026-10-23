#!/usr/bin/env python3
"""Durable native macro probe, built on the public schema/macro reference store."""

from __future__ import annotations

import base64
import os
import sqlite3
from contextlib import closing
from typing import Any

import pyarrow as pa
from tests.catalog.test_macro_overload_protocol import MacroCatalog
from vgi.catalog import MacroInfo, OnConflict
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.macros import canonicalize_macro_defaults
from vgi.typed_values import TypedValueBatch
from vgi.worker import Worker


class NativeMacroCatalog(MacroCatalog):
    """Finite fixture definitions, durable public mutation, and hostile read mode."""

    database_path = os.environ["VGI_MACRO_DB"]

    def macro_create(self, **kwargs: Any) -> None:
        """Resolve one finite default expression and atomically persist a family."""
        for overload in kwargs["overloads"]:
            if overload.evaluation_context != QueryEvaluationContext():
                raise ValueError("native fixture only stores default context")
            allowed = {"x + 1", "upper(x)", "x", "SELECT x AS value"}
            if overload.definition not in allowed:
                raise ValueError("native fixture definition is outside the finite set")

        def resolve(overload: Any, parameter: Any) -> Any:
            if parameter.default_expression is None:
                return parameter.default_value
            if parameter.default_expression.sql != "40 + 2":
                raise ValueError("native fixture default is outside the finite set")
            return TypedValueBatch(
                pa.record_batch(
                    [pa.array([42], type=pa.int64())], names=[parameter.name]
                )
            )

        candidate = MacroInfo(
            comment="Native family probe",
            tags={},
            name=kwargs["name"],
            schema_path=kwargs["schema_path"],
            macro_type=kwargs["macro_type"],
            overloads=canonicalize_macro_defaults(kwargs["overloads"], resolve),
        )
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(
                db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"]
            )
            key = self._macro_key(kwargs["schema_path"], kwargs["name"])
            if key in data and kwargs["on_conflict"] != OnConflict.REPLACE:
                raise ValueError("macro already exists")
            data[key] = base64.b64encode(candidate.serialize_to_bytes()).decode()
            self._store(db, version + 1, data, kwargs["transaction_opaque_data"])

    def schema_contents(self, **kwargs: Any) -> Any:
        """A hostile provider can expose syntactically invalid SQL in a valid envelope."""
        from dataclasses import replace

        entries = super().schema_contents(**kwargs)
        bad_definitions = {
            "1": "x + )",
            "window": "(SELECT foreign_catalog.main.sum(x) OVER ())",
            "foreign": "foreign_catalog.main.upper(x)",
            "unknown": "missing_function(x)",
        }
        if bad := bad_definitions.get(os.environ.get("VGI_MACRO_BAD", "")):
            return [
                replace(
                    info,
                    overloads=[
                        info.overloads[0],
                        replace(info.overloads[1], definition=bad),
                    ],
                )
                if info.name == "family"
                else info
                for info in entries
            ]
        return entries


class NativeMacroWorker(Worker):
    """Ordinary Worker serving the durable public macro reference."""

    catalog_interface = NativeMacroCatalog


if __name__ == "__main__":
    NativeMacroWorker.main()
