#!/usr/bin/env python3
"""Hostile-metadata fixture: tables whose PRIMARY KEY / UNIQUE constraints name the row_id column.

A worker controls its table metadata, so the client must treat an odd
constraint as bad input, never as an invariant violation. This one names the
rowid pseudocolumn through canonical raw column paths inside key constraints:

* ``items``              PRIMARY KEY (rowid)
* ``items_no_returning`` PRIMARY KEY (id, rowid), UNIQUE (rowid, name)

The client used to raise an InternalException from
VgiTableEntry::GetStorageInfo for these, which DuckDB treats as fatal: a single
``SELECT * FROM duckdb_tables()`` invalidated the whole database.
The canonical contract rejects the whole constraint at the Worker boundary;
it must never silently drop the rowid component and report a different key.

Run as ``uv run --project <vgi-python> python test/support/rowid_constraint_worker.py``.
"""

from __future__ import annotations

import dataclasses

from vgi._test_fixtures.simple_writable import SimpleWritableCatalog, SimpleWritableWorker
from vgi.catalog.constraints import (
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintValidation,
)
from vgi.catalog.dependencies import CatalogObjectRef

_CONSTRAINTS = {
    "items": [(ConstraintKind.PRIMARY_KEY, ["rowid"])],
    "items_no_returning": [
        (ConstraintKind.PRIMARY_KEY, ["id", "rowid"]),
        (ConstraintKind.UNIQUE, ["rowid", "name"]),
    ],
}


class RowidConstraintCatalog(SimpleWritableCatalog):
    """SimpleWritableCatalog whose key constraints include the rowid column."""

    def _build_table_info(self, *, name, schema_path):  # type: ignore[no-untyped-def]
        info = super()._build_table_info(name=name, schema_path=schema_path)
        definitions = _CONSTRAINTS.get(name)
        if definitions is None:
            return info
        constraints = [
            ConstraintInfo(
                ref=CatalogObjectRef(
                    kind="constraint", schema_path=schema_path, name=f"hostile_{index}", owner_table=name
                ),
                definition=ConstraintDefinition(kind=kind, column_paths=[[column] for column in columns]),
                enforcement=ConstraintEnforcement.METADATA_ONLY,
                validation=ConstraintValidation.UNVALIDATED,
            ).serialize_to_bytes()
            for index, (kind, columns) in enumerate(definitions)
        ]
        return dataclasses.replace(info, constraints=constraints)


class RowidConstraintWorker(SimpleWritableWorker):
    """Worker exposing :class:`RowidConstraintCatalog`."""

    catalog_interface = RowidConstraintCatalog


if __name__ == "__main__":
    RowidConstraintWorker.main()
