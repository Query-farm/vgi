#!/usr/bin/env python3
"""Hostile-metadata fixture: tables whose PRIMARY KEY / UNIQUE constraints name the row_id column.

A worker controls its table metadata, so the client must treat an odd
constraint as bad input, never as an invariant violation. This one lists the
rowid pseudocolumn (Arrow index 3 in the simple-writable ``items`` schema:
id, name, qty, rowid) inside key constraints:

* ``items``              PRIMARY KEY (rowid)          -- nothing left once rowid is skipped
* ``items_no_returning`` PRIMARY KEY (id, rowid), UNIQUE (rowid, name)

The client used to raise an InternalException from
VgiTableEntry::GetStorageInfo for these, which DuckDB treats as fatal: a single
``SELECT * FROM duckdb_tables()`` invalidated the whole database.

Run as ``uv run --project <vgi-python> python test/support/rowid_constraint_worker.py``.
"""

from __future__ import annotations

import dataclasses

from vgi._test_fixtures.simple_writable import SimpleWritableCatalog, SimpleWritableWorker

_ROWID_INDEX = 3

_CONSTRAINTS = {
    "items": {"primary_key_constraints": [[_ROWID_INDEX]], "unique_constraints": []},
    "items_no_returning": {
        "primary_key_constraints": [[0, _ROWID_INDEX]],
        "unique_constraints": [[_ROWID_INDEX, 1]],
    },
}


class RowidConstraintCatalog(SimpleWritableCatalog):
    """SimpleWritableCatalog whose key constraints include the rowid column."""

    def _build_table_info(self, *, name, schema_path):  # type: ignore[no-untyped-def]
        info = super()._build_table_info(name=name, schema_path=schema_path)
        overrides = _CONSTRAINTS.get(name)
        return dataclasses.replace(info, **overrides) if overrides else info


class RowidConstraintWorker(SimpleWritableWorker):
    """Worker exposing :class:`RowidConstraintCatalog`."""

    catalog_interface = RowidConstraintCatalog


if __name__ == "__main__":
    RowidConstraintWorker.main()
