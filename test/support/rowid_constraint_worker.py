#!/usr/bin/env python3
"""Constraint-record fixture: trusted keys, keys naming the row_id column, and METADATA_ONLY keys.

The DuckDB extension reads only ENFORCED + ALL_ROWS constraint records
(informational constraints are not supported) and must reject anything else
at the Worker boundary as ordinary bad input — never as an invariant violation
that invalidates the database. The catalog name chosen at ATTACH selects
which records the pre-defined ``simple_writable`` tables carry:

* ``enforced_keys``  — supported records, all ENFORCED + ALL_ROWS:
    ``items``               PRIMARY KEY (id)
    ``items_no_returning``  PRIMARY KEY (id), UNIQUE (name)
* ``rowid_keys``     — ENFORCED + ALL_ROWS records that name the rowid column:
    ``items``               PRIMARY KEY (rowid)
    ``items_no_returning``  PRIMARY KEY (id, rowid), UNIQUE (rowid, name)
* ``metadata_only_keys`` — an informational (METADATA_ONLY) key:
    ``items``               PRIMARY KEY (id), METADATA_ONLY / UNVALIDATED

The client used to raise an InternalException from
VgiTableEntry::GetStorageInfo for rowid keys, which DuckDB treats as fatal: a
single ``SELECT * FROM duckdb_tables()`` invalidated the whole database. The
canonical contract rejects the whole constraint; it must never silently drop
the rowid component and report a different key.

vgi-python validates records before serving them, exactly as the extension
does, so a hostile record would never reach the extension. This fixture exists
to test the extension's own checks, so it turns that serve-time validation off.

Run as ``uv run --project <vgi-python> python test/support/rowid_constraint_worker.py``.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import vgi.worker
from vgi._test_fixtures.simple_writable import CATALOG_NAME, SimpleWritableCatalog, SimpleWritableWorker, _init_db
from vgi.catalog.capabilities import CONSTRAINT_RECORDS_READ, CatalogCapabilities
from vgi.catalog.constraints import (
    ConstraintEnforcement,
    ConstraintKind,
    ConstraintValidation,
    constraint_record,
)

_TRUSTED = (ConstraintEnforcement.ENFORCED, ConstraintValidation.ALL_ROWS)
_INFORMATIONAL = (ConstraintEnforcement.METADATA_ONLY, ConstraintValidation.UNVALIDATED)

# catalog name -> (mode byte, {table: [(kind, columns, (enforcement, validation))]})
_MODES: dict[str, tuple[int, dict[str, list[Any]]]] = {
    "enforced_keys": (
        1,
        {
            "items": [(ConstraintKind.PRIMARY_KEY, ["id"], _TRUSTED)],
            "items_no_returning": [
                (ConstraintKind.PRIMARY_KEY, ["id"], _TRUSTED),
                (ConstraintKind.UNIQUE, ["name"], _TRUSTED),
            ],
        },
    ),
    "rowid_keys": (
        2,
        {
            "items": [(ConstraintKind.PRIMARY_KEY, ["rowid"], _TRUSTED)],
            "items_no_returning": [
                (ConstraintKind.PRIMARY_KEY, ["id", "rowid"], _TRUSTED),
                (ConstraintKind.UNIQUE, ["rowid", "name"], _TRUSTED),
            ],
        },
    ),
    "metadata_only_keys": (
        3,
        {"items": [(ConstraintKind.PRIMARY_KEY, ["id"], _INFORMATIONAL)]},
    ),
}
_BY_BYTE = {code: definitions for code, definitions in _MODES.values()}

# Serve records unvalidated so the extension, not the SDK, judges them. Fail
# loudly if the SDK stops routing serve-time validation through this name:
# otherwise the test would silently assert the SDK's messages instead.
if not hasattr(vgi.worker, "validate_constraint_records"):
    raise RuntimeError("vgi.worker.validate_constraint_records moved; update this fixture")
vgi.worker.validate_constraint_records = lambda *args, **kwargs: None


class RowidConstraintCatalog(SimpleWritableCatalog):
    """SimpleWritableCatalog whose tables carry the records of the attached mode."""

    def catalog_capabilities(self, *, attach_opaque_data):  # type: ignore[no-untyped-def,override]
        """Serve constraint records; writes are not offered, so the tables are read-only."""
        del attach_opaque_data
        return CatalogCapabilities(read=[CONSTRAINT_RECORDS_READ])

    def catalog_attach(self, *, name: str, **kwargs: Any):  # type: ignore[no-untyped-def,override]
        if name not in _MODES:
            raise ValueError(f"Unknown catalog: {name!r} (expected one of {sorted(_MODES)})")
        result = super().catalog_attach(name=CATALOG_NAME, **kwargs)
        # The first opaque byte carries the mode; the rest stays a per-attach id.
        opaque = bytes([_MODES[name][0]]) + bytes(result.attach_opaque_data)[1:]
        _init_db(opaque)
        return dataclasses.replace(result, attach_opaque_data=type(result.attach_opaque_data)(opaque))

    @staticmethod
    def _with_records(info, attach_opaque_data):  # type: ignore[no-untyped-def]
        definitions = _BY_BYTE[bytes(attach_opaque_data)[0]].get(info.name)
        if not definitions:
            return info
        constraints = [
            constraint_record(
                info.schema_path,
                info.name,
                f"key_{index}",
                kind,
                [[column] for column in columns],
                enforcement=enforcement,
                validation=validation,
            ).serialize_to_bytes()
            for index, (kind, columns, (enforcement, validation)) in enumerate(definitions)
        ]
        return dataclasses.replace(info, constraints=constraints)

    def table_get(self, *, attach_opaque_data, **kwargs):  # type: ignore[no-untyped-def,override]
        info = super().table_get(attach_opaque_data=attach_opaque_data, **kwargs)
        return None if info is None else self._with_records(info, attach_opaque_data)

    def schema_contents(self, *, attach_opaque_data, type, **kwargs):  # type: ignore[no-untyped-def,override]
        items = super().schema_contents(attach_opaque_data=attach_opaque_data, type=type, **kwargs)
        if type.name != "TABLE":
            return items
        return [self._with_records(info, attach_opaque_data) for info in items]


class RowidConstraintWorker(SimpleWritableWorker):
    """Worker exposing :class:`RowidConstraintCatalog`."""

    catalog_interface = RowidConstraintCatalog


if __name__ == "__main__":
    RowidConstraintWorker.main()
