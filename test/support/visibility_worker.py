#!/usr/bin/env python3
"""Protocol 3.3 function-visibility fixture (docs/function_visibility.md).

Serves catalog ``visfix``, schema ``main``:

    good    scan ``good_scan``: a BIGINT rowid (needs projection pushdown). The table
            does not set ``expose_scan``, so vgi-python lists the scan ``table_only``.
    ghost   scan ``ghost_scan``, which this worker drops from its function listing:
            the extension must refuse every scan of ``ghost`` with a Binder Error
            naming the function, never scan it with every capability off.
    notes   scan ``notes_scan``, insert function ``notes_insert`` (always ``table_only``).
            ``notes_insert`` returns its row count from ``finalize`` only, so an
            INSERT reports the right count only if the extension knew, from the hidden
            listing entry, that the function has a finalize stage.

With ``--bad-visibility`` the listing sends ``good_scan`` with visibility ``bogus``,
which the extension must reject naming the function.

The listing is edited by overriding ``ReadOnlyCatalogInterface._function_infos``:
vgi-python always lists the functions a table names, correctly, so a fixture that
tests the extension's handling of a worker that does not has to break it on purpose.

Run as ``uv run --project <vgi-python> python test/support/visibility_worker.py``.
"""

from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, Table
from vgi.invocation import BindResponse
from vgi.metadata import FunctionStability
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker
from vgi.write_results import row_id_field, write_count_batch, write_options, write_result_schema

CATALOG_NAME = "visfix"
BAD_VISIBILITY_FLAG = "--bad-visibility"
BAD_VISIBILITY = BAD_VISIBILITY_FLAG in sys.argv
if BAD_VISIBILITY:
    sys.argv.remove(BAD_VISIBILITY_FLAG)

GOOD = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("label", pa.string())])
NOTES = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("note", pa.string())])

# notes rows, per worker process (one stdio worker per connection).
_NOTES: list[dict[str, Any]] = []
# Rows notes_insert received in the current statement; finalize reports them.
_PENDING: list[int] = [0]


@dataclass(kw_only=True)
class _Once(ArrowSerializableDataclass):
    done: bool = False


@init_single_worker
class _Scan(TableFunctionGenerator[None, _Once]):
    """A no-argument scan emitting one batch, only the columns DuckDB asked for."""

    OUTPUT: ClassVar[pa.Schema]

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """The rows to emit."""
        raise NotImplementedError

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """The table's columns."""
        del params
        return BindResponse(output_schema=cls.OUTPUT)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _Once:
        """Nothing emitted yet."""
        del params
        return _Once()

    @classmethod
    def process(cls, params: ProcessParams[None], state: _Once, out: OutputCollector) -> None:
        """Emit the rows once."""
        if state.done:
            out.finish()
            return
        state.done = True
        rows = cls.rows()
        arrays = [
            pa.array([r["id"] if f.name == "rowid" else r[f.name] for r in rows], type=f.type)
            for f in params.output_schema
        ]
        out.emit(pa.RecordBatch.from_arrays(arrays, schema=params.output_schema))
        out.finish()


class GoodScan(_Scan):
    """main.good, with a rowid equal to id."""

    OUTPUT = pa.schema([*GOOD, row_id_field()])

    class Meta:
        """Function metadata."""

        name = "good_scan"
        description = "The rows of main.good, with rowid"
        stability = FunctionStability.VOLATILE
        # DuckDB reads `rowid` as a virtual column, which needs projection pushdown.
        projection_pushdown = True

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Three fixed rows."""
        return [{"id": i, "label": label} for i, label in ((1, "a"), (2, "b"), (3, "c"))]


class GhostScan(_Scan):
    """main.ghost: implemented, but never listed (see VisfixCatalog)."""

    OUTPUT = GOOD

    class Meta:
        """Function metadata."""

        name = "ghost_scan"
        description = "Dropped from the listing on purpose"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Never reached when the extension refuses the unlisted scan."""
        return [{"id": 1, "label": "should not be scanned"}]


class NotesScan(_Scan):
    """main.notes."""

    OUTPUT = NOTES

    class Meta:
        """Function metadata."""

        name = "notes_scan"
        description = "The rows of main.notes"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Rows inserted so far by this worker process."""
        return sorted(_NOTES, key=lambda r: r["id"])


class NotesInsert(TableInOutGenerator[None, None]):
    """Backs INSERT INTO main.notes; the row count comes from finalize only."""

    class Meta:
        """Function metadata."""

        name = "notes_insert"
        description = "Backs INSERT INTO main.notes, counting in finalize"
        stability = FunctionStability.VOLATILE
        has_finalize = True

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Count mode only; a new statement starts a new count."""
        _PENDING[0] = 0
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, NOTES))

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Store the rows and report nothing yet: finalize reports the count."""
        del params, state
        rows = batch.to_pylist()
        _NOTES.extend({"id": r["id"], "note": r["note"]} for r in rows)
        _PENDING[0] += len(rows)
        out.emit(pa.RecordBatch.from_pydict({"count": []}, schema=write_result_schema("count", NOTES)))

    @classmethod
    def finalize(cls, params: ProcessParams[None]) -> list[pa.RecordBatch]:
        """The statement's row count."""
        del params
        count, _PENDING[0] = _PENDING[0], 0
        return [write_count_batch(count)]


CATALOG = Catalog(
    name=CATALOG_NAME,
    schemas=[
        Schema(
            path=["main"],
            functions=[],
            tables=[
                Table(name="good", function=GoodScan),
                Table(name="ghost", function=GhostScan),
                Table(name="notes", function=NotesScan, insert_function=NotesInsert),
            ],
        )
    ],
)


class VisfixCatalog(ReadOnlyCatalogInterface):
    """The descriptor catalog with a deliberately broken function listing."""

    catalog = CATALOG
    catalog_name = CATALOG_NAME

    def _function_infos(self, path_key: Any, schema_path: Any, type_enum: Any) -> tuple[Any, ...]:
        infos = []
        for info in super()._function_infos(path_key, schema_path, type_enum):
            if info.name == "ghost_scan":
                continue
            if BAD_VISIBILITY and info.name == "good_scan":
                info = dataclasses.replace(info, visibility="bogus")
            infos.append(info)
        return tuple(infos)


class VisfixWorker(Worker):
    """Serves the ``visfix`` catalog."""

    catalog_interface = VisfixCatalog
    catalog_name = CATALOG_NAME
    catalog = CATALOG


def main() -> None:
    """Run the worker (stdio, ``--unix`` or ``--http``)."""
    VisfixWorker.main()


if __name__ == "__main__":
    main()
