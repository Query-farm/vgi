#!/usr/bin/env python3
"""Protocol 3.3 write-path fixture for the extension's own tests.

Serves catalog ``wcfix``, schema ``main``:

    logs    ``id BIGINT NOT NULL PRIMARY KEY``, ``msg VARCHAR``; INSERT only
            (``logs_insert``) and **no row id**. MERGE (and INSERT ... ON CONFLICT,
            which DuckDB plans as a MERGE) needs a row id, so the extension must
            refuse it with a Binder Error at planning, not reach
            PhysicalMergeInto's InternalException, which invalidates the database.
    sink    ``id BIGINT`` with a row id (= id), rows 1..3 that never change, and a
            DELETE function ``sink_delete`` that only counts the row ids it
            receives. ``sink_received()`` reports the count, so a test sees whether
            the extension sent a row id more than once in one write call.
    plain   ``id BIGINT``, declared with explicit columns, so its ``TableInfo``
            inlines no scan function and every scan asks
            ``catalog_table_scan_branches_get``.
    phantom never listed; ``CREATE TABLE IF NOT EXISTS main.phantom`` replies
            ``created = false`` (as if another session had created it after
            DuckDB's binder looked), so a CTAS must insert nothing.

Flags break the worker on purpose:

    --no-branches          ``catalog_table_scan_branches_get`` answers
                           MethodNotImplemented, like a pre-3.3 worker that only
                           had ``catalog_table_scan_function_get``.
    --mistyped-create-reply    ``catalog_table_create`` replies ``created`` as a string
                           instead of ``TableCreateResponse(created: bool not null)``.

State is per worker process, as in visibility_worker.py.

Run as ``uv run --project <vgi-python> python test/support/write_contract_worker.py``.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector
from vgi_rpc.rpc._common import MethodNotImplementedError

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, Table, constraint_record
from vgi.invocation import BindResponse
from vgi.metadata import FunctionStability
from vgi.protocol import TableCreateRequest, TableCreateResponse
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker
from vgi.write_results import row_id_field, write_options, write_result_batch, write_result_schema

CATALOG_NAME = "wcfix"


def _flag(name: str) -> bool:
    if name in sys.argv:
        sys.argv.remove(name)
        return True
    return False


NO_BRANCHES = _flag("--no-branches")
MISTYPED_CREATE_REPLY = _flag("--mistyped-create-reply")

LOGS = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("msg", pa.string())])
SINK = pa.schema([pa.field("id", pa.int64(), nullable=False)])
PLAIN = pa.schema([pa.field("id", pa.int64())])

# Per worker process.
_LOGS: list[dict[str, Any]] = []
_SINK_RECEIVED: list[int] = [0, 0]  # row ids received, write calls


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


class LogsScan(_Scan):
    """main.logs (no row id)."""

    OUTPUT = LOGS

    class Meta:
        """Function metadata."""

        name = "logs_scan"
        description = "The rows of main.logs"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Rows inserted so far by this worker process."""
        return sorted(_LOGS, key=lambda r: r["id"])


class LogsInsert(TableInOutGenerator[None, None]):
    """Backs INSERT INTO main.logs; a duplicate id fails."""

    class Meta:
        """Function metadata."""

        name = "logs_insert"
        description = "Backs INSERT INTO main.logs"
        stability = FunctionStability.VOLATILE

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """count or rows."""
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, LOGS))

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Validate the batch, then store it."""
        del state
        rows = [{"id": r["id"], "msg": r["msg"]} for r in batch.to_pylist()]
        seen = {r["id"] for r in _LOGS}
        for row in rows:
            if row["id"] in seen:
                raise ValueError(f"wcfix: duplicate id {row['id']}")
            seen.add(row["id"])
        _LOGS.extend(rows)
        out.emit(write_result_batch(write_options(params).result_mode, LOGS, [None] * len(rows), rows))


class SinkScan(_Scan):
    """main.sink, with rowid = id."""

    OUTPUT = pa.schema([*SINK, row_id_field()])

    class Meta:
        """Function metadata."""

        name = "sink_scan"
        description = "The rows of main.sink, with rowid"
        stability = FunctionStability.VOLATILE
        projection_pushdown = True

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """Three fixed rows."""
        return [{"id": i} for i in (1, 2, 3)]


class SinkDelete(TableInOutGenerator[None, None]):
    """Backs DELETE FROM main.sink: counts the row ids it receives, deletes nothing."""

    class Meta:
        """Function metadata."""

        name = "sink_delete"
        description = "Counts the row ids a DELETE sends"
        stability = FunctionStability.VOLATILE

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """count only; a new call starts here."""
        _SINK_RECEIVED[1] += 1
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, SINK))

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Count the batch's row ids; report each as deleted."""
        del state
        _SINK_RECEIVED[0] += batch.num_rows
        rows = [{"id": r} for r in batch.column("rowid").to_pylist()]
        out.emit(write_result_batch(write_options(params).result_mode, SINK, rows, [None] * len(rows)))


class SinkReceived(_Scan):
    """``sink_received()``: row ids sink_delete received, and its write calls."""

    OUTPUT = pa.schema([pa.field("rowids", pa.int64()), pa.field("calls", pa.int64())])

    class Meta:
        """Function metadata."""

        name = "sink_received"
        description = "Row ids sink_delete received in this worker process, and its calls"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls) -> list[dict[str, Any]]:
        """One row."""
        return [{"rowids": _SINK_RECEIVED[0], "calls": _SINK_RECEIVED[1]}]


def _constraints(table: str, key: str) -> tuple[Any, ...]:
    def record(constraint_id: str, kind: str, column: str) -> Any:
        return constraint_record(
            ["main"], table, constraint_id, kind, [[column]], enforcement="ENFORCED", validation="ALL_ROWS"
        )

    return (record(f"{key}_not_null", "NOT_NULL", key), record("pk", "PRIMARY_KEY", key))


CATALOG = Catalog(
    name=CATALOG_NAME,
    schemas=[
        Schema(
            path=["main"],
            functions=[SinkReceived],
            tables=[
                Table(
                    name="logs",
                    function=LogsScan,
                    insert_function=LogsInsert,
                    write_result_modes={"insert": "rows"},
                    constraints=_constraints("logs", "id"),
                ),
                Table(
                    name="sink",
                    function=SinkScan,
                    delete_function=SinkDelete,
                    write_result_modes={"delete": "rows"},
                ),
                Table(name="plain", columns=PLAIN),
            ],
        )
    ],
)


@dataclass(frozen=True, slots=True, kw_only=True)
class _MistypedCreateResponse(TableCreateResponse):
    """A reply whose ``created`` is a string: the wire shape 3.3 forbids."""

    created: str = "yes"  # type: ignore[assignment]


class WcfixCatalog(ReadOnlyCatalogInterface):
    """The descriptor catalog."""

    catalog = CATALOG
    catalog_name = CATALOG_NAME


class WcfixWorker(Worker):
    """Serves ``wcfix``, broken as the flags say."""

    catalog_interface = WcfixCatalog
    catalog_name = CATALOG_NAME
    catalog = CATALOG

    def catalog_table_scan_branches_get(
        self,
        attach_opaque_data: bytes,
        schema_path: list[str],
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_opaque_data: bytes | None = None,
        pushdown_filters: bytes | None = None,
    ) -> bytes:
        """With --no-branches, behave like a worker without the method."""
        if NO_BRANCHES:
            raise MethodNotImplementedError("catalog_table_scan_branches_get is not implemented by this worker")
        return super().catalog_table_scan_branches_get(
            attach_opaque_data,
            schema_path,
            name,
            at_unit=at_unit,
            at_value=at_value,
            transaction_opaque_data=transaction_opaque_data,
            pushdown_filters=pushdown_filters,
        )

    def catalog_table_create(self, request: TableCreateRequest) -> TableCreateResponse:
        """``phantom`` IF NOT EXISTS: created=False; --mistyped-create-reply: a string; else refuse."""
        if MISTYPED_CREATE_REPLY:
            return _MistypedCreateResponse()
        if request.name == "phantom" and str(request.on_conflict).upper().endswith("IGNORE"):
            # As if another session had created it after DuckDB's binder looked:
            # IF NOT EXISTS finds it, so this call created nothing.
            return TableCreateResponse(created=False)
        raise ValueError("wcfix: CREATE TABLE is not supported")


def main() -> None:
    """Run the worker (stdio, ``--unix`` or ``--http``)."""
    WcfixWorker.main()


if __name__ == "__main__":
    main()
