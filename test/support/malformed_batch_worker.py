#!/usr/bin/env python3
"""Hostile-data fixture: table functions whose output batches are malformed Arrow.

Each function emits ONE batch that is structurally well-formed IPC (the
flatbuffer metadata and buffer layout are fine, so Arrow's IPC reader accepts
it) but whose buffer *contents* are invalid:

    malformed_string_offsets()  utf8 whose last offset points 1 MiB past the data buffer
    malformed_list_offsets()    list<int64> whose offset points 1 MiB past the child array
    malformed_bad_utf8()        utf8 holding the bytes FF FE (not UTF-8)
    malformed_dict_index()      dictionary<int32, utf8> with index 1 MiB over a 2-entry dictionary

pyarrow validates arrays when they are built, so each batch is built valid over
a mutable bytearray and corrupted afterwards; pyarrow does not re-validate and
the IPC writer serializes whatever the memory holds. The emit goes to the
innermost collector so no framework-level check sees it either.

The first two are caught by Arrow's structural ``Validate()``; the last two only
by ``ValidateFull()``.

Run as ``uv run --project <vgi-python> python test/support/malformed_batch_worker.py``.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Callable, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

_FAR = 1 << 20


def _string_offsets() -> tuple[pa.Array, Callable[[], None]]:
    offsets, data = bytearray(struct.pack("<3i", 0, 1, 2)), bytearray(b"ab")
    array = pa.Array.from_buffers(pa.utf8(), 2, [None, pa.py_buffer(offsets), pa.py_buffer(data)])
    return array, lambda: offsets.__setitem__(slice(8, 12), struct.pack("<i", _FAR))


def _list_offsets() -> tuple[pa.Array, Callable[[], None]]:
    offsets = bytearray(struct.pack("<2i", 0, 2))
    array = pa.Array.from_buffers(
        pa.list_(pa.int64()), 1, [None, pa.py_buffer(offsets)], children=[pa.array([1, 2], pa.int64())]
    )
    return array, lambda: offsets.__setitem__(slice(4, 8), struct.pack("<i", _FAR))


def _bad_utf8() -> tuple[pa.Array, Callable[[], None]]:
    offsets, data = bytearray(struct.pack("<2i", 0, 2)), bytearray(b"ab")
    array = pa.Array.from_buffers(pa.utf8(), 1, [None, pa.py_buffer(offsets), pa.py_buffer(data)])
    return array, lambda: data.__setitem__(slice(0, 2), b"\xff\xfe")


def _dict_index() -> tuple[pa.Array, Callable[[], None]]:
    indices = bytearray(struct.pack("<i", 0))
    index_array = pa.Array.from_buffers(pa.int32(), 1, [None, pa.py_buffer(indices)])
    array = pa.DictionaryArray.from_arrays(index_array, pa.array(["a", "b"]))
    return array, lambda: indices.__setitem__(slice(0, 4), struct.pack("<i", _FAR))


@dataclass(slots=True, frozen=True, kw_only=True)
class _NoArgs:
    pass


@dataclass(kw_only=True)
class _State(ArrowSerializableDataclass):
    emitted: bool = False


def _make(fn_name: str, arrow_type: pa.DataType, builder: Callable[[], tuple[pa.Array, Callable[[], None]]]) -> type:
    @init_single_worker
    @bind_fixed_schema
    class _Malformed(TableFunctionGenerator[_NoArgs, _State]):
        FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("c", arrow_type)])

        class Meta:
            name = fn_name
            description = "DELIBERATELY MALFORMED output batch (hostile-worker test fixture)"

        @classmethod
        def initial_state(cls, params: ProcessParams[Any]) -> _State:
            return _State()

        @classmethod
        def process(cls, params: ProcessParams[Any], state: _State, out: OutputCollector) -> None:
            if state.emitted:
                out.finish()
                return
            array, corrupt = builder()
            batch = pa.RecordBatch.from_arrays([array], schema=params.output_schema)
            corrupt()
            inner = out
            while hasattr(inner, "_inner"):
                inner = inner._inner
            inner.emit(batch)
            state.emitted = True

    _Malformed.__name__ = _Malformed.__qualname__ = "".join(p.title() for p in fn_name.split("_")) + "Function"
    return _Malformed


FUNCTIONS = [
    _make("malformed_string_offsets", pa.utf8(), _string_offsets),
    _make("malformed_list_offsets", pa.list_(pa.int64()), _list_offsets),
    _make("malformed_bad_utf8", pa.utf8(), _bad_utf8),
    _make("malformed_dict_index", pa.dictionary(pa.int32(), pa.utf8()), _dict_index),
]


class MalformedBatchWorker(Worker):
    """Worker exposing the ``malformed`` catalog."""

    catalog = Catalog(name="malformed", schemas=[Schema(path=["main"], functions=FUNCTIONS)])


if __name__ == "__main__":
    MalformedBatchWorker.main()
