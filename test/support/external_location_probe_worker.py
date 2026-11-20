#!/usr/bin/env python3
"""Hostile and well-formed external-location pointers, for the client's fetch checks.

Every function emits one zero-row ``vgi_rpc.location`` pointer batch (the shape
a server's ExternalLocationConfig produces) and then finishes. The pointed-at
objects come from a blob server this process runs on 127.0.0.1, so the tests
need ``VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1`` in the client -- except
``ext_probe_url``, whose URL the test chooses (for the refusals that happen
before any fetch).

All functions return ``(i BIGINT, payload VARCHAR)``; row ``i`` holds
``repeat(chr(97 + i % 26), width)``.

=====================================  =========================================
Function                               Pointer
=====================================  =========================================
``ext_probe_ok(rows, width)``          correct ``vgi_rpc.location.sha256``
``ext_probe_no_sha(rows, width)``      no digest (refused: protocol 3.4 requires one)
``ext_probe_bad_sha(rows, width)``     a digest of different bytes
``ext_probe_corrupt(rows, width)``     digest of the object, server sends it altered
``ext_probe_unsized(rows, width)``     correct digest, body without Content-Length
``ext_probe_gzip(rows, width)``        correct digest, body gzip-encoded
                                       (``X-VGI-Content-Encoding: gzip``)
``ext_probe_url(url)``                 the given URL verbatim
=====================================  =========================================

Run as ``uv run --project <vgi-python> python test/support/external_location_probe_worker.py``
(``VGI_EXTERNAL_PROBE_WORKER``). Drives test/sql/external_storage/fetch_*.test.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Annotated, Any, ClassVar, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.worker import Worker

OUTPUT_SCHEMA = schema(i=pa.int64(), payload=pa.string())
LOCATION_KEY = "vgi_rpc.location"
SHA256_KEY = "vgi_rpc.location.sha256"


@dataclass
class _Blob:
    body: bytes
    content_length: bool = True
    encoding: str | None = None


_blobs: dict[str, _Blob] = {}
_blobs_lock = threading.Lock()
_base_url: str | None = None


class _BlobHandler(BaseHTTPRequestHandler):
    # HTTP/1.0: a response without Content-Length is delimited by closing.
    protocol_version = "HTTP/1.0"

    def do_GET(self) -> None:  # noqa: N802
        with _blobs_lock:
            blob = _blobs.get(self.path.rsplit("/", 1)[-1])
        if blob is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        if blob.content_length:
            self.send_header("Content-Length", str(len(blob.body)))
        if blob.encoding:
            self.send_header("X-VGI-Content-Encoding", blob.encoding)
        self.end_headers()
        self.wfile.write(blob.body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass  # stdout is the RPC channel


def _base() -> str:
    global _base_url
    with _blobs_lock:
        if _base_url is None:
            server = ThreadingHTTPServer(("127.0.0.1", 0), _BlobHandler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            _base_url = f"http://127.0.0.1:{server.server_address[1]}/blobs"
        return _base_url


def _publish(blob: _Blob) -> str:
    key = uuid.uuid4().hex
    base = _base()
    with _blobs_lock:
        _blobs[key] = blob
    return f"{base}/{key}"


def _ipc_stream(rows: int, width: int) -> bytes:
    batch = pa.RecordBatch.from_pydict(
        {"i": list(range(rows)), "payload": [chr(97 + i % 26) * width for i in range(rows)]},
        schema=OUTPUT_SCHEMA,
    )
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, OUTPUT_SCHEMA) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pointer_metadata(kind: str, rows: int, width: int) -> dict[str, str]:
    data = _ipc_stream(rows, width)
    if kind == "ok":
        return {LOCATION_KEY: _publish(_Blob(data)), SHA256_KEY: _sha(data)}
    if kind == "no_sha":
        return {LOCATION_KEY: _publish(_Blob(data))}
    if kind == "bad_sha":
        return {LOCATION_KEY: _publish(_Blob(data)), SHA256_KEY: _sha(data + b"other")}
    if kind == "corrupt":
        # Flip the last payload byte: still a parseable stream, different bytes.
        altered = bytearray(data)
        altered[-9] ^= 0x01
        return {LOCATION_KEY: _publish(_Blob(bytes(altered))), SHA256_KEY: _sha(data)}
    if kind == "unsized":
        return {LOCATION_KEY: _publish(_Blob(data, content_length=False)), SHA256_KEY: _sha(data)}
    if kind == "gzip":
        return {LOCATION_KEY: _publish(_Blob(gzip.compress(data), encoding="gzip")), SHA256_KEY: _sha(data)}
    raise ValueError(kind)


@dataclass(kw_only=True)
class _OnceState(ArrowSerializableDataclass):
    emitted: bool = False


@dataclass(slots=True, frozen=True)
class _SizeArgs:
    rows: Annotated[int, Arg(0, doc="Rows in the pointed-at batch", ge=0)]
    width: Annotated[int, Arg(1, doc="Characters per row", ge=0)]


@dataclass(slots=True, frozen=True)
class _UrlArgs:
    url: Annotated[str, Arg(0, doc="Location URL to point at")]


def _emit_pointer(out: OutputCollector, output_schema: pa.Schema, metadata: dict[str, str]) -> None:
    empty = pa.RecordBatch.from_pydict({"i": [], "payload": []}, schema=output_schema)
    cast(Any, out).emit(empty, metadata=metadata)


def _make_sized(kind: str, doc: str) -> type:
    @init_single_worker
    @bind_fixed_schema
    class _Fn(TableFunctionGenerator[_SizeArgs, _OnceState]):
        class Meta:
            name = f"ext_probe_{kind}"
            description = doc

        FIXED_SCHEMA: ClassVar[pa.Schema] = OUTPUT_SCHEMA

        @classmethod
        def cardinality(cls, params: BindParams[_SizeArgs]) -> TableCardinality:
            return TableCardinality(estimate=params.args.rows, max=params.args.rows)

        @classmethod
        def initial_state(cls, params: ProcessParams[_SizeArgs]) -> _OnceState:
            return _OnceState()

        @classmethod
        def process(cls, params: ProcessParams[_SizeArgs], state: _OnceState, out: OutputCollector) -> None:
            if state.emitted:
                out.finish()
                return
            _emit_pointer(out, params.output_schema, _pointer_metadata(kind, params.args.rows, params.args.width))
            state.emitted = True

    _Fn.__name__ = _Fn.__qualname__ = f"ExtProbe{kind.title().replace('_', '')}Function"
    return _Fn


@init_single_worker
@bind_fixed_schema
class ExtProbeUrlFunction(TableFunctionGenerator[_UrlArgs, _OnceState]):
    """A pointer to the given URL, verbatim and without a digest."""

    class Meta:
        name = "ext_probe_url"
        description = "Pointer to an arbitrary URL"

    FIXED_SCHEMA: ClassVar[pa.Schema] = OUTPUT_SCHEMA

    @classmethod
    def initial_state(cls, params: ProcessParams[_UrlArgs]) -> _OnceState:
        return _OnceState()

    @classmethod
    def process(cls, params: ProcessParams[_UrlArgs], state: _OnceState, out: OutputCollector) -> None:
        if state.emitted:
            out.finish()
            return
        _emit_pointer(out, params.output_schema, {LOCATION_KEY: params.args.url})
        state.emitted = True


CATALOG = Catalog(
    name="ext_probe",
    default_schema="main",
    schemas=[
        Schema(
            path=["main"],
            functions=[
                _make_sized("ok", "Pointer with a correct digest"),
                _make_sized("no_sha", "Pointer without a digest"),
                _make_sized("bad_sha", "Pointer whose digest names other bytes"),
                _make_sized("corrupt", "Pointer to an object altered after hashing"),
                _make_sized("unsized", "Pointer to a body sent without Content-Length"),
                _make_sized("gzip", "Pointer to a gzip-encoded body"),
                ExtProbeUrlFunction,
            ],
        )
    ],
)


class ExtProbeCatalog(ReadOnlyCatalogInterface):
    """Catalog interface for ``ext_probe``."""

    catalog = CATALOG


class ExtProbeWorker(Worker):
    """Serves the ``ext_probe`` catalog."""

    catalog = CATALOG
    catalog_interface = ExtProbeCatalog


if __name__ == "__main__":
    ExtProbeWorker.main()
