#!/usr/bin/env python3
"""The vgi-fixture-worker catalogs, externalizing every result over a byte transport.

The fixture worker's ``main()`` with one change: its ``RpcServer`` gets an
``ExternalLocationConfig`` whose threshold is 1 byte, so every unary result,
stream header and data batch it sends over stdin/stdout (or the launcher's
AF_UNIX socket) is a zero-row ``vgi_rpc.location`` pointer. The blobs are served
from an in-process HTTP server on 127.0.0.1. This is the byte-transport
counterpart of ``vgi-fixture-http --demo-storage``: externalization is a server
setting, not an HTTP feature, so the client must resolve pointers on every
transport.

Run as ``uv run --project <vgi-python> python test/support/external_storage_worker.py``
(``make test_launcher_external_storage``).
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import vgi.rpc_server
from vgi._test_fixtures import worker as fixture_worker
from vgi.http.demo_storage import DemoBlobStorage, localhost_only_validator
from vgi_rpc import ExternalLocationConfig


def _serve_blobs(storage: DemoBlobStorage) -> None:
    """Serve ``storage`` at http://127.0.0.1:<port>/__blobs__/<key> from a daemon thread."""

    class BlobHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            entry = storage.get(self.path.rsplit("/", 1)[-1])
            if entry is None:
                self.send_error(404)
                return
            data, content_encoding = entry
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            if content_encoding:
                self.send_header("Content-Encoding", content_encoding)
                self.send_header("X-VGI-Content-Encoding", content_encoding)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            pass  # stdout is the RPC channel; keep stderr quiet too

    server = ThreadingHTTPServer(("127.0.0.1", 0), BlobHandler)
    storage.set_base_url(f"http://127.0.0.1:{server.server_address[1]}")
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main() -> None:
    """Run the fixture worker with every result externalized."""
    storage = DemoBlobStorage(max_blobs=100_000)
    _serve_blobs(storage)
    external_location = ExternalLocationConfig(
        storage=storage,
        externalize_threshold_bytes=1,
        url_validator=localhost_only_validator,
    )
    build = vgi.rpc_server.build_rpc_server

    def build_externalizing(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("external_location", external_location)
        return build(*args, **kwargs)

    # MetaWorker.serve imports build_rpc_server from vgi.rpc_server at call time.
    vgi.rpc_server.build_rpc_server = build_externalizing
    fixture_worker.main()


if __name__ == "__main__":
    main()
