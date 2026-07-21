# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Serve the VGI fixture worker over HTTP with compression fully disabled.

``vgi-fixture-http`` always enables the compression middleware, so it can only
ever advertise a *non-empty* ``VGI-Supported-Encodings``.  This variant passes
``compression_level=None`` to ``make_wsgi_app``, which makes the server:

* advertise ``VGI-Supported-Encodings:`` with an **empty** value — the
  positive statement "I speak no compression", as distinct from an absent
  header (a legacy server, for which clients assume zstd), and
* reject any compressed request body (it has no decompressor installed).

It is the server the engine's identity-downgrade path is tested against:
a client that keeps sending zstd against this server fails with
``Invalid IPC stream: negative continuation token``.

Prints ``PORT:<port>`` on stdout once bound, matching the other fixture
servers so ``test/run_http_no_compression_integration.sh`` can pick it up.
"""

from __future__ import annotations

import os
import socket
import sys

from vgi._test_fixtures.worker import ExampleWorker
from vgi.protocol import VgiProtocol
from vgi_rpc import RpcServer
from vgi_rpc.http import make_wsgi_app


def main() -> None:
    # Ephemeral single-process fixture — no value in fsyncing through the WAL.
    os.environ.setdefault("VGI_WORKER_SQLITE_PATH", ":memory:")

    import waitress

    host = os.environ.get("VGI_HTTP_HOST", "127.0.0.1")
    port = int(os.environ.get("VGI_HTTP_PORT", "0"))
    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind((host, 0))
            port = int(probe.getsockname()[1])

    worker = ExampleWorker(quiet=True)
    worker._signing_key = os.urandom(32)
    server = RpcServer(VgiProtocol, worker)
    app = make_wsgi_app(
        server,
        token_key=worker._signing_key,
        compression_level=None,
        enable_landing_page=False,
    )

    print(f"PORT:{port}", flush=True)
    sys.stderr.write(f"compression disabled (compression_level=None) on {host}:{port}\n")
    sys.stderr.flush()
    waitress.serve(app, host=host, port=port, threads=8)


if __name__ == "__main__":
    main()
