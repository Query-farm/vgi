#!/usr/bin/env python3
"""HTTP VGI fixture behind a trusted loopback Iroh bridge boundary.

The worker deliberately has no bearer-token fallback.  Every request must
carry the canonical endpoint header inserted by ``vgi-iroh-bridge`` and must
arrive from the explicitly trusted loopback proxy address.  This makes the
native integration test prove identity propagation, rather than merely prove
that HTTP bytes can traverse Iroh.
"""

from __future__ import annotations

import os
import socket

from vgi._test_fixtures.worker import ExampleWorker
from vgi.protocol import VgiProtocol
from vgi_rpc.http import iroh_forwarded_header_provider, make_wsgi_app
from vgi_rpc.rpc import RpcServer, peer_identity_primary


def main() -> None:
    """Serve an ExampleWorker authenticated only by forwarded Iroh identity."""
    os.environ.setdefault("VGI_WORKER_SQLITE_PATH", ":memory:")
    host = "127.0.0.1"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        port = int(sock.getsockname()[1])

    provider = iroh_forwarded_header_provider(
        issuer="vgi.test",
        trusted_proxy_addresses=(host,),
    )
    server = RpcServer(VgiProtocol, ExampleWorker(quiet=True))
    app = make_wsgi_app(
        server,
        peer_identity_providers=(provider,),
        peer_authentication_policy=peer_identity_primary("iroh"),
        enable_landing_page=False,
    )

    try:
        import waitress
    except ImportError as error:  # pragma: no cover - integration dependency
        raise SystemExit("HTTP integration fixture requires waitress") from error

    print(f"PORT:{port}", flush=True)
    waitress.serve(app, host=host, port=port, _quiet=True)


if __name__ == "__main__":
    main()
