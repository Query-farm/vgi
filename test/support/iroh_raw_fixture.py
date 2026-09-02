#!/usr/bin/env python3
"""Loopback VGI fixture behind the trusted Iroh PROXY-v2 boundary."""

from __future__ import annotations

from vgi._test_fixtures.worker import ExampleWorker
from vgi.meta_worker import MetaWorker
from vgi.protocol import VgiProtocol
from vgi_rpc.rpc import RpcServer, peer_identity_primary, serve_tcp


def main() -> None:
    implementation = MetaWorker([ExampleWorker()])
    server = RpcServer(VgiProtocol, implementation)
    serve_tcp(
        server,
        "127.0.0.1",
        0,
        threaded=True,
        proxy_protocol="required",
        trusted_proxy_addresses=("127.0.0.1",),
        iroh_proxy_issuer="vgi.test",
        peer_authentication_policy=peer_identity_primary("iroh"),
        on_bound=lambda host, port: print(f"TCP:{host}:{port}", flush=True),
    )


if __name__ == "__main__":
    main()
