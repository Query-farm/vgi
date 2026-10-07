#!/usr/bin/env python3
"""A worker that predates vgi-rpc reflection: vgi-python's fixture worker with
``vgi_rpc.Reflection.v1`` not hosted.

Every released VGI SDK hosts reflection on every transport, so a real
pre-reflection worker is no longer buildable from a current SDK. This one
answers a reflection request exactly as such a worker's server does -- with
``protocol_not_supported`` -- while serving the ordinary fixture catalogs over
``vgi.v2``. Drives test/sql/integration/reflection/pre_reflection.test.

Run as ``uv run --project <vgi-python> python test/support/no_reflection_worker.py``
(stdin/stdout, or ``--unix PATH`` under the launcher, like the fixture worker).
"""

from __future__ import annotations

from typing import Any

import vgi.rpc_server
import vgi.worker

_build_rpc_server = vgi.rpc_server.build_rpc_server


def _build_without_reflection(worker: Any, **kwargs: Any) -> Any:
    kwargs["describe"] = False
    return _build_rpc_server(worker, **kwargs)


# MetaWorker.serve imports the builder from vgi.rpc_server at call time;
# Worker binds it at import. Patch both before the fixture starts.
vgi.rpc_server.build_rpc_server = _build_without_reflection  # type: ignore[assignment]
vgi.worker.build_rpc_server = _build_without_reflection  # type: ignore[attr-defined]

if __name__ == "__main__":
    from vgi._test_fixtures.worker import main

    main()
