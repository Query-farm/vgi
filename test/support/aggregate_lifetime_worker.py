#!/usr/bin/env python3
"""Aggregate-lifetime probe: journals every aggregate RPC of one execution.

Serves catalog ``agglife``, schema ``main``, with vgi-python's ``vgi_sum``
aggregate. Every aggregate RPC appends one line to the journal named by
``--journal PATH`` before it is handled::

    <monotonic_ns>,<pid>,<method>,<execution_id hex>

The journal is shared by every process of this worker (stdio pools, the
launcher) and appends are serialized with ``flock``, so the order of the lines
is the order in which the worker saw the calls, whichever process took them.

test/sql/aggregate_lifetime/*.test read it back with ``read_csv`` and check the
extension's aggregate lifetime contract: one ``aggregate_destructor`` per
execution, sent after every other call of that execution. The destructor
clears the execution's whole function storage (bind-time arguments included),
so a call after it reaches a worker that has forgotten the execution: workers
whose storage is shared across processes fail it with "no bind-time arguments
in function storage".

Run as ``uv run --project <vgi-python> python test/support/aggregate_lifetime_worker.py
--journal PATH`` (stdin/stdout, or ``--unix PATH`` under the launcher).
"""

from __future__ import annotations

import fcntl
import os
import sys
import time
from typing import Any

from vgi._test_fixtures.aggregate.basic import SumFunction
from vgi.catalog import Catalog, Schema
from vgi.worker import Worker

JOURNAL_FLAG = "--journal"


def _take_journal_path() -> str:
    if JOURNAL_FLAG not in sys.argv:
        msg = f"aggregate_lifetime_worker: {JOURNAL_FLAG} PATH is required"
        raise SystemExit(msg)
    index = sys.argv.index(JOURNAL_FLAG)
    path = sys.argv[index + 1]
    del sys.argv[index : index + 2]
    return path


JOURNAL = _take_journal_path()

CATALOG = Catalog(
    name="agglife",
    default_schema="main",
    schemas=[Schema(path=["main"], functions=[SumFunction])],
)


def _record(method: str, request: Any) -> None:
    execution_id = getattr(request, "execution_id", None) or b""
    fd = os.open(JOURNAL, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        line = f"{time.monotonic_ns()},{os.getpid()},{method},{bytes(execution_id).hex()}\n"
        os.write(fd, line.encode())
    finally:
        os.close(fd)


class AggregateLifetimeWorker(Worker):
    """Serves ``agglife`` and journals each aggregate RPC before handling it."""

    catalog_name = "agglife"
    catalog = CATALOG

    def aggregate_update(self, request: Any, ctx: Any) -> Any:
        _record("update", request)
        return super().aggregate_update(request, ctx)

    def aggregate_combine(self, request: Any, ctx: Any) -> Any:
        _record("combine", request)
        return super().aggregate_combine(request, ctx)

    def aggregate_finalize(self, request: Any, ctx: Any) -> Any:
        _record("finalize", request)
        return super().aggregate_finalize(request, ctx)

    def aggregate_destructor(self, request: Any, ctx: Any) -> Any:
        _record("destructor", request)
        return super().aggregate_destructor(request, ctx)


def main() -> None:
    """Run the worker (stdio, ``--unix`` or ``--http``)."""
    AggregateLifetimeWorker.main()


if __name__ == "__main__":
    main()
