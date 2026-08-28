# © Copyright 2025, 2026 Query Farm LLC - https://query.farm
"""Small real VGI worker used by database-package build demonstrations."""

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa

from vgi import Arg, Worker
from vgi.catalog import Catalog, Schema
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)


@dataclass(slots=True, frozen=True, kw_only=True)
class SeriesArgs:
    """Arguments for the packaged series function."""

    count: Annotated[int, Arg(0, doc="Number of integers to generate", ge=0)]


@init_single_worker
@bind_fixed_schema
class Series(TableFunctionGenerator[SeriesArgs]):
    """Generate integers from zero through ``count - 1``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("n", pa.int64())])

    @classmethod
    def process(cls, params: ProcessParams[SeriesArgs], state: None, out: OutputCollector) -> None:
        """Emit the requested series and finish."""
        out.emit(pa.RecordBatch.from_pydict({"n": list(range(params.args.count))}, schema=params.output_schema))
        out.finish()


class PackagedPythonWorker(Worker):
    """Worker exposing the ``python_package`` catalog."""

    catalog = Catalog(
        name="python_package",
        schemas=[Schema(name="main", functions=[Series])],
    )


if __name__ == "__main__":
    PackagedPythonWorker().run()
