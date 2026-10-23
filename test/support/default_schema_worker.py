#!/usr/bin/env python3
"""Native default-schema adaptation probe; uses the ordinary Worker boundary."""

from __future__ import annotations

import os
from dataclasses import replace
from typing import Annotated, Any

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, View
from vgi.catalog.setting import Setting
from vgi.worker import Worker

CATALOG = Catalog(
    name="default_schema_probe",
    default_schema="main",
    schemas=[
        Schema(path=["main"], views=[View(name="marker", definition="SELECT 11 AS value")]),
        Schema(path=["root.with.dot"], views=[View(name="marker", definition="SELECT 22 AS value")]),
    ],
)


class DefaultSchemaCatalog(ReadOnlyCatalogInterface):
    """Return a selected default while preserving both raw root schema names."""

    catalog = CATALOG

    def catalog_attach(self, **kwargs: Any) -> Any:
        result = super().catalog_attach(**kwargs)
        mode = os.environ["VGI_DEFAULT_SCHEMA_MODE"]
        defaults = {"none": None, "main": "main", "dotted": "root.with.dot"}
        return replace(result, default_schema=defaults[mode])


class DefaultSchemaWorker(Worker):
    """Expose an option to detect registration after a rejected attachment."""

    catalog = CATALOG
    catalog_interface = DefaultSchemaCatalog

    class Settings:
        """A sentinel setting that must not survive rejected attachment."""

        vgi_default_schema_probe_setting: Annotated[int, Setting(desc="Default schema probe sentinel")] = 7


if __name__ == "__main__":
    DefaultSchemaWorker.main()
