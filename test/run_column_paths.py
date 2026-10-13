#!/usr/bin/env python3
"""Run native successor-column SQL coverage against the durable Python reference."""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile


def main() -> int:
    native = Path(__file__).resolve().parents[1]
    python = Path(os.environ.get("VGI_PYTHON_DIR") or native.parent / "vgi-python").resolve()
    build = Path(os.environ.get("VGI_BUILD_DIR") or native / "build/release").resolve()
    state = build / "column-reference"
    state.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=state) as directory:
        env = dict(os.environ)
        env["VGI_COLUMN_REFERENCE_WORKER"] = shlex.join([
            "env", f"VGI_COLUMN_REFERENCE_DB={directory}/catalog.sqlite",
            "uv", "--directory", str(python), "run", "--no-sync",
            "python", "-m", "tests.catalog.column_native_reference",
        ])
        env["VGI_REQUIRED_FILTER_WORKER"] = shlex.join([
            "uv", "--directory", str(python), "run", "--no-sync",
            "python", "-m", "tests.catalog.required_filters_native",
        ])
        env["VGI_VIEW_ALIAS_WORKER"] = shlex.join([
            "uv", "--directory", str(python), "run", "--no-sync",
            "python", "-m", "tests.catalog.view_alias_native",
        ])
        return subprocess.call([
            str(build / "test/unittest"), "--test-config", "test/configs/no_error_skip.json",
            "test/sql/duckdb2/column_paths.test,test/sql/duckdb2/required_filter_paths.test,test/sql/duckdb2/view_alias_metadata.test",
        ], cwd=native, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
