#!/usr/bin/env python3
"""Run nullable-default native unit and ordinary subprocess SQL probes."""

import os
from pathlib import Path
import shlex
import subprocess


def main() -> int:
    native = Path(__file__).resolve().parents[1]
    python = Path(os.environ.get("VGI_PYTHON_DIR") or native.parent / "vgi-python").resolve()
    build = Path(os.environ.get("VGI_BUILD_DIR") or native / "build/release").resolve()
    env = dict(os.environ)
    for mode in ("none", "main", "dotted"):
        env[f"VGI_DEFAULT_SCHEMA_{mode.upper()}_WORKER"] = shlex.join(
            [
                "env",
                f"VGI_DEFAULT_SCHEMA_MODE={mode}",
                "uv",
                "--directory",
                str(python),
                "run",
                "--no-sync",
                "python",
                str(native / "test/support/default_schema_worker.py"),
            ]
        )
    subprocess.run(
        [str(build / "extension/vgi/vgi_catalog_tests"), "[default-schema]"], cwd=native, env=env, check=True
    )
    return subprocess.call(
        [
            str(build / "test/unittest"),
            "--test-config",
            "test/configs/no_error_skip.json",
            "test/sql/duckdb2/default_schema.test",
        ],
        cwd=native,
        env=env,
    )


if __name__ == "__main__":
    raise SystemExit(main())
