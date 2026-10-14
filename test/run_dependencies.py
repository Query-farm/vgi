#!/usr/bin/env python3
"""Run native schema lifecycle SQL against the durable dependency authority."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


def main() -> int:
    native = Path(__file__).resolve().parents[1]
    python = Path(os.environ.get("VGI_PYTHON_DIR") or native.parent / "vgi-python").resolve()
    build = Path(os.environ.get("VGI_BUILD_DIR") or native / "build/release").resolve()
    state = build / "dependency-reference"
    state.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=state) as directory:
        env = dict(os.environ)
        env["VGI_DEPENDENCY_REFERENCE_WORKER"] = shlex.join([
            "env", f"VGI_DEPENDENCY_REFERENCE_DB={directory}/catalog.sqlite",
            "uv", "--directory", str(python), "run", "--no-sync",
            "python", "-m", "tests.catalog.dependencies_native",
        ])
        return subprocess.call([
            str(build / "test/unittest"), "--test-config", "test/configs/no_error_skip.json",
            "test/sql/duckdb2/schema_dependencies.test",
        ], cwd=native, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
