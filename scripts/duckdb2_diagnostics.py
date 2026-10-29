#!/usr/bin/env python3
"""Compile the complete native VGI source list against the audited 2.0 pin.

Run on the Linux/Clang build host in a separate source checkout. This is an
extension-only diagnostic lane, not an engine link or runtime qualification.
Existing nanoarrow/FlatCC/OpenSSL/zstd/LZ4/SQLite package installations are
consumed without running vcpkg. Install the pinned overlay ports first.
"""

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

ENGINE_REVISION = "927f673d7e63aece903e81d5fb4be330e24be038"
ENGINE_VERSION = "v2.0.0-dev88803"
ROOT = Path(__file__).resolve().parents[1]


def revision(path):
    return subprocess.check_output(
        ["git", "-C", str(path), "rev-parse", "HEAD"], text=True
    ).strip()


def run_logged(command, log):
    print(shlex.join(command), flush=True)
    with log.open("w") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
    print(f"Exit {result.returncode}; log: {log}", flush=True)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-source", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--dependency-prefix", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--cc", default="clang")
    parser.add_argument("--cxx", default="clang++")
    args = parser.parse_args()
    engine = args.engine_source.resolve()
    build = args.build_dir.resolve()
    prefix = args.dependency_prefix.resolve()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if revision(engine) != ENGINE_REVISION:
        parser.error(f"engine must be the audited pin {ENGINE_REVISION}")
    if subprocess.check_output(
        ["git", "-C", str(engine), "status", "--porcelain", "--untracked-files=no"],
        text=True,
    ).strip():
        parser.error("the audited engine checkout has tracked modifications")
    if (ROOT / "duckdb").resolve() != engine:
        parser.error(
            "use an isolated source checkout whose duckdb path points to --engine-source"
        )
    if build == ROOT or build == engine or build == prefix:
        parser.error("--build-dir must be a separate output directory")
    cache = build / "CMakeCache.txt"
    if (
        cache.exists()
        and f"CMAKE_HOME_DIRECTORY:INTERNAL={engine}\n" not in cache.read_text()
    ):
        parser.error(
            "refusing to reuse a build directory configured for another engine"
        )
    if cache.exists() and re.search(
        r"^CMAKE_TOOLCHAIN_FILE:[^=]+=.+$", cache.read_text(), re.MULTILINE
    ):
        parser.error(
            "refusing a cached toolchain; use a separate diagnostic build directory"
        )
    build.mkdir(parents=True, exist_ok=True)
    record = {
        "engine_revision": ENGINE_REVISION,
        "engine_version": ENGINE_VERSION,
        "native_revision": revision(ROOT),
        "source_root": str(ROOT),
        "dependency_prefix": str(prefix),
        "scope": "All native VGI translation units; Iroh disabled; no httpfs, engine link or runtime proof",
        "error_limit_per_translation_unit": 12,
        "native_worktree_status": subprocess.check_output(
            ["git", "-C", str(ROOT), "status", "--porcelain", "--ignore-submodules=all"], text=True
        ).splitlines(),
    }
    source_files = sorted(
        p for source_dir in (ROOT / "src", ROOT / "vcpkg_overlay_ports")
        for p in source_dir.rglob("*") if p.is_file()
    )
    source_files += [
        ROOT / "CMakeLists.txt",
        ROOT / "vcpkg.json",
        ROOT / "extension_config_duckdb2_diagnostics.cmake",
        Path(__file__),
    ]
    source_manifest = {
        str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in source_files
    }
    manifest_bytes = (
        json.dumps(source_manifest, indent=2, sort_keys=True) + "\n"
    ).encode()
    (build / "source-manifest.json").write_bytes(manifest_bytes)
    record["source_manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    status = prefix.parent / "vcpkg/status"
    if status.exists():
        record["dependency_status_sha256"] = hashlib.sha256(
            status.read_bytes()
        ).hexdigest()
    compiler_version = subprocess.check_output([args.cxx, "--version"], text=True)
    error_limit = "-ferror-limit=12" if "clang" in compiler_version.lower() else "-fmax-errors=12"
    command = [
        "cmake",
        "-S",
        str(engine),
        "-B",
        str(build),
        "-G",
        "Ninja",
        "-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
        "-DCMAKE_TOOLCHAIN_FILE=",
        "-DCMAKE_UNITY_BUILD=OFF",
        "-DCMAKE_BUILD_TYPE=Debug",
        "-DCMAKE_CXX_FLAGS_DEBUG=-O0 -g0",
        f"-DCMAKE_C_COMPILER={args.cc}",
        f"-DCMAKE_CXX_COMPILER={args.cxx}",
        f"-DCMAKE_CXX_FLAGS={error_limit} -fdiagnostics-color=never",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
        "-DDISABLE_UNITY=ON",
        "-DBUILD_EXTENSIONS_ONLY=ON",
        "-DBUILD_UNITTESTS=OFF",
        "-DBUILD_SHELL=OFF",
        "-DVGI_ENABLE_IROH=OFF",
        f"-DDUCKDB_EXPLICIT_VERSION={ENGINE_VERSION}",
        f"-DGIT_COMMIT_HASH={ENGINE_REVISION}",
        f"-DDUCKDB_EXTENSION_CONFIGS={ROOT / 'extension_config_duckdb2_diagnostics.cmake'}",
        f"-DCMAKE_PREFIX_PATH={prefix}",
        f"-DOPENSSL_ROOT_DIR={prefix}",
    ]
    record["configure_command"] = command
    code = run_logged(command, build / "configure.log")
    record["configure_exit"] = code
    if code:
        (build / "inventory.json").write_text(json.dumps(record, indent=2) + "\n")
        return code
    cache_values = dict(
        re.findall(r"^([A-Za-z_][A-Za-z_0-9-]*):[^=\n]*=(.*)$", cache.read_text(), re.MULTILINE)
    )
    compiler = cache_values["CMAKE_CXX_COMPILER"]
    record["tools"] = {
        "compiler": subprocess.check_output(
            [compiler, "--version"], text=True
        ).splitlines()[0],
        "cmake": subprocess.check_output(
            ["cmake", "--version"], text=True
        ).splitlines()[0],
        "compiler_launcher": cache_values.get("CMAKE_CXX_COMPILER_LAUNCHER", ""),
    }
    entries = json.loads((build / "compile_commands.json").read_text())
    units = {}
    for entry in entries:
        argv = entry.get("arguments") or shlex.split(entry["command"])
        output = argv[argv.index("-o") + 1]
        if "CMakeFiles/vgi_extension.dir/" in output:
            units[output] = {
                "source": entry["file"],
                "generated": not Path(entry["file"]).is_relative_to(ROOT),
                "errors": [],
            }
    if not units:
        raise RuntimeError("configured build contains no static VGI translation units")
    command = [
        "cmake",
        "--build",
        str(build),
        "--target",
        "vgi_extension",
        "--parallel",
        str(args.jobs),
        "--",
        "-k",
        "0",
    ]
    record["build_command"] = command
    code = run_logged(command, build / "compile.log")
    record["build_exit"] = code
    active = None
    for line in (build / "compile.log").read_text(errors="replace").splitlines():
        if line.startswith("FAILED:"):
            active = next((output for output in units if output in line.split()), None)
            if active:
                units[active]["status"] = "failed"
        elif re.match(r"^\[\d+/\d+\]", line):
            active = None
        elif active and re.search(r": (?:fatal )?error:", line):
            units[active]["errors"].append(line)
    for output, unit in units.items():
        unit.setdefault(
            "status", "compiled" if (build / output).exists() else "not_attempted"
        )
    record["translation_units"] = list(units.values())
    record["counts"] = {
        name: sum(unit["status"] == name for unit in units.values())
        for name in ("compiled", "failed", "not_attempted")
    }
    record["native_cpp_counts"] = {
        name: sum(
            unit["status"] == name and not unit["generated"] for unit in units.values()
        )
        for name in ("compiled", "failed", "not_attempted")
    }
    record["limits"] = (
        "First 12 compiler errors per unit; shared-header failures may mask later errors. No runtime or final linking qualification."
    )
    (build / "inventory.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record["counts"]), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
