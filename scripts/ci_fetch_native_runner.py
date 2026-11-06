#!/usr/bin/env python3
"""Package or fetch the native CI executables for an exact VGI revision.

SDK development CI must use a client with the same wire contract as its workers.
The signed community extension is released separately and can still implement
the previous contract. This helper never falls back to another revision.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlencode


REPOSITORY = "Query-farm/vgi"
DEFAULT_BRANCH = "v2.0"


def command(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()


def platform_name() -> str:
    machines = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
    if platform.system() != "Linux" or platform.machine() not in machines:
        raise RuntimeError("native SDK CI artifacts require Linux amd64 or arm64")
    return "linux_" + machines[platform.machine()]


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


_ENGINE_TEST_VAR = re.compile(r"VGI_ENGINE_[A-Z0-9_]+")


def parse_engine_test_env(text: str) -> dict[str, str]:
    """``NAME=1`` lines from ``make engine_test_env`` (none for upstream DuckDB).

    The .test files gate engine-dependent files on these with require-env /
    require-env-not, and test/sql/engine_features/ fails when they disagree
    with the binary, so a lane running this unittest needs exactly these.
    """
    values = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        name, sep, value = line.strip().partition("=")
        if not sep or not _ENGINE_TEST_VAR.fullmatch(name) or value != "1":
            raise RuntimeError(f"unexpected engine test variable line: {line!r}")
        values[name] = value
    return values


def engine_test_env(source: Path) -> dict[str, str]:
    return parse_engine_test_env(
        command("make", "-s", "--no-print-directory", "-C", str(source), "engine_test_env")
    )


def engine_flavor(source: Path) -> str:
    """The engine the `duckdb` tree is: the same haybarn.h test as the build."""
    return "haybarn" if (source / "duckdb/src/include/duckdb/haybarn.h").is_file() else "duckdb"


def package(source: Path, build: Path, output: Path, revision: str) -> None:
    # The shell is build/haybarn on Haybarn and build/duckdb upstream. It is
    # packaged as `haybarn` either way, the name the SDK lanes already run.
    cli = build / "haybarn"
    if not cli.is_file():
        cli = build / "duckdb"
    binaries = {"unittest": build / "test/unittest", "haybarn": cli}
    output.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for name, binary in binaries.items():
        if not binary.is_file():
            raise RuntimeError(f"missing native build executable: {binary}")
        shutil.copy2(binary, output / name)
        hashes[name] = digest(output / name)
        # The artifact is the bare executables: any shared library they need from
        # the build tree (libhaybarn.so, or libduckdb.so upstream) is missing on the
        # SDK runners, where the binary then fails to start and every test is
        # silently skipped.
        linked = command("ldd", str(output / name))
        if "libhaybarn" in linked or "libduckdb" in linked or "not found" in linked:
            raise RuntimeError(f"{name} is not self-contained:\n{linked}")
    header = (source / "src/generated/vgi_protocol_version.hpp").read_text()
    version = re.search(r'VGI_PROTOCOL_VERSION\s*=\s*"([^"]+)"', header)
    if version is None:
        raise RuntimeError("generated protocol version is missing")
    manifest = {
        "revision": revision,
        # The engine actually checked out, not the committed submodule pin:
        # scripts/use_duckdb_engine.sh can switch the tree to upstream.
        "engine": engine_flavor(source),
        "engine_revision": command("git", "-C", str(source / "duckdb"), "rev-parse", "HEAD"),
        "platform": platform_name(),
        "protocol_version": version.group(1),
        "engine_test_env": engine_test_env(source),
        "sha256": hashes,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2), flush=True)


def api(path: str) -> dict:
    return json.loads(command("gh", "api", path))


def fetch(revision: str, output: Path, timeout: int, branch: str = DEFAULT_BRANCH) -> None:
    artifact_name = "vgi-native-runner-" + platform_name()
    deadline = time.monotonic() + timeout
    run_id = None
    while True:
        runs = api(
            f"repos/{REPOSITORY}/actions/workflows/integration.yml/runs"
            "?" + urlencode({"branch": branch, "head_sha": revision, "per_page": 10})
        )["workflow_runs"]
        for run in runs:
            # Never consume a pull-request merge build or a different revision.
            if run["head_sha"] != revision or run["head_branch"] != branch:
                continue
            if run["event"] not in ("push", "workflow_dispatch"):
                continue
            artifacts = api(f"repos/{REPOSITORY}/actions/runs/{run['id']}/artifacts")["artifacts"]
            if any(item["name"] == artifact_name and not item["expired"] for item in artifacts):
                run_id = run["id"]
                break
        if run_id is not None:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"no {artifact_name} artifact for VGI {revision}; "
                "check that revision's native Integration suite build"
            )
        print(f"Waiting for {artifact_name} at VGI {revision} ...", flush=True)
        time.sleep(min(15, max(0, deadline - time.monotonic())))

    with tempfile.TemporaryDirectory(prefix="vgi-native-download-") as directory:
        staging = Path(directory)
        command(
            "gh", "run", "download", str(run_id), "--repo", REPOSITORY,
            "--name", artifact_name, "--dir", str(staging),
        )
        manifest = json.loads((staging / "manifest.json").read_text())
        if manifest["revision"] != revision or manifest["platform"] != platform_name():
            raise RuntimeError("native artifact provenance does not match the requested revision/platform")
        for name in ("unittest", "haybarn"):
            if digest(staging / name) != manifest["sha256"][name]:
                raise RuntimeError(f"native artifact checksum mismatch: {name}")
        output.mkdir(parents=True, exist_ok=True)
        for name in ("unittest", "haybarn", "manifest.json"):
            shutil.copy2(staging / name, output / name)
        for name in ("unittest", "haybarn"):
            (output / name).chmod(0o755)

    values = {
        "HAYBARN_UNITTEST": str(output / "unittest"),
        "HAYBARN_CLI": str(output / "haybarn"),
        "RUNNER_LINKS_EXTENSIONS": "1",
    }
    # Artifacts packaged before the engine gates carry none (and their tests
    # have no gates). Re-validate: the manifest came from another workflow.
    engine_env = manifest.get("engine_test_env", {})
    if not isinstance(engine_env, dict):
        raise RuntimeError("native artifact engine_test_env is malformed")
    values.update(parse_engine_test_env("\n".join(f"{k}={v}" for k, v in engine_env.items())))
    environment = os.environ.get("GITHUB_ENV")
    if environment:
        with Path(environment).open("a") as target:
            for name, value in values.items():
                if "\n" in value or "\r" in value:
                    raise RuntimeError("native output path cannot contain newlines")
                target.write(f"{name}={value}\n")
    print(f"Using native VGI {revision}, protocol {manifest['protocol_version']}, run {run_id}", flush=True)
    for name, value in values.items():
        print(f"{name}={value}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pack", action="store_true")
    parser.add_argument("--build", type=Path, default=Path("build/release"))
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument(
        "--branch", default=os.environ.get("VGI_NATIVE_RUNNER_BRANCH", DEFAULT_BRANCH),
        help=f"Branch that built the exact requested revision (default: {DEFAULT_BRANCH})",
    )
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    revision = command("git", "-C", str(source), "rev-parse", "HEAD")
    if args.pack:
        build = args.build if args.build.is_absolute() else source / args.build
        package(source, build, output, revision)
    else:
        fetch(revision, output, args.timeout, args.branch)


if __name__ == "__main__":
    main()
