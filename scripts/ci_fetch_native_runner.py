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


def package(source: Path, build: Path, output: Path, revision: str) -> None:
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
    header = (source / "src/generated/vgi_protocol_version.hpp").read_text()
    version = re.search(r'VGI_PROTOCOL_VERSION\s*=\s*"([^"]+)"', header)
    if version is None:
        raise RuntimeError("generated protocol version is missing")
    manifest = {
        "revision": revision,
        "engine_revision": command("git", "-C", str(source), "rev-parse", "HEAD:duckdb"),
        "platform": platform_name(),
        "protocol_version": version.group(1),
        "sha256": hashes,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2), flush=True)


def api(path: str) -> dict:
    return json.loads(command("gh", "api", path))


def fetch(revision: str, output: Path, timeout: int, branch: str = "main") -> None:
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
        "--branch", default=os.environ.get("VGI_NATIVE_RUNNER_BRANCH", "main"),
        help="Branch that built the exact requested revision (default: main)",
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
