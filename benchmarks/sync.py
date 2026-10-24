#!/usr/bin/env python3
"""Copy tracked working-tree inputs (including edits) to a dedicated SSH workspace.

Does not copy environments, credentials, git object databases or build outputs.
Records content hashes because a commit alone does not identify a dirty checkout.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import subprocess
import tarfile
import tempfile


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args])


def inputs(root):
    files = {os.fsdecode(p) for p in git(root, "ls-files", "-z").split(b"\0") if p}
    # The harness is useful before its first commit. Only this known directory's
    # new source files are included, never arbitrary untracked working-tree files.
    if (root / "benchmarks/bench.py").is_file():
        files.update(str(p.relative_to(root)) for p in (root / "benchmarks").rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts and p.suffix in {".py", ".cpp", ".sh", ".md", ".json", ".cmake"})
    # Rust SDK currently gitignores Cargo.lock. Copy it explicitly to reproduce
    # the resolved dependencies; the remote build must use --locked.
    if (root / "Cargo.lock").is_file():
        files.add("Cargo.lock")
    return sorted(p for p in files if (root / p).is_file() or (root / p).is_symlink())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--remote-dir", required=True, help="absolute, dedicated workspace directory")
    parser.add_argument("--repo", action="append", required=True, metavar="NAME=PATH")
    args = parser.parse_args()
    destination = PurePosixPath(args.remote_dir)
    if not destination.is_absolute() or len(destination.parts) < 4 or ".." in destination.parts:
        parser.error("use an absolute dedicated directory below your remote home")
    manifests = {}
    repositories = []
    for spec in args.repo:
        name, source = spec.split("=", 1)
        if not name or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-+_" for c in name):
            parser.error("invalid repository name")
        root = Path(source).expanduser().resolve()
        repositories.append((name, root))
        if name == "vgi":
            for child in ("duckdb", "extension-ci-tools"):
                if (root / child).is_dir():
                    repositories.append((name + "/" + child, root / child))
    with tempfile.TemporaryFile() as archive:
        with tarfile.open(fileobj=archive, mode="w:gz", dereference=False) as tar:
            for name, root in repositories:
                tree = hashlib.sha256()
                count = 0
                for relative in inputs(root):
                    path = root / relative
                    if path.is_symlink():
                        data = os.readlink(path).encode()
                    else:
                        data = path.read_bytes()
                    tree.update(relative.encode() + b"\0" + str(path.lstat().st_mode).encode() + b"\0" + hashlib.sha256(data).digest())
                    tar.add(path, arcname="repos/" + name + "/" + relative, recursive=False)
                    count += 1
                manifests[name] = {"commit": git(root, "rev-parse", "HEAD").decode().strip(),
                                   "status": git(root, "status", "--porcelain").decode(),
                                   "tree_sha256": tree.hexdigest(), "files": count}
                print(f"{name}: {count} inputs, sha256 {tree.hexdigest()}", flush=True)
        archive.seek(0)
        remote = shlex.quote(str(destination))
        # Require an empty source destination on first use; subsequent syncs are
        # permitted only in a workspace bearing our marker. Never --delete.
        command = (f"(test -f {remote}/.vgi-benchmark-workspace || test ! -e {remote}) && "
                   f"mkdir -p {remote} && touch {remote}/.vgi-benchmark-workspace && tar -xzf - -C {remote}")
        subprocess.run(["ssh", args.host, command], stdin=archive, check=True)
    subprocess.run(["ssh", args.host, f"cat > {shlex.quote(str(destination / 'sources.json'))}"],
                   input=(json.dumps(manifests, indent=2) + "\n").encode(), check=True)
    print(f"Sources copied to {args.host}:{destination}; builds and tests have not run.")


if __name__ == "__main__":
    main()
