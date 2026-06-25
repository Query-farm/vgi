#!/usr/bin/env python3
# © Copyright 2026 Query Farm LLC - https://query.farm
"""Header fan-out guardrail for the VGI extension.

Two jobs:

1. Report per-header *transitive TU-reach*: how many compiled translation units
   (the .cpp files in CMakeLists.txt's EXTENSION_SOURCES) reach each project header
   through the quoted-include graph. A header reached by many TUs is expensive to
   edit -- every reacher recompiles. This is the metric the header-hygiene cleanup
   moves.

2. Enforce a *heavy-include denylist*: configured "hub" headers (those reached by
   many TUs) must NOT transitively pull in heavy third-party umbrellas (Arrow,
   Arrow IPC, ...). Forward-declare instead and push the real include into the .cpp
   that needs the definition -- the `vgi_logging.hpp` pattern. Violations exit
   non-zero so CI catches regressions.

Pure source scan -- no compiler, no build coupling. Resolves quoted includes the
way the build does: includer's own directory first, then the single -I root
`src/include`. System/angle-bracket includes are treated as opaque leaf nodes so we
can still detect heavy third-party umbrellas by name.

Usage:
    python scripts/header_reach.py                 # print reach table
    python scripts/header_reach.py --check         # also enforce denylist (CI mode)
    python scripts/header_reach.py --top 25        # show more rows
    python scripts/header_reach.py --header vgi_catalog_api.hpp   # who reaches it
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src"
INCLUDE_ROOT = SRC / "include"

# The single -I path the build adds for project headers (CMakeLists.txt:65).
INCLUDE_DIRS = [INCLUDE_ROOT]

QUOTED_INCLUDE = re.compile(r'^\s*#\s*include\s*"([^"]+)"')
ANGLE_INCLUDE = re.compile(r'^\s*#\s*include\s*<([^>]+)>')

# Heavy third-party umbrellas. A hub header reaching any of these (by the prefix
# match below) is flagged. Keep this list aligned with the "Header hygiene" section
# of CLAUDE.md.
HEAVY_INCLUDES = [
    "arrow/api.h",
    "arrow/ipc/",
    "arrow/io/api.h",
    "arrow/compute/",
]

# Hub headers whose heavy-include reach is enforced in --check mode. These are the
# widely-reached project headers that were deliberately made Arrow-free during the
# header-hygiene cleanup (they use Arrow only as forward-declared shared_ptr members
# / parameters). If a future edit re-introduces a heavy include into one of these,
# --check fails — forward-declare and push the real include into the .cpp instead.
# See CLAUDE.md "Header hygiene".
#
# NOT guarded (legitimate Arrow carriers — their consumers genuinely need Arrow, so
# forward-declaring would only relocate the include): vgi_rpc_types.hpp,
# vgi_arrow_utils.hpp, vgi_arrow_ipc.hpp, vgi_catalog_rpc.hpp, vgi_function_connection.hpp.
GUARDED_HEADERS = {
    "vgi_protocol.hpp": ["arrow/api.h", "arrow/ipc/", "arrow/io/api.h"],
    "vgi_attach_parameters.hpp": ["arrow/api.h", "arrow/ipc/", "arrow/io/api.h"],
    "vgi_catalog_metadata.hpp": ["arrow/api.h", "arrow/ipc/", "arrow/io/api.h"],
    "vgi_logging.hpp": ["arrow/api.h", "arrow/ipc/", "arrow/io/api.h"],
}


def read_extension_sources() -> list[Path]:
    """Parse the EXTENSION_SOURCES list out of CMakeLists.txt."""
    text = (REPO / "CMakeLists.txt").read_text()
    m = re.search(r"set\(EXTENSION_SOURCES(.*?)\)", text, re.DOTALL)
    if not m:
        sys.exit("could not find EXTENSION_SOURCES in CMakeLists.txt")
    sources = []
    for tok in m.group(1).split():
        tok = tok.strip()
        if tok.endswith(".cpp"):
            sources.append((REPO / tok).resolve())
    return sources


def resolve(include: str, includer: Path) -> Path | None:
    """Resolve a quoted include to a project file, or None if external."""
    candidates = [includer.parent / include]
    candidates += [d / include for d in INCLUDE_DIRS]
    for c in candidates:
        c = c.resolve()
        try:
            c.relative_to(SRC)
        except ValueError:
            continue
        if c.is_file():
            return c
    return None


def parse_includes(path: Path) -> tuple[list[Path], list[str]]:
    """Return (project includes resolved to paths, raw angle-bracket includes)."""
    project: list[Path] = []
    external: list[str] = []
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return project, external
    for line in lines:
        qm = QUOTED_INCLUDE.match(line)
        if qm:
            target = resolve(qm.group(1), path)
            if target is not None:
                project.append(target)
            else:
                external.append(qm.group(1))
            continue
        am = ANGLE_INCLUDE.match(line)
        if am:
            external.append(am.group(1))
    return project, external


def build_graph(seed: list[Path]) -> tuple[dict[Path, list[Path]], dict[Path, list[str]]]:
    """BFS the project include graph from the TU seeds. Returns (edges, externals)."""
    edges: dict[Path, list[Path]] = {}
    externals: dict[Path, list[str]] = {}
    queue = list(seed)
    while queue:
        node = queue.pop()
        if node in edges:
            continue
        proj, ext = parse_includes(node)
        edges[node] = proj
        externals[node] = ext
        for nxt in proj:
            if nxt not in edges:
                queue.append(nxt)
    return edges, externals


def transitive_headers(tu: Path, edges: dict[Path, list[Path]]) -> set[Path]:
    """All project headers reachable from a TU."""
    seen: set[Path] = set()
    stack = list(edges.get(tu, []))
    while stack:
        h = stack.pop()
        if h in seen:
            continue
        seen.add(h)
        stack.extend(edges.get(h, []))
    return seen


def transitive_externals(start: Path, edges, externals) -> set[str]:
    """All angle-bracket / external includes reachable from a header (incl. itself)."""
    seen_nodes: set[Path] = set()
    ext: set[str] = set()
    stack = [start]
    while stack:
        n = stack.pop()
        if n in seen_nodes:
            continue
        seen_nodes.add(n)
        ext.update(externals.get(n, []))
        stack.extend(edges.get(n, []))
    return ext


def is_heavy(name: str) -> bool:
    return any(name.startswith(h) for h in HEAVY_INCLUDES)


def rel(path: Path) -> str:
    return str(path.relative_to(REPO))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="enforce the GUARDED_HEADERS heavy-include denylist (CI mode)")
    ap.add_argument("--top", type=int, default=20, help="rows in the reach table")
    ap.add_argument("--header", help="show which TUs reach this header, then exit")
    args = ap.parse_args()

    tus = read_extension_sources()
    edges, externals = build_graph(tus)

    # header -> set of TUs reaching it
    reach: dict[Path, set[Path]] = defaultdict(set)
    for tu in tus:
        for h in transitive_headers(tu, edges):
            reach[h].add(tu)

    if args.header:
        target = (INCLUDE_ROOT / args.header).resolve()
        if target not in reach:
            # try a suffix match
            matches = [h for h in reach if h.name == args.header]
            if not matches:
                print(f"no header matching {args.header!r} is reached by any TU")
                return 1
            target = matches[0]
        tus_reaching = sorted(reach[target], key=rel)
        print(f"{rel(target)} is reached by {len(tus_reaching)} TUs:")
        for t in tus_reaching:
            print(f"  {rel(t)}")
        return 0

    # Only report project headers (under src/include).
    rows = []
    for h, tu_set in reach.items():
        try:
            h.relative_to(INCLUDE_ROOT)
        except ValueError:
            continue
        heavy = sorted({e for e in transitive_externals(h, edges, externals) if is_heavy(e)})
        n_lines = sum(1 for _ in h.open(errors="replace"))
        rows.append((len(tu_set), n_lines, h, heavy))
    rows.sort(key=lambda r: (-r[0], r[2].name))

    print(f"{'TUs':>4}  {'lines':>5}  header")
    print("-" * 72)
    for n, n_lines, h, heavy in rows[: args.top]:
        flag = "  <- heavy: " + ", ".join(heavy) if heavy else ""
        print(f"{n:>4}  {n_lines:>5}  {h.relative_to(INCLUDE_ROOT)}{flag}")
    print(f"\n{len(tus)} translation units, {len(rows)} project headers reached.")

    if args.check:
        violations = []
        for h, banned in GUARDED_HEADERS.items():
            target = (INCLUDE_ROOT / h).resolve()
            if not target.is_file():
                violations.append(f"GUARDED_HEADERS names {h!r} but it does not exist")
                continue
            reachable_ext = transitive_externals(target, edges, externals)
            for bad in banned:
                hits = sorted(e for e in reachable_ext if e.startswith(bad))
                if hits:
                    violations.append(
                        f"{h} must not reach heavy include(s) {hits} "
                        f"(reached by {len(reach.get(target, ()))} TUs)"
                    )
        if violations:
            print("\nDENYLIST VIOLATIONS:", file=sys.stderr)
            for v in violations:
                print(f"  - {v}", file=sys.stderr)
            return 1
        print("\ndenylist check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
