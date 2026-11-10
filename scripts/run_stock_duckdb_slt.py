#!/usr/bin/env python3
"""Run SQLLogicTest files on stock DuckDB with a locally built VGI extension.

The integration suite runs on the Haybarn engine, which cannot load the
spatial extension, so every `require spatial` file skips there. This runner
uses the stock `duckdb` Python package instead: it loads the VGI extension
built by this tree (unsigned, metadata mismatch allowed) next to the real
spatial and httpfs extensions from DuckDB's repository, and runs the given
.test files against VGI_TEST_WORKER.

It supports the subset of the SQLLogicTest format those files use:
`require`, `require-env`, `statement ok|error`, and `query <types>` with plain
or `<REGEX>:` / `<!REGEX>:` expected values. Anything else is an error, so a
test cannot silently run less than it says.

Usage (from the repository root):

  VGI_TEST_WORKER='<worker command or http://host:port>' \\
    uv run --with duckdb==2.0.0 python scripts/run_stock_duckdb_slt.py \\
      --extension build/release/extension/vgi/vgi.duckdb_extension FILE...

Exits non-zero if any file fails, or if a file was skipped and --no-skip is set.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

SUPPORTED_REQUIRES = {"vgi", "spatial", "httpfs"}


@dataclass
class Record:
    kind: str  # "statement" or "query"
    line: int
    sql: str
    expect_error: bool = False
    expected: list[str] | None = None


class Skip(Exception):
    pass


def substitute(text: str) -> str:
    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in os.environ:
            raise Skip(f"require-env {name}")
        return os.environ[name]

    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", repl, text)


def parse(path: Path) -> tuple[list[str], list[str], list[Record]]:
    lines = path.read_text().splitlines()
    requires: list[str] = []
    require_envs: list[str] = []
    records: list[Record] = []
    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.strip()
        if not line or line.startswith("#"):
            i += 1
            continue
        words = line.split()
        if words[0] == "require":
            requires.append(words[1])
            i += 1
        elif words[0] == "require-env":
            require_envs.append(words[1])
            i += 1
        elif words[0] in ("statement", "query"):
            start = i + 1
            i += 1
            body: list[str] = []
            while i < len(lines) and lines[i].strip() and lines[i].strip() != "----":
                body.append(lines[i])
                i += 1
            expected: list[str] | None = None
            if i < len(lines) and lines[i].strip() == "----":
                i += 1
                expected = []
                while i < len(lines) and lines[i].strip():
                    expected.append(lines[i])
                    i += 1
            if words[0] == "statement":
                if words[1] not in ("ok", "error"):
                    raise ValueError(f"{path}:{start}: unsupported statement mode {words[1]!r}")
                records.append(Record("statement", start, "\n".join(body), expect_error=words[1] == "error",
                                      expected=expected))
            else:
                if len(words) > 2:
                    raise ValueError(f"{path}:{start}: unsupported query sort mode {words[2:]!r}")
                records.append(Record("query", start, "\n".join(body), expected=expected or []))
        else:
            raise ValueError(f"{path}:{i + 1}: unsupported directive {words[0]!r}")
    return requires, require_envs, records


def render(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return repr(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "backslashreplace")
    return str(value)


def matches(actual: str, expected: str) -> bool:
    if expected.startswith("<REGEX>:"):
        return re.fullmatch(expected[len("<REGEX>:"):], actual) is not None
    if expected.startswith("<!REGEX>:"):
        return re.fullmatch(expected[len("<!REGEX>:"):], actual) is None
    if actual == expected:
        return True
    try:  # 0.5 vs 0.50, 1 vs 1.0
        return float(actual) == float(expected)
    except ValueError:
        return False


def connect(extension: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true", "allow_extensions_metadata_mismatch": "true"})
    con.execute(f"LOAD '{extension}'")
    return con


def run_file(path: Path, extension: str) -> tuple[str, list[str]]:
    requires, require_envs, records = parse(path)
    for name in require_envs:
        if name not in os.environ:
            return f"skipped (require-env {name})", []
    unknown = [r for r in requires if r not in SUPPORTED_REQUIRES]
    if unknown:
        return f"skipped (require {', '.join(unknown)})", []
    con = connect(extension)
    for name in requires:
        if name != "vgi":
            con.execute(f"INSTALL {name}")
            con.execute(f"LOAD {name}")
    failures: list[str] = []
    executed = 0
    for record in records:
        try:
            sql = substitute(record.sql)
        except Skip as skip:
            return f"skipped ({skip})", []
        where = f"{path}:{record.line}"
        try:
            rows = con.execute(sql).fetchall()
        except duckdb.Error as error:
            executed += 1
            if record.kind == "statement" and record.expect_error:
                if record.expected and not any(e in str(error) for e in record.expected):
                    failures.append(f"{where}: error text {str(error)!r} does not contain {record.expected!r}")
                continue
            failures.append(f"{where}: {type(error).__name__}: {error}")
            continue
        executed += 1
        if record.kind == "statement":
            if record.expect_error:
                failures.append(f"{where}: expected an error, statement succeeded")
            continue
        expected = record.expected or []
        actual = ["\t".join(render(v) for v in row) for row in rows]
        if len(actual) != len(expected):
            failures.append(f"{where}: expected {len(expected)} rows, got {len(actual)}: {actual[:5]!r}")
            continue
        for row_actual, row_expected in zip(actual, expected, strict=True):
            a_cols, e_cols = row_actual.split("\t"), row_expected.split("\t")
            if len(a_cols) != len(e_cols) or not all(matches(a, e) for a, e in zip(a_cols, e_cols, strict=True)):
                shown = row_actual if len(row_actual) < 2000 else row_actual[:2000] + "..."
                failures.append(f"{where}: expected {row_expected!r}\n    got {shown!r}")
                break
    con.close()
    return f"ran {executed} records", failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--extension", required=True, help="path to the built vgi.duckdb_extension")
    parser.add_argument("--no-skip", action="store_true", help="fail if any file is skipped")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    extension = str(Path(args.extension).resolve())
    print(f"duckdb {duckdb.__version__}, extension {extension}")
    failed = skipped = 0
    for name in args.files:
        status, failures = run_file(Path(name), extension)
        mark = "FAIL" if failures else ("SKIP" if status.startswith("skipped") else "ok")
        print(f"{mark:4}  {name}: {status}")
        for failure in failures:
            print(f"      {failure}")
        failed += bool(failures)
        skipped += status.startswith("skipped")
    print(f"{len(args.files) - failed - skipped} passed, {failed} failed, {skipped} skipped")
    return 1 if failed or (args.no_skip and skipped) else 0


if __name__ == "__main__":
    sys.exit(main())
