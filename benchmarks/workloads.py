"""Deterministic SQL shared by every framework; no SDK-specific timing code."""
from __future__ import annotations

MODES = {"native", "echo", "buffer_input", "cached_echo"}


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def workload(case, mode):
    if mode not in MODES:
        raise ValueError(f"unsupported mode: {mode}")
    rows, columns = case["rows"], case["columns"]
    width, null_every = case.get("value_bytes", 32), case.get("null_every", 0)
    if not isinstance(rows, int) or not 1 <= rows <= 10**12:
        raise ValueError("rows must be in [1, 10^12]")
    if not isinstance(columns, int) or not 1 <= columns <= 2048:
        raise ValueError("columns must be in [1, 2048]")
    if not isinstance(null_every, int) or null_every < 0:
        raise ValueError("null_every must be a nonnegative integer")
    if case["shape"] not in {"int64", "varchar"}:
        raise ValueError("shape must be int64 or varchar")
    if case["shape"] == "varchar" and (not isinstance(width, int) or not 24 <= width <= 2**20):
        raise ValueError("varchar value_bytes must be in [24, 1048576]")
    source_kind = case.get("source", "sql")
    if source_kind not in {"sql", "batch"}:
        raise ValueError("source must be sql or batch")
    if source_kind == "batch":
        batch_rows = case.get("batch_rows")
        if case["shape"] != "varchar" or not isinstance(batch_rows, int) or not 1 <= batch_rows <= 2048:
            raise ValueError("batch source requires varchar and batch_rows in [1, 2048]")
    elif "batch_rows" in case:
        raise ValueError("batch_rows requires source=batch")
    missing = (rows - 1) // null_every + 1 if null_every else 0
    missing_sum = null_every * missing * (missing - 1) // 2
    present = rows - missing
    expressions, checks, expected = [], [], 0
    for c in range(columns):
        if case["shape"] == "int64":
            expr = f"(i + {c})::BIGINT"
            check = f"coalesce(sum(c{c}), 0)"
            expected += rows * (rows - 1) // 2 - missing_sum + c * present
        else:
            # Distinct row/column suffix prevents a constant-vector benchmark.
            expr = f"repeat('x', {width - 24}) || lpad(i::VARCHAR, 20, '0') || '{c:04d}'"
            check = f"coalesce(sum(length(c{c})), 0)"
            expected += present * width
        if null_every:
            expr = f"CASE WHEN i % {null_every} = 0 THEN NULL ELSE {expr} END"
        expressions.append(f"{expr} AS c{c}")
        checks.append(check)
    source = f"SELECT {', '.join(expressions)} FROM range({rows}) t(i)"
    if source_kind == "batch":
        source = f"SELECT * FROM bench_strings({rows}, {columns}, {width}, {batch_rows}, {null_every})"
    relation = f"({source}) input" if mode == "native" else f"bench.{mode}(({source}))"
    query = f"SELECT ({' + '.join(checks)})::HUGEINT FROM {relation}"
    return query, str(expected), rows * columns * (8 if case["shape"] == "int64" else width)
