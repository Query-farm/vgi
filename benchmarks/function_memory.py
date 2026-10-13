#!/usr/bin/env python3
"""Linux diagnostic memory matrix across VGI function families.

Uses the benchmark host and existing example workers. Records exact SQL, checksums,
process memory, allocator counters and release stages. Timings are diagnostic:
allocator inspection, sampling and any host contention preclude regression gates.
"""
import argparse
import contextlib
import csv
import html
import json
import os
import shlex
import shutil
import sqlite3
from pathlib import Path
import statistics
import sys
import time

from bench import CLEAR_SQL, HERE, Session, digest, machine, write_json
from diagnose_release import control
from function_cases import cases
from workloads import sql_literal


def disk_usage(directory):
    logical = allocated = files = 0
    for root, dirs, names in os.walk(directory):
        for name in names:
            try:
                s = (Path(root) / name).stat()
                logical += s.st_size
                allocated += s.st_blocks * 512
                files += 1
            except FileNotFoundError:
                pass
    return dict(logical_bytes=logical, allocated_bytes=allocated, files=files)


def storage_accounting(directory):
    """Read only fixture storage; file size alone cannot establish a state leak."""
    result = []
    for path in directory.rglob("*"):
        if path.suffix not in (".db", ".sqlite", ".sqlite3") or not path.is_file():
            continue
        item = {"path": str(path.relative_to(directory)), "tables": {}}
        try:
            with contextlib.closing(sqlite3.connect(path.as_uri()+"?mode=ro", uri=True, timeout=2)) as connection:
                for name, in connection.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                    quoted = '"' + name.replace('"','""') + '"'
                    item["tables"][name] = {"rows": connection.execute(f"SELECT count(*) FROM {quoted}").fetchone()[0]}
                    columns = {r[1] for r in connection.execute(f"PRAGMA table_info({quoted})")}
                    if "value" in columns:
                        item["tables"][name]["value_bytes"] = connection.execute(f"SELECT coalesce(sum(length(value)),0) FROM {quoted}").fetchone()[0]
                for pragma in ("page_count", "freelist_count", "page_size"):
                    item[pragma] = connection.execute(f"PRAGMA {pragma}").fetchone()[0]
        except sqlite3.Error as e:
            item["error"] = str(e)
        result.append(item)
    return result


def trial(args, directory, case, worker):
    directory.mkdir()
    # Replace only our named token; SQL struct literals also use braces.
    def expand(sql):
        return sql.replace("{directory}", str(directory))
    write_json(directory / "case.json", case)
    (directory / "query.sql").write_text(expand(case["sql"]) + ";\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("VGI_")}
    env.update({"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "ARROW_NUM_THREADS": "1",
                "QUERY_FARM_TELEMETRY_OPT_OUT": "1"})
    env.update({k: os.path.expandvars(v) for k, v in worker.get("env", {}).items()})
    env.update(case.get("env", {}))
    for key in ("XDG_CACHE_HOME", "XDG_STATE_HOME", "XDG_CONFIG_HOME", "TMPDIR"):
        path = directory / key.lower()
        path.mkdir()
        env[key] = str(path)
    env["VGI_TRANSACTOR_DB_DIR"] = env["TMPDIR"]
    result = dict(case=case, worker=args.worker, status="failed", diagnostic=True, events=[],
                  machine_before=machine())
    events = result["events"]
    session = None
    try:
        with (directory / "samples.jsonl").open("w") as samples:
            session = Session(args, directory, env, samples)
            result["host_pid"] = session.pid

            def checkpoint(phase):
                event = session.settle(phase)
                event["allocator"] = json.loads(control(session, "allocator"))
                if args.arrow_probe:
                    arrow = json.loads(control(session, "arrow_allocator", str(args.extension)))
                    if "error" in arrow:
                        raise RuntimeError(arrow["error"])
                    event["allocator"]["arrow"] = arrow
                event["temporary_storage"] = disk_usage(directory / "tmpdir")
                if args.inspect_storage:
                    event["storage_accounting"] = storage_accounting(directory / "tmpdir")
                events.append(event)

            events.append(session.execute("load", f"LOAD {sql_literal(args.extension)}"))
            events.append(session.execute("settings", f"SET threads={case.get('threads',1)}; SET memory_limit='2GB'"))
            for sql in case.get("pre_attach", []):
                events.append(session.execute("pre_attach", expand(sql)))
            if not case.get("native"):
                catalog = case.get("catalog", "example")
                location = case.get("location", worker["location"])
                if case.get("fixture_module"):
                    location = shlex.quote(str(Path(worker["location"]).parent / "python")) + " -m " + shlex.quote(case["fixture_module"])
                events.append(session.execute("attach", f"ATTACH {sql_literal(catalog)} AS bench (TYPE vgi, LOCATION {sql_literal(location)}, CACHE {'true' if case.get('cache') else 'false'})"))
            for sql in case.get("settings", []) + case.get("setup", []):
                events.append(session.execute("setup", expand(sql)))
            if not case.get("operation"):
                event = session.execute("plan", "EXPLAIN " + expand(case["sql"]), operation="plan")
                (directory / "plan.txt").write_text(event.pop("checksum"))
                events.append(event)
            checkpoint("before_queries")
            for n in range(args.iterations):
                for sql in case.get("before_each", []):
                    events.append(session.execute(f"prepare:{n}", expand(sql)))
                events.append(session.execute(f"query:{n}", expand(case["sql"]), case["expected"], operation=case.get("operation")))
                checkpoint(f"released:{n}")
                for verification in case.get("verify", []):
                    events.append(session.execute(f"verify:{n}", expand(verification["sql"]), verification["expected"]))
                if case.get("operation"):
                    # Verify SQL + a new VGI exchange after an error/interrupt.
                    events.append(session.execute(f"recovery:{n}", "SELECT sum(n) FROM bench.sequence(10)", "45"))
            events.append(session.accounting("accounting"))
            for sql in case.get("cleanup", []):
                events.append(session.execute("cleanup", expand(sql)))
            events.append(session.execute("clear_cache", CLEAR_SQL))
            checkpoint("cache_cleared")
            if not case.get("native"):
                events.append(session.execute("detach", "DETACH bench"))
            events.append(session.execute("flush_workers", "SELECT * FROM vgi_worker_pool_flush()"))
            checkpoint("workers_flushed")
            events.append(session.execute("close_database", close=True))
            checkpoint("database_closed")
            if args.require_zero_arrow_after_close and events[-1]["allocator"]["arrow"]["bytes_allocated"] != 0:
                raise AssertionError(f"live Arrow allocations after database close: {events[-1]['allocator']['arrow']['bytes_allocated']}")
            if args.arrow_probe:
                started = time.monotonic()
                result["arrow_release"] = json.loads(control(session, "release_arrow", str(args.extension)))
                result["arrow_release_wall_s"] = time.monotonic() - started
                checkpoint("after_arrow_release")
            if args.trim_glibc:
                result["malloc_trim_return"] = int(control(session, "trim_glibc"))
                checkpoint("after_glibc_trim")
            result["status"] = "ok"
    except Exception as e:
        result["error"] = str(e)
    finally:
        if session:
            session.stop(abort=result["status"] != "ok")
            if session.process.returncode:
                result["status"] = "failed"
                result.setdefault("error", f"host exited {session.process.returncode}")
        result["machine_after"] = machine()
        write_json(directory / "result.json", result)
    return result


def summarize(results, directory):
    rows = []
    for result in results:
        events = result["events"]
        queries = [e for e in events if e["phase"].startswith("query:")]
        releases = [e for e in events if e["phase"].startswith("released:")]
        stages = {e["phase"]: e for e in events}
        row = dict(case=result["case"]["name"], family=result["case"]["family"], trial=result["trial"], status=result["status"])
        if queries:
            for role in ("host", "worker", "tree"):
                for key in ("rss_bytes", "pss_bytes"):
                    values = [e["peaks"][role][key] for e in queries]
                    row[f"peak_{role}_{key}"] = max(values) if all(v is not None for v in values) else None
            row["wall_s"] = statistics.median(e["wall_s"] for e in queries)
            row["host_cpu_s"] = statistics.median(e["host_cpu_s"] for e in queries)
            row["worker_cpu_sampled_s"] = statistics.median(e["worker_cpu_sampled_s"] for e in queries)
            row["source_rows"] = sum(e["source_rows"] for e in queries)
            row["host_lifetime_peak_rss_bytes"] = max(e["host_lifetime_peak_rss_bytes"] for e in queries)
            row["worker_processes_after_query"] = max(e["after"]["worker"]["processes"] for e in queries)
        for phase in ("before_queries", "cache_cleared", "workers_flushed", "database_closed", "after_arrow_release", "after_glibc_trim"):
            if phase in stages:
                e = stages[phase]
                row[phase + "_host_rss_bytes"] = e["after"]["host"]["rss_bytes"]
                row[phase + "_worker_rss_bytes"] = e["after"]["worker"]["rss_bytes"]
                row[phase + "_arrow_live_bytes"] = e["allocator"].get("arrow", {}).get("bytes_allocated")
        if releases:
            row["released_first_host_rss_bytes"] = releases[0]["after"]["host"]["rss_bytes"]
            row["released_last_host_rss_bytes"] = releases[-1]["after"]["host"]["rss_bytes"]
            row["released_first_worker_rss_bytes"] = releases[0]["after"]["worker"]["rss_bytes"]
            row["released_last_worker_rss_bytes"] = releases[-1]["after"]["worker"]["rss_bytes"]
            row["arrow_high_water_bytes"] = releases[-1]["allocator"].get("arrow", {}).get("max_memory")
            row["released_arrow_live_bytes"] = releases[-1]["allocator"].get("arrow", {}).get("bytes_allocated")
            row["temporary_storage_bytes"] = releases[-1]["temporary_storage"]["allocated_bytes"]
        if "error" in result:
            row["error"] = result["error"]
        rows.append(row)
    write_json(directory / "summary.json", rows)
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with (directory / "summary.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    columns = ["case", "trial", "status", "peak_host_rss_bytes", "peak_worker_rss_bytes", "released_arrow_live_bytes", "database_closed_host_rss_bytes", "after_arrow_release_host_rss_bytes", "wall_s"]
    def cell(key, value):
        if value is None:
            return "—"
        return f"{value/2**20:.1f}" if key.endswith("bytes") else html.escape(str(value))
    body = "".join("<tr>" + "".join(f"<td>{cell(k,r.get(k))}</td>" for k in columns) + "</tr>" for r in rows)
    (directory / "report.html").write_text("<!doctype html><meta charset='utf-8'><title>VGI function memory</title><style>body{font:14px system-ui;margin:30px}table{border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:right}td:first-child{text-align:left}</style><h1>VGI function memory diagnostics</h1><p>Memory values are MiB. Peak RSS is sampled; live Arrow bytes and allocator retention are different measures. Timings are diagnostic, not regression baselines. Each row is a fresh process; see case.json, plan.txt, samples.jsonl and result.json for exact evidence.</p><table><thead><tr>" + "".join("<th>"+k+"</th>" for k in columns) + "</tr></thead><tbody>" + body + "</tbody></table>")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--driver", type=Path, required=True)
    p.add_argument("--extension", type=Path, required=True)
    p.add_argument("--workers", type=Path, default=HERE / "workers.example.json")
    p.add_argument("--worker", default="rust")
    p.add_argument("--suite", type=Path, help="optional JSON {cases:[...]} with arbitrary SQL scenarios")
    p.add_argument("--cases", help="comma-separated case names")
    p.add_argument("--families", help="comma-separated family names")
    p.add_argument("--iterations", type=int, default=3)
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--cpus", default="4,5,6,7")
    p.add_argument("--interval", type=float, default=.05)
    p.add_argument("--settle", type=float, default=.3)
    p.add_argument("--timeout", type=float, default=300)
    p.add_argument("--rss-limit-mib", type=int, default=8192)
    p.add_argument("--arrow-probe", action="store_true")
    p.add_argument("--inspect-storage", action="store_true", help="inspect fixture SQLite row counts at release checkpoints")
    p.add_argument("--trim-glibc", action="store_true", help="diagnostic malloc_trim after database close and optional Arrow release")
    p.add_argument("--require-zero-arrow-after-close", action="store_true", help="allocator regression assertion; requires --arrow-probe")
    p.add_argument("--quick", action="store_true")
    p.add_argument("--profile", choices=("none", "cpu", "alloc", "pages"), default="none")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    if sys.platform != "linux" or min(args.iterations,args.trials,args.interval,args.settle,args.timeout,args.rss_limit_mib) <= 0:
        p.error("requires Linux and positive run parameters")
    if args.require_zero_arrow_after_close and not args.arrow_probe:
        p.error("--require-zero-arrow-after-close requires --arrow-probe")
    os.sched_setaffinity(0, {int(c) for c in args.cpus.split(",")})
    args.driver, args.extension, args.out = args.driver.resolve(), args.extension.resolve(), args.out.resolve()
    worker = json.loads(args.workers.read_text())["workers"][args.worker]
    worker["location"] = os.path.expandvars(worker["location"])
    if "${" in worker["location"] or worker["location"].startswith(("launch:","http:","https:","unix:","docker:")):
        p.error("requires an owned local worker; expand VGI_WORKSPACE")
    selected = json.loads(args.suite.read_text())["cases"] if args.suite else cases(args.quick)
    excluded = [c["name"] for c in selected if c.get("worker_only",args.worker) != args.worker]
    selected = [c for c in selected if c["name"] not in excluded]
    for key, field in ((args.cases,"name"),(args.families,"family")):
        if key:
            requested = set(key.split(","))
            if requested - {c[field] for c in selected}:
                p.error(f"unknown {field}: {requested - {c[field] for c in selected}}")
            selected = [c for c in selected if c[field] in requested]
    if not selected:
        p.error("no cases selected")
    args.out.mkdir(parents=True, exist_ok=False)
    write_json(args.out / "suite.json", {"cases":selected})
    metadata = dict(diagnostic=True, status="running", machine=machine(), worker=worker, excluded_worker_cases=excluded,
                    arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
                    binaries={str(path):digest(path) for path in (args.driver,args.extension)},
                    source_sha256={name:digest(HERE/name) for name in ("function_memory.py","function_cases.py","bench.py","proc.py","driver.cpp")})
    worker_executable = Path(worker["location"])
    if worker_executable.is_file():
        metadata["worker_executable_sha256"] = digest(worker_executable)
    write_json(args.out / "metadata.json", metadata)
    source_archive = args.out / "harness-source"
    source_archive.mkdir()
    for name in ("function_memory.py", "function_cases.py", "bench.py", "proc.py", "driver.cpp", "diagnose_release.py", "workloads.py", "arrow_probe.cpp"):
        shutil.copy2(HERE/name, source_archive/name)
    results = []
    for case in selected:
        for i in range(args.trials):
            print(f"{case['name']} trial={i}", flush=True)
            result = trial(args,args.out/f"{case['name']}-{i}",case,worker)
            result["trial"] = i
            results.append(result)
            print(result["status"],result.get("error",""),flush=True)
            write_json(args.out / "results.json", results)
            summarize(results,args.out)
            if result["status"] != "ok":
                break  # preserve first failure; don't waste identical repeat trials
    metadata["status"] = "ok" if all(r["status"] == "ok" for r in results) else "failed"
    write_json(args.out / "metadata.json", metadata)
    return 0 if metadata["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
