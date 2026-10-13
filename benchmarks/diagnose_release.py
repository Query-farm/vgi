#!/usr/bin/env python3
"""Allocator observations in a live benchmark host; diagnostic, not a timing gate.

Optional glibc trimming happens only after database close. This tests whether
resident pages are reclaimable; it is not a production allocation policy.
"""
import argparse
import json
import os
from pathlib import Path
import sys

from bench import CLEAR_SQL, HERE, Session, digest, machine, write_json
from workloads import sql_literal, workload


def control(session, operation, argument=""):
    session.phase, session.samples = operation, []
    session.sample()
    if "\n" in argument or "\t" in argument:
        raise ValueError("control argument must be a single line")
    session.process.stdin.write(f"{operation}\t\t{argument}\n".encode())
    return session.receive()["checksum"]


def diagnose(args, directory, case, mode, worker):
    directory.mkdir()
    sql, expected, logical_bytes = workload(case, mode)
    (directory / "query.sql").write_text(sql + ";\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("VGI_")}
    env.update({"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "ARROW_NUM_THREADS": "1",
                "QUERY_FARM_TELEMETRY_OPT_OUT": "1", "VGI_RPC_CLIENT_TIMING": "1"})
    env.update({k: os.path.expandvars(v) for k, v in worker.get("env", {}).items()})
    if args.arrow_pool:
        env["ARROW_DEFAULT_MEMORY_POOL"] = args.arrow_pool
    for key in ("XDG_CACHE_HOME", "XDG_STATE_HOME", "XDG_CONFIG_HOME", "TMPDIR"):
        path = directory / key.lower()
        path.mkdir()
        env[key] = str(path)
    env["VGI_TRANSACTOR_DB_DIR"] = env["TMPDIR"]
    result = {"case": case, "mode": mode, "worker": args.worker if mode != "native" else "native",
              "logical_bytes": logical_bytes, "status": "failed", "events": [], "diagnostic": True}
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
                    event["allocator"]["arrow"] = json.loads(control(session, "arrow_allocator", str(args.extension)))
                    if "error" in event["allocator"]["arrow"]:
                        raise RuntimeError(event["allocator"]["arrow"]["error"])
                event["after_allocator_observation"] = session.samples[-1]
                events.append(event)

            events.append(session.execute("load", f"LOAD {sql_literal(args.extension)}"))
            events.append(session.execute("settings", "SET threads=1; SET memory_limit='4GB'"))
            if mode != "native":
                cache = "true" if mode == "cached_echo" else "false"
                events.append(session.execute("attach", f"ATTACH 'example' AS bench (TYPE vgi, LOCATION {sql_literal(worker['location'])}, CACHE {cache})"))
            checkpoint("before_queries")
            for n in range(args.iterations):
                events.append(session.execute(f"query:{n}", sql, expected))
                checkpoint(f"released:{n}")
            events.append(session.execute("clear_cache", CLEAR_SQL))
            checkpoint("cache_cleared")
            if mode != "native":
                events.append(session.execute("detach", "DETACH bench"))
            events.append(session.execute("flush_workers", "SELECT * FROM vgi_worker_pool_flush()"))
            checkpoint("workers_flushed")
            events.append(session.execute("close_database", close=True))
            checkpoint("database_closed")
            if args.trim_glibc:
                result["malloc_trim_return"] = int(control(session, "trim_glibc"))
                checkpoint("after_glibc_trim")
            if args.arrow_probe:
                result["arrow_release"] = json.loads(control(session, "release_arrow", str(args.extension)))
                if "error" in result["arrow_release"]:
                    raise RuntimeError(result["arrow_release"]["error"])
                checkpoint("after_arrow_release")
            result["status"] = "ok"
    except Exception as e:
        result["error"] = str(e)
    finally:
        if session:
            session.stop(abort=result["status"] != "ok")
            if session.process.returncode:
                result["status"] = "failed"
                result.setdefault("error", f"host exited {session.process.returncode}")
        write_json(directory / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path, default=HERE.parent / "build/benchmark/extension/vgi/vgi_bench_driver")
    parser.add_argument("--extension", type=Path, default=HERE.parent / "build/benchmark/extension/vgi/vgi.duckdb_extension")
    parser.add_argument("--suite", type=Path, default=HERE / "suites/string_batches.json")
    parser.add_argument("--workers", type=Path, default=HERE / "workers.example.json")
    parser.add_argument("--worker", default="rust")
    parser.add_argument("--cases", default="strings64k_sql,strings64k_b2048,strings64k_b64,wide_strings_sql,wide_strings_b2048,wide_strings_b128")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--cpus", default="4,5,6,7")
    parser.add_argument("--trim-glibc", action="store_true")
    parser.add_argument("--arrow-probe", action="store_true", help="requires VGI_BENCH_ARROW_PROBE=ON; releases unused Arrow pages after close")
    parser.add_argument("--arrow-pool", choices=("system", "mimalloc", "jemalloc"), help="diagnostic allocator override in the owned host/worker environment")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux" or args.iterations < 1:
        parser.error("requires Linux and positive iterations")
    os.sched_setaffinity(0, {int(c) for c in args.cpus.split(",")})
    args.driver, args.extension, args.out = args.driver.resolve(), args.extension.resolve(), args.out.resolve()
    args.profile, args.interval, args.settle, args.timeout, args.rss_limit_mib = "none", .05, .5, 300, 8192
    worker = json.loads(args.workers.read_text())["workers"][args.worker]
    worker["location"] = os.path.expandvars(worker["location"])
    if "${" in worker["location"] or worker["location"].startswith(("launch:", "http:", "https:", "unix:", "docker:")):
        parser.error("requires an owned local worker; expand VGI_WORKSPACE")
    cases = json.loads(args.suite.read_text())["cases"]
    requested = set(args.cases.split(","))
    if requested - {c["name"] for c in cases}:
        parser.error("unknown case")
    cases = [c for c in cases if c["name"] in requested]
    args.out.mkdir(parents=True, exist_ok=False)
    metadata = {"diagnostic": True, "machine": machine(), "worker": worker, "status": "running",
                "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "binaries": {str(p): digest(p) for p in (args.driver, args.extension)},
                "source_sha256": {name: digest(HERE / name) for name in
                                  ("diagnose_release.py", "driver.cpp", "arrow_probe.cpp", "bench.py", "proc.py", "workloads.py")}}
    write_json(args.out / "metadata.json", metadata)
    results = []
    for case in cases:
        for mode in case["modes"]:
            print(case["name"], mode, flush=True)
            result = diagnose(args, args.out / f"{case['name']}-{mode}", case, mode, worker)
            results.append(result)
            write_json(args.out / "results.json", results)
    metadata["status"] = "ok" if all(r["status"] == "ok" for r in results) else "failed"
    write_json(args.out / "metadata.json", metadata)
    return 0 if metadata["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
