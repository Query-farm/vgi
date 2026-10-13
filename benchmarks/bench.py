#!/usr/bin/env python3
"""Linux VGI resource benchmark. Standard library only; see README.md."""
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import os
import platform
import random
import re
import selectors
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from proc import ProcessTree, totals, worker_cpu
from workloads import workload, sql_literal

HERE = Path(__file__).resolve().parent
VERSION = 1
CLEAR_SQL = "SELECT * FROM vgi_result_cache_flush(); SELECT * FROM vgi_clear_cache()"


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n")


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def git_revision(path):
    def git(*args):
        r = subprocess.run(["git", "-C", str(path), *args], capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else None
    return {"path": str(path), "commit": git("rev-parse", "HEAD"), "status": git("status", "--porcelain")}


def machine():
    cpu = Path("/proc/cpuinfo").read_text()
    model = next((line.split(":", 1)[1].strip() for line in cpu.splitlines()
                  if line.startswith("model name")), None)
    if model is None:
        model = "; ".join(sorted(set(line.strip() for line in cpu.splitlines()
                                    if line.startswith(("CPU implementer", "CPU architecture", "CPU part")))))
    return {"hostname": platform.node(), "machine": platform.machine(), "kernel": platform.release(),
            "cpu_model": model, "cpu_count": os.cpu_count(), "affinity": sorted(os.sched_getaffinity(0)),
            "memory_kib": int(Path("/proc/meminfo").read_text().splitlines()[0].split()[1]),
            "load_average": os.getloadavg(), "python": platform.python_version()}


class Session:
    def __init__(self, args, directory, env, sample_file):
        self.args, self.directory, self.sample_file = args, directory, sample_file
        self.tree, self.phase, self.samples = None, "startup", []
        self.started = time.monotonic()
        self.selector = selectors.DefaultSelector()
        command = [str(args.driver)]
        if args.profile == "cpu":
            command = ["perf", "record", "-q", "-F", "99", "-e", "cpu-clock:u", "--call-graph", "fp",
                       "-o", str(directory / "perf.data"), "--", *command]
        elif args.profile == "alloc":
            # With jemalloc this captures malloc-family allocations only. The
            # separate --profile pages mode includes mmap-backed allocations.
            command = ["valgrind", "--tool=massif", "--trace-children=no", "--time-unit=ms",
                       "--massif-out-file=" + str(directory / "massif.%p.out"), *command]
        elif args.profile == "pages":
            command = ["valgrind", "--tool=massif", "--trace-children=no", "--pages-as-heap=yes",
                       "--time-unit=ms", "--massif-out-file=" + str(directory / "massif.%p.out"), *command]
        self.log = (directory / "stderr.log").open("w")
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.log, env=env, start_new_session=True, bufsize=0)
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b""
        try:
            event = self.receive()
            if event.get("event") != "ready":
                raise RuntimeError(f"driver startup failed: {event}")
            self.pid, self.engine_version = event["pid"], event["engine_version"]
            self.tree = ProcessTree(self.pid)
            write_json(directory / "command.json", {"argv": command, "host_pid": self.pid})
        except BaseException:
            self.stop()
            raise

    def sample(self):
        if self.tree is None:
            return
        processes = self.tree.snapshot()
        item = {"elapsed_s": time.monotonic() - self.started, "phase": self.phase,
                "processes": processes, **{role: totals(processes, role) for role in ("host", "worker", "tree")}}
        self.sample_file.write(json.dumps(item) + "\n")
        self.samples.append(item)
        if item["tree"]["rss_bytes"] > self.args.rss_limit_mib * 2**20:
            raise MemoryError(f"observed process-tree RSS exceeded {self.args.rss_limit_mib} MiB safety limit")

    def receive(self):
        deadline = time.monotonic() + self.args.timeout
        while True:
            if b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                event = json.loads(line)
                if event.get("event") == "error":
                    raise RuntimeError(event["message"])
                return event
            if time.monotonic() >= deadline:
                raise TimeoutError(f"phase {self.phase} exceeded {self.args.timeout}s")
            ready = self.selector.select(min(self.args.interval, max(0, deadline - time.monotonic())))
            self.sample()
            if ready:
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError(f"driver exited during {self.phase}; see {self.directory / 'stderr.log'}")
                self.buffer += chunk

    def execute(self, phase, sql="", expected=None, close=False, inspect=False, operation=None):
        if "\n" in sql or "\t" in sql:
            raise ValueError("SQL must be a single line without tabs")
        self.phase, self.samples = phase, []
        self.sample()
        operation = operation or ("close" if close else "inspect" if inspect else "exec" if expected is None else "query")
        self.process.stdin.write(f"{operation}\t{expected or ''}\t{sql}\n".encode())
        event = self.receive()
        if event.get("event") != "result":
            raise RuntimeError(f"unexpected driver event: {event}")
        self.sample()
        event.update({"phase": phase, "worker_cpu_sampled_s": worker_cpu(self.samples),
                      "observations": len(self.samples), "before": self.samples[0], "after": self.samples[-1]})
        event["host_cpu_s"] = event["host_user_s"] + event["host_system_s"]
        event["peak_worker_processes"] = max(s["worker"]["processes"] for s in self.samples)
        event["peaks"] = {}
        for role in ("host", "worker", "tree"):
            event["peaks"][role] = {}
            for key in ("rss_bytes", "pss_bytes", "private_bytes", "anonymous_bytes"):
                values = [s[role][key] for s in self.samples]
                event["peaks"][role][key] = max(values) if all(v is not None for v in values) else None
        return event

    def settle(self, phase):
        self.phase, self.samples = phase, []
        self.sample()
        end = time.monotonic() + self.args.settle
        while time.monotonic() < end:
            time.sleep(min(self.args.interval, end - time.monotonic()))
            self.sample()
        return {"phase": phase, "after": self.samples[-1]}

    def accounting(self, phase):
        # Outside the measured query. Exposes live DuckDB buffer accounting and
        # VGI retained cache accounting, neither of which equals process RSS.
        sql = ("SELECT json_object('duckdb_memory_bytes', (SELECT coalesce(sum(memory_usage_bytes),0) FROM duckdb_memory()), "
               "'cache', (SELECT to_json(s) FROM vgi_result_cache_stats() s))::VARCHAR")
        event = self.execute(phase, sql, inspect=True)
        event["accounting"] = json.loads(event.pop("checksum"))
        return event

    def stop(self, abort=False):
        # Only our new process group is signalled; never kill an existing worker.
        try:
            if abort:
                try:
                    os.killpg(self.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            if self.process.stdin:
                self.process.stdin.close()
            try:
                self.process.wait(timeout=2 if abort else 10)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait()
        finally:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait()
            self.selector.close()
            self.process.stdout.close()
            self.log.close()


def profile_reports(args, directory, pid):
    if args.profile == "cpu":
        for name, extra in (("cpu-tree", []), ("cpu-host", ["--pid", str(pid)])):
            with (directory / (name + ".txt")).open("w") as f:
                subprocess.run(["perf", "report", "--stdio", "--percent-limit", "0.5", "--sort", "dso,symbol",
                                "-i", str(directory / "perf.data"), *extra], stdout=f, stderr=subprocess.STDOUT, check=True)
    elif args.profile in {"alloc", "pages"}:
        path = directory / f"massif.{pid}.out"
        with (directory / "allocations.txt").open("w") as f:
            subprocess.run(["ms_print", str(path)], stdout=f, stderr=subprocess.STDOUT, check=True)


def run_trial(args, directory, case, mode, worker, trial):
    directory.mkdir()
    sql, expected, logical_bytes = workload(case, mode)
    (directory / "query.sql").write_text(sql + ";\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("VGI_")}
    env.update({"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "ARROW_NUM_THREADS": str(args.threads),
                "QUERY_FARM_TELEMETRY_OPT_OUT": "1"})
    env.update(worker.get("env", {}))
    if args.transport == "shm":
        env["VGI_RPC_SHM_SIZE_BYTES"] = str(args.shm_bytes)
    for key in ("XDG_CACHE_HOME", "XDG_STATE_HOME", "XDG_CONFIG_HOME", "TMPDIR"):
        path = directory / key.lower()
        path.mkdir()
        env[key] = str(path)
    env["VGI_TRANSACTOR_DB_DIR"] = env["TMPDIR"]
    events = []
    session = None
    result = {"schema_version": VERSION, "case": case, "mode": mode, "worker": worker["name"],
              "transport": args.transport if mode != "native" else "native", "trial": trial,
              "logical_bytes": logical_bytes, "expected_checksum": expected, "events": events, "status": "failed"}
    try:
        with (directory / "samples.jsonl").open("w") as samples:
            session = Session(args, directory, env, samples)
            result["host_pid"], result["engine_version"] = session.pid, session.engine_version
            events.append(session.execute("load", f"LOAD {sql_literal(args.extension)}"))
            events.append(session.execute("settings", f"SET threads={args.threads}; SET memory_limit={sql_literal(args.memory_limit)}"))
            if mode != "native":
                cache = "true" if mode == "cached_echo" else "false"
                events.append(session.execute("attach", f"ATTACH 'example' AS bench (TYPE vgi, LOCATION {sql_literal(worker['location'])}, CACHE {cache})"))
            # The first call stays visible as a cold query; warm-up is the same
            # workload, in this same connection/process, excluded from medians.
            events.append(session.execute("cold", sql, expected))
            for n in range(args.warmup):
                events.append(session.execute(f"warmup:{n}", sql, expected))
            events.append(session.settle("warm_baseline"))
            warm_accounting = session.accounting("warm_accounting")
            events.append(warm_accounting)
            if mode == "cached_echo" and warm_accounting["accounting"]["cache"]["exchange_stores"] == 0:
                raise RuntimeError("cache workload did not store an exchange result")
            if case.get("release_each") and mode != "native":
                events.append(session.execute("initial_clear", CLEAR_SQL))
            for n in range(args.iterations):
                events.append(session.execute(f"measure:{n}", sql, expected))
                events.append(session.settle(f"result_released:{n}"))
                events.append(session.accounting(f"result_accounting:{n}"))
                if case.get("release_each") and mode != "native":
                    if mode == "cached_echo":
                        # Observe replay with cache populated, then clear before
                        # the next cycle's fill. Do not mix replay into medians.
                        events.append(session.execute(f"cache_replay:{n}", sql, expected))
                    events.append(session.execute(f"cycle_clear:{n}", CLEAR_SQL))
                    events.append(session.settle(f"cycle_released:{n}"))
                    events.append(session.accounting(f"cycle_accounting:{n}"))
            events.append(session.execute("clear_cache", CLEAR_SQL))
            events.append(session.settle("cache_cleared"))
            cleared = session.accounting("cleared_accounting")
            events.append(cleared)
            if cleared["accounting"]["cache"]["total_bytes"] != 0:
                raise RuntimeError("result-cache bytes remained after an explicit flush")
            if mode != "native":
                events.append(session.execute("detach", "DETACH bench"))
                events.append(session.settle("catalog_detached"))
            events.append(session.execute("flush_workers", "SELECT * FROM vgi_worker_pool_flush()"))
            events.append(session.settle("workers_flushed"))
            events.append(session.execute("close_database", close=True))
            events.append(session.settle("database_closed"))
            result["status"] = "ok"
    except Exception as e:
        result["error"] = str(e)
    finally:
        if session:
            session.stop(abort=result["status"] != "ok")
            result["exit_code"] = session.process.returncode
            if session.process.returncode:
                result["status"] = "failed"
                result.setdefault("error", f"driver/profiler exit {session.process.returncode}")
        write_json(directory / "result.json", result)
    if result["status"] == "ok" and args.profile != "none":
        try:
            profile_reports(args, directory, session.pid)
        except Exception as e:
            result.update(status="failed", error=f"profile report failed: {e}")
            write_json(directory / "result.json", result)
    return result


def summarize(results):
    groups = {}
    for result in results:
        key = (result["case"]["name"], result["mode"], result["worker"], result["transport"])
        groups.setdefault(key, []).append(result)
    rows = []
    for key, trials in sorted(groups.items()):
        row = dict(zip(("case", "mode", "worker", "transport"), key))
        row.update(trials=len(trials), status="ok" if all(t["status"] == "ok" for t in trials) else "failed")
        if row["status"] == "ok":
            row["cold_wall_s"] = statistics.median(next(e for e in t["events"] if e["phase"] == "cold")["wall_s"] for t in trials)
            row["host_lifetime_peak_rss_bytes"] = statistics.median(max(e.get("host_lifetime_peak_rss_bytes", 0) for e in t["events"]) for t in trials)
            row["cache_peak_accounted_bytes"] = statistics.median(max(e["accounting"]["cache"]["total_bytes"] for e in t["events"] if "accounting" in e) for t in trials)
            measured = [[e for e in t["events"] if e["phase"].startswith("measure:")] for t in trials]
            # Independent trials get equal weight; iterations within one process
            # are correlated. Expose dispersion, do not call this a CI interval.
            for metric in ("wall_s", "host_cpu_s", "worker_cpu_sampled_s"):
                medians = [statistics.median(e[metric] for e in events) for events in measured]
                row[metric] = statistics.median(medians)
                row[metric + "_min"] = min(medians)
                row[metric + "_max"] = max(medians)
            for role in ("host", "worker", "tree"):
                for metric in ("rss_bytes", "pss_bytes"):
                    values = [max(e["peaks"][role][metric] for e in events)
                              if all(e["peaks"][role][metric] is not None for e in events) else None
                              for events in measured]
                    row[f"{role}_peak_{metric}"] = statistics.median(values) if all(v is not None for v in values) else None
                for phase in ("warm_baseline", "cache_cleared", "workers_flushed", "database_closed"):
                    values = [next(e for e in t["events"] if e["phase"] == phase)["after"][role]["rss_bytes"] for t in trials]
                    row[f"{role}_{phase}_rss_bytes"] = statistics.median(values)
                deltas = []
                for t in trials:
                    releases = [e for e in t["events"] if e["phase"].startswith("cycle_released:" if t["case"].get("release_each") else "result_released:")]
                    deltas.append(releases[-1]["after"][role]["rss_bytes"] - releases[0]["after"][role]["rss_bytes"])
                row[f"{role}_retained_delta_bytes"] = statistics.median(deltas)
            row["rows_per_s"] = trials[0]["case"]["rows"] / row["wall_s"]
        rows.append(row)
    return rows


def report(directory, rows, results):
    write_json(directory / "summary.json", rows)
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with (directory / "summary.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    blocks = ["<!doctype html><meta charset='utf-8'><title>VGI memory benchmark</title>",
              "<style>body{font:15px system-ui;max-width:1100px;margin:40px auto;padding:0 20px}table{border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:right}th:first-child,td:first-child{text-align:left}svg{width:100%;height:150px}code{font-size:12px}a{color:#1466a0}</style>",
              "<h1>VGI CPU and memory</h1><p>Host = DuckDB + extension; worker = observed descendants. RSS includes shared pages; PSS is in the CSV. Peaks are sampled; host lifetime high-water is in raw results. Retention is not a leak verdict. Profiling runs are not timing baselines.</p>",
              "<table><tr><th>Case / worker</th><th>Seconds</th><th>Host CPU s</th><th>Host peak MiB</th><th>Worker peak MiB</th><th>Host after DB close MiB</th></tr>"]
    for row in rows:
        label = html.escape(f"{row['case']} / {row['mode']} / {row['worker']} / {row['transport']}")
        values = [row.get("wall_s"), row.get("host_cpu_s"), *[row.get(k, 0) / 2**20 if row.get(k) is not None else None for k in
                  ("host_peak_rss_bytes", "worker_peak_rss_bytes", "host_database_closed_rss_bytes")]]
        blocks.append("<tr><td>" + label + "</td>" + "".join(f"<td>{v:.3f}</td>" if v is not None else "<td>failed</td>" for v in values) + "</tr>")
    blocks.append("</table><h2>Memory after each phase</h2><p>Blue: host RSS. Orange: worker RSS. Hover on a point for the phase and MiB. Raw samples include the peaks during each query.</p>")
    for result in results:
        blocks.append("<h3>" + html.escape(f"{result['case']['name']} / {result['mode']} / {result['worker']} / trial {result['trial']}") + "</h3>")
        events = [e for e in result["events"] if "after" in e]
        if not events:
            continue
        maximum = max(1, *(e["after"][r]["rss_bytes"] for e in events for r in ("host", "worker")))
        blocks.append('<svg viewBox="0 0 1000 150" role="img" aria-label="Memory after each phase">')
        for role, color in (("host", "#1466a0"), ("worker", "#df7613")):
            points = [(10 + i * 980 / max(1, len(events) - 1), 140 - e["after"][role]["rss_bytes"] * 125 / maximum) for i, e in enumerate(events)]
            blocks.append(f'<polyline fill="none" stroke="{color}" points="' + " ".join(f"{x:.1f},{y:.1f}" for x, y in points) + '"/>')
            for (x, y), e in zip(points, events):
                label = html.escape(f"{role}: {e['phase']}: {e['after'][role]['rss_bytes'] / 2**20:.2f} MiB")
                blocks.append(f'<circle cx="{x}" cy="{y}" r="3" fill="{color}"><title>{label}</title></circle>')
        blocks.append("</svg>")
    (directory / "report.html").write_text("\n".join(blocks))


def run(args):
    if sys.platform != "linux":
        raise ValueError("resource measurement requires Linux; run on the EC2 workspace")
    if args.cpus:
        os.sched_setaffinity(0, {int(x) for x in args.cpus.split(",")})
    args.driver, args.extension = args.driver.resolve(), args.extension.resolve()
    for path in (args.driver, args.extension):
        if not path.is_file():
            raise ValueError(f"missing build artifact: {path}")
    for key in ("trials", "iterations", "threads", "interval", "timeout", "shm_bytes", "rss_limit_mib"):
        if not math.isfinite(getattr(args, key)) or getattr(args, key) <= 0:
            raise ValueError(f"{key} must be positive")
    if args.warmup < 0 or not math.isfinite(args.settle) or args.settle < 0:
        raise ValueError("warmup and settle must be nonnegative")
    if args.profile != "none":
        for tool in (["perf"] if args.profile == "cpu" else ["valgrind", "ms_print"]):
            if not shutil.which(tool):
                raise ValueError(f"{tool} is required for {args.profile} profiles")
    suite = json.loads(args.suite.read_text())
    if suite["schema_version"] != VERSION:
        raise ValueError("unsupported suite schema")
    cases = suite["cases"]
    names = [c["name"] for c in cases]
    if len(set(names)) != len(names) or any(not re.fullmatch(r"[a-zA-Z0-9_-]+", n) for n in names):
        raise ValueError("case names must be unique and contain only letters, digits, _ and -")
    if args.cases:
        requested = set(args.cases.split(","))
        if requested - set(names):
            raise ValueError(f"unknown cases: {requested - set(names)}")
        cases = [c for c in cases if c["name"] in requested]
    if args.quick:
        cases = [{**c, "rows": min(c["rows"], 2048)} for c in cases]
    worker_config = json.loads(args.workers.read_text())
    if worker_config["schema_version"] != VERSION:
        raise ValueError("unsupported worker schema")
    workers = []
    for name in args.worker.split(","):
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError("invalid worker name")
        worker = {**worker_config["workers"][name], "name": name}
        for key in ("location", "repo"):
            if key in worker:
                worker[key] = os.path.expandvars(worker[key])
        if "env" in worker:
            worker["env"] = {k: os.path.expandvars(v) for k, v in worker["env"].items()}
        location = worker["location"]
        if "${" in location or location.startswith(("launch:", "http:", "https:", "unix:", "docker:")):
            raise ValueError("v1 requires a local subprocess worker for trustworthy ownership/accounting; expand VGI_WORKSPACE")
        argv = shlex.split(location)
        if not argv or not shutil.which(argv[0]):
            raise ValueError(f"worker executable missing: {location}")
        workers.append(worker)
    jobs = []
    for trial in range(args.trials):
        batch = []
        for case in cases:
            for mode in case["modes"]:
                workload(case, mode)  # fail before starting any processes
                if args.modes and mode not in args.modes.split(","):
                    continue
                for worker in ([{"name": "native", "location": ""}] if mode == "native" else workers):
                    batch.append((case, mode, worker, trial))
        random.Random(args.seed + trial).shuffle(batch)
        jobs.extend(batch)
    if not jobs:
        raise ValueError("no matching workloads")
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    compatibility = {"schema_version": VERSION, "cases": cases, "modes": args.modes, "threads": args.threads,
                     "iterations": args.iterations, "warmup": args.warmup, "trials": args.trials,
                     "interval": args.interval, "settle": args.settle, "memory_limit": args.memory_limit,
                     "rss_limit_mib": args.rss_limit_mib, "seed": args.seed,
                     "transport": args.transport, "shm_bytes": args.shm_bytes, "profile": args.profile,
                     "workers": workers, "machine": {k: v for k, v in machine().items() if k not in ("load_average", "python")}}
    compatibility["harness_sha256"] = {name: digest(HERE / name) for name in ("bench.py", "proc.py", "workloads.py", "driver.cpp")}
    metadata = {"schema_version": VERSION, "started_at": datetime.now(timezone.utc).isoformat(),
                "compatibility": compatibility, "machine": machine(), "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "binaries": {str(p): digest(p) for p in (args.driver, args.extension)},
                "sources": [git_revision(HERE.parent), git_revision(HERE.parent / "duckdb"),
                            *[git_revision(w["repo"]) for w in workers if "repo" in w]], "status": "running"}
    if args.provenance:
        metadata["source_snapshot"] = json.loads(args.provenance.read_text())
    metadata["worker_fingerprints"] = {}
    for worker in workers:
        repo = Path(worker.get("repo", "."))
        metadata["worker_fingerprints"][worker["name"]] = {
            "launcher_sha256": digest(shutil.which(shlex.split(worker["location"])[0])),
            "source": metadata.get("source_snapshot", {}).get(repo.name, git_revision(repo)),
            "locks": {p.name: digest(p) for p in (repo / n for n in ("uv.lock", "Cargo.lock", "go.sum", "bun.lock", "package-lock.json", "packages.lock.json", "gradle.lockfile")) if p.is_file()}}
    cache = args.driver.parents[2] / "CMakeCache.txt"
    if cache.is_file():
        shutil.copyfile(cache, args.out / "CMakeCache.txt")
        metadata["build_settings"] = [line for line in cache.read_text().splitlines() if line.startswith(
            ("CMAKE_BUILD_TYPE:", "CMAKE_CXX_FLAGS:", "CMAKE_C_FLAGS:", "CMAKE_CXX_COMPILER:", "ENABLE_JEMALLOC:", "VGI_ENABLE_IROH:", "VGI_BENCH_ARROW_PROBE:", "STANDARD_VECTOR_SIZE:"))]
    write_json(args.out / "metadata.json", metadata)
    results = []
    try:
        for index, (case, mode, worker, trial) in enumerate(jobs):
            label = f"{index:03d}-{case['name']}-{mode}-{worker['name']}-t{trial}"
            print(f"[{index + 1}/{len(jobs)}] {label}", flush=True)
            result = run_trial(args, args.out / label, case, mode, worker, trial)
            results.append(result)
            with (args.out / "results.jsonl").open("a") as f:
                f.write(json.dumps(result) + "\n")
            if result["status"] != "ok":
                print("FAILED: " + result.get("error", "unknown error"), file=sys.stderr, flush=True)
        metadata["status"] = "ok" if all(r["status"] == "ok" for r in results) else "failed"
    finally:
        if metadata["status"] == "running":
            metadata["status"] = "interrupted"
        metadata["finished_at"] = datetime.now(timezone.utc).isoformat()
        metadata["machine_after"] = machine()
        write_json(args.out / "metadata.json", metadata)
        report(args.out, summarize(results), results)
    print(f"Results: {args.out / 'report.html'}")
    return 0 if metadata["status"] == "ok" else 1


def compare(args):
    old_meta, new_meta = [json.loads((p / "metadata.json").read_text()) for p in (args.baseline, args.candidate)]
    if old_meta["status"] != "ok" or new_meta["status"] != "ok":
        raise ValueError("cannot compare failed or incomplete runs")
    if old_meta["compatibility"] != new_meta["compatibility"]:
        raise ValueError("incompatible runs: machine, workers, workload, sampling, transport or settings differ")
    if old_meta["compatibility"]["profile"] != "none":
        raise ValueError("profiled timings are diagnostic, not regression baselines")
    if old_meta.get("build_settings") != new_meta.get("build_settings"):
        raise ValueError("build settings differ (optimization, allocator or toolchain)")
    if getattr(args, "component", "extension") == "extension":
        if old_meta.get("worker_fingerprints") != new_meta.get("worker_fingerprints"):
            raise ValueError("worker inputs changed; hold them fixed for an extension comparison, or use --component framework")
    elif old_meta.get("binaries") != new_meta.get("binaries"):
        raise ValueError("extension/host binaries changed; hold them fixed for a framework comparison")
    if old_meta["compatibility"]["trials"] < 3 and args.fail:
        raise ValueError("regression gates require at least three independent trials")
    def indexed(path):
        return {(r["case"], r["mode"], r["worker"], r["transport"]): r for r in json.loads((path / "summary.json").read_text())}
    old, new = indexed(args.baseline), indexed(args.candidate)
    if old.keys() != new.keys():
        raise ValueError("missing or changed benchmark coverage")
    regressed = False
    for key in sorted(old):
        for metric in args.metrics.split(","):
            a, b = old[key].get(metric), new[key].get(metric)
            if a is None or b is None:
                raise ValueError(f"unavailable metric: {metric} for {key}")
            delta = b - a
            percent = 100 * delta / a if a else None
            floor = args.memory_floor_mib * 2**20 if metric.endswith("bytes") else args.time_floor_ms / 1000
            regression = delta > floor and (a == 0 or percent > args.threshold)
            regressed |= regression
            ratio = f"{percent:+.1f}%" if percent is not None else "baseline zero"
            print(f"{'REGRESSION' if regression else 'ok':10s} {'/'.join(key):55s} {metric:28s} {a:.4f} -> {b:.4f} ({ratio})")
    return 1 if regressed and args.fail else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--driver", type=Path, default=HERE.parent / "build/benchmark/extension/vgi/vgi_bench_driver")
    run_parser.add_argument("--extension", type=Path, default=HERE.parent / "build/benchmark/extension/vgi/vgi.duckdb_extension")
    run_parser.add_argument("--suite", type=Path, default=HERE / "suites/memory.json")
    run_parser.add_argument("--workers", type=Path, default=HERE / "workers.example.json")
    run_parser.add_argument("--worker", default="python")
    run_parser.add_argument("--out", type=Path, required=True)
    run_parser.add_argument("--provenance", type=Path)
    run_parser.add_argument("--cases")
    run_parser.add_argument("--modes")
    run_parser.add_argument("--quick", action="store_true", help="smoke only: cap rows at 2048")
    run_parser.add_argument("--trials", type=int, default=3)
    run_parser.add_argument("--iterations", type=int, default=5)
    run_parser.add_argument("--warmup", type=int, default=2)
    run_parser.add_argument("--threads", type=int, default=1)
    run_parser.add_argument("--interval", type=float, default=0.05)
    run_parser.add_argument("--settle", type=float, default=0.2)
    run_parser.add_argument("--timeout", type=float, default=300)
    run_parser.add_argument("--memory-limit", default="4GB")
    run_parser.add_argument("--rss-limit-mib", type=float, default=8192, help="abort if sampled host+worker RSS exceeds this (not an OS hard limit)")
    run_parser.add_argument("--transport", choices=["stdio", "shm"], default="stdio")
    run_parser.add_argument("--shm-bytes", type=int, default=64 * 2**20)
    run_parser.add_argument("--cpus", help="comma-separated CPU IDs, inherited by host and workers")
    run_parser.add_argument("--seed", type=int, default=1729)
    run_parser.add_argument("--profile", choices=["none", "cpu", "alloc", "pages"], default="none")
    compare_parser = sub.add_parser("compare")
    compare_parser.add_argument("baseline", type=Path)
    compare_parser.add_argument("candidate", type=Path)
    compare_parser.add_argument("--metrics", default="wall_s,host_cpu_s,host_peak_rss_bytes,worker_peak_rss_bytes,host_database_closed_rss_bytes")
    compare_parser.add_argument("--threshold", type=float, default=10)
    compare_parser.add_argument("--memory-floor-mib", type=float, default=4)
    compare_parser.add_argument("--time-floor-ms", type=float, default=1)
    compare_parser.add_argument("--fail", action="store_true")
    compare_parser.add_argument("--component", choices=["extension", "framework"], default="extension")
    args = parser.parse_args()
    try:
        return run(args) if args.command == "run" else compare(args)
    except (ValueError, OSError, KeyError) as e:
        parser.error(str(e))


if __name__ == "__main__":
    sys.exit(main())
