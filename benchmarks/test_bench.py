"""Run on Linux: python3 -m unittest discover -s benchmarks -v.

Set VGI_BENCH_DRIVER to run the actual native host integration tests too.
"""
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import signal
import sys
import tempfile
import unittest

import bench
from proc import ProcessTree, parse_stat, totals, worker_cpu
from workloads import workload


class Workloads(unittest.TestCase):
    def test_nullable_checksum_arithmetic(self):
        for shape in ("int64", "varchar"):
            for n in (1, 7, 8, 19):
                case = {"rows": n, "columns": 4, "shape": shape, "null_every": 7, "value_bytes": 32}
                sql, expected, size = workload(case, "echo")
                reference = sum(i + c if shape == "int64" else 32 for i in range(n) if i % 7 for c in range(4))
                self.assertEqual(int(expected), reference)
                self.assertEqual(size, n * 4 * (8 if shape == "int64" else 32))
                for c in range(4):
                    self.assertIn(f"c{c}", sql)

    def test_invalid_matrix_rejected(self):
        for update in ({"rows": 0}, {"columns": 0}, {"shape": "bad"}, {"null_every": -1}):
            with self.assertRaises(ValueError):
                workload({"rows": 1, "columns": 1, "shape": "int64", **update}, "echo")
        for update in ({"source": "missing"}, {"batch_rows": 64},
                       {"source": "batch", "batch_rows": 0},
                       {"source": "batch", "batch_rows": 2049},
                       {"source": "batch", "batch_rows": 64, "shape": "int64"}):
            with self.assertRaises(ValueError):
                workload({"rows": 1, "columns": 1, "shape": "varchar", **update}, "echo")


class Accounting(unittest.TestCase):
    def test_live_child_memory_is_separate(self):
        code = "import os,time; data=bytearray(32*1024*1024); print(os.getpid(),flush=True); time.sleep(30)"
        host = subprocess.Popen([sys.executable, "-c",
                                 "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]]); p.wait()", code],
                                stdout=subprocess.PIPE, text=True, start_new_session=True)
        try:
            child_pid = int(host.stdout.readline())
            sample = ProcessTree(host.pid).snapshot()
            self.assertEqual({p["pid"] for p in sample}, {host.pid, child_pid})
            self.assertGreater(totals(sample, "worker")["rss_bytes"], 32 * 2**20)
            self.assertGreater(totals(sample, "host")["rss_bytes"], 0)
        finally:
            os.killpg(host.pid, signal.SIGTERM)
            host.wait(timeout=5)
            host.stdout.close()

    def test_process_name_with_parens(self):
        fields = ["S"] + ["0"] * 21
        fields[1], fields[11], fields[12], fields[19] = "17", "200", "100", "12345"
        parsed = parse_stat("42 (a (worker) name) " + " ".join(fields))
        self.assertEqual(parsed["ppid"], 17)
        self.assertEqual(parsed["start_ticks"], 12345)
        self.assertEqual(parsed["user_s"], 200 / os.sysconf("SC_CLK_TCK"))

    def test_missing_pss_is_not_zero(self):
        p = {"role": "host", "rss_bytes": 100, "pss_bytes": None, "private_bytes": None, "anonymous_bytes": 1, "swap_bytes": 0}
        self.assertIsNone(totals([p], "host")["pss_bytes"])
        self.assertEqual(totals([p], "worker")["rss_bytes"], 0)

    def test_worker_exit_and_pid_reuse(self):
        def p(pid, start, cpu):
            return {"pid": pid, "start_ticks": start, "role": "worker", "user_s": cpu, "system_s": 0}
        samples = [{"processes": [p(2, 10, 5)]}, {"processes": [p(2, 10, 7), p(3, 20, 3)]},
                   {"processes": [p(2, 30, 1)]}, {"processes": []}]
        self.assertEqual(worker_cpu(samples), 6)


class Comparisons(unittest.TestCase):
    def test_regression_gate_and_noise_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = [Path(tmp) / name for name in ("a", "b")]
            row = {"case": "a", "mode": "echo", "worker": "p", "transport": "stdio", "wall_s": 1}
            for path in paths:
                path.mkdir()
                bench.write_json(path / "metadata.json", {"status": "ok", "compatibility": {"profile": "none", "trials": 3}})
                bench.write_json(path / "summary.json", [row])
            args = argparse.Namespace(baseline=paths[0], candidate=paths[1], fail=True, metrics="wall_s", threshold=10, memory_floor_mib=4, time_floor_ms=1)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(bench.compare(args), 0)
                bench.write_json(paths[1] / "summary.json", [{**row, "wall_s": 1.5}])
                self.assertEqual(bench.compare(args), 1)
                args.time_floor_ms = 600
                self.assertEqual(bench.compare(args), 0)

    def test_gate_rejects_missing_coverage_and_failed_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = [Path(tmp) / name for name in ("a", "b")]
            for path in paths:
                path.mkdir()
                bench.write_json(path / "metadata.json", {"status": "ok", "compatibility": {"profile": "none", "trials": 3}})
            bench.write_json(paths[0] / "summary.json", [{"case": "a", "mode": "echo", "worker": "p", "transport": "stdio", "wall_s": 1}])
            bench.write_json(paths[1] / "summary.json", [])
            args = argparse.Namespace(baseline=paths[0], candidate=paths[1], fail=True, metrics="wall_s", threshold=10, memory_floor_mib=4, time_floor_ms=1)
            with self.assertRaisesRegex(ValueError, "coverage"):
                bench.compare(args)
            bench.write_json(paths[1] / "metadata.json", {"status": "failed"})
            with self.assertRaisesRegex(ValueError, "failed"):
                bench.compare(args)

    def test_profile_and_incompatible_runs_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = [Path(tmp) / name for name in ("a", "b")]
            for path in paths:
                path.mkdir()
                bench.write_json(path / "metadata.json", {"status": "ok", "compatibility": {"profile": "cpu", "trials": 3}})
            args = argparse.Namespace(baseline=paths[0], candidate=paths[1], fail=True)
            with self.assertRaisesRegex(ValueError, "profiled"):
                bench.compare(args)
            bench.write_json(paths[1] / "metadata.json", {"status": "ok", "compatibility": {"profile": "none", "trials": 3}})
            with self.assertRaisesRegex(ValueError, "incompatible"):
                bench.compare(args)


@unittest.skipUnless(os.environ.get("VGI_BENCH_DRIVER"), "set VGI_BENCH_DRIVER for native integration")
class NativeDriver(unittest.TestCase):
    def invoke(self, commands):
        process = subprocess.run([os.environ["VGI_BENCH_DRIVER"]], input="\n".join(commands) + "\n", text=True, capture_output=True, timeout=30)
        return process, [json.loads(line) for line in process.stdout.splitlines()]

    def test_error_and_interrupt_leave_connection_usable(self):
        process, events = self.invoke([
            "expect_error\tConversion Error\tSELECT CAST('invalid' AS INTEGER)",
            "query\t42\tSELECT 42",
            "interrupt\t50\tSELECT sum(sin(i)) FROM range(10000000000) t(i)",
            "query\t43\tSELECT 43", "close\t\t"])
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertIn("Interrupt", events[3]["checksum"])
        self.assertEqual(events[4]["checksum"], "43")

    @unittest.skipUnless(os.environ.get("VGI_BENCH_PROBE_EXTENSION"), "requires allocator-probe extension")
    def test_reused_schema_releases_previous_export(self):
        extension = os.environ["VGI_BENCH_PROBE_EXTENSION"]
        process, events = self.invoke([
            "exec\t\tLOAD '" + extension.replace("'", "''") + "'",
            "schema_reuse\t\t" + extension, "query\t1\tSELECT 1", "close\t\t"])
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertTrue(json.loads(events[2]["checksum"])["schema_reuse_passed"])

    def test_actual_native_sql_matrix(self):
        commands = []
        for shape in ("int64", "varchar"):
            for null_every in (0, 3):
                sql, expected, _ = workload({"shape": shape, "rows": 17, "columns": 32, "value_bytes": 256, "null_every": null_every}, "native")
                commands.append(f"query\t{expected}\t{sql}")
        process, events = self.invoke(commands + ["close\t\t"])
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(len(events), 6)
        self.assertGreater(events[-1]["host_lifetime_peak_rss_bytes"], 0)

    def test_bad_sql_and_checksum_fail(self):
        for command in ("query\t3\tSELECT 2", "query\t1\tSELECT * FROM nonexistent", "query\t1\tSELECT NULL", "bad"):
            process, events = self.invoke([command])
            self.assertNotEqual(process.returncode, 0)
            self.assertEqual(events[-1]["event"], "error")

    def test_batch_source_values_nulls_and_chunk_boundaries(self):
        commands = []
        cases = []
        for batch_rows in (1, 7, 64, 2048):
            for null_every in (0, 3):
                case = {"shape": "varchar", "rows": 2051, "columns": 4, "value_bytes": 32,
                        "source": "batch", "batch_rows": batch_rows, "null_every": null_every}
                sql, expected, _ = workload(case, "native")
                commands.append(f"query\t{expected}\t{sql}")
                cases.append(case)
        # Exact content (not just lengths), including the tail and column suffix.
        commands.append("query\t0\tSELECT count(*) FROM bench_strings(2051, 4, 32, 7, 0) b "
                        "FULL JOIN (SELECT repeat('x',8)||lpad(i::VARCHAR,20,'0')||'0000' c0, "
                        "repeat('x',8)||lpad(i::VARCHAR,20,'0')||'0003' c3 FROM range(2051) t(i)) s "
                        "USING(c0,c3) WHERE b.c0 IS NULL OR s.c0 IS NULL")
        process, events = self.invoke(commands)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        for case, event in zip(cases, events[1:]):
            self.assertEqual(event["source_rows"], case["rows"])
            self.assertEqual(event["source_batches"], (case["rows"] + case["batch_rows"] - 1) // case["batch_rows"])
            self.assertEqual(event["source_max_batch_bytes"], case["batch_rows"] * 4 * 32)

    def test_batch_source_rejects_invalid_parameters(self):
        for args in ("1,1,32,0,0", "1,1,32,2049,0", "1,1,23,1,0", "1,0,32,1,0", "NULL,1,32,1,0"):
            process, events = self.invoke([f"exec\t\tSELECT * FROM bench_strings({args})"])
            self.assertNotEqual(process.returncode, 0, process.stdout)
            self.assertEqual(events[-1]["event"], "error")

    def test_allocator_observation_after_database_close(self):
        process, events = self.invoke(["allocator\t\t", "close\t\t", "allocator\t\t"])
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        for event in (events[1], events[3]):
            stats = json.loads(event["checksum"])
            self.assertIn("glibc", stats)
            self.assertIn("jemalloc", stats)
            if stats["glibc"] is not None:
                self.assertGreaterEqual(stats["glibc"]["arena"], stats["glibc"]["fordblks"])

    def test_session_timeout_kills_owned_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            args = argparse.Namespace(driver=Path(os.environ["VGI_BENCH_DRIVER"]), profile="none", timeout=2, interval=.01, settle=0, rss_limit_mib=8192)
            with (path / "samples.jsonl").open("w") as samples:
                session = bench.Session(args, path, os.environ.copy(), samples)
                args.timeout = .1
                try:
                    with self.assertRaises(TimeoutError):
                        session.execute("timeout", "SELECT sum(sin(i)) FROM range(1000000000000) t(i)", "0")
                finally:
                    session.stop(abort=True)
                self.assertIsNotNone(session.process.returncode)


if __name__ == "__main__":
    unittest.main()
