"""CI artifacts must match the requested source revision and verified contents."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/ci_fetch_native_runner.py"
SPEC = importlib.util.spec_from_file_location("ci_native_runner", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class NativeArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.revision = "a" * 40
        self.run = {"id": 7, "head_sha": self.revision, "head_branch": "v2.0", "event": "push"}
        self.artifact = {"name": "vgi-native-runner-linux_amd64", "expired": False}
        self.bad_revision = False
        self.corrupt = False
        self.engine_env = None  # None: a manifest packaged before engine gates

    def api(self, path):
        if path.endswith("/artifacts"):
            return {"artifacts": [self.artifact]}
        return {"workflow_runs": [self.run]}

    def download(self, *args):
        staging = Path(args[args.index("--dir") + 1])
        for name in ("unittest", "haybarn"):
            (staging / name).write_bytes(b"built executable")
        manifest = {
            "revision": "b" * 40 if self.bad_revision else self.revision,
            "platform": "linux_amd64",
            "protocol_version": "3.0.0",
            "sha256": {name: runner.digest(staging / name) for name in ("unittest", "haybarn")},
        }
        if self.engine_env is not None:
            manifest["engine_test_env"] = self.engine_env
        (staging / "manifest.json").write_text(json.dumps(manifest))
        if self.corrupt:
            (staging / "unittest").write_bytes(b"corrupt executable")
        return ""

    def fetch(self, branch=None):
        with (
            patch.object(runner, "api", side_effect=self.api),
            patch.object(runner, "command", side_effect=self.download),
            patch.object(runner, "platform_name", return_value="linux_amd64"),
            patch.dict(os.environ, {"GITHUB_ENV": str(self.root / "environment")}),
        ):
            options = {"branch": branch} if branch is not None else {}
            runner.fetch(self.revision, self.root / "output", timeout=0, **options)

    def test_matching_build_exports_executable_paths(self):
        self.fetch()
        environment = (self.root / "environment").read_text()
        self.assertIn("RUNNER_LINKS_EXTENSIONS=1\n", environment)
        self.assertIn(f"HAYBARN_UNITTEST={self.root}/output/unittest\n", environment)
        self.assertTrue(os.access(self.root / "output/unittest", os.X_OK))

    def test_haybarn_artifact_exports_engine_test_env(self):
        self.engine_env = {
            "VGI_ENGINE_CORRELATED_NAMED_ARGS": "1",
            "VGI_ENGINE_DELETE_RETURNING_VIRTUAL_COLUMNS": "1",
        }
        self.fetch()
        environment = (self.root / "environment").read_text()
        self.assertIn("VGI_ENGINE_CORRELATED_NAMED_ARGS=1\n", environment)
        self.assertIn("VGI_ENGINE_DELETE_RETURNING_VIRTUAL_COLUMNS=1\n", environment)

    def test_upstream_and_legacy_artifacts_export_no_engine_test_env(self):
        for engine_env in ({}, None):
            with self.subTest(engine_env=engine_env):
                self.engine_env = engine_env
                (self.root / "environment").unlink(missing_ok=True)
                self.fetch()
                environment = (self.root / "environment").read_text()
                self.assertIn("HAYBARN_UNITTEST=", environment)
                self.assertNotIn("VGI_ENGINE_", environment)

    def test_malformed_engine_test_env_is_rejected(self):
        for engine_env in ({"VGI_ENGINE_X": "0"}, {"PATH": "1"}, ["VGI_ENGINE_X=1"]):
            with self.subTest(engine_env=engine_env):
                self.engine_env = engine_env
                with self.assertRaisesRegex(RuntimeError, "engine"):
                    self.fetch()
                self.assertFalse((self.root / "environment").exists())

    def test_engine_flavor_follows_the_checked_out_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "duckdb/src/include/duckdb").mkdir(parents=True)
            self.assertEqual(runner.engine_flavor(source), "duckdb")
            (source / "duckdb/src/include/duckdb/haybarn.h").write_text("#define HAYBARN 1\n")
            self.assertEqual(runner.engine_flavor(source), "haybarn")

    def test_parse_engine_test_env(self):
        self.assertEqual(runner.parse_engine_test_env(""), {})
        self.assertEqual(
            runner.parse_engine_test_env("VGI_ENGINE_A=1\nVGI_ENGINE_B=1\n"),
            {"VGI_ENGINE_A": "1", "VGI_ENGINE_B": "1"},
        )
        for bad in ("make: Entering directory", "VGI_ENGINE_A=", "VGI_ENGINE_A=yes", "OTHER=1"):
            with self.subTest(line=bad), self.assertRaises(RuntimeError):
                runner.parse_engine_test_env(bad)

    def test_mismatched_manifest_is_rejected(self):
        self.bad_revision = True
        with self.assertRaisesRegex(RuntimeError, "provenance"):
            self.fetch()
        self.assertFalse((self.root / "environment").exists())

    def test_corrupt_binary_is_rejected(self):
        self.corrupt = True
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            self.fetch()
        self.assertFalse((self.root / "output").exists())

    def test_other_revision_never_falls_back(self):
        self.run["head_sha"] = "c" * 40
        with self.assertRaisesRegex(RuntimeError, "no vgi-native-runner"):
            self.fetch()

    def test_pull_request_build_is_not_used(self):
        self.run["event"] = "pull_request"
        with self.assertRaisesRegex(RuntimeError, "no vgi-native-runner"):
            self.fetch()

    def test_explicit_branch_dispatch_is_accepted(self):
        self.run.update(head_branch="release/vgi-v3-ci", event="workflow_dispatch")
        self.fetch(branch="release/vgi-v3-ci")
        self.assertTrue((self.root / "output/unittest").is_file())

    def test_other_branch_never_falls_back(self):
        self.run["head_branch"] = "release/vgi-v3-ci"
        with self.assertRaisesRegex(RuntimeError, "no vgi-native-runner"):
            self.fetch()

    def test_expired_artifact_is_not_used(self):
        self.artifact["expired"] = True
        with self.assertRaisesRegex(RuntimeError, "no vgi-native-runner"):
            self.fetch()
