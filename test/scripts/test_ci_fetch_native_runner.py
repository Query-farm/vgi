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
        self.run = {"id": 7, "head_sha": self.revision, "head_branch": "main", "event": "push"}
        self.artifact = {"name": "vgi-native-runner-linux_amd64", "expired": False}
        self.bad_revision = False
        self.corrupt = False

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
        (staging / "manifest.json").write_text(json.dumps(manifest))
        if self.corrupt:
            (staging / "unittest").write_bytes(b"corrupt executable")
        return ""

    def fetch(self):
        with (
            patch.object(runner, "api", side_effect=self.api),
            patch.object(runner, "command", side_effect=self.download),
            patch.object(runner, "platform_name", return_value="linux_amd64"),
            patch.dict(os.environ, {"GITHUB_ENV": str(self.root / "environment")}),
        ):
            runner.fetch(self.revision, self.root / "output", timeout=0)

    def test_matching_build_exports_executable_paths(self):
        self.fetch()
        environment = (self.root / "environment").read_text()
        self.assertIn("RUNNER_LINKS_EXTENSIONS=1\n", environment)
        self.assertIn(f"HAYBARN_UNITTEST={self.root}/output/unittest\n", environment)
        self.assertTrue(os.access(self.root / "output/unittest", os.X_OK))

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

    def test_expired_artifact_is_not_used(self):
        self.artifact["expired"] = True
        with self.assertRaisesRegex(RuntimeError, "no vgi-native-runner"):
            self.fetch()
