"""Registry SAB fixtures must resolve one coherent SDK/RPC/Arrow dependency set."""

from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


class RustFixtureLocksTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        shutil.copy2(ROOT / "scripts/check_rust_fixture_locks.sh", self.root / "scripts")
        for fixture in ("sabffi", "sabtable"):
            dest = self.root / "test/support" / fixture
            dest.mkdir(parents=True)
            shutil.copy2(ROOT / "test/support" / fixture / "Cargo.lock", dest)

    def alter_package(self, fixture, name, *, version=None, source=None):
        path = self.root / "test/support" / fixture / "Cargo.lock"
        text = path.read_text()
        pattern = re.compile(r'(?ms)^\[\[package\]\]\nname = "' + re.escape(name) + r'"\n.*?(?=^\[\[package\]\]|\Z)')
        match = pattern.search(text)
        self.assertIsNotNone(match)
        block = match.group()
        if version is not None:
            block = re.sub(r'^version = .*$', f'version = "{version}"', block, count=1, flags=re.M)
        if source is not None:
            block = re.sub(r'^source = .*$', f'source = "{source}"', block, count=1, flags=re.M)
        path.write_text(text[:match.start()] + block + text[match.end():])

    def check(self):
        return subprocess.run(
            ["bash", str(self.root / "scripts/check_rust_fixture_locks.sh")],
            capture_output=True, text=True, timeout=15,
        )

    def test_repository_locks_are_coherent(self):
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rpc_version_drift_is_rejected(self):
        self.alter_package("sabffi", "vgi-rpc", version="99.0.0")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("two fixtures resolve different vgi-rpcs", result.stderr)

    def test_protocol_git_override_is_rejected(self):
        self.alter_package("sabtable", "vgi-protocol", source="git+https://example.invalid/vgi#candidate")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not crates.io", result.stderr)

    def test_sdk_protocol_version_drift_is_rejected(self):
        self.alter_package("sabtable", "vgi-protocol", version="99.0.0")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("matching SDK and protocol packages", result.stderr)

    def test_arrow_version_drift_is_rejected(self):
        self.alter_package("sabffi", "arrow-schema", version="99.0.0")
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected Arrow", result.stderr)

    def test_duplicate_arrow_dependency_is_rejected(self):
        path = self.root / "test/support/sabtable/Cargo.lock"
        with path.open("a") as output:
            output.write(f'\n[[package]]\nname = "arrow-array"\nversion = "99.0.0"\nsource = "{REGISTRY}"\n')
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("resolves 2 copies of arrow-array", result.stderr)


if __name__ == "__main__":
    unittest.main()
