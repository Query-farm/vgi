"""Select one registered SQL test per file, including packaged native runners."""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/run_tests.py"
SPEC = importlib.util.spec_from_file_location("run_tests", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class TestRegistrationSelection(unittest.TestCase):
    def setUp(self):
        self.path = Path("test/sql/integration/stateful.test")

    def test_duplicate_relative_and_absolute_executes_relative_once(self):
        selected = runner._select_test_names(
            [self.path], [str(self.path), "/build/vgi/" + str(self.path)]
        )
        self.assertEqual(selected, {self.path: str(self.path)})

    def test_packaged_runner_absolute_registration(self):
        absolute = "/original/build/vgi/" + str(self.path)
        self.assertEqual(runner._select_test_names([self.path], [absolute]), {self.path: absolute})

    def test_missing_registration_fails_instead_of_passing_zero_assertions(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            runner._select_test_names([self.path], ["test/sql/unrelated.test"])

    def test_ambiguous_absolute_registration_fails(self):
        with self.assertRaisesRegex(ValueError, "found 2"):
            runner._select_test_names([self.path], ["/a/" + str(self.path), "/b/" + str(self.path)])


class TestSkipDetection(unittest.TestCase):
    def test_skipped_file_is_detected_on_either_output_stream(self):
        banner = "Skipped tests for the following reasons:\nrequire vgi: 1\n"
        for stdout, stderr in ((banner, ""), ("All tests were skipped (total skipped 1)\n", banner)):
            with self.subTest(stdout=stdout, stderr=stderr), patch.object(runner.subprocess, "Popen") as spawn:
                process = spawn.return_value
                process.pid = 123456
                process.returncode = 0
                process.communicate.return_value = (stdout, stderr)
                result = runner.run_one(Path("test/example.test"), Path("/unused/unittest"), {}, [], 1)
                self.assertEqual(result["exit"], 0)
                self.assertEqual(result["skip_reasons"], {"require vgi": 1})
                self.assertIn("SKIP", runner._format_result_line(1, 1, result))

    def test_successful_execution_has_no_skip_reason(self):
        self.assertEqual(runner._parse_skips("All tests passed (8 assertions in 1 test case)\n", ""), {})


if __name__ == "__main__":
    unittest.main()
