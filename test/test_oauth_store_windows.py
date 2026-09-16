#!/usr/bin/env python3
"""Native DPAPI/mutex regressions. Every store operation runs in a new process."""

import argparse
import concurrent.futures
import contextlib
import ctypes
from ctypes import wintypes
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid


class OAuthStoreWindowsTests(unittest.TestCase):
    probe = None

    @classmethod
    def setUpClass(cls):
        cls.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        cls.kernel.OpenMutexW.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR)
        cls.kernel.OpenMutexW.restype = wintypes.HANDLE
        cls.kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        cls.kernel.CloseHandle.restype = wintypes.BOOL
        cls.kernel.CreateFileW.argtypes = (
            wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
            wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
        )
        cls.kernel.CreateFileW.restype = wintypes.HANDLE

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="vgi-oauth-store-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        # Scoped to child processes: never inspect or prune the user's real cache.
        self.env = dict(os.environ, LOCALAPPDATA=str(self.root))
        self.prefix = "vgi-test-" + uuid.uuid4().hex

    def key(self, profile):
        return self.prefix + "|" + profile

    def credential_path(self, profile):
        digest = hashlib.sha256(self.key(profile).encode()).hexdigest()
        return self.root / "QueryFarm" / "VGI" / "oauth" / (digest + ".bin")

    def run_probe(self, operation, profile, value=None, mode="persistent", error=None):
        args = [str(self.probe), operation, self.key(profile), mode]
        if value is not None:
            args.append(value)
        result = subprocess.run(args, env=self.env, capture_output=True, text=True, timeout=15)
        if error is None:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertIn(error, result.stderr)
        return result

    @contextlib.contextmanager
    def abandoned_mutex(self, profile):
        """Kill a real owner while retaining the named mutex object in Windows."""
        owner = subprocess.Popen(
            [str(self.probe), "hold", self.key(profile), "persistent"],
            env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        retained = None
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                ready = pool.submit(owner.stdout.readline)
                try:
                    self.assertEqual(ready.result(timeout=15).strip(), "LOCKED")
                except concurrent.futures.TimeoutError:
                    owner.kill()
                    owner.wait(timeout=5)
                    self.fail("Credential lock owner did not become ready")
            digest = hashlib.sha256(self.key(profile).encode()).hexdigest()
            retained = self.kernel.OpenMutexW(0x00100000, False, "Local\\QueryFarm.VGI.OAuth." + digest)
            if not retained:
                raise ctypes.WinError(ctypes.get_last_error())
            owner.kill()
            owner.wait(timeout=5)
            yield
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.wait(timeout=5)
            owner.stdout.close()
            owner.stderr.close()
            if retained:
                self.kernel.CloseHandle(retained)

    def test_reuse_overwrite_and_delete_across_processes(self):
        for mode in ("auto", "persistent"):
            with self.subTest(mode=mode):
                self.run_probe("store", mode, "synthetic-R1", mode=mode)
                self.run_probe("read", mode, "synthetic-R1", mode=mode)
                self.run_probe("store", mode, "synthetic-R2", mode=mode)
                self.run_probe("read", mode, "synthetic-R2", mode=mode)
                self.run_probe("delete", mode, mode=mode)
                self.run_probe("absent", mode, mode=mode)
                self.run_probe("delete", mode, mode=mode)

    def test_abandoned_owner_recovers_and_releases_lease(self):
        for mode in ("auto", "persistent"):
            with self.subTest(mode=mode):
                self.run_probe("store", mode, "synthetic-R1")
                with self.abandoned_mutex(mode):
                    self.run_probe("read", mode, "synthetic-R1", mode=mode)
                    # The recovered lease must be released for later processes.
                    self.run_probe("store", mode, "synthetic-R2", mode=mode)
                    self.run_probe("read", mode, "synthetic-R2", mode=mode)

    def test_abandoned_owner_does_not_bypass_record_validation(self):
        self.run_probe("store", "corrupt", "synthetic-R1")
        with self.abandoned_mutex("corrupt"):
            self.credential_path("corrupt").write_bytes(b"invalid DPAPI test record")
            self.run_probe("read", "corrupt", "synthetic-R1", error="DPAPI credential decrypt failed")
            self.run_probe("store", "corrupt", "synthetic-R2")
            self.run_probe("read", "corrupt", "synthetic-R2")

    def test_denied_deletion_reports_failure_and_can_be_retried(self):
        for mode in ("auto", "persistent"):
            with self.subTest(mode=mode):
                self.run_probe("store", mode, "synthetic-token")
                # Share read/write access but omit FILE_SHARE_DELETE.
                handle = self.kernel.CreateFileW(
                    str(self.credential_path(mode)), 0x80000000, 0x1 | 0x2, None, 3, 0x80, None,
                )
                if handle == ctypes.c_void_p(-1).value:
                    raise ctypes.WinError(ctypes.get_last_error())
                try:
                    self.run_probe("delete", mode, mode=mode, error="DPAPI cache delete failed")
                    self.run_probe("read", mode, "synthetic-token")
                finally:
                    self.kernel.CloseHandle(handle)
                self.run_probe("delete", mode, mode=mode)
                self.run_probe("absent", mode)

    def test_memory_modes_leave_persistent_credentials_alone(self):
        for mode in ("memory", "none"):
            with self.subTest(mode=mode):
                self.run_probe("store", mode, "synthetic-token")
                self.run_probe("delete", mode, mode=mode)
                self.run_probe("store", mode, "synthetic-memory-token", mode=mode)
                self.run_probe("read", mode, "synthetic-token")

    def test_concurrent_processes_preserve_updates(self):
        self.run_probe("store", "counter", "0")
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.run_probe, "increment", "counter") for _ in range(8)]
            for future in futures:
                future.result()
        self.run_probe("read", "counter", "8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", required=True, type=Path, help="Path to vgi_oauth_store_probe.exe")
    args = parser.parse_args()
    if sys.platform != "win32":
        parser.error("These tests require native Windows DPAPI and named mutexes")
    OAuthStoreWindowsTests.probe = args.probe.resolve(strict=True)
    unittest.main(argv=[sys.argv[0]], verbosity=2)
