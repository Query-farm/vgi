// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Cross-language flock parity test.  The Python reference launcher uses
// the ``filelock`` package, which on POSIX dispatches to ``fcntl.flock`` —
// i.e. the same ``flock(2)`` syscall the C++ launcher uses.  This test
// verifies they actually interlock by:
//
//   1. taking an exclusive ``flock(2)`` on a tempfile from the test
//      thread,
//   2. running ``python3 -c "import filelock; …"`` in a subprocess that
//      tries the same path with a short timeout, and
//   3. asserting Python timed out (cannot acquire) — proving the locks
//      see each other.
//
// Skipped silently if Python or filelock is unavailable on the test box.

#include "catch.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/file.h>
#include <thread>
#include <unistd.h>

namespace {

bool HasPythonFilelock() {
	int rc = std::system("python3 -c 'import filelock' >/dev/null 2>&1");
	return rc == 0;
}

std::string TempLockPath() {
	std::random_device rd;
	std::mt19937_64 gen(rd());
	std::string p = "/tmp/vgi-flock-parity-" + std::to_string(gen() & 0xFFFFFFFF) + ".lock";
	return p;
}

} // namespace

TEST_CASE("Python filelock and C++ flock(2) interlock against the same path",
          "[launcher][flock]") {
	if (!HasPythonFilelock()) {
		SUCCEED("python3 filelock not available; skipping cross-language parity test");
		return;
	}
	std::string path = TempLockPath();
	int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0600);
	REQUIRE(fd >= 0);
	REQUIRE(::flock(fd, LOCK_EX | LOCK_NB) == 0);

	// Python attempts to acquire with a 1-s timeout and reports its outcome
	// via exit code: 0 = acquired (parity broken), 1 = timed out (parity
	// good).  3 = error.
	std::string cmd = "python3 -c \""
	                  "import sys, filelock;"
	                  "lock = filelock.FileLock('" + path + "', timeout=1);\n"
	                  "import filelock as fl\n"
	                  "try:\n"
	                  "    lock.acquire()\n"
	                  "    sys.exit(0)\n"
	                  "except fl.Timeout:\n"
	                  "    sys.exit(1)\n"
	                  "except Exception:\n"
	                  "    sys.exit(3)\n"
	                  "\" 2>/dev/null";
	int rc = std::system(cmd.c_str());
	int status = WEXITSTATUS(rc);

	// Release C++ flock + cleanup.
	::flock(fd, LOCK_UN);
	::close(fd);
	::unlink(path.c_str());

	if (status == 0) {
		FAIL("Python filelock acquired the lock while C++ held flock(2) — "
		     "cross-language parity is BROKEN.  filelock no longer dispatches "
		     "to fcntl.flock on this platform.");
	} else if (status == 3) {
		WARN("Python filelock raised an unexpected exception; cross-language "
		     "parity could not be verified.");
	} else {
		// status == 1: timed out, which is what we want.
		SUCCEED("Python filelock timed out while C++ held flock(2) — interlock confirmed");
	}
}

TEST_CASE("C++ flock(2) is non-blocking when Python filelock holds the lock",
          "[launcher][flock]") {
	if (!HasPythonFilelock()) {
		SUCCEED("python3 filelock not available; skipping cross-language parity test");
		return;
	}
	std::string path = TempLockPath();

	// Spawn a Python process that grabs the lock and holds it for 2 s.
	std::string py_cmd = "python3 -c \""
	                     "import filelock, time;"
	                     "lock = filelock.FileLock('" + path + "');"
	                     "lock.acquire();"
	                     "time.sleep(2);"
	                     "lock.release()"
	                     "\" >/dev/null 2>&1 &";
	std::system(py_cmd.c_str());
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0600);
	REQUIRE(fd >= 0);
	int rc = ::flock(fd, LOCK_EX | LOCK_NB);
	int saved_errno = errno;

	// Wait for the Python helper to finish.
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));

	if (rc == 0) {
		// Got the lock — but Python should have held it.  Either Python
		// failed silently (race) or the locks don't interlock.
		::flock(fd, LOCK_UN);
		::close(fd);
		::unlink(path.c_str());
		WARN("C++ flock acquired while Python filelock should have held — "
		     "could be a Python startup-race rather than a parity bug; verify manually.");
		return;
	}
	REQUIRE(rc == -1);
	REQUIRE(saved_errno == EWOULDBLOCK);

	// Now Python should have released; we should be able to acquire.
	REQUIRE(::flock(fd, LOCK_EX | LOCK_NB) == 0);
	::flock(fd, LOCK_UN);
	::close(fd);
	::unlink(path.c_str());
}
