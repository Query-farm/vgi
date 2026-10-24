#!/usr/bin/env python3
"""Test-only Python fixture: opt upper_case into per-value caching.

Launch with the vgi-python fixture virtualenv. The SDK checkout is unchanged.
Used by suites/adaptive_cached_strings.json to check identity fallback with
duplicate input values, NULLs, full cache hits and partial cache hits.
"""
from vgi._test_fixtures.scalar.binary import UpperCaseFunction
from vgi._test_fixtures.worker import main
from vgi.cache_control import CacheControl

UpperCaseFunction.CACHE_CONTROL = CacheControl(ttl=300, per_value=True)

if __name__ == "__main__":
    main()
