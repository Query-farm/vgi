#!/usr/bin/env bash
# Run VGI integration tests over HTTP with everything externalized through a
# fake blob vendor: large results are fetched from vgi_rpc.location pointers,
# large requests go up through __upload_url__, and worker storage values over
# a threshold live in the vendor instead of SQLite.
#
# Usage: ./test/run_http_vendor_integration.sh [unittest-args...]
# Example: ./test/run_http_vendor_integration.sh "test/sql/external_storage/*"
#
# The fake vendor (vgi-rpc's `vgi-rpc-fake-vendor --test-routes`, started by
# test/start_fake_blob_vendor.sh from a pinned vgi-rpc-python commit) runs
# once for the whole lane on 127.0.0.1; every per-test fixture server inherits
# VGI_BLOB_VENDOR_URL and the tiny thresholds that script exports, so modest
# test data triggers every path. Override any of them from the caller. The
# fixture server is vgi-python's (run_http_integration.sh); another SDK's lane
# sources test/start_fake_blob_vendor.sh and starts its own worker instead.
set -euo pipefail

# shellcheck source=test/start_fake_blob_vendor.sh
source "$(dirname "$0")/start_fake_blob_vendor.sh"
trap stop_fake_blob_vendor EXIT

rc=0
"$(dirname "$0")/run_http_integration.sh" "$@" || rc=$?
# Evidence that the lane externalized at all: the vendor's lifetime counters.
echo "fake blob vendor counters: $(python3 -c 'import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1], timeout=10).read().decode().strip())' "$VGI_BLOB_VENDOR_URL/__stats__" 2>&1)"
exit "$rc"
