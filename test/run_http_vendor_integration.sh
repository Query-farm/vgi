#!/usr/bin/env bash
# Run VGI integration tests over HTTP with everything externalized through a
# fake blob vendor: large results are fetched from vgi_rpc.location pointers,
# large requests go up through __upload_url__, and worker storage values over
# a threshold live in the vendor instead of SQLite.
#
# Usage: ./test/run_http_vendor_integration.sh [unittest-args...]
# Example: ./test/run_http_vendor_integration.sh "test/sql/external_storage/*"
#
# The fake vendor (vgi._test_fixtures.fake_blob_vendor, from vgi-python) runs
# once for the whole lane on 127.0.0.1; every per-test fixture server inherits
# VGI_BLOB_VENDOR_URL and the thresholds below. The thresholds are tiny so
# modest test data triggers every path; override any of them from the caller.
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
VENDOR_TOKEN="${VGI_BLOB_VENDOR_TOKEN:-test-token}"

LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/vgi-fake-vendor.XXXXXX")"
uv run --project "$VGI_PYTHON_DIR" python -m vgi._test_fixtures.fake_blob_vendor \
    --token "$VENDOR_TOKEN" > "$LOG_FILE" 2>&1 &
VENDOR_PID=$!

cleanup() {
    kill "$VENDOR_PID" 2>/dev/null || true
    wait "$VENDOR_PID" 2>/dev/null || true
    rm -f "$LOG_FILE"
}
trap cleanup EXIT

VENDOR_URL=""
for _ in $(seq 1 60); do
    if ! kill -0 "$VENDOR_PID" 2>/dev/null; then
        echo "ERROR: fake blob vendor failed to start. Log:" >&2
        cat "$LOG_FILE" >&2
        exit 1
    fi
    VENDOR_URL=$(sed -n 's/^URL:\(.*\)$/\1/p' "$LOG_FILE" 2>/dev/null | head -1)
    if [[ -n "$VENDOR_URL" ]]; then
        break
    fi
    sleep 0.5
done
if [[ -z "$VENDOR_URL" ]]; then
    echo "ERROR: timed out waiting for the fake blob vendor. Log:" >&2
    cat "$LOG_FILE" >&2
    exit 1
fi
echo "fake blob vendor: $VENDOR_URL (pid $VENDOR_PID)"

export VGI_BLOB_VENDOR_URL="$VENDOR_URL"
export VGI_BLOB_VENDOR_TOKEN="$VENDOR_TOKEN"
# Results over 4 KiB are externalized; requests over 16 KiB must be uploaded.
export VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES="${VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES:-4096}"
export VGI_RPC_MAX_REQUEST_BYTES="${VGI_RPC_MAX_REQUEST_BYTES:-16384}"
# Worker storage (SQLite here) keeps values over 4 KiB in the vendor.
export VGI_STORAGE_EXTERNALIZE="${VGI_STORAGE_EXTERNALIZE:-1}"
export VGI_STORAGE_EXTERNALIZE_BYTES="${VGI_STORAGE_EXTERNALIZE_BYTES:-4096}"
# The fake vendor's signed URLs are http://127.0.0.1; production stores are https.
export VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1

exec "$(dirname "$0")/run_http_integration.sh" "$@"
