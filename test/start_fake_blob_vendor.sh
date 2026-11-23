#!/usr/bin/env bash
# Start vgi-rpc's fake blob vendor and export the environment the blob-vendor
# externalization tests (test/sql/external_storage/) and every SDK's fixture
# worker need. Language-neutral: any SDK lane can source this, then start its
# own fixture worker (HTTP) and run the sqllogictests.
#
#   source test/start_fake_blob_vendor.sh   # starts the vendor, exports below
#   trap stop_fake_blob_vendor EXIT         # the caller owns clean-up
#
# The vendor is `vgi-rpc-fake-vendor --test-routes` from vgi-rpc-python, run
# with uvx from a pinned git commit (no vgi-python, no project checkout):
#
#   VGI_RPC_FAKE_VENDOR_REF   vgi-rpc-python commit to run (bump here).
#   VGI_RPC_FAKE_VENDOR_FROM  full `uvx --from` spec, overriding the ref, e.g.
#                             a local checkout or 'vgi-rpc==X.Y.Z'.
#   VGI_FAKE_BLOB_VENDOR_LOG_DIR
#                             where the vendor's log goes (default ${TMPDIR:-/tmp}).
#
# Exported for the worker and the tests (each keeps a caller's value, so a lane
# can override any threshold):
#
#   VGI_BLOB_VENDOR_URL                  the vendor's base URL (http://127.0.0.1:<port>);
#                                        tests read $VGI_BLOB_VENDOR_URL/__stats__ and
#                                        drive /__admin__/fault and /__admin__/reset.
#   VGI_BLOB_VENDOR_TOKEN                bearer token (default test-token, tenant-a).
#   VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES  4096: result batches over this are vendor pointers.
#   VGI_RPC_MAX_REQUEST_BYTES            32768: the worker's advertised request cap; larger
#                                        requests go up through sized vendor upload URLs
#                                        (VGI-Upload-URL-Support: 2).
#   VGI_RPC_MAX_UPLOAD_BYTES             268435456: the advertised upload cap.
#   VGI_STORAGE_EXTERNALIZE              1: wrap the worker's function storage so large
#                                        values live in the vendor.
#   VGI_STORAGE_EXTERNALIZE_BYTES        4096: storage values of at least this many bytes
#                                        are vendor blobs (tests use it via ${...}).
#   VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS
#                                        1: lets the extension fetch the fake's
#                                        http://127.0.0.1 signed URLs (production is https).
#   VGI_TEST_ARGUMENTS_REF_FAULTS        1: enables the extension's malformed-BindRequest
#                                        hook (SET VARIABLE vgi_test_arguments_ref_fault) that
#                                        arguments_ref_errors.test drives. Extension only.
#
# Also set (not exported): VGI_FAKE_BLOB_VENDOR_PID, VGI_FAKE_BLOB_VENDOR_LOG.
# The contract the worker must meet is docs/testing/blob-test-catalog.md.
#
# Diagnosing a vendor that dies mid-lane ("connection refused" for the rest of
# the suite): the vendor's stdout/stderr go to $VGI_FAKE_BLOB_VENDOR_LOG, in
# VGI_FAKE_BLOB_VENDOR_LOG_DIR (default ${TMPDIR:-/tmp}). Its path is printed at
# start and the file is kept after stop. The vendor logs each SIGTERM/SIGINT/
# SIGHUP it receives with the sender's pid and command line. A watcher appends
# the time the process disappeared, and stop_fake_blob_vendor reports whether it
# was still alive; if it had died, it prints the exit status or signal and the
# log's tail. Never stop a vendor with `pkill -f` on the shared command line:
# kill $VGI_FAKE_BLOB_VENDOR_PID.

# protocol-3.4 branch (upload URLs v2: sized client PUTs, upload fault and
# refusal counters; client-upload counters and an upload fault limited to
# client uploads, both keyed on origin="client_upload"; signal logging).
# TODO(release-wave): pin the released vgi-rpc.
: "${VGI_RPC_FAKE_VENDOR_REF:=f0fb6754ae5b70900ba21629a1e2d48eb2182774}"
: "${VGI_RPC_FAKE_VENDOR_FROM:=git+https://github.com/Query-farm/vgi-rpc-python@${VGI_RPC_FAKE_VENDOR_REF}}"
: "${VGI_BLOB_VENDOR_TOKEN:=test-token}"

# Describe an exit status from `wait`: "exit N" or "signal NAME (N)".
_fake_blob_vendor_status() {
    local status=$1
    if (( status > 128 )); then
        echo "signal $(kill -l "$((status - 128))" 2>/dev/null || echo '?') ($((status - 128)))"
    else
        echo "exit $status"
    fi
}

stop_fake_blob_vendor() {
    local pid="${VGI_FAKE_BLOB_VENDOR_PID:-}" status=0
    if [[ -n "${VGI_FAKE_BLOB_VENDOR_WATCH_PID:-}" ]]; then
        kill "$VGI_FAKE_BLOB_VENDOR_WATCH_PID" 2>/dev/null || true
        wait "$VGI_FAKE_BLOB_VENDOR_WATCH_PID" 2>/dev/null || true
        VGI_FAKE_BLOB_VENDOR_WATCH_PID=""
    fi
    [[ -n "$pid" ]] || return 0
    VGI_FAKE_BLOB_VENDOR_PID=""
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || status=$?
        echo "fake blob vendor: was alive at stop (pid $pid, then $(_fake_blob_vendor_status "$status")); log kept: $VGI_FAKE_BLOB_VENDOR_LOG"
    else
        # Already gone: reap it for its status (it is this shell's child).
        wait "$pid" 2>/dev/null || status=$?
        {
            echo "WARNING: fake blob vendor (pid $pid) had DIED before stop: $(_fake_blob_vendor_status "$status")."
            echo "  Every vendor request after that failed (connection refused). Log: $VGI_FAKE_BLOB_VENDOR_LOG"
            echo "  ---- last 40 log lines ----"
            tail -n 40 "$VGI_FAKE_BLOB_VENDOR_LOG" 2>/dev/null | sed 's/^/  | /'
            echo "  ----"
        } >&2
    fi
}

_start_fake_blob_vendor() {
    local log_dir="${VGI_FAKE_BLOB_VENDOR_LOG_DIR:-${TMPDIR:-/tmp}}"
    mkdir -p "$log_dir"
    VGI_FAKE_BLOB_VENDOR_LOG="$(mktemp "$log_dir/vgi-fake-vendor.XXXXXX")"
    mv "$VGI_FAKE_BLOB_VENDOR_LOG" "$VGI_FAKE_BLOB_VENDOR_LOG.log"
    VGI_FAKE_BLOB_VENDOR_LOG="$VGI_FAKE_BLOB_VENDOR_LOG.log"
    echo "fake blob vendor log: $VGI_FAKE_BLOB_VENDOR_LOG"
    # The PID is uvx's; uvx runs the vendor as its child and forwards SIGTERM
    # to it, so `kill $VGI_FAKE_BLOB_VENDOR_PID` stops both. A SIGTERM sent by
    # uvx is therefore logged by the vendor as coming from uvx's pid.
    bash -c 'exec uvx --from "$1" vgi-rpc-fake-vendor --test-routes --token "$2=tenant-a"' _ \
        "$VGI_RPC_FAKE_VENDOR_FROM" "$VGI_BLOB_VENDOR_TOKEN" > "$VGI_FAKE_BLOB_VENDOR_LOG" 2>&1 &
    VGI_FAKE_BLOB_VENDOR_PID=$!

    local url="" _
    # The first run builds vgi-rpc from git: allow up to 3 minutes.
    for _ in $(seq 1 360); do
        if ! kill -0 "$VGI_FAKE_BLOB_VENDOR_PID" 2>/dev/null; then
            echo "ERROR: fake blob vendor failed to start ($VGI_RPC_FAKE_VENDOR_FROM). Log:" >&2
            cat "$VGI_FAKE_BLOB_VENDOR_LOG" >&2
            return 1
        fi
        url=$(sed -n 's/^URL:\(.*\)$/\1/p' "$VGI_FAKE_BLOB_VENDOR_LOG" 2>/dev/null | head -1)
        [[ -n "$url" ]] && break
        sleep 0.5
    done
    if [[ -z "$url" ]]; then
        echo "ERROR: timed out waiting for the fake blob vendor. Log:" >&2
        cat "$VGI_FAKE_BLOB_VENDOR_LOG" >&2
        stop_fake_blob_vendor
        return 1
    fi
    echo "fake blob vendor: $url (pid $VGI_FAKE_BLOB_VENDOR_PID, from $VGI_RPC_FAKE_VENDOR_FROM)"

    # Note in the log when (not why: the vendor logs signals itself) the process
    # disappeared, so a death can be lined up with the test that was running.
    # Lanes run with `set -e`; keep the watcher's failures to itself.
    (
        trap - EXIT
        while kill -0 "$VGI_FAKE_BLOB_VENDOR_PID" 2>/dev/null; do sleep 1; done
        echo "start_fake_blob_vendor.sh: $(date -u +%Y-%m-%dT%H:%M:%SZ) vendor pid $VGI_FAKE_BLOB_VENDOR_PID is gone" >> "$VGI_FAKE_BLOB_VENDOR_LOG"
    ) </dev/null >/dev/null 2>&1 &
    VGI_FAKE_BLOB_VENDOR_WATCH_PID=$!

    export VGI_BLOB_VENDOR_URL="$url"
    export VGI_BLOB_VENDOR_TOKEN
    # Not lower: an uploaded request is replaced by a zero-row pointer that keeps
    # the request's schema, and the widest fixture input (echo over all_types)
    # has a ~20 KiB schema, so its pointer alone would exceed a smaller cap.
    export VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES="${VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES:-4096}"
    export VGI_RPC_MAX_REQUEST_BYTES="${VGI_RPC_MAX_REQUEST_BYTES:-32768}"
    # The client refuses a request over the advertised upload cap; the suite has
    # multi-MB exchange batches (dedup_large_strings), so keep it well above them.
    export VGI_RPC_MAX_UPLOAD_BYTES="${VGI_RPC_MAX_UPLOAD_BYTES:-268435456}"
    export VGI_STORAGE_EXTERNALIZE="${VGI_STORAGE_EXTERNALIZE:-1}"
    export VGI_STORAGE_EXTERNALIZE_BYTES="${VGI_STORAGE_EXTERNALIZE_BYTES:-4096}"
    export VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1
    export VGI_TEST_ARGUMENTS_REF_FAULTS="${VGI_TEST_ARGUMENTS_REF_FAULTS:-1}"
}

_start_fake_blob_vendor
