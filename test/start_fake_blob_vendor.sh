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
#
# Also set (not exported): VGI_FAKE_BLOB_VENDOR_PID, VGI_FAKE_BLOB_VENDOR_LOG.
# The contract the worker must meet is docs/testing/blob-test-catalog.md.

# protocol-3.4 branch (upload URLs v2: sized client PUTs, upload fault and
# refusal counters). TODO(release-wave): pin the released vgi-rpc.
: "${VGI_RPC_FAKE_VENDOR_REF:=a07ccf2e1bf1c0832ae43757a55e8b4817674b2d}"
: "${VGI_RPC_FAKE_VENDOR_FROM:=git+https://github.com/Query-farm/vgi-rpc-python@${VGI_RPC_FAKE_VENDOR_REF}}"
: "${VGI_BLOB_VENDOR_TOKEN:=test-token}"

stop_fake_blob_vendor() {
    if [[ -n "${VGI_FAKE_BLOB_VENDOR_PID:-}" ]]; then
        kill "$VGI_FAKE_BLOB_VENDOR_PID" 2>/dev/null || true
        wait "$VGI_FAKE_BLOB_VENDOR_PID" 2>/dev/null || true
        VGI_FAKE_BLOB_VENDOR_PID=""
    fi
    if [[ -n "${VGI_FAKE_BLOB_VENDOR_LOG:-}" ]]; then
        rm -f "$VGI_FAKE_BLOB_VENDOR_LOG"
    fi
}

_start_fake_blob_vendor() {
    VGI_FAKE_BLOB_VENDOR_LOG="$(mktemp "${TMPDIR:-/tmp}/vgi-fake-vendor.XXXXXX")"
    # exec: the PID is the vendor's own (uvx execs the tool), so kill stops it.
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
}

_start_fake_blob_vendor
