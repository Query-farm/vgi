#!/usr/bin/env bash
# End-to-end native VGI qualification for raw Arrow-mux and HTTP-over-Iroh.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-release}"
VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
VGI_RPC_PYTHON_DIR="${VGI_RPC_PYTHON_DIR:-$HOME/Development/vgi-rpc-python}"
VGI_IROH_BRIDGE_BIN="${VGI_IROH_BRIDGE_BIN:-$HOME/Development/vgi-rpc-rust/target/release/vgi-iroh-bridge}"
HAYBARN="${ROOT_DIR}/build/${BUILD_DIR}/haybarn"

for required in "$HAYBARN" "$VGI_IROH_BRIDGE_BIN"; do
    if [[ ! -x "$required" ]]; then
        echo "missing executable: $required" >&2
        exit 1
    fi
done

RUN_DIR="$(mktemp -d)"
PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$RUN_DIR"
}
trap cleanup EXIT

wait_for_value() {
    local file="$1"
    local expression="$2"
    local value=""
    for _ in $(seq 1 120); do
        value="$(sed -n "$expression" "$file" 2>/dev/null | head -1)"
        if [[ -n "$value" ]]; then
            printf '%s' "$value"
            return 0
        fi
        sleep 0.25
    done
    echo "timed out waiting for discovery output in $file" >&2
    sed -n '1,160p' "$file" >&2 || true
    return 1
}

PYTHONPATH="$VGI_RPC_PYTHON_DIR" \
    uv run --project "$VGI_PYTHON_DIR" python "$ROOT_DIR/test/support/iroh_http_fixture.py" \
    >"$RUN_DIR/http-worker.log" 2>&1 &
PIDS+=("$!")
HTTP_PORT="$(wait_for_value "$RUN_DIR/http-worker.log" 's/.*PORT:\([0-9][0-9]*\).*/\1/p')"

# The upstream worker is fail-closed for RPC dispatch: bypassing the bridge
# supplies no peer evidence. OPTIONS remains intentionally unauthenticated so
# clients can discover transport capabilities before making an authenticated
# call.
DIRECT_STATUS="$(
    python3 - "$HTTP_PORT" <<'PY'
import sys
import time
import urllib.error
import urllib.request

request = urllib.request.Request(f"http://127.0.0.1:{sys.argv[1]}/bind", data=b"", method="POST")
for _ in range(40):
    try:
        urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as error:
        print(error.code)
        break
    except urllib.error.URLError:
        time.sleep(0.1)
    else:
        print("unexpected-success")
        break
else:
    print("unreachable")
PY
)"
if [[ "$DIRECT_STATUS" != "401" ]]; then
    echo "HTTP worker did not require forwarded Iroh identity (status: $DIRECT_STATUS)" >&2
    exit 1
fi

PYTHONPATH="$VGI_RPC_PYTHON_DIR" \
    uv run --project "$VGI_PYTHON_DIR" python "$ROOT_DIR/test/support/iroh_raw_fixture.py" \
    >"$RUN_DIR/raw-worker.log" 2>&1 &
PIDS+=("$!")
RAW_PORT="$(wait_for_value "$RUN_DIR/raw-worker.log" 's/^TCP:127\.0\.0\.1:\([0-9][0-9]*\)$/\1/p')"

"$VGI_IROH_BRIDGE_BIN" --ephemeral --http-upstream "http://127.0.0.1:$HTTP_PORT" \
    >"$RUN_DIR/http-bridge.log" 2>&1 &
PIDS+=("$!")
HTTP_ENDPOINT="$(wait_for_value "$RUN_DIR/http-bridge.log" '/^[0-9a-f][0-9a-f]*$/p')"

"$VGI_IROH_BRIDGE_BIN" --ephemeral --raw-upstream "tcp://127.0.0.1:$RAW_PORT" \
    >"$RUN_DIR/raw-bridge.log" 2>&1 &
PIDS+=("$!")
RAW_ENDPOINT="$(wait_for_value "$RUN_DIR/raw-bridge.log" '/^[0-9a-f][0-9a-f]*$/p')"

if [[ ${#HTTP_ENDPOINT} -ne 64 || ${#RAW_ENDPOINT} -ne 64 ]]; then
    echo "bridge emitted an invalid EndpointId" >&2
    exit 1
fi

HTTP_RESULT="$($HAYBARN :memory: -csv -noheader -c "
SET vgi_iroh_connect_timeout_seconds = 7;
SET vgi_iroh_io_timeout_seconds = 19;
ATTACH 'example' AS remote_a (TYPE vgi, LOCATION 'httpi://${HTTP_ENDPOINT}');
SELECT remote_a.main.double(21);
SELECT remote_a.main.whoami(1);
SET vgi_iroh_connect_timeout_seconds = 11;
SET vgi_iroh_io_timeout_seconds = 23;
ATTACH 'example' AS remote_b (TYPE vgi, LOCATION 'httpi://${HTTP_ENDPOINT}');
SELECT remote_b.main.whoami(1);
")"
grep -Fxq '42' <<<"$HTTP_RESULT"
HTTP_PRINCIPALS="$(grep -E '^peer/iroh/vgi\.test/[0-9a-f]{64}$' <<<"$HTTP_RESULT" || true)"
if [[ "$(wc -l <<<"$HTTP_PRINCIPALS" | tr -d ' ')" != "2" ]] || \
        [[ "$(sort -u <<<"$HTTP_PRINCIPALS" | wc -l | tr -d ' ')" != "1" ]]; then
    echo "HTTP worker did not observe one process-stable authenticated Iroh principal across timeout configurations" >&2
    printf '%s\n' "$HTTP_RESULT" >&2
    exit 1
fi

RAW_RESULT="$($HAYBARN :memory: -csv -noheader -c "
ATTACH 'example' AS remote (TYPE vgi, LOCATION 'iroh://${RAW_ENDPOINT}');
SELECT remote.main.double(21);
SELECT sum(i) FROM remote.main.sequence(10000) t(i);
")"
grep -Fxq '42' <<<"$RAW_RESULT"
grep -Fxq '49995000' <<<"$RAW_RESULT"

echo "Native VGI Iroh integration passed (httpi required identity + scalar; raw scalar + streaming)."
