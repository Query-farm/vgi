#!/usr/bin/env bash
# Run VGI integration tests against a worker packaged as an OCI container image.
#
# Usage: ./test/run_docker_integration.sh [unittest-args...]
# Example: ./test/run_docker_integration.sh "test/sql/integration/container/*"
#
# Skips cleanly (exit 0) when no container runtime is available, so it is safe to
# wire into CI jobs that may or may not have docker/podman. Honors:
#   VGI_DOCKER_IMAGE   image ref (default ghcr.io/query-farm/vgi-sklearn:latest)
#   CONTAINER_RUNTIME  force a runtime (docker|podman|nerdctl); else auto-detect
#   VGI_DOCKER_NO_PULL set to 1 to skip the pre-pull (use a locally-built image)
#   BUILD_DIR          release (default) | debug
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-release}"
IMAGE="${VGI_DOCKER_IMAGE:-ghcr.io/query-farm/vgi-sklearn:latest}"
FILTER="${1:-test/sql/integration/container/*}"
shift 2>/dev/null || true

# Resolve a container runtime: honor the override, else probe in priority order.
RUNTIME=""
if [[ -n "${CONTAINER_RUNTIME:-}" ]]; then
    if command -v "$CONTAINER_RUNTIME" >/dev/null 2>&1; then
        RUNTIME="$CONTAINER_RUNTIME"
    fi
else
    for r in docker podman nerdctl; do
        if command -v "$r" >/dev/null 2>&1; then
            RUNTIME="$r"
            break
        fi
    done
fi

if [[ -z "$RUNTIME" ]]; then
    echo "run_docker_integration.sh: no container runtime found — skipping container tests."
    exit 0
fi

# A runtime binary on PATH isn't enough; the daemon/engine must answer.
if ! "$RUNTIME" info >/dev/null 2>&1; then
    echo "run_docker_integration.sh: '$RUNTIME' present but engine not responding — skipping."
    exit 0
fi

echo "Container runtime: $RUNTIME"
echo "Image:             $IMAGE"

if [[ "${VGI_DOCKER_NO_PULL:-}" != "1" ]]; then
    echo "Pulling $IMAGE ..."
    if ! "$RUNTIME" pull "$IMAGE"; then
        echo "run_docker_integration.sh: failed to pull $IMAGE — skipping (set VGI_DOCKER_NO_PULL=1 to use a local image)."
        exit 0
    fi
fi

# The errors.test gate uses VGI_TEST_WORKER; the smoke test uses VGI_DOCKER_IMAGE
# as the oci:// LOCATION. The extension auto-detects the runtime itself, so we
# only export the image ref here.
export VGI_DOCKER_IMAGE="oci://$IMAGE"
export VGI_TEST_WORKER="oci://$IMAGE"

# shared_tcp.test runs only when the image advertises a "tcp" transport (and thus
# provides a `tcp` entrypoint); gate it on the farm.query.vgi.transports label.
if "$RUNTIME" image inspect --format '{{ index .Config.Labels "farm.query.vgi.transports" }}' "$IMAGE" 2>/dev/null \
        | grep -q '"tcp"'; then
    echo "Image advertises tcp transport — enabling shared_tcp.test"
    export VGI_DOCKER_TCP_IMAGE="oci://$IMAGE"
fi

echo "Running container integration tests: $FILTER"
./build/"$BUILD_DIR"/test/unittest "$FILTER" "$@"
