#!/usr/bin/env bash
# Install the vgi-python fixture workers so the integration suite can actually
# run in CI.
#
# Every VGI integration .test file carries `require-env VGI_TEST_WORKER`, so
# without a worker on the runner they all SKIP — and a green CI run means
# "built + header-clean", not "integration suite passed". That gap is why the
# cross-SDK drift documented in CLAUDE.md accumulated unnoticed.
#
# The workers are deliberately NOT in the published vgi-python wheel (they would
# pollute end-user PATH), so they come from the public repo's dev-only sidecar
# distribution via the uv workspace. The repo is public, so no token is needed.
#
# Idempotent, and a no-op unless VGI_INSTALL_FIXTURES=1 — a local `make
# test_release` must not start cloning things.
set -euo pipefail

if [ "${VGI_INSTALL_FIXTURES:-0}" != "1" ]; then
    exit 0
fi

DEST="${VGI_FIXTURES_DIR:-/tmp/vgi-python}"
REF="${VGI_FIXTURES_REF:-main}"

if [ ! -d "$DEST/.git" ]; then
    echo "==> cloning vgi-python fixtures into $DEST"
    git clone --depth 1 --branch "$REF" https://github.com/Query-farm/vgi-python.git "$DEST"
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "==> installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
fi

echo "==> syncing the fixture workspace"
uv sync --project "$DEST" >/dev/null

# Prove the worker is actually runnable BEFORE the suite starts. Without this a
# broken install shows up as ~300 silently skipped tests reported as passing,
# which is the exact failure mode this script exists to prevent.
if ! uv run --project "$DEST" vgi-fixture-worker --help >/dev/null 2>&1; then
    echo "ERROR: vgi-fixture-worker is not runnable from $DEST" >&2
    echo "       The integration suite would SKIP every test and report success." >&2
    exit 1
fi
echo "==> fixture workers ready: uv run --project $DEST vgi-fixture-worker"
