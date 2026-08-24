#!/usr/bin/env bash
# Run every schema-parity check across the VGI repos, in one command.
#
# The protocol's Arrow record shapes are declared once, in vgi-python's
# `VgiProtocol`, and reproduced in seven places: five SDK codegen outputs, a
# handful of hand-written records inside vgi-python itself, and the DuckDB
# client's hand-built request builders. Each reproduction can drift on its own,
# and drift is close to invisible from inside the drifted peer — every SDK is
# self-consistent right up until it talks to another one.
#
# Three layers of check exist, and they catch different things:
#
#   1. Codegen drift    — the checked-in generated headers vs what the
#                         generators emit today. Catches "someone edited a
#                         generated file" and "someone changed the protocol and
#                         didn't regenerate".
#   2. Hand-built wire  — vgi-python records that declare ARROW_SCHEMA by hand
#      records            AND build their row by hand. `from_pylist` silently
#                         drops undeclared keys and nulls omitted columns, so
#                         neither half of a mismatch shows up in the bytes.
#   3. C++ client       — the DuckDB client's hand-built request records vs the
#      requests           generated schemas. This layer existed nowhere until
#                         2026-08, because the generated header did not carry
#                         request schemas at all: a method's params schema is
#                         the outer envelope (`request: binary`) and the record
#                         rides inside as an opaque blob.
#
# Usage:
#   scripts/check_schema_parity.sh              # all three layers
#   scripts/check_schema_parity.sh --cpp-only   # skip the pytest legs
#
# Every leg runs even if an earlier one fails, so one drifted SDK doesn't mask
# the others; the exit status is non-zero if any failed.

set -uo pipefail

VGI_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_CONFIG="${BUILD_CONFIG:-release}"
CPP_ONLY=0

for arg in "$@"; do
    case "$arg" in
        --cpp-only) CPP_ONLY=1 ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done

rc=0
declare -a FAILED=()

run_leg() {
    local name="$1"; shift
    echo
    echo "==================== $name ===================="
    if "$@"; then
        echo "-- $name: OK"
    else
        echo "-- $name: FAILED"
        FAILED+=("$name")
        rc=1
    fi
}

# ---------------------------------------------------------------------------
# Layers 1 and 2 — vgi-python owns the generators and the hand-written records.
# ---------------------------------------------------------------------------
if [ "$CPP_ONLY" -eq 0 ]; then
    if [ -d "$VGI_PYTHON_DIR" ]; then
        run_leg "codegen drift (all SDKs)" \
            uv run --project "$VGI_PYTHON_DIR" pytest -q -p no:cacheprovider \
                "$VGI_PYTHON_DIR/tests/test_generated_cpp_schemas.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_cpp_request_builders.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_cpp_constants.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_cpp_protocol_version.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_cpp_secret.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_go_schemas.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_java_schemas.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_rust_schemas.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_rust_request_builders.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_ts_schemas.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_ts_client.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_protocol_version.py" \
                "$VGI_PYTHON_DIR/tests/test_generated_schemas_cross_lang.py"

        run_leg "hand-built wire records (vgi-python)" \
            uv run --project "$VGI_PYTHON_DIR" pytest -q -p no:cacheprovider \
                "$VGI_PYTHON_DIR/tests/test_wire_record_schema_parity.py"
    else
        echo "!! vgi-python not found at $VGI_PYTHON_DIR — skipping the codegen and"
        echo "   hand-built-record legs. Set VGI_PYTHON_DIR to run them."
        FAILED+=("vgi-python legs SKIPPED (not a pass)")
        rc=1
    fi
fi

# ---------------------------------------------------------------------------
# Layer 3 — the DuckDB client's hand-built request records.
#
# The unit-test target is guarded by BUILD_VGI_UNIT_TESTS, read from the
# environment at CONFIGURE time. If the build dir was configured without it the
# target does not exist, and `cmake --build --target` fails with a message about
# the target rather than about the code — so check the cache and say so plainly.
# ---------------------------------------------------------------------------
run_cpp_parity() {
    local build_dir="$VGI_DIR/build/$BUILD_CONFIG"
    if [ ! -f "$build_dir/CMakeCache.txt" ]; then
        echo "no configured build at $build_dir."
        echo "Build it first:  BUILD_VGI_UNIT_TESTS=1 GEN=ninja make $BUILD_CONFIG"
        return 1
    fi
    if ! grep -q '^BUILD_VGI_UNIT_TESTS' "$build_dir/CMakeCache.txt"; then
        echo "$build_dir was configured without BUILD_VGI_UNIT_TESTS, so the"
        echo "vgi_unit_tests target does not exist there. Reconfigure with:"
        echo "    BUILD_VGI_UNIT_TESTS=1 GEN=ninja make $BUILD_CONFIG"
        return 1
    fi
    cmake --build "$build_dir" --target vgi_unit_tests || return 1
    "$build_dir/extension/vgi/vgi_unit_tests" "[schema-parity]" || return 1
}

run_leg "C++ client request records" run_cpp_parity

echo
echo "==================== summary ===================="
if [ "$rc" -eq 0 ]; then
    echo "all schema-parity checks passed"
else
    echo "FAILED:"
    for f in "${FAILED[@]}"; do
        echo "  - $f"
    done
fi
exit "$rc"
