#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Paths ────────────────────────────────────────────────────────────────
EMSDK_DIR=~/Development/wasm-upgrades/emsdk
DUCKDB_WASM_DIR=~/Development/wasm-upgrades/duckdb-wasm
DUCKDB_WASM_ARROW="$DUCKDB_WASM_DIR/build/relsize/eh-loadable/third_party/arrow/install"
VCPKG_INSTALLED="$SCRIPT_DIR/vcpkg_installed/wasm32-emscripten"
DEPLOY_DIR="$DUCKDB_WASM_DIR/extensions/v1.5.1/wasm_eh"
BUILD_DIR="$SCRIPT_DIR/build/wasm_eh"

# ── Activate emsdk ───────────────────────────────────────────────────────
EMSDK_QUIET=1 source "$EMSDK_DIR/emsdk_env.sh"

# ── Clean & prepare build directory ──────────────────────────────────────
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Symlink vcpkg_installed into build dir so spatial extension's relative
# glob path (../../vcpkg_installed/...) resolves correctly at link time
ln -s ../../vcpkg_installed "$BUILD_DIR/vcpkg_installed"

# ── Configure ────────────────────────────────────────────────────────────
emcmake cmake \
  -DDUCKDB_EXTENSION_CONFIGS="$SCRIPT_DIR/extension_config_wasm.cmake" \
  -DWASM_LOADABLE_EXTENSIONS=1 \
  -DBUILD_EXTENSIONS_ONLY=1 \
  -DEXTENSION_STATIC_BUILD=1 \
  -DDUCKDB_WASM_ARROW_PREFIX="$DUCKDB_WASM_ARROW" \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1" \
  -DCMAKE_PREFIX_PATH="$VCPKG_INSTALLED" \
  -DCMAKE_FIND_ROOT_PATH="$VCPKG_INSTALLED" \
  -B"$BUILD_DIR" \
  -S./duckdb \
  -DDUCKDB_EXPLICIT_PLATFORM=wasm_eh \
  -DDUCKDB_CUSTOM_PLATFORM=wasm_eh

# ── Build ────────────────────────────────────────────────────────────────
emmake make -C"$BUILD_DIR" -j8

# ── Deploy ───────────────────────────────────────────────────────────────
mkdir -p "$DEPLOY_DIR"
cp "$BUILD_DIR"/repository/v1.5.1/wasm_eh/*.duckdb_extension.wasm "$DEPLOY_DIR/"

echo ""
echo "Deployed to $DEPLOY_DIR:"
ls -lh "$DEPLOY_DIR"/*.duckdb_extension.wasm
