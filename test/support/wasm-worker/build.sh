#!/usr/bin/env bash
# Build the VGI browser `worker:` module: sabtable's real vgi table-function
# serve entry + the worker-side SAB js-library, linked with emscripten into a
# MODULARIZE Web Worker module (vgi_worker.js + .wasm). The bridge spawns
# vgi-worker-boot.js, which importScripts this module and runs the serve
# dispatcher over DuckDB's delivered SAB channel. See docs + the WASM worker
# transport design.
set -eo pipefail
cd "$(dirname "$0")"
: "${EMSDK_DIR:=/tmp/emsdk}"
# emsdk_env.sh UNSETS EMSDK_DIR on source, so stash it and restore before we use it.
_VGI_EMSDK_DIR="$EMSDK_DIR"
set +u; source "$EMSDK_DIR/emsdk_env.sh" >/dev/null 2>&1; set -u
EMSDK_DIR="$_VGI_EMSDK_DIR"
export PATH="$EMSDK_DIR/upstream/emscripten:$PATH"
SABLIB="${SABLIB:-$(cd ../sabtable && pwd)/target/wasm32-unknown-emscripten/release/libsabtable.a}"
# NB: -Z build-std recompiles compiler_builtins, which needs +atomics,+bulk-memory
# via RUSTFLAGS or wasm-ld rejects --shared-memory ("disallowed by compiler_builtins").
[ -f "$SABLIB" ] || { echo "build sabtable first: (cd ../sabtable && RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-args=-pthread' cargo +nightly build --target wasm32-unknown-emscripten -Z build-std=std,panic_abort --release)"; exit 1; }
# PTHREAD_POOL_SIZE must be >= the channel slot count (EnsureVgiSabChannel = 4) so
# every serve thread gets a pre-spawned pool worker (pthread_create-after-dlopen is
# flaky, so pre-spawn the full set). --pre-js injects DuckDB's SAB into each pthread
# realm; PThread must be exported so the boot can hook getNewWorker.
emcc vgi_worker_main.c "$SABLIB" \
  --js-library vgi_worker_lib.js \
  --pre-js vgi_worker_pre.js \
  -sMODULARIZE=1 -sEXPORT_NAME=VgiWorker \
  -pthread -sPTHREAD_POOL_SIZE=4 -sSHARED_MEMORY=1 \
  -fwasm-exceptions \
  -sENVIRONMENT=web,worker \
  -sEXPORTED_FUNCTIONS=_main,_vgi_rust_serve_table_sab_slot,_vgi_worker_serve_pool,_malloc,_free \
  -sEXPORTED_RUNTIME_METHODS=HEAPU8,PThread \
  -sEXIT_RUNTIME=0 -sALLOW_MEMORY_GROWTH=1 \
  -o vgi_worker.js
echo "built vgi_worker.js + vgi_worker.wasm"
