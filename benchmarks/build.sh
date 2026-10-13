#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
build=${VGI_BENCH_BUILD_DIR:-$root/build/benchmark}
if [[ $(uname -s) != Linux ]]; then
    echo 'Build and run benchmarks on Linux (use the dedicated EC2 workspace).' >&2
    exit 1
fi
# Keep the configured CMake version when revisiting a build directory. Different
# CMake versions can reorder compiler arguments and invalidate every object.
vgi_cmake=cmake
if [[ -f "$build/CMakeCache.txt" ]]; then
    while IFS= read -r line; do
        case "$line" in
            CMAKE_COMMAND:INTERNAL=*)
                if [[ -x "${line#*=}" ]]; then vgi_cmake="${line#*=}"; fi
                ;;
        esac
    done < "$build/CMakeCache.txt"
fi
# Supply CMAKE_PREFIX_PATH for preinstalled Arrow/OpenSSL, or a vcpkg toolchain
# with -DCMAKE_TOOLCHAIN_FILE=... in the arguments. Keep -O3/NDEBUG production
# optimization; add symbols/frame pointers to both engine and extension.
"$vgi_cmake" -S "$root/duckdb" -B "$build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS='-g1 -fno-omit-frame-pointer' \
    -DCMAKE_C_FLAGS='-g1 -fno-omit-frame-pointer' -DDISABLE_UNITY=OFF \
    -DBUILD_UNITTESTS=OFF -DBUILD_SHELL=ON -DBUILD_VGI_BENCHMARKS=ON \
    -DVGI_BENCH_ARROW_PROBE=OFF \
    -DENABLE_EXTENSION_AUTOLOADING=OFF -DENABLE_EXTENSION_AUTOINSTALL=OFF \
    -DDUCKDB_EXTENSION_CONFIGS="$root/benchmarks/extension_config.cmake" "$@"
"$vgi_cmake" --build "$build" --target vgi_bench_driver vgi_loadable_extension shell -j "${JOBS:-8}"
mkdir -p "$build/benchmark-metadata"
cp "$build/CMakeCache.txt" "$build/benchmark-metadata/"
"${CXX:-c++}" --version > "$build/benchmark-metadata/compiler.txt"
