#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Flags ────────────────────────────────────────────────────────────────
# Spatial is disabled by default; pass --with-spatial to include it.
WITH_SPATIAL=0
for arg in "$@"; do
  case "$arg" in
    --with-spatial) WITH_SPATIAL=1 ;;
    --no-spatial)   WITH_SPATIAL=0 ;;
    -h|--help)
      echo "Usage: $0 [--with-spatial|--no-spatial]"
      echo "  --with-spatial  Build the spatial extension (default: off)"
      echo "  --no-spatial    Skip the spatial extension (default)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

# ── Paths ────────────────────────────────────────────────────────────────
EMSDK_DIR=~/Development/wasm-upgrades/emsdk
DUCKDB_WASM_DIR=~/Development/wasm-upgrades/duckdb-wasm
DUCKDB_WASM_ARROW="$DUCKDB_WASM_DIR/build/relperf/coi/third_party/arrow/install"
VCPKG_INSTALLED="$SCRIPT_DIR/vcpkg_installed/wasm32-emscripten-threads"
DEPLOY_DIR="$DUCKDB_WASM_DIR/extensions/v1.5.1/wasm_threads"
BUILD_DIR="$SCRIPT_DIR/build/wasm_threads"

# ── Activate emsdk ───────────────────────────────────────────────────────
EMSDK_QUIET=1 source "$EMSDK_DIR/emsdk_env.sh"

# Locate Emscripten's CMake toolchain so vcpkg can chainload to it for the
# user project (vcpkg's wasm32-emscripten-threads triplet only chainloads for
# port builds — without VCPKG_CHAINLOAD_TOOLCHAIN_FILE the duckdb project
# itself falls back to the host /usr/bin/c++).
: "${EMSCRIPTEN_ROOT:=${EMSDK:-$EMSDK_DIR}/upstream/emscripten}"
EMSCRIPTEN_TOOLCHAIN="$EMSCRIPTEN_ROOT/cmake/Modules/Platform/Emscripten.cmake"
if [ ! -f "$EMSCRIPTEN_TOOLCHAIN" ]; then
  echo "ERROR: Emscripten.cmake not found at $EMSCRIPTEN_TOOLCHAIN" >&2
  exit 1
fi
export EMSCRIPTEN_ROOT

# ── Clean & prepare build directory ──────────────────────────────────────
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Symlink vcpkg_installed into build dir so spatial extension's relative
# glob path (../../vcpkg_installed/...) resolves correctly at link time
ln -s ../../vcpkg_installed "$BUILD_DIR/vcpkg_installed"

# Merge vcpkg deps from every loaded extension. Each fetched extension
# (httpfs→openssl+curl, ducklake→roaring, vgi→arrow+openssl, …) declares
# its own vcpkg.json; cmake's EXTENSION_CONFIG_BUILD pass runs
# merge_vcpkg_deps.py and writes the combined manifest. Arrow is satisfied
# at vcpkg-port level via our overlay at vcpkg_overlay_ports/arrow.
#
# We invoke cmake directly (rather than `make extension_configuration_wasm`)
# because that target tail-calls `emmake make` on what was generated as a
# Ninja build, which fails harmlessly after the merge has already written
# the manifest.
echo "Merging vcpkg manifests (cmake EXTENSION_CONFIG_BUILD pass)..."
rm -rf "$SCRIPT_DIR/build/extension_configuration" "$SCRIPT_DIR/duckdb/build/extension_configuration"
mkdir -p "$SCRIPT_DIR/build/extension_configuration" "$SCRIPT_DIR/duckdb/build/extension_configuration"
emcmake cmake \
  -DDUCKDB_EXTENSION_CONFIGS="$SCRIPT_DIR/extension_config_wasm.cmake" \
  -DEXTENSION_CONFIG_BUILD=TRUE \
  -B"$SCRIPT_DIR/build/extension_configuration" \
  -S"$SCRIPT_DIR/duckdb"
# merge_vcpkg_deps.py runs as a build-time custom_target. Use cmake --build
# so we don't care whether the generator is ninja or make. The merge writes
# to <duckdb>/build/extension_configuration/vcpkg.json (relative to its
# WORKING_DIRECTORY = PROJECT_SOURCE_DIR = the duckdb submodule).
cmake --build "$SCRIPT_DIR/build/extension_configuration" --target duckdb_merge_vcpkg_manifests
cp "$SCRIPT_DIR/duckdb/build/extension_configuration/vcpkg.json" \
   "$SCRIPT_DIR/build/extension_configuration/vcpkg.json"
WASM_MANIFEST_DIR="$SCRIPT_DIR/build/extension_configuration"

# Patch cloned httpfs source: make find_package(OpenSSL/CURL) conditional on
# NOT EMSCRIPTEN. Upstream's httpfs/CMakeLists.txt only links these on
# non-EMSCRIPTEN; under EMSCRIPTEN it links duckdb_mbedtls instead. The
# unconditional REQUIRED find_package is a configure-time wart — upstream's
# shipped 356 KB wasm httpfs.duckdb_extension.wasm contains no openssl/curl
# symbols, confirming neither is actually used. Skipping find_package avoids
# building openssl-from-source for wasm32-emscripten-threads (~10 min).
# httpfs has its own pinned duckdb submodule (8a585197...). FetchContent
# leaves it empty, but the build can still consult httpfs/duckdb/ for ancillary
# files (scripts, append_metadata.cmake, third_party paths) — and any mismatch
# vs the duckdb that built duckdb-coi.wasm shows up at runtime as
# "function signature mismatch" inside HTTP code paths. Force httpfs/duckdb to
# be the same tree (and same patches) we built duckdb-coi.wasm from.
HTTPFS_FC_SRC="$SCRIPT_DIR/build/extension_configuration/_deps/httpfs_extension_fc-src"
DUCKDB_WASM_DUCKDB="$DUCKDB_WASM_DIR/submodules/duckdb"
if [ -d "$HTTPFS_FC_SRC" ] && [ -d "$DUCKDB_WASM_DUCKDB" ]; then
  if [ -L "$HTTPFS_FC_SRC/duckdb" ] || [ -d "$HTTPFS_FC_SRC/duckdb" ]; then
    rm -rf "$HTTPFS_FC_SRC/duckdb"
  fi
  ln -s "$DUCKDB_WASM_DUCKDB" "$HTTPFS_FC_SRC/duckdb"
  echo "Linked httpfs/duckdb -> $DUCKDB_WASM_DUCKDB (matches the duckdb that built duckdb-coi.wasm)"
fi

HTTPFS_CMAKE="$SCRIPT_DIR/build/extension_configuration/_deps/httpfs_extension_fc-src/CMakeLists.txt"
if [ -f "$HTTPFS_CMAKE" ]; then
  python3 - "$HTTPFS_CMAKE" <<'PYEOF'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
s = p.read_text()
old = """find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${CURL_INCLUDE_DIRS})"""
new = """if(NOT EMSCRIPTEN)
  find_package(OpenSSL REQUIRED)
  find_package(CURL REQUIRED)
  include_directories(${OPENSSL_INCLUDE_DIR})
  include_directories(${CURL_INCLUDE_DIRS})
endif()"""
if old in s:
    p.write_text(s.replace(old, new))
    print("Patched httpfs CMakeLists.txt: find_package(OpenSSL/CURL) now guarded by NOT EMSCRIPTEN")
elif new in s:
    print("httpfs CMakeLists.txt already patched")
else:
    print("WARNING: httpfs CMakeLists.txt patch target not found — has upstream changed?")
PYEOF
fi

# Also patch src/CMakeLists.txt: crypto.cpp is in the base HTTPFS_SOURCES list
# *before* the EMSCRIPTEN branch, so it builds on every platform. crypto.cpp
# uses openssl headers — should only build on non-EMSCRIPTEN, where the
# NOT-EMSCRIPTEN branch already adds it (so removing from base is a no-op for
# native builds and a fix for wasm builds).
HTTPFS_SRC_CMAKE="$SCRIPT_DIR/build/extension_configuration/_deps/httpfs_extension_fc-src/src/CMakeLists.txt"
if [ -f "$HTTPFS_SRC_CMAKE" ]; then
  python3 - "$HTTPFS_SRC_CMAKE" <<'PYEOF'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
s = p.read_text()
old = """set(HTTPFS_SOURCES
    hffs.cpp
    s3fs.cpp
    httpfs.cpp
    http_state.cpp
    crypto.cpp
    hash_functions.cpp
    create_secret_functions.cpp
    httpfs_extension.cpp
    s3_multi_part_upload.cpp)"""
new = """set(HTTPFS_SOURCES
    hffs.cpp
    s3fs.cpp
    httpfs.cpp
    http_state.cpp
    hash_functions.cpp
    create_secret_functions.cpp
    httpfs_extension.cpp
    s3_multi_part_upload.cpp)"""
if old in s:
    p.write_text(s.replace(old, new))
    print("Patched httpfs src/CMakeLists.txt: removed crypto.cpp from base HTTPFS_SOURCES (now only in NOT EMSCRIPTEN branch)")
elif new in s:
    print("httpfs src/CMakeLists.txt already patched")
else:
    print("WARNING: httpfs src/CMakeLists.txt patch target not found — has upstream changed?")
PYEOF
fi

# Strip openssl/curl from the merged vcpkg manifest: httpfs's vcpkg.json
# declares them, but under EMSCRIPTEN they are unused (see patch above).
python3 - "$WASM_MANIFEST_DIR/vcpkg.json" <<'PYEOF'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
data = json.loads(p.read_text())
before = [d if isinstance(d, str) else d.get("name") for d in data["dependencies"]]
data["dependencies"] = [
    d for d in data["dependencies"]
    if (d if isinstance(d, str) else d.get("name")) not in ("openssl", "curl")
]
after = [d if isinstance(d, str) else d.get("name") for d in data["dependencies"]]
p.write_text(json.dumps(data, indent=4))
print(f"vcpkg deps: {before} -> {after}")
PYEOF

# ── Configure ────────────────────────────────────────────────────────────
EXTENSION_CONFIGS="$SCRIPT_DIR/extension_config_wasm.cmake"
if [ "$WITH_SPATIAL" = "1" ]; then
  EXTENSION_CONFIGS="$EXTENSION_CONFIGS;$SCRIPT_DIR/extension_config_wasm_spatial.cmake"
  echo "Spatial extension: ENABLED"
else
  echo "Spatial extension: DISABLED (pass --with-spatial to enable)"
fi

emcmake cmake \
  -DDUCKDB_EXTENSION_CONFIGS="$EXTENSION_CONFIGS" \
  -DFETCHCONTENT_SOURCE_DIR_HTTPFS_EXTENSION_FC="$SCRIPT_DIR/build/extension_configuration/_deps/httpfs_extension_fc-src" \
  -DWASM_LOADABLE_EXTENSIONS=1 \
  -DBUILD_EXTENSIONS_ONLY=1 \
  -DEXTENSION_STATIC_BUILD=1 \
  -DDUCKDB_WASM_ARROW_PREFIX="$DUCKDB_WASM_ARROW" \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DUSE_WASM_THREADS=TRUE \
  -DCMAKE_CXX_STANDARD=17 \
  -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -msimd128 -DWEBDB_SIMD=1 -mbulk-memory -DWEBDB_BULK_MEMORY=1 -pthread -sUSE_PTHREADS=1 -sSHARED_MEMORY=1 -DWEBDB_THREADS=1 -DEMSCRIPTEN" \
  -DCMAKE_PREFIX_PATH="$VCPKG_INSTALLED" \
  -DCMAKE_FIND_ROOT_PATH="$VCPKG_INSTALLED" \
  -DVCPKG_BUILD=1 \
  -DCMAKE_TOOLCHAIN_FILE="$SCRIPT_DIR/vcpkg/scripts/buildsystems/vcpkg.cmake" \
  -DVCPKG_TARGET_TRIPLET=wasm32-emscripten-threads \
  -DVCPKG_MANIFEST_DIR="$WASM_MANIFEST_DIR" \
  -DVCPKG_OVERLAY_TRIPLETS="$SCRIPT_DIR/extension-ci-tools/toolchains" \
  -DVCPKG_OVERLAY_PORTS="$SCRIPT_DIR/extension-ci-tools/vcpkg_ports;$SCRIPT_DIR/vcpkg_overlay_ports" \
  -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE="$EMSCRIPTEN_TOOLCHAIN" \
  -B"$BUILD_DIR" \
  -S./duckdb \
  -DDUCKDB_EXPLICIT_PLATFORM=wasm_threads \
  -DDUCKDB_CUSTOM_PLATFORM=wasm_threads

# ── Patch spatial extension for GDAL 3.10+ API changes ──────────────────
GDAL_MODULE="$BUILD_DIR/_deps/spatial_extension_fc-src/src/spatial/modules/gdal/gdal_module.cpp"
if [ "$WITH_SPATIAL" = "1" ] && [ -f "$GDAL_MODULE" ]; then
  sed -i '' \
    -e 's/VSIVirtualHandle \*Open(const char \*gdal_file_path, const char \*access, bool set_error,/VSIVirtualHandleUniquePtr Open(const char *gdal_file_path, const char *access, bool set_error,/' \
    -e 's/return new DuckDBFileHandle(std::move(file));/return VSIVirtualHandleUniquePtr(new DuckDBFileHandle(std::move(file)));/' \
    -e 's/bool IsLocal(const char \*gdal_file_path) override/bool IsLocal(const char *gdal_file_path) const override/' \
    -e 's/int Rename(const char \*oldpath, const char \*newpath) override/int Rename(const char *oldpath, const char *newpath, GDALProgressFunc \/*pProgressFunc*\/, void * \/*pProgressData*\/) override/' \
    "$GDAL_MODULE"
  # Add Error() and ClearErr() overrides (pure virtual in GDAL 3.10+)
  sed -i '' 's/int Eof() override {/int Error() override { return 0; } void ClearErr() override {} int Eof() override {/' "$GDAL_MODULE"
  echo "Patched spatial extension for GDAL 3.10+ compatibility"

  # Replace memvfs approach with MEMFS file write for PROJ database.
  # memvfs uses sqlite3 VFS function pointer callbacks that don't work correctly
  # across MAIN_MODULE/SIDE_MODULE boundaries with shared memory (pthreads).
  # Instead, write the embedded proj.db to Emscripten's virtual filesystem.
  PROJ_MODULE="$BUILD_DIR/_deps/spatial_extension_fc-src/src/spatial/modules/proj/proj_module.cpp"
  if [ -f "$PROJ_MODULE" ]; then
    python3 << PYEOF
path = "$PROJ_MODULE"
with open(path, "r") as f:
    content = f.read()

# Replace the memvfs init block with MEMFS file write
old = """		sqlite3_initialize();
		sqlite3_memvfs_init(nullptr, nullptr, nullptr);
		const auto vfs = sqlite3_vfs_find("memvfs");
		if (!vfs) {
			throw InternalException("Could not find sqlite memvfs extension");
		}
		sqlite3_vfs_register(vfs, 0);

		// We set the default context proj.db path to the one in the binary here
		// Otherwise GDAL will try to load the proj.db from the system
		// Any PJ_CONTEXT we create after this will inherit these settings (on this thread?)
		auto path = StringUtil::Format("file:/proj.db?immutable=1&ptr=%llu&sz=%lu&max=%lu",
		                               static_cast<void *>(proj_db), proj_db_len, proj_db_len);

		proj_context_set_sqlite3_vfs_name(nullptr, "memvfs");

		// Try to open the database
		sqlite3 *sdb = nullptr;
		const auto sok = sqlite3_open_v2(path.c_str(), &sdb, SQLITE_OPEN_READONLY, "memvfs");
		if (sok != SQLITE_OK) {
			throw InternalException("Could not open sqlite3 memvfs database");
		}

		const auto ok = proj_context_set_database_path(nullptr, path.c_str(), nullptr, nullptr);
		if (!ok) {
			throw InternalException("Could not set proj.db path");
		}"""

new = """		// Write embedded proj.db to Emscripten's virtual filesystem (MEMFS).
		// memvfs VFS callbacks don't work across MAIN_MODULE/SIDE_MODULE with shared memory.
		{
			FILE *check = fopen("/tmp/proj.db", "rb");
			if (!check) {
				auto *f = fopen("/tmp/proj.db", "wb");
				if (!f) {
					throw InternalException("Could not write proj.db to virtual filesystem");
				}
				fwrite(proj_db, 1, proj_db_len, f);
				fclose(f);
			} else {
				fclose(check);
			}
		}

		const auto ok = proj_context_set_database_path(nullptr, "/tmp/proj.db", nullptr, nullptr);
		if (!ok) {
			throw InternalException("Could not set proj.db path");
		}"""

if old in content:
    content = content.replace(old, new)
else:
    print("WARNING: memvfs init block not found (may already be patched)")

# Also patch GetThreadProjContext to use /tmp/proj.db instead of memvfs
old2 = """	auto path = StringUtil::Format("file:/proj.db?immutable=1&ptr=%llu&sz=%lu&max=%lu", static_cast<void *>(proj_db),
	                               proj_db_len, proj_db_len);

	proj_context_set_sqlite3_vfs_name(ctx, "memvfs");
	const auto ok = proj_context_set_database_path(ctx, path.c_str(), nullptr, nullptr);"""

new2 = """	const auto ok = proj_context_set_database_path(ctx, "/tmp/proj.db", nullptr, nullptr);"""

if old2 in content:
    content = content.replace(old2, new2)

with open(path, "w") as f:
    f.write(content)
print("Patched proj_module.cpp: memvfs replaced with MEMFS file write")
PYEOF
  fi

  # Regenerate proj_db.c from the vcpkg PROJ database to match the installed PROJ version
  PROJ_DB_SRC="$BUILD_DIR/_deps/spatial_extension_fc-src/src/spatial/modules/proj/proj_db.c"
  PROJ_DB_FILE="$VCPKG_INSTALLED/share/proj/proj.db"
  if [ -f "$PROJ_DB_SRC" ] && [ -f "$PROJ_DB_FILE" ]; then
    xxd -i "$PROJ_DB_FILE" | \
      sed 's/unsigned char .*/unsigned char proj_db[] = {/;s/unsigned int .*/unsigned int proj_db_len = '"$(wc -c < "$PROJ_DB_FILE" | tr -d ' ')"';/' \
      > "$PROJ_DB_SRC"
    echo "Regenerated proj_db.c from vcpkg PROJ $(wc -c < "$PROJ_DB_FILE" | tr -d ' ') bytes"
  fi
fi

# Fix spatial extension's vcpkg libs: use threads triplet, exclude libcrypto/libssl/libcurl
SPATIAL_CMAKE="$BUILD_DIR/_deps/spatial_extension_fc-src/CMakeLists.txt"
if [ "$WITH_SPATIAL" = "1" ] && [ -f "$SPATIAL_CMAKE" ]; then
  sed -i '' 's|"../../vcpkg_installed/wasm32-emscripten/lib/lib\*.a"|"../../vcpkg_installed/wasm32-emscripten-threads/lib/libgdal.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libproj.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libgeos.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libgeos_c.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libgeotiff.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libtiff.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libjpeg.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libjson-c.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/liblzma.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libexpat.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libz.a" "../../vcpkg_installed/wasm32-emscripten-threads/lib/libsqlite3.a"|' "$SPATIAL_CMAKE"
  echo "Patched spatial vcpkg libs (explicit list, threads triplet)"
fi

# ── Build ────────────────────────────────────────────────────────────────
emmake make -C"$BUILD_DIR" -j8

# ── Deploy ───────────────────────────────────────────────────────────────
mkdir -p "$DEPLOY_DIR"
REPO_VERSION_DIR=$(ls -d "$BUILD_DIR"/repository/*/wasm_threads 2>/dev/null | head -n1)
if [ -z "$REPO_VERSION_DIR" ] || ! compgen -G "$REPO_VERSION_DIR/*.duckdb_extension.wasm" > /dev/null; then
  echo "ERROR: no built wasm extensions found under $BUILD_DIR/repository/*/wasm_threads" >&2
  exit 1
fi
cp "$REPO_VERSION_DIR"/*.duckdb_extension.wasm "$DEPLOY_DIR/"

echo ""
echo "Deployed to $DEPLOY_DIR:"
ls -lh "$DEPLOY_DIR"/*.duckdb_extension.wasm
