#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Paths ────────────────────────────────────────────────────────────────
EMSDK_DIR=~/Development/wasm-upgrades/emsdk
DUCKDB_WASM_DIR=~/Development/wasm-upgrades/duckdb-wasm
DUCKDB_WASM_ARROW="$DUCKDB_WASM_DIR/build/relsize/coi/third_party/arrow/install"
VCPKG_INSTALLED="$SCRIPT_DIR/vcpkg_installed/wasm32-emscripten-threads"
DEPLOY_DIR="$DUCKDB_WASM_DIR/extensions/v1.5.1/wasm_threads"
BUILD_DIR="$SCRIPT_DIR/build/wasm_threads"

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
  -DUSE_WASM_THREADS=TRUE \
  -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -msimd128 -DWEBDB_SIMD=1 -mbulk-memory -DWEBDB_BULK_MEMORY=1 -pthread -sUSE_PTHREADS=1 -sSHARED_MEMORY=1 -DWEBDB_THREADS=1" \
  -DCMAKE_PREFIX_PATH="$VCPKG_INSTALLED" \
  -DCMAKE_FIND_ROOT_PATH="$VCPKG_INSTALLED" \
  -B"$BUILD_DIR" \
  -S./duckdb \
  -DDUCKDB_EXPLICIT_PLATFORM=wasm_threads \
  -DDUCKDB_CUSTOM_PLATFORM=wasm_threads

# ── Patch spatial extension for GDAL 3.10+ API changes ──────────────────
GDAL_MODULE="$BUILD_DIR/_deps/spatial_extension_fc-src/src/spatial/modules/gdal/gdal_module.cpp"
if [ -f "$GDAL_MODULE" ]; then
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
if [ -f "$SPATIAL_CMAKE" ]; then
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
