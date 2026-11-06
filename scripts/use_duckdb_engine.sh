#!/usr/bin/env bash
# Select the DuckDB 2.0 engine the `duckdb` submodule builds VGI against.
#
#   scripts/use_duckdb_engine.sh haybarn   # the committed Haybarn pin (default)
#   scripts/use_duckdb_engine.sh duckdb    # upstream DuckDB v2.0.0
#   scripts/use_duckdb_engine.sh status    # report the current engine
#
# extension_config.cmake picks the matching httpfs fork, and
# src/include/vgi_engine_compat.hpp gates the Haybarn-only engine APIs, by
# whether the engine ships duckdb/haybarn.h. Selecting upstream leaves the
# submodule modified in `git status`: do not commit that change.
#
# The CMake cache records the engine it was configured against, so remove
# build/ (or use a separate checkout/worktree per engine) after switching.
# See docs/upstream-duckdb-build.md.
set -euo pipefail

UPSTREAM_DUCKDB_URL=https://github.com/duckdb/duckdb.git
UPSTREAM_DUCKDB_REF=v2.0.0
UPSTREAM_DUCKDB_COMMIT=d54cbbe3df17e8e5ce68312f483b4155836112aa

cd "$(git rev-parse --show-toplevel)"

engine_of() {
    if [ -f duckdb/src/include/duckdb/haybarn.h ]; then echo haybarn; else echo duckdb; fi
}

case "${1:-status}" in
haybarn)
    git submodule update --init duckdb
    ;;
duckdb)
    [ -e duckdb/.git ] || git submodule update --init duckdb
    if [ "$(git -C duckdb rev-parse HEAD)" != "$UPSTREAM_DUCKDB_COMMIT" ]; then
        git -C duckdb fetch --depth 1 "$UPSTREAM_DUCKDB_URL" "$UPSTREAM_DUCKDB_COMMIT"
        git -C duckdb checkout --detach "$UPSTREAM_DUCKDB_COMMIT"
    fi
    # DuckDB's build derives version() from `git describe`; a shallow fetch has
    # no tags, so pin the release name the extension ABI check expects.
    echo "Build with OVERRIDE_GIT_DESCRIBE=$UPSTREAM_DUCKDB_REF"
    ;;
status) ;;
*)
    echo "usage: $0 {haybarn|duckdb|status}" >&2
    exit 2
    ;;
esac

# Without this check, git -C on an empty submodule directory reports the
# superproject's HEAD.
if [ ! -e duckdb/.git ]; then
    echo "duckdb submodule: not initialised (run: $0 haybarn)"
    exit 0
fi
echo "duckdb submodule: $(engine_of) engine at $(git -C duckdb rev-parse --short=12 HEAD)"
if [ "${1:-status}" != status ]; then
    for cache in build/*/CMakeCache.txt; do
        [ -f "$cache" ] || continue
        echo "note: $(dirname "$cache") was configured before this switch; remove build/ before rebuilding" >&2
    done
fi
