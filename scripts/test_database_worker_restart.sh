#!/usr/bin/env bash
# Prove database-packaged workers survive a complete DuckDB process restart.

set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
duckdb_bin=${VGI_DATABASE_WORKER_DUCKDB:-"${project_dir}/build/release/haybarn"}
python_worker=${VGI_DATABASE_PYTHON_WORKER:?set VGI_DATABASE_PYTHON_WORKER}
bun_worker=${VGI_DATABASE_BUN_WORKER:?set VGI_DATABASE_BUN_WORKER}
rust_worker=${VGI_DATABASE_RUST_WORKER:?set VGI_DATABASE_RUST_WORKER}

for required_file in "${duckdb_bin}" "${python_worker}" "${bun_worker}" "${rust_worker}"; do
	if [[ ! -f "${required_file}" ]]; then
		echo "database-worker restart test input does not exist: ${required_file}" >&2
		exit 1
	fi
done
if [[ ! -x "${duckdb_bin}" ]]; then
	echo "database-worker restart test requires an executable DuckDB CLI: ${duckdb_bin}" >&2
	exit 1
fi

# File paths become SQL string literals below. Doubling quotes is DuckDB's
# portable escaping rule and keeps paths with spaces or apostrophes valid.
sql_escape() {
	local value=$1
	printf '%s' "${value//\'/\'\'}"
}

python_sql=$(sql_escape "${python_worker}")
bun_sql=$(sql_escape "${bun_worker}")
rust_sql=$(sql_escape "${rust_worker}")

runtime_dir=$(mktemp -d "${TMPDIR:-/tmp}/vgi-database-worker-restart.XXXXXX")
registry_db="${runtime_dir}/worker_registry.duckdb"
cache_dir="${runtime_dir}/fresh-reader-cache"
registry_sql=$(sql_escape "${registry_db}")
cache_sql=$(sql_escape "${cache_dir}")
cleanup() {
	rm -rf "${runtime_dir}"
}
trap cleanup EXIT

echo "Phase 1: storing Python, Bun, and Rust workers in ${registry_db}"
"${duckdb_bin}" -bail -batch -no-init -noheader -csv "${registry_db}" <<SQL
LOAD vgi;
CREATE TABLE worker_packages AS
    SELECT * FROM vgi_worker_package(
        '${python_sql}', 'python-fixture', 'restart-1')
    UNION ALL
    SELECT * FROM vgi_worker_package(
        '${bun_sql}', 'open-meteo', 'restart-1')
    UNION ALL
    SELECT * FROM vgi_worker_package(
        '${rust_sql}', 'rust-fixture', 'restart-1',
        entrypoint := 'bin/vgi-rust-worker');
SELECT CASE
    WHEN count(*) = 3
     AND count(DISTINCT worker_name) = 3
     AND bool_and(octet_length(contents) > 1000000)
     AND bool_and(length(sha256) = 64)
    THEN 'registry-ok'
    ELSE error('persistent worker registry was not populated correctly')
END
FROM worker_packages;
CHECKPOINT;
SQL

# Do not leak the build inputs into the reader process through its environment.
unset VGI_DATABASE_PYTHON_WORKER VGI_DATABASE_BUN_WORKER VGI_DATABASE_RUST_WORKER

# This is a distinct DuckDB process. It starts with an empty artifact cache and
# receives only the registry database path: the original package paths above
# are deliberately absent from this invocation.
echo "Phase 2: opening the registry from a fresh DuckDB process and empty cache"
TMPDIR="${runtime_dir}" XDG_CACHE_HOME="${cache_dir}" \
"${duckdb_bin}" -bail -batch -no-init -noheader -csv :memory: <<SQL
LOAD vgi;
SET vgi_worker_cache_dir = '${cache_sql}';
ATTACH '${registry_sql}' AS worker_registry (READ_ONLY);
SELECT CASE WHEN count(*) = 0 THEN 'cache-empty' ELSE error('reader cache was not empty') END
FROM vgi_worker_cache();

ATTACH 'python_package' AS python_worker (
    TYPE vgi,
    LOCATION 'database://worker_registry/main/worker_packages/python-fixture?package_version=restart-1');
SELECT CASE
    WHEN (SELECT count(*) FROM python_worker.main.series(6)) = 6
     AND (SELECT sum(n) FROM python_worker.main.series(6)) = 15
    THEN 'python-ok'
    ELSE error('persisted Python worker returned the wrong result')
END;

ATTACH 'open_meteo' AS meteo_worker (
    TYPE vgi,
    LOCATION 'database://worker_registry/main/worker_packages/open-meteo?package_version=restart-1');
SELECT CASE
    WHEN (SELECT count(*) FROM meteo_worker.main.weather_codes) > 0
    THEN 'bun-ok'
    ELSE error('persisted Bun worker returned no weather codes')
END;

ATTACH 'example' AS rust_worker (
    TYPE vgi,
    LOCATION 'database://worker_registry/main/worker_packages/rust-fixture?package_version=restart-1');
SELECT CASE
    WHEN (SELECT count(*) FROM rust_worker.main.sequence(7)) = 7
     AND (SELECT sum(n) FROM rust_worker.main.sequence(7)) = 21
    THEN 'rust-ok'
    ELSE error('persisted Rust worker returned the wrong result')
END;

SELECT CASE
    WHEN count(*) = 3 THEN 'cache-rebuilt-from-database'
    ELSE error('fresh process did not materialize all three database workers')
END
FROM vgi_worker_cache();
SQL

echo "Database-worker restart test passed"
