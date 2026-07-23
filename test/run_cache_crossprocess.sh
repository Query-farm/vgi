#!/usr/bin/env bash
# Cross-process / cross-restart disk-cache serve: process A spills a result to a
# shared vgi_result_cache_dir and EXITS; a FRESH process B (empty in-memory
# singleton, cold cache) must serve it from disk — proving a blob written by one
# process's incremental streaming writer is discovered + read by another process.
#
# Two separate processes, which sqllogictest (single process) can't do, so this is
# a standalone script. Requires VGI_TEST_WORKER + a haybarn build.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HAYBARN="${VGI_HAYBARN:-$HERE/build/release/haybarn}"
WORKER="${VGI_TEST_WORKER:-uv run --project $HOME/Development/vgi-python vgi-fixture-worker}"
TMP="$(mktemp -d)"
RC="$TMP/xproc"
ROWS="${VGI_XPROC_ROWS:-200000}"
EXPECT_SUM=$(python3 -c "n=$ROWS; print(n*(n-1)//2)")
trap 'rm -rf "$TMP"' EXIT

echo "== cross-process disk-cache serve (rows=$ROWS) =="

# --- Process A: capture + spill to disk, then exit -------------------------
cat > "$TMP/a.sql" <<SQL
SET vgi_result_cache_dir='$RC';
SET vgi_result_cache_disk_max_bytes=4294967296;
SET vgi_result_cache_max_entry_bytes=4096;
ATTACH 'example' AS ex (TYPE vgi, LOCATION '$WORKER');
SELECT COUNT(*) FROM ex.main.cache_parallel($ROWS);
SQL
"$HAYBARN" -unsigned -f "$TMP/a.sql" >/dev/null 2>&1
BLOBS=$(ls "$RC"/*/objects/*.vrc 2>/dev/null | wc -l | tr -d ' ')
echo "process A spilled: $BLOBS blob(s) on disk"
[ "$BLOBS" -ge 1 ] || { echo "FAIL: process A wrote no blob"; exit 1; }

# --- Process B: FRESH process, cold memory, must serve from disk (streaming) --
cat > "$TMP/b.sql" <<SQL
SET vgi_result_cache_dir='$RC';
SET vgi_result_cache_disk_max_bytes=4294967296;
SET vgi_result_cache_max_entry_bytes=1;
SET vgi_result_cache_max_bytes=1;
ATTACH 'example' AS ex (TYPE vgi, LOCATION '$WORKER');
CREATE TEMP TABLE r AS SELECT COUNT(*) c, SUM(v) s FROM ex.main.cache_parallel($ROWS);
COPY (SELECT 'RESULT ' || c || ' ' || s FROM r) TO '$TMP/result.txt' (HEADER false, DELIMITER '~');
SQL
# VGI_STDERR_LOG surfaces the cache decision reliably (duckdb_logs flush timing is
# unreliable across a -f script). The disk-streaming hit line proves B served A's blob.
VGI_STDERR_LOG=1 "$HAYBARN" -unsigned -f "$TMP/b.sql" >/dev/null 2>"$TMP/b.stderr"

RESULT=$(cat "$TMP/result.txt" 2>/dev/null | tr -d '"')
STREAMED=$(grep -c 'result_cache.hit.*tier=disk_streaming' "$TMP/b.stderr" 2>/dev/null || true)
echo "process B: $RESULT ; disk_streaming_hits=$STREAMED"

[ "$RESULT" = "RESULT $ROWS $EXPECT_SUM" ] || { echo "FAIL: result '$RESULT' != 'RESULT $ROWS $EXPECT_SUM'"; exit 1; }
[ "$STREAMED" -ge 1 ] || { echo "FAIL: not served from disk streaming (hits=$STREAMED)"; exit 1; }
echo "PASS: fresh process served the spilled entry from disk (streaming), byte-correct"
