-- Test script for multi-worker partitioned_range function
-- Run with: ./build/debug/duckdb < test_multiworker.sql

LOAD vgi;
LOAD json;

-- Enable VGI logging
CALL enable_logging('vgi');

-- Clear any existing logs
CALL truncate_duckdb_logs();

-- Set the worker path (adjust as needed)
SET VARIABLE vgi_worker = '../vgi-python/.venv/bin/vgi-example-worker';

-- Test partitioned_range with a large range (100k values)
-- This should produce multiple batches and potentially multiple workers
SELECT 'Running partitioned_range with 100000 values...' as status;

SELECT COUNT(*) as total_count,
       COUNT(DISTINCT value) as distinct_count,
       MIN(value) as min_val,
       MAX(value) as max_val
FROM vgi_table_function(getvariable('vgi_worker'), 'partitioned_range', [100000]);

-- Show all log entries
SELECT '=== All VGI log entries ===' as section;
SELECT timestamp, type, message
FROM duckdb_logs()
WHERE starts_with(type, 'table_function.')
ORDER BY timestamp;

-- Show batch_received events with parsed worker_pid
SELECT '=== Batch received events with worker PIDs ===' as section;
SELECT timestamp,
       (message::JSON)->>'worker_pid' as worker_pid,
       (message::JSON)->>'batch_rows' as batch_rows,
       (message::JSON)->>'function_name' as function_name
FROM duckdb_logs()
WHERE type = 'table_function.batch_received'
ORDER BY timestamp;

-- Count distinct worker PIDs
SELECT '=== Worker PID summary ===' as section;
SELECT (message::JSON)->>'worker_pid' as worker_pid,
       COUNT(*) as batch_count,
       SUM(((message::JSON)->>'batch_rows')::INTEGER) as total_rows
FROM duckdb_logs()
WHERE type = 'table_function.batch_received'
GROUP BY worker_pid
ORDER BY worker_pid;

-- Show init_local events to see primary vs secondary workers
SELECT '=== Init local events (primary vs secondary workers) ===' as section;
SELECT timestamp,
       (message::JSON)->>'worker_pid' as worker_pid,
       (message::JSON)->>'worker_type' as worker_type,
       (message::JSON)->>'global_execution_id' as global_execution_id
FROM duckdb_logs()
WHERE type = 'table_function.init_local'
ORDER BY timestamp;

-- Final summary
SELECT '=== Summary ===' as section;
SELECT
    (SELECT COUNT(DISTINCT (message::JSON)->>'worker_pid') FROM duckdb_logs() WHERE type = 'table_function.batch_received') as unique_worker_pids,
    (SELECT COUNT(*) FROM duckdb_logs() WHERE type = 'table_function.batch_received') as total_batches,
    (SELECT COUNT(*) FROM duckdb_logs() WHERE type = 'table_function.init_local') as init_local_calls;
