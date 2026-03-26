-- Test partitioned_range with 1 million values to exercise multiple workers
LOAD vgi;
LOAD json;

CALL enable_logging(level := 'debug');
CALL truncate_duckdb_logs();

SET VARIABLE vgi_worker = '../vgi-python/.venv/bin/vgi-example-worker';

-- With 1000000 values and CHUNK_SIZE=1000, we get 1000 chunks for workers to process
SELECT 'Testing partitioned_range with 1000000 values...' as status;
SELECT COUNT(*) as total, COUNT(DISTINCT value) as distinct_count, MIN(value) as min_val, MAX(value) as max_val
FROM vgi_table_function(getvariable('vgi_worker'), 'partitioned_range', [1000000]);

-- Show distinct worker PIDs from batch_received events
SELECT 'Distinct worker PIDs that produced batches:' as section;
SELECT DISTINCT regexp_extract(message, 'worker_pid=(\d+)', 1) as worker_pid
FROM duckdb_logs()
WHERE type = 'VGI' AND message LIKE '%batch_received%'
ORDER BY worker_pid;

-- Count batches per worker
SELECT 'Batches per worker:' as section;
SELECT regexp_extract(message, 'worker_pid=(\d+)', 1) as worker_pid,
       COUNT(*) as batch_count
FROM duckdb_logs()
WHERE type = 'VGI' AND message LIKE '%batch_received%'
GROUP BY worker_pid
ORDER BY worker_pid;
