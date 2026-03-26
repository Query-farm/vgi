-- Test single-worker function with logging
LOAD vgi;
LOAD json;

-- Enable all logging
CALL enable_logging(level := 'debug');
CALL truncate_duckdb_logs();

SET VARIABLE vgi_worker = '../vgi-python/.venv/bin/vgi-example-worker';

-- Test sequence function (max_workers=1, so no secondary workers)
SELECT 'Testing sequence with 10000 values...' as status;
SELECT COUNT(*) as total, MIN(n) as min_val, MAX(n) as max_val
FROM vgi_table_function(getvariable('vgi_worker'), 'sequence', [10000]);

-- Check all logs
SELECT 'All log entries:' as section;
SELECT * FROM duckdb_logs() LIMIT 20;
