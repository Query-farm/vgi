-- Simple test - single worker function
LOAD vgi;

SET VARIABLE vgi_worker = '../vgi-python/.venv/bin/vgi-example-worker';

-- Test with simple sequence function (single worker)
SELECT 'Testing sequence function (single worker)...' as status;
SELECT COUNT(*) FROM vgi_table_function(getvariable('vgi_worker'), 'sequence', [100]);
