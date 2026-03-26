-- Test worker pool behavior with catalog API
ATTACH 'example' AS vgi (TYPE vgi, LOCATION '../vgi-python/.venv/bin/vgi-example-worker');

-- First call - should spawn a worker
SHOW ALL TABLES;

-- Check pool status
SELECT * FROM vgi_worker_pool();

-- Second call - should reuse the pooled worker
SHOW ALL TABLES;

-- Check pool status again
SELECT * FROM vgi_worker_pool();
