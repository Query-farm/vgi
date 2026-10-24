"""Function-family memory scenarios. SQL/checksums are saved with each trial.

Fixtures use the existing example catalog; no production worker changes.
Native controls have equivalent scalar results, not necessarily identical plans.
"""
from workloads import sql_literal


def cases(quick=False):
    result = []

    def add(name, family, sql, expected, **kw):
        result.append(dict(name=name, family=family, sql=sql, expected=str(expected), **kw))

    def strings(n, width=65536, batch=64, cols=1):
        return f"bench_strings({n}, {cols}, {width}, {batch}, 0)"

    # Current-row frames keep total result size linear, isolating partition
    # shipment from the inherently quadratic output of unbounded listagg.
    for n in ([32, 64] if quick else [128, 512, 2048]):
        for native in (False, True):
            fn = "first_value" if native else "bench.main.vgi_window_listagg"
            add(f"window_strings_{n}_{'native' if native else 'vgi'}", "window",
                f"SELECT sum(length(v)) FROM (SELECT {fn}(c0) OVER (ORDER BY c0 ROWS BETWEEN CURRENT ROW AND CURRENT ROW) v FROM {strings(n)})",
                n * 65536, native=native, rows=n, value_bytes=65536)
    n = 64 if quick else 2048
    add("window_strings_partitioned", "window",
        f"SELECT sum(length(v)) FROM (SELECT bench.main.vgi_window_listagg(c0) OVER (PARTITION BY CAST(substr(c0,65513,20) AS BIGINT) % 16 ORDER BY c0 ROWS BETWEEN CURRENT ROW AND CURRENT ROW) v FROM {strings(n)})",
        n * 65536, rows=n, partitions=16, value_bytes=65536, threads=4)
    for n in ([64] if quick else [10000, 1000000]):
        for native in (False, True):
            fn = "sum" if native else "bench.main.vgi_streaming_sum"
            add(f"streaming_window_{n}_{'native' if native else 'vgi'}", "streaming_window",
                f"SELECT sum(v) FROM (SELECT {fn}(i) OVER (ORDER BY i) v FROM range({n}) t(i))",
                n*(n-1)*(n+1)//6, native=native, rows=n)
    n = 512 if quick else 100000
    for groups in ([1, 32] if quick else [1, 100, 10000]):
        for native in (False, True):
            fn = "sum" if native else "bench.main.vgi_sum"
            add(f"groups_{groups}_{'native' if native else 'vgi'}", "aggregate",
                f"SELECT sum(v) FROM (SELECT {fn}(i) v FROM range({n}) t(i) GROUP BY i % {groups})",
                n * (n - 1) // 2, native=native, rows=n, groups=groups)
    n = 512 if quick else 1000000
    for native in (False, True):
        fn = "sum" if native else "bench.main.vgi_sum"
        add(f"groups_parallel_{'native' if native else 'vgi'}", "aggregate",
            f"SELECT sum(v) FROM (SELECT {fn}(i) v FROM input_data GROUP BY i % 1000)",
            n * (n - 1) // 2, native=native, rows=n, groups=1000, threads=4,
            setup=[f"CREATE TABLE input_data AS SELECT i FROM range({n}) t(i)"], cleanup=["DROP TABLE input_data"])
    for n in ([64] if quick else [4096, 16384]):
        for groups in (1, 32):
            add(f"listagg_{n}_g{groups}", "aggregate",
                f"SELECT sum(length(v)) FROM (SELECT bench.main.vgi_listagg(c0) v FROM {strings(n,4096)} GROUP BY CAST(substr(c0,4073,20) AS BIGINT) % {groups})",
                n * 4096 + n - min(n, groups), rows=n, value_bytes=4096, groups=groups)
        add(f"listagg_{n}_native", "aggregate",
            f"SELECT length(string_agg(c0,',')) FROM {strings(n,4096)}", n*4096+n-1,
            native=True, rows=n, value_bytes=4096, groups=1)
        add(f"listagg_append_{n}", "aggregate_append",
            f"SELECT length(bench.main.vgi_window_listagg(c0)) FROM {strings(n,4096)}", n*4096+n-1,
            rows=n, value_bytes=4096, groups=1)
    n = 128 if quick else 4096
    for batch in (64, 2048):
        for native in (False, True):
            fn = "upper" if native else "bench.upper_case"
            add(f"scalar_strings_b{batch}_{'native' if native else 'vgi'}", "scalar",
                f"SELECT sum(length(v)) + count(*) FILTER (WHERE starts_with(v, 'X')) FROM (SELECT {fn}(c0) v FROM {strings(n,batch=batch)})",
                n * 65537, native=native, rows=n, batch_rows=batch, value_bytes=65536)
    add("scalar_unique_dedup_off", "scalar_dedup",
        f"SELECT sum(length(v)) + count(*) FILTER (WHERE starts_with(v,'X')) FROM (SELECT bench.upper_case(c0) v FROM {strings(n,batch=2048)})",
        n*65537, rows=n, value_bytes=65536, batch_rows=2048,
        settings=["SET vgi_exchange_input_dedup=false"])
    add("scalar_unique_dedup_on", "scalar_dedup",
        f"SELECT sum(length(v)) + count(*) FILTER (WHERE starts_with(v,'X')) FROM (SELECT bench.upper_case(c0) v FROM {strings(n,batch=2048)})",
        n*65537, rows=n, value_bytes=65536, batch_rows=2048,
        settings=["SET vgi_exchange_input_dedup=true"])
    for enabled in (True,False):
        add(f"scalar_repeated_dedup_{'on' if enabled else 'off'}", "scalar_dedup",
            f"SELECT sum(length(v)) + count(*) FILTER (WHERE starts_with(v,'X')) FROM (SELECT bench.upper_case(repeat('x',65512)||lpad((i%64)::VARCHAR,24,'0')) v FROM range({n}) t(i))",
            n*65537, rows=n, distinct=64, value_bytes=65536,
            settings=[f"SET vgi_exchange_input_dedup={'true' if enabled else 'false'}"])
    n = 128 if quick else 10000
    add("scalar_wide", "scalar",
        "SELECT " + " + ".join(f"sum(length(bench.upper_case(c{i})))" for i in range(32)) + f" FROM {strings(n,256,128,32)}",
        n * 32 * 256, rows=n, columns=32, value_bytes=256)
    n = 128 if quick else 4096
    for columns in (1, 4, 16, 32):
        add(f"scalar_connections_{columns}", "scalar_connections",
            "SELECT " + " + ".join(f"sum(length(bench.upper_case(c{i})))" for i in range(columns)) + f" FROM {strings(n,256,128,columns)}",
            n*columns*256, rows=n, columns=columns, value_bytes=256)
    add("scalar_connections_pool1", "scalar_connections",
        "SELECT " + " + ".join(f"sum(length(bench.upper_case(c{i})))" for i in range(16)) + f" FROM {strings(n,256,128,16)}",
        n*16*256, rows=n, columns=16, value_bytes=256, pre_attach=["SET vgi_worker_pool_max=1"])
    n = 512 if quick else 100000
    for distinct in (32, n):
        add(f"scalar_memo_d{distinct}", "scalar_cache",
            f"SELECT sum(bench.cached_double_scalar(i % {distinct})) FROM range({n}) t(i)",
            2 * sum(i % distinct for i in range(n)), rows=n, distinct=distinct, cache=True,
            settings=["SET vgi_result_cache_per_value=true"])
    for n in ([128] if quick else [4096, 16384]):
        add(f"producer_strings_{n}", "producer",
            f"SELECT sum(length(col_0)) FROM bench.constant_columns({n}, repeat('x',65536))",
            n * 65536, rows=n, value_bytes=65536)
    n = 128 if quick else 4096
    add("producer_sync", "producer",
        f"SELECT sum(length(col_0)) FROM bench.constant_columns({n}, repeat('x',65536))",
        n * 65536, rows=n, value_bytes=65536, settings=["SET vgi_async_prefetch=false"])
    add("producer_wide", "producer",
        "SELECT " + " + ".join(f"sum(length(col_{i}))" for i in range(128)) +
        f" FROM bench.constant_columns({n}, " + ",".join("repeat('x',256)" for _ in range(128)) + ")",
        n * 128 * 256, rows=n, columns=128, value_bytes=256)
    add("producer_limit", "producer",
        "SELECT sum(length(col_0)) FROM (SELECT * FROM bench.constant_columns(1000000,repeat('x',65536)) LIMIT 1)", 65536)
    add("producer_failure", "error", "SELECT sum(n) FROM bench.generator_exception(4)",
        "Intentional failure", operation="expect_error")
    n = 64 if quick else 2048
    for fanout in (1, 16):
        add(f"lateral_fanout{fanout}", "lateral",
            f"SELECT sum(length(c0)) FROM {strings(n,16384)} t, LATERAL bench.blended_explode({fanout} + CAST(substr(c0,16361,20) AS BIGINT) % 2) r",
            (n * fanout + n // 2) * 16384, rows=n, value_bytes=16384, fanout=fanout)
    add("lateral_native", "lateral_controls",
        f"SELECT sum(length(c0)) FROM {strings(n,16384)} t, LATERAL range(16 + CAST(substr(c0,16361,20) AS BIGINT) % 2) r",
        (n*16+n//2)*16384, native=True, rows=n, value_bytes=16384, fanout=16)
    add("lateral_precomputed", "lateral_controls",
        f"WITH t AS (SELECT c0, 16+CAST(substr(c0,16361,20) AS BIGINT)%2 AS fanout FROM {strings(n,16384)}) SELECT sum(length(c0)) FROM t, LATERAL bench.blended_explode(t.fanout) r",
        (n*16+n//2)*16384, rows=n, value_bytes=16384, fanout=16)
    n = 128 if quick else 100000
    for threads in (1, 4):
        add(f"buffered_t{threads}", "buffered",
            "SELECT sum(length(c0)+length(c1)+length(c2)+length(c3)) FROM bench.buffer_input((SELECT * FROM input_data))",
            n * 4 * 256, rows=n, threads=threads,
            setup=[f"CREATE TABLE input_data AS SELECT * FROM {strings(n,256,128,4)}"], cleanup=["DROP TABLE input_data"])
        add(f"finalize_t{threads}", "finalize",
            "SELECT sum(n) FROM bench.substream_partial_sum((SELECT i AS n FROM input_data))",
            n * (n - 1) // 2, rows=n, threads=threads,
            setup=[f"CREATE TABLE input_data AS SELECT i FROM range({n}) t(i)"], cleanup=["DROP TABLE input_data"])
    n = 32 if quick else 1024
    add("exchange_expand", "exchange",
        f"SELECT sum(length(c0)) FROM bench.repeat_inputs(8,(SELECT * FROM {strings(n,16384)}))",
        n * 8 * 16384, rows=n, value_bytes=16384, fanout=8)
    n = 64 if quick else 8192
    for threads in (1, 4):
        add(f"copy_to_t{threads}", "copy_to",
            "COPY input_data TO '{directory}/output.csv' (FORMAT 'bench.example_lines_out', null_string 'NA')", n,
            rows=n, threads=threads,
            setup=[f"CREATE TABLE input_data AS SELECT * FROM {strings(n,4096)}"], cleanup=["DROP TABLE input_data"],
            verify=[dict(sql="SELECT sum(length(c0)) FROM read_csv('{directory}/output.csv',columns={'c0':'VARCHAR'},header=false)", expected=str(n*4096))])
    add("copy_from", "copy_from",
        "COPY output_data FROM '{directory}/input.csv' (FORMAT 'bench.example_lines', null_string 'NA')", n,
        rows=n, setup=[f"COPY (SELECT * FROM {strings(n,4096)}) TO '{{directory}}/input.csv' (FORMAT csv,HEADER false)",
                       "CREATE TABLE output_data(c0 VARCHAR)"],
        before_each=["DELETE FROM output_data"], cleanup=["DROP TABLE output_data"],
        verify=[dict(sql="SELECT sum(length(c0)) FROM output_data", expected=str(n*4096))])
    for action in ("insert", "update", "delete"):
        insert = "INSERT INTO bench.main.items SELECT CAST(substr(c0,4073,20) AS BIGINT), c0, 1 FROM input_data"
        prepare = ["DELETE FROM bench.main.items"]
        if action != "insert":
            prepare.append(insert)
        sql = {"insert": insert, "update": "UPDATE bench.main.items SET qty=qty+1",
               "delete": "DELETE FROM bench.main.items"}[action]
        add(f"dml_{action}", "dml", sql, n, worker_only="python", catalog="simple_writable",
            fixture_module="vgi._test_fixtures.simple_writable", rows=n,
            setup=[f"CREATE TABLE input_data AS SELECT * FROM {strings(n,4096)}"], before_each=prepare,
            cleanup=["DELETE FROM bench.main.items", "DROP TABLE input_data"],
            verify=[dict(sql="SELECT coalesce(sum(length(name)+qty),0) FROM bench.main.items",
                         expected=str(0 if action == "delete" else n * (4096 + (2 if action == "update" else 1))))])
    # Long-running workloads exercise real cancellation, followed by a successful
    # worker query on the same connection. Delay includes bind time; source counters
    # and logs must be checked before claiming cancellation reached data execution.
    for family, sql in (
        ("producer", "SELECT sum(length(col_0)) FROM bench.constant_columns(100000000,repeat('x',65536))"),
        ("scalar", "SELECT sum(length(bench.upper_case(c0))) FROM bench_strings(100000000,1,65536,64,0)"),
        ("buffered", "SELECT sum(length(c0)) FROM bench.buffer_input((SELECT * FROM bench_strings(100000000,1,4096,64,0)))"),
        ("window", "SELECT sum(v) FROM (SELECT bench.main.vgi_window_sum(i) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) v FROM range(100000000) t(i))"),
    ):
        add(f"cancel_{family}", "cancellation", sql, "1500", operation="interrupt", target_family=family)
    add("cancel_window_strings", "cancellation",
        "SELECT sum(length(v)) FROM (SELECT bench.main.vgi_window_listagg(c0) OVER (ORDER BY c0 ROWS BETWEEN CURRENT ROW AND CURRENT ROW) v FROM bench_strings(2048,1,65536,64,0))",
        "1000", operation="interrupt", target_family="window", env={"VGI_STDERR_LOG":"1"})
    add("cancel_window_after_init", "cancellation",
        "SELECT sum(length(v)) FROM (SELECT bench.main.vgi_window_listagg(c0) OVER (ORDER BY c0 ROWS BETWEEN CURRENT ROW AND CURRENT ROW) v FROM bench_strings(512,1,65536,64,0))",
        "3000", operation="interrupt", target_family="window", worker_only="rust", env={"VGI_STDERR_LOG":"1"})
    n = 128 if quick else 10000
    missing = (n-1)//7+1
    add("schema_nested", "schema_validation",
        f"SELECT sum(list_sum(xs))+sum(s.id)+sum(coalesce(length(s.txt),0))+count(*) FILTER (WHERE s.txt IS NULL) FROM bench.buffer_input((SELECT [i,NULL,i+1] xs, {{'id':i,'txt':CASE WHEN i%7=0 THEN NULL ELSE repeat('x',64) END}} s FROM range({n}) t(i)))",
        n*n+n*(n-1)//2+(n-missing)*64+missing)
    add("schema_dictionary", "schema_validation",
        f"SELECT sum(CASE color WHEN 'red' THEN 1 WHEN 'green' THEN 2 ELSE 3 END) FROM bench.buffer_input((SELECT (CASE i%3 WHEN 0 THEN 'red' WHEN 1 THEN 'green' ELSE 'blue' END)::shade color FROM range({n}) t(i)))",
        sum(i%3+1 for i in range(n)), setup=["CREATE TYPE shade AS ENUM('red','green','blue')"])
    add("schema_large_output", "schema_validation",
        f"SELECT sum(n)+count(*) FROM bench.buffer_emit_wide({n},(SELECT 1))",n*(n-1)//2+n)
    add("schema_cached", "schema_validation",
        f"SELECT sum(x) FROM bench.cached_sum_all((SELECT i AS x FROM range({n}) t(i)))",n*(n-1)//2,cache=True)
    return result
