"""VGI worker-pool + launcher stress harness.

Driven by haybarn-python (the Query Farm DuckDB distribution that ships VGI as a
signed community extension). Exercises the per-process subprocess worker pool and
the cross-process launcher under genuine concurrency, with correctness /
throughput / soak modes and external-SIGKILL fault injection.

Run with the haybarn venv python from the repo root, e.g.:

    /Users/rusty/Development/haybarn-python/.venv/bin/python -m scripts.stress \\
        --target pool --mode correctness --concurrency 8 --queries 50
"""
