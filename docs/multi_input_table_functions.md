# Table Functions with Multiple Relational Inputs

> **Status: discussion / feature request — Rusty Conover (Query Farm).** Written to be read without prior context.

## The ask

Allow a table function to declare more than one `LogicalType::TABLE` argument, and deliver the rows of each table expression to the function.

That is the whole request. It needs no new SQL grammar (table arguments already use existing syntax), no join semantics, and no partitioning, ordering, or co-partitioning model. A function may declare N relational inputs and receive the rows of each. What it does with them is its own business.

I'm asking the core team to consider this because the workarounds available today are either incorrect or lossy, and the need is recurring rather than hypothetical. The rest of this document is the evidence.

## The first objection: "two tables means a join"

The reflexive response to a function over two tables is that it is really a join, and joins are what the engine already does. That response is wrong, and it is worth being precise about why, because it is the reason the feature has not been built.

A join matches rows of two relations on a predicate and emits combinations of input rows, with an output schema derived from the inputs. It is reorderable by the optimizer and lives inside relational algebra. The functions below take two or more relations and produce a newly *computed* relation that bears no row-combination relationship to the inputs:

- **UniFrac PCoA.** Input: a feature table `(sample_id, feature_id, value)` and a rooted phylogenetic tree. Output: `(sample_id, PC1, PC2, …)` ordination coordinates. The tree is never matched against feature rows. It defines the distance metric over which an eigendecomposition runs. No predicate, no row combination.
- **Model fit.** Input: a training relation and a relation of hyperparameter configurations. Output: one fitted model (or score) per configuration, each derived from all training rows.
- **Faith's PD / PERMANOVA.** Feature table plus tree (plus an optional per-sample metadata relation) in; a per-sample diversity value or a `(variable, pseudo_F, p_value)` statistics table out. A statistical reduction over several relations, not a combination of their rows.

Even the case that looks most join-like is not one. **Short-sequence matching** (reads against a set of reference barcodes/primers) emits `(query_id, ref_id, mismatches)`, but the matching predicate is approximate: Hamming distance ≤ k, with an error budget and early exit. The algebra can only express that as a cross product followed by a filter, O(Q × R), which is exactly the cost a purpose-built matcher avoids. It is a similarity operator the algebra does not have.

A join is one special case of a function of two relations: the case where the function is an equi-combination of rows. The broader space of statistical reductions, similarity matching, model fitting, graph and tree algorithms, and scientific kernels is large and inexpressible as joins. The engine should not assume two-table means join, because most two-table functions are not joins.

## This is not niche

The pattern covers any analytical function that needs more than one input relation and does not combine rows: phylogenetics, sequence analysis, statistics, machine learning, graph and tree analytics, geospatial.

For a concrete and independent data point: the third-party [`duckdb-miint`](https://github.com/the-miint/duckdb-miint) bioinformatics extension, which I do not maintain, is built almost entirely on multi-relation functions. UniFrac (2–3 relations), short-barcode matching (2), coverage (3), the aligners (`query_table` + `subject_table`), vsearch search, chimera detection. None are joins, and all of them currently work around DuckDB's single-relation limit.

```sql
-- two and three relations, none a join:
SELECT * FROM unifrac_pcoa('observations', 'tree');
SELECT * FROM unifrac_permanova('observations', 'tree', 'metadata', variables := ['body_site']);
SELECT * FROM match_short_barcodes('query_seqs', 'reference_barcodes', max_nm := 1);
```

Out-of-process and remote-execution extensions, which wrap a function running in another process or service, need the same capability for the same reason: they must hand several relations to the function. The request does not depend on any one such system.

## Today there is no way to express it

A DuckDB table function binds a single `LogicalType::TABLE` argument as the operator's one child pipeline. There is no second relational input. Functions that need more than one relation pass relations by name, as `VARCHAR` strings, and read each through a private `Connection`:

```cpp
// registered as {VARCHAR, VARCHAR, VARCHAR} — three relation *names*
Connection conn(DatabaseInstance::GetDatabase(context));
auto probe  = conn.Query("SELECT … FROM " + KeywordHelper::WriteOptionallyQuoted(name) + " LIMIT 0");
auto result = conn.Query("SELECT … FROM " + KeywordHelper::WriteOptionallyQuoted(name));
```

(`duckdb-miint`: [`match_short_barcodes.cpp:319, :192-193`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/match_short_barcodes.cpp#L319); [`unifrac_permanova_function.cpp:433, :241-243`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/unifrac_permanova_function.cpp#L433); [`unifrac_function_common.cpp:19-31`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/unifrac_function_common.cpp#L19-L31); the canonical recipe is documented at [`docs/internals/reading-tables-views.md`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/internals/reading-tables-views.md).)

This is not a stylistic choice. A separate `Connection` is required, because `GetEntry` does not resolve views, views have no physical storage to read, and `context.Query()` deadlocks during bind/execution since the context is already locked (`reading-tables-views.md:6-8`).

### What the string-name workaround costs

The serious problem is correctness. A private connection reads under a different transaction snapshot. Uncommitted data from the surrounding statement is invisible, and session variables vanish: `getvariable()` returns NULL inside the internal connection, with a documented "materialize to a real table first" workaround (`reading-tables-views.md:57`, `table-functions.md:60-74`). The only way to feed a second relation to a function today can silently return wrong answers. For an analytical engine that is the strongest reason on its own.

Two further costs follow from the same design:

- **No relational composability.** The argument must name a base table or view that already exists. You cannot pass an inline subquery, a CTE, a `VALUES` list, or another function's output without first materializing it into a named object. Relational algebra stops at the function boundary.
- **No pushdown, full materialization, hand-rolled validation.** The function reads whole relations itself with no projection or filter pushdown, and, lacking a binder, re-implements schema and type checks through `LIMIT 0` probe queries and bespoke errors. Sharp edges follow, by the extension's own account: DECIMAL identifiers silently mismatch under string-cast comparison (`per-sample-pattern.md:134`).

### The other workaround: a tagged union

A second technique sidesteps the unsoundness. Rather than read relations by name, concatenate them into a single table input where each row carries a discriminator tag equal to the source argument's position, expressed with DuckDB's `UNION` type (`union_value(t0 := …)`, the tag being the member index), and have the function demux by tag. The inputs now flow as ordinary query subqueries, so this keeps the same transaction snapshot, visible session variables, and full composability with inline subqueries and CTEs. On correctness it is strictly better than the string-name approach. Its cost is structural.

- **It collapses N logical streams into one physical pipeline.** A single `UNION ALL` forces every input through one pipeline, interleaved and in no guaranteed order, so the function must buffer and demux, and the engine can no longer push projection or filters into each input independently. This is fine for reductions that materialize everything anyway (UniFrac, PERMANOVA, model fitting), but it defeats the small-build/large-stream asymmetry: a large input can no longer be streamed against a small one without paying per-row union overhead and buffering the large side.
- **Construction is on the caller, and the tag is unchecked.** Someone must build the tagged `UNION ALL`, enumerating each table into a union member and conforming every branch to the full union type. The position tag is caller-assigned, so a mis-tag is a silent wrong-demux. With real table arguments, position is structural.

Schema heterogeneity is not a downside here. A `TABLE` argument is already unconstrained until bind, and a typed `UNION`'s member types are discovered at bind the same way, so the function stays as polymorphic as before; it introspects union members instead of separate input schemas. The cost is the pipeline collapse, not the mixed schemas.

### Two workarounds, two sacrifices

Both available paths are workarounds, and each gives up something the engine would otherwise provide. The string-name approach surrenders transactional correctness and composability. The tagged-union approach surrenders independent streaming, per-input pushdown, and ergonomics. Authors hand-encoding a sum type over relations to smuggle N inputs through a one-input hole is itself evidence that the hole should be widened.

### Confirmed by a working prototype

A runnable prototype, an out-of-process worker driven by a DuckDB CLI, confirms the tagged-union path and pins down the macro question.

The tagged-union approach works end-to-end and is sound. Two relations with different schemas, `values(id, v)` and `weights(w)`, were combined into one `(src BIGINT, payload JSON)` input and demuxed inside the function, which computed a non-join broadcast result: each `v` scaled by the sum of all weights, so it had to see the whole second relation before emitting any output. Heterogeneous schemas were a non-issue, since the payload is decoded per tag. When the union is written inline, composability is fully preserved: feeding CTEs and filtered subqueries (`WHERE id > 1`, `w * 2`) produced the correct scaled output. This is the soundness the string-name workaround lacks.

A macro can hide the boilerplate only by giving composability back up. Wrapping the union in a table macro works only when its arguments are `VARCHAR` table names read via `query_table(name)`. Passing a subquery to that macro fails at bind with `Binder Error: Table function cannot contain subqueries`, because a DuckDB table macro cannot take a relation/subquery parameter and use it in `FROM` (a bare `FROM a` resolves `a` as a literal table name). The choice is concrete and forced: write the union inline and keep composability but repeat the boilerplate at every call site, or hide it in a macro and fall back to string table names. Neither path gives both without real multi-`TABLE` support.

### Why table macros are not the answer

DuckDB table macros already provide multi-relation composition: `genome_coverage` takes three unquoted relation names and combines them with ordinary SQL joins ([`duckdb-miint docs/analysis-functions.md:509`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/analysis-functions.md?plain=1#L509)). That is the right tool when the whole computation is expressible in SQL. It cannot help a function whose body is not SQL: a native library (UniFrac, alignment, matching) or an external/out-of-process computation. Those are exactly the functions stuck on the string-name workaround. The macro mechanism marks the boundary of the gap; it does not fill it.

## The request, technically

Permit a table function to declare more than one `LogicalType::TABLE` argument, bind each as a relation, and deliver each relation's rows to the function at execution.

- **No new SQL syntax.** Multiple relational arguments are already expressible. The change is in binding and delivery, not the grammar.
- **The execution mechanism appears to already exist.** Routing several relations into one operator via child pipelines with dependencies is what hash join and materialized CTE already do. Distinct inputs arrive on distinct pipelines and are never interleaved. The intuition that one Sink callback cannot tell two inputs apart is not an obstacle.
- **The change appears to be confined to the binder.** Binding N `TABLE` arguments, type-checking each and attaching them to the operator's children, is the piece that does not exist today. I'd want a maintainer to confirm that reading, since you know the binder and pipeline builder far better than I do.

For what it is worth, this is a recognized construct. SQL:2016 treats it as first-class (Polymorphic Table Functions), and it ships in subsets elsewhere (Apache Flink, Teradata). The point is only that a function over several relations, distinct from a join, is an established idea rather than an exotic one. I'm not asking you to chase the standard, only noting that the shape is well-trodden.

## Explicitly out of scope

To keep the ask minimal and reassure against feature creep, this request does **not** ask for: a join operator; partitioning or `ORDER BY` over inputs; co-partitioning (`COPARTITION`) or any keyed-merge semantics; pushdown guarantees into the inputs; or any particular cardinality model. Those can be layered later. The v1 request is only: **more than one table argument, rows delivered.** The function decides the rest.

## Open validation

One honest unknown remains. The workaround path is already validated end-to-end (see "Confirmed by a working prototype"). The first-class path is not, and it is cheap to settle: a small spike confirming the binder is the only blocker, i.e. that N `TABLE` arguments can be bound and attached to an operator with N children, after which execution reuses the existing child-pipeline machinery. The claim "execution already exists, only binding is missing" should be backed by that spike before being treated as fact. I'd value the core team's read on whether that is the right place to cut the scope.

## What I'm asking for

Consider supporting more than one `LogicalType::TABLE` argument per table function. It needs no grammar change, introduces no join or partitioning semantics, and reuses the child-pipeline execution that hash join and materialized CTE already rely on. In exchange it removes a correctness hazard (the separate-snapshot read) that today's only general workaround cannot avoid. I'd like to discuss scope and the binder spike with whoever owns that path.

## References

- [`duckdb-miint`](https://github.com/the-miint/duckdb-miint) (pinned to commit [`d0336e9`](https://github.com/the-miint/duckdb-miint/tree/d0336e96efe09a2b9920220d6c267ac6e1d32afb)):
  [`src/match_short_barcodes.cpp`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/match_short_barcodes.cpp),
  [`src/unifrac_function_common.cpp`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/unifrac_function_common.cpp),
  [`src/unifrac_permanova_function.cpp`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/unifrac_permanova_function.cpp),
  [`src/per_sample_table_function.cpp`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/src/per_sample_table_function.cpp);
  [`docs/unifrac.md`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/unifrac.md),
  [`docs/analysis-functions.md`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/analysis-functions.md),
  [`docs/table-functions.md`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/table-functions.md),
  [`docs/internals/reading-tables-views.md`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/internals/reading-tables-views.md),
  [`docs/internals/per-sample-pattern.md`](https://github.com/the-miint/duckdb-miint/blob/d0336e96efe09a2b9920220d6c267ac6e1d32afb/docs/internals/per-sample-pattern.md).
- SQL:2016 Polymorphic Table Functions: [ISO/IEC TR 19075-7](https://www.iso.org/standard/69776.html). Realizations: [Apache Flink Process Table Functions](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/ptfs/) ([FLIP-440](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=298781093)); Teradata/Aster table operators ([SQL/MapReduce, VLDB 2009](http://www.vldb.org/pvldb/vol2/vldb09-464.pdf)).
