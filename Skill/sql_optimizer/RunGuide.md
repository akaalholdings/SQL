# SQL Optimizer Run Guide

Use this guide to execute the three-scenario benchmark for **a single query** through `azure-sql-mcp` and return the before/after numbers. Run it after `queryguide.md` has produced the plan findings, the semantically-identical rewrite, and the candidate index changes.

The deliverable is three measured results for the same query:

1. **Baseline** — the original query as supplied.
2. **Optimized** — the semantically-identical rewrite, no index changes.
3. **Optimized + indexes** — the rewrite plus the new test indexes.

Do not invent a workload of extra queries. Everything here measures the one query the user supplied.

---

## Environment and safety

Call `list_databases` to see the server, default database, and the full `allowed_databases` list, then ask the user which one to target. There is no fixed alias list to assume — the available databases are whatever the running `azure-sql-mcp` server's `AZURE_SQL_ALLOWED_DATABASES` configures.

The read/write boundary is by **tool**, not by database:

- **Baseline and Optimized run read-only on any allowed database.** `execute_sql` and `explain_query` are always validated read-only by `SafeSqlValidator`, regardless of `AZURE_SQL_ACCESS_MODE` — there is no separate "read-only alias" to default to.
- **Optimized + indexes runs through the dedicated test-index tools.** `create_test_index` / `drop_test_index` manage disposable, prefix-namespaced (`IX_Testing_`) test indexes only — the drop tool physically cannot touch a real index, identifiers are strictly validated, and the CREATE response carries its rollback DROP. Raw DDL remains impossible: `execute_tsql_unrestricted` hard-denylists `CREATE|DROP|ALTER INDEX` (and all DDL/DML/`EXEC`). On older servers without these tools, scenario 3 falls back to the **emit-script protocol** (step 4 fallback).
- All admin tools (`create_test_index`, `drop_test_index`, `update_statistics`, `rebuild_index`) are double-gated: they execute only with server config `AZURE_SQL_WRITE_POLICY=apply` **and** an explicit `dry_run=false` per call, and every preview/apply is written to the server's JSONL audit. Explicit user approval is still required on top — the gates stack, they don't substitute.
- Test indexes always use the test prefix so they are obviously disposable. Surface both the CREATE and the DROP (rollback) statements in the response, always — tool-created or emitted.
- Live test-index DDL additionally requires `AZURE_SQL_TEST_INDEX_DATABASES` to explicitly allow the approved sandbox database. An empty allowlist blocks non-dry-run create/drop calls.
- Record the experiment lifecycle with `test_index_ledger.py`: begin before create, mark-created after the tool succeeds, and mark-dropped after cleanup. If the session crashes, run `test_index_ledger.py pending`, verify whether each index exists, and clean it up before continuing.
- **Prefer a sandbox clone for index testing and DML tuning** — see `SandboxGuide.md`. On a clone, standing approval for test-index DDL is safe; on production, ask per index.

**Timeouts:** the server's default per-query timeout is `AZURE_SQL_QUERY_TIMEOUT_SECONDS=30`. The queries most worth tuning are exactly the ones that blow through it. If the baseline times out, ask the operator to raise the timeout for the session — or use Query Store history (`tune_query` evidence pack, `get_top_queries`) as the baseline measurement instead of a live rerun, and say so in the results matrix.

**Repetition:** a single execution is not a benchmark. Pass `runs=3` to `runs=5` to `benchmark_query_rewrite` — the server interleaves baseline and rewrite executions and returns per-run medians with min/max spread, so one side does not systematically receive the cold cache. For scenarios measured via `explain_query` alone (baseline, optimized+indexes) repeat the call 3–5 times yourself and report the **median** with min/max. Either way, treat deltas within ~20% as noise, not wins. If the query has no contractual `ORDER BY`, pass `compare_order=false` for the sample check; otherwise keep the default ordered comparison.

`azure-sql-mcp` is the execution engine. Preferred fast path:

- `tune_query` for the baseline evidence pack (plan summary, bounded sample, Query Store history, waits, stats health, index analysis).
- `benchmark_query_rewrite(runs=3..5)` for baseline-vs-rewrite actual-plan/sample comparison with server-side median/spread aggregation.

Use lower-level `execute_sql`/`explain_query` when you need a specific proof query, a raw artifact, or a manual rerun. Use `get_object_details`/`get_table_stats` for base-table index/stats inventory.

---

## Workflow

### 1. Capture the baseline (read-only)

Run the original query and capture, for the single query:

- Duration
- Best-effort parsed execution CPU time
- Best-effort parsed logical reads
- Best-effort parsed physical reads
- Row count
- Actual XML execution plan
- Query hash and plan hash when present in the XML plan
- Memory grant, spills, and warnings from the actual plan
- Missing-index requests from the plan (leads only)
- Whether predicates are pushed down to the base tables

```
explain_query(sql=<baseline query>, analyze=true, database_name=<chosen database>)
execute_sql(sql=<baseline query>, database_name=<chosen database>)
```

`explain_query(analyze=true)` runs `SET STATISTICS XML ON` and returns parsed actual-plan evidence in `summary`. Raw XML is token-safe by default via `raw_xml_resource_uri`; set `include_raw_xml=true` only when you truly need the full XML inline. Parse the summary/resource per `queryguide.md` §1.1 for CPU time, elapsed time, per-operator `ActualRows`/`ActualExecutions`/`ActualLogicalReads`, memory grant, spills, warnings, and missing-index hints. `execute_sql` returns the actual rows — bounded by the server's `AZURE_SQL_ROW_LIMIT` (default 200), with a `truncated` flag — for the row count here and the equivalence proof in step 5.

### 2. Inventory the base tables

For each base table the query (or its actual plan) touches, inspect existing indexes, row counts, and statistics so index recommendations can be overlap-checked before anything is created.

```
get_object_details(schema_name=<schema>, object_name=<table>, object_type="table", database_name=<chosen database>)
get_table_stats(schema_name=<schema>, database_name=<chosen database>)
analyze_query_indexes(queries=[<baseline query>], database_name=<chosen database>)
analyze_index_recommendations(database_name=<chosen database>)
```

`analyze_query_indexes` analyzes this query's index needs read-only (no what-if index creation is involved); `analyze_index_recommendations` returns the missing-index DMV plus automatic-tuning recommendations — both are **evidence sources feeding `IndexingGuide.md`'s gate**, never auto-accepted. The catalog queries in `queryguide.md` section 2.2 still work verbatim through `execute_sql` — they're plain `SELECT`s — if you need a column these tools don't surface.

### 3. Run the optimized rewrite (read-only)

Run the rewrite from `queryguide.md` with the same parameter values used for the baseline. Prefer:

```
benchmark_query_rewrite(baseline_sql=<baseline query>, rewrite_sql=<optimized query>, analyze=true, runs=3, parameter_values=<same bucket values>, database_name=<chosen database>)
```

For parameterized SQL, pass the same explicit `parameter_values` (or the same
Query Store-derived parameter bucket) for both sides. Heuristic auto-binding is
exploratory only and must not be presented as a production workload distribution.

If you need separate artifacts, capture the same metrics and the new actual plan:

```
explain_query(sql=<optimized query>, analyze=true, database_name=<chosen database>)
execute_sql(sql=<optimized query>, database_name=<chosen database>)
```

Compare the two `raw_xml` plans manually (scans → seeks, lookups removed, estimated-vs-actual gap closed). There is no single "diff two arbitrary plans" tool on this server: `compare_query_plans` only diffs two Query Store `plan_id`s for the *same* `query_id`, which doesn't apply here — a rewrite is different SQL text, so it is a different `query_id` (if it has run at all yet).

### 4. Run the optimized rewrite plus test indexes (gated tools)

After explicit approval of the target database and the index (standing approval is fine on a sandbox clone — `SandboxGuide.md`), run the durable create → capture → drop workflow:

1. **Begin the experiment record** before creating anything:

   ```bash
   python3 test_index_ledger.py begin --database <sandbox database> --schema <schema> \
       --table <table> --index "IX_Testing_BS_<Table>_<LeadCols>_<8char_hash>"
   ```

   Save the returned experiment id.

2. **Create the test index** the plan evidence justifies (name must carry the `IX_Testing_` prefix — the tool enforces it):

   ```
   create_test_index(schema_name=<schema>, table_name=<table>,
                     index_name="IX_Testing_BS_<Table>_<LeadCols>_<8char_hash>",
                     key_columns=[<key cols, optional " DESC" suffix>],
                     include_columns=[<covering cols>],
                     dry_run=false, database_name=<approved database>)
   ```

   The response carries the exact DDL executed and its rollback DROP — surface both in the final answer.

   ```bash
   python3 test_index_ledger.py mark-created <experiment-id>
   ```

3. **Capture the optimized rewrite with the index present** (repeat per the repetition rule):

   ```
   explain_query(sql=<optimized query>, analyze=true, database_name=<approved database>)
   ```

4. **Drop the test index** (rollback), unless the user asks to keep it for further testing:

   ```
   drop_test_index(schema_name=<schema>, table_name=<table>, index_name=<same name>,
                   dry_run=false, database_name=<approved database>)
   ```

   Only after the tool confirms the DROP succeeded:

   ```bash
   python3 test_index_ledger.py mark-dropped <experiment-id>
   ```

If creation fails before the index exists, run `mark-failed <experiment-id>`. If creation
succeeded but capture or cleanup fails, **do not** mark the experiment failed or dropped:
leave it in `created`, run `pending`, and complete the recorded rollback. A created record
cannot transition to `failed`, so a live test index cannot disappear from recovery output.

**Emit-script fallback (older servers only).** If the server does not expose the test-index tools: emit one fenced block with the single `CREATE INDEX` statement and its `DROP` rollback; the operator runs the CREATE; **verify the index exists read-only before measuring** (`get_object_details`, or `execute_sql` over `sys.indexes` filtered to the test prefix — never trust the confirmation alone; absent → re-emit, don't measure); capture; then emit the DROP reminder and verify cleanup the same way.

If you are testing more than one candidate index, create/capture/drop them one at a time. Candidate index shapes follow `queryguide.md` Phase 2 (equality columns before inequality, most selective equality first, INCLUDE the covering columns). Only create indexes the actual plan, row counts, and selectivity support, and only after the overlap check in step 2.

### 5. Prove result equivalence

Before recommending the rewrite, confirm baseline and optimized return identical results. Compare in both directions with `EXCEPT`, and use duplicate-sensitive comparison where duplicates can matter (number the rows over all projected columns first). Drop any `ORDER BY` for the set comparison itself. The validator behind `execute_sql` allows CTEs and set operators, so this runs as a single statement, entirely server-side — only mismatched rows (typically zero) come back over the wire:

```sql
WITH
    baseline_result AS
(
    /* original query, no ORDER BY */
),
    optimized_result AS
(
    /* optimized query, no ORDER BY */
)
SELECT
    issue = 'in_baseline_not_optimized',
    br.*
FROM baseline_result AS br

EXCEPT

SELECT
    issue = 'in_baseline_not_optimized',
    opt.*
FROM optimized_result AS opt;
```

Then reverse it (`optimized` `EXCEPT` `baseline`) and run both through `execute_sql`. Both directions must return zero rows. Aggregate signatures or checksums (`COUNT_BIG(*)`, `CHECKSUM_AGG`) may support the comparison but never replace an exact row comparison when one is practical.

### 6. Return the three-scenario results matrix

| Scenario | Duration ms | CPU ms | Logical Reads | Physical Reads | Rows | Plan Notes |
|---|---:|---:|---:|---:|---:|---|
| Baseline |  |  |  |  |  |  |
| Optimized |  |  |  |  |  |  |
| Optimized + indexes |  |  |  |  |  |  |

Report the median of the repeated runs (with min/max noted) per the repetition rule. State which metrics were parsed and which were unavailable, and note how the test index was applied (gated tool, or operator-applied on the emit-script fallback).

### 7. Final recommendations

- Baseline results, including which metrics were parsed vs unavailable
- Actual execution plan findings for the baseline
- The optimized query
- Index changes to ADD, with evidence-tied justification and the overlap check
- Index changes to DROP, only as candidates (a single query cannot prove an index is globally unused — limit drops to true duplicates / near-duplicates with equal-or-better coverage)
- Indexes that were tested but are **not** recommended
- Risks, tradeoffs, and expected write overhead from new indexes
- Rollback script (drop the new indexes / revert)
- Deployment script (create the recommended indexes with `ONLINE = ON`)
- Before/after summary tied to the results matrix

---

## Success criteria

The optimization is successful when, for the supplied query, the optimized approach:

- Reduces duration and logical reads
- Improves predicate pushdown on the filtered columns
- Reduces unnecessary scans, sorts, lookups, or hash operations
- Preserves result correctness (both `EXCEPT` directions return zero rows)
- Avoids duplicate or low-value indexes
- Keeps deployment risk low with clear deployment and rollback scripts

---

## Short version

Run the supplied query through `azure-sql-mcp` (`execute_sql` + `explain_query`, repeated per the repetition rule) to capture a read-only baseline and actual plan, inventory the base-table indexes with `get_object_details`/`get_table_stats`/`analyze_query_indexes`, run the optimized rewrite, then create the test index via `create_test_index` (gated; emit-script fallback on older servers), re-measure, and drop it via `drop_test_index`. Prove result equivalence both ways, return the three-row results matrix (medians), and produce final index add/drop, rollback, and deployment scripts.
