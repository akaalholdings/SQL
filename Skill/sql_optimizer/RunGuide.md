# SQL Optimizer Run Guide

Use this guide to execute the three-scenario benchmark for **a single query** through `query_geneva_db` and return the before/after numbers. Run it after `queryguide.md` has produced the plan findings, the semantically-identical rewrite, and the candidate index changes.

The deliverable is three measured results for the same query:

1. **Baseline** — the original query as supplied.
2. **Optimized** — the semantically-identical rewrite, no index changes.
3. **Optimized + indexes** — the rewrite plus the new test indexes.

Do not invent a workload of extra queries. Everything here measures the one query the user supplied.

---

## Environment and safety

Ask which Shell environment to use before live access, then:

- **Baseline and Optimized run read-only.** Default to `mid` (read-only prod replica) for evidence gathering and state that assumption if the user gave no preference.
- **Optimized + indexes needs a writable environment.** Default to `mid_dev` (database `mid_Dev`). Creating the test indexes is DDL, so confirm the writable environment and DDL with the user before this step. Never create indexes in `mid` or `mid_prod` as part of the benchmark.
- Test indexes use the approved test prefix so they are obviously disposable. Create them for the measurement, capture the result, then drop them — surface both the CREATE and the DROP (rollback) scripts.
- `mid_prod` is for approved primary-production maintenance only, never for benchmarking.

`query_geneva_db` is the execution engine: read-only baseline/optimized runs, actual-plan capture, base-table index/stats inventory, result-equivalence checks, and — only in the chosen writable environment after explicit approval — the test-index DDL for scenario 3.

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

```bash
query_geneva_db mid --dba --tune-capture --query-file /tmp/baseline.sql --max-rows 100 --format json
```

Use read-only baseline capture unless the user explicitly approves DDL in a writable environment.

### 2. Inventory the base tables

For each base table the query (or its actual plan) touches, inspect existing indexes, row counts, and statistics so index recommendations can be overlap-checked before anything is created. Use the catalog queries in `queryguide.md` section 2.2, or:

```bash
query_geneva_db mid --dba --index-inventory dbo.your_table --format json
```

### 3. Run the optimized rewrite (read-only)

Run the rewrite from `queryguide.md` with the same parameter values used for the baseline. Capture the same metrics and the new actual plan.

```bash
query_geneva_db mid --dba --benchmark --query-file /tmp/baseline.sql --query-file2 /tmp/optimized.sql --max-rows 100 --format json
```

`--benchmark` fetches **both** result sets into memory for the duplicate-sensitive comparison, so `--max-rows 0` (unlimited) can blow up locally on large queries. Bound `--max-rows` for the timing run and rely on the server-side `EXCEPT` proof in step 5 (or an aggregate/checksum comparison) for exact equivalence on large result sets.

### 4. Run the optimized rewrite plus test indexes (writable, gated)

After explicit approval of the writable environment and DDL, run three separate commands — create, capture, drop. `--benchmark`/`--query-file2` only run read-only `SELECT`s and **cannot** apply DDL; index DDL goes through the single-statement DBA maintenance path (`--dba` with one `CREATE INDEX` / `DROP INDEX` statement per call).

1. Create the test-prefixed index the plan evidence justifies (one statement per file):

   ```bash
   query_geneva_db mid_dev --dba --query-file /tmp/create_index.sql --format json
   ```

2. Capture the optimized rewrite with the index now present:

   ```bash
   query_geneva_db mid_dev --dba --tune-capture --query-file /tmp/optimized.sql --max-rows 100 --format json
   ```

3. Drop the test index (rollback), unless the user asks to keep it for further testing:

   ```bash
   query_geneva_db mid_dev --dba --query-file /tmp/drop_index.sql --format json
   ```

Each DDL file must contain a **single** statement — the maintenance path rejects multiple statements in one call. If you are testing more than one candidate index, create/drop them one statement at a time. Candidate index shapes follow `queryguide.md` Phase 2 (equality columns before inequality, most selective equality first, INCLUDE the covering columns). Only create indexes the actual plan, row counts, and selectivity support, and only after the overlap check in step 2.

### 5. Prove result equivalence

Before recommending the rewrite, confirm baseline and optimized return identical results. Compare in both directions with `EXCEPT`, and use duplicate-sensitive comparison where duplicates can matter (number the rows over all projected columns first). Drop any `ORDER BY` for the set comparison itself.

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

Then reverse it (`optimized` `EXCEPT` `baseline`). Both directions must return zero rows. Aggregate signatures or checksums may support the comparison but never replace an exact row comparison when one is practical.

### 6. Return the three-scenario results matrix

| Scenario | Duration ms | CPU ms | Logical Reads | Physical Reads | Rows | Plan Notes |
|---|---:|---:|---:|---:|---:|---|
| Baseline |  |  |  |  |  |  |
| Optimized |  |  |  |  |  |  |
| Optimized + indexes |  |  |  |  |  |  |

State which metrics were parsed and which were unavailable.

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

Run the supplied query through `query_geneva_db` to capture a read-only baseline and actual plan, inventory the base-table indexes, run the optimized rewrite, then (in a writable environment after approval) run the rewrite with test indexes. Prove result equivalence both ways, return the three-row results matrix, and produce final index add/drop, rollback, and deployment scripts.
