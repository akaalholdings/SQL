# SQL Optimizer Indexing Guide

Use this guide after `queryguide.md` has identified the query shape, actual-plan
findings, and base tables. It converts the Brent Ozar indexing corpus in
`sources/brentozar-indexing/` plus the Kendra Little / SQLWorkbooks indexing
corpus in `sources/kendra-indexing/` into operational rules for this Azure SQL
Database single-query skill.

The corpus was built from the public Brent Ozar Indexing category plus sitemap
keyword gap scan on 2026-07-01:

- 221 Indexing category posts
- 47 additional sitemap keyword candidates
- 268 deduped source URLs analyzed
- 0 scrape errors

Durable source files:

- `sources/brentozar-indexing/manifest.json` - crawl scope, counts, URLs, errors
- `sources/brentozar-indexing/article-digests.json` - per-post title, URL, topics, short derived digest
- `sources/brentozar-indexing/article-digests.md` - human-readable source index

Additional Kendra Little / SQLWorkbooks corpus built on 2026-07-01:

- 7 Index Design and Tuning courses from `https://kendralittle.com/topics/index-design-and-tuning/`
- 52 lesson URLs from `https://kendralittle.com/sitemap.xml`
- 59 web pages analyzed
- 33 SQLWorkbooks files analyzed from `https://github.com/LitKnd/SQLWorkbooks`
- 0 scrape errors

Durable source files:

- `sources/kendra-indexing/manifest.json` - topic courses, lesson URLs, SQLWorkbooks commit, counts
- `sources/kendra-indexing/web-digests.json` - per-course/page title, URL, topics, short derived digest
- `sources/kendra-indexing/sqlworkbooks-digests.json` - per-workbook path, source URL, topic signals, script counts
- `sources/kendra-indexing/digests.md` - human-readable source index

Raw HTML is intentionally excluded from the durable guide and kept only in the
ignored `.cache/` directory.

## Index Recommendation Gate

Before proposing any index DDL, prove all five checks:

1. The query rewrite has already fixed obvious SARGability, row-goal, projection,
   join, and parameter-shape problems that can be fixed without DDL.
2. The actual plan shows a concrete index-related problem: expensive scan,
   lookup, sort, bad predicate pushdown, repeated key/RID lookup, missing-index
   hint, partition elimination failure, or write statement scanning to find rows.
3. Existing indexes, constraints, filters, includes, key order, row counts, size,
   and usage context have been inspected for the base table.
4. The candidate does not duplicate or trivially overlap an existing index.
5. The candidate can be measured in the three-scenario benchmark:
   baseline, optimized rewrite, optimized rewrite plus test index.

If any check is missing, label the index as a hypothesis and request the missing
evidence instead of producing production DDL.

## Server Index Tools (evidence sources)

`azure-sql-mcp` exposes read-only index analysis that feeds checks 2–4 above — use
them as evidence, never as auto-accepted recommendations:

- `analyze_query_indexes(queries=[...], database_name=...)` — per-query index analysis for
  the supplied statement; the fastest way to satisfy check 2 with tool output tied to
  this exact query.
- `analyze_index_recommendations(database_name=...)` — the missing-index DMV plus
  automatic-tuning recommendations. Same trust level as plan missing-index hints:
  leads only, overlap-check before proposing.
- `analyze_workload_indexes(database_name=...)` / `optimize_indexes(database_name=...)` —
  workload-wide analysis across Query Store. Out of scope for tuning the one supplied
  query, but the right evidence when a drop candidate needs the broader-workload proof
  this guide demands (drops stay candidate-only without it).
- `get_object_details` / `get_table_stats` — the per-table inventory for checks 3–4.

Every rule below still applies to tool output exactly as it applies to plan hints:
the tools shortcut evidence *gathering*, not the decision gate.

## Source-Backed Rules

### Missing Index Hints

Treat missing-index hints, DMVs, and automatic tuning recommendations as leads.
They do not know the full workload, duplicate indexes, write overhead, or best
key order.

Required handling:

- Extract equality, inequality, INCLUDE columns, table, and estimated impact.
- Compare against all existing indexes on the same base table.
- Prefer amending an existing useful index when a small INCLUDE addition solves
  the lookup without creating a parallel index family.
- Reject single-column or obviously partial hints unless the actual plan and
  predicates prove the shape.
- Merge compatible hints across parameter buckets before recommending DDL.

Representative source URLs:

- https://www.brentozar.com/archive/2013/07/dude-who-stole-my-missing-index-recommendation/
- https://www.brentozar.com/archive/2017/02/crappy-missing-index-requests/
- https://www.brentozar.com/archive/2017/08/missing-index-recommendations-arent-perfect/
- https://www.brentozar.com/archive/2019/11/the-problems-with-sql-servers-index-recommendations/
- https://www.brentozar.com/archive/2020/03/things-to-consider-when-sql-server-asks-for-an-index/

### SARGability First

Fix predicate shape before adding an index. A better index cannot fully rescue a
predicate that hides the column behind functions, incompatible types, leading
wildcards, optional-parameter patterns, or expressions that prevent selective
access.

Required handling:

- Move functions/calculations to the literal or parameter side where semantics
  allow it.
- Match parameter and column data types to avoid column-side implicit conversion.
- Replace date-part predicates with half-open ranges.
- For recurring expression predicates, consider an indexed computed column only
  after proving the expression is stable, deterministic, and central to the
  workload.
- For leading-wildcard search, do not pretend a normal B-tree index fixes the
  predicate. Use a measured search-specific design such as computed/reversed
  columns only when it preserves behavior.

Representative source URLs:

- https://www.brentozar.com/archive/2018/03/cant-index-probably-not-sargable/
- https://www.brentozar.com/archive/2018/06/can-non-sargable-predicates-ever-seek/
- https://www.brentozar.com/archive/2018/12/when-does-sargability-matter-most/
- https://www.brentozar.com/archive/2020/02/where-getdate-between-startdate-and-enddate-is-hard-to-tune/
- https://www.brentozar.com/archive/2025/08/how-to-make-leading-wildcard-searches-fast/

### Key Order

Pick key order from the access pattern, not from a generic "most selective first"
rule. Equality predicates, range predicates, joins, grouping, ordering, and
window functions all compete for the leading key positions.

Required handling:

- Put equality predicates that materially reduce rows before range predicates.
- Choose among equality columns using selectivity, join/order requirements, and
  parameter-bucket stability.
- Put range predicates before columns that cannot be used after the range for
  seek narrowing, unless ORDER BY/GROUP BY evidence justifies a different order.
- Use ASC/DESC only when the plan needs that order and the benchmark proves it
  removes a sort or reduces reads.
- For GROUP BY, ORDER BY, and window functions, design the key to support both
  filtering and required ordering where practical.

Representative source URLs:

- https://www.brentozar.com/archive/2015/06/indexing-for-group-by/
- https://www.brentozar.com/archive/2015/06/indexing-for-windowing-functions/
- https://www.brentozar.com/archive/2018/04/index-key-column-order-and-supporting-sorts/
- https://www.brentozar.com/archive/2018/06/does-it-matter-which-field-goes-first-in-an-index/
- https://www.brentozar.com/archive/2019/11/how-to-think-like-the-engine-index-column-order-matters-a-lot/
- https://www.brentozar.com/archive/2022/01/when-should-you-use-desc-in-indexes/

### INCLUDE and Covering

Cover only the columns needed to remove a proven lookup, sort, hash, or wide base
table access. INCLUDE columns are not free: they increase storage, memory, write
cost, and maintenance time.

Required handling:

- Tie every INCLUDE column to SELECT output, join predicate, filter predicate,
  grouping, ordering, or lookup removal.
- Remember that clustered index key columns are already carried in nonclustered
  indexes. Do not redundantly include them unless metadata proves a different
  base structure.
- Prefer a narrow index that removes the worst lookup over a wide "cover
  everything" index when write overhead is material.
- Estimate size and write overhead before recommending permanent DDL.

Representative source URLs:

- https://www.brentozar.com/archive/2013/07/how-to-find-secret-columns-in-nonclustered-indexes/
- https://www.brentozar.com/archive/2015/04/index-included-columns-multi-column-statistics/
- https://www.brentozar.com/archive/2015/08/clustered-index-key-columns-in-nonclustered-indexes/
- https://www.brentozar.com/archive/2019/10/how-to-think-like-the-sql-server-engine-whats-a-key-lookup/
- https://www.brentozar.com/archive/2019/11/how-to-think-like-the-sql-server-engine-included-columns-arent-free/
- https://www.brentozar.com/archive/2019/11/how-to-think-like-the-engine-should-columns-go-in-the-key-or-the-includes/

SQLWorkbooks source anchors:

- `how_index_keys_and_includes_work/How-Keys-and-Included-Columns-Work.sql`
- `learn_indexing_by_solving_problems/Topic-3_Keys-vs-Includes_Solution.sql`
- `learn_indexing_by_solving_problems/Topic-7_Choosing-Between-Similar-Indexes_Solution.sql`

### Filtered Indexes

Use filtered indexes only when the query predicate reliably matches the filter.
Parameterization, dynamic SQL, optional predicates, and subtle NULL semantics can
prevent usage or create fragile plans.

Required handling:

- Verify the exact predicate relationship between the query and filter.
- Check parameterized forms across representative values.
- Do not drop or merge filtered indexes as duplicates unless the filter
  semantics are truly covered.
- Include filter definition in the final overlap check.

Representative source URLs:

- https://www.brentozar.com/archive/2013/11/filtered-indexes-and-dynamic-sql/
- https://www.brentozar.com/archive/2013/11/what-you-can-and-cant-do-with-filtered-indexes/
- https://www.brentozar.com/archive/2015/02/filtered-indexes-or-in-sql-server/
- https://www.brentozar.com/archive/2015/09/filtered-indexes-and-is-not-null/
- https://www.brentozar.com/archive/2017/01/filtered-indexes-variables-less-doom-gloom/
- https://www.brentozar.com/archive/2018/10/filtered-indexes-vs-parameterization-again/

### Parameter Sensitivity

An index that helps one parameter value can hurt another. Do not validate index
design against one convenient execution.

Required handling:

- Test common, rare, high-cardinality, low-cardinality, NULL/optional, and
  boundary values when applicable.
- Compare compiled values, runtime values, actual rows, estimated rows, logical
  reads, and plan shape across buckets.
- Consider whether an index fix, rewrite, PSP behavior, statistics fix, or
  targeted hint is the least risky remedy.
- Do not default to `OPTIMIZE FOR UNKNOWN`.

Representative source URLs:

- https://www.brentozar.com/archive/2017/09/optional-parameters-missing-index-requests/
- https://www.brentozar.com/archive/2017/08/columnstore-indexes-rowgroup-elimination-parameter-sniffing-stored-procedures/
- https://www.brentozar.com/archive/2018/10/batch-mode-for-row-store-does-it-fix-parameter-sniffing/
- https://www.brentozar.com/archive/2026/05/free-webcast-fixing-parameter-sniffing-with-index-tuning/

### Duplicates, Overlap, and Drops

This skill optimizes one supplied query. It can identify duplicate or
near-duplicate index candidates, but it cannot prove a globally unused index
from one query.

Required handling:

- Compare keys, INCLUDE columns, filters, uniqueness, constraints, compression,
  partition alignment, and usage context.
- Drop recommendations are candidate-only unless there is broader workload
  evidence.
- Never drop primary keys, unique constraints, filtered indexes with distinct
  semantics, or indexes supporting unknown read/write paths.
- Prefer "merge this candidate into existing index X" over adding a low-value
  sibling index.

Representative source URLs:

- https://www.brentozar.com/archive/2009/07/tuning-tip-identify-overlapping-indexes/
- https://www.brentozar.com/archive/2018/10/index-tuning-week-how-many-indexes-are-too-many/
- https://www.brentozar.com/archive/2018/10/unused-indexes-are-they-really-unused-or-have-they-just-not-been-used-yet/
- https://www.brentozar.com/archive/2018/11/tales-from-overindexing-too-many-one-column-indexes/
- https://www.brentozar.com/archive/2024/10/how-many-indexes-is-too-many/

### Maintenance Is Not Query Tuning

Fragmentation, rebuilds, reorganizes, fill factor, and broad statistics jobs are
not default fixes for a slow query. They may be relevant only when the actual
plan/runtime evidence points there.

Required handling:

- Do not recommend rebuilds because a query is slow.
- Do not use fragmentation percentage as a substitute for logical reads,
  duration, CPU, waits, row estimates, or plan shape.
- Use targeted statistics review when cardinality is wrong.
- Treat ADR/RCSI and Azure SQL operational behavior as reasons to be conservative
  about rebuild advice.

Representative source URLs:

- https://www.brentozar.com/archive/2012/08/sql-server-index-fragmentation/
- https://www.brentozar.com/archive/2013/09/why-index-fragmentation-doesnt-matter-video/
- https://www.brentozar.com/archive/2014/12/rebuild-reorganize-set-index-maintenance-sql-server/
- https://www.brentozar.com/archive/2022/02/meme-week-setting-fill-factor-to-fix-fragmentation/
- https://www.brentozar.com/archive/2025/01/index-rebuilds-make-even-less-sense-with-adr-rcsi/

### Columnstore

Columnstore is a workload-shape decision, not a generic index upgrade.

Required handling:

- Consider it for large analytic scans, aggregations, and batch-mode-friendly
  patterns.
- Avoid it as a default fix for selective OLTP lookups.
- Test rowgroup elimination, ordered data behavior, lock/write side effects,
  actual logical reads, duration, and service-tier support.
- State when logical reads are not directly comparable to rowstore reads.

Representative source URLs:

- https://www.brentozar.com/archive/2014/03/add-nonclustered-indexes-clustered-columnstore-indexes/
- https://www.brentozar.com/archive/2017/08/columnstore-indexes-rowgroup-elimination-parameter-sniffing-stored-procedures/
- https://www.brentozar.com/archive/2020/06/when-a-columnstore-index-makes-your-query-fail/
- https://www.brentozar.com/archive/2022/07/columnstore-indexes-are-finally-sorted-in-sql-server-2022/
- https://www.brentozar.com/archive/2026/03/logical-reads-arent-repeatable-on-columnstore-indexes-sigh/

SQLWorkbooks source anchors:

- `execution_plans_partitioning_columnstore/02-Demo-Reading-Execution-Plans-Partitioned-Tables-Columnstore-Indexes.sql`
- `execution_plans_partitioning_columnstore/03-Batch-mode-hacks.sql`

### Temp Tables

Index temp tables only when the full statement sequence proves it helps. The
cost is create/load/update plus the later read, not the read alone.

Required handling:

- Measure temp table population cost and downstream read benefit together.
- Prefer indexing before load only when it helps constraints/order or avoids a
  later expensive build; otherwise test after load.
- Do not index table variables as a reflex. Consider compatibility level,
  deferred compilation, row-count variance, and recompilation cost.

Representative source URLs:

- https://www.brentozar.com/archive/2016/12/indexing-temp-tables/
- https://www.brentozar.com/archive/2017/04/ever-worth-adding-indexes-table-variables/
- https://www.brentozar.com/archive/2021/08/you-probably-shouldnt-index-your-temp-tables/

### Partitioning and Indexed Views

Partitioning and indexed views are exceptional design choices, not default
single-query index fixes.

Required handling:

- Recommend partitioning only when manageability, data movement, elimination, or
  aligned maintenance requirements justify it.
- Verify partition elimination in the actual plan; do not assume it.
- Recommend indexed views only when Azure SQL Database eligibility, SET options,
  write overhead, locking impact, and matching behavior are all acceptable.
- For a normal query rewrite, index the base tables unless the view is already an
  intentional indexed-view design.

Representative source URLs:

- https://www.brentozar.com/archive/2008/06/sql-server-partitioning-not-the-answer-to-everything/
- https://www.brentozar.com/archive/2013/11/filtered-indexes-vs-table-partitioning/
- https://www.brentozar.com/archive/2013/11/what-you-can-and-cant-do-with-indexed-views/
- https://www.brentozar.com/archive/2018/12/indexed-view-matching-with-group-by-and-distinct/
- https://www.brentozar.com/archive/2025/02/vertical-partitioning-is-almost-never-the-answer-heres-why/

SQLWorkbooks source anchors:

- `table_partitioning_performance/Why-Table-Partitioning-Does-Not-Speed-Up-Query-Performance_Demo.sql`
- `execution_plans_partitioning_columnstore/02-Demo-Reading-Execution-Plans-Partitioned-Tables-Columnstore-Indexes.sql`

### One-Query Index Design Exercises

The Kendra course and SQLWorkbooks material repeatedly frames index design as a
measured exercise against a specific query, not as a static rule list.

Required handling:

- Define "best" for the supplied query before testing: usually lower logical
  reads, duration, CPU, spills, or blocking footprint while preserving results.
- Test multiple plausible index shapes when key order is ambiguous.
- Do not keep an index merely because it looks elegant; keep it because it wins
  the measured scenario and has acceptable write/storage cost.
- When advanced features are out of scope for the user's request, explicitly
  constrain the solution to rowstore/nonclustered/basic DDL and state what was
  not tested.

Representative source URLs:

- https://kendralittle.com/course/design-the-best-index-for-one-year-wonders-sqlchallenge/
- https://kendralittle.com/course/learn-indexing-by-solving-problems-sql-seminar-june-2018/

SQLWorkbooks source anchors:

- `index_one_year_wonders_sqlchallenge/01_Index-One-Year-Wonders_SQLChallenge_Problem.sql`
- `index_one_year_wonders_sqlchallenge/03_Index-One-Year-Wonders_SQLChallenge_Levels-2-and-3-Solutions.sql`
- `learn_indexing_by_solving_problems/Topic-2_Nonclustered-Key-Choice_Solution.sql`

### GROUP BY, TOP, ORDER BY, and Window Functions

These query shapes often need ordering support as much as filtering support.

Required handling:

- Inspect whether the current plan pays for a Sort, Window Aggregate, Segment,
  Sequence Project, Hash Aggregate, or large memory grant.
- Design the key around the needed partition/order/group columns when that can
  remove a blocking operator without exploding reads.
- For `TOP` with `ORDER BY`, verify whether the index can satisfy both the row
  goal and the ordering contract.
- For window functions, test rowstore and columnstore patterns separately when
  both are plausible; do not assume columnstore is better for every windowed
  query.

Representative source URLs:

- https://kendralittle.com/course/indexing-for-windowing-functions/
- https://kendralittle.com/course/learn-indexing-by-solving-problems-sql-seminar-june-2018/topic-4-indexing-top-order-by-session-recording/
- https://kendralittle.com/course/learn-indexing-by-solving-problems-sql-seminar-june-2018/topic-6-indexing-for-group-by-session-recording/

SQLWorkbooks source anchors:

- `indexing_for_windowing_functions/Indexing-for-Windowing-Functions.sql`
- `learn_indexing_by_solving_problems/Topic-4_Indexing-Top-Order-By_Solution.sql`
- `learn_indexing_by_solving_problems/Topic-6_Indexing-for-Group-By_Solution.sql`

### Regression Testing: Indexes Can Make Queries Slower

Adding an index can make the optimizer choose a worse plan. The optimized+indexes
scenario exists to catch this before a recommendation ships.

Required handling:

- Benchmark the rewrite without indexes before testing DDL.
- Test the index with the same parameter buckets, result-equivalence proof, and
  metrics as the baseline.
- Compare plan shape, not just duration: a faster-looking single run can hide a
  fragile lookup, bad join order, or parameter-sensitive plan.
- If a test index makes the query slower, report it under "Indexes tested but
  not recommended" and explain the plan regression.

Representative source URLs:

- https://kendralittle.com/course/why-creating-an-index-can-make-a-query-slower/

SQLWorkbooks source anchors:

- `why_creating_an_index_can_slow_down_a_query/01-query-tuning-challenge-problem.sql`
- `why_creating_an_index_can_slow_down_a_query/02-query-tuning-challenge-solution.sql`
- `why_creating_an_index_can_slow_down_a_query/actual-plan-before-adding-index.sqlplan`
- `why_creating_an_index_can_slow_down_a_query/actual-plan-after-adding-index.sqlplan`

## Output Requirements

In the final response, every index recommendation must include:

- The actual plan problem it addresses.
- The existing-index overlap result.
- The selected key order and why.
- The INCLUDE columns and why each belongs.
- Filter predicate, if any, and why the query reliably matches it.
- Expected plan change.
- Expected read benefit and write/storage cost.
- Whether it was benchmarked in the optimized+indexes scenario.
- CREATE script and rollback DROP script.
- Any parameter buckets where it was not proven.

Do not cite this guide as external authority in the user-facing answer. Use it to
shape the recommendation and cite the concrete source URLs only when useful.
