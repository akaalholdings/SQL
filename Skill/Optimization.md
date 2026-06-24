# Environment-Level Optimization Guidance (Azure SQL Database)

Scope: these are database/environment actions for the **user** to run — they cannot be applied by rewriting a query. Reference them in the "Azure SQL Database notes" section of a response only when the evidence in the query or plan points to them (e.g., stale-statistics symptoms, recurring plan regressions, heavy fragmentation hints from the user). Do not recite this list wholesale.

## Optimization Steps

1. **Enable and configure Query Store**
   Query Store is enabled in **READ_WRITE** mode by default on Azure SQL Database — verify it has not been disabled or flipped to READ_ONLY (often caused by hitting the size quota). It captures query text, execution plans, and runtime stats, enabling comparison of current performance with historical baselines. Configure retention and cleanup to keep size manageable, and use Query Performance Insight in the Azure portal to spot long-running or regressed queries.

2. **Collect performance and wait statistics**
   Use the database-scoped DMVs to diagnose bottlenecks:
   * `sys.dm_db_wait_stats` for top waits inside the database.
   * `sys.dm_db_resource_stats` for CPU, memory, and I/O pressure relative to the service objective's limits (a query that is "slow" may simply be hitting the tier's resource caps — scaling up is then the fix, not rewriting).
   * `sys.dm_io_virtual_file_stats` for storage latency.
   Correlate the dominant waits with resource metrics to determine whether tuning or scaling is required.

3. **Identify and prioritise heavy queries**
   Query `sys.dm_exec_query_stats` with `CROSS APPLY sys.dm_exec_sql_text(sql_handle)` (and `sys.dm_exec_query_plan(plan_handle)`), sorting by `total_worker_time`, `total_logical_reads`, or `execution_count`. Tackle queries with the highest cumulative resource usage first — optimising a query executed thousands of times yields the greatest payoff. The free First Responder Kit procedures (**sp_BlitzCache**, **sp_BlitzIndex**) run on Azure SQL Database (with some feature limitations) and help surface the worst offenders.

4. **Examine execution plans and apply tuning recommendations**
   For each high-impact query, inspect the actual plan. Look for expensive operators (scans, lookups, sorts, hash matches). Validate missing-index hints, but verify that recommended indexes align with workload patterns. If an optimal plan regresses, use Query Store plan forcing (with `OPTIMIZED_PLAN_FORCING = ON` to cut recompile cost) to stabilise performance while you investigate. Also review the built-in Automatic Tuning recommendations (CREATE INDEX / DROP INDEX / FORCE LAST GOOD PLAN) in the portal or `sys.dm_db_tuning_recommendations` — FORCE_LAST_GOOD_PLAN is on by default.

5. **Maintain indexes with minimal effort**
   Schedule index maintenance tuned to realistic thresholds:
   * Skip tiny indexes (< 30,000 pages).
   * Reorganise when fragmentation ≥ 50%.
   * Rebuild online when fragmentation ≥ 80%.
   This preserves page density, improves range-scan performance, and reduces buffer-pool waste without excessive CPU consumption. Azure SQL Database has no SQL Agent — schedule maintenance with Elastic Jobs, Azure Automation runbooks, or Logic Apps during off-peak hours.

6. **Update statistics regularly**
   After significant data changes or bulk loads, run `UPDATE STATISTICS` (FULLSCAN on large tables when feasible). Fresh statistics improve cardinality estimates and reduce the risk of poor plan choices. Consider enabling `AUTO_UPDATE_STATISTICS_ASYNC` so automatic updates do not block the triggering query.

7. **Mitigate parameter sniffing and enforce plan stability**
   If a query performs inconsistently for different parameter values, apply one of these remedies (see queryguide Phase 3.1 for the full decision order, including compatibility level 160 Parameter Sensitive Plan optimization):
   * `OPTION (RECOMPILE)` for per-execution optimisation when the overhead is acceptable.
   * `OPTION (OPTIMIZE FOR (@param = <typical_value>))` to guide the optimiser toward a representative plan.
   * Query Store hints via `sys.sp_query_store_set_hints` when the query text cannot be changed.
   * Query Store plan forcing to lock in a known-good plan.

8. **Refactor inefficient query patterns**
   * Replace large table variables with temporary tables when row counts are volatile or the compatibility level is below 150 (see queryguide 1.3).
   * Convert cursors and row-by-row loops to set-based operations using joins or window functions.
   * Eliminate scalar UDFs where possible; inline logic or rewrite as inline table-valued functions.
   These changes typically reduce CPU time and I/O, and improve parallelism.
