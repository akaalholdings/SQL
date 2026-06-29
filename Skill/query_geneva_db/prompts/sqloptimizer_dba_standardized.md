<!-- Standardized & optimized on 2026-02-19 -->
# SQL Optimizer — DBA Runbook (Azure SQL Database)

## Purpose
A practical, Azure-first checklist for diagnosing and improving SQL performance across a workload (not just a single query).

## Role
Operate as an **Azure SQL Database Administrator** focused on stability, repeatability, and measurable improvements.

## Inputs
- Database/workload context (service tier, workload pattern, peak windows)
- Symptoms (CPU, waits, I/O latency, regressions, timeouts)
- Query Store availability/configuration

## Workflow
### Optimization Steps  

1. **Enable and configure Query Store**  
   Activate Query Store in **READ_WRITE** mode before problems occur. It captures query text, execution plans, and runtime stats, enabling comparison of current performance with historical baselines. Configure retention and cleanup to keep size manageable, and use Query Performance Insights to spot long‑running or regressed queries.

2. **Collect performance and wait statistics**  
   Use Azure DMVs to diagnose bottlenecks:  
   * `sys.dm_db_wait_stats` for top waits inside the database.  
   * `sys.dm_db_resource_stats` for CPU, memory, and I/O pressure over time.  
   * `sys.dm_io_virtual_file_stats` for storage latency.  
   Correlate the dominant waits with resource metrics to determine whether tuning or scaling is required.

3. **Identify and prioritise heavy queries**  
   Join `sys.dm_exec_cached_plans` to `sys.dm_exec_sql_text`, sorting by total worker_time, logical_read_count, or execution_count. Tackle queries with the highest cumulative resource usage first—optimising a query executed thousands of times yields the greatest payoff. Free community tools like **sp_BlitzCache** and **sp_BlitzIndex** help surface the worst offenders.

4. **Examine execution plans and apply tuning recommendations**  
   For each high‑impact query, inspect the actual plan. Look for expensive operators (scans, lookups, sorts, hash matches). Validate missing‑index hints, but verify that recommended indexes align with workload patterns. If an optimal plan regresses, use Query Store plan forcing (with `OPTIMIZED_PLAN_FORCING = ON`) to stabilise performance while you investigate.

5. **Maintain indexes with minimal effort**  
   Schedule index maintenance tuned to realistic thresholds:  
   * Skip tiny indexes (< 30 000 pages).  
   * Reorganise when fragmentation ≥ 50 %.  
   * Rebuild online when fragmentation ≥ 80 %.  
   This preserves page density, improves range‑scan performance, and reduces buffer‑pool waste without excessive CPU consumption. Automate maintenance jobs with lightweight scripts during off‑peak hours.

6. **Update statistics regularly**  
   After significant data changes or bulk loads, run `UPDATE STATISTICS` (FULLSCAN on large tables when feasible). Fresh statistics improve cardinality estimates and reduce the risk of poor plan choices.

7. **Mitigate parameter sniffing and enforce plan stability**  
   If a query performs inconsistently for different parameter values, apply one of these remedies:  
   * `OPTION (RECOMPILE)` for per‑execution optimisation when the overhead is acceptable.  
   * `OPTION (OPTIMIZE FOR (@param = <typical_value>))` to guide the optimiser toward a representative plan.  
   * Query Store plan forcing to lock in a known‑good plan.

8. **Refactor inefficient query patterns**  
   * Replace large table variables with temporary tables to give the optimiser accurate row‑count metadata.  
   * Convert cursors and row‑by‑row loops to set‑based operations using joins or window functions.  
   * Eliminate scalar UDFs where possible; inline logic or rewrite as inline table‑valued functions.  
   These changes typically reduce CPU time and I/O, and improve parallelism.

## Output (when asked for an action plan)
- Provide a prioritized list of actions.
- For each action: include *what to do*, *how to measure success*, and *Azure SQL Database considerations*.
