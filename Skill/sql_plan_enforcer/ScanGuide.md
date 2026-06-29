# Scan Guide

Find tuning candidates by reading **Query Store** (read-only) through `query_geneva_db`. Run all four scans, concatenate their row arrays into a single JSON list, and pipe it to `scan_rank.py`, which scores, tiers, and threshold-gates them into one prioritized list.

```bash
query_geneva_db mid --dba --query-file /tmp/scan_regression.sql --format json     # -> rows
query_geneva_db mid --dba --query-file /tmp/scan_top.sql --format json
query_geneva_db mid --dba --query-file /tmp/scan_paramsens.sql --format json
query_geneva_db mid --dba --query-file /tmp/scan_stale.sql --format json
# merge the four "rows" arrays into one /tmp/candidates.json, then:
python3 scan_rank.py --input /tmp/candidates.json --eligible-only --limit 5
```

Scan read-only (`mid` replica by default). Forcing/hints target the workload database (`mid_prod`) later, per `EnforceGuide.md`.

## Units and conventions

- Query Store durations and CPU are **microseconds**. `scan_rank.py` is ratio-based, so units cancel.
- Window aggregates are **executions-weighted**: `SUM(avg_metric * count_executions) / SUM(count_executions)`.
- Every scan emits a `category` column matching one of: `regression`, `top_consumer`, `param_sensitive`, `stale_forced`. `scan_rank.py` keys off it.
- Tune the window and floors with the `DECLARE` block at the top of each script (mirror the thresholds in `scan_rank.py` so the SQL pre-filter and the ranker agree).

Catalog views used: `sys.query_store_query`, `sys.query_store_plan`, `sys.query_store_runtime_stats`, `sys.query_store_runtime_stats_interval`.

---

## 1. Plan regressions (`/tmp/scan_regression.sql`)

A query whose current plan is materially slower than a still-present, known-good plan. The fix is to force the good plan.

```sql
DECLARE @window_start datetime2 = DATEADD(DAY, -7, SYSUTCDATETIME());
DECLARE @min_executions bigint = 30;
DECLARE @regression_factor float = 1.5;   /* current >= 1.5x the good plan's duration */

WITH
    plan_stats AS
(
    SELECT
        p.query_id,
        rs.plan_id,
        SUM(rs.count_executions) AS executions,
        SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS avg_duration,
        MAX(rs.last_execution_time) AS last_execution_time
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON rs.plan_id = p.plan_id
    WHERE rsi.start_time >= @window_start
    GROUP BY p.query_id, rs.plan_id
),
    current_plan AS
(
    SELECT
        query_id,
        plan_id,
        avg_duration,
        executions,
        ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY last_execution_time DESC) AS rn
    FROM plan_stats
),
    best_plan AS
(
    SELECT
        query_id,
        plan_id,
        avg_duration,
        executions,
        ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY avg_duration ASC) AS rn
    FROM plan_stats
    WHERE executions >= @min_executions
)
SELECT
    category = 'regression',
    c.query_id,
    current_plan_id = c.plan_id,
    proposed_plan_id = b.plan_id,
    count_executions = c.executions,
    c.avg_duration,
    best_avg_duration = b.avg_duration,
    regression_pct = (c.avg_duration - b.avg_duration) / NULLIF(b.avg_duration, 0),
    proposed_lever = 'force_plan'
FROM current_plan AS c
INNER JOIN best_plan AS b
    ON c.query_id = b.query_id
WHERE c.rn = 1
    AND b.rn = 1
    AND b.plan_id <> c.plan_id
    AND c.avg_duration > b.avg_duration * @regression_factor
ORDER BY regression_pct * c.executions DESC;
```

---

## 2. Top resource consumers (`/tmp/scan_top.sql`)

The queries costing the fleet the most overall. Multi-plan ones may be forceable; single-plan stable ones are usually rewrite jobs for `sql-optimizer` (`scan_rank.py` marks them `handoff_optimizer` when no `proposed_plan_id` is present).

```sql
DECLARE @window_start datetime2 = DATEADD(DAY, -7, SYSUTCDATETIME());
DECLARE @min_executions bigint = 30;
DECLARE @top_n int = 25;

WITH
    query_stats AS
(
    SELECT
        p.query_id,
        count_executions = SUM(rs.count_executions),
        avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0),
        total_duration = SUM(rs.avg_duration * rs.count_executions),
        total_cpu_time = SUM(rs.avg_cpu_time * rs.count_executions),
        total_logical_reads = SUM(rs.avg_logical_io_reads * rs.count_executions),
        distinct_plans = COUNT(DISTINCT rs.plan_id)
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON rs.plan_id = p.plan_id
    WHERE rsi.start_time >= @window_start
    GROUP BY p.query_id
)
SELECT TOP (@top_n)
    category = 'top_consumer',
    query_id,
    count_executions,
    avg_duration,
    total_duration,
    total_cpu_time,
    total_logical_reads,
    distinct_plans
FROM query_stats
WHERE count_executions >= @min_executions
ORDER BY total_duration DESC;
```

---

## 3. Parameter-sensitive / high-variance (`/tmp/scan_paramsens.sql`)

A query with high run-to-run variance — a parameter-sniffing suspect. The fix is a Query Store hint (`OPTION(RECOMPILE)`, `OPTIMIZE FOR UNKNOWN`), per `EnforceGuide.md`. Reuse the diagnosis in `sql-optimizer`'s `queryguide.md` §3.1.

```sql
DECLARE @window_start datetime2 = DATEADD(DAY, -7, SYSUTCDATETIME());
DECLARE @min_executions bigint = 30;
DECLARE @min_avg_duration float = 1000.0;   /* microseconds; skip trivially fast queries */
DECLARE @min_cv float = 0.5;                 /* coefficient of variation floor */

WITH
    plan_stats AS
(
    SELECT
        p.query_id,
        rs.plan_id,
        count_executions = SUM(rs.count_executions),
        avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0),
        stdev_duration = SUM(rs.stdev_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0)
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p
        ON rs.plan_id = p.plan_id
    WHERE rsi.start_time >= @window_start
    GROUP BY p.query_id, rs.plan_id
)
SELECT
    category = 'param_sensitive',
    query_id,
    current_plan_id = plan_id,
    count_executions,
    avg_duration,
    stdev_duration,
    coefficient_of_variation = stdev_duration / NULLIF(avg_duration, 0),
    proposed_lever = 'set_hints'
FROM plan_stats
WHERE count_executions >= @min_executions
    AND avg_duration >= @min_avg_duration
    AND stdev_duration / NULLIF(avg_duration, 0) >= @min_cv
ORDER BY (stdev_duration / NULLIF(avg_duration, 0)) * avg_duration * count_executions DESC;
```

---

## 4. Stale / failing forced plans (`/tmp/scan_stale.sql`)

Plans already forced (manually or by Automatic Tuning) that are now **failing to force** (`force_failure_count > 0`) or have been **beaten** by a newer non-forced plan. These are re-evaluated first (tier 0 when failing).

```sql
DECLARE @window_start datetime2 = DATEADD(DAY, -7, SYSUTCDATETIME());
DECLARE @min_executions bigint = 30;
DECLARE @better_factor float = 0.8;   /* alternate plan <= 80% of the forced plan's duration */

WITH
    plan_stats AS
(
    SELECT
        rs.plan_id,
        count_executions = SUM(rs.count_executions),
        avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0)
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    WHERE rsi.start_time >= @window_start
    GROUP BY rs.plan_id
)
SELECT
    category = 'stale_forced',
    p.query_id,
    current_plan_id = p.plan_id,
    p.force_failure_count,
    last_force_failure_reason = p.last_force_failure_reason_desc,
    count_executions = ISNULL(ps.count_executions, 0),
    avg_duration = ISNULL(ps.avg_duration, 0),
    proposed_plan_id = alt.plan_id,
    alt_avg_duration = alt.avg_duration,
    proposed_lever = 'unforce_plan'
FROM sys.query_store_plan AS p
LEFT JOIN plan_stats AS ps
    ON p.plan_id = ps.plan_id
OUTER APPLY
(
    SELECT TOP (1)
        ps2.plan_id,
        ps2.avg_duration
    FROM plan_stats AS ps2
    INNER JOIN sys.query_store_plan AS p2
        ON ps2.plan_id = p2.plan_id
    WHERE p2.query_id = p.query_id
        AND p2.plan_id <> p.plan_id
        AND ps2.count_executions >= @min_executions
        AND ps2.avg_duration < ISNULL(ps.avg_duration, ps2.avg_duration) * @better_factor
    ORDER BY ps2.avg_duration ASC
) AS alt
WHERE p.is_forced_plan = 1
    AND (p.force_failure_count > 0 OR alt.plan_id IS NOT NULL);
```

---

## Ranking

`scan_rank.py` produces, per candidate: `tier` (0 = most urgent), `score`, `proposed_lever`, `total_cost`, `eligible`, and a `reason`. It sorts eligible candidates first within each tier. Take the top *N* eligible (the blast-radius cap) into `EnforceGuide.md`; everything `handoff_optimizer` goes to `sql-optimizer`, not forced here.
