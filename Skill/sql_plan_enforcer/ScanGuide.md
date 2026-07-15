# Scan Guide

Find tuning candidates by reading **Query Store** (read-only) through `azure-sql-mcp`. The primary path uses the server's dedicated detection tools; the appendix keeps hand-written scans for cases the tools don't cover.

## Primary path — server scan tools

One call per category (databases are whatever `AZURE_SQL_ALLOWED_DATABASES` configures — call `list_databases` and pick the workload DB):

```
get_database_configuration(database_name=<workload db>)                       # Query Store + Automatic Tuning ownership
detect_regressed_queries(window_minutes=10080, database_name=<workload db>)     # regressions (automatic-tuning based)
get_top_queries(sort_by="total_duration", window_minutes=10080, limit=25, database_name=<workload db>)  # top consumers
detect_parameter_sniffing(window_minutes=10080, database_name=<workload db>)    # param-sensitive
get_forced_plans(window_minutes=10080, database_name=<workload db>)             # stale/failing forced plans
```

Save each payload to a file, then normalize and rank:

```bash
python3 scan_adapter.py --input /tmp/reg.json --input /tmp/top.json \
    --input /tmp/sniff.json --input /tmp/forced.json > /tmp/candidates.json
python3 scan_rank.py --input /tmp/candidates.json --eligible-only --limit 5
```

`scan_adapter.py` converts each tool's payload (field names, ms↔µs units, string ids) into the candidate schema and tags every candidate `adapted_from: "<tool>"`; `scan_rank.py` stays the single ranking brain. Adapter caveats to keep in mind:

- Every payload must identify its database with `database_name` (or an explicit
  environment field). That value is copied to each candidate. Missing or placeholder
  environments are ineligible because a query id without its database is ambiguous.
- A `truncated` result is copied to candidates and makes them ineligible. Do not rank a
  partial scan as though it covered the workload.

- **`detect_regressed_queries` is automatic-tuning based** — an empty result does NOT mean no regressions exist (quiet databases and disabled automatic tuning both return nothing). If it comes back empty, run the fallback regression scan below.
- Every row from `sys.dm_db_tuning_recommendations` remains **review-only** in this custom
  loop. Preserve the documented recommendation state (`Active`, `Verifying`, `Success`,
  `Reverted`, or `Expired`) and action initiator, but never feed the row itself into custom
  apply. `Active` may be applied manually or by `FORCE_LAST_GOOD_PLAN`; `Verifying` and
  `Success` may already be engine-owned; `Reverted` and `Expired` are not valid apply
  candidates. Build a separate candidate from the fallback Query Store scan only after
  confirming there is no overlapping tuning recommendation and the configuration output
  does not give Automatic Tuning ownership of the action.
- `get_forced_plans` carries `plan_forcing_type_desc`. Only `MANUAL` forced plans can be
  eligible for custom unforce. `AUTO` and missing/unknown ownership remain review-only.
- In `get_database_configuration.automatic_tuning_options`, if `FORCE_LAST_GOOD_PLAN`
  has `actual_state_desc = ON`, do not custom-force an overlapping regression. Report it
  and let the engine complete or revert its own verification. This skill never changes the
  Automatic Tuning configuration.
- **`detect_parameter_sniffing` has no stdev** — the adapter synthesizes a spread proxy from best/worst duration, which is not numerically comparable to the fallback scan's coefficient of variation (`SQL_PLAN_ENFORCER_MIN_CV` was calibrated against the latter). The fallback scan is the precision path when a param-sensitive candidate is borderline.
- `plan_health_review(window_minutes=1440, top_n=20, database_name=...)` also adapts, but its actions lack Query Store metrics (only failing forced plans rank eligible from it) — use it as a snapshot, not the ranking source. `plan_enforcer_tick` is **preview-only**: never call it with `dry_run=false`; applies go exclusively through `force_query_plan` under this skill's ledger, or the server tick and this loop can double-apply.

## Fallback — custom scans via `execute_sql`

Use these when: `detect_regressed_queries` returned empty, a param-sensitive candidate needs the true CV, results were truncated, or you need custom windows/thresholds. `execute_sql` validates every call through `SafeSqlValidator` (a `sqlglot` T-SQL AST parse), which allows exactly one read-only statement:

- **CTEs are allowed** — a leading `WITH` is fine. The four scans below use derived tables (subqueries in `FROM`) rather than CTEs for historical reasons only; either form works, so don't feel constrained to derived tables when writing new scans.
- Rejects multiple statements and any DML/DDL/`EXEC`/`SELECT *`.

Thresholds in the queries below are **inlined literals** (no `DECLARE` — multi-statement batches aren't allowed either way). Tunable values are called out per query; edit them in the SQL text.

```
execute_sql(sql=<contents of scan_regression.sql>, database_name=<workload db>)
execute_sql(sql=<contents of scan_top.sql>, database_name=<workload db>)
execute_sql(sql=<contents of scan_param_sensitive.sql>, database_name=<workload db>)
execute_sql(sql=<contents of scan_stale_forced.sql>, database_name=<workload db>)
```

```bash
# merge the four `rows` arrays into one list -> /tmp/candidates.json (no adapter needed —
# these scans already emit the candidate schema)
python3 scan_rank.py --input /tmp/candidates.json --eligible-only --limit 5
```

`execute_sql` returns structured rows directly. There is no per-call row-limit override: `execute_sql` always truncates at the server's `AZURE_SQL_ROW_LIMIT` (default 200) and sets `truncated: true` when it does. Only `top_consumer` caps itself (`TOP (25)`) — the other three scans are uncapped, so check `truncated` on each call; a truncated result is not eligible for ranking or enforcement. Ask the server operator to raise `AZURE_SQL_ROW_LIMIT` or narrow the scan before continuing. Tunables (window, floors) are repeated as literals in each query — keep them in sync with the thresholds in `scan_rank.py`.

Catalog views: `sys.query_store_query`, `sys.query_store_plan`, `sys.query_store_runtime_stats`, `sys.query_store_runtime_stats_interval`. Durations/CPU are **microseconds**; window aggregates are executions-weighted.

---

## 1. Plan regressions (`/tmp/scan_regression.sql`)

Current plan materially slower than a still-present, known-good plan → force the good plan. Tunables: window `-7` days, `min_executions 30`, regression factor `1.5`.

```sql
SELECT
    category = 'regression',
    cur.query_id,
    current_plan_id = cur.plan_id,
    proposed_plan_id = best.plan_id,
    count_executions = cur.executions,
    cur.avg_duration,
    best_avg_duration = best.avg_duration,
    regression_pct = (cur.avg_duration - best.avg_duration) / NULLIF(best.avg_duration, 0),
    proposed_lever = 'force_plan'
FROM
(
    SELECT
        query_id, plan_id, avg_duration, executions,
        rn = ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY last_execution_time DESC)
    FROM
    (
        SELECT
            p.query_id, rs.plan_id,
            executions = SUM(rs.count_executions),
            avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0),
            last_execution_time = MAX(rs.last_execution_time)
        FROM sys.query_store_runtime_stats AS rs
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        INNER JOIN sys.query_store_plan AS p ON rs.plan_id = p.plan_id
        WHERE rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
        GROUP BY p.query_id, rs.plan_id
    ) AS ps
) AS cur
INNER JOIN
(
    SELECT
        query_id, plan_id, avg_duration, executions,
        rn = ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY avg_duration ASC)
    FROM
    (
        SELECT
            p.query_id, rs.plan_id,
            executions = SUM(rs.count_executions),
            avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0)
        FROM sys.query_store_runtime_stats AS rs
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
        INNER JOIN sys.query_store_plan AS p ON rs.plan_id = p.plan_id
        WHERE rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
        GROUP BY p.query_id, rs.plan_id
    ) AS ps
    WHERE ps.executions >= 30
) AS best ON cur.query_id = best.query_id
WHERE cur.rn = 1 AND best.rn = 1 AND best.plan_id <> cur.plan_id
    AND cur.avg_duration > best.avg_duration * 1.5
ORDER BY (cur.avg_duration - best.avg_duration) / NULLIF(best.avg_duration, 0) * cur.executions DESC
```

---

## 2. Top resource consumers (`/tmp/scan_top.sql`)

Highest aggregate cost. Multi-plan ones may be forceable; single-plan stable ones are rewrite jobs for `sql-optimizer` (`scan_rank.py` marks them `handoff_optimizer`). Tunables: window `-7` days, `min_executions 30`, `TOP (25)`.

```sql
SELECT TOP (25)
    category = 'top_consumer',
    qs.query_id,
    qs.count_executions,
    qs.avg_duration,
    qs.total_duration,
    qs.total_cpu_time,
    qs.total_logical_reads,
    qs.distinct_plans
FROM
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
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p ON rs.plan_id = p.plan_id
    WHERE rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
    GROUP BY p.query_id
) AS qs
WHERE qs.count_executions >= 30
ORDER BY qs.total_duration DESC
```

---

## 3. Parameter-sensitive / high-variance (`/tmp/scan_param_sensitive.sql`)

High run-to-run variance → a Query Store hint (`EnforceGuide.md`). Tunables: window `-7` days, `min_executions 30`, `min_avg_duration 1000.0` (microseconds), `min_cv 0.5`.

```sql
SELECT
    category = 'param_sensitive',
    ps.query_id,
    current_plan_id = ps.plan_id,
    ps.count_executions,
    ps.avg_duration,
    ps.stdev_duration,
    coefficient_of_variation = ps.stdev_duration / NULLIF(ps.avg_duration, 0),
    proposed_lever = 'set_hints'
FROM
(
    SELECT
        p.query_id, rs.plan_id,
        count_executions = SUM(rs.count_executions),
        avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0),
        stdev_duration = SUM(rs.stdev_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0)
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    INNER JOIN sys.query_store_plan AS p ON rs.plan_id = p.plan_id
    WHERE rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
    GROUP BY p.query_id, rs.plan_id
) AS ps
WHERE ps.count_executions >= 30 AND ps.avg_duration >= 1000.0
    AND ps.stdev_duration / NULLIF(ps.avg_duration, 0) >= 0.5
ORDER BY (ps.stdev_duration / NULLIF(ps.avg_duration, 0)) * ps.avg_duration * ps.count_executions DESC
```

---

## 4. Stale / failing forced plans (`/tmp/scan_stale_forced.sql`)

Forced plans now **failing to force** (`force_failure_count > 0`) or **beaten** by a newer plan. Re-evaluated first (tier 0 when failing). The richer, attributed unforce-failed detector is in `ReviewGuide.md`. Tunables: window `-7` days, `min_executions 30`, better-factor `0.8`.

```sql
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
LEFT JOIN
(
    SELECT
        rs.plan_id,
        count_executions = SUM(rs.count_executions),
        avg_duration = SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0)
    FROM sys.query_store_runtime_stats AS rs
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
    WHERE rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
    GROUP BY rs.plan_id
) AS ps ON p.plan_id = ps.plan_id
OUTER APPLY
(
    SELECT TOP (1) ps2.plan_id, ps2.avg_duration
    FROM
    (
        SELECT
            rs2.plan_id,
            count_executions = SUM(rs2.count_executions),
            avg_duration = SUM(rs2.avg_duration * rs2.count_executions) / NULLIF(SUM(rs2.count_executions), 0)
        FROM sys.query_store_runtime_stats AS rs2
        INNER JOIN sys.query_store_runtime_stats_interval AS rsi2 ON rs2.runtime_stats_interval_id = rsi2.runtime_stats_interval_id
        WHERE rsi2.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
        GROUP BY rs2.plan_id
    ) AS ps2
    INNER JOIN sys.query_store_plan AS p2 ON ps2.plan_id = p2.plan_id
    WHERE p2.query_id = p.query_id AND p2.plan_id <> p.plan_id
        AND ps2.count_executions >= 30
        AND ps2.avg_duration < ISNULL(ps.avg_duration, ps2.avg_duration) * 0.8
    ORDER BY ps2.avg_duration ASC
) AS alt
WHERE p.is_forced_plan = 1 AND (p.force_failure_count > 0 OR alt.plan_id IS NOT NULL)
```

---

## Ranking

`scan_rank.py` annotates each candidate with `tier` (0 = most urgent), `score`, `proposed_lever`, `eligible`, and a `reason`, eligible-first within each tier. Take the top *N* eligible (blast-radius cap) into `EnforceGuide.md`; `handoff_optimizer` items go to `sql-optimizer`.
