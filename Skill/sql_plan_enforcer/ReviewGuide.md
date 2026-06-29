# Review Guide

**Review mode = monitor only.** Scan Query Store, diagnose, and produce a read-only Plan Health Report. Change **nothing**: no apply, no dry-run scripts staged for execution, no state writes, no ledger entries. Every "recommended action" in the report is informational — what enforce mode *would* do. This is the safest mode and a good default for "just tell me what's wrong with my plans."

Review mode ignores the apply gate entirely because it never applies. It does not require `SQL_PLAN_ENFORCER_APPLY` or an allowlist.

## Workflow

1. **Scan (read-only)** — run the four scans from `ScanGuide.md` **plus** the failing-forced-plan detector below, all through `query_geneva_db <env> --dba --query-file ... --format json`. Use `mid` (read-only replica) for evidence.
2. **Rank** — merge the rows and run `scan_rank.py` (this annotates `eligible` / `score` / `reason`; review mode keeps ineligible rows too, as monitoring-only items).
3. **Report** — pipe the ranked candidates to `review_report.py`:

   ```bash
   python3 scan_rank.py --input /tmp/candidates.json | python3 review_report.py
   python3 review_report.py --input /tmp/ranked.json --json   # machine-readable
   ```

   The report groups issues by severity (failing forced plans = **critical**, regressions = **high**, parameter-sensitive / beaten forced plans = **medium**, top consumers = **low**) and shows the action enforce mode would take — without taking it.

## Failing forced plans (unforce-failed detector)

A forced plan with `force_failure_count > 0` is **actively broken**: SQL Server cannot apply the forced plan, which can cause general failure and slower compile times. This detector finds them and surfaces the exact unforce command.

Adapted for **Azure SQL Database (single database)** from Kendra Little's `dba_QueryStoreUnforceFailed.sql`:
<https://github.com/LitKnd/FreeSQLServerScripts/blob/main/queryStore/dba_QueryStoreUnforceFailed.sql>
(why it matters: <https://kendralittle.com/2024/08/12/query-store-failed-forced-plans-general-failure-even-slower-compile-time/>).

Adaptations from the original: it runs against the **current database only** (Azure SQL Database has no cross-database `sys.databases` loop), and it **drops `NOLOCK`** (RCSI is on by default; dirty reads are discouraged — consistent with this skill's anti-pattern stance). In review mode it only *reports*; in enforce mode the same condition is `ScanGuide.md` category 4 (tier 0) and is acted on with the generated unforce command.

```sql
/* Failing forced plans: is_forced_plan = 1 AND force_failure_count > 0.
   Adapted for Azure SQL Database from Kendra Little's dba_QueryStoreUnforceFailed.sql
   (https://github.com/LitKnd/FreeSQLServerScripts). Single database; no NOLOCK. */
SELECT
    category = 'stale_forced',
    qsqp.query_id,
    current_plan_id = qsqp.plan_id,
    qsqp.force_failure_count,
    last_force_failure_reason = qsqp.last_force_failure_reason_desc,
    query_sql_text = LEFT(qsqt.query_sql_text, 4000),
    proposed_lever = 'unforce_plan',
    unforce_command =
        N'EXEC sys.sp_query_store_unforce_plan @query_id = '
        + CAST(qsqp.query_id AS nvarchar(20))
        + N', @plan_id = '
        + CAST(qsqp.plan_id AS nvarchar(20))
        + N';'
FROM sys.query_store_plan AS qsqp
LEFT JOIN sys.query_store_query AS qsq
    ON qsqp.query_id = qsq.query_id
LEFT JOIN sys.query_store_query_text AS qsqt
    ON qsq.query_text_id = qsqt.query_text_id
WHERE qsqp.is_forced_plan = 1
    AND qsqp.force_failure_count > 0;
```

`review_report.py` flags any row with `force_failure_count > 0` as a **critical** `failing_forced_plan` and echoes its `unforce_command` (informational). It carries `category = 'stale_forced'` so the same rows also rank correctly (tier 0) if this run later switches to enforce mode.

## Output

A **Plan Health Report**: severity counts, then issues grouped critical → info, each with the query_id, a one-line diagnosis, and the action enforce mode would take (never executed here). Nothing else changes — review mode leaves the database, the ledger, and the coverage state untouched.

## Relationship to the other modes

- **Review** (this guide): observe + report. No writes anywhere.
- **Dry-run** (`SafetyGuide.md`): the enforce pipeline, but emits apply scripts instead of executing; writes the coverage state and ledger (`outcome: dry_run`).
- **Apply** (gated): executes reversible controls and verifies them.

Run review mode any time for a health snapshot; run it first when adopting the skill on a new database to see the landscape before enabling apply.
