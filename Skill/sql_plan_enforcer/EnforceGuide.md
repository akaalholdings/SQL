# Enforce Guide

For each eligible candidate from `scan_rank.py` (top-down, within the blast-radius cap), choose a lever, record the baseline, write the ledger, apply (if the gate passes), then verify and auto-rollback. Every step is reversible; `SafetyGuide.md` governs whether anything is actually executed.

## Per-candidate loop

### 1. Choose the lever

| `proposed_lever` | When | Apply | Rollback |
|---|---|---|---|
| `force_plan` | A known-good historical `plan_id` is materially better (regression, or beaten forced plan) | `sys.sp_query_store_force_plan` | `sys.sp_query_store_unforce_plan` |
| `set_hints` | High variance / parameter sensitivity; no single better plan to force | `sys.sp_query_store_set_hints` | `sys.sp_query_store_clear_hints` |
| `unforce_plan` | A forced plan is failing (`force_failure_count > 0`) or no longer best | `sys.sp_query_store_unforce_plan` | re-force prior `plan_id` if one was recorded |
| `handoff_optimizer` | Top consumer with no better plan to force | **Do not enforce.** Emit a recommendation for `sql-optimizer` | n/a |

Only force a `plan_id` that **actually ran** in Query Store and was measurably better (it is in the scan output). Never synthesize a plan.

### 2. Record the pre-change baseline

Capture the candidate's current executions-weighted metrics from the scan (or a fresh read): `avg_duration`, `avg_cpu_time`, `avg_logical_io_reads`, `count_executions`. This baseline is what `verify_decision.py` judges the change against, and it goes in the ledger.

### 3. Build the exact apply + rollback SQL

Force a plan:

```sql
EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = 7;
-- rollback (unforce takes BOTH query_id and plan_id):
EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;
```

Set a hint (no query-text change — ideal for ORM/vendor SQL). Choose the value from `sql-optimizer`'s `queryguide.md` §3.1 (`OPTION(RECOMPILE)` for volatile params, `OPTIMIZE FOR UNKNOWN` for skew):

```sql
EXEC sys.sp_query_store_set_hints @query_id = 42, @query_hints = N'OPTION(RECOMPILE)';
-- rollback:
EXEC sys.sp_query_store_clear_hints @query_id = 42;
```

Unforce a failing/stale plan:

```sql
EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;
-- rollback: re-force the previously forced plan_id, only if it was healthy
EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = <prior_plan_id>;
```

### 4. Write the ledger (mandatory, before applying)

```bash
python3 enforcement_ledger.py --input /tmp/action.json
```

`/tmp/action.json` carries `query_id`, `lever`, `plan_id`, `action_sql`, `rollback_sql`, `baseline_metrics`, `environment`, `category`, `mode`, `outcome`, `reason`. A non-zero exit is a **hard stop** — do not apply (or, if you already did, run the rollback immediately). See `AuditGuide.md`.

### 5. Apply — only through the gate

Check `authorization.can_apply(environment, query_id)`:

- **Gate open** (kill switch off, apply mode on, target allowlisted): execute the apply SQL via `query_geneva_db <env> --dba --query-file /tmp/apply.sql`, set the ledger `outcome` to `applied`.
- **Gate closed** (dry-run, not allowlisted, or kill switch): do **not** execute. Emit the apply + rollback scripts in the response and set `outcome` to `dry_run`/`skipped`.

`mid_prod` apply requires `mid_prod` in the allowlist — that is the human's config-time approval.

### 6. Verify and auto-rollback

After applying, let Query Store accumulate fresh intervals (or run a bounded `query_geneva_db --benchmark` of the query), then capture the post-change metrics and decide:

```bash
python3 verify_decision.py --input /tmp/verify.json   # {"baseline": {...}, "candidate": {...}}
```

- `keep` — the change beat baseline past the improvement floor. Update the ledger `outcome` to `kept`.
- `rollback` — the change regressed OR earned nothing. Execute the recorded rollback SQL, write a new ledger row (`lever`: `unforce_plan`/`clear_hints`, `outcome`: `rolled_back`).
- `hold` — too few post-change executions to judge. Leave it; re-check next cycle.

An autonomous loop keeps **only demonstrable wins**. A forced plan or hint that does nothing useful is reverted — it is added risk and maintenance for no benefit.

## Thresholds (shared with the scan/verify modules)

- Eligibility floors (executions, regression %, coefficient of variation) live in `scan_rank.py` and the scan `DECLARE` blocks — keep them in sync.
- Keep/rollback floors (min improvement %, regression tolerance %, min executions to judge) live in `verify_decision.py`.
- Override either via the `SQL_PLAN_ENFORCER_*` environment variables documented in those modules.

## Idempotency

Re-running a cycle must not double-apply. The stale-forced scan (category 4) re-surfaces controls already in place; `enforcement_ledger.py --pending` lists every active control. Before forcing a query, confirm it is not already force/hint-controlled in the ledger; if it is and still healthy, skip it.
