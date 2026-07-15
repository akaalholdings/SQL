# Enforce Guide

For each eligible candidate from `scan_rank.py` (top-down, within the blast-radius cap), choose a lever, record the baseline, append a `prepared` ledger row, apply (if the gate passes), append the confirmed outcome, then verify and auto-rollback. Every step is reversible; `SafetyGuide.md` governs whether anything is actually executed.

## Per-candidate loop

### 1. Choose the lever

| `proposed_lever` | When | Apply | Rollback |
|---|---|---|---|
| `force_plan` | A known-good historical `plan_id` is materially better (regression, or beaten forced plan) | `sys.sp_query_store_force_plan` | `sys.sp_query_store_unforce_plan` |
| `set_hints` | High variance / parameter sensitivity; no single better plan to force | `sys.sp_query_store_set_hints` | `sys.sp_query_store_clear_hints` |
| `unforce_plan` | A forced plan is failing (`force_failure_count > 0`) or no longer best | `sys.sp_query_store_unforce_plan` | re-force prior `plan_id` if one was recorded |
| `handoff_optimizer` | Top consumer with no better plan to force | **Do not enforce.** Build an evidence pack and enqueue it via `handoff_queue.py add` (fail-closed); `sql-optimizer` consumes it from the queue | reopen the pack if the shipped rewrite regresses |

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

Set a hint (no query-text change — ideal for ORM/vendor SQL). Choose the value from `sql-optimizer`'s `queryguide.md` §3.1 — `OPTION(RECOMPILE)` for genuinely volatile params, or `OPTIMIZE FOR (@param = 'value')` for a known dominant value. **Do not default to `OPTIMIZE FOR UNKNOWN`** (see §3.1; it forces the density-average plan and masks the root cause):

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

### 4. Prepare the ledger (mandatory, before applying)

```bash
python3 enforcement_ledger.py --input /tmp/action.json
```

`/tmp/action.json` carries `query_id`, `lever`, `plan_id`, `action_sql`, `rollback_sql`, `baseline_metrics`, `environment`, `category`, `mode`, `outcome`, `reason`. For a live call, set `mode: "apply"` and `outcome: "prepared"`. A non-zero exit is a **hard stop** — do not apply (or, if you already did, run the rollback immediately). See `AuditGuide.md`.

### 5. Apply — only through the gate, per lever

Check `authorization.can_apply(environment, query_id)` first. When the skill gate is closed (dry-run, not allowlisted, or kill switch), do **not** apply anything by any channel: emit the apply + rollback scripts in the response and set the ledger `outcome` to `dry_run`/`skipped`. When the gate is open, the channel depends on the lever:

**Force / unforce — executable today.** After the `prepared` row is durable, call `force_query_plan(query_id=..., plan_id=..., unforce=<false|true>, dry_run=false, database_name=...)`, then append a second ledger row with `outcome: applied` (force) or `rolled_back` (unforce). If the tool fails, append `force_failed`. The server has its own gate on top of this skill's: execution requires `AZURE_SQL_WRITE_POLICY=apply` and the explicit `dry_run=false`, and the call lands in the server's JSONL audit. Both gates must be open.

**Set / clear hints — executable via the dedicated tools.** After the `prepared` row is durable, call `set_query_store_hints(query_id=..., query_hints="OPTION(...)", dry_run=false, database_name=...)`, then append a second `outcome: applied` row. Same double gate as force/unforce: this skill's allowlist AND the server's `AZURE_SQL_WRITE_POLICY=apply` + explicit `dry_run=false`, audited server-side. The hints string is validated server-side against a strict allowlist of documented Query Store hints (`OPTION(RECOMPILE)`, `OPTIMIZE FOR (...)`, `MAXDOP n`, `USE HINT('...')`, grant/percent hints, join/union/group hints) — an unsupported hint is rejected before anything runs, so build the string from `queryguide.md` §3.1's recommendations, not free-form. Rollback is `clear_query_store_hints(query_id=..., dry_run=false, database_name=...)`; append a `rolled_back` row only after the tool confirms removal.

**Emit-script fallback (older servers only).** If the server does not expose `set_query_store_hints` (pre-upgrade deployment), fall back to the human-in-the-loop protocol:

1. Write the ledger row with `outcome: "emitted"` (it must carry both the apply and rollback SQL — validation enforces this).
2. Hand the exact `EXEC sys.sp_query_store_set_hints ...` script and its `clear_hints` rollback to the human in the response.
3. When the human confirms it ran, verify read-only before believing it:

   ```
   execute_sql(sql="SELECT query_id, query_hint_text FROM sys.query_store_query_hints WHERE query_id = <id>", database_name=...)
   ```

   Hint present → write a second ledger row with `outcome: "applied"`, `mode: "apply"`, reason "human-executed script confirmed". Hint absent → treat as not applied; re-emit or mark `skipped`.
4. The change then enters the normal verify/auto-rollback loop like any applied control.

Production apply requires the target database to be in this skill's own allowlist (`SafetyGuide.md`) — that is the human's config-time approval, separate from and in addition to whatever `azure-sql-mcp`'s `AZURE_SQL_ALLOWED_DATABASES` and write policy permit.

### 6. Verify and auto-rollback

After applying, let Query Store accumulate fresh intervals (or run the query a bounded number of times via `execute_sql`), then capture the post-change metrics and decide:

```bash
python3 verify_decision.py --input /tmp/verify.json   # {"baseline": {...}, "candidate": {...}}
```

- `keep` — the change beat baseline past the improvement floor. Append a new ledger row with `outcome: kept`.
- `rollback` — the change regressed OR earned nothing. Execute the recorded rollback and write a new ledger row (`lever`: `unforce_plan`/`clear_hints`, `outcome`: `rolled_back`). For forced plans that is `force_query_plan(unforce=true, dry_run=false)`; for hints it is `clear_query_store_hints(query_id=..., dry_run=false)` (on the emit-script fallback: hand the recorded `sp_query_store_clear_hints` statement to the human, then confirm removal via `sys.query_store_query_hints` before writing the `rolled_back` row).
- `hold` — too few post-change executions to judge. Leave it; re-check next cycle.

An autonomous loop keeps **only demonstrable wins**. A forced plan or hint that does nothing useful is reverted — it is added risk and maintenance for no benefit.

## Thresholds (shared with the scan/verify modules)

- Eligibility floors (executions, regression %, coefficient of variation) live in `scan_rank.py` and the scan `DECLARE` blocks — keep them in sync.
- Keep/rollback floors (min improvement %, regression tolerance %, min executions to judge) live in `verify_decision.py`.
- Override either via the `SQL_PLAN_ENFORCER_*` environment variables documented in those modules.

## Idempotency

Re-running a cycle must not double-apply. The stale-forced scan (category 4) re-surfaces controls already in place; `enforcement_ledger.py --pending` lists every active control. Before forcing a query, confirm it is not already force/hint-controlled in the ledger; if it is and still healthy, skip it.
