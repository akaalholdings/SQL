# Audit Guide

The enforcement ledger is the durable record of every plan control this skill places or removes. Unlike the `sql-optimizer` audit corpus (best-effort, non-blocking), this ledger is **safety-critical and fail-closed**: for an autonomous loop, the record of what was forced *is* the ability to undo it.

## When to record

Record **one row per action**, including dry-runs and rollbacks:

| outcome | use when |
|---|---|
| `dry_run` | gate closed; the apply/rollback SQL was emitted but not executed |
| `applied` | a force/hint was executed and is now in place, pending verification |
| `kept` | `verify_decision.py` confirmed the change beat baseline; left in place |
| `rolled_back` | the change regressed or earned nothing; the rollback was executed |
| `force_failed` | `sp_query_store_force_plan` reported a force failure |
| `skipped` | eligible but not acted on (blast-radius cap, already controlled, etc.) |

Write the row **before/with** applying. A non-zero exit from `enforcement_ledger.py` is a hard stop: do not apply, or roll back immediately if you already did.

## How to record

Write an action document to `/tmp/action.json`, then:

```bash
python3 enforcement_ledger.py --input /tmp/action.json
```

It derives `id` / `timestamp` / `detail_file`, validates against the ledger contract, appends one line to `audits/index.jsonl`, and writes `audits/runs/<id>.md`.

### Action document shape (`/tmp/action.json`)

```json
{
  "environment": "mid_prod",
  "query_id": 42,
  "query_text": "<optional raw text, only for the query_hash>",
  "category": "regression",
  "lever": "force_plan",
  "plan_id": 7,
  "action_sql": "EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = 7;",
  "rollback_sql": "EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;",
  "baseline_metrics": { "avg_duration": 5000, "avg_cpu_time": 3000, "avg_logical_io_reads": 1200, "count_executions": 500 },
  "mode": "apply",
  "outcome": "applied",
  "reason": "current plan 220% slower than plan 7 over 7 days"
}
```

### Field conventions

- **`query_id` / `plan_id`** — Query Store integer ids, exactly as the scan returned them. `force_plan` requires a `plan_id`.
- **`action_sql` / `rollback_sql`** — the exact statements. An applied control (`force_plan`/`set_hints` with outcome `applied`/`kept`) **must** carry a non-empty `rollback_sql`; validation rejects it otherwise.
- **`baseline_metrics`** — the pre-change executions-weighted metrics `verify_decision.py` will judge against.
- **`mode`** — `apply` or `dry_run`, mirroring whether the gate was open.

## Reconstructing active controls

```bash
python3 enforcement_ledger.py --pending      # every control still in place + its rollback SQL
python3 enforcement_ledger.py --validate ~/.copilot/skills/sql_plan_enforcer/audits/index.jsonl
```

`--pending` walks the ledger and returns, per `(environment, query_id, family)`, the rollback statement for anything left active (force/unforce share one family, set/clear another). This is the panic-button revert list.

## Privacy

The ledger lives at `~/.copilot/skills/sql_plan_enforcer/audits/` (override with `SQL_PLAN_ENFORCER_AUDIT_DIR`). It records production `query_id`s, plan controls, and any raw `query_text` you pass — treat it as sensitive: secure it, back it up, do not commit it. The writer drops a `.gitignore` as insurance.

Surface the ledger in the response: one line stating how many actions were recorded and the path, e.g. `Recorded 3 enforcement actions to ~/.copilot/skills/sql_plan_enforcer/audits/ (run enforcement_ledger.py --pending to see active controls).`

## Failure handling

The ledger is fail-closed, the inverse of the optimizer audit: if a write fails, **stop the apply** for that action. Never apply a control you could not record — that is an unrevertable change by definition.
