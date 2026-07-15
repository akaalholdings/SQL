# Audit Guide

The enforcement ledger is the durable record of every plan control this skill places or removes. Unlike the `sql-optimizer` audit corpus (best-effort, non-blocking), this ledger is **safety-critical and fail-closed**: for an autonomous loop, the record of what was forced *is* the ability to undo it.

## When to record

Record **one row per action**, including dry-runs and rollbacks:

| outcome | use when |
|---|---|
| `dry_run` | gate closed; the apply/rollback SQL was emitted but not executed |
| `prepared` | live action durably recorded before the tool call; not yet confirmed active |
| `emitted` | gate **open** but the server lacks the hints tools (older deployment): the script was handed to the human for execution, awaiting confirmation |
| `applied` | a force/hint was executed and is now in place, pending verification |
| `kept` | `verify_decision.py` confirmed the change beat baseline; left in place |
| `rolled_back` | the change regressed or earned nothing; the rollback was executed |
| `force_failed` | `sp_query_store_force_plan` reported a force failure |
| `skipped` | eligible but not acted on (blast-radius cap, already controlled, etc.) |

Use a two-phase append-only record for live work: write `outcome: "prepared"` **before** applying, then append the confirmed `applied`, `rolled_back`, or `force_failed` row after the tool response. A non-zero exit from `enforcement_ledger.py` is a hard stop: do not apply, or roll back immediately if you already did.

### Two-row hint lifecycle and autonomous live lifecycle

The ledger never mutates an existing row. Autonomous live actions use `prepared` then a confirmed outcome; the older-server hint fallback uses `emitted` then a confirmed outcome (`EnforceGuide.md` §5):

1. `outcome: "prepared"` — written before an autonomous live call. Must carry the exact action and rollback SQL. It does **not** count as active in `--pending`.
2. `outcome: "emitted"` — written when a script is handed to a human. Must carry both `action_sql` and `rollback_sql`. It does **not** count as active in `--pending`.
3. `outcome: "applied"` — appended only after the tool succeeds, or after a human confirms and you verify the hint read-only via `sys.query_store_query_hints`. This row activates the control and enters the verify/auto-rollback loop.

If confirmation never comes or verification finds no hint, close it out with a `skipped`
row carrying the same exact `action_sql` instead — never leave an `emitted` row as the
final word on a control you believe is live. A later outcome resolves only the exact
prepared apply or rollback statement, not merely another action for the same query.

## How to record

Write an action document to `/tmp/action.json`, then:

```bash
python3 enforcement_ledger.py --input /tmp/action.json
```

It derives `id` / `timestamp` / `detail_file`, validates against the ledger contract, appends one line to `audits/index.jsonl`, and writes `audits/runs/<id>.md`.

### Action document shape (`/tmp/action.json`)

```json
{
  "environment": "awlt_prod",
  "query_id": 42,
  "query_text": "<optional raw text, only for the query_hash>",
  "category": "regression",
  "lever": "force_plan",
  "plan_id": 7,
  "action_sql": "EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = 7;",
  "rollback_sql": "EXEC sys.sp_query_store_unforce_plan @query_id = 42, @plan_id = 7;",
  "baseline_metrics": { "avg_duration": 5000, "avg_cpu_time": 3000, "avg_logical_io_reads": 1200, "count_executions": 500 },
  "mode": "apply",
  "outcome": "prepared",
  "reason": "current plan 220% slower than plan 7 over 7 days"
}
```

### Field conventions

- **`query_id` / `plan_id`** — positive Query Store integer ids, exactly as the scan returned them. `force_plan` and `unforce_plan` require a `plan_id`; hint levers require `null`.
- **`action_sql` / `rollback_sql`** — the exact statements. An applied control (`force_plan`/`set_hints` with outcome `applied`/`kept`) **must** carry a non-empty `rollback_sql`; validation rejects it otherwise.
- **`baseline_metrics`** — the pre-change executions-weighted metrics `verify_decision.py` will judge against.
- **`mode`** — `apply` or `dry_run`, mirroring whether the gate was open. `dry_run`
  outcomes require `dry_run`; `prepared`, `applied`, `kept`, `rolled_back`, and `force_failed`
  require `apply`. Validation rejects contradictory records.

## Reconstructing active controls

```bash
python3 enforcement_ledger.py --pending      # every control still in place + its rollback SQL
python3 enforcement_ledger.py --validate ~/.sql-skills/sql_plan_enforcer/audits/index.jsonl
```

`--pending` walks the ledger and returns, per `(environment, query_id, family)`, the rollback statement for anything left active (force/unforce share one family, set/clear another). If a `prepared` row has no later confirmed outcome, it prints the recorded rollback and exits non-zero: verify the target read-only and resolve that uncertainty before any new apply. This is the panic-button and crash-recovery list.

The ledger, detail files, queue, locks, and coverage state are owner-only durable stores:
directories use mode `700`, files use mode `600`, and writes are serialized with atomic
replacement or fsynced append. Never copy these runtime records into a shared location.

## Privacy

The ledger lives at the resolved audit dir — `$SQL_PLAN_ENFORCER_AUDIT_DIR`, else the legacy `~/.copilot/skills/sql_plan_enforcer/audits/` when it already exists, else `~/.sql-skills/sql_plan_enforcer/audits/` (see `enforcement_ledger.py`). It records production `query_id`s, plan controls, and any raw `query_text` you pass — treat it as sensitive: secure it, back it up, do not commit it. The writer drops a `.gitignore` as insurance.

Surface the ledger in the response: one line stating how many actions were recorded and the path, e.g. `Recorded 3 enforcement actions to ~/.sql-skills/sql_plan_enforcer/audits/ (run enforcement_ledger.py --pending to see active controls).`

## Failure handling

The ledger is fail-closed, the inverse of the optimizer audit: if a write fails, **stop the apply** for that action. Never apply a control you could not record — that is an unrevertable change by definition.
