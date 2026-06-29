# Safety Guide

This skill applies changes to a **production** database autonomously. That is only acceptable because every lever is reversible and every change is verified. This guide is the preflight you run **before** scanning, and the set of guardrails that must all hold before anything executes.

## Why autonomous apply is defensible here

The only two levers are Query Store **plan forcing** and Query Store **hints**. Both:

- change *plan selection only* — never schema, data, statistics, or indexes;
- are undone by a single command (`sp_query_store_unforce_plan` / `sp_query_store_clear_hints`);
- are the same mechanism class as Azure SQL Database's built-in Automatic Tuning (`FORCE_LAST_GOOD_PLAN`).

So "autonomous" means a custom, fully-audited automatic-tuning loop with a verify-and-rollback net — **not** an agent issuing irreversible DDL. The moment a change would be irreversible (an index, a stats update, a rewrite), it is **out of scope** and goes to `sql-optimizer` / a human.

## The fail-closed apply gate

Nothing is applied to a live database unless **all three** hold (`authorization.can_apply`):

1. **Kill switch off** — `SQL_PLAN_ENFORCER_DISABLE` is not set/truthy. Setting it truthy halts all applies instantly, mid-run.
2. **Apply mode on** — `SQL_PLAN_ENFORCER_APPLY` is truthy. Default is **dry-run**: scan, rank, emit scripts, execute nothing.
3. **Target allowlisted** — the `(environment, query_id)` passes the allowlist file.

Any one missing → dry-run for that change. This is the design's core reconciliation: the existing convention is "`mid_prod` = maintenance only after explicit approval." Here that approval moves to **config time** — the human enables apply mode and puts `mid_prod` (and the permitted query_ids) in the allowlist once, deliberately. Per-change approval is replaced by per-change *verification*.

### Allowlist file

Default `~/.copilot/skills/sql_plan_enforcer/allowlist.json` (or `$SQL_PLAN_ENFORCER_ALLOWLIST`). A missing or invalid file denies everything.

```json
{
  "environments": ["mid_dev", "mid_prod"],
  "query_ids": "*",
  "deny_query_ids": [101, 102]
}
```

- `environments` — apply is permitted only in these. Omit `mid_prod` to keep production hands-off while still enforcing in lower environments.
- `query_ids` — `"*"` for any query in the allowed environments, or an explicit list of Query Store `query_id`s for a narrow rollout.
- `deny_query_ids` — always wins over allow. Put system, ETL, and known-fragile queries here.

## Blast-radius caps

- **Max changes per run** — take only the top *N* eligible candidates from `scan_rank.py` (default small, e.g. 5). The rest wait for the next cycle.
- **Eligibility floors** — minimum executions, minimum regression magnitude, minimum variance, minimum duration. Sub-threshold candidates are marked ineligible and never auto-applied (`scan_rank.py`).
- **Observed plans only** — force only a `plan_id` that actually ran and was measurably better.
- **One family per query** — do not stack a forced plan and a hint on the same query in the same cycle.

## Mandatory, fail-closed ledger

The ledger (`enforcement_ledger.py`) is **not** opt-out and **not** best-effort — unlike the `sql-optimizer` audit corpus. For an autonomous loop, the record of what was forced *is* the ability to undo it.

- Write the ledger row **before/with** applying. A failed ledger write is a hard stop: do not apply, or roll back immediately if already applied.
- Every applied control stores its exact rollback statement (validation rejects an applied control with no rollback).
- `enforcement_ledger.py --pending` reconstructs every control still in place — the panic-button "revert everything we touched" list.

## Never auto-applied (hand to `sql-optimizer` / a human)

- `CREATE` / `ALTER` / `DROP INDEX`, or any DDL.
- `UPDATE STATISTICS` or any statistics maintenance.
- Query rewrites / text changes (Query Store hints change behavior of compilation, not the SQL text — that is allowed; editing the query is not).
- Anything on a query in `deny_query_ids`, or in an environment not in the allowlist.
- Compatibility-level, database-scoped-configuration, or Automatic Tuning setting changes.

## Preflight checklist (run before scanning)

1. Read the kill switch, apply mode, and allowlist; decide and state the run mode (dry-run vs gated apply) and environments.
2. Confirm Query Store is `READ_WRITE` on the target DB (forcing/hints need it).
3. Run `enforcement_ledger.py --pending` and re-evaluate already-active controls first (scan category 4) — clear failing ones before adding new ones.
4. Set the blast-radius cap for this run.
