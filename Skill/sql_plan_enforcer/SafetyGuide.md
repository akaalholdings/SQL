# Safety Guide

This skill applies changes to a **production** database autonomously. That is only acceptable because every lever is reversible and every change is verified. This guide is the preflight you run **before** scanning, and the set of guardrails that must all hold before anything executes.

## Why autonomous apply is defensible here

The only two levers are Query Store **plan forcing** and Query Store **hints**. Both:

- change *plan selection only* — never schema, data, statistics, or indexes;
- are undone by a single command (`sp_query_store_unforce_plan` / `sp_query_store_clear_hints`);
- are the same mechanism class as Azure SQL Database's built-in Automatic Tuning (`FORCE_LAST_GOOD_PLAN`).

So "autonomous" means a custom, fully-audited automatic-tuning loop with a verify-and-rollback net — **not** an agent issuing irreversible DDL. The moment a change would be irreversible (an index, a stats update, a rewrite), it is **out of scope** and goes to `sql-optimizer` / a human.

## Two independent gates

Apply is double-gated — this skill's gate and the server's gate are separate, and **both** must be open:

1. **This skill's gate** (`authorization.can_apply`, below): kill switch, apply mode, allowlist. Campaign-level, config-time approval.
2. **The server's gate** (`azure-sql-mcp` AdminPolicy): admin tools execute only with `AZURE_SQL_WRITE_POLICY=apply` **and** an explicit `dry_run=false` on the call, and every preview/apply/block is written to the server's JSONL audit (`AZURE_SQL_AUDIT_DIR`). `execute_tsql_unrestricted` additionally hard-denylists DDL, DML, and `EXEC` — all applies go through the dedicated, parameterized tools (`force_query_plan`, `set_query_store_hints`, `clear_query_store_hints`), never raw SQL.

A blocked apply can therefore come from either layer; check both when debugging. Neither gate substitutes for the other: the server does not know this skill's allowlist, and this skill must not treat server-side gating as a reason to skip its own.

## The fail-closed apply gate

Nothing is applied to a live database unless **all three** hold (`authorization.can_apply`):

1. **Kill switch off** — `SQL_PLAN_ENFORCER_DISABLE` is unset or explicitly false. Any
   other non-empty value, including an unrecognized value, halts applies so a typo cannot
   disable the emergency stop.
2. **Apply mode on** — `SQL_PLAN_ENFORCER_APPLY` is truthy. Default is **dry-run**: scan, rank, emit scripts, execute nothing.
3. **Target allowlisted** — the `(environment, query_id)` passes the allowlist file.

Any one missing → dry-run for that change. This is the design's core reconciliation: the existing convention is "production database = maintenance only after explicit approval." Here that approval moves to **config time** — the human enables apply mode and puts the production database (and the permitted query_ids) in the allowlist once, deliberately. Per-change approval is replaced by per-change *verification*.

### Allowlist file

Path resolved by `authorization.py`: `$SQL_PLAN_ENFORCER_ALLOWLIST`, else the legacy `~/.copilot/skills/sql_plan_enforcer/allowlist.json` when it already exists, else `~/.sql-skills/sql_plan_enforcer/allowlist.json`. A missing or invalid file denies everything.

```json
{
  "environments": ["awlt_dev", "awlt_prod"],
  "query_ids": "*",
  "deny_query_ids": [101, 102]
}
```

- `environments` — database names from `azure-sql-mcp`'s `AZURE_SQL_ALLOWED_DATABASES` (call `list_databases` to see what's configured). Apply is permitted only in these. Omit the production database to keep production hands-off while still enforcing in lower environments.
- `query_ids` — `"*"` for any query in the allowed environments, or an explicit list of Query Store `query_id`s for a narrow rollout.
- `deny_query_ids` — always wins over allow. Put system, ETL, and known-fragile queries here.

## Identity, provenance, and incomplete evidence

- Query Store `query_id` is database-scoped. Coverage state, allowlist decisions, queue
  dedupe, and ledger rollback reconstruction use the pair `(environment, query_id)`;
  never use a query id by itself.
- Coverage state is v2. When `coverage_state.py` opens a v1 file, entries with no known
  environment are retained as quarantined legacy records and cannot suppress or authorize
  work. Do not hand-edit an environment onto a legacy entry; re-observe it in the target
  database.
- Every lifecycle transition must carry both `environment` and integer `query_id`. Illegal
  state changes fail closed; a corrupt state file stops the tick.
- `pending_verify` and `emitted` entries must retain a non-empty finite baseline and the
  exact generated rollback for their query/plan. An active control cannot transition to
  `evaluated` or `skipped`; only an explicit `kept` or `reverted` result closes it.
- Enforcement verification requires expected environment, query, and plan provenance on
  both baseline and candidate evidence. Baseline evidence is pre-change, candidate evidence
  is post-change, and their windows must be non-overlapping and source-consistent.
- Any `truncated` flag at the metric, evidence, provenance, or scan-result level makes the
  candidate ineligible or the verification decision `hold`. Raise the server row limit or
  narrow the scan before continuing.

## Owner-only durable state

Coverage state, queue packs, queue indexes, locks, ledger runs, and ledger indexes are
created with directory mode `700` and file mode `600`. Writes use an owner-only temporary
file, atomic replacement, append fsync, and directory fsync while holding the store lock.
Existing modes are tightened; a symlink in any storage path component is rejected. A failed
durable write is a hard stop.

## Blast-radius caps

- **Max changes per run** — take only the top *N* eligible candidates from `scan_rank.py` (default small, e.g. 5). The rest wait for the next cycle.
- **Eligibility floors** — minimum executions, minimum regression magnitude, minimum variance, minimum duration. Sub-threshold candidates are marked ineligible and never auto-applied (`scan_rank.py`).
- **Observed plans only** — force only a `plan_id` that actually ran and was measurably better.
- **One family per query** — do not stack a forced plan and a hint on the same query in the same cycle.

## Mandatory, fail-closed ledger

The ledger (`enforcement_ledger.py`) is **not** opt-out and **not** best-effort — unlike the `sql-optimizer` audit corpus. For an autonomous loop, the record of what was forced *is* the ability to undo it.

- Append a `prepared` row **before** every live call, then append the confirmed outcome. A failed ledger write is a hard stop: do not apply, or roll back immediately if already applied.
- Every applied control stores its exact rollback statement (validation rejects an applied control with no rollback).
- A confirmed row resolves only the exact prepared apply or rollback statement. An unrelated
  row for the same query cannot hide crash-recovery work; duplicate prepared rows remain
  visible until each has a matching outcome.
- `enforcement_ledger.py --pending` reconstructs every control still in place. It returns non-zero and shows the recorded rollback when a `prepared` row has no confirmed outcome; verify that target read-only and resolve it before any new apply.

## Never auto-applied (hand to `sql-optimizer` / a human)

- `CREATE` / `ALTER` / `DROP INDEX`, or any DDL.
- `UPDATE STATISTICS` or any statistics maintenance.
- Query rewrites / text changes (Query Store hints change behavior of compilation, not the SQL text — that is allowed; editing the query is not).
- Anything on a query in `deny_query_ids`, or in an environment not in the allowlist.
- Compatibility-level, database-scoped-configuration, or Automatic Tuning setting changes.

## Preflight checklist (run before scanning)

1. Read the kill switch, apply mode, and allowlist; confirm the server gate too (`AZURE_SQL_WRITE_POLICY` — applies need `apply`); decide and state the run mode (dry-run vs gated apply) and environments.
2. Call `get_database_configuration(database_name=...)`. Confirm Query Store is
   `READ_WRITE`, then inspect `automatic_tuning_options`. Automatic Tuning recommendations
   are review-only; never custom-force or unforce engine-owned (`AUTO`) work.
3. Run `enforcement_ledger.py --pending` and re-evaluate already-active controls first (scan category 4) — clear failing ones before adding new ones.
4. Set the blast-radius cap for this run.
