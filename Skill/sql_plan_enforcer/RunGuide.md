# Run Guide

How to drive a full enforcement cycle through `query_geneva_db` and the helper scripts. Same execution engine and environments as `sql-optimizer`; the difference is this skill also issues **reversible plan-control commands** through the gated maintenance path — never DDL.

## Environments

- `mid` — read-only prod replica. Default for **scans / evidence**.
- `mid_prod` — primary production. The only place forcing affects the real workload; apply here only when allowlisted (`SafetyGuide.md`).
- `mid_preprod`, `mid_test`, `mid_dev`, `mid_sandbox` — dry-run and validation targets.

Query Store is per-database. Read evidence from `mid`; apply to the database that runs the workload.

## Cycle

### 1. Preflight (`SafetyGuide.md`)

```bash
# run mode + active controls
echo "apply=$SQL_PLAN_ENFORCER_APPLY disable=$SQL_PLAN_ENFORCER_DISABLE"
python3 enforcement_ledger.py --pending
```

### 2. Scan (read-only) and rank

```bash
query_geneva_db mid --dba --query-file /tmp/scan_regression.sql --format json
query_geneva_db mid --dba --query-file /tmp/scan_top.sql --format json
query_geneva_db mid --dba --query-file /tmp/scan_paramsens.sql --format json
query_geneva_db mid --dba --query-file /tmp/scan_stale.sql --format json
# merge the four row arrays -> /tmp/candidates.json
python3 scan_rank.py --input /tmp/candidates.json --eligible-only --limit 5
```

### 3. Per candidate: ledger, then apply (gated)

```bash
# always record first (fail-closed)
python3 enforcement_ledger.py --input /tmp/action.json

# apply ONLY if authorization.can_apply passed; single statement per call, like sql-optimizer DDL path
query_geneva_db mid_prod --dba --query-file /tmp/apply.sql --format json
```

`--benchmark` / `--query-file2` run read-only `SELECT`s only and **cannot** issue `EXEC sys.sp_query_store_*`. Plan-control commands go through the single-statement `--dba` maintenance path (one `EXEC` per file), the same channel `sql-optimizer` uses for index DDL.

### 4. Verify and auto-rollback

```bash
# capture post-change metrics for the query, then:
python3 verify_decision.py --input /tmp/verify.json   # {"baseline": {...}, "candidate": {...}}

# if it says rollback, execute the recorded rollback and log it:
query_geneva_db mid_prod --dba --query-file /tmp/rollback.sql --format json
python3 enforcement_ledger.py --input /tmp/rollback_action.json
```

Let Query Store accrue enough post-change executions before judging (`verify_decision.py` returns `hold` when there are too few). For a faster signal, run a bounded `query_geneva_db --benchmark` across representative parameter buckets rather than waiting on organic traffic.

## Success criteria

A cycle is successful when:

- Every applied control measurably beat its baseline (kept), or was reverted (rollback), or is being watched (hold) — none left unverified.
- Every action is in the ledger with an exact rollback statement.
- Nothing irreversible was executed; rewrite/index needs were handed to `sql-optimizer`.
- The blast-radius cap and allowlist were honored.

## Short version

Preflight the gate and active controls → run the four read-only Query Store scans on `mid` → rank with `scan_rank.py` → for each eligible candidate, ledger first, then (if the gate is open) apply one reversible `EXEC sys.sp_query_store_*` on the workload DB → verify with `verify_decision.py` and auto-rollback losers/no-ops → confirm the ledger.
