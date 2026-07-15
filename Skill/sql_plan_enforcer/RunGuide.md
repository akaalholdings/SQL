# Run Guide

How to drive an enforcement cycle through `azure-sql-mcp` and the helper scripts.

## Execution channel — read this first

`azure-sql-mcp` is the execution layer (same server `sql-optimizer` uses):

- **Scans and verification reads** go through `execute_sql` — always read-only, validated by `SafeSqlValidator`, regardless of which allowed database you target.
- **Plan review / tick fast path** goes through `plan_health_review` and `plan_enforcer_tick`. `plan_enforcer_tick` defaults to dry-run, ranks force/unforce candidates, and records preview/apply audit entries through the server's `AdminPolicy`. Treat the tick as **preview-only** — never call it with `dry_run=false`; applies go exclusively through `force_query_plan` so this skill's ledger stays authoritative (otherwise the server tick and this loop can double-apply).
- **Force/unforce** go through the dedicated `force_query_plan` tool (`unforce=true` releases instead of forcing, `dry_run=true` by default). It wraps `sys.sp_query_store_force_plan` / `sys.sp_query_store_unforce_plan` directly — narrow and parameterized, not a raw-SQL bypass. Execution additionally requires the server gate: `AZURE_SQL_WRITE_POLICY=apply` and an explicit `dry_run=false`; every call is written to the server's JSONL audit.
- **Set/clear hints** go through the dedicated `set_query_store_hints` / `clear_query_store_hints` tools (dry-run by default, same server gate as force/unforce). The hints string is validated server-side against a strict allowlist of documented Query Store hints — build it from `queryguide.md` §3.1, not free-form. On older servers without these tools, fall back to the **emit-script protocol** in `EnforceGuide.md` §5: hand the exact `EXEC` script (with rollback) to the human, verify the applied hint read-only through `sys.query_store_query_hints`, then record the outcome in the ledger.

Treat the `EXEC ...` commands in `EnforceGuide.md` as the parameterized arguments to the dedicated tools (`force_query_plan` for force/unforce, `set_query_store_hints`/`clear_query_store_hints` for hints) — they execute through both gates (this skill's `authorization.can_apply` AND the server's write policy). Never send raw `EXEC` text to `execute_tsql_unrestricted` — it is hard-denylisted there.

## Environments

There is no fixed alias list — call `list_databases` to see the server's configured `allowed_databases` and ask which one to target. Query Store is per-database — scan and control the database whose plans you intend to change. This skill's own allowlist (`SafetyGuide.md`; path resolved by `authorization.py` — `$SQL_PLAN_ENFORCER_ALLOWLIST`, legacy `~/.copilot/...` if present, else `~/.sql-skills/sql_plan_enforcer/allowlist.json`) is a separate, additional gate on top of whatever the server allows — apply only where both agree.

## Cycle

### 1. Preflight (`SafetyGuide.md`)

```bash
# run mode + active controls
echo "apply=$SQL_PLAN_ENFORCER_APPLY disable=$SQL_PLAN_ENFORCER_DISABLE"
python3 enforcement_ledger.py --pending
```

Then call `get_database_configuration(database_name=<workload db>)`. Query Store must be
`READ_WRITE`. Inspect `automatic_tuning_options` before ranking: DMV recommendations and
`AUTO` forced plans are review-only, and `FORCE_LAST_GOOD_PLAN = ON` owns overlapping
regression forcing.

### 2. Scan (read-only) and rank

Preferred fast path for a database-level snapshot:

```
plan_health_review(window_minutes=1440, top_n=20, database_name=<workload db>)
```

For one safe enforcement cycle preview:

```
plan_enforcer_tick(window_minutes=1440, max_actions=1, dry_run=true, database_name=<workload db>)
```

Primary scan path (`ScanGuide.md`) — the four server tools, normalized then ranked:

```
detect_regressed_queries(window_minutes=10080, database_name=<workload db>)
get_top_queries(sort_by="total_duration", window_minutes=10080, limit=25, database_name=<workload db>)
detect_parameter_sniffing(window_minutes=10080, database_name=<workload db>)
get_forced_plans(window_minutes=10080, database_name=<workload db>)
```

The adapter preserves recommendation state/action initiator and forced-plan ownership.
Every tuning recommendation plus every `AUTO` or unknown-owner forced plan stays visible
but ineligible. Only independently evidenced custom candidates and `MANUAL` forced plans
can reach the apply gate.

```bash
python3 scan_adapter.py --input /tmp/reg.json --input /tmp/top.json \
    --input /tmp/sniff.json --input /tmp/forced.json > /tmp/candidates.json
python3 scan_rank.py --input /tmp/candidates.json --eligible-only --limit 5
```

The hand-written `execute_sql` scans remain in `ScanGuide.md` as the fallback (empty `detect_regressed_queries`, borderline CV, custom windows); their rows are already in candidate schema and skip the adapter.

### 3. Per candidate: prepare ledger, apply, confirm ledger (gated)

```bash
# live action: outcome=prepared; always record first (fail-closed)
python3 enforcement_ledger.py --input /tmp/action.json
```

The lever determines the tool (`EnforceGuide.md` §5) — both execute ONLY if `authorization.can_apply` passed, and the server gate applies too (`AZURE_SQL_WRITE_POLICY=apply`):

```
# force_plan / unforce_plan
force_query_plan(query_id=<id>, plan_id=<id>, unforce=<false|true>, dry_run=false, database_name=<workload db>)

# set_hints / clear_hints (hints string validated server-side against the documented allowlist)
set_query_store_hints(query_id=<id>, query_hints="OPTION(<hint>)", dry_run=false, database_name=<workload db>)
clear_query_store_hints(query_id=<id>, dry_run=false, database_name=<workload db>)
```

After the tool response, append a second ledger row with the confirmed `applied`,
`rolled_back`, or `force_failed` outcome. Never rewrite the `prepared` row.

On older servers without the hints tools, use the emit-script fallback (`EnforceGuide.md` §5) and verify read-only via `sys.query_store_query_hints` before recording `outcome: applied`.

### 4. Verify and auto-rollback

```bash
# capture post-change metrics for the query, then:
python3 verify_decision.py --input /tmp/verify.json   # provenance is required by default
```

The input must identify the same environment/query and expected plan on both snapshots,
with a comparable parameter mix. Include non-truncated, non-overlapping evidence windows.
The baseline is explicitly pre-change and the candidate explicitly post-change:

```json
{
  "baseline": {
    "avg_duration": 1000, "avg_cpu_time": 500, "avg_logical_io_reads": 1000,
    "count_executions": 200,
    "evidence": {
      "source": "query_store", "window_start": "2026-07-09T10:00:00Z",
      "window_end": "2026-07-09T11:00:00Z", "post_change": false,
      "environment": "awlt_prod", "query_id": 42, "plan_id": 7,
      "parameter_buckets": ["small", "large"]
    }
  },
  "candidate": {
    "avg_duration": 400, "avg_cpu_time": 200, "avg_logical_io_reads": 500,
    "count_executions": 200,
    "evidence": {
      "source": "query_store", "window_start": "2026-07-09T12:00:00Z",
      "window_end": "2026-07-09T13:00:00Z", "post_change": true,
      "environment": "awlt_prod", "query_id": 42, "plan_id": 7,
      "parameter_buckets": ["small", "large"]
    }
  },
  "expected": {"environment": "awlt_prod", "query_id": 42, "plan_id": 7}
}
```

Missing provenance, an environment/query/plan mismatch on either side, overlapping windows,
mismatched parameter buckets, any metric/evidence truncation, or a target mismatch returns
`hold` rather than making an unsafe decision. There is no switch that disables this check.

Every `coverage_state.py record` transition must include `{environment, query_id, state}`.
Coverage v1 files are migrated conservatively: entries without an environment are visible
as quarantined legacy state and never match a new candidate. Handoff packs remain unresolved
while `open` or `claimed`; only `shipped` and `declined` are terminal for dedupe.

If it says rollback, execute the recorded rollback (`force_query_plan` with `unforce=true` for forced plans; `clear_query_store_hints` for hints — or, on the emit-script fallback, hand the recorded script to the human and confirm via `sys.query_store_query_hints`) and log it:

```bash
python3 enforcement_ledger.py --input /tmp/rollback_action.json
```

Let Query Store accrue enough post-change executions before judging (`verify_decision.py` returns `hold` when there are too few). For a faster signal, run the candidate query a bounded number of times across representative parameter buckets via `execute_sql` rather than waiting on organic traffic.

## Success criteria

A cycle is successful when:

- Every applied control measurably beat its baseline (kept), or was reverted (rollback), or is being watched (hold) — none left unverified.
- Every action is in the ledger with an exact rollback statement.
- Nothing irreversible was executed; rewrite/index needs were handed to `sql-optimizer`.
- The blast-radius cap and allowlist were honored.

## Short version

Preflight the gate and active controls → run the four read-only Query Store scans via `execute_sql` → rank with `scan_rank.py` → for each eligible candidate, ledger first, then (if the gate is open) apply one reversible control — `force_query_plan` for force/unforce; `set_query_store_hints`/`clear_query_store_hints` for hints (emit-script fallback on older servers) — on the workload database → verify with `verify_decision.py` and auto-rollback losers/no-ops → confirm the ledger.
