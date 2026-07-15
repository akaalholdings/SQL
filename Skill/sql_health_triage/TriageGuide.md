# Triage Guide

The symptom → tool decision tree. Enter at the branch matching the complaint, follow the drilldown, exit with a named cause, evidence, and an owner. Everything here is read-only.

## Operational caveats (read once, apply everywhere)

- **Timeouts:** the server's default per-query timeout is `AZURE_SQL_QUERY_TIMEOUT_SECONDS=30`. Diagnostic tools are cheap, but targeted `execute_sql` proof queries against big DMV joins can hit it — narrow the query rather than asking for a raised timeout during an incident.
- **Row limits:** `execute_sql` truncates at `AZURE_SQL_ROW_LIMIT` (default 200) and sets `truncated: true`. A truncated diagnostic read is incomplete evidence — it is inconclusive, must not trigger an owner handoff or corrective action, and must render the need to narrow or re-query. Filter or aggregate server-side instead of paging through truncation.
- **DMV visibility varies by principal.** If a tool returns nothing where something is expected, check `check_capabilities` output before concluding health — "cannot see" is not "not happening".

## Step 0 — always, before any branch

```
list_databases()                                   # pick the target with the user
check_capabilities(database_name=<db>)             # what can this principal actually see?
get_resource_limits(database_name=<db>)            # the governance ceilings every number below is judged against
```

Skipping Step 0 produces adjective-based findings ("CPU seems high") — Hard Rule 4 forbids those. Every threshold below is relative to what `get_resource_limits` reports for THIS database's service objective.

## Symptom router

| Symptom | Entry call | Drilldown | Exit / owner |
|---|---|---|---|
| "Everything is slow" / DTU pegged | `get_resource_stats_history` + `get_wait_stats` | dominant wait class routes: CPU → `get_query_wait_stats` + `get_top_queries(sort_by="cpu")`; IO → `get_io_stats`, `get_storage_diagnostics`; memory → `get_memory_grants`; lock waits → blocking branch | culprit query → **sql-optimizer** (pack); at governance ceiling with no culprit → **human** (scale/capacity) |
| Blocked / hanging requests | `get_currently_waiting_tasks` | `get_lock_details` (who holds what), `get_open_transactions` (oldest open txn), `get_active_sessions` (head blocker attribution) | head blocker session → **human** (kill recommendation, exact command); repeat-offender query → **sql-optimizer** (pack) |
| Deadlock alerts | `get_deadlock_history` | extract victim/survivor statements and resources from the deadlock graph | each participating query → **sql-optimizer** (pack); lock-ordering application fix → **human** |
| Tempdb errors / full | `get_tempdb_usage` | `get_tempdb_space_breakdown` (user objects vs internal vs version store); version store growth → `get_open_transactions` | culprit query (spills, wide sorts) → **sql-optimizer** (pack); long-open transaction → **human** |
| Timeouts / connection failures | `get_connection_diagnostics` | `get_resource_limits` (worker/session caps), `get_active_sessions` (who is consuming workers) | worker/session exhaustion by a query flood → **sql-optimizer**; pool sizing / retry storm → **human** (application) |
| "It was fast yesterday" (regression) | — | cross-skill route: this is plan instability, not an incident | → **sql-plan-enforcer** review mode (`plan_health_review`) |
| Compile / plan-cache pressure | `get_plan_cache_analysis` | `get_query_compilation_stats` (compiles/sec vs batches), `get_top_cached_queries` (single-use plan bloat), `get_cached_routine_stats` | unparameterized SQL flood → **human** (application parameterization) with the evidence; expensive recompiling query → **sql-optimizer** (pack) |
| Memory-grant waits (RESOURCE_SEMAPHORE) | `get_memory_grants` | pending grants > 0 is the finding; the granting queries come from `get_top_queries(sort_by="memory")` | oversized-grant query → **sql-optimizer** (pack) |
| No symptom — health sweep | `analyze_db_health` (11 checks) | drill into each failing check with the matching branch above; `get_database_configuration` for config drift (compat level, Query Store state, auto-tuning) | per-check routing as above |

## Branch discipline

- **One branch at a time.** Follow the entry symptom's branch to its exit before opening another. A slow database with blocking AND tempdb pressure gets two findings from two branches — not one blended guess.
- **Waits are a router, not a diagnosis.** "Top wait is PAGEIOLATCH" is not a finding; the finding is the query/file/limit the IO branch surfaces underneath it.
- **Name the query when there is one.** A finding that implicates a specific `query_id` must carry it — that is what makes the sql-optimizer handoff pack buildable.
- **Compare to the window, not to zero.** `get_wait_stats` and friends accumulate; judge deltas over the incident window (or use the tools' windowed parameters) rather than lifetime totals.

## Exit

Every branch exits into `ReportGuide.md`: normalize what you saw into finding objects, classify with `triage_report.py`, enqueue optimizer packs, and return the Triage Report. If the branch found nothing over threshold, the exit is a `healthy`/`inconclusive` result stated plainly (Hard Rule 6).
