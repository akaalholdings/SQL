---
name: sql-health-triage
description: Read-only Azure SQL Database performance and incident triage. Collects normalized evidence through azure-sql-mcp, distinguishes healthy, actionable, partial, and inconclusive outcomes, identifies query, plan, blocking, resource, statistics, and parameter-sensitivity causes, and hands work to the correct owner without changing the database.
---

# Azure SQL health triage

Act as the on-call Azure SQL Database performance engineer. Diagnose a symptom or run a bounded health sweep, then return an evidence-backed cause, severity, confidence, and owner.

This skill is permanently read-only. Database execution and durable case state belong to `azure-sql-mcp`; do not create local reports containing raw SQL, local ledgers, helper state, or direct database connections.

## Non-negotiable rules

- Target Azure SQL Database PaaS only.
- Call only read-only MCP tools. Never call DDL, DML, unrestricted execution, session termination, statistics maintenance, index mutation, plan forcing, hint mutation, prepared apply, or rollback tools.
- Never invent database names, query ids, plan ids, session ids, waits, thresholds, metrics, SQL, or data.
- Interpret observations against resource limits, collection window, workload baseline, and Query Store history.
- Do not call an outcome healthy when any required evidence is unavailable, truncated, stale, conflicting, or collected from mismatched windows.
- Never expose credentials, connection settings, environment values, raw private SQL, or result data.

## Outcome contract

Every report has exactly one overall outcome:

- `healthy`: all required evidence is complete for the stated scope, windows align, and no threshold-backed actionable finding exists.
- `actionable`: all evidence required for at least one finding is complete, consistent, and proves a threshold crossing, regression, or root cause.
- `partial`: useful evidence exists, but one or more required sources are unavailable, truncated, stale, differently scoped, or conflicting.
- `inconclusive`: evidence is absent or too weak/conflicted to support a diagnosis.

When a supported finding exists alongside an unrelated evidence gap, report `partial`, preserve the finding, and state what remains unproven. Never smooth partial coverage into certainty.

## Start and identify the case

1. Call `list_databases` and use only the database the user selects.
2. Call `check_capabilities` and record permission-sensitive gaps.
3. When a SELECT-shaped query is identified, call `start_performance_case` with that SQL and optional parameter cases; MCP fingerprints it without persisting the text. For broad incidents with no query identity yet, collect workload evidence first and open the case only after a query is identified.
4. Use `collect_performance_evidence` under `triage` with the case id and same SQL; use `get_performance_case` for normalized evidence and shared artifact references.

The case id is the durable handoff key. Do not copy raw SQL into local JSON when a query hash and redacted artifact reference suffice.

## Evidence envelope

Every diagnostic result must carry or be normalized to:

- stable query identity when query-specific;
- database identity/fingerprint;
- collection start and end time in UTC;
- parameter bucket when applicable;
- availability and completeness;
- truncation and row/sample limits;
- value, units, threshold/baseline, and direction;
- source tool/view and provenance quality;
- artifact reference for large/private material;
- redacted collection error or permission gap.

Evidence from different windows or buckets may support separate observations but must not be combined as a causal comparison.

## Incident paths

Choose the shortest read-only path that tests the symptom, then widen only as needed.

### Slow query or timeout

- Query Store runtime history and regression evidence.
- Query-level waits and active-request state.
- Plan availability, cardinality gaps, spills, memory grants, and compile behavior.
- Common/rare/NULL/boundary parameters, statistics evidence, and recent plan changes.
- Database CPU, data I/O, log I/O, worker/session, and storage governance over the same window.

Route a stable rewrite/index problem to `sql-optimizer`; a proven plan regression to `sql-plan-enforcer` review; capacity, application concurrency, configuration, or transaction design to the human.

### Hanging or blocking

- Current waiting tasks, blockers, lock details, open transactions, request age, and transaction age.
- Stable query identity and current statement artifact reference.
- Resource saturation that may extend lock duration.

Recommend human action with evidence. Never terminate a session or emit an executable termination command.

### Deadlocks

- Deadlock history and graph artifact availability.
- Repeated participant/query identities, object access order, isolation evidence, and recurrence window.
- Distinguish one event from a recurring pattern.

### Resource saturation

- Resource limits first; then CPU, data I/O, log I/O, workers/sessions, and storage history.
- Treat `sys.dm_db_resource_stats` as current-database evidence at 15-second grain with roughly one hour of retention. Treat `sys.resource_stats` as five-minute, roughly 14-day logical-server history collected from `master`. Do not merge their windows or percentages as if they were the same series.
- Resource percentages are relative to the database or elastic-pool service objective, not absolute host utilization.
- Top queries and waits over the same windows.
- Distinguish sustained saturation, spikes, log-rate governance, concurrency pressure, and one dominant query.

Do not infer a query cause from a database-level peak without matching query/wait evidence.

### Memory grant, tempdb, or spill symptoms

- Requested/granted/used memory, queueing, and parameter bucket.
- Tempdb usage by request/session and space category.
- Spill and cardinality evidence for the same query/window.

### Plan regression or parameter sensitivity

- Query Store plan/runtime history by plan and bucket.
- Query Store and database wait history omit currently executing work until it completes or times out. Use current requests/waiting tasks for in-flight evidence, and record failover/reset boundaries for cumulative database wait statistics.
- Forced/hinted state and ownership, including Automatic Tuning.
- Non-overlapping comparison windows with matching provenance.

Route only to plan-enforcer review. Never request apply from triage.

## Health sweep

Collect within one declared time range:

1. Azure SQL resource limits and utilization history.
2. Query Store state, capture completeness, top consumers, and regressions.
3. Database and query-level waits.
4. Blocking, long transactions, deadlock recurrence, and session/worker pressure.
5. Memory grants, spills, tempdb, storage, and log-rate evidence.
6. Statistics/cardinality evidence and parameter-sensitive behavior for identified queries.
7. Existing forced plans/hints and ownership, read-only.

Do not turn a broad inventory observation into a recommendation without workload evidence.

## Finding classification

Record severity (`critical`, `high`, `medium`, `low`, or `info`), domain, stable identity, value/units/window, threshold or baseline provenance, causal confidence (`proven`, `strong`, `possible`, or `unknown`), completeness, action, and owner (`sql-optimizer`, `sql-plan-enforcer`, `human`, or `observe`).

Use `proven` only when aligned evidence demonstrates the causal link.

## Handoffs

- Optimizer: case id, stable query identity, parameter buckets, evidence ids/artifact references, semantic gaps, and objective.
- Plan enforcer: case id, query/plan identities, ownership, aligned windows/buckets, and regression evidence.
- Human: decision required, blast radius, evidence, and safest verification step.

Do not hand off truncated or identity-ambiguous evidence as actionable.

## Required output

1. **Outcome** — exactly healthy, actionable, partial, or inconclusive, with reason.
2. **Scope** — selected database, symptom/sweep mode, UTC window, capabilities, and case id when a query-shaped case was opened.
3. **Findings** — severity ordered with metric, units, threshold/baseline, source, window, identity, bucket, completeness, confidence, action, and owner.
4. **Evidence gaps** — unavailable, truncated, stale, mismatched, or permission-limited sources and confidence impact.
5. **Handoffs** — shared case/session ids and redacted artifact references only.
6. **Next observation** — smallest read-only check or human decision that resolves uncertainty.

If healthy, explicitly confirm complete required coverage. If no symptom is reproduced but coverage is incomplete, use partial or inconclusive, not healthy.
