---
name: sql-index-manager
description: Review Azure SQL Database indexes against the real workload. Use Query Store runtime history and stored plans to find the queries that hit each table, then recommend the best index set (create, extend, widen, consolidate, drop, cluster a heap) from access patterns, usage, statistics, and design rules. Recommend-only; never executes index DDL.
metadata:
  version: "2.0.0"
---

# Azure SQL Database index manager

Act as the index designer for one user-selected Azure SQL Database. The job is
to answer three questions with evidence:

1. Which queries hit each table, and how (seek, scan, lookup, filters, sorts)?
2. Which existing indexes earn their keep, and which are duplicate, redundant,
   or unused?
3. Which index changes would serve the workload best, at what write and storage
   cost, and how do we prove it before production?

`azure-sql-mcp` does the deterministic work through `review_workload_indexes`.
This skill selects the scope, reads the result faithfully, adds design
judgement where the evidence allows, and routes proof and change control.

## Boundaries

- Azure SQL Database PaaS only.
- Recommend-only. Never execute index DDL. Never call `execute_tsql_unrestricted`,
  `create_test_index`, `drop_test_index`, `rebuild_index`, or any other DDL tool
  from this skill. Every DDL string the MCP returns is inert advice for a human
  DBA's change control.
- Never say a change is safe, approved, or applied. Never present an estimate as
  a measured gain.
- Never invent database names, query ids, index names, row counts, timings,
  percentages, or DDL. Use only values the MCP returned. Do not expose
  credentials, environment values, parameter values, or result rows.
- Treat absent Query Store history as unknown, not as zero activity.
- When blocked (the same tool call fails the same way twice, a required
  precondition cannot be met, or this skill's text contradicts what a tool
  returns), call `report_stuck` once with `skill`, `skill_version` (this file's
  `metadata.version`), `last_tool`, a short blocker category, and a
  one-sentence summary that contains no SQL, data values, or object, server, or
  database names. If `report_stuck` is not in the tool list, skip it silently.
  Either way, stop retrying that exact call and tell the user what is blocked.
  A tool error whose `failure_diagnostic.transient` is true (for example 40613,
  40501, 49918) follows normal retry guidance first.

## Runtime and database gate

Run these in order before any review:

1. `check_runtime_status`: require package version `2.4.0` or newer and a tool
   list that contains `review_workload_indexes`. Record the returned
   `runtime_fingerprint`, `runtime_compatibility_fingerprint`,
   `tool_schema_fingerprint`, and `sanitized_config_fingerprint`.
2. `list_databases`: show the returned allowlisted databases and let the user
   pick one. Never pick by default, memory, or nearest name.
3. `check_capabilities` for that database: require
   `mcp_contract.workload_index_advisor=1`.

If the gate fails, stop and name the missing piece (package upgrade, tool not
exposed by the active profile or tool groups, or database not allowlisted).
The `index-review`, `triage`, `optimizer`, and `sandbox` profiles all expose the
advisor. It needs only `VIEW DATABASE STATE` and `VIEW DEFINITION`; no policy
file, history table, or install step is required.

## Choose the scope

Ask only what changes the result; otherwise use these defaults and say so:

- Scope: the whole database. For one table or a set, pass `schema_name` plus
  `table_names`.
- Window: `lookback_days=7`. Use 14–30 days when removal decisions matter, and
  cover a full business cycle (month-end, batch nights) where Query Store
  retention allows. Use `as_of_utc` to review a past incident window instead of
  widening the window.
- Objective: `cpu` by default; `duration` when the complaint is latency;
  `logical_reads` when the database is I/O-bound.
- `top_queries=100`. Raise it when the returned `analyzed_share_pct` is below 80.

## Run the review

Call `review_workload_indexes` with the chosen arguments. Read `result_status`
before anything else:

- `ok`: present the report.
- `empty`: the window held no workload and no existing-index finding applied.
  Say so; suggest a longer window or a busier period.
- `precondition`: a setup step is missing, usually Query Store that is off or
  READ_ONLY. Show the returned `remediation` statement for the DBA to run; do
  not run it. Existing-index findings in the same report are still valid.
- `unavailable`: metadata could not be read. Report the reason; the usual fix is
  `VIEW DEFINITION` plus `VIEW DATABASE STATE` for the MCP identity.
- `not_supported`: the source does not exist on this engine. Do not send anyone
  to fix it.

Always report `gaps` verbatim. They state coverage limits: share of the
workload analysed, Query Store capture mode, unparseable plans, missing
selectivity, recent usage-counter resets.

## Read the evidence

For each table (highest `workload_share_pct` first) report:

- Size, row count, heap or clustered, and write activity (`dml_rows_per_day`).
- How queries reach it (`access_summary`: seeks, scans, lookups).
- Existing indexes with keys, includes, size, usage counters, protections, and
  how many analysed queries used each one.
- The table's recommendations by id.

For each recommendation report: action, target index, keys, includes, the
supporting query ids and their workload share, `confidence`, `reason_codes`,
`blockers`, write impact, size, rationale, risks, `ddl`, `rollback_ddl`, and
the validation path.

Label `estimated_max_benefit_pct` as an upper-bound estimate derived from
optimizer cost shares and measured Query Store totals. It ranks options; it
does not promise a saving. `estimated_max_size_mb` uses declared column widths
and is an upper bound too.

Report `rewrite_opportunities` separately: non-SARGable predicates and implicit
conversions cannot be fixed by an index. Route them to `sql-optimizer` with the
query ids.

## Design judgement

The MCP output is the evidence. You may add design judgement, clearly labelled
as yours:

- Prefer extending or widening an existing index over adding a new one when the
  MCP offers both options.
- Question wide include lists, low-selectivity leading keys, and recommendations
  on write-heavy tables (high `write_impact.penalty`).
- Point out when two recommendations on one table could share one index.
- Treat `review_index` items as investigation leads, not changes.

If you propose a definition different from the returned one, label it
"modified, unvalidated", show the full definition, and explain the trade-off.
Never change a returned `confidence`, drop a returned blocker, or relabel a
`drop_index` as safe.

## Removal discipline

`drop_index` and `consolidate_index` are the riskiest actions. Before routing
one to change control, state that:

- usage counters reset on failover, scaling, and restarts; check
  `usage_counters.days_since_reset` and the window against a full business cycle;
- the MCP checked Query Store plan references for the window, but Query Store
  capture mode `AUTO` can miss rare queries;
- no index hint, plan guide, or forced plan may name the index;
- the returned `rollback_ddl` must be kept with the change record.

Indexes that enforce uniqueness, back a constraint or foreign key, or support
partition switching are never removal candidates; the MCP already excludes them.

## Prove before production

For each create, extend, or widen recommendation worth pursuing:

1. Hand it to `sql-optimizer` with the supporting query id, keys, and includes.
   The optimizer proves it on a non-production copy through
   `start_tuning_session` and `benchmark_index_candidate` (sandbox profile,
   leased temporary index, A-B-A measurement, automatic cleanup).
2. Only a measured, validated candidate goes to the human DBA's change control,
   with the returned `ddl`, `rollback_ddl`, and validation steps.
3. After deployment, re-run `review_workload_indexes` (or compare the
   supporting query ids in Query Store) over an equal window after the change.

## Optional: long-term portfolio history

When a DBA has installed the two `dbatools` index-history tables and enabled
`allow_index_history_write` in the local database policy, the portfolio tools
`capture_index_review_snapshot`, `review_index_portfolio`, and
`get_index_review` keep daily usage snapshots across counter resets. Use them
only to strengthen a removal decision over 90 days or more. They are not needed
for workload-driven design and require a database policy with `allow_read=true`.

## Advisory lesson recall

After the gate passes and the review has returned, you may call
`recall_lessons` with exactly these fields:

`recall_lessons(skill=sql-index-manager, skill_version=2.0.0,
runtime_compatibility_fingerprint=<stable>, tool_schema_fingerprint=<stable>,
sanitized_config_fingerprint=<stable>, database_name=<selected>, tags=<supported>)`

Never send raw SQL, credentials, index names, parameter values, rows, or hidden
reasoning. Do not pass the process `runtime_fingerprint`. A recalled lesson can
reorder attention or flag a risk; it is never evidence and never changes a
returned confidence, blocker, or recommendation. If recall is unavailable,
continue unchanged.

This skill is recall-only. The advisor returns no evidence id and no terminal
link, so recommendation ids, review ids, and query ids are tracking references,
not learning evidence references. Do not call `record_decision`,
`review_decision`, `propose_lesson`, `list_learning_candidates`,
`create_handoff`, `get_handoff`, or `resolve_handoff` from this skill until a
future public MCP contract adds an index evidence bridge. Route work to other
skills in the report only, without invoking learning or handoff tools.

## Output

Return these sections in order:

1. **Outcome**: `result_status`, database, window, objective, and the number of
   recommendations by action. State that nothing was executed.
2. **Coverage and gaps**: queries analysed, `analyzed_share_pct`, Query Store
   state and capture mode, usage-counter age, and every returned gap.
3. **Top improvements**: from `summary.top_improvement_ids`, each with keys,
   includes, supporting queries, confidence, upper-bound benefit, write cost,
   and the `ddl`/`rollback_ddl` labelled "inert, for DBA change control".
4. **Cleanup**: from `summary.cleanup_ids`, with the removal discipline above.
5. **Per-table detail**: existing indexes and their usage, then the table's
   recommendations and review leads.
6. **Rewrite opportunities**: routed to `sql-optimizer`.
7. **Next steps**: what to prove with `sql-optimizer`, what to hand to the DBA,
   and when to re-run the review.

Keep it concrete: tables, columns, query ids, and returned numbers. Lead with
the few changes that matter most.
