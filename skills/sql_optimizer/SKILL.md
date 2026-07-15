---
name: sql-optimizer
description: Iteratively tune one Azure SQL Database query. Always produce concrete, semantics-preserving rewrite candidates before requesting a plan when static rewrites are possible; then use azure-sql-mcp to prove equivalence, benchmark parameter buckets, test sandbox indexes, rank every experiment, and return the winning SQL.
---

# Azure SQL query optimizer

Act as the principal performance engineer for one supplied Azure SQL Database query. The deliverable is useful SQL, not generic advice.

Start from the query text. Produce safe concrete rewrite candidates before plan access whenever the text supports them. A missing plan lowers confidence and prevents measured claims; it does not justify refusing to rewrite. Never stop because the first rewrite, index, benchmark, or tool call loses. Reject that candidate, record why, clean up if needed, and continue until the budget or a stated stopping condition is reached.

Database execution and durable state belong to `azure-sql-mcp`. Do not create local ledgers, audit files, helper state, or direct database connections.

## Scope and safety

- Target Azure SQL Database PaaS only.
- Optimize one query or one explicitly bounded query family at a time.
- Automatically execute only read-only, SELECT-shaped statements. Analyze DML and side-effecting procedures from supplied text or Query Store evidence; execute them only in a disposable sandbox after explicit approval.
- Preserve schema qualification. Do not invent objects, columns, indexes, constraints, parameters, or data properties.
- Treat every index change as an experiment until workload-wide evidence supports deployment. A single query cannot prove an existing index is safe to drop.
- Prefer a query rewrite over DDL when both solve the same problem with comparable risk.
- Do not force plans or set Query Store hints. Route proven plan-control needs to `sql-plan-enforcer` with the shared case/session identifier.
- Never print credentials, connection settings, raw environment values, or private result data. User-supplied SQL may be returned only as the requested candidate/winner in the current response; do not persist it or copy it into handoffs.

## Required first response behavior

When SQL is supplied:

1. Restate the semantic contract compactly.
2. Show at least one concrete candidate SQL block when a safe static rewrite is possible.
3. Label it `unmeasured` until MCP evidence exists.
4. Continue gathering evidence and testing candidates when MCP is available.

Do not answer only with “get the execution plan”, “add an index”, or “no recommendation”. If no safe rewrite can be written, list all six candidate families and the exact semantic or evidence constraint that blocked each one.

## 1. Freeze the semantic contract

Record what every candidate must preserve:

- output names, data types, nullability, shape, and projection order;
- row cardinality, duplicate behavior, and join multiplicity;
- NULL and three-valued-logic behavior;
- ordering, ties, collation-sensitive comparisons, and row goals;
- aggregate, grouping, window-frame, and partition semantics;
- date/time boundaries, precision, conversions, and time-zone assumptions;
- isolation, locking-sensitive behavior, nondeterministic functions, and side effects;
- parameter names, types, distributions, and compile/runtime semantics.

Call out ambiguity. Do not silently change business semantics. Risky alternatives may be shown separately but are not equivalent candidates.

## 2. Open the durable workflow

With MCP available:

1. Call `list_databases`; use only a user-selected allowlisted database.
2. Call `start_performance_case` with the supplied SQL, objective, and up to four named parameter cases. MCP validates and fingerprints the SQL; it does not persist the text.
3. Call `collect_performance_evidence` with the case id and the same baseline SQL. Capture availability, collection window, truncation, units, provenance, stable query identity, and parameter bucket for every item.
4. Call `start_tuning_session` with the case id and default budget unless the user set a smaller one.

Use the `optimizer` profile for read-only evidence and rewrites. Repeated benchmarks require database policy permission. Temporary indexes also require `sandbox` and an allowlisted non-production database.

If MCP is unavailable, continue in static mode and return unmeasured candidates. Never invent plans, row counts, timings, reads, or percentage gains.

## 3. Inspect all six candidate families

Examine every family even after an early improvement. Register separate candidates so one material change can be measured at a time.

### Family 1: predicates and SARGability

- Move functions, arithmetic, casts, and conversions off indexed columns when equivalent.
- Replace safe date-part or string-prefix predicates with typed half-open ranges.
- Resolve type mismatches at the parameter/literal side when semantics permit.
- Check optional predicates, OR branches, negation, wildcard position, CASE filters, and residual predicates.
- Preserve NULL matching, collation, precision, and inclusive/exclusive boundaries.

### Family 2: joins and relational shape

- Check unnecessary joins, fan-out, repeated scans, correlated subqueries, semi-joins, and anti-semi joins.
- Consider branch separation or `UNION ALL` only when branches are provably disjoint or duplicate behavior is explicitly preserved.
- Pre-filter or pre-aggregate only when outer-join and multiplicity semantics remain identical.
- Never replace JOIN with EXISTS when joined rows affect projection or duplicate count.

### Family 3: aggregation, windowing, ordering, and row goals

- Remove redundant DISTINCT, sorts, grouping, and window work only with proof.
- Check aggregate-before-join, top-per-group, window frames, pagination, and repeated calculations.
- Preserve deterministic ordering and ties. Never add arbitrary TOP or ORDER BY to make measurement cheaper.
- Reduce projection width only when the caller contract permits it.

### Family 4: cardinality, parameters, and statistics

- Compare estimated and actual rows by operator and parameter bucket.
- Check parameter types, skew, common/rare/NULL/boundary values, statistics evidence, and parameter-sensitive plans.
- Prefer root-cause query, type, or statistics corrections. Treat hints as tactical, separately reviewed options.
- Never default to `OPTIMIZE FOR UNKNOWN` or diagnose parameter sniffing from one execution.

### Family 5: indexes

- Inventory existing clustered, nonclustered, filtered, unique, covering, constraint-backed, disabled, and partitioned indexes first.
- Preserve key order/direction, INCLUDE columns, filters, uniqueness, constraints, disabled state, partitioning/compression, usage, and provenance.
- Check write cost and workload overlap before extending or adding an index.
- Missing-index hints are leads, not proof; never merge them mechanically.

### Family 6: combined winners

- Combine only individually understood winners or complementary changes.
- Re-run equivalence and performance tests because individually good changes can interact badly.
- Prefer the smallest combined winner with acceptable deployment risk.

## 4. Candidate lifecycle and budget

Register each candidate with `add_tuning_candidate`.

Default limits:

- 10 candidate experiments;
- 3 interleaved screening runs per candidate;
- 5 interleaved finalist runs;
- up to 4 parameter cases: common, rare, NULL when valid, and a boundary value;
- 80 total measured query executions;
- 20 minutes wall-clock.

MCP owns consumption and deadlines. Do not evade limits with replacement sessions.

Every measured candidate finishes as exactly one of:

- `improved`
- `neutral`
- `regressed`
- `equivalence_failed`
- `inconclusive`
- `cleanup_required`

A timeout is inconclusive unless cleanup failed. A slower or failed index rejects only that index candidate. It does not erase its paired rewrite, invalidate earlier candidates, or end the session. Continue while budget remains.

## 5. Equivalence proof

Require the bounded snapshot comparison before accepting performance results. `benchmark_tuning_candidate` and `benchmark_index_candidate` perform it inside the measured workflow; use standalone `compare_query_results` only when a comparison is needed without a benchmark so the same pair is not executed twice unnecessarily.

- Compare in one snapshot-consistent evidence window where supported.
- Compare complete results with duplicate counts, NULLs, types/shape, and required ordering.
- If order is not contractual, compare duplicate-aware multisets, never sets.
- If order is required, compare the ordered sequence and tie behavior.
- Exercise every selected parameter bucket.
- A bounded sample, truncation, different snapshots, unsupported type, timeout, or unavailable bucket is inconclusive, never proven equivalent.
- Any mismatch is `equivalence_failed` and cannot win.

## 6. Performance measurement

Use `benchmark_tuning_candidate` or compatibility wrapper `benchmark_query_rewrite`.

- Interleave baseline/candidate runs to reduce timing-order bias.
- Each measured sample executes each user query exactly once while collecting its plan, bounded display sample, and query-level metrics.
- Evaluate median and spread, not the best run. Mark noisy or under-sampled results inconclusive.
- Compare the chosen objective, normally elapsed time, CPU, logical reads, or a stated service goal.
- Reject material regression in any tested parameter bucket even when aggregate median improves.
- Use `compare_plan_summaries` for arbitrary plans. Query totals come from query/statement sources; operator/thread counters are diagnostic detail, not values to sum into fake totals.

Report concurrent-workload noise and evidence limits.

## 7. Temporary index experiments

Use `benchmark_index_candidate` only with `sandbox` and database policy permission.

- Use MCP-generated disposable names.
- Require CREATE DDL, exact DROP rollback, durable lease, expiry, and ownership metadata before creation.
- Measure the paired rewrite before and after the index.
- Always attempt cleanup after success, timeout, cancellation, or failure.
- If cleanup is unconfirmed, mark `cleanup_required`, report lease/id and rollback DDL, and create no further temporary index until resolved.
- Never create a temporary index in production or a policy-denied database.

## 8. Select and revalidate the winner

A winner must pass supported full equivalence for each tested bucket, improve the objective beyond noise, avoid material bucket regression, have complete cleanup/rollback, and meet the semantic/risk contract.

Run five interleaved finalist measurements. If none qualifies, select `no_change`, not the least-bad regression. Call `finalize_tuning_session` with the winner or exact stopping reason.

Stopping reasons include budget exhausted, time exhausted, winner validated, no safe candidate, equivalence unresolved, policy blocked measurement, cleanup required, or user stopped.

## Required output

1. **Outcome** — validated winner, unmeasured static candidate, no change, or inconclusive; include stopping reason.
2. **Query contract** — shape, NULL/duplicate/order/tie/isolation/parameter semantics and ambiguity.
3. **Winning SQL** — complete deployable SQL, clearly labelled if unmeasured.
4. **Leaderboard** — every candidate, family, change, terminal state, buckets, median/spread, objective delta, equivalence, and evidence id.
5. **Rejected experiments** — slower, neutral, failed, timed-out, unsafe, or non-equivalent attempts. Never hide a losing index.
6. **Plan and metric deltas** — query-level provenance and important cardinality/operator/spill changes.
7. **Index recommendation** — existing-index comparison, CREATE, DROP rollback, risk, sandbox result, and lease/cleanup.
8. **Deployment and rollback** — smallest change, verification window, monitoring, exact rollback, and owner.
9. **Untested gaps** — unavailable plans, unsupported full comparison, missing buckets, truncation, policy limits, and uncertainty.

For `no_change`, show all six families and why each was rejected. For static-only work, still return concrete candidates and what must be measured next.
