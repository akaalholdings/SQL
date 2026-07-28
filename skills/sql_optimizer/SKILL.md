---
name: sql-optimizer
description: Tune one Azure SQL Database PaaS query through rewrite-first static analysis and an evidence-bound azure-sql-mcp workflow. Produce complete SQL, prove equivalence, benchmark parameter buckets, test gated sandbox indexes or views, preserve every experiment in a leaderboard, and hand off only a validated deployable winner.
---

# Azure SQL Database query optimizer

Act as the performance engineer for one supplied Azure SQL Database PaaS query or one explicitly bounded query family. The deliverable is concrete SQL and an evidence-bounded decision, not generic tuning advice.

Start from the query text. Produce safe concrete rewrite candidates before plan access whenever static analysis supports one. A missing plan lowers confidence; it does not prevent candidate generation. Never stop because the first rewrite, index, benchmark, or tool call loses. A timeout is a candidate state, not a reason to abandon the session. Reject that candidate, record the terminal state, clean up when required, and continue while budget and safety gates permit.

`azure-sql-mcp` owns database execution and durable performance state. Do not open a direct database connection, create local ledgers or audit files, persist raw SQL, print credentials or raw environment values, or invent metrics. Synthetic SQL is acceptable in examples; do not invent schema, indexes, parameters, statistics, or data properties for a real query.

## Scope and mutation boundary

- Target Azure SQL Database PaaS only. Do not tune Azure SQL Managed Instance, SQL Server, or a generic engine through this skill.
- Automatically execute only read-only SELECT-shaped SQL. Analyze DML or side-effecting procedures from supplied text and evidence, but do not execute them through this workflow. The only DDL exceptions are separately authorized MCP sandbox index/view workflows; they are never automatic.
- Preserve schema qualification, result names/types/shape, duplicates, NULL and three-valued logic, ordering and ties, collation, date precision, isolation, nondeterminism, and parameter compile/runtime semantics.
- Prefer a rewrite over DDL when comparable. Treat every index or view change as an experiment; one query cannot prove a production-wide index drop or write-cost tradeoff.
- Do not force plans or set Query Store hints. Hand plan-control needs to `sql-plan-enforcer` with the shared case/session identifier.

## Runtime contract gate

Initialize the MCP contract by calling `check_runtime_status`, then
`list_databases`, then `check_capabilities`. Read the returned
`tool_groups` before selecting tools; do not infer exposed groups
from the profile name. Complete this sequence before `explain_query` or any
case/session tool. Record the returned `runtime_fingerprint`,
`tool_schema_fingerprint`, `sanitized_config_fingerprint`, active profile, and
tool groups.
Require the returned `runtime_fingerprint`, `tool_schema_fingerprint`, and
`sanitized_config_fingerprint` to be present and stable for the same MCP process.
A missing, changed, or mismatched runtime/tool schema is a hard stop for measured
work: remain static or report `inconclusive`; do not guess the schema or fall back
to a different host/profile. Re-check the fingerprints before a campaign and
when resuming persisted state.

After any configuration, environment, tool-group, or profile change, perform a
full host restart before using the MCP again. Do not rely on hot reload or a
partial child-process restart. Never widen the returned local policy to satisfy
a requested budget; report the policy cap and stop or continue within it.

## Required first response behavior: rewrite-first response contract

When SQL is supplied:

1. State the semantic contract and ambiguities compactly. Repeat every supplied
   column/parameter SQL type, nullability, exact allowed value domain, and
   boundary/overflow precondition; never weaken an exact range to “bounded”.
2. Show at least one concrete candidate SQL block whenever a safe static rewrite is possible. Return it as a complete fenced `sql` candidate labelled `static candidate` and `unmeasured`.
3. Explain the trigger, preconditions, and risk for each material rewrite.
4. Then request or collect plan/evidence. MCP changes confidence and measurement status, not candidate generation.

Do not answer only “get the execution plan”, “add an index”, or “no recommendation”. If no safe rewrite is possible, name every pattern family below and the exact missing semantic or evidence precondition. A losing candidate never ends the session.

If MCP is unavailable, continue in static mode and return unmeasured candidates. Do not invent plans, row counts, timings, reads, or percentage gains.

## Candidate states

Keep these states distinct in prose and in the MCP leaderboard:

- **Static candidate**: complete SQL generated from text; no MCP proof; all metrics are `not collected`.
- **Measured candidate**: registered with `add_tuning_candidate` and screened by MCP; report only returned medians, spread, objective deltas, plan evidence, and equivalence status.
- **Finalist**: a measured candidate that merits the finalist phase after screening; re-run it interleaved across the selected buckets.
- **Performance-only outcome**: an explicitly selected `performance_only` result; report its returned performance classification and selected objective, but set equivalence to `not proven` and deployment to `not ready`.
- **Deployable winner**: a finalist with full supported equivalence, improvement beyond observed noise, no material bucket regression, known rollback, complete cleanup, and a written deployment/verification handoff. It is never implied by a static or screening result.

The session outcome may be `no_change`, `performance_only`, `inconclusive`, or a validated winner. Never convert an unavailable, truncated, timed-out, or unmeasured value into zero, a percentage, or a claim of improvement.

## Freeze the semantic contract

Record before comparing candidates:

- projection order, aliases, types, nullability, row shape, cardinality, duplicate multiplicity, and join fan-out;
- NULL matching, three-valued logic, collation, case/accent sensitivity, and implicit conversion behavior;
- required order, tie rules, pagination continuation, TOP/row-goal semantics, and deterministic ordering;
- aggregate/group/window partition and frame semantics;
- date/time boundaries, precision, time zone assumptions, and inclusive/exclusive endpoints;
- isolation, locking-sensitive behavior, nondeterministic functions, side effects, and parameter names/types/distributions.

Reproduce supplied SQL types with their length, precision, and scale, and
reproduce exact domain endpoints in the report. If a rewrite relies on an
overflow, rounding, or representability precondition, state that precondition
with the exact boundary. Do not summarize `2000-01-01 through 9999-12-30` as
merely “bounded”, for example.

Do not silently turn an equivalence question into a business-rule change. Put risky alternatives in a separate non-equivalent section.

## Operational pattern cards

Each card is actionable only when its semantic preconditions hold. For every card, record the corresponding MCP evidence before calling a measured result proven.

### Family 1: predicates and SARGability

- **Trigger:** a function, arithmetic expression, cast, or type conversion wraps an indexed/filter column, or the plan shows a residual predicate or scan.
- **Safe rewrite/action:** move the operation to the parameter/literal side; use a typed half-open range for a date/time bucket; align parameter and column types.
- **Preconditions:** the conversion is equivalent for all values, precision and collation are unchanged, and NULL/boundary behavior is preserved.
- **Counterexample/risk:** converting the parameter can change overflow, rounding, non-sargable collation behavior, or the matched datetime boundary; an implicit conversion may be intentional.
- **MCP evidence:** `explain_query`; actual plan predicate/residual details; `compare_plan_summaries`; type metadata; row estimates versus actual rows; logical reads and elapsed time by bucket.

#### Concrete predicate rewrites

For a date bucket, prefer a typed half-open range over a function on the column. This preserves every time value in the day and gives an index on `OrderDate` a seekable boundary:

```sql
-- Static candidate, unmeasured
SELECT
    o.OrderId,
    o.OrderDate
FROM dbo.Orders AS o
WHERE o.OrderDate >= @Day
  AND o.OrderDate < DATEADD(day, 1, @Day);
```

Use this only when `@Day` is typed consistently with the contract and the upper-bound calculation cannot overflow. Do not replace an inclusive arbitrary timestamp range with this form.

Keep the column bare when types can be aligned:

```sql
-- Static candidate, unmeasured
SELECT
    o.OrderId
FROM dbo.Orders AS o
WHERE o.CustomerId = @CustomerId;
```

Bind `@CustomerId` as the column’s SQL type. Do not repair a mismatched application parameter by converting `o.CustomerId` in the predicate. Likewise, move safe arithmetic to the constant side (`Amount >= @MinimumAmount`, not `Amount * 1.2 >= @Threshold`) only after checking decimal scale, rounding, overflow, and NULL behavior.

For prefix search, `Name LIKE @Prefix + N'%'` may be seekable under the active collation. A leading wildcard remains a contains search and must not be represented as a prefix range. Avoid `ISNULL(column, value)`, `COALESCE(column, value)`, CASE, scalar functions, or implicit type conversion around a search column unless no equivalent bare-column predicate exists.

### Family 2: joins and relational shape

#### Optional filters, OR branches, and LIKE

- **Trigger:** optional filters such as `(@p IS NULL OR col = @p)`, broad OR branches, CASE-wrapped filters, negation, or `LIKE` with a non-prefix wildcard.
- **Safe rewrite/action:** split genuinely distinct parameter states into explicit branches or separate statements; use a prefix range only for a known prefix search; keep an optional-filter branch when NULL means “all rows”.
- **Preconditions:** branch result multisets and order are identical, parameter types match, branches are disjoint or duplicate behavior is preserved, and the application can select the branch.
- **Counterexample/risk:** `UNION ALL` can duplicate rows; `LIKE '%x'` cannot become a prefix seek; dynamic SQL can change security, plan cache, or parameter semantics.
- **MCP evidence:** parameter buckets, Query Store history, `explain_query` for NULL/common/rare values, residual predicates, plan variants, and `compare_query_results` per branch/bucket.

When NULL means “no filter”, generate two independently executable candidates instead of assuming one catch-all predicate will compile well:

```sql
-- NULL branch: static candidate, unmeasured
SELECT
    o.OrderId,
    o.CustomerId
FROM dbo.Orders AS o;
```

```sql
-- Non-NULL branch: static candidate, unmeasured
SELECT
    o.OrderId,
    o.CustomerId
FROM dbo.Orders AS o
WHERE o.CustomerId = @CustomerId;
```

The application or stored procedure must choose exactly one branch. Do not combine overlapping branches with `UNION ALL`, and do not use dynamic SQL unless ownership, parameterization, permissions, and plan-cache behavior are part of the approved design.

#### Joins, EXISTS, and fan-out

- **Trigger:** a join multiplies rows, a correlated subquery repeats work, or the query checks existence without selecting child columns.
- **Safe rewrite/action:** use `EXISTS`/`NOT EXISTS` only for a true semi/anti-join; pre-filter or pre-aggregate the many-side only when multiplicity and outer-join behavior stay the same; remove a provably unused join.
- **Preconditions:** join keys, NULL behavior, duplicate count, outer-join preservation, selected columns, and predicates are fully accounted for.
- **Counterexample/risk:** replacing JOIN with EXISTS removes fan-out; pre-aggregation can change NULL groups; a “redundant” join may enforce security or filter rows.
- **MCP evidence:** object/dependency metadata, join estimates versus actuals, fan-out rows, key lookups, `compare_query_results` with duplicate-aware comparison, and plan deltas.

If child rows are used only to test existence, a semi-join can avoid fan-out:

```sql
-- Static candidate, unmeasured
SELECT
    c.CustomerId,
    c.DisplayName
FROM dbo.Customers AS c
WHERE EXISTS
(
    SELECT 1
    FROM dbo.Orders AS o
    WHERE o.CustomerId = c.CustomerId
      AND o.Status = @Status
);
```

This is not equivalent when the original query returned one row per matching order. For `NOT EXISTS`, verify NULL semantics instead of mechanically replacing `NOT IN`. For outer joins, keep predicates on the nullable side in the correct `ON` or `WHERE` location; moving them can turn an outer join into an inner join.

Pre-aggregate the many-side only when the original result already had one row per parent:

```sql
-- Static candidate, unmeasured
WITH OrderTotals AS
(
    SELECT
        o.CustomerId,
        SUM(o.Amount) AS TotalAmount
    FROM dbo.Orders AS o
    WHERE o.OrderDate >= @FromDate
      AND o.OrderDate < @ToDate
    GROUP BY o.CustomerId
)
SELECT
    c.CustomerId,
    ot.TotalAmount
FROM dbo.Customers AS c
INNER JOIN OrderTotals AS ot
    ON ot.CustomerId = c.CustomerId;
```

Check empty groups, duplicate parents, decimal accumulation type, and predicates from other child tables. A CTE is syntax, not guaranteed materialization. If an expensive intermediate result is referenced repeatedly, test a temporary table only in an approved disposable execution context and include its populate cost, indexes, recompilation effect, and cleanup in the measurement.

### Family 3: aggregation, windowing, ordering, and row goals

- **Trigger:** repeated sorts/scans, late aggregation, `ROW_NUMBER`, `TOP`, `OFFSET/FETCH`, or a top-per-group pattern dominates the plan.
- **Safe rewrite/action:** aggregate before a one-to-many join when proven safe; use a partitioned window with an explicit tie rule; use keyset pagination only when the caller has a stable unique continuation key; remove redundant DISTINCT/grouping.
- **Preconditions:** grouping keys, window frame, ties, page boundary, total-count behavior, and output order are contractual and preserved.
- **Counterexample/risk:** `TOP (1)` without a unique order is nondeterministic; keyset pagination is not equivalent to arbitrary OFFSET; moving an aggregate across an outer join changes empty-group results.
- **MCP evidence:** actual row counts, sort/aggregate operators, memory grants, spill warnings, ordering metadata, per-bucket equivalence, and `compare_plan_summaries`.

For top-per-group work, make ties deterministic:

```sql
-- Static candidate, unmeasured
WITH RankedOrders AS
(
    SELECT
        o.CustomerId,
        o.OrderId,
        o.OrderDate,
        ROW_NUMBER() OVER
        (
            PARTITION BY o.CustomerId
            ORDER BY o.OrderDate DESC, o.OrderId DESC
        ) AS row_number
    FROM dbo.Orders AS o
)
SELECT
    CustomerId,
    OrderId,
    OrderDate
FROM RankedOrders
WHERE row_number = 1;
```

The unique tie-breaker is part of the result contract. Do not add it if the business rule intentionally returns all ties. For deep pagination, test keyset continuation on the complete unique sort key; it is not a drop-in replacement for arbitrary page-number access. `DISTINCT` is not a generic join-fan-out repair: remove it only after proving uniqueness without it.

#### Row goals, sorts, and spills

- **Trigger:** a row goal, large sort, memory-grant warning, tempdb spill, or parallel exchange is visible in the plan.
- **Safe rewrite/action:** make required order and tie-breakers explicit; narrow only contract-approved projections; align filtering and ordering keys; remove work only when the result contract permits it.
- **Preconditions:** no arbitrary TOP/ORDER BY is introduced, order semantics remain exact, and memory/resource changes are measured under representative concurrency.
- **Counterexample/risk:** a smaller first page can regress later pages; eliminating a sort can change order; a spill can be a shared memory-pressure symptom rather than a query-shape defect.
- **MCP evidence:** actual plan row goals, sort keys, spills, memory grants, worker/parallelism details, elapsed/CPU/logical reads, resource history, waits, and concurrent blocking.

#### UDFs, APPLY, and intermediate work

- A scalar UDF can hide row-by-row CPU. Confirm whether scalar UDF inlining is supported, enabled, and actually used before rewriting it. Inline the logic manually only when exception, NULL, collation, data-access, and nondeterministic behavior remain identical.
- `CROSS APPLY` and `OUTER APPLY` are useful for correlated top-N or reusable expressions, but they do not guarantee one execution. Inspect actual executions and row counts on the inner side.
- A temporary table can improve cardinality between phases but adds writes, tempdb use, statistics, and compilation boundaries. Include all phases in the benchmark.
- Table variables, multi-statement table-valued functions, and opaque predicates can hide cardinality. Treat modern deferred compilation or interleaved execution as capability-dependent, not assumed.

### Family 4: cardinality, parameters, and statistics

- **Trigger:** estimated and actual rows diverge materially, a seek/lookup choice varies by value, statistics are stale, or common and rare values behave differently.
- **Safe rewrite/action:** correct parameter/literal types, make predicates estimable, refresh or create statistics only through an approved change path, and isolate a proven skew-sensitive branch.
- **Preconditions:** statistics/object metadata and parameter values are real, freshness is known, and one execution is not treated as a distribution.
- **Counterexample/risk:** `OPTIMIZE FOR UNKNOWN` can harm both common and rare values; a statistics update has write/concurrency cost; parameter “sniffing” is not proven from one plan.
- **MCP evidence:** Query Store history, statistics metadata, estimated/actual rows by operator, parameter buckets, plan fingerprints, resource pressure, and `compare_plan_summaries`.

#### Parameter compilation and intelligent query processing

- **Trigger:** parameter-sensitive plans, optional-parameter optimization, memory-grant feedback, cardinality feedback, or plan variants appear in evidence.
- **Safe rewrite/action:** first make the predicate and parameter contract compatible with the feature; preserve a feature-compatible query, or test explicit branches only when the feature is absent/insufficient.
- **Preconditions:** database compatibility level, feature state, Query Store history, parameter distribution, and variant behavior are confirmed.
- **Counterexample/risk:** disabling a feedback feature or adding a hint can remove useful variants; a variant seen in one window may not represent current workload.
- **MCP evidence:** Query Store plan variants, compatibility/configuration evidence, parameter-sensitivity tools, memory-grant/row feedback, and non-overlapping bucket measurements.

Execute supplied parameters through typed `sp_executesql`, not local `DECLARE` variables, because local-variable compilation can change estimates and hide parameter sensitivity. Every measured bucket carries one exact value and declared SQL type for every query parameter. Keep baseline and candidate on the same bucket.

Do not prescribe `OPTION (RECOMPILE)`, `OPTIMIZE FOR`, `OPTIMIZE FOR UNKNOWN`, a Query Store hint, or forced parameterization by reflex:

- recompilation spends CPU and removes plan reuse; it can be appropriate only when execution savings dominate compile cost;
- optimize-for choices trade one distribution for another and must be measured across common and rare values;
- Query Store hints and forcing belong to `sql-plan-enforcer`, not this skill;
- Parameter Sensitive Plan optimization, Optional Parameter Plan Optimization, memory-grant feedback, cardinality feedback, batch mode, deferred compilation, and UDF inlining depend on compatibility level, database-scoped configuration, query shape, and observed runtime state.

Use `check_capabilities`, database configuration, Query Store variants, and actual plan evidence before crediting or disabling an intelligent-query-processing feature. A compatibility-level change affects the whole database and is never a single-query tuning experiment.

For Azure SQL Database, interpret the capability envelope with these concrete gates:

- compatibility level 140 enables the applicable adaptive-join, batch-mode memory-grant-feedback, and interleaved-execution families when their database-scoped configurations and query shapes also qualify;
- compatibility level 150 enables the applicable row-mode memory-grant feedback, batch mode on rowstore, table-variable deferred compilation, and scalar UDF inlining families;
- compatibility level 160 enables Parameter Sensitive Plan optimization and cardinality-estimation feedback when configured; persistent feedback features also require Query Store in `READ_WRITE`;
- compatibility level 170 enables Optional Parameter Plan Optimization for eligible optional predicates when its database-scoped configuration is on;
- DOP feedback is capability- and Query-Store-dependent and can remain off even at a qualifying compatibility level.

An applicable or enabled feature is not proof that this query used it. Require an observed dispatcher/variant, feedback state, plan shape, or Query Store evidence before attributing an improvement. If `check_capabilities` cannot verify Azure SQL Database PaaS, compatibility level, configuration, or Query Store state, mark the feature conclusion inconclusive.

### Family 5: indexes

#### Ordinary, filtered, computed, and indexed-view options

- **Trigger:** a proven predicate/join/order is uncovered, a lookup is material, or existing indexes overlap while writes are costly.
- **Safe rewrite/action:** compare existing keys/includes first; test the smallest covering or key-order change; consider a filtered index for a stable selective predicate, a persisted computed-column index for a deterministic expression, or an indexed view for a stable expensive aggregate.
- **Preconditions:** exact data types, deterministic/set-option requirements, filter truth domain, write workload, uniqueness/constraints, partitioning, compression, and indexed-view restrictions are verified from MCP metadata and an approved sandbox.
- **Counterexample/risk:** a filtered index misses parameter values; computed/indexed-view maintenance adds write cost and storage; missing-index suggestions are leads, not proof; never drop a production index from one query.
- **MCP evidence:** `analyze_query_indexes`, `analyze_workload_indexes`, object/index metadata, usage and write impact, plan access paths, benchmark results for every bucket, and lease/cleanup state.

Design from the access pattern, not from a missing-index percentage:

- preserve key column order and ASC/DESC direction; equality predicates often lead, followed by range or required ordering columns, but actual selectivity and sort requirements decide;
- use INCLUDE columns only for output/residual coverage; an INCLUDE cannot seek or provide key order;
- keep filters logically identical to the stable workload predicate and verify parameter values can use the filtered index;
- preserve uniqueness, constraint ownership, disabled state, partition alignment, compression, and current covering/overlap metadata;
- count write amplification, storage, log generation, maintenance, and plan-cache effects; never infer a safe production drop from one query;
- verify the candidate plan actually uses the test index. A faster run that ignored it is not evidence for the index.

Computed-column and indexed-view options require deterministic expressions, supported data types, precise SET options, ownership compatibility, and write-cost review. A normal view stores no data and normally expands into the consumer query; changing it affects every caller. An indexed view is physical schema, not merely a query rewrite.

## Plan reading discipline

- **Trigger:** a plan is available or the candidate changes an operator, access path, estimate, memory grant, or parallel shape.
- **Safe rewrite/action:** identify the first material divergence from the desired shape, then change one cause at a time; preserve the query contract while addressing residual predicates, lookups, spills, or bad estimates.
- **Preconditions:** distinguish estimated from actual plans, query totals from operator counters, and plan evidence from Query Store/resource evidence.
- **Counterexample/risk:** operator counters cannot be summed into fake query totals; a missing actual plan is not proof of no issue; a forced plan belongs to the enforcement workflow.
- **MCP evidence:** `explain_query`, `compare_plan_summaries`, actual plan artifact/provenance, Query Store plan identity, wait/resource envelopes, and query-level metrics.

Treat `explain_query` as an actual query plan only when the request used
`analyze=true`, the response says `query_executed=true`, and non-empty
`summary.actual_metrics` are accompanied by non-empty `metric_provenance` or
equivalent provenance. Require `plan_kind=actual` for that claim.
`analyze=true` alone is insufficient. Otherwise label the result estimated or
metadata-only and do not report actual rows, elapsed time, reads, or other
execution metrics.

Read plans from the first material divergence, not from the visually most expensive icon:

- compare estimated rows, actual rows, actual rows read, executions, and output rows at the same operator;
- distinguish seek predicates from residual predicates and a useful lookup from millions of repeated lookups;
- inspect join inputs, build/probe choice, spills, memory grant requested/granted/used, row goals, serial zones, exchange skew, and batch versus row mode;
- treat estimated subtree cost as optimizer currency, not elapsed time;
- use client wall time, Query Store duration/CPU, and `STATISTICS IO` table messages only with their stated units and collection window;
- never sum per-operator or per-thread runtime counters into fake query totals;
- correlate Query Store only through an exact query identity or plan/query hash. Ambiguous or missing identity is inconclusive, never a fuzzy nearest-text match.

## Azure SQL Database operating context

- **Trigger:** CPU, data IO, log IO, workers, memory grants, throttling, waits, blocking, deadlocks, or concurrent load coincides with the query regression.
- **Safe rewrite/action:** reduce query work only after separating shared pressure; adjust the test window or hand off an estate/resource issue to triage; do not claim a query fix from a quieter sample.
- **Preconditions:** Azure SQL service tier/compute model, collection window, concurrency, isolation, workload overlap, and replica/read-scale destination are known.
- **Counterexample/risk:** scaling or killing sessions is outside this skill; a faster isolated run can be worse under concurrency; resource evidence may be partial.
- **MCP evidence:** `collect_performance_evidence`, Query Store/resource history, waits, blocking/open transactions, worker/memory/IO limits, and availability/truncation/provenance fields.

Interpret the query inside its Azure SQL Database limits:

- compare CPU, data I/O, log write, workers/sessions, memory, storage, and governance percentages with the database’s actual objective and collection window;
- in an elastic pool, separate database pressure from shared pool pressure;
- in serverless, exclude resume/cold-cache effects or label them as part of the workload requirement;
- in Hyperscale or read scale-out, record the replica and data-access path because cache and remote-page behavior can change measurements;
- check blocking, lock waits, snapshot/version-store behavior, memory grants, and concurrent workload before blaming the plan;
- verify Query Store state, capture mode, retention, and interval coverage; absent history is not zero activity;
- detect Automatic Tuning ownership and do not silently compete with automatic force-plan or index actions;
- use database-scoped configuration and supported Query Store controls. Do not recommend instance trace flags, SQL Server service changes, or instance tempdb file layout for Azure SQL Database.

### Family 6: combined rewrites and rewrite-plus-index lineage

- **Trigger:** two or more safe rewrite families interact.
- **Safe rewrite/action:** register a combined multi-family rewrite as `strategy=combined` with no parent lineage and benchmark it with `benchmark_tuning_candidate`.
- **Preconditions:** every combined rewrite preserves the frozen contract; measure its complete SQL as one candidate and do not add metrics from separate runs.
- **Counterexample/risk:** combined rewrites can hide which change caused a regression or alter fan-out, order, or NULL behavior.
- **MCP evidence:** candidate strategy, complete SQL fingerprint, per-bucket result/equivalence evidence, and plan deltas.
- **Trigger:** a proven `improved` finalist or an evidence-backed `performance_only` finalist remains worth testing with a temporary index.
- **Safe rewrite/action:** register the `rewrite_plus_index` lineage with `strategy=rewrite_plus_index` and `artifact_ref` exactly `candidate:<parent-id>`, then run only `benchmark_index_candidate` for the parent's marginal A-B-A effect.
- **Preconditions:** the parent is in the same session, is either proven or has complete nonzero performance-only finalist evidence, and has stable SQL/database/runtime fingerprints. A performance-only parent propagates `parent_equivalence=unproven`; an improving child remains `performance_only` and can never become proven, `improved`, or deploy-ready. Neutral, regressed, equivalence-failed, inconclusive, cleanup-required, or cross-session parents are ineligible.
- **Counterexample/risk:** a good rewrite plus a good index can duplicate work, change join order, or regress another bucket; lineage never upgrades index evidence into rewrite proof.
- **MCP evidence:** `get_tuning_session` lineage, parent/child evidence ids, per-bucket A-B-A deltas, equivalence scope, and sandbox cleanup state.

### Views and deployment handoff

- **Trigger:** the query is against a view, the definition hides a costly join/aggregate, or an indexed-view option is proposed.
- **Safe rewrite/action:** first rewrite the consumer query; if the view definition itself is the candidate, capture the complete definition and dependencies, use `prepare_view_change`, and apply only through the gated sandbox workflow.
- **Preconditions:** dependency graph, callers, security ownership, schema binding/set options where applicable, prior definition, validation query, and rollback definition are recorded.
- **Counterexample/risk:** changing a view changes every caller; an indexed view has strict eligibility and maintenance costs; a definition that helps one query can regress others.
- **MCP evidence:** `get_object_details`, `get_dependencies`, view/index metadata, `prepare_view_change`, `apply_prepared_view_change`, `verify_view_change`, `rollback_view_change`, and before/after workload evidence.

## Exact MCP workflow

Use the explicit case/session tools so the leaderboard is complete. Compatibility tools `tune_query` and `benchmark_query_rewrite` may be used only when the server exposes no explicit equivalent and must still return the same states and evidence.

1. **Static pass:** freeze the contract, inspect every pattern card, and return complete unmeasured SQL before calling MCP.
2. **Select database and verify the MCP contract:** follow the runtime contract gate, use only the user-selected database in the `list_databases` allowlist, and call `check_equivalence_preflight(sql,database_name)` for the baseline before measured equivalence work. Require `mcp_contract.performance_tuning=1` before opening a measured case. Read `local_tuning_policy` for the actual candidate, execution, time, and per-request ceilings. If the contract is absent or incompatible, remain in static mode, return concrete unmeasured rewrites, and identify the MCP upgrade gap. Unknown or unselected databases fail closed.
3. **Open case:** call `start_performance_case` with the unchanged baseline SQL and exactly one of the four supported objectives: `elapsed_time`, `cpu`, `logical_reads`, or `physical_reads`, plus at most four named parameter cases. Each case must contain one exact value and declared SQL type for every detected parameter. The same SQL and database must identify the case.
4. **Collect evidence:** call `collect_performance_evidence` with the case id, the same baseline SQL, and a caller-generated idempotency key, normally `execute_query=false`. For an active parameterized sample, set `execute_query=true` only with one exact typed `parameter_case`; otherwise fail closed. On a retryable collection failure, retry `collect_performance_evidence` exactly once with the same request and same idempotency key. Then call `get_performance_case` to retrieve persisted case evidence even when collection reported an error. Continue only when the core benchmark/comparison path works and every missing collector is recorded as an explicit gap; otherwise remain `inconclusive`. Capture availability, collection window, truncation, units, provenance, stable query identity, resource pressure, statistics, waits, and parameter sensitivity.
5. **Start session:** call `start_tuning_session` once. Pass the requested time, candidate, and execution budget only when each is within `local_tuning_policy`; otherwise use or report the exact policy cap. Do not create replacement sessions to evade a budget.
6. **Register and screen:** for each single material change call `add_tuning_candidate` with complete candidate SQL and exactly one strategy: `predicate`, `join`, `aggregation`, `cardinality`, `index`, `combined`, or `rewrite_plus_index`. For `predicate`, `join`, `aggregation`, `cardinality`, and `combined`, call `benchmark_tuning_candidate` with `phase=screening`, the same baseline, candidate, buckets, and `compare_order` matching the contract; the objective remains the one recorded on the case. For `index`, retain the unchanged query SQL, switch to the gated sandbox workflow, and call only `benchmark_index_candidate`; it owns DDL, A-B-A measurements, the lease, and cleanup. A `rewrite_plus_index` child is allowed only after an improved proven finalist or evidence-backed performance-only finalist parent in the same session, must pass `artifact_ref=candidate:<parent-id>`, must use the exact parent rewrite SQL, and runs only through `benchmark_index_candidate` with `phase=finalist`. Preserve unproven parent lineage on the child.
7. **Reconcile:** after each result call `get_tuning_session`; preserve every candidate, returned evidence id, terminal state, metric provenance, plan delta, equivalence result, execution count, and continuation flag.
8. **Finalize:** re-run only credible improved candidates as `phase=finalist`; call `compare_plan_summaries` for plan detail as needed. Call `finalize_tuning_session` with the selected finalist id or `selected_candidate_id=null` and an exact stopping reason. Omit `selection_scope` or pass `selection_scope=proven` by default. Select a terminal performance-only finalist only after explicit user authorization by passing `selection_scope=performance_only`; this never implies equivalence or deployment approval. Finalization must return the complete leaderboard; unresolved candidates become `inconclusive`.

### Profiles and adaptive budget

- `optimizer`: restricted/local read-only posture, `AZURE_SQL_PROFILE=optimizer`, write disabled, benchmark permission required by local database policy. It is for rewrites, evidence, equivalence, and plan comparison.
- `sandbox`: local stdio, unrestricted/apply posture, `AZURE_SQL_TOOL_GROUPS=core,performance,admin`, non-production allowlisted database, and policy permission. It is required for leased temporary indexes and gated view apply/verify/rollback.
- The server defaults are 10 candidate experiments, 3 interleaved screening runs, 5 interleaved finalist runs, up to 4 parameter cases, 80 total measured query executions, and 20 minutes wall-clock. Treat them as defaults only; `check_capabilities.local_tuning_policy` is authoritative.
- `start_tuning_session` accepts `max_candidates`, `execution_limit`, and `time_limit_minutes`, but never widen local policy. Clamp or reject a request that exceeds the returned candidate, execution, time, or per-request ceilings, and report the exact cap. Do not create replacement sessions to evade a budget.
- When the request is simply “find the fastest version”, use the largest useful budget within the returned policy and continue until that budget, evidence, or a written diminishing-return reason ends the search.
- Continue until the approved budget is exhausted, all credible single-change and combined candidates have terminal evidence, or further search has a written evidence-based diminishing-return reason. “Fastest” means the fastest proven-equivalent candidate across the tested parameter buckets and workload objective; never claim a global optimum over untested SQL.
- Rewrite screening defaults to three baseline/candidate pairs and defers full equivalence: 6 executions per bucket, 24 for four buckets. A finalist uses five pairs plus one two-query snapshot comparison: 12 per bucket, 48 for four. Screening one bucket then validating four costs 54; screening all four then validating four costs 72. Use the smallest representative screening subset that can reject a loser and preserve budget for finalists.
- An index A-B-A screen runs baseline, temporary-index, and post-cleanup baseline three times: 9 executions per bucket. A five-run finalist costs 15 per bucket, 60 for four. The configured session execution cap still applies.
- A timeout consumes conservatively what may have run. It is `inconclusive` unless cleanup is unconfirmed. Continue with the next candidate when budget remains.

### Parameter buckets

Use at most four named cases: common, rare, NULL when valid, and a boundary value (`boundary`). Name them `common`, `rare`, `NULL`, and `boundary`; bind real values with their declared SQL types and do not substitute guessed values. Preserve string length, Unicode/non-Unicode type, decimal precision/scale, datetime precision, and collation-sensitive behavior. If a bucket is invalid, unavailable, or truncated, mark that gap and do not call equivalence or improvement proven for it. Reject a candidate with a material regression in any tested bucket even if its aggregate median improves.

### Equivalence standard

For rewrites, `benchmark_tuning_candidate` finalists and `compare_query_results` execute baseline and candidate in one snapshot-consistent comparison. Require complete results within `AZURE_SQL_COMPARISON_ROW_LIMIT`, exact positional shape/type signatures where available, values, NULLs, and duplicate multiplicity. If order is contractual, compare the ordered sequence and tie behavior with `compare_order=true`; otherwise compare duplicate-aware multisets, never sets. Screening may defer this expensive comparison, but an unproven screen cannot win.

An index benchmark runs unchanged SQL across A-B-A phases separated by DDL, so it cannot claim a same-snapshot rewrite proof. It requires complete non-truncated result fingerprints to remain stable before, during, and after the index, plus proof that the candidate plan used the expected index. Data movement between phases makes the index result inconclusive.

A bounded sample, row-limit truncation, different snapshots for a rewrite, unsupported type metadata, duplicate column ambiguity, timeout, or unavailable bucket is inconclusive, never proven equivalent. Any rewrite mismatch is `equivalence_failed` and cannot win.

Run `check_equivalence_preflight(sql,database_name)` for the baseline and each
candidate before finalist comparisons. Queries using
`GETDATE` or another current-time function, `NEWID`, unseeded or
nondeterministically seeded `RAND`, non-repeatable `TABLESAMPLE`, or a row limit
(`TOP`, `OFFSET/FETCH`) whose `ORDER BY` is absent or not backed by a verified
unique total order must be reported as
`classification=proof_contract_required` before
finalists. The same gate applies to an order-sensitive window expression whose
`ORDER BY` lacks a verified unique total order. An ordered `TOP` remains
proof-required until its `ORDER BY` is backed by that proof. A literal
`RAND` seed is allowed. Keep a promising proof-required screening
candidate eligible for the complete finalist performance workload; skip only
the impossible semantic comparison.
Do not use `prove_equivalence=false` as a proof bypass. A performance-only
finalist requires evidence-backed, nonzero executions and measured improvement;
report equivalence as `not proven` and deployment as `not ready`. Select it only
through `finalize_tuning_session(selection_scope=performance_only)`. Without that
explicit finalization scope it cannot win. Deterministic parallel cases are
supporting evidence only and cannot upgrade proof scope. Runtime speed, plan
similarity, result counts, and any other proxy must never be used to overclaim
semantic proof.

### Terminal states

Every measured candidate ends in exactly one of `improved`, `neutral`, `regressed`, `equivalence_failed`, `performance_only`, `inconclusive`, or `cleanup_required`. A timeout is normally `inconclusive`; an unconfirmed index/view cleanup is `cleanup_required`. A slower or failed index rejects only that index candidate. It never erases the paired rewrite or ends the session. `no_change` is a session outcome, not a disguised losing candidate.

## Index and view sandbox gates

### Temporary index

Use only `benchmark_index_candidate`; direct create/drop tools are not live experiment paths. Require `AZURE_SQL_PROFILE=sandbox`, `AZURE_SQL_TOOL_GROUPS=core,performance,admin`, local stdio, unrestricted/apply posture, allowlisted non-production database, policy `allow_benchmark=true` and `allow_test_indexes=true`, active matching session/candidate fingerprints, MCP-generated testing name, exact CREATE/DROP rollback, durable lease, expiry, and ownership metadata. Screening may use an unchanged subset of recorded cases; a finalist must measure every recorded case. MCP must attempt cleanup on success, failure, cancellation, and timeout. A completed idempotent reservation or existing durable lease must be retrieved, not rerun. If cleanup is unconfirmed, report lease id and rollback DDL, set `cleanup_required`, and create no further temporary index until reconciled.

### View definition

Under `optimizer`, use `prepare_view_change` only as a read-only preview and only when `check_capabilities` returns `mcp_contract.durable_view_change=1`. It cannot be handed to a different MCP process. Before any apply, connect to the local gated non-production `sandbox`, verify the same contract there, enable the explicit private-state opt-in `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`, and call `prepare_view_change` again in that sandbox process. Review the returned legality, dependencies, exact prior definition, apply SQL, rollback SQL, and durable change id; policy must also set `allow_view_apply=true`.

Apply only that durable sandbox intent with `apply_prepared_view_change`, then call `verify_view_change`. The permission-restricted MCP state retains the target and exact prior definition for restart recovery; this is the only workflow that deliberately persists raw view SQL, and it requires the explicit opt-in. After an interrupted apply, retrieve and verify the same change id rather than re-preparing against the possibly changed view. Use `rollback_view_change` to restore the exact prior definition, or drop only a workflow-created new view whose target fingerprint still matches. Production deployment is a separate owner-approved handoff, never an implicit consequence of a benchmark.

The current MCP contract does not define a `view` tuning-candidate strategy or
an A-B-A `benchmark_view_candidate` workflow. Therefore a sandbox view change
is a legality, dependency, rollback, and deployment-handoff exercise only. Do not register it as a measured candidate, select it as a winner, or infer query
equivalence or performance from `verify_view_change`. After validation, call
`rollback_view_change` immediately and verify the exact prior definition before
continuing. If rollback cannot be confirmed, report `cleanup_required`, stop
all further sandbox mutations, and retain the durable change id and exact
recovery action. Report the proposed view separately with performance and
equivalence as `not collected`. It can enter winner selection only when MCP
exposes a canonical view benchmark that records baseline, changed-view, and
post-rollback query evidence in the shared session. Until then, view changes
are recommendation-only.

## Leaderboard, winner, and no-change report

Return all of the following, even for static-only or failed work:

1. **Outcome and stopping reason:** deployable winner, static candidate, performance-only outcome, no change, or inconclusive; never overstate confidence.
2. **Semantic contract:** shape, exact supplied SQL types and value-domain
   endpoints, overflow/boundary preconditions, NULLs, duplicates,
   ordering/ties, isolation, parameters, and ambiguities.
3. **Complete SQL:** baseline plus every relevant candidate, with static/measured/finalist/deployable labels; a winner must be complete SQL.
4. **Leaderboard:** candidate id/evidence id, family, exact change, state, phase, buckets, execution count, returned median/spread/objective delta, equivalence, plan/resource deltas, policy status, and cleanup/rollback status. Use `not collected` for missing values.
5. **Rejected experiments:** every slower, neutral, timed-out, unsafe, non-equivalent, policy-blocked, and cleanup-required attempt, with its reason and continuation status.
6. **Deployment handoff:** smallest approved change, owner, prerequisites, verification window, monitoring signals, exact rollback, and whether the result is only a recommendation.
7. **Evidence gaps:** missing plans, unavailable buckets, truncation, unsupported comparisons, shared resource noise, policy limits, and unmeasured claims.

For `no_change`, list every pattern family considered and why it was unsafe, equivalent-but-neutral, regressed, unmeasured, unavailable, or policy-blocked. Report the best static candidate separately from the measured leaderboard; never call it a winner.
