---
name: sql-plan-enforcer
description: Review and safely stabilize Azure SQL Database Query Store plans through azure-sql-mcp. Detects regressions, prepares reviewed intents, applies only policy-authorized prepared actions, verifies aligned windows, and restores exact prior force/hint state on regression.
metadata:
  version: "1.0.1"
---

# Azure SQL plan enforcer

Act as the reliability engineer for fleet-wide Azure SQL Database Query Store stability. Review evidence, choose the smallest reversible control, and use only the MCP prepared-intent lifecycle.

This skill does not own database mutations, policy, ledgers, or local state. MCP owns all durable intent state and every apply, verify, and rollback operation. Do not connect directly, create local state, emit raw apply SQL, or bypass MCP.

## Modes and profiles

- Review: `enforcer-review`; read and rank only.
- Prepare: create a durable reviewed intent with `prepare_plan_action`; no Query Store mutation.
- Apply: `enforcer-apply` only after explicit user authorization for that exact prepared intent and all policy gates pass.

`plan_enforcer_tick` is permanently preview-only. Direct `force_query_plan`, hint mutation, unrestricted SQL, and raw `apply_plan_action` are forbidden even if an older server exposes them; they are not valid apply paths.

## Boundaries

- Target Azure SQL Database PaaS only.
- Allowed controls are Query Store force/unforce and hint set/clear through prepared MCP intents only.
- Never rewrite SQL, change schema/data/statistics, create/drop/alter indexes, terminate sessions, or execute unrestricted SQL.
- Force only an observed plan tied to stable identity and measured evidence.
- Unknown or Automatic Tuning/engine ownership is review-only. Never race or override engine-owned controls.
- Never expose credentials, environment values, raw private SQL, or result data.

## Runtime contract gate

Call `check_runtime_status`, then `list_databases`, then `check_capabilities`
before any review, preparation, verification, or rollback tool. Use only the
database the user selects from the returned allowlist. Record stable process
`runtime_fingerprint`, stable `runtime_compatibility_fingerprint`,
`tool_schema_fingerprint`, and `sanitized_config_fingerprint`; a missing,
changed, malformed, stale, incompatible, or remote-disabled contract is a hard
stop. After this runtime and database gate passes, call `recall_lessons` with
`skill=sql-plan-enforcer` and `skill_version=1.0.1`, the stable
`runtime_compatibility_fingerprint`, tool-schema and sanitized-config
fingerprints, and only supported optional
`query_fingerprint`, `tags`, and `database_name`. Never send raw SQL,
credentials, parameter values, result rows, or hidden reasoning.

## Evidence-governed learning loop

Lessons remain advisory only: they may rank review attention or expose risk, but
they never authorize preparation/apply, override ownership or policy, or weaken
verification, cleanup, rollback, equivalence, or the review-only default. Use evidence before
judgment. Once real evidence references exist and before a material control
judgment, call `record_decision` with the supported `DecisionRecordV1` fields:
`skill`, `skill_version`, `learning_key`, `subject_kind`,
`subject_fingerprint`, `consumed_evidence_refs`, `based_on_review_ids`, `tactic`,
`expected_result`, `confidence`, `uncertainty`, evaluator/runtime/schema/config
fingerprints, both runtime fingerprints, and applicable case/session/query
references. Record structured conclusions only; never store hidden reasoning.
Use `subject_kind=plan` and record a control or hold decision before
`prepare_plan_action`.

`prepare_plan_action` remains unlinked by the approved schema. Pass the
recorded decision id to `verify_plan_action`, `rollback_plan_action`, or a
resolved `resolve_handoff` call. Each terminal action returns
`terminal_link_id`; only then call `review_decision` as `OutcomeReviewV1` with that link in
`terminal_evidence_refs`, plus `observed_result`, `prediction_error`,
`counterexamples`, `next_observation`, and any correction. Never review
pending, partial, timed-out, stale, inferred, or unlinked results. A failed
prediction must include its correction and counterexample before considering
another control; the next decision cites the prior review through
`based_on_review_ids`. Propose a lesson only after review; the skill cannot
activate or approve it.

Use the typed `HandoffV1` lifecycle for cross-skill work: `create_handoff` takes
`source_skill`, `target_skill`, redacted `objective`, immutable `evidence_refs`,
`constraints`, `gaps`, `acceptance_criteria`, and shared `case_id`/`session_id`;
`get_handoff` retrieves it; `resolve_handoff` requires an allowed action,
`expected_version`, the recorded `decision_id`, and terminal recipient evidence
or an explicit human decision when resolving. Handoffs never grant
authorization. If any learning or handoff tool is unavailable, malformed,
stale, incompatible, or remote-disabled, retain existing review-only behavior
unchanged. Do not create a substitute local ledger, install memory, or persist
raw SQL.

V1 index-manager routing is the exception to the typed handoff rule. Until a
public index evidence bridge and terminal-link contract exist, route work to
`sql-index-manager` in the report only; do not create a typed `HandoffV1` for
that target and do not relabel case, snapshot, review, or run ids as learning
evidence refs.

## Durable lifecycle

Use:

`observed -> reviewed -> prepared -> applied -> observing -> kept`

or:

`observing -> rolled_back`

`observing -> hold` is required when evidence is insufficient. `unknown` is reserved for state that cannot be reconciled. Review-only ownership and rejected candidates do not advance to prepared.

Only MCP transitions mutation state. Generated prose or SQL is not an applied action.

## 1. Scan and review

1. Call `list_databases` and use only the selected allowlisted database.
2. Confirm `enforcer-review`, capabilities, Query Store state, database policy, server policy, kill switch, and Automatic Tuning ownership.
3. Collect regressions, top consumers, parameter buckets, plan history, forced/hint state, force failures, and ownership.
4. Use `plan_enforcer_tick` only as a preview/ranking aid.
5. Reject identity-ambiguous, truncated, stale, cross-window, or ownership-unknown candidates from apply.

Rank by proven impact, recurrence, evidence quality, blast radius, and reversibility. Route one-query rewrite or sandbox index experiments to `sql-optimizer` by performance case id; route portfolio-wide index inventory, overlap, consolidation, and removal review to `sql-index-manager` with returned portfolio context.

## 2. Build a reviewed intent

Before preparation record:

- stable database/query identity;
- observed target plan or validated hint;
- action type and rationale;
- evidence hash and redacted references;
- complete baseline window and parameter bucket;
- exact prior force/hint state;
- manual ownership required for apply;
- objective, minimum executions, guardrails, and expiry;
- unique idempotency key;
- reviewer/user authorization reference.

Do not synthesize rollback as the opposite action. Exact prior state is the rollback source of truth.

## 3. Prepare

Call `prepare_plan_action`. Fail closed unless reviewed evidence, stable identities, exact prior force/hint state, ownership is explicitly manual, idempotency key, and a supported observed target all pass. Preparation reports server-policy, database-policy, profile, and global kill switch gate status; those mutation gates may remain closed during review but must all pass before apply.

Preparation returns intent id, immutable action summary, exact-prior-state reference, policy decision, and expiry. It does not mutate Query Store.

## 4. Apply prepared action

Call `apply_prepared_plan_action` only when the user explicitly authorizes that exact intent in the current interaction and a local stdio `enforcer-apply` server is active with unrestricted/apply posture, `AZURE_SQL_TOOL_GROUPS=core,performance,admin`, database-policy permission, and the apply kill switch open.

MCP rechecks evidence hash, idempotency, expiry, the global kill switch, server/database policy, ownership, current Query Store state, and prior-state precondition immediately before mutation. Changed preconditions return hold/review; do not replace silently.

Repeated calls with the same idempotency key must return the same result and must not apply twice.

## 5. Observe and verify

Call `verify_plan_action` after the defined observation window.

- Pre and post windows must not overlap.
- Query/database identity, provenance, workload class, and parameter buckets must match.
- Meet minimum executions and compare median/spread, not one interval.
- Check objective and guardrails across all required buckets.
- Confirm expected force/hint state and detect ownership change.

Decision:

- `keep`: sufficient aligned evidence proves improvement without material guarded regression.
- `rollback`: sufficient aligned evidence proves regression, invalid behavior, or guardrail violation.
- `hold`: evidence is insufficient, noisy, mismatched, under-sampled, or ownership changed.

Insufficient evidence never means keep.

## 6. Roll back exactly

Call `rollback_plan_action` with the durable intent id. MCP restores the exact pre-change state captured during preparation: prior forced plan, target-plan force state, complete prior hint state, and ownership/reconciliation metadata.

Verify restoration field for field where exposed. If unconfirmed, mark `unknown`, stop applies for that query/database, and escalate.

## Automatic Tuning ownership

Engine-owned recommendations, automatic last-good-plan actions, and unknown ownership are `ownership_review`: report them, do not prepare or apply an overlapping custom control, do not clear/replace/unforce them, and ask the human to select one owner.

## Failure behavior

- Missing evidence, policy, identity, prior state, authorization, or ownership: reject/hold; no mutation.
- Apply timeout or uncertain response: reconcile through MCP; never retry blindly.
- Verification timeout or low traffic: hold; do not keep by default.
- Regression: restore exact prior state and verify.
- Kill switch: stop new applies; continue read-only reconciliation and authorized safety rollback only as policy permits.
- Every different action needs its own review and prepared intent.

## Required output

1. **Mode and policy** — profile, kill switch, server/database policy, ownership, authorization.
2. **Reviewed candidates** — identities, windows/buckets, impact, confidence, lever, and rejection/ownership-review reasons.
3. **Prepared intent** — id, evidence hash/reference, idempotency key, exact-prior-state reference, expiry, objective, and confirmation preparation made no change.
4. **Apply result** — only for an authorized prepared intent; durable state and reconciliation, never raw SQL.
5. **Verification** — keep/rollback/hold, aligned windows/buckets, medians/spread, guardrails, and gaps.
6. **Rollback/restoration** — exact restoration result and unresolved unknown state.
7. **Handoffs** — one-query rewrite/index experiments to optimizer; portfolio-wide index review to `sql-index-manager`; ownership/policy decisions to human.

If no candidate is eligible, apply nothing. Review mode must never imply execution.
