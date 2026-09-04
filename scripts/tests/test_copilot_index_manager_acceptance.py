from __future__ import annotations

import unittest

from scripts import copilot_index_manager_acceptance as acceptance

VALID_TRACE = """Outcome: partial.
Mode: review mode, the default mode. inventory and recheck are also supported.
check_runtime_status returned package_version=2.3.1, runtime_fingerprint=process-1,
runtime_compatibility_fingerprint=compat-1, tool_schema_fingerprint=schema-1,
sanitized_config_fingerprint=config-1. MCP package 2.3.1 or newer is required
separately from the unchanged public contract 2.3.0. list_databases returned one Azure SQL
Database; the user selected that returned database and it is in the configured
allowlist.
The MCP is operator-owned local stdio and configured for the currently signed-in
Entra identity. It contains no fixed user principal name. It uses existing
effective database permissions and does not create or require an additional
database user or role. Review requires SELECT on both history tables. Capture
requires SELECT and INSERT on both history tables. Broader effective permissions,
including dbo, do not fail the contract probe. The restricted profile, database
allowlist, and allow_index_history_write are application-layer controls; they do
not reduce SQL permissions outside MCP. Per-caller Entra delegation for a shared
remote service is out of scope.
check_capabilities returned the public MCP contract version 2.3.0,
mcp_contract.index_portfolio_review=1, exactly the three approved schemas:
capture_index_review_snapshot(database_name, optional idempotency_key),
review_index_portfolio(database_name, optional as_of_run_id, optional
prior_review_id), and get_index_review(database_name, review_id).
mcp_contract.index_history_schema_version=index-history-v1,
mcp_contract.index_history_schema_fingerprint=history-schema-1, and
mcp_contract.index_review_snapshot_reuse_hours=48 were returned. The policy is
restricted with one narrow append-only snapshot write; allow_read=true,
allow_index_history_write=false by default, and the fixed capability value
index_review_min_observation_days=90. It is not a policy key; the policy may
optionally return business_cycle_extension_days.
No caller idempotency key was supplied, so capture omitted it. No new key was
generated after any uncertain response.
The returned run was less than 48 hours old and was reused.
review_index_portfolio(database_name=selected, as_of_run_id=run-index-1)
returned review_id=review-index-1, as_of_run_id=run-index-1,
overall_state=partial, evidence_id=None.
recall_lessons(skill=sql-index-manager, skill_version=1.0.1,
runtime_compatibility_fingerprint=compat-1, tool_schema_fingerprint=schema-1,
sanitized_config_fingerprint=config-1, database_name=selected) was unavailable
because learning is remote-disabled; the advisory fallback is unchanged. Do not
create a local ledger or installed memory.
There is no index evidence bridge and no terminal link. Review, as-of-run, run,
snapshot, subject, and artifact ids are portfolio tracking identifiers, not
artifacts and not learning evidence refs. No V1 portfolio result or human
resolution becomes OutcomeReviewV1. No learning handoff was created.
get_index_review(database_name=selected, review_id=review-index-1) returned the
prior review. A later non-overlapping recheck is available. The returned later
run was less than 48 hours old and was reused.
review_index_portfolio(database_name=selected, as_of_run_id=run-index-2,
prior_review_id=review-index-1) returned review_id=review-index-2,
as_of_run_id=run-index-2, overall_state=no_change, evidence_id=None. Preserve
the returned recheck classification and overall_state as portfolio evidence
only, never as a learning outcome.
When no fresh later run exists, recheck uses the same verified two-gate
separate capture fallback and binds the returned run; otherwise it remains
inconclusive.

Inventory is classification-free: report definitions, protections, usage
epochs, size/write-burden metrics, and coverage only. Inventory omits lifecycle
states, reason_codes, candidate scripts, and recommendations. Raw returned
coverage values are preserved verbatim; incomplete and unknown are not
remapped.
For review and recheck, preserve each exact returned reason_codes array
verbatim; LLM explanation does not replace or override it.

The five portfolio classifications/states are keep, create_candidate,
consolidate_candidate, drop_candidate, and observe. The four overall outcomes
are actionable, no_change, partial, and inconclusive. The deterministic rules
are keep: protection, a valid read delta, or any executed Query Store plan
reference; create_candidate only for the same exact recurring request across at
least two runtime intervals with a material positive existing MCP score,
complete Query Store coverage, no exact or covering index, and projected
storage strictly below 90 percent; missing-index DMV-only evidence remains
observe; consolidate_candidate only for an exact duplicate or strict coverage
after comparing key order/direction, includes, uniqueness, filter, type,
partition/data space, compression, and options, with every proposed removal
independently passing the full drop gate. Exact duplicate returns
overlap_relation=exact_duplicate and
reason_codes=[exact_duplicate_definition]; strict coverage returns
overlap_relation=strict_coverage and
reason_codes=[strict_coverage_overlap]. drop_candidate is returned only for an
enabled user-created nonunique standalone type-2 rowstore with at least 90
continuous usable days plus a business-cycle extension, persisted daily history
with no gap over 48 hours, the same database/engine/counter epoch, stable
definition, counters never decrease, zero seek/scan/lookup deltas, measurable
write or storage cost, complete Query Store/hint/dependency/protection coverage,
and no references/hints/dependencies/protections. A retained unexecuted
stored-plan reference fails no_stored_plan_without_execution, blocks removal
and produces observe. Observe applies to a first-run removal lead,
resets/failovers/gaps/insufficient duration, conflicts, Query Store gaps, and
specialist types. First-run status does not suppress a valid create_candidate.
Post-classification execution-readiness checks do not rewrite the MCP state.
Protected defaults include primary-key,
unique-constraint, clustered, foreign-key-supporting, indexed-view, hinted,
partition-switch-dependent, and automatically managed indexes; they are never
removal candidates and are keep when protected. Standalone unique, filtered,
partitioned, columnstore, disabled, XML, spatial, hash, JSON, memory-optimised,
and hypothetical are specialist types and observe unless stronger keep evidence
exists. Report all coverage and blockers.

The returned artifact filenames are exactly these seven: index-review.json,
index-review.md, create-candidates.sql, consolidation-candidates.sql,
drop-candidates.sql, rollback.sql, and validation.sql. No invented
artifact-reference fields are allowed. These are recommend-only artifacts.
Query validation routes to sql-optimizer, plan control to sql-plan-enforcer,
incidents to sql-health-triage, and production changes to the external human DBA
change control. This is external change control. No index DDL was executed.
Capture is separately policy-gated. A human resolution remains an external
change-control fact, not learning evidence. Never describe removal as safe,
approved, or applied.
"""

VALID_CAPTURE_TRACE = VALID_TRACE.replace(
    "The returned run was less than 48 hours old and was reused.\n"
    "review_index_portfolio(database_name=selected, as_of_run_id=run-index-1)\n"
    "returned review_id=review-index-1, as_of_run_id=run-index-1,",
    "The selected database returned allow_read=true and\n"
    "allow_index_history_write=true; both gates were verified.\n"
    "capture_index_review_snapshot(database_name=selected) returned\n"
    "run_id=run-index-1, snapshot_id=snapshot-index-1.\n"
    "review_index_portfolio(database_name=selected, as_of_run_id=run-index-1)\n"
    "returned review_id=review-index-1, as_of_run_id=run-index-1,",
    1,
)


class CopilotIndexManagerAcceptanceTests(unittest.TestCase):
    def test_review_trace_passes(self) -> None:
        self.assertEqual(acceptance.validate_response(VALID_TRACE), [])

    def test_capture_trace_passes(self) -> None:
        self.assertEqual(acceptance.validate_response(VALID_CAPTURE_TRACE), [])

    def test_unknown_or_disallowed_tool_calls_are_rejected(self) -> None:
        for call in (
            "execute_sql(query=select-1)",
            "I used execute_sql.",
            "Tool: execute_sql with query=select-1.",
            "benchmark_query(query_id=query-1)",
            "run_index_maintenance(database_name=selected)",
            "apply_index_candidate(candidate_id=candidate-1)",
        ):
            with self.subTest(call=call):
                self.assertIn(
                    "only approved index calls",
                    acceptance.validate_response(VALID_TRACE + "\n" + call),
                )

    def test_public_history_contract_fields_are_required(self) -> None:
        for field in (
            "mcp_contract.index_history_schema_version=index-history-v1",
            "mcp_contract.index_history_schema_fingerprint=history-schema-1",
            "mcp_contract.index_review_snapshot_reuse_hours=48",
        ):
            with self.subTest(field=field):
                self.assertIn(
                    "allowlist and contract gate",
                    acceptance.validate_response(VALID_TRACE.replace(field, "missing", 1)),
                )

    def test_mcp_package_gate_is_required(self) -> None:
        bad = VALID_TRACE.replace(
            "package_version=2.3.1",
            "package_version=2.3.0",
            1,
        )

        self.assertIn("MCP 2.3.1 package gate", acceptance.validate_response(bad))

    def test_mcp_package_gate_accepts_newer_package_versions(self) -> None:
        for version in ("2.3.2", "2.4.0", "3.0.0"):
            with self.subTest(version=version):
                trace = VALID_TRACE.replace("package_version=2.3.1", f"package_version={version}", 1)
                self.assertEqual(acceptance.validate_response(trace), [])

    def test_mcp_package_gate_is_bound_to_runtime_status(self) -> None:
        bad = VALID_TRACE.replace(
            "package_version=2.3.1",
            "package_version=2.3.0",
            1,
        ) + "\nA later note mentions package_version=2.3.1."

        self.assertIn("MCP 2.3.1 package gate", acceptance.validate_response(bad))

    def test_current_user_entra_boundary_is_required(self) -> None:
        bad = VALID_TRACE.replace(
            "It contains no fixed user principal name.",
            "It contains a configured identity.",
            1,
        )

        self.assertIn(
            "current-user Entra existing-permission boundary",
            acceptance.validate_response(bad),
        )

    def test_current_user_entra_boundary_rejects_negated_claims(self) -> None:
        bad = VALID_TRACE.replace(
            "The MCP is operator-owned local stdio and configured for the currently signed-in\n"
            "Entra identity.",
            "The MCP is not operator-owned local stdio and is not configured for the currently signed-in\n"
            "Entra identity.",
            1,
        )

        self.assertIn(
            "current-user Entra existing-permission boundary",
            acceptance.validate_response(bad),
        )

    def test_stale_reused_run_is_rejected(self) -> None:
        bad = VALID_TRACE.replace(
            "The returned run was less than 48 hours old and was reused.",
            "The returned run was 48 hours old and was reused.",
            1,
        )
        self.assertIn("snapshot freshness gate", acceptance.validate_response(bad))

    def test_recheck_requires_freshness_and_capture_fallback(self) -> None:
        cases = (
            VALID_TRACE.replace(
                "The returned later\nrun was less than 48 hours old and was reused.",
                "The returned later run was stale.",
                1,
            ),
            VALID_TRACE.replace(
                "When no fresh later run exists, recheck uses the same verified two-gate\n"
                "separate capture fallback and binds the returned run; otherwise it remains\n"
                "inconclusive.",
                "No recheck fallback was described.",
                1,
            ),
        )
        for bad in cases:
            with self.subTest(bad=bad):
                self.assertIn(
                    "recheck freshness and capture fallback",
                    acceptance.validate_response(bad),
                )

    def test_recall_before_runtime_gate_is_rejected(self) -> None:
        bad = "recall_lessons(skill=sql-index-manager)\n" + VALID_TRACE
        self.assertIn(
            "runtime/database gate before learning",
            acceptance.validate_response(bad),
        )

    def test_each_forbidden_learning_or_handoff_call_is_rejected(self) -> None:
        for tool in acceptance._FORBIDDEN_LEARNING_CALLS:
            with self.subTest(tool=tool):
                bad = VALID_TRACE + f"\n{tool}(subject=index)."
                self.assertIn(
                    "recall-only V1 learning boundary",
                    acceptance.validate_response(bad),
                )

    def test_affirmative_learning_claims_are_rejected(self) -> None:
        for claim in (
            "I called record_decision.",
            "record_decision succeeded.",
            "A learning handoff was created.",
            "A learning handoff exists.",
            "The human resolution became OutcomeReviewV1.",
            "The human resolution was converted to OutcomeReviewV1.",
        ):
            with self.subTest(claim=claim):
                self.assertIn(
                    "recall-only V1 learning boundary",
                    acceptance.validate_response(VALID_TRACE + "\n" + claim),
                )

    def test_non_null_or_invented_learning_evidence_is_rejected(self) -> None:
        cases = (
            VALID_TRACE.replace("evidence_id=None", "evidence_id=evidence-1", 1),
            VALID_TRACE + "\nevidence_ref=evidence-1.",
            VALID_TRACE + "\nterminal_link_id=terminal-1.",
            VALID_TRACE + "\nconsumed_evidence_refs=[review-index-1].",
        )
        for bad in cases:
            with self.subTest(bad=bad):
                self.assertIn(
                    "recall-only V1 learning boundary",
                    acceptance.validate_response(bad),
                )

    def test_real_review_fields_are_required(self) -> None:
        for field in ("review_id", "as_of_run_id", "overall_state", "evidence_id"):
            with self.subTest(field=field):
                marker = {
                    "review_id": "returned review_id=review-index-1",
                    "as_of_run_id": (
                        "returned review_id=review-index-1, "
                        "as_of_run_id=run-index-1"
                    ),
                    "overall_state": "overall_state=partial",
                    "evidence_id": "evidence_id=None",
                }[field]
                bad = VALID_TRACE.replace(
                    marker,
                    marker.replace(f"{field}=", f"missing_{field}="),
                    1,
                )
                self.assertIn(
                    "real V1 review fields",
                    acceptance.validate_response(bad),
                )

    def test_recheck_requires_returned_review_lineage(self) -> None:
        bad = VALID_TRACE.replace(
            "prior_review_id=review-index-1",
            "prior_review_id=review-other",
            1,
        )
        self.assertIn("recheck uses prior review", acceptance.validate_response(bad))

    def test_recheck_requires_a_later_as_of_run(self) -> None:
        bad = VALID_TRACE.replace(
            "as_of_run_id=run-index-2,\nprior_review_id=review-index-1",
            "prior_review_id=review-index-1",
            1,
        )
        self.assertIn("recheck uses prior review", acceptance.validate_response(bad))

    def test_recheck_classification_does_not_require_terminal_learning(self) -> None:
        self.assertEqual(acceptance.validate_response(VALID_TRACE), [])
        self.assertNotIn("terminal_link_id=", VALID_TRACE)
        self.assertNotIn("review_decision(", VALID_TRACE)

    def test_each_portfolio_tool_rejects_extra_arguments(self) -> None:
        bad_calls = (
            "capture_index_review_snapshot(database_name=selected, bogus=value)",
            "review_index_portfolio(database_name=selected, bogus=value)",
            "get_index_review(database_name=selected, review_id=review-index-1, bogus=value)",
        )
        for bad_call in bad_calls:
            with self.subTest(bad_call=bad_call):
                self.assertIn(
                    "only approved index calls",
                    acceptance.validate_response(VALID_TRACE + "\n" + bad_call),
                )

    def test_each_portfolio_tool_requires_database_name(self) -> None:
        bad_calls = (
            "capture_index_review_snapshot(idempotency_key=key-1)",
            "review_index_portfolio(prior_review_id=review-index-1)",
            "get_index_review(review_id=review-index-1)",
        )
        for bad_call in bad_calls:
            with self.subTest(bad_call=bad_call):
                self.assertIn(
                    "only approved index calls",
                    acceptance.validate_response(VALID_TRACE + "\n" + bad_call),
                )

    def test_each_portfolio_tool_rejects_decision_id(self) -> None:
        bad_calls = (
            "capture_index_review_snapshot(database_name=selected, decision_id=decision-1)",
            "review_index_portfolio(database_name=selected, decision_id=decision-1)",
            "get_index_review(database_name=selected, review_id=review-index-1, decision_id=decision-1)",
        )
        for bad_call in bad_calls:
            with self.subTest(bad_call=bad_call):
                self.assertIn(
                    "only approved index calls",
                    acceptance.validate_response(VALID_TRACE + "\n" + bad_call),
                )

    def test_capture_without_policy_gate_is_rejected(self) -> None:
        bad = VALID_TRACE + (
            "\ncapture_index_review_snapshot(database_name=selected) returned "
            "run_id=run-capture-1. "
            "review_index_portfolio(database_name=selected, "
            "as_of_run_id=run-capture-1) returned review_id=review-capture-1, "
            "as_of_run_id=run-capture-1, overall_state=partial, evidence_id=None."
        )
        self.assertIn("capture policy gate", acceptance.validate_response(bad))

    def test_capture_must_bind_the_next_review_to_the_returned_run(self) -> None:
        bad = VALID_CAPTURE_TRACE.replace(
            "review_index_portfolio(database_name=selected, as_of_run_id=run-index-1)",
            "review_index_portfolio(database_name=selected, as_of_run_id=run-other)",
            1,
        )
        self.assertIn("capture policy gate", acceptance.validate_response(bad))

    def test_capture_rejects_hypothetical_or_negated_policy_gates(self) -> None:
        marker = (
            "The selected database returned allow_read=true and\n"
            "allow_index_history_write=true; both gates were verified."
        )
        replacements = (
            (
                "Hypothetically, the selected database returned allow_read=true and\n"
                "allow_index_history_write=true; both gates were verified."
            ),
            (
                "The selected database did not return allow_read=true and\n"
                "allow_index_history_write=true; both gates were not verified."
            ),
            "Policy was not verified.\n" + marker,
        )
        for replacement in replacements:
            with self.subTest(replacement=replacement):
                bad = VALID_CAPTURE_TRACE.replace(marker, replacement, 1)
                self.assertIn("capture policy gate", acceptance.validate_response(bad))

    def test_capture_rejects_invented_idempotency_keys(self) -> None:
        bad = VALID_CAPTURE_TRACE.replace(
            "capture_index_review_snapshot(database_name=selected)",
            "capture_index_review_snapshot(database_name=selected, "
            "idempotency_key=made-up-key)",
            1,
        )
        self.assertIn("idempotency key provenance", acceptance.validate_response(bad))

    def test_uncertain_capture_cannot_retry_with_a_new_key(self) -> None:
        bad = VALID_CAPTURE_TRACE + (
            "\nThe first capture was uncertain. "
            "capture_index_review_snapshot(database_name=selected, "
            "idempotency_key=new-key) was retried."
        )
        self.assertIn("idempotency key provenance", acceptance.validate_response(bad))

    def test_process_fingerprint_is_not_a_recall_argument(self) -> None:
        bad = VALID_TRACE.replace(
            "recall_lessons(skill=sql-index-manager, skill_version=1.0.1,\n"
            "runtime_compatibility_fingerprint=compat-1,",
            "recall_lessons(skill=sql-index-manager, skill_version=1.0.1,\n"
            "runtime_fingerprint=process-1, "
            "runtime_compatibility_fingerprint=compat-1,",
            1,
        )
        self.assertIn("exact recall schema", acceptance.validate_response(bad))

    def test_invented_artifact_fields_or_files_are_rejected(self) -> None:
        cases = (
            VALID_TRACE + "\nprior_state_ref=state-1.",
            VALID_TRACE + "\nartifact_ref=review-index-1.",
            VALID_TRACE + "\nnotes.md was also returned.",
        )
        for bad in cases:
            with self.subTest(bad=bad):
                self.assertIn(
                    "exact returned artifact filenames",
                    acceptance.validate_response(bad),
                )

    def test_create_candidate_requires_every_positive_gate(self) -> None:
        replacements = {
            "same exact recurring request": "recurring request",
            "material positive existing MCP score,": "existing MCP score,",
            "complete Query Store coverage,": "partial Query Store coverage,",
            "no exact or covering index,": "no exact index,",
            "projected\nstorage strictly below 90 percent": "projected storage at 90 percent",
        }
        for old, new in replacements.items():
            with self.subTest(gate=old):
                bad = VALID_TRACE.replace(old, new, 1)
                self.assertIn(
                    "portfolio gates and overlap",
                    acceptance.validate_response(bad),
                )

    def test_exact_reason_codes_and_inventory_contract_are_required(self) -> None:
        cases = (
            (
                VALID_TRACE.replace(
                    "preserve each exact returned reason_codes array\nverbatim",
                    "summarise returned reasons",
                    1,
                ),
                "exact returned reason codes",
            ),
            (
                VALID_TRACE.replace(
                    "Inventory is classification-free",
                    "Inventory includes classifications",
                    1,
                ),
                "inventory is classification-free",
            ),
            (
                VALID_TRACE.replace(
                    "incomplete and unknown are not\nremapped",
                    "coverage is normalised",
                    1,
                ),
                "raw coverage vocabulary",
            ),
        )
        for bad, requirement in cases:
            with self.subTest(requirement=requirement):
                self.assertIn(requirement, acceptance.validate_response(bad))

    def test_safe_to_drop_wording_is_rejected(self) -> None:
        bad = (
            VALID_TRACE
            + " The index is "
            + " ".join(("safe", "to", "drop"))  # noqa: FLY002
            + "."
        )
        self.assertIn("recommend-only rollback", acceptance.validate_response(bad))

    def test_executable_index_ddl_is_rejected(self) -> None:
        for statement in (
            "CREATE INDEX IX_bad ON dbo.T (Id);",
            "DROP INDEX IX_bad ON dbo.T;",
            "ALTER INDEX IX_bad ON dbo.T REBUILD;",
        ):
            with self.subTest(statement=statement):
                self.assertIn(
                    "no executable index DDL or Database Watcher claim",
                    acceptance.validate_response(VALID_TRACE + "\n" + statement),
                )

    def test_commented_inert_ddl_example_is_allowed(self) -> None:
        safe = VALID_TRACE + (
            "\nThe create-candidates.sql proposal contains a commented, inert "
            "CREATE INDEX example; it was not executed."
        )
        self.assertEqual(acceptance.validate_response(safe), [])

    def test_database_watcher_integration_claim_is_rejected(self) -> None:
        bad = VALID_TRACE + (
            "\nDatabase Watcher integration supplied the collected evidence."
        )
        self.assertIn(
            "no executable index DDL or Database Watcher claim",
            acceptance.validate_response(bad),
        )

    def test_affirmative_change_or_watcher_claims_are_rejected(self) -> None:
        for claim in (
            "drop_candidate is approved.",
            "drop_candidate is authorised.",
            "Index removal was applied.",
            "The index was removed.",
            "Index DDL was executed.",
            "Database Watcher provided evidence.",
            "Database Watcher furnished evidence.",
        ):
            with self.subTest(claim=claim):
                self.assertIn(
                    "no executable index DDL or Database Watcher claim",
                    acceptance.validate_response(VALID_TRACE + "\n" + claim),
                )

    def test_later_inventory_or_reason_code_contradictions_are_rejected(self) -> None:
        for claim in (
            "LLM overrides reason_codes.",
            "Inventory includes lifecycle states and candidate recommendations.",
        ):
            with self.subTest(claim=claim):
                self.assertIn(
                    "no inventory or reason-code contradiction",
                    acceptance.validate_response(VALID_TRACE + "\n" + claim),
                )

    def test_retired_portfolio_names_are_rejected(self) -> None:
        for parts in (
            ("get_index_portfolio_", "snapshot"),
            ("capture_index_portfolio_", "snapshot"),
            ("classify_index_", "portfolio"),
            ("recheck_index_", "portfolio"),
        ):
            bad = VALID_TRACE + " " + "".join(parts)
            self.assertIn(
                "no retired portfolio surface",
                acceptance.validate_response(bad),
            )


if __name__ == "__main__":
    unittest.main()
