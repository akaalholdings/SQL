from __future__ import annotations

from scripts import (
    copilot_health_triage_acceptance,
    copilot_index_manager_acceptance,
    copilot_optimizer_acceptance,
    copilot_plan_enforcer_acceptance,
)
from scripts.tests.test_copilot_index_manager_acceptance import (
    VALID_TRACE as INDEX_VALID_TRACE,
)

LEARNING_TRACE = """Mode: review-only; no authorization.
Outcome: partial.
check_runtime_status -> runtime_fingerprint=process-1,
runtime_compatibility_fingerprint=compat-1, tool_schema_fingerprint=schema-1,
sanitized_config_fingerprint=config-1.
list_databases -> the user-selected database; check_capabilities -> the returned
read-only capability envelope.
recall_lessons(skill=sql-health-triage, skill_version=1.0.1,
runtime_compatibility_fingerprint=compat-1,
tool_schema_fingerprint=schema-1, sanitized_config_fingerprint=config-1,
database_name=selected) was unavailable because learning is remote-disabled;
fallback to the existing static rewrite-first/read-only/review-only behavior is
unchanged.
recall_lessons(skill=sql-optimizer, skill_version=2.3.1,
runtime_compatibility_fingerprint=compat-1,
tool_schema_fingerprint=schema-1, sanitized_config_fingerprint=config-1,
database_name=selected) was unavailable because learning is remote-disabled;
recall_lessons(skill=sql-plan-enforcer, skill_version=1.0.1,
runtime_compatibility_fingerprint=compat-1,
tool_schema_fingerprint=schema-1, sanitized_config_fingerprint=config-1,
database_name=selected) was unavailable because learning is remote-disabled.
Evidence reference: evidence-health-1 and terminal-plan-review-1.
DecisionRecordV1: record_decision(skill=sql-health-triage, skill_version=1.0.1,
subject_kind=database,
subject_fingerprint=subject-health, based_on_review_ids=[],
runtime_fingerprint=process-1, runtime_compatibility_fingerprint=compat-1)
-> decision_id=decision-health-1.
collect_performance_evidence(case_id=case-health, decision_id=decision-health-1)
-> terminal_link_id=terminal-health-1 (terminal evidence).
review_decision(decision_id=decision-health-1,
terminal_evidence_refs=[terminal-health-1], OutcomeReviewV1=correction-health,
counterexamples=[], next_observation=observe-health) followed
only the returned terminal_link_id; correction and counterexample precede the
next hypothesis.
Next health hypothesis: record_decision(skill=sql-health-triage,
skill_version=1.0.1, subject_kind=database,
subject_fingerprint=subject-health-next,
based_on_review_ids=[review-health-1], runtime_fingerprint=process-1,
runtime_compatibility_fingerprint=compat-1) -> decision_id=decision-health-2.
record_decision(skill=sql-optimizer, skill_version=2.3.1, subject_kind=query,
subject_fingerprint=subject-optimizer, based_on_review_ids=[],
runtime_fingerprint=process-1,
runtime_compatibility_fingerprint=compat-1) -> decision_id=decision-opt-1.
benchmark_tuning_candidate(session_id=session-1, candidate_id=candidate-1,
decision_id=decision-opt-1) -> terminal_link_id=terminal-opt-1.
review_decision(decision_id=decision-opt-1,
terminal_evidence_refs=[terminal-opt-1], OutcomeReviewV1=correction-opt,
counterexamples=[counterexample-opt], next_observation=observe-opt); correction,
counterexample, then next candidate.
record_decision(skill=sql-optimizer, skill_version=2.3.1, subject_kind=query,
subject_fingerprint=subject-index,
based_on_review_ids=[review-opt-1], runtime_fingerprint=process-1,
runtime_compatibility_fingerprint=compat-1) -> decision_id=decision-opt-2.
benchmark_index_candidate(session_id=session-1, candidate_id=index-1,
decision_id=decision-opt-2) -> terminal_link_id=terminal-opt-2.
review_decision(decision_id=decision-opt-2,
terminal_evidence_refs=[terminal-opt-2], counterexamples=[],
next_observation=observe-index, OutcomeReviewV1=correction-index).
record_decision(skill=sql-optimizer, skill_version=2.3.1, subject_kind=query,
subject_fingerprint=subject-final,
based_on_review_ids=[review-opt-2], runtime_fingerprint=process-1,
runtime_compatibility_fingerprint=compat-1) -> decision_id=decision-opt-3.
finalize_tuning_session(session_id=session-1, decision_id=decision-opt-3)
-> terminal_link_id=terminal-opt-3.
review_decision(decision_id=decision-opt-3,
terminal_evidence_refs=[terminal-opt-3], counterexamples=[],
next_observation=observe-final, OutcomeReviewV1=correction-final).
HandoffV1: create_handoff(source_skill=sql-optimizer,
target_skill=sql-plan-enforcer, evidence_refs=[terminal-opt-3],
acceptance_criteria=[terminal evidence]); get_handoff.
Plan control: record_decision(skill=sql-plan-enforcer, skill_version=1.0.1,
subject_kind=plan, subject_fingerprint=subject-plan, based_on_review_ids=[],
runtime_fingerprint=process-1, runtime_compatibility_fingerprint=compat-1)
-> decision_id=decision-plan-1 before prepare_plan_action.
prepare_plan_action(...) remains unlinked by the approved schema.
verify_plan_action(intent_id=intent-1, decision_id=decision-plan-1)
-> terminal_link_id=terminal-plan-1 (terminal evidence).
rollback_plan_action(intent_id=intent-1, decision_id=decision-plan-1)
-> terminal_link_id=terminal-plan-2.
resolve_handoff(handoff_id=handoff-1, action=claim, owner=sql-plan-enforcer,
expected_version=0).
resolve_handoff(handoff_id=handoff-1, action=resolve, expected_version=1,
decision_id=decision-plan-1) -> terminal_link_id=terminal-handoff-1.
review_decision(decision_id=decision-plan-1,
terminal_evidence_refs=[terminal-plan-1], OutcomeReviewV1=correction-plan,
counterexamples=[counterexample-plan], next_observation=observe-plan).
This was advisory, carries no authorization, and
cannot activate lessons. Do not install memory and do not create a local ledger.
"""


def test_all_four_acceptance_validators_accept_their_ordered_traces() -> None:
    assert copilot_health_triage_acceptance.validate_response(LEARNING_TRACE) == []
    assert copilot_optimizer_acceptance.validate_learning_loop_response(LEARNING_TRACE) == []
    assert copilot_plan_enforcer_acceptance.validate_response(LEARNING_TRACE) == []


def test_index_manager_acceptance_validator_accepts_its_ordered_trace() -> None:
    assert copilot_index_manager_acceptance.validate_response(
        INDEX_VALID_TRACE
    ) == []


def test_index_canary_rejects_a_decision_id_on_a_portfolio_call() -> None:
    bad = INDEX_VALID_TRACE.replace(
        "review_index_portfolio(database_name=selected, as_of_run_id=run-index-2,\n"
        "prior_review_id=review-index-1)",
        "review_index_portfolio(database_name=selected, as_of_run_id=run-index-2,\n"
        "prior_review_id=review-index-1, decision_id=decision-index-1)",
        1,
    )
    assert "only approved index calls" in copilot_index_manager_acceptance.validate_response(bad)


def test_index_canary_rejects_extra_arguments_on_each_portfolio_tool() -> None:
    bad_calls = (
        "capture_index_review_snapshot(database_name=selected, bogus=value)",
        "review_index_portfolio(database_name=selected, bogus=value)",
        "get_index_review(database_name=selected, review_id=review-index-1, bogus=value)",
    )
    for bad_call in bad_calls:
        assert "only approved index calls" in (
            copilot_index_manager_acceptance.validate_response(
                INDEX_VALID_TRACE + "\n" + bad_call
            )
        )


def test_index_canary_rejects_missing_database_name_on_each_portfolio_tool() -> None:
    bad_calls = (
        "capture_index_review_snapshot(idempotency_key=key-1)",
        "review_index_portfolio(prior_review_id=review-index-1)",
        "get_index_review(review_id=review-index-1)",
    )
    for bad_call in bad_calls:
        assert "only approved index calls" in (
            copilot_index_manager_acceptance.validate_response(
                INDEX_VALID_TRACE + "\n" + bad_call
            )
        )


def test_index_canary_rejects_decision_id_on_each_portfolio_tool() -> None:
    bad_calls = (
        "capture_index_review_snapshot(database_name=selected, decision_id=decision-index-1)",
        "review_index_portfolio(database_name=selected, decision_id=decision-index-1)",
        "get_index_review(database_name=selected, review_id=review-index-1, decision_id=decision-index-1)",
    )
    for bad_call in bad_calls:
        assert "only approved index calls" in (
            copilot_index_manager_acceptance.validate_response(
                INDEX_VALID_TRACE + "\n" + bad_call
            )
        )


def test_index_canary_requires_review_id_for_review_retrieval() -> None:
    bad = INDEX_VALID_TRACE + "\nget_index_review(database_name=selected)"
    assert "only approved index calls" in (
        copilot_index_manager_acceptance.validate_response(bad)
    )


def test_index_canary_rejects_an_invented_terminal_link() -> None:
    bad = INDEX_VALID_TRACE + "\nterminal_link_id=terminal-index-1."
    assert "recall-only V1 learning boundary" in (
        copilot_index_manager_acceptance.validate_response(bad)
    )


def test_index_canary_rejects_every_learning_write_or_handoff_call() -> None:
    for tool in copilot_index_manager_acceptance._FORBIDDEN_LEARNING_CALLS:
        bad = INDEX_VALID_TRACE + f"\n{tool}(subject=index)."
        assert "recall-only V1 learning boundary" in (
            copilot_index_manager_acceptance.validate_response(bad)
        )


def test_index_canary_rejects_non_null_learning_evidence() -> None:
    bad = INDEX_VALID_TRACE.replace("evidence_id=None", "evidence_id=evidence-1", 1)
    assert "recall-only V1 learning boundary" in (
        copilot_index_manager_acceptance.validate_response(bad)
    )


def test_canaries_reject_recall_before_database_gate() -> None:
    bad = LEARNING_TRACE.replace(
        "check_runtime_status -> runtime_fingerprint=process-1,\n"
        "runtime_compatibility_fingerprint=compat-1, tool_schema_fingerprint=schema-1,\n"
        "sanitized_config_fingerprint=config-1.\n"
        "list_databases -> the user-selected database; check_capabilities -> the returned\n"
        "read-only capability envelope.\n"
        "recall_lessons",
        "recall_lessons",
        1,
    )
    assert "runtime/database gate before lesson recall" in (
        copilot_optimizer_acceptance.validate_learning_loop_response(bad)
    )
    assert "runtime/database gate before lesson recall" in (
        copilot_health_triage_acceptance.validate_response(bad)
    )


def test_canaries_reject_process_fingerprint_in_recall() -> None:
    bad = LEARNING_TRACE.replace(
        "recall_lessons(skill=sql-optimizer, skill_version=2.3.1,\n"
        "runtime_compatibility_fingerprint=compat-1,",
        "recall_lessons(skill=sql-optimizer, skill_version=2.3.1,\n"
        "runtime_fingerprint=process-1, "
        "runtime_compatibility_fingerprint=compat-1,",
        1,
    )
    assert "exact recall schema" in (
        copilot_optimizer_acceptance.validate_learning_loop_response(bad)
    )


def test_canaries_reject_review_before_terminal_evidence() -> None:
    bad = LEARNING_TRACE.replace(
        "collect_performance_evidence(case_id=case-health, decision_id=decision-health-1)\n"
        "-> terminal_link_id=terminal-health-1 (terminal evidence).\n"
        "review_decision(decision_id=decision-health-1,",
        "review_decision(decision_id=decision-health-1,",
        1,
    )
    assert "decision contract and review order" in (
        copilot_optimizer_acceptance.validate_learning_loop_response(bad)
    )
    assert "decision contract and terminal-only review" in (
        copilot_health_triage_acceptance.validate_response(bad)
    )


def test_canaries_reject_a_missing_health_decision_link() -> None:
    bad = LEARNING_TRACE.replace(
        "collect_performance_evidence(case_id=case-health, decision_id=decision-health-1)",
        "collect_performance_evidence(case_id=case-health)",
        1,
    )
    assert "health terminal decision link" in (
        copilot_health_triage_acceptance.validate_response(bad)
    )


def test_canaries_reject_a_missing_optimizer_decision_link() -> None:
    bad = LEARNING_TRACE.replace(
        "benchmark_index_candidate(session_id=session-1, candidate_id=index-1,\n"
        "decision_id=decision-opt-2)",
        "benchmark_index_candidate(session_id=session-1, candidate_id=index-1)",
        1,
    )
    assert "each material benchmark/final is linked" in (
        copilot_optimizer_acceptance.validate_learning_loop_response(bad)
    )


def test_canaries_reject_a_missing_plan_terminal_link() -> None:
    bad = LEARNING_TRACE.replace(
        "verify_plan_action(intent_id=intent-1, decision_id=decision-plan-1)\n"
        "-> terminal_link_id=terminal-plan-1 (terminal evidence).",
        "verify_plan_action(intent_id=intent-1, decision_id=decision-plan-1).",
        1,
    )
    assert "plan terminal decision links" in (
        copilot_plan_enforcer_acceptance.validate_response(bad)
    )


def test_canaries_reject_review_without_the_returned_terminal_link() -> None:
    bad = LEARNING_TRACE.replace(
        "terminal_evidence_refs=[terminal-opt-2]",
        "terminal_evidence_refs=[terminal-health-1]",
        1,
    )
    assert "each material benchmark/final is linked" in (
        copilot_optimizer_acceptance.validate_learning_loop_response(bad)
    )


def test_plan_enforcer_canary_rejects_an_actual_mutation_call() -> None:
    bad = LEARNING_TRACE.replace(
        "Mode: review-only; no authorization.",
        "Mode: review-only; no authorization. Called apply_prepared_plan_action.",
        1,
    )
    assert "no mutation call" in copilot_plan_enforcer_acceptance.validate_response(bad)
