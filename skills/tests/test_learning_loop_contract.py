from __future__ import annotations

import pathlib
import re

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SKILLS = {
    "sql-health-triage": REPO_ROOT / "skills/sql_health_triage/SKILL.md",
    "sql-optimizer": REPO_ROOT / "skills/sql_optimizer/SKILL.md",
    "sql-plan-enforcer": REPO_ROOT / "skills/sql_plan_enforcer/SKILL.md",
}


def _compact(path: pathlib.Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8").casefold())


def test_exact_skill_versions_are_published() -> None:
    expected = {
        "sql-health-triage": 'metadata: version: "1.0.0"',
        "sql-optimizer": 'metadata: version: "2.3.0"',
        "sql-plan-enforcer": 'metadata: version: "1.0.0"',
    }
    for skill, phrase in expected.items():
        assert phrase in _compact(SKILLS[skill]), skill


def test_every_skill_gates_database_and_runtime_before_recall() -> None:
    for skill, path in SKILLS.items():
        text = _compact(path)
        assert text.index("check_runtime_status") < text.index("recall_lessons"), skill
        assert text.index("list_databases") < text.index("recall_lessons"), skill
        assert text.index("check_capabilities") < text.index("recall_lessons"), skill
        for fingerprint in (
            "runtime_fingerprint",
            "runtime_compatibility_fingerprint",
            "tool_schema_fingerprint",
            "sanitized_config_fingerprint",
        ):
            assert fingerprint in text, (skill, fingerprint)
        assert "skill_version" in text, skill
        assert "raw sql" in text
        assert "hidden reasoning" in text
        recall_arguments = text.split("call `recall_lessons`", 1)[1].split(
            "never send", 1
        )[0]
        assert "runtime_compatibility_fingerprint" in recall_arguments, skill
        assert "`runtime_fingerprint`" not in recall_arguments, skill


def test_learning_order_is_evidence_decision_terminal_review_correction() -> None:
    required = (
        "record_decision",
        "decisionrecordv1",
        "decision_id",
        "subject_kind",
        "subject_fingerprint",
        "based_on_review_ids",
        "terminal",
        "terminal_link_id",
        "review_decision",
        "outcomereviewv1",
        "correction",
        "counterexample",
        "next_observation",
        "create_handoff",
        "get_handoff",
        "resolve_handoff",
        "handoffv1",
    )
    for skill, path in SKILLS.items():
        text = _compact(path)
        for phrase in required:
            assert phrase in text, (skill, phrase)
        learning = text.split("## evidence-governed learning loop", 1)[1]
        assert learning.index("record_decision") < learning.index("review_decision"), skill
        assert learning.index("review_decision") < learning.index("correction"), skill
        assert learning.index("counterexample") < learning.index("next_observation"), skill
        assert "evidence before" in learning, skill


def test_supported_decision_links_are_skill_specific() -> None:
    health = _compact(SKILLS["sql-health-triage"])
    optimizer = _compact(SKILLS["sql-optimizer"])
    plan = _compact(SKILLS["sql-plan-enforcer"])

    for tool in ("analyze_db_health", "collect_performance_evidence", "resolve_handoff"):
        assert tool in health
    for tool in (
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "finalize_tuning_session",
    ):
        assert tool in optimizer
    assert "prepare_plan_action` remains unlinked" in plan
    for tool in ("verify_plan_action", "rollback_plan_action", "resolve_handoff"):
        assert tool in plan
    assert "decision_id" in health and "decision_id" in optimizer and "decision_id" in plan

    for text in (health, optimizer, plan):
        assert "do not invent a decision_id" not in text
        assert "do not invent a decision_id parameter" not in text


def test_learning_fallback_and_safety_boundaries_are_explicit() -> None:
    for skill, path in SKILLS.items():
        text = _compact(path)
        for phrase in (
            "unavailable",
            "malformed",
            "stale",
            "incompatible",
            "remote-disabled",
            "unchanged",
            "advisory",
            "cannot activate",
            "local ledger",
            "install memory",
            "authorization",
            "equivalence",
            "cleanup",
            "verification",
            "rollback",
        ):
            assert phrase in text, (skill, phrase)
        assert "decision_id" in text, skill
