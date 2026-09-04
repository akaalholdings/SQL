from __future__ import annotations

import pathlib
import re

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SKILLS = {
    "sql-health-triage": REPO_ROOT / "skills/sql_health_triage/SKILL.md",
    "sql-optimizer": REPO_ROOT / "skills/sql_optimizer/SKILL.md",
    "sql-plan-enforcer": REPO_ROOT / "skills/sql_plan_enforcer/SKILL.md",
    "sql-index-manager": REPO_ROOT / "skills/sql_index_manager/SKILL.md",
}
WRITE_LEARNING_SKILLS = {
    skill: path
    for skill, path in SKILLS.items()
    if skill != "sql-index-manager"
}


def _compact(path: pathlib.Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8").casefold())


def test_exact_skill_versions_are_published() -> None:
    expected = {
        "sql-health-triage": 'metadata: version: "1.0.1"',
        "sql-optimizer": 'metadata: version: "2.3.1"',
        "sql-plan-enforcer": 'metadata: version: "1.0.1"',
        "sql-index-manager": 'metadata: version: "1.0.1"',
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
    for skill, path in WRITE_LEARNING_SKILLS.items():
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

    index = _compact(SKILLS["sql-index-manager"])
    for tool in (
        "capture_index_review_snapshot",
        "review_index_portfolio",
        "get_index_review",
    ):
        assert tool in index
    assert "prior_review_id" in index
    assert "registered subject `index`" in index
    assert "evidence_id=none" in index

    for text in (health, optimizer, plan, index):
        assert "do not invent a decision_id" not in text
        assert "do not invent a decision_id parameter" not in text


def test_index_portfolio_artifacts_are_exact_files_and_ids_are_tracking_only() -> None:
    index = _compact(SKILLS["sql-index-manager"])
    for filename in (
        "index-review.json",
        "index-review.md",
        "create-candidates.sql",
        "consolidation-candidates.sql",
        "drop-candidates.sql",
        "rollback.sql",
        "validation.sql",
    ):
        assert filename in index
    assert "snapshot_id" in index
    assert "as_of_run_id" in index
    assert "evidence_id=none" in index
    assert "review_id" in index
    assert "run_id" in index
    assert "not learning evidence refs" in index
    assert "never invent a non-null `evidence_id`" in index
    assert "evidence_ref" not in index
    for invented in (
        "prior_state_ref",
        "classification_ref",
        "blocker_ref",
        "validation_ref",
        "rollback_ref",
    ):
        assert invented not in index


def test_retired_index_portfolio_tool_names_are_absent() -> None:
    retired = (
        "get_index_portfolio_" + "snapshot",
        "capture_index_portfolio_" + "snapshot",
        "classify_index_" + "portfolio",
        "recheck_index_" + "portfolio",
    )
    roots = (REPO_ROOT / "README.md", REPO_ROOT / "skills", REPO_ROOT / "scripts", REPO_ROOT / "docs")
    paths = [path for root in roots for path in ([root] if root.is_file() else root.rglob("*"))]
    for path in paths:
        relative_parts = path.relative_to(REPO_ROOT).parts
        if (
            not path.is_file()
            or any(part.startswith(".") for part in relative_parts)
            or "__pycache__" in path.parts
            or path.suffix == ".pyc"
        ):
            continue
        text = path.read_text(encoding="utf-8")
        for name in retired:
            assert name not in text, f"{name} remains in {path}"


def test_learning_fallback_and_safety_boundaries_are_explicit() -> None:
    for skill, path in WRITE_LEARNING_SKILLS.items():
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


def test_index_manager_is_recall_only_until_an_evidence_bridge_exists() -> None:
    index = _compact(SKILLS["sql-index-manager"])
    for phrase in (
        "recall-only",
        "future public mcp contract",
        "index evidence bridge",
        "evidence_id=none",
        "no terminal link",
        "not a learning evidence reference",
        (
            "no v1 initial result, later recheck, or explicit human resolution"
            " becomes an `outcomereviewv1`"
        ),
        "recheck classification remains valid",
        "without invoking learning or handoff tools",
    ):
        assert phrase in index, phrase

    forbidden = (
        "record_decision",
        "review_decision",
        "propose_lesson",
        "list_learning_candidates",
        "create_handoff",
        "get_handoff",
        "resolve_handoff",
    )
    for tool in forbidden:
        assert f"`{tool}`" in index
        assert re.search(rf"\b{tool}\s*\(", index) is None


def test_peer_routes_to_index_manager_are_report_only_in_v1() -> None:
    for skill, path in WRITE_LEARNING_SKILLS.items():
        text = _compact(path)
        assert (
            "v1 index-manager routing is the exception to the typed handoff rule"
            in text
        ), skill
        assert "do not create a typed `handoffv1` for that target" in text, skill
        assert (
            "do not relabel case, snapshot, review, or run ids as learning evidence refs"
            in text
        ), skill
