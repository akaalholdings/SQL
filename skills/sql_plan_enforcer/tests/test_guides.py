from __future__ import annotations

import pathlib
import re

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
TEXT = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")


def test_enforcer_is_self_contained_and_mcp_owned() -> None:
    assert TEXT.startswith("---\nname: sql-plan-enforcer\n")
    assert 'metadata:\n  version: "1.0.1"' in TEXT
    assert "MCP prepared-intent lifecycle" in TEXT
    assert "MCP owns all durable intent state" in TEXT
    assert not re.search(r"https?://|[A-Za-z]+Guide\.md", TEXT)


def test_direct_apply_paths_are_forbidden_and_tick_is_preview_only() -> None:
    assert "`plan_enforcer_tick` is permanently preview-only" in TEXT
    for tool in (
        "force_query_plan",
        "apply_plan_action",
        "unrestricted SQL",
    ):
        assert tool in TEXT
    assert "not valid apply paths" in TEXT


def test_prepared_intent_lifecycle_is_complete() -> None:
    for tool in (
        "prepare_plan_action",
        "apply_prepared_plan_action",
        "verify_plan_action",
        "rollback_plan_action",
    ):
        assert f"`{tool}`" in TEXT
    assert "observed -> reviewed -> prepared -> applied -> observing -> kept" in TEXT


def test_apply_is_fail_closed_and_idempotent() -> None:
    for phrase in (
        "local stdio `enforcer-apply`",
        "`AZURE_SQL_TOOL_GROUPS=core,performance,admin`",
        "unrestricted/apply posture",
        "evidence hash",
        "exact prior force/hint state",
        "database policy",
        "global kill switch",
        "idempotency key",
        "ownership is explicitly manual",
        "must not apply twice",
    ):
        assert phrase in TEXT


def test_verification_and_rollback_use_matching_evidence_and_exact_prior_state() -> None:
    for phrase in (
        "Pre and post windows must not overlap",
        "parameter buckets must match",
        "Insufficient evidence never means keep",
        "restores the exact pre-change state",
        "mark `unknown`",
    ):
        assert phrase in TEXT


def test_automatic_tuning_and_unknown_ownership_are_review_only() -> None:
    assert "Unknown or Automatic Tuning/engine ownership is review-only" in TEXT
    assert "do not prepare or apply an overlapping custom control" in TEXT
