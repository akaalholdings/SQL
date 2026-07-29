from __future__ import annotations

import pathlib
import re

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
TEXT = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")


def test_triage_is_self_contained_and_read_only() -> None:
    assert TEXT.startswith("---\nname: sql-health-triage\n")
    assert 'metadata:\n  version: "1.0.0"' in TEXT
    assert "This skill is permanently read-only" in TEXT
    assert "Never call DDL, DML, unrestricted execution" in TEXT
    assert not re.search(r"https?://|[A-Za-z]+Guide\.md", TEXT)


def test_triage_uses_exact_outcome_vocabulary() -> None:
    for state in ("healthy", "actionable", "partial", "inconclusive"):
        assert f"`{state}`" in TEXT
    assert "exactly one overall outcome" in TEXT


def test_incomplete_evidence_can_never_be_healthy() -> None:
    assert "Do not call an outcome healthy when any required evidence is unavailable" in TEXT
    assert "report `partial`" in TEXT
    assert "use partial or inconclusive, not healthy" in TEXT


def test_triage_normalizes_provenance_window_units_and_identity() -> None:
    for phrase in (
        "collection start and end time in UTC",
        "availability and completeness",
        "truncation and row/sample limits",
        "value, units, threshold/baseline",
        "stable query identity",
        "parameter bucket",
        "artifact reference",
    ):
        assert phrase in TEXT


def test_triage_uses_shared_cases_and_safe_handoffs() -> None:
    for tool in (
        "start_performance_case",
        "collect_performance_evidence",
        "get_performance_case",
    ):
        assert f"`{tool}`" in TEXT
    assert "case id is the durable handoff key" in TEXT
    assert "Do not copy raw SQL into local JSON" in TEXT


def test_deprecated_query_health_heuristics_are_absent() -> None:
    forbidden = (
        "fragment" + "ation",
        "page life " + "expectancy",
        "buffer cache " + "hit ratio",
    )
    lowered = TEXT.casefold()
    for phrase in forbidden:
        assert phrase not in lowered
