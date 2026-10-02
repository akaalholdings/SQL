from __future__ import annotations

import pathlib
import re

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
TEXT = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")


def test_triage_is_self_contained_and_read_only() -> None:
    assert TEXT.startswith("---\nname: sql-health-triage\n")
    assert 'metadata:\n  version: "1.0.1"' in TEXT
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


def test_triage_reports_a_blocker_once_then_stops_retrying() -> None:
    # report_stuck writes a local backlog entry, so the read-only rule must name
    # it or the two rules contradict each other.
    rules = " ".join(TEXT.split("## Non-negotiable rules", 1)[1].split("## ", 1)[0].split())
    assert "Call only read-only MCP tools, plus `report_stuck`" in rules
    assert "Never call DDL, DML, unrestricted execution" in rules
    for phrase in (
        "the same tool call fails the same way twice",
        "a required precondition cannot be met",
        "this skill's text contradicts what a tool returns",
        "call `report_stuck` once with `skill`, `skill_version` (this file's `metadata.version`), `last_tool`,",
        "no SQL, data values, or object, server, or database names",
        "If `report_stuck` is not in the tool list, skip it silently",
        "stop retrying that exact call and tell the user what is blocked",
        "A tool error whose `failure_diagnostic.transient` is true (for example 40613, 40501, 49918) follows normal retry guidance first",
    ):
        assert phrase in rules
