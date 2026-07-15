from __future__ import annotations

import pathlib
import re


SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
TEXT = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")


def test_optimizer_is_one_self_contained_workflow() -> None:
    assert TEXT.startswith("---\nname: sql-optimizer\n")
    assert "Required first response behavior" in TEXT
    assert "Missing plan lowers confidence" not in TEXT  # wording stays imperative, not a slogan
    assert "A missing plan lowers confidence" in TEXT
    assert not re.search(r"https?://|[A-Za-z]+Guide\.md|sources/", TEXT)


def test_optimizer_rewrites_before_plan_and_continues_after_losses() -> None:
    assert "Produce safe concrete rewrite candidates before plan access" in TEXT
    assert "Show at least one concrete candidate SQL block" in TEXT
    assert "Never stop because the first rewrite, index, benchmark, or tool call loses" in TEXT
    assert "A slower or failed index rejects only that index candidate" in TEXT
    assert "If MCP is unavailable, continue in static mode" in TEXT


def test_optimizer_covers_all_six_candidate_families() -> None:
    expected = (
        "Family 1: predicates and SARGability",
        "Family 2: joins and relational shape",
        "Family 3: aggregation, windowing, ordering, and row goals",
        "Family 4: cardinality, parameters, and statistics",
        "Family 5: indexes",
        "Family 6: combined winners",
    )
    for heading in expected:
        assert heading in TEXT


def test_optimizer_encodes_budget_terminal_states_and_parameter_buckets() -> None:
    for phrase in (
        "10 candidate experiments",
        "3 interleaved screening runs",
        "5 interleaved finalist runs",
        "up to 4 parameter cases",
        "80 total measured query executions",
        "20 minutes wall-clock",
        "common, rare, NULL when valid, and a boundary value",
    ):
        assert phrase in TEXT
    for state in (
        "improved",
        "neutral",
        "regressed",
        "equivalence_failed",
        "inconclusive",
        "cleanup_required",
    ):
        assert f"`{state}`" in TEXT


def test_optimizer_requires_duplicate_order_and_snapshot_aware_equivalence() -> None:
    for phrase in (
        "snapshot-consistent",
        "duplicate-aware multisets",
        "compare the ordered sequence",
        "bounded sample",
        "never proven equivalent",
    ):
        assert phrase in TEXT


def test_optimizer_uses_typed_mcp_session_and_sandbox_tools() -> None:
    for tool in (
        "start_performance_case",
        "collect_performance_evidence",
        "start_tuning_session",
        "add_tuning_candidate",
        "benchmark_tuning_candidate",
        "benchmark_index_candidate",
        "compare_query_results",
        "compare_plan_summaries",
        "finalize_tuning_session",
    ):
        assert f"`{tool}`" in TEXT


def test_optimizer_has_no_retired_or_private_runtime_dependencies() -> None:
    retired = "_".join(("query", "geneva", "db"))
    retired_transport = "".join(("connect", "or"))
    assert retired not in TEXT
    assert retired_transport not in TEXT.casefold()
    assert "raw environment values" in TEXT
