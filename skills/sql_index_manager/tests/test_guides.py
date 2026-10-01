from __future__ import annotations

import pathlib
import re

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
TEXT = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
NORMALIZED = " ".join(TEXT.casefold().split())
PUBLIC_README = (SKILL_DIR.parent / "README.md").read_text(encoding="utf-8")
PUBLIC_NORMALIZED = " ".join(PUBLIC_README.casefold().split())


def _position(term: str) -> int:
    return NORMALIZED.index(term.casefold())


def test_frontmatter_names_a_workload_driven_recommend_only_skill() -> None:
    assert TEXT.startswith("---\nname: sql-index-manager\n")
    assert 'metadata:\n  version: "2.0.0"' in TEXT
    description = TEXT.split("description:", 1)[1].split("\n", 1)[0].casefold()
    assert "query store" in description
    assert "recommend-only" in description
    assert "never executes index ddl" in description
    assert not re.search(r"https?://|/Users/", TEXT)
    assert " ".join(("safe", "to", "drop")) not in NORMALIZED  # noqa: FLY002


def test_runtime_gate_runs_before_the_review_in_order() -> None:
    ordered = (
        "check_runtime_status",
        "package version `2.4.0` or newer",
        "list_databases",
        "let the user pick one",
        "check_capabilities",
        "mcp_contract.workload_index_advisor=1",
        "call `review_workload_indexes`",
    )
    positions = [_position(term) for term in ordered]
    assert positions == sorted(positions)


def test_the_advisor_needs_no_install_step_or_policy_file() -> None:
    for phrase in (
        "view database state",
        "view definition",
        "no policy file, history table, or install step is required",
    ):
        assert phrase in NORMALIZED


def test_every_result_status_has_explicit_handling() -> None:
    for status in ("`ok`", "`empty`", "`precondition`", "`unavailable`", "`not_supported`"):
        assert status in TEXT
    # Remediation is shown to the DBA, never executed by the skill.
    assert "show the returned `remediation` statement for the dba to run; do not run it" in NORMALIZED
    assert "report `gaps` verbatim" in NORMALIZED


def test_scope_defaults_and_past_windows_are_explicit() -> None:
    for phrase in ("`schema_name`", "`table_names`", "`lookback_days=7`", "`as_of_utc`", "`top_queries=100`"):
        assert phrase in TEXT
    assert "full business cycle" in NORMALIZED
    assert "`analyzed_share_pct` is below 80" in NORMALIZED


def test_skill_never_executes_ddl_or_calls_ddl_tools() -> None:
    assert "recommend-only. never execute index ddl" in NORMALIZED
    for tool in ("execute_tsql_unrestricted", "create_test_index", "drop_test_index", "rebuild_index"):
        assert f"`{tool}`" in TEXT
        assert re.search(rf"\b{tool}\s*\(", TEXT) is None
    assert "inert advice for a human dba's change control" in NORMALIZED
    assert "never say a change is safe, approved, or applied" in NORMALIZED


def test_estimates_are_labelled_as_upper_bounds_not_promises() -> None:
    assert "`estimated_max_benefit_pct` as an upper-bound estimate" in NORMALIZED
    assert "does not promise a saving" in NORMALIZED
    assert "`estimated_max_size_mb`" in TEXT
    assert "never invent database names, query ids, index names, row counts, timings" in NORMALIZED


def test_proof_and_rewrites_route_to_the_optimizer() -> None:
    proof = NORMALIZED.split("## prove before production", 1)[1]
    for phrase in ("`sql-optimizer`", "`start_tuning_session`", "`benchmark_index_candidate`", "non-production copy"):
        assert phrase in proof
    assert "only a measured, validated candidate goes to the human dba's change control" in proof
    assert "non-sargable predicates and implicit conversions cannot be fixed by an index" in NORMALIZED


def test_removal_discipline_covers_counter_resets_capture_mode_and_rollback() -> None:
    removal = NORMALIZED.split("## removal discipline", 1)[1].split("## prove before production", 1)[0]
    for phrase in (
        "usage counters reset on failover",
        "`usage_counters.days_since_reset`",
        "capture mode `auto` can miss rare queries",
        "no index hint, plan guide, or forced plan",
        "`rollback_ddl` must be kept",
        "never removal candidates",
    ):
        assert phrase in removal


def test_design_judgement_is_labelled_and_cannot_override_evidence() -> None:
    assert '"modified, unvalidated"' in NORMALIZED
    assert "never change a returned `confidence`, drop a returned blocker" in NORMALIZED


def test_portfolio_history_tools_are_optional_and_policy_gated() -> None:
    section = NORMALIZED.split("## optional: long-term portfolio history", 1)[1].split("##", 1)[0]
    for tool in ("capture_index_review_snapshot", "review_index_portfolio", "get_index_review"):
        assert f"`{tool}`" in section
    assert "not needed for workload-driven design" in section
    assert "allow_read=true" in section


def test_output_sections_are_ordered() -> None:
    ordered = (
        "**outcome**",
        "**coverage and gaps**",
        "**top improvements**",
        "**cleanup**",
        "**per-table detail**",
        "**rewrite opportunities**",
        "**next steps**",
    )
    positions = [_position(term) for term in ordered]
    assert positions == sorted(positions)
    assert '"inert, for dba change control"' in NORMALIZED


def test_public_readme_describes_the_workload_driven_flow() -> None:
    for phrase in (
        "review_workload_indexes",
        "sql-index-manager",
        "recommend-only",
    ):
        assert phrase in PUBLIC_NORMALIZED
    assert "seven" not in PUBLIC_NORMALIZED.split("sql-index-manager", 1)[1].split("\n", 1)[0]
