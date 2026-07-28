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
        "Treat them as defaults only",
        "never widen local policy",
        "fastest proven-equivalent candidate",
        "largest useful budget within the returned policy",
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
    assert "`mcp_contract.performance_tuning=1`" in TEXT
    assert "`mcp_contract.durable_view_change=1`" in TEXT
    assert "`local_tuning_policy`" in TEXT
    assert "remain in static mode" in TEXT


def test_optimizer_has_no_retired_or_private_runtime_dependencies() -> None:
    retired = "_".join(("query", "geneva", "db"))  # noqa: FLY002
    retired_transport = "".join(("connect", "or"))  # noqa: FLY002
    assert retired not in TEXT
    assert retired_transport not in TEXT.casefold()
    assert "raw environment values" in TEXT


def test_optimizer_is_compact_and_has_operational_pattern_cards() -> None:
    assert len(TEXT.splitlines()) < 500
    for heading in (
        "Family 1: predicates and SARGability",
        "Family 2: joins and relational shape",
        "Family 3: aggregation, windowing, ordering, and row goals",
        "Family 4: cardinality, parameters, and statistics",
        "Family 5: indexes",
        "Family 6: combined winners",
        "Parameter compilation and intelligent query processing",
        "Ordinary, filtered, computed, and indexed-view options",
        "Plan reading discipline",
        "Azure SQL Database operating context",
        "Views and deployment handoff",
    ):
        assert heading in TEXT
    for card_field in (
        "**Trigger:**",
        "**Safe rewrite/action:**",
        "**Preconditions:**",
        "**Counterexample/risk:**",
        "**MCP evidence:**",
    ):
        assert TEXT.count(card_field) >= 12


def test_optimizer_distinguishes_candidate_states_and_no_change() -> None:
    for phrase in (
        "Static candidate",
        "Measured candidate",
        "Finalist",
        "Deployable winner",
        "`no_change`",
        "not collected",
        "complete leaderboard",
        "exact stopping reason",
        "deployment handoff",
        "Evidence gaps",
    ):
        assert phrase in TEXT


def test_optimizer_has_exact_profiles_gates_and_adaptive_budget() -> None:
    for phrase in (
        "AZURE_SQL_PROFILE=optimizer",
        "AZURE_SQL_PROFILE=sandbox",
        "AZURE_SQL_TOOL_GROUPS=core,performance,admin",
        "allow_benchmark=true",
        "allow_test_indexes=true",
        "allow_view_apply=true",
        "AZURE_SQL_PERSIST_VIEW_SQL_STATE=true",
        "local stdio",
        "6 executions per bucket",
        "24 for four buckets",
        "48",
        "9 executions per bucket",
        "60 for four",
        "80 total measured query executions",
        "common`, `rare`, `NULL",
        "`benchmark_index_candidate`",
        "`prepare_view_change`",
        "`apply_prepared_view_change`",
        "`verify_view_change`",
        "`rollback_view_change`",
        "optimizer`, use `prepare_view_change` only as a read-only preview",
        "call only `benchmark_index_candidate`",
    ):
        assert phrase in TEXT
    normalized = " ".join(TEXT.split())
    for phrase in (
        "does not define a `view` tuning-candidate strategy",
        "Do not register it as a measured candidate",
        "call `rollback_view_change` immediately",
        "report `cleanup_required`",
        "performance and equivalence as `not collected`",
    ):
        assert phrase in normalized


def test_optimizer_has_resource_and_semantic_safety_language() -> None:
    for phrase in (
        "implicit conversion",
        "optional filters",
        "LIKE",
        "EXISTS",
        "fan-out",
        "top-per-group",
        "pagination",
        "residual predicate",
        "key lookups",
        "memory grants",
        "duplicate-aware multisets",
        "compare the ordered sequence",
        "snapshot-consistent",
        "never proven equivalent",
        "resource pressure",
        "concurrency",
        "never weaken an exact range to “bounded”",
        "reproduce exact domain endpoints",
        "overflow/boundary preconditions",
    ):
        assert phrase in TEXT


def test_optimizer_bakes_in_azure_sql_specific_tuning_knowledge() -> None:
    for phrase in (
        "DATEADD(day, 1, @Day)",
        "typed `sp_executesql`",
        "A CTE is syntax, not guaranteed materialization",
        "scalar UDF",
        "Parameter Sensitive Plan optimization",
        "Optional Parameter Plan Optimization",
        "compatibility level",
        "Automatic Tuning",
        "elastic pool",
        "serverless",
        "Hyperscale",
        "instance trace flags",
        "actual rows read",
        "seek predicates",
        "residual predicates",
        "write amplification",
    ):
        assert phrase in TEXT


def test_optimizer_distinguishes_rewrite_equivalence_from_index_stability() -> None:
    for phrase in (
        "one snapshot-consistent comparison",
        "complete results within `AZURE_SQL_COMPARISON_ROW_LIMIT`",
        "unchanged SQL across A-B-A phases",
        "cannot claim a same-snapshot rewrite proof",
        "candidate plan used the expected index",
    ):
        assert phrase in TEXT


def test_optimizer_requires_runtime_and_schema_fingerprints_before_database_tools() -> None:
    runtime_call = (
        "calling `check_runtime_status`, then\n"
        "`list_databases`, then `check_capabilities`"
    )
    assert runtime_call in TEXT
    for phrase in (
        "Initialize the MCP contract",
        "Complete this sequence before\n`explain_query` or any case/session tool",
        "runtime_fingerprint",
        "tool_schema_fingerprint",
        "sanitized_config_fingerprint",
        "stable for the same MCP process",
        "full host restart",
        "Never widen the returned local policy",
    ):
        assert phrase in TEXT


def test_optimizer_requires_provenance_for_actual_query_plans() -> None:
    for phrase in (
        "analyze=true",
        "query_executed=true",
        "`summary.actual_metrics`",
        "`metric_provenance`",
        "plan_kind=actual",
        "`analyze=true` alone is insufficient",
    ):
        assert phrase in TEXT


def test_optimizer_retries_evidence_once_and_recovers_persisted_case() -> None:
    for phrase in (
        "retry `collect_performance_evidence` exactly once",
        "same request and same idempotency key",
        "retrieve persisted case evidence",
        "core benchmark/comparison path works",
        "explicit gap",
    ):
        assert phrase in TEXT


def test_optimizer_gates_volatile_and_unordered_proof_claims() -> None:
    for phrase in (
        "GETDATE",
        "current-time function",
        "`NEWID`",
        "nondeterministically seeded `RAND`",
        "non-repeatable `TABLESAMPLE`",
        "deterministic `RAND` seed is allowed",
        "ordered `TOP`",
        "order-sensitive window expression",
        "verified unique total",
        "classification=proof_contract_required",
        "before\nfinalists",
        "`performance_only`",
        "proxy must never be used to overclaim",
        "no deterministic proof input",
    ):
        assert phrase in TEXT


def test_optimizer_requires_parent_lineage_for_combined_rewrite_and_index() -> None:
    for phrase in (
        "child `combined` candidate",
        "artifact_ref` exactly `candidate:<proven-parent-id>",
        "proven `improved` finalist state",
        "same session",
        "No unproven, neutral, regressed, performance-only, or cross-session parent",
        "runs only through `benchmark_index_candidate` with `phase=finalist`",
    ):
        assert phrase in TEXT


def test_optimizer_keeps_views_recommendation_only_and_does_not_claim_warmup() -> None:
    assert "recommendation-only" in TEXT
    assert "canonical view benchmark" in TEXT
    assert "warmup" not in TEXT.casefold()
