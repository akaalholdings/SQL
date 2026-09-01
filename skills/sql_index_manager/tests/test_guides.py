from __future__ import annotations

import pathlib
import re

SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
TEXT = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
NORMALIZED = " ".join(TEXT.casefold().split())
PUBLIC_README = (SKILL_DIR.parent / "README.md").read_text(encoding="utf-8")
PUBLIC_NORMALIZED = " ".join(PUBLIC_README.casefold().split())


def test_index_manager_is_self_contained_and_recommend_only() -> None:
    assert TEXT.startswith("---\nname: sql-index-manager\n")
    assert 'metadata:\n  version: "1.0.0"' in TEXT
    assert "recommend-only" in TEXT
    assert "never executes index DDL" in TEXT
    assert "one narrow,\nappend-only snapshot-history write" in TEXT
    assert "not a read-only workflow" in TEXT
    assert "All index DDL" in TEXT
    assert not re.search(r"https?://|[A-Za-z]+Guide\\.md|/Users/", TEXT)
    assert " ".join(("safe", "to", "drop")) not in TEXT.casefold()  # noqa: FLY002


def test_modes_default_and_runtime_database_gates_are_ordered() -> None:
    for mode in ("`inventory`", "`review`", "`recheck`"):
        assert mode in TEXT
    assert "`review` (the default)" in TEXT
    ordered = (
        "check_runtime_status",
        "list_databases",
        "user-selected allowlisted",
        "check_capabilities",
        "mcp_contract.index_portfolio_review=1",
        "allow_read=true",
        "index_review_min_observation_days=90",
        "review_index_portfolio(database_name=<selected>)",
    )
    positions = [NORMALIZED.index(term.casefold()) for term in ordered]
    assert positions == sorted(positions)
    for fingerprint in (
        "runtime_fingerprint",
        "runtime_compatibility_fingerprint",
        "tool_schema_fingerprint",
        "sanitized_config_fingerprint",
        "index_history_schema_fingerprint",
    ):
        assert fingerprint in NORMALIZED
    remaining = TEXT.replace("tool_schema_fingerprint", "").replace(
        "index_history_schema_fingerprint", ""
    )
    assert "schema_fingerprint" not in remaining
    assert "catalog_fingerprint" not in NORMALIZED
    assert "mcp_contract.index_history_schema_version=index-history-v1" in NORMALIZED
    assert "mcp_contract.index_review_snapshot_reuse_hours=48" in NORMALIZED
    calls = re.findall(
        r"capture_index_review_snapshot\([^)]*\)|review_index_portfolio\([^)]*\)|get_index_review\([^)]*\)",
        TEXT,
    )
    assert all("decision_id" not in call for call in calls)


def test_snapshot_age_and_controlled_capture_are_fail_closed() -> None:
    for phrase in (
        "less than 48 hours old",
        "48 hours old or older is stale",
        "capture_index_review_snapshot",
        "controlled",
        "`idempotency_key` is optional",
        "mcp can use its default",
        "same-key no-retry safety",
        "allow-capture",
        "capture kill switch",
        "do not classify candidates",
        "partial",
        "inconclusive",
    ):
        assert phrase.casefold() in NORMALIZED
    assert "only database write this skill may request" in NORMALIZED
    assert "allow_index_history_write` policy defaults to `false`" in TEXT
    assert "fixed capability, not a per-database policy key" in NORMALIZED
    assert "optional `business_cycle_extension_days`" in TEXT
    assert "capture is a separate explicit tool step" in NORMALIZED
    assert (
        "reuse a returned later non-overlapping run only when it is less than 48 hours old"
        in NORMALIZED
    )
    assert "same explicit selected-database" in NORMALIZED
    assert "then invoke `review_index_portfolio" in NORMALIZED
    assert " ".join(  # noqa: FLY002
        ("positive", "idempotency", "key")
    ) not in NORMALIZED


def test_public_policy_docs_keep_capability_and_policy_contracts_distinct() -> None:
    for text in (TEXT, PUBLIC_README):
        normalized = " ".join(text.casefold().split())
        assert "index_review_min_observation_days=90" in normalized
        assert "fixed capability" in normalized
        assert "not a per-database policy key" in normalized
        assert "business_cycle_extension_days" in normalized
    assert '"index_review_min_observation_days"' not in PUBLIC_README
    assert "public mcp contract remains `2.3.0`" in PUBLIC_NORMALIZED


def test_create_candidate_contract_is_complete_and_dmv_only_stays_observe() -> None:
    section = TEXT[TEXT.index("## Executed Query Store recurrence and workload review") :]
    section = " ".join(section.split()).casefold()
    for phrase in (
        "request identity must recur exactly",
        "at least two runtime intervals",
        "material positive existing mcp score",
        "complete query store coverage",
        "no exact or covering index",
        "projected storage strictly below 90 percent",
        "missing-index dmv-only evidence remains `observe`",
    ):
        assert phrase.casefold() in section, phrase


def test_v1_learning_is_exact_advisory_recall_only() -> None:
    section = TEXT[TEXT.index("## Evidence-governed advisory recall") :]
    section = section[: section.index("## Snapshot and observation gates")]
    assert "recall_lessons(skill=sql-index-manager" in section
    assert "evidence_id=None" in section
    assert "no terminal\nlink" in section
    assert "future public MCP contract" in section
    assert "explicit human resolution becomes an\n`OutcomeReviewV1`" in section
    for tool in (
        "record_decision",
        "review_decision",
        "propose_lesson",
        "list_learning_candidates",
        "create_handoff",
        "get_handoff",
        "resolve_handoff",
    ):
        assert f"`{tool}`" in section
        assert re.search(rf"\b{tool}\s*\(", section) is None


def test_outcomes_and_subject_states_are_exact() -> None:
    for outcome in ("actionable", "no_change", "partial", "inconclusive"):
        assert f"`{outcome}`" in TEXT
    for state in (
        "keep",
        "create_candidate",
        "consolidate_candidate",
        "drop_candidate",
        "observe",
    ):
        assert f"`{state}`" in TEXT
    assert "exactly one state" in TEXT
    assert "Overall outcomes are exactly `actionable`, `no_change`, `partial`, or" in TEXT
    assert "initial review, a later non-overlapping recheck" in NORMALIZED
    assert "never become `outcomereviewv1`" in NORMALIZED
    assert "llm cannot promote" in NORMALIZED
    assert "exact `reason_codes` array verbatim" in NORMALIZED
    assert "exact_duplicate_definition" in NORMALIZED
    assert "strict_coverage_overlap" in NORMALIZED
    section = TEXT[
        TEXT.index("## Deterministic classification and outcomes") :
        TEXT.index("## Removal gates: 90-day minimum, stable epoch, and no gap")
    ]
    keep_section = section[section.index("- `keep`") : section.index("- `create_candidate`")]
    assert "fallback" not in keep_section.casefold()
    assert (
        "default whenever evidence does not justify `keep` or a candidate"
        in " ".join(section.casefold().split())
    )


def test_removal_gates_include_stable_epoch_and_no_gap_rules() -> None:
    section = TEXT[TEXT.index("## Removal gates: 90-day minimum, stable epoch, and no gap") :]
    section = section[: section.index("## Protection and special-index matrix")]
    section = " ".join(section.split()).casefold()
    for phrase in (
        "enabled, user-created, nonunique",
        "type-2 rowstore",
        "at least 90 continuous days",
        "business-cycle extension",
        "persisted daily history has no gap over 48 hours",
        "same database, engine",
        "counters never decrease",
        "seek, scan, and lookup deltas are zero",
        "measurable write or storage cost",
        "Query Store, hint, dependency, and protection coverage is complete",
        "any executed Query Store plan reference",
        "query store gap",
        "no_stored_plan_without_execution",
        "retained stored-plan reference without execution",
        "post-classification execution-readiness",
        "never promote, demote, or rewrite the mcp state",
    ):
        assert phrase.casefold() in section, phrase
    assert " ".join(  # noqa: FLY002
        ("at", "least", "3", "distinct", "UTC", "days")
    ) not in section
    assert " ".join(  # noqa: FLY002
        ("at", "least", "5", "total", "executions")
    ) not in section
    assert " ".join(("same", "90-day", "window")) not in section  # noqa: FLY002


def test_protection_matrix_and_exact_overlap_rules_are_complete() -> None:
    for phrase in (
        "Primary-key or unique-constraint",
        "Clustered rowstore",
        "Foreign-key-supporting",
        "indexed-view",
        "partition-switch-dependent",
        "automatically managed",
        "Standalone unique",
        "Filtered index",
        "Partitioned index",
        "columnstore",
        "XML",
        "spatial",
        "hash",
        "JSON",
        "memory-optimised",
        "hypothetical",
        "Active lease, experiment, automatic-tuning action",
        "key ordinal",
        "ascending/descending direction",
        "order-insensitive set",
        "left-key-prefix",
        "Filter implication is never inferred",
        "strict coverage",
        "data space",
        "every index option",
    ):
        assert phrase.casefold() in NORMALIZED, phrase


def test_coverage_artifacts_rollback_and_owner_routing_are_required() -> None:
    for phrase in (
        "index catalog",
        "usage counters",
        "Query Store executed history",
        "constraints/dependencies",
        "policy/ownership",
        "index-review.json",
        "index-review.md",
        "create-candidates.sql",
        "consolidation-candidates.sql",
        "drop-candidates.sql",
        "rollback.sql",
        "validation.sql",
        "snapshot_id",
        "as_of_run_id",
        "evidence_id=None",
        "review_id",
        "run_id",
        "Owner routing",
        "no learning handoff was created",
        "sql-optimizer",
        "sql-plan-enforcer",
        "sql-health-triage",
        "human dba",
        "exact pre-change definition",
        "exact definition fingerprint",
        "non-overlapping observation window",
        "raw returned coverage value verbatim",
        "`incomplete`",
        "`unknown`",
        "exact size and write-burden metrics",
        "inventory is classification-free",
        "do not output lifecycle states",
    ):
        assert phrase.casefold() in NORMALIZED, phrase
    assert "evidence_ref" not in NORMALIZED
    assert "handoffv1" not in NORMALIZED
    for invented in (
        "_".join(("prior", "state", "ref")),  # noqa: FLY002
        "_".join(("classification", "ref")),  # noqa: FLY002
        "_".join(("blocker", "ref")),  # noqa: FLY002
        "_".join(("validation", "ref")),  # noqa: FLY002
        "_".join(("rollback", "ref")),  # noqa: FLY002
    ):
        assert invented not in TEXT


def test_learning_identity_is_index_scoped_and_fails_closed() -> None:
    for phrase in (
        "registered subject `index`",
        "recall_lessons(skill=sql-index-manager",
        "skill_version=1.0.0",
        "do not pass the process",
        "remote-disabled",
        "recall-only",
        "evidence_id=None",
        "portfolio tracking reference",
        "not a learning evidence",
        "future public MCP contract",
        "index evidence bridge",
        "do not call `record_decision`",
        "`review_decision`",
        "`propose_lesson`",
        "`list_learning_candidates`",
        "`create_handoff`",
        "`get_handoff`",
        "`resolve_handoff`",
        "cannot authorize capture",
        "no v1 initial result",
        "later recheck",
        "explicit human resolution",
        "outcomereviewv1",
    ):
        assert phrase.casefold() in NORMALIZED, phrase


def test_dmv_epoch_and_query_store_auto_are_explicitly_fail_closed() -> None:
    for phrase in (
        "stable physical database incarnation",
        "sqlserver_start_time",
        "full reversible definition fingerprint",
        "any reset, decrease, or change",
        "starts a new observation epoch",
        "QUERY_CAPTURE_MODE=AUTO",
        "absence under AUTO is never removal proof",
        "forces `observe`/`inconclusive` as appropriate",
    ):
        assert phrase.casefold() in NORMALIZED, phrase


def test_owner_routing_does_not_create_a_learning_handoff() -> None:
    section = TEXT[TEXT.index("## Owner routing and seven returned artifact files") :]
    section = section[: section.index("## Exact validation and rollback instructions")]
    for owner in (
        "sql-optimizer",
        "sql-plan-enforcer",
        "sql-health-triage",
        "human DBA",
    ):
        assert owner in section
    assert "without calling a learning or handoff tool" in section
    assert "Do not create, retrieve, claim, or resolve" in section


def test_only_approved_index_portfolio_tools_are_named() -> None:
    approved = {
        "capture_index_review_snapshot",
        "review_index_portfolio",
        "get_index_review",
    }
    named = set(re.findall(r"\b[a-z]+_index_[a-z_]+", TEXT))
    assert named & approved == approved
    calls = re.findall(
        r"capture_index_review_snapshot\([^)]*\)|review_index_portfolio\([^)]*\)|get_index_review\([^)]*\)",
        TEXT,
    )
    assert calls
    assert all("decision_id" not in call for call in calls)


def test_retired_index_portfolio_tool_names_are_absent_from_guide() -> None:
    retired = (
        "get_index_portfolio_" + "snapshot",
        "capture_index_portfolio_" + "snapshot",
        "classify_index_" + "portfolio",
        "recheck_index_" + "portfolio",
    )
    for name in retired:
        assert name not in TEXT
