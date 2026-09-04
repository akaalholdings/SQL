#!/usr/bin/env python3
"""Print or validate the clean-room sql-index-manager acceptance scenario."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROMPT = """Use sql-index-manager in clean-room review mode.

Synthetic contract:
- check_runtime_status returned package_version=2.3.1,
  runtime_fingerprint=process-1,
  runtime_compatibility_fingerprint=compat-1, tool_schema_fingerprint=schema-1,
  and sanitized_config_fingerprint=config-1;
- list_databases returned one Azure SQL Database that the user selected and
  that is in the configured allowlist;
- the MCP is an operator-owned local stdio process configured for the currently
  signed-in Entra identity, with no fixed user principal name, and uses that
  identity's existing effective database permissions without creating an
  additional database user or role;
- check_capabilities returned public MCP contract 2.3.0,
  mcp_contract.index_portfolio_review=1, exactly the three approved portfolio
  tool schemas, mcp_contract.index_history_schema_version=index-history-v1,
  mcp_contract.index_history_schema_fingerprint=<returned fingerprint>,
  mcp_contract.index_review_snapshot_reuse_hours=48, allow_read=true,
  allow_index_history_write=false by default, fixed capability
  index_review_min_observation_days=90, and optional
  business_cycle_extension_days;
- review_index_portfolio returned review_id=review-index-1,
  as_of_run_id=run-index-1, overall_state=partial, and evidence_id=None; and
- no caller idempotency key was supplied; and
- recall_lessons is unavailable because learning is remote-disabled.

Return one outcome and an ordered trace. The trace must:
- gate with check_runtime_status, list_databases, user selection, and
  check_capabilities in that order, and require MCP package 2.3.1 or newer
  separately from the unchanged public contract 2.3.0;
- state the current-user Entra boundary: operator-owned local stdio, no fixed
  user principal name, existing effective database permissions, `SELECT` for
  review, `SELECT` plus `INSERT` for capture, and no additional database user or
  role. State that broader permissions leave the profile and policy as
  application-layer controls, and that a shared remote service has no
  per-caller Entra delegation;
- use only capture_index_review_snapshot(database_name, optional
  idempotency_key), review_index_portfolio(database_name, optional
  as_of_run_id, optional prior_review_id), and
  get_index_review(database_name, review_id);
- treat capture as a separate policy-gated step and bind the next review to the
  returned run. Reuse a run only when it is less than the returned 48-hour
  reuse capability; never invent an idempotency key;
- call review_index_portfolio before the exact advisory
  recall_lessons(skill=sql-index-manager, skill_version=1.0.1,
  runtime_compatibility_fingerprint, tool_schema_fingerprint,
  sanitized_config_fingerprint, database_name), without a process fingerprint;
- preserve evidence_id=None. Review, as-of-run, run, snapshot, subject, and
  artifact identifiers are portfolio tracking fields, not learning evidence
  refs;
- preserve each exact returned reason_codes array verbatim in review and
  recheck; an LLM explanation cannot replace or override it;
- keep inventory classification-free: report only returned definitions,
  protections, usage epochs, size/write-burden metrics, and coverage, without
  lifecycle states, reason_codes, candidate scripts, or recommendations;
- never call record_decision, review_decision, propose_lesson,
  list_learning_candidates, create_handoff, get_handoff, or resolve_handoff.
  V1 has no index evidence bridge or terminal link, and neither a portfolio
  result nor an explicit human resolution becomes OutcomeReviewV1;
- demonstrate recheck by retrieving the returned prior review_id, then calling
  review_index_portfolio with that prior_review_id and a fresh later
  as_of_run_id. If no fresh later run exists, use the same separately gated
  capture fallback and bind its run, otherwise remain inconclusive. Preserve
  the returned recheck classification and overall_state without treating it as
  a learning outcome; and
- route query validation to sql-optimizer, plan control to sql-plan-enforcer,
  incidents to sql-health-triage, and production changes to the external human
  DBA without creating a learning handoff.

Report inventory, review (default), and recheck; keep, create_candidate,
consolidate_candidate, drop_candidate, and observe; actionable, no_change,
partial, and inconclusive; all deterministic create, overlap, removal,
protection, specialist, coverage, blocker, rollback, and validation rules.
List exactly index-review.json, index-review.md, create-candidates.sql,
consolidation-candidates.sql, drop-candidates.sql, rollback.sql, and
validation.sql. State that the artifacts are recommend-only and no index DDL
was executed. Never describe removal as safe, approved, or applied, and never
invent names, metrics, DDL, identifiers, evidence, or hidden reasoning.
"""

_RETIRED_OPERATION_NAMES = (
    "get_index_portfolio_" + "snapshot",
    "capture_index_portfolio_" + "snapshot",
    "classify_index_" + "portfolio",
    "recheck_index_" + "portfolio",
)
_INDEX_TOOLS = (
    "capture_index_review_snapshot",
    "review_index_portfolio",
    "get_index_review",
)
_ALLOWED_TOOL_CALLS = frozenset(
    {
        "check_runtime_status",
        "list_databases",
        "check_capabilities",
        "recall_lessons",
        *_INDEX_TOOLS,
    }
)
_FORBIDDEN_LEARNING_CALLS = (
    "record_decision",
    "review_decision",
    "propose_lesson",
    "list_learning_candidates",
    "create_handoff",
    "get_handoff",
    "resolve_handoff",
)
_ARTIFACT_FILENAMES = (
    "index-review.json",
    "index-review.md",
    "create-candidates.sql",
    "consolidation-candidates.sql",
    "drop-candidates.sql",
    "rollback.sql",
    "validation.sql",
)
_REJECTED_ARTIFACT_FIELDS = (
    "_".join(("prior", "state", "ref")),  # noqa: FLY002
    "_".join(("classification", "ref")),  # noqa: FLY002
    "_".join(("blocker", "ref")),  # noqa: FLY002
    "_".join(("validation", "ref")),  # noqa: FLY002
    "_".join(("rollback", "ref")),  # noqa: FLY002
)
_REJECTED_LEARNING_FIELDS = (
    "evidence_ref",
    "consumed_evidence_refs",
    "resolution_evidence_refs",
    "terminal_evidence_refs",
    "terminal_link_id",
    "decision_id",
    "handoff_id",
)
_PORTFOLIO_ARGUMENTS = {
    "capture_index_review_snapshot": {"database_name", "idempotency_key"},
    "review_index_portfolio": {
        "database_name",
        "as_of_run_id",
        "prior_review_id",
    },
    "get_index_review": {"database_name", "review_id"},
}
_PORTFOLIO_REQUIRED_ARGUMENTS = {
    "capture_index_review_snapshot": {"database_name"},
    "review_index_portfolio": {"database_name"},
    "get_index_review": {"database_name", "review_id"},
}
_SCHEMA_SIGNATURES = {
    "capture_index_review_snapshot": "database_name, optional idempotency_key",
    "review_index_portfolio": (
        "database_name, optional as_of_run_id, optional prior_review_id"
    ),
    "get_index_review": "database_name, review_id",
}
_ARTIFACT_SUFFIX = re.compile(
    r"(?<![a-z0-9_./-])([a-z0-9][a-z0-9_.-]*\.(?:json|md|sql))(?![a-z0-9_-])"
)
_INVENTED_ARTIFACT_FIELD = re.compile(
    r"\b(?:artifact[-_]refs?|artifact[-_]references?|artifact[-_]files?|"
    r"artifact[-_]ids?)\s*=|"
    r"\b[a-z][a-z0-9_-]*(?:artifact|filename|file)[a-z0-9_-]*\s*=|"
    r"\b(?:index|create|consolidation|drop|rollback|validation)[a-z0-9_-]*"
    r"(?:artifact|file|filename)?[-_]refs?\s*="
)
_RETIRED_RULES = (
    " ".join(("at", "least", "3", "distinct", "utc", "days")),  # noqa: FLY002
    " ".join(("at", "least", "5", "total", "executions")),  # noqa: FLY002
    " ".join(("same", "90-day", "window")),  # noqa: FLY002
)


def _has_supported_runtime_package(response: str) -> bool:
    lowered = " ".join(response.casefold().split())
    match = re.search(
        r"\bcheck_runtime_status\s+returned\s+package_version\s*=\s*"
        r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\b",
        lowered,
    )
    if match is None:
        return False
    version = tuple(int(match.group(name)) for name in ("major", "minor", "patch"))
    return version >= (2, 3, 1)


def _has_current_user_entra_boundary(response: str) -> bool:
    lowered = " ".join(response.casefold().split())
    required = (
        "operator-owned local stdio",
        "currently signed-in entra identity",
        "no fixed user principal name",
        "existing effective database permissions",
        "does not create or require an additional database user or role",
        "review requires select on both history tables",
        "capture requires select and insert on both history tables",
        "broader effective permissions",
        "application-layer controls",
        "per-caller entra delegation",
        "shared remote",
    )
    contradictions = (
        r"\b(?:mcp|server|process)\b[^.]{0,80}\b(?:is|was)\s+not\s+operator-owned local stdio\b",
        r"\b(?:is|was)\s+not\s+configured\s+for\s+(?:the\s+)?currently signed-in entra identity\b",
    )
    return all(phrase in lowered for phrase in required) and not any(
        re.search(pattern, lowered) for pattern in contradictions
    )


def _ordered(response: str, *terms: str) -> bool:
    lowered = " ".join(response.casefold().split())
    positions = [lowered.find(term.casefold()) for term in terms]
    return all(position >= 0 for position in positions) and positions == sorted(positions)


def _has_outcome(response: str) -> bool:
    return sum(
        bool(re.search(rf"(?im)^\s*(?:#+\s*)?outcome\s*:\s*{state}\b", response))
        for state in ("actionable", "no_change", "partial", "inconclusive")
    ) == 1


def _portfolio_call_matches(lowered: str) -> list[re.Match[str]]:
    matches: list[re.Match[str]] = []
    for match in re.finditer(
        r"\b(?P<tool>capture_index_review_snapshot|review_index_portfolio|"
        r"get_index_review)\s*\((?P<args>[^)]*)\)",
        lowered,
    ):
        tool = match.group("tool")
        args = " ".join(match.group("args").split())
        context = lowered[max(0, match.start() - 220) : match.start()]
        if (
            args == _SCHEMA_SIGNATURES[tool]
            and re.search(r"\b(?:schema|schemas|signature|signatures)\b", context)
        ):
            continue
        matches.append(match)
    return matches


def _argument_names(args: str) -> list[str]:
    return re.findall(r"(?<![a-z0-9_])([a-z][a-z0-9_]*)\s*=", args)


def _argument_value(args: str, name: str) -> str | None:
    match = re.search(rf"(?<![a-z0-9_]){name}\s*=\s*([^,)\s]+)", args)
    return match.group(1).strip() if match else None


def _has_exact_portfolio_arguments(lowered: str) -> bool:
    calls = _portfolio_call_matches(lowered)
    if not calls:
        return False
    for call in calls:
        tool = call.group("tool")
        args = call.group("args")
        names = _argument_names(args)
        parts = [part.strip() for part in args.split(",") if part.strip()]
        if len(names) != len(set(names)) or any("=" not in part for part in parts):
            return False
        if set(names) - _PORTFOLIO_ARGUMENTS[tool]:
            return False
        if _PORTFOLIO_REQUIRED_ARGUMENTS[tool] - set(names):
            return False
    return True


def _has_only_approved_calls(lowered: str) -> bool:
    calls = set(re.findall(r"\b([a-z][a-z0-9_]*)\s*\(", lowered))
    return _has_exact_portfolio_arguments(lowered) and calls <= _ALLOWED_TOOL_CALLS


def _has_forbidden_tool_use_claim(response: str) -> bool:
    patterns = (
        re.compile(
            r"\btool\s*:\s*(?P<tool>[a-z][a-z0-9_]+)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:called|invoked|used|ran|executed)\s+(?:the\s+)?"
            r"(?P<tool>[a-z][a-z0-9_]+)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?P<tool>[a-z][a-z0-9_]+)\b[^.\n]{0,50}\b"
            r"(?:was\s+)?(?:called|invoked|used|ran|executed|succeeded)\b",
            re.IGNORECASE,
        ),
    )
    for pattern in patterns:
        for match in pattern.finditer(response):
            tool = match.group("tool").casefold()
            if (
                "_" in tool
                and tool not in _ALLOWED_TOOL_CALLS
                and not _is_negated(response, match.start())
            ):
                return True
    return False


def _has_exact_recall(lowered: str) -> bool:
    calls = list(re.finditer(r"\brecall_lessons\s*\((?P<args>[^)]*)\)", lowered))
    if len(calls) != 1:
        return False
    args = calls[0].group("args")
    names = _argument_names(args)
    required = {
        "skill",
        "skill_version",
        "runtime_compatibility_fingerprint",
        "tool_schema_fingerprint",
        "sanitized_config_fingerprint",
        "database_name",
    }
    return (
        required <= set(names)
        and set(names) <= required | {"tags"}
        and len(names) == len(set(names))
        and _argument_value(args, "skill") == "sql-index-manager"
        and _argument_value(args, "skill_version") == "1.0.1"
        and "runtime_fingerprint" not in names
    )


def _returned_fields(lowered: str, call: re.Match[str]) -> str:
    next_call = re.search(r"\b[a-z][a-z0-9_]*\s*\(", lowered[call.end() :])
    end = call.end() + next_call.start() if next_call else min(len(lowered), call.end() + 800)
    return lowered[call.end() : end]


def _has_real_review_fields(lowered: str) -> bool:
    reviews = [
        call
        for call in _portfolio_call_matches(lowered)
        if call.group("tool") == "review_index_portfolio"
    ]
    if len(reviews) < 2:
        return False
    for review in reviews:
        returned = _returned_fields(lowered, review)
        if not all(
            re.search(rf"\b{name}\s*=\s*[^,.\s]+", returned)
            for name in ("review_id", "as_of_run_id", "overall_state")
        ):
            return False
        if re.search(r"\bevidence_id\s*=\s*none\b", returned) is None:
            return False
    return True


def _has_recheck_lineage(lowered: str) -> bool:
    calls = _portfolio_call_matches(lowered)
    reviews = [
        call for call in calls if call.group("tool") == "review_index_portfolio"
    ]
    gets = [call for call in calls if call.group("tool") == "get_index_review"]
    initial = next(
        (
            call
            for call in reviews
            if "prior_review_id" not in _argument_names(call.group("args"))
        ),
        None,
    )
    later = next(
        (
            call
            for call in reviews
            if "prior_review_id" in _argument_names(call.group("args"))
        ),
        None,
    )
    if initial is None or later is None or len(gets) != 1:
        return False
    get = gets[0]
    initial_id = _argument_value(_returned_fields(lowered, initial), "review_id")
    return bool(
        initial.start() < get.start() < later.start()
        and initial_id
        and _argument_value(get.group("args"), "review_id") == initial_id
        and _argument_value(later.group("args"), "prior_review_id") == initial_id
        and _argument_value(later.group("args"), "as_of_run_id")
        and "later non-overlapping" in lowered[get.end() : later.start()]
        and "preserve" in lowered[later.end() :]
        and "classification" in lowered[later.end() :]
    )


def _has_capture_policy_gate(lowered: str) -> bool:
    calls = _portfolio_call_matches(lowered)
    captures = [
        call
        for call in calls
        if call.group("tool") == "capture_index_review_snapshot"
    ]
    if not captures:
        return True
    if len(captures) != 1:
        return False
    capture = captures[0]
    before = lowered[: capture.start()]

    verified_policy = re.search(
        r"(?:^|[.!?]\s+)the selected database returned allow_read=true and "
        r"allow_index_history_write=true; both gates were verified\.\s*$",
        before,
    )
    if verified_policy is None:
        return False
    policy_context = before[max(0, verified_policy.start() - 120) :]
    if re.search(r"\b(?:hypothetical|hypothetically|conditional|negated)\b", policy_context):
        return False
    preceding_policy = before[max(0, verified_policy.start() - 200) : verified_policy.start()]
    if re.search(
        r"\b(?:policy|gate|gates|allow_read|allow_index_history_write)\b"
        r"[^.]{0,100}\b(?:not\s+verified|unverified|failed|denied|false|unknown)\b",
        preceding_policy,
    ):
        return False
    following = [call for call in calls if call.start() > capture.end()]
    if not following or following[0].group("tool") != "review_index_portfolio":
        return False
    run_id = _argument_value(_returned_fields(lowered, capture), "run_id")
    return bool(
        run_id
        and _argument_value(following[0].group("args"), "as_of_run_id") == run_id
    )


def _has_snapshot_reuse_gate(lowered: str) -> bool:
    if "mcp_contract.index_review_snapshot_reuse_hours=48" not in lowered:
        return False
    calls = _portfolio_call_matches(lowered)
    if any(call.group("tool") == "capture_index_review_snapshot" for call in calls):
        return True
    first_review = next(
        (
            call
            for call in calls
            if call.group("tool") == "review_index_portfolio"
        ),
        None,
    )
    if first_review is None:
        return False
    return (
        re.search(
            r"(?:^|[.!?]\s+)the returned run was less than 48 hours old and "
            r"was reused\.\s*$",
            lowered[: first_review.start()],
        )
        is not None
    )


def _has_idempotency_provenance(lowered: str) -> bool:
    if "no caller idempotency key was supplied" not in lowered:
        return False
    captures = [
        call
        for call in _portfolio_call_matches(lowered)
        if call.group("tool") == "capture_index_review_snapshot"
    ]
    return all(
        "idempotency_key" not in _argument_names(call.group("args"))
        for call in captures
    ) and "no new key was generated after any uncertain response" in lowered


def _has_recheck_freshness_and_fallback(lowered: str) -> bool:
    required = (
        "the returned later run was less than 48 hours old and was reused",
        (
            "when no fresh later run exists, recheck uses the same verified "
            "two-gate separate capture fallback and binds the returned run"
        ),
        "otherwise it remains inconclusive",
    )
    return all(phrase in lowered for phrase in required)


def _has_recall_only_boundary(lowered: str) -> bool:
    if any(
        re.search(rf"\b{tool}\s*\(", lowered)
        for tool in _FORBIDDEN_LEARNING_CALLS
    ):
        return False
    if any(re.search(rf"\b{field}\s*=", lowered) for field in _REJECTED_LEARNING_FIELDS):
        return False
    non_null_evidence = re.search(r"\bevidence_id\s*=\s*(?!none\b)[^,.\s]+", lowered)
    return (
        non_null_evidence is None
        and "evidence_id=none" in lowered
        and "no index evidence bridge" in lowered
        and "no terminal link" in lowered
        and "portfolio tracking" in lowered
        and "not learning evidence refs" in lowered
        and "no v1 portfolio result or human resolution becomes outcomereviewv1"
        in lowered
    )


def _has_affirmative_learning_violation(response: str) -> bool:
    patterns = (
        re.compile(
            r"\b(?:called|invoked|used|ran)\s+(?:the\s+)?(?:"
            + "|".join(_FORBIDDEN_LEARNING_CALLS)
            + r")\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:"
            + "|".join(_FORBIDDEN_LEARNING_CALLS)
            + r")\b[^.\n]{0,50}\b(?:was\s+)?(?:called|invoked|used|ran)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:learning|typed)\s+handoff\b[^.\n]{0,80}"
            r"\b(?:was\s+)?(?:created|opened|resolved|returned)|"
            r"\b(?:learning|typed)\s+handoff\s+exists\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:human resolution|portfolio result|recheck)\b[^.\n]{0,120}"
            r"\boutcomereviewv1\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:"
            + "|".join(_FORBIDDEN_LEARNING_CALLS)
            + r")\b[^.\n]{0,50}\b(?:succeeded|completed|recorded|wrote|created|resolved)\b",
            re.IGNORECASE,
        ),
    )
    return any(
        not _is_negated(response, match.start())
        for pattern in patterns
        for match in pattern.finditer(response)
    )


def _has_review_schemas(lowered: str) -> bool:
    return all(
        phrase in lowered
        for phrase in (
            "capture_index_review_snapshot(database_name",
            "review_index_portfolio(database_name",
            "get_index_review(database_name",
            "optional idempotency_key",
            "optional as_of_run_id",
            "optional prior_review_id",
        )
    )


def _has_exact_artifacts(lowered: str) -> bool:
    filenames = set(_ARTIFACT_SUFFIX.findall(lowered))
    return (
        filenames == set(_ARTIFACT_FILENAMES)
        and "returned artifact filenames are exactly these seven" in lowered
        and "portfolio tracking identifiers, not artifacts" in lowered
        and not any(field in lowered for field in _REJECTED_ARTIFACT_FIELDS)
        and _INVENTED_ARTIFACT_FIELD.search(lowered) is None
    )


def _has_no_invented_thresholds(lowered: str) -> bool:
    for match in re.finditer(
        r"\b(?P<number>\d+)\s*(?:distinct\s+)?(?:-\s*)?days?\b",
        lowered,
    ):
        if match.group("number") != "90":
            return False
    return not any(rule in lowered for rule in _RETIRED_RULES)


def _is_negated(response: str, start: int) -> bool:
    line_start = response.rfind("\n", 0, start) + 1
    sentence_start = max(
        line_start,
        response.rfind(".", 0, start) + 1,
        response.rfind(";", 0, start) + 1,
    )
    return re.search(
        r"\b(?:no|not|never|without|does\s+not|do\s+not|did\s+not|"
        r"must\s+not|cannot|forbidden|out\s+of\s+scope)\b",
        response[sentence_start : start + 120],
        re.IGNORECASE,
    ) is not None


def _has_forbidden_mutation_or_watcher_claim(response: str) -> bool:
    ddl = re.compile(r"\b(?:create|drop|alter)\s+index\b", re.IGNORECASE)
    for line in response.splitlines():
        match = ddl.search(line)
        if not match:
            continue
        if re.match(r"^\s*(?:create|drop|alter)\s+index\b", line, re.IGNORECASE):
            return True
        inert = bool(re.search(r"\b(?:commented|inert|non[- ]executable|example)\b", line, re.IGNORECASE))
        if not inert or not _is_negated(response, match.start() + response.find(line)):
            return True

    watcher = re.compile(
        r"\bdatabase\s+watcher\b[^.\n]{0,100}\b(?:collect(?:ed|ion|ing)?|"
        r"integrat(?:e|ed|ion)|used|source|enabled|invoked|"
        r"provid(?:e|es|ed|ing)\s+evidence|"
        r"suppli(?:ed|es|ying)\s+evidence|"
        r"furnish(?:ed|es|ing)\s+evidence|evidence)\b",
        re.IGNORECASE,
    )
    if any(
        not _is_negated(response, match.start())
        for match in watcher.finditer(response)
    ):
        return True

    applied = re.compile(
        r"\b(?:drop_candidate|create_candidate|consolidate_candidate|"
        r"index(?:\s+(?:removal|change|ddl))?)\b[^.\n]{0,80}\b"
        r"(?:(?:is|was|were|has\s+been|had\s+been)\s+)?"
        r"(?:safe|approved|authori[sz]ed|applied|executed|removed|created|"
        r"altered|rebuilt|disabled|enabled)\b",
        re.IGNORECASE,
    )
    return any(
        not _is_negated(response, match.start())
        for match in applied.finditer(response)
    )


def _has_inventory_or_reason_code_contradiction(response: str) -> bool:
    patterns = (
        re.compile(
            r"\bllm\b[^.\n]{0,60}\b(?:overrides?|rewrites?|replaces?|changes?)\b"
            r"[^.\n]{0,60}\b(?:reason_codes|state|gate)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\breason_codes\b[^.\n]{0,60}\b"
            r"(?:overridden|rewritten|replaced|changed|summari[sz]ed)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\binventory\b[^.\n]{0,80}\b"
            r"(?:includes?|contains?|outputs?|reports?)\b[^.\n]{0,80}\b"
            r"(?:lifecycle states?|reason_codes|candidate scripts?|recommendations?)\b",
            re.IGNORECASE,
        ),
    )
    return any(
        not _is_negated(response, match.start())
        for pattern in patterns
        for match in pattern.finditer(response)
    )


def validate_response(response: str) -> list[str]:
    lowered = " ".join(response.casefold().split())
    call_matches = _portfolio_call_matches(lowered)
    first_review = next(
        (
            call
            for call in call_matches
            if call.group("tool") == "review_index_portfolio"
        ),
        None,
    )
    gate_at = lowered.find("check_capabilities")
    review_at = first_review.start() if first_review else -1
    recall_at = lowered.find("recall_lessons")
    checks = {
        "exactly one outcome": _has_outcome(response),
        "mode and default": all(
            phrase in lowered
            for phrase in ("inventory", "review mode", "default", "recheck")
        ),
        "runtime/database gate": _ordered(
            response,
            "check_runtime_status",
            "list_databases",
            "user selected",
            "check_capabilities",
        ),
        "allowlist and contract gate": (
            "allowlist" in lowered
            and "mcp_contract.index_portfolio_review=1" in lowered
            and "mcp_contract.index_history_schema_version=index-history-v1"
            in lowered
            and "mcp_contract.index_review_snapshot_reuse_hours=48" in lowered
            and re.search(
                r"\bmcp_contract\.index_history_schema_fingerprint="
                r"[a-z0-9][a-z0-9_-]*\b",
                lowered,
            )
            is not None
            and "restricted" in lowered
            and "public mcp contract version 2.3.0" in lowered
        ),
        "current-user Entra existing-permission boundary": (
            _has_current_user_entra_boundary(response)
        ),
        "MCP 2.3.1 package gate": (
            _has_supported_runtime_package(response)
            and "mcp package 2.3.1 or newer" in lowered
            and "unchanged public contract 2.3.0" in lowered
        ),
        "peer runtime fingerprints": all(
            phrase in lowered
            for phrase in (
                "runtime_fingerprint",
                "runtime_compatibility_fingerprint",
                "tool_schema_fingerprint",
                "sanitized_config_fingerprint",
            )
        ),
        "policy defaults and observation floor": (
            "allow_index_history_write=false" in lowered
            and "default" in lowered
            and "allow_read=true" in lowered
            and "index_review_min_observation_days=90" in lowered
            and "fixed capability value" in lowered
            and "not a policy key" in lowered
            and "business_cycle_extension_days" in lowered
        ),
        "exact index schemas": _has_review_schemas(lowered),
        "capture policy gate": _has_capture_policy_gate(lowered),
        "snapshot freshness gate": _has_snapshot_reuse_gate(lowered),
        "idempotency key provenance": _has_idempotency_provenance(lowered),
        "recheck freshness and capture fallback": _has_recheck_freshness_and_fallback(
            lowered
        ),
        "only approved index calls": (
            _has_only_approved_calls(lowered)
            and not _has_forbidden_tool_use_claim(response)
        ),
        "real V1 review fields": _has_real_review_fields(lowered),
        "no invented fingerprints": re.search(
            r"(?<!tool_)(?<!index_history_)schema_fingerprint\s*=|"
            r"\bcatalog_fingerprint\s*=",
            lowered,
        )
        is None,
        "exact recall schema": _has_exact_recall(lowered),
        "runtime/database gate before learning": (
            gate_at >= 0 and review_at > gate_at and recall_at > review_at
        ),
        "learning fallback": all(
            phrase in lowered
            for phrase in (
                "unavailable",
                "remote-disabled",
                "unchanged",
                "advisory",
                "local ledger",
                "installed memory",
            )
        ),
        "recall-only V1 learning boundary": (
            _has_recall_only_boundary(lowered)
            and not _has_affirmative_learning_violation(response)
        ),
        "inventory is classification-free": all(
            phrase in lowered
            for phrase in (
                "inventory is classification-free",
                "definitions, protections, usage epochs, size/write-burden metrics, and coverage only",
                "inventory omits lifecycle states, reason_codes, candidate scripts, and recommendations",
            )
        ),
        "exact returned reason codes": (
            "preserve each exact returned reason_codes array verbatim" in lowered
            and "llm explanation does not replace or override it" in lowered
        ),
        "no inventory or reason-code contradiction": (
            not _has_inventory_or_reason_code_contradiction(response)
        ),
        "raw coverage vocabulary": all(
            phrase in lowered
            for phrase in (
                "raw returned coverage values are preserved verbatim",
                "incomplete and unknown are not remapped",
            )
        ),
        "recheck uses prior review": _has_recheck_lineage(lowered),
        "no executable index DDL or Database Watcher claim": not _has_forbidden_mutation_or_watcher_claim(
            response
        ),
        "subject states": all(
            state in lowered
            for state in (
                "keep",
                "create_candidate",
                "consolidate_candidate",
                "drop_candidate",
                "observe",
            )
        ),
        "five classifications and four outcomes": (
            "five portfolio classifications" in lowered
            and "four overall outcomes" in lowered
            and all(
                state in lowered
                for state in (
                    "actionable",
                    "no_change",
                    "partial",
                    "inconclusive",
                )
            )
        ),
        "portfolio gates and overlap": all(
            phrase in lowered
            for phrase in (
                "protection, a valid read delta, or any executed query store plan reference",
                "same exact recurring request across at least two runtime intervals",
                "material positive existing mcp score",
                "complete query store coverage",
                "no exact or covering index",
                "projected storage strictly below 90 percent",
                "missing-index dmv-only evidence remains observe",
                "exact duplicate or strict coverage",
                "overlap_relation=exact_duplicate",
                "reason_codes=[exact_duplicate_definition]",
                "overlap_relation=strict_coverage",
                "reason_codes=[strict_coverage_overlap]",
                "key order/direction",
                "includes",
                "uniqueness",
                "filter",
                "type",
                "partition/data space",
                "compression",
                "options",
                "full drop gate",
                "enabled user-created nonunique standalone type-2 rowstore",
                "at least 90 continuous usable days plus a business-cycle extension",
                "persisted daily history with no gap over 48 hours",
                "same database/engine/counter epoch",
                "stable definition",
                "counters never decrease",
                "zero seek/scan/lookup deltas",
                "measurable write or storage cost",
                "complete query store/hint/dependency/protection coverage",
                "no references/hints/dependencies/protections",
                "no_stored_plan_without_execution",
                "retained unexecuted stored-plan reference",
                "blocks removal and produces observe",
                "first-run removal lead",
                "first-run status does not suppress a valid create_candidate",
                "resets/failovers/gaps/insufficient duration",
                "conflicts",
                "query store gaps",
                "specialist types",
                "coverage",
                "blockers",
                "post-classification execution-readiness checks do not rewrite the mcp state",
            )
        ),
        "protected and specialist defaults": all(
            phrase in lowered
            for phrase in (
                "primary-key",
                "unique-constraint",
                "clustered",
                "foreign-key-supporting",
                "indexed-view",
                "hinted",
                "partition-switch-dependent",
                "automatically managed",
                "standalone unique",
                "filtered",
                "partitioned",
                "columnstore",
                "disabled",
                "xml",
                "spatial",
                "hash",
                "json",
                "memory-optimised",
                "hypothetical",
                "specialist type",
                "never removal candidates",
                "observe unless stronger keep evidence exists",
            )
        ),
        "exact returned artifact filenames": _has_exact_artifacts(lowered),
        "owner routing without learning handoff": (
            all(
                target in lowered
                for target in (
                    "sql-optimizer",
                    "sql-plan-enforcer",
                    "sql-health-triage",
                    "human dba",
                )
            )
            and "external change control" in lowered
            and "no learning handoff was created" in lowered
        ),
        "recommend-only rollback": all(
            phrase in lowered
            for phrase in (
                "recommend-only",
                "no index ddl was executed",
                "validation.sql",
                "rollback.sql",
                "later non-overlapping",
                "human resolution",
            )
        )
        and " ".join(("safe", "to", "drop")) not in lowered,  # noqa: FLY002
        "no retired portfolio surface": not any(
            name in lowered for name in _RETIRED_OPERATION_NAMES
        ),
        "no retired thresholds or invented fields": (
            _has_no_invented_thresholds(lowered)
            and not any(field in lowered for field in _REJECTED_ARTIFACT_FIELDS)
            and _INVENTED_ARTIFACT_FIELD.search(lowered) is None
        ),
    }
    return [name for name, passed in checks.items() if not passed]


def _read_response(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).expanduser().read_text(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--print-prompt", action="store_true")
    mode.add_argument("--response", help="Response file, or - for stdin.")
    args = parser.parse_args(argv)
    if args.print_prompt:
        print(PROMPT.rstrip())
        return 0
    missing = validate_response(_read_response(args.response))
    if missing:
        for requirement in missing:
            print(f"missing acceptance requirement: {requirement}", file=sys.stderr)
        return 1
    print("Copilot sql-index-manager clean-room acceptance passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
