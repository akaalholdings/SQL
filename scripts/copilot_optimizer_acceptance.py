#!/usr/bin/env python3
"""Print or validate the clean-room sql-optimizer Copilot acceptance scenario."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROMPT = """Use sql-optimizer in clean-room static mode.

Synthetic contract:
- dbo.SyntheticOrders(OrderId bigint NOT NULL, CreatedAt datetime2(7) NOT NULL)
- @TargetDate is date and is limited to 2000-01-01 through 9999-12-30 so the
  exclusive DATEADD upper bound remains representable
- preserve output names/types, duplicates, NULL behavior, and unordered row semantics
- MCP is unavailable

Query:
```sql
SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE CONVERT(date, o.CreatedAt) = @TargetDate;
```

A prior isolated index candidate was 12 percent slower and cleanup was confirmed.
Reject only that index and continue. Return:
- the outcome and explicit stopping reason;
- the semantic contract;
- at least one complete rewritten query in a fenced `sql` block labelled unmeasured;
- an experiment leaderboard containing the rewrite and losing index;
- rejected experiments, evidence gaps, and deployability status; and
- the next evidence/experiment steps.
Do not invent any other measurements.

Evidence-governed learning trace:
- after `check_runtime_status`, `list_databases`, and `check_capabilities` pass
  for the selected database, record the process `runtime_fingerprint` and
  stable `runtime_compatibility_fingerprint`, then call `recall_lessons` with
  the exact `skill`, `skill_version`, stable compatibility fingerprint, and
  supported schema/identifier fields; do not pass the process fingerprint to
  recall;
- if learning is unavailable, malformed, stale, incompatible, or
  remote-disabled, retain this static/rewrite-first behavior unchanged;
- consume evidence before each `record_decision`, including
  `subject_kind`, `subject_fingerprint`, `based_on_review_ids`, and both
  runtime fingerprints;
- pass each returned `decision_id` to the matching
  `benchmark_tuning_candidate`, `benchmark_index_candidate`, and
  `finalize_tuning_session` call;
- call `review_decision` only after the matching returned `terminal_link_id`,
  including `counterexamples` and `next_observation` before the next candidate;
  the next decision must cite the review through `based_on_review_ids`; and
- use typed `HandoffV1` `create_handoff`, `get_handoff`, and `resolve_handoff`
  for a cross-skill route without authorization. Do not install memory or
  create a local ledger.
"""

_FENCED_BLOCK = re.compile(
    r"```(?P<language>[^\r\n`]*)\r?\n(?P<body>.*?)```",
    re.DOTALL,
)
_TARGET_DATE = (
    r"(?:@targetdate|cast\s*\(\s*@targetdate\s+as\s+datetime2"
    r"(?:\s*\(\s*[0-7]\s*\))?\s*\)|convert\s*\(\s*datetime2"
    r"(?:\s*\(\s*[0-7]\s*\))?\s*,\s*@targetdate\s*\))"
)
_TARGET_DATE_LOWER_BOUND = re.compile(
    rf"(?:o\s*\.\s*)?createdat\s*>=\s*{_TARGET_DATE}",
    re.IGNORECASE,
)
_TARGET_DATE_UPPER_BOUND = re.compile(
    rf"(?:o\s*\.\s*)?createdat\s*<\s*dateadd\s*\(\s*day\s*,\s*1\s*,\s*{_TARGET_DATE}\s*\)",
    re.IGNORECASE,
)
_REWRITE_SHAPE = re.compile(
    rf"^select\s+o\s*\.\s*orderid\s*,\s*o\s*\.\s*createdat\s+"
    rf"from\s+dbo\s*\.\s*syntheticorders\s+(?:as\s+)?o\s+where\s+"
    rf"(?:"
    rf"o\s*\.\s*createdat\s*>=\s*{_TARGET_DATE}\s+and\s+"
    rf"o\s*\.\s*createdat\s*<\s*dateadd\s*\(\s*day\s*,\s*1\s*,\s*{_TARGET_DATE}\s*\)"
    rf"|"
    rf"o\s*\.\s*createdat\s*<\s*dateadd\s*\(\s*day\s*,\s*1\s*,\s*{_TARGET_DATE}\s*\)\s+and\s+"
    rf"o\s*\.\s*createdat\s*>=\s*{_TARGET_DATE}"
    rf")$",
    re.IGNORECASE,
)


def extract_sql_blocks(response: str) -> tuple[str, ...]:
    """Return only fenced SQL/TSQL blocks; prose is never treated as SQL."""

    blocks: list[str] = []
    for match in _FENCED_BLOCK.finditer(response):
        language = match.group("language").strip().casefold()
        if language in {"sql", "tsql"}:
            blocks.append(match.group("body").strip())
    return tuple(blocks)


def _prose_only(response: str) -> str:
    """Remove SQL fences while retaining claims hidden in other fences."""

    def replace_fence(match: re.Match[str]) -> str:
        language = match.group("language").strip().casefold()
        if language in {"sql", "tsql"}:
            return " "
        return f"\n{match.group('body')}\n"

    return _FENCED_BLOCK.sub(replace_fence, response)


def _labelled_section(
    response: str,
    label: str,
    *,
    stop_labels: tuple[str, ...],
) -> tuple[str, ...]:
    """Return non-empty lines under one report heading."""

    heading = re.compile(
        rf"^\s*(?:#+\s*)?{re.escape(label)}\s*:?\s*$",
        re.IGNORECASE,
    )
    stop_heading = re.compile(
        r"^\s*(?:#+\s*)?(?:"
        + "|".join(re.escape(value) for value in stop_labels)
        + r")\s*:?",
        re.IGNORECASE,
    )
    collecting = False
    lines: list[str] = []
    for line in response.splitlines():
        if not collecting:
            collecting = bool(heading.match(line))
            continue
        if stop_heading.match(line):
            break
        if line.strip():
            lines.append(line.strip())
    return tuple(lines)


def _cleanup_is_negated(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:no|not|never|without)\b[^\n.;]{0,40}\bcleanup\b|"
            r"\bcleanup\b[^\n.;]{0,40}\b(?:not\s+confirmed|"
            r"(?:was|is|has|had|wasn't|isn't|hasn't|hadn't)\s+not\s+confirmed|"
            r"unconfirmed|failed|pending|unknown)\b",
            text,
        )
    )


def _has_confirmed_cleanup(text: str) -> bool:
    return bool(
        re.search(
            r"\bcleanup\b\s*(?::|-)?\s*(?:was\s+|is\s+)?confirmed\b",
            text,
        )
    ) and not _cleanup_is_negated(text)


def _has_affirmative_index_rejection(clause: str) -> bool:
    if re.search(
        r"\b(?:no|neither)\b[^\n.;]{0,40}\bindex\b"
        r"[^\n.;]{0,30}\brejected\b|"
        r"\b(?:do|does|did|will|would|should|can|could|must)\s+not\s+reject\b|"
        r"\b(?:don't|doesn't|didn't|won't|wouldn't|shouldn't|can't|"
        r"couldn't|mustn't)\s+reject\b|"
        r"\bcannot\s+reject\b|"
        r"\bindex\b[^\n.;]{0,30}\b(?:not|never)\s+rejected\b|"
        r"\b(?:not|never)\s+rejected\b|"
        r"\b(?:unable|refus(?:e|es|ed|ing))\s+to\s+reject\b|"
        r"\bwithout\s+rejecting\b",
        clause,
    ):
        return False
    return bool(
        re.search(
            r"\breject(?:ed)?\s+(?:only\s+)?"
            r"(?:(?:that|this|the|losing|slower|regressed)\s+)?index\b|"
            r"\bindex\b[^\n.;]{0,40}\b(?:was\s+|is\s+)?rejected\b",
            clause,
        )
    )


def _has_affirmative_continuation(clause: str) -> bool:
    if re.search(
        r"\b(?:no|neither)\s+(?:further\s+)?(?:candidate|experiment|next)\b|"
        r"\bthere\s+(?:is|are|will\s+be|would\s+be)\s+no\s+next\b|"
        r"\b(?:do|does|did|will|would|should|can|could|must)\s+not\s+continue\b|"
        r"\b(?:don't|doesn't|didn't|won't|wouldn't|shouldn't|can't|"
        r"couldn't|mustn't)\s+continue\b|"
        r"\bcannot\s+continue\b|"
        r"\bnot\s+able\s+to\s+continue\b|"
        r"\b(?:unable|refus(?:e|es|ed|ing))\s+to\s+continue\b",
        clause,
    ):
        return False
    return bool(
        re.search(
            r"\bcontinue(?:s|d|ing)?\b[^\n.;]{0,60}"
            r"\b(?:statically|candidate|experiment)\b|"
            r"\bsession_continues\s*(?::|=)\s*true\b",
            clause,
        )
    )


def _is_allowed_index_measurement(line: str) -> bool:
    return (
        "index" in line
        and "rewrite" not in line
        and bool(re.search(r"\b12\s*(?:percent|%)\s+slower\b", line))
    )


def _has_invented_measurement_claim(lines: tuple[str, ...]) -> bool:
    numeric_measurement = re.compile(
        r"\b\d+(?:\.\d+)?\s*(?:ms|milliseconds?|seconds?|%|percent|"
        r"logical\s+reads?)\b"
    )
    qualitative_claim = re.compile(
        r"\b(?:improved|faster|winner|deployable|deployment-ready|"
        r"regressed|slower)\b"
    )
    for line in lines:
        allowed_index_line = _is_allowed_index_measurement(line)
        if numeric_measurement.search(line) and not allowed_index_line:
            return True
        for match in qualitative_claim.finditer(line):
            if match.group(0) in {"regressed", "slower"} and allowed_index_line:
                continue
            prefix = line[max(0, match.start() - 64) : match.start()]
            if re.search(
                r"\b(?:(?:do|does|did|is|was|are|were|can|could|would|"
                r"should|will|has|have|had)\s+not|not|never|no|cannot)"
                r"(?:\s+\w+){0,4}\s*$",
                prefix,
            ):
                continue
            return True
    return False


def _without_comments(sql: str) -> str:
    # The acceptance query grammar does not need string literals or quoted
    # identifiers. Reject them before stripping comments so comment markers
    # inside quotes can never change the statement that is validated.
    if "'" in sql or '"' in sql:
        return ""
    return re.sub(r"--[^\r\n]*|/\*.*?\*/", " ", sql, flags=re.DOTALL)


def _compact(sql: str) -> str:
    return re.sub(r"\s+", " ", _without_comments(sql).strip())


def _is_single_statement(sql: str) -> bool:
    stripped = _without_comments(sql).strip()
    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return bool(stripped) and ";" not in stripped


def is_valid_rewrite(sql: str) -> bool:
    """Validate the actual synthetic candidate, not nearby prose.

    This is deliberately a small structural/textual validator for the clean-room
    contract. It does not attempt to be a general T-SQL parser.
    """

    if not _is_single_statement(sql):
        return False
    compact = _compact(sql)
    if compact.endswith(";"):
        compact = compact[:-1].rstrip()
    if not compact.casefold().startswith("select "):
        return False
    if re.search(
        r"\b(?:distinct|top|union|group\s+by|order\s+by|insert|update|delete|drop|alter|exec)\b",
        compact,
        re.IGNORECASE,
    ):
        return False
    if re.search(r"convert\s*\(\s*date\s*,\s*o\s*\.\s*createdat\s*\)", compact, re.IGNORECASE):
        return False
    return bool(_REWRITE_SHAPE.fullmatch(compact))


def validate_response(response: str) -> list[str]:
    lowered = _prose_only(response).casefold()
    leaderboard_lines = _labelled_section(
        lowered,
        "experiment leaderboard",
        stop_labels=(
            "rejected experiments",
            "evidence gaps",
            "deployment",
            "next evidence",
            "next steps",
        ),
    )
    blocks = extract_sql_blocks(response)
    report_lines = tuple(line.strip() for line in lowered.splitlines() if line.strip())
    valid_rewrites = [block for block in blocks if is_valid_rewrite(block)]
    has_typed_date_contract = (
        bool(
            re.search(
                r"\borderid\b.{0,50}\bbigint\b.{0,30}\bnot\s+null\b",
                lowered,
                re.DOTALL,
            )
        )
        and bool(
            re.search(
                r"\bcreatedat\b.{0,50}\bdatetime2\s*\(\s*7\s*\)"
                r".{0,30}\bnot\s+null\b",
                lowered,
                re.DOTALL,
            )
        )
        and bool(
            re.search(
                r"@targetdate.{0,40}\b(?:is\s+)?date\b",
                lowered,
                re.DOTALL,
            )
        )
        and "2000-01-01" in lowered
        and "9999-12-30" in lowered
        and "upper bound" in lowered
        and any(term in lowered for term in ("representable", "overflow"))
    )
    has_static_stop = (
        "stopping reason" in lowered
        and "mcp" in lowered
        and any(term in lowered for term in ("unavailable", "static mode", "not available"))
    )
    rewrite_leaderboard_rows = tuple(
        (ordinal, line)
        for ordinal, line in enumerate(leaderboard_lines)
        if "rewrite" in line
        and "index" not in line
        and any(state in line for state in ("unmeasured", "static"))
    )
    index_leaderboard_rows = tuple(
        (ordinal, line)
        for ordinal, line in enumerate(leaderboard_lines)
        if "index" in line
        and "rewrite" not in line
        and "regressed" in line
        and bool(re.search(r"\b12\s*(?:percent|%)\s+slower\b", line))
        and _has_confirmed_cleanup(line)
    )
    has_distinct_leaderboard_rows = any(
        rewrite_ordinal != index_ordinal
        for rewrite_ordinal, _rewrite_line in rewrite_leaderboard_rows
        for index_ordinal, _index_line in index_leaderboard_rows
    )
    cleanup_is_negated = _cleanup_is_negated(lowered)
    has_confirmed_cleanup = (
        not cleanup_is_negated
        and any(_has_confirmed_cleanup(line) for line in report_lines)
    )
    action_clauses = tuple(
        clause.strip()
        for clause in re.split(r"[\n.;]+|\bbut\b|\bhowever\b", lowered)
        if clause.strip()
    )
    index_rejection_clauses = tuple(
        clause
        for clause in action_clauses
        if "index" in clause and re.search(r"\breject(?:ed|ion)?\b", clause)
    )
    has_affirmative_index_rejection = any(
        _has_affirmative_index_rejection(clause)
        for clause in index_rejection_clauses
    )
    has_rejected_index = (
        all(term in lowered for term in ("index", "regressed", "slower"))
        and has_confirmed_cleanup
        and has_affirmative_index_rejection
    )
    has_session_continuation = any(
        _has_affirmative_continuation(clause)
        for clause in action_clauses
    )
    action_pattern = re.compile(
        r"\b(?:collect|select|start|register|screen|benchmark|compare|"
        r"promote|finalize|test|try)\b"
    )
    next_step_clauses = (
        tuple(
            clause.strip()
            for clause in re.split(
                r"[\n.;]+|\bbut\b|\bhowever\b",
                lowered.split("next steps", 1)[1],
            )
            if clause.strip()
        )
        if "next steps" in lowered
        else ()
    )
    has_actionable_next_steps = any(
        action_pattern.search(clause)
        and not re.search(
            r"\b(?:do|does|did|will|would|should|can|could|must)\s+not\b|"
            r"\b(?:don't|doesn't|didn't|won't|wouldn't|shouldn't|can't|"
            r"couldn't|mustn't)\b|"
            r"\b(?:never|cannot|unable|refus(?:e|es|ed|ing))\b",
            clause,
        )
        for clause in next_step_clauses
    )
    has_session_continuation = has_session_continuation or has_actionable_next_steps
    has_unmeasured_label = any(
        "unmeasured" in block.casefold() for block in valid_rewrites
    ) or bool(
        re.search(
            r"\b(?:rewrite(?:n)?(?:\s+query|\s+candidate)?|static\s+candidate)\b"
            r"[^\n.;]{0,120}\bunmeasured\b|"
            r"\bunmeasured\b[^\n.;]{0,80}"
            r"\b(?:rewrite(?:n)?(?:\s+query|\s+candidate)?|static\s+candidate)\b",
            lowered,
        )
    )
    has_invented_measurement_claim = _has_invented_measurement_claim(report_lines)
    has_evidence_gaps = (
        any(
            phrase in lowered
            for phrase in (
                "evidence gap",
                "evidence could not be collected",
                "no mcp evidence",
            )
        )
        and "plan" in lowered
        and any(term in lowered for term in ("metric", "performance", "timing", "reads"))
    )
    checks = {
        "outcome": bool(
            re.search(
                r"(?im)^\s*(?:#+\s*)?outcome\s*:\s*"
                r"(?:static candidate|no[_ -]?change|inconclusive)\b",
                lowered,
            )
        ),
        "explicit static stopping reason": has_static_stop,
        "concrete SQL code block": bool(valid_rewrites),
        "rewritten query structure": bool(valid_rewrites),
        "unmeasured label": has_unmeasured_label,
        "no invented measurements": not has_invented_measurement_claim,
        "semantic contract": (
            "semantic contract" in lowered
            and bool(re.search(r"\bduplicates?\b|\bduplicate\s+multiplicity\b", lowered))
            and any(
                phrase in lowered
                for phrase in ("null behavior", "null semantics", "three-valued")
            )
            and any(
                phrase in lowered
                for phrase in (
                    "unordered",
                    "ordering",
                    "order semantics",
                    "result order",
                )
            )
        ),
        "typed date boundary contract": has_typed_date_contract,
        "complete static leaderboard": (
            has_distinct_leaderboard_rows
        ),
        "losing index recorded and cleaned": has_rejected_index,
        "evidence gaps": has_evidence_gaps,
        "not deployable": bool(
            re.search(
                r"(?im)^\s*(?:#+\s*)?deployment\s*:\s*"
                r"(?:none|not deployable|not deployment-ready)\b",
                lowered,
            )
        ),
        "session continues": has_session_continuation,
        "SARGable lower bound": any(
            bool(_TARGET_DATE_LOWER_BOUND.search(_compact(block)))
            for block in valid_rewrites
        ),
        "SARGable upper bound": any(
            bool(_TARGET_DATE_UPPER_BOUND.search(_compact(block)))
            for block in valid_rewrites
        ),
        "next evidence": (
            "next evidence" in lowered
            or (
                has_actionable_next_steps
                and any(
                    term in lowered.split("next steps", 1)[1]
                    for term in (
                        "evidence",
                        "plan",
                        "benchmark",
                        "query store",
                        "performance case",
                    )
                )
            )
        ),
    }
    return [name for name, passed in checks.items() if not passed]


def validate_learning_loop_response(response: str) -> list[str]:
    """Validate the ordered MCP learning trace without evaluating SQL prose."""

    lowered = " ".join(response.casefold().split())

    def ordered(*terms: str) -> bool:
        positions = [lowered.find(term.casefold()) for term in terms]
        return all(position >= 0 for position in positions) and positions == sorted(positions)

    recall_call = re.search(
        r"recall_lessons\s*\((?P<args>[^)]*"
        r"skill\s*=\s*sql-optimizer[^)]*)\)",
        lowered,
        re.DOTALL,
    )
    recall_args = recall_call.group("args") if recall_call else ""
    exact_recall = (
        recall_call is not None
        and re.search(r"skill_version\s*=\s*2\.3\.1", recall_args) is not None
        and "runtime_compatibility_fingerprint" in recall_args
        and "tool_schema_fingerprint" in recall_args
        and "sanitized_config_fingerprint" in recall_args
        and re.search(r"\bruntime_fingerprint\s*=", recall_args) is None
    )
    fallback = (
        "recall_lessons" in lowered
        and any(
            term in lowered
            for term in ("unavailable", "malformed", "stale", "incompatible", "remote-disabled")
        )
        and "unchanged" in lowered
        and any(term in lowered for term in ("static", "rewrite-first"))
    )
    decision_ids = re.findall(
        r"record_decision\s*\([^)]*\)\s*(?:->|returns?)\s*"
        r"decision_id\s*=\s*([a-z0-9][a-z0-9_:-]*[a-z0-9])(?=[\s.,;)])",
        lowered,
    )
    decision = lowered.find("record_decision")
    terminal_link = lowered.find("terminal_link_id", decision)
    review = lowered.find("review_decision", decision)

    def linked_terminal_ids(tool: str) -> list[str]:
        ids: list[str] = []
        for decision_id in decision_ids:
            match = re.search(
                rf"{tool}\s*\([^)]*decision_id\s*=\s*{re.escape(decision_id)}\b"
                rf"[^)]*\)\s*(?:->|returns?)\s*terminal_link_id\s*=\s*"
                rf"([a-z0-9][a-z0-9_:-]*[a-z0-9])(?=[\s.,;)])",
                lowered,
            )
            if match:
                ids.append(match.group(1))
        return ids

    linked_terminal_by_tool = {
        tool: linked_terminal_ids(tool)
        for tool in (
            "benchmark_tuning_candidate",
            "benchmark_index_candidate",
            "finalize_tuning_session",
        )
    }
    all_terminal_ids = [
        terminal_id
        for terminal_ids in linked_terminal_by_tool.values()
        for terminal_id in terminal_ids
    ]

    def has_linked_review(terminal_id: str) -> bool:
        terminal = lowered.find(f"terminal_link_id={terminal_id}")
        return (
            terminal >= 0
            and lowered.find("review_decision", terminal) > terminal
            and re.search(
                rf"terminal_evidence_refs\s*=\s*\[[^]]*\b{re.escape(terminal_id)}\b",
                lowered,
            )
            is not None
        )

    decision_review_order = (
        len(decision_ids) >= 3
        and lowered.find("decisionrecordv1") >= 0
        and "subject_kind" in lowered
        and "subject_fingerprint" in lowered
        and "based_on_review_ids" in lowered
        and "runtime_compatibility_fingerprint" in lowered
        and decision >= 0
        and terminal_link >= decision
        and review > terminal_link
        and lowered.find("terminal_evidence_refs", review) > review
        and lowered.find("counterexamples", review) > review
        and lowered.find("next_observation", review) > review
    )
    handoff_fields = all(
        field in lowered
        for field in (
            "source_skill",
            "target_skill",
            "evidence_refs",
            "acceptance_criteria",
            "expected_version",
        )
    )
    checks = {
        "runtime/database gate before lesson recall": ordered(
            "check_runtime_status",
            "list_databases",
            "check_capabilities",
            "recall_lessons",
        ),
        "exact recall schema": exact_recall,
        "lesson fallback unchanged": fallback,
        "evidence before decision": ordered("evidence", "record_decision"),
        "decision contract and review order": decision_review_order,
        "each material benchmark/final is linked": (
            all(linked_terminal_by_tool.values())
            and all(has_linked_review(terminal_id) for terminal_id in all_terminal_ids)
        ),
        "correction before next candidate": ordered(
            "correction",
            "counterexample",
            "next candidate",
        ),
        "typed handoff lifecycle": ordered(
            "HandoffV1",
            "create_handoff",
            "get_handoff",
            "resolve_handoff",
        )
        and handoff_fields
        and bool(
            re.search(
                r"resolve_handoff\s*\([^)]*decision_id\s*=\s*"
                r"[a-z0-9][a-z0-9._:-]*",
                lowered,
            )
        ),
        "advisory and no authorization": (
            "advisory" in lowered
            and "no authorization" in lowered
            and "cannot activate" in lowered
        ),
        "review lineage for next decision": (
            lowered.find("based_on_review_ids", review) > review
        ),
        "no local memory": "not install" in lowered and "local ledger" in lowered,
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

    response = _read_response(args.response)
    missing = validate_response(response)
    missing.extend(validate_learning_loop_response(response))
    if missing:
        for requirement in missing:
            print(f"missing acceptance requirement: {requirement}", file=sys.stderr)
        return 1
    print("Copilot sql-optimizer clean-room acceptance passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
