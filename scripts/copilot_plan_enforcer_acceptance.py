#!/usr/bin/env python3
"""Print or validate the clean-room sql-plan-enforcer acceptance scenario."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROMPT = """Use sql-plan-enforcer in clean-room enforcer-review mode.

Synthetic contract:
- check_runtime_status returned a process runtime_fingerprint, a stable
  runtime_compatibility_fingerprint, tool_schema_fingerprint, and
  sanitized_config_fingerprint values;
- list_databases returned the one selected allowlisted database;
- check_capabilities returned the enforcer-review policy envelope; and
- recall_lessons is unavailable because learning is remote-disabled.

One redacted terminal evidence reference is supplied: terminal-plan-review-1.
Do not invent identities, timings, credentials, SQL, parameter values, result
rows, or evidence.

Return a review-only report with an ordered trace that:
- calls check_runtime_status, list_databases, and check_capabilities before
  recall_lessons, using exact skill=sql-plan-enforcer, skill_version=1.0.0,
  the stable runtime_compatibility_fingerprint, and only supported
  fingerprint/identifier fields; do not pass the process runtime_fingerprint
  to recall;
- falls back unchanged to review-only behavior when recall is unavailable,
  malformed, stale, incompatible, or remote-disabled;
- consumes terminal-plan-review-1 before record_decision, including
  subject_kind, subject_fingerprint, based_on_review_ids, and both runtime
  fingerprints; records a control/hold before prepare_plan_action;
- leaves prepare_plan_action unlinked by the approved schema, but passes the
  returned decision_id to verify_plan_action, rollback_plan_action, and a
  resolved resolve_handoff terminal action;
- calls review_decision only after the matching terminal_link_id, using that
  returned link in terminal_evidence_refs and an OutcomeReviewV1
  counterexamples/next_observation pair before the next hypothesis; the next
  decision cites the review through based_on_review_ids;
- uses typed HandoffV1 create_handoff, get_handoff, and resolve_handoff with
  source_skill, target_skill, evidence_refs, acceptance_criteria, and
  expected_version, without authorization; and
- does not call force_query_plan, apply_plan_action, or
  apply_prepared_plan_action. Do not invent unsupported fields.

Do not create a local ledger, install memory, or expose hidden reasoning.
"""


def _ordered(response: str, *terms: str) -> bool:
    lowered = " ".join(response.casefold().split())
    positions = [lowered.find(term.casefold()) for term in terms]
    return all(position >= 0 for position in positions) and positions == sorted(positions)


def _called_mutation(response: str) -> bool:
    mutation = re.compile(
        r"\b(?:force_query_plan|apply_plan_action|apply_prepared_plan_action)\b",
        re.IGNORECASE,
    )
    for line in response.splitlines():
        for match in mutation.finditer(line):
            prefix = line[: match.start()]
            if re.search(
                r"\b(?:do|did|will|would|should|must|can)\s+not\s+"
                r"(?:call|invoke|execute|apply)\b|"
                r"\bnever\s+(?:call|invoke|execute|apply)\b|"
                r"\bwithout\s+(?:calling|invoking|executing|applying)\b",
                prefix,
                re.IGNORECASE,
            ):
                continue
            if re.search(r"\b(?:called|invoked|executed|applied)\b", prefix, re.IGNORECASE):
                return True
    return False


def validate_response(response: str) -> list[str]:
    lowered = " ".join(response.casefold().split())
    recall_call = re.search(
        r"recall_lessons\s*\((?P<args>[^)]*"
        r"skill\s*=\s*sql-plan-enforcer[^)]*)\)",
        lowered,
        re.DOTALL,
    )
    recall_args = recall_call.group("args") if recall_call else ""
    exact_recall = (
        recall_call is not None
        and re.search(r"skill_version\s*=\s*1\.0\.0", recall_args) is not None
        and "runtime_compatibility_fingerprint" in recall_args
        and "tool_schema_fingerprint" in recall_args
        and "sanitized_config_fingerprint" in recall_args
        and re.search(r"\bruntime_fingerprint\s*=", recall_args) is None
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
    decision_ids = re.findall(
        r"record_decision\s*\([^)]*\)\s*(?:->|returns?)\s*"
        r"decision_id\s*=\s*([a-z0-9][a-z0-9_:-]*[a-z0-9])(?=[\s.,;)])",
        lowered,
    )
    decision = lowered.find("record_decision")
    terminal_evidence = lowered.find("terminal evidence", decision)
    review = lowered.find("review_decision", decision)

    def has_linked_terminal(tool: str) -> bool:
        return any(
            re.search(
                rf"{tool}\s*\([^)]*decision_id\s*=\s*{re.escape(decision_id)}\b"
                rf"[^)]*\)\s*(?:->|returns?)\s*terminal_link_id\s*=\s*"
                rf"[a-z0-9][a-z0-9_:-]*[a-z0-9](?=[\s.,;)])",
                lowered,
            )
            for decision_id in decision_ids
        )

    terminal_ids = re.findall(
        r"(?:verify_plan_action|rollback_plan_action|resolve_handoff)\s*"
        r"\([^)]*decision_id\s*=\s*[a-z0-9][a-z0-9._:-]*[^)]*\)\s*"
        r"(?:->|returns?)\s*terminal_link_id\s*=\s*"
        r"([a-z0-9][a-z0-9_:-]*[a-z0-9])(?=[\s.,;)])",
        lowered,
    )
    linked_review = any(
        lowered.find("review_decision", lowered.find(f"terminal_link_id={terminal_id}"))
        > lowered.find(f"terminal_link_id={terminal_id}")
        and re.search(
            rf"terminal_evidence_refs\s*=\s*\[[^]]*\b{re.escape(terminal_id)}\b",
            lowered,
        )
        for terminal_id in terminal_ids
    )
    decision_review_order = (
        lowered.find("decisionrecordv1") >= 0
        and decision >= 0
        and len(decision_ids) >= 1
        and all(
            field in lowered
            for field in (
                "subject_kind",
                "subject_fingerprint",
                "based_on_review_ids",
                "runtime_compatibility_fingerprint",
            )
        )
        and terminal_evidence >= decision
        and review > terminal_evidence
        and lowered.find("terminal_evidence_refs", review) > review
        and lowered.find("outcomereviewv1", review) > review
        and lowered.find("counterexamples", review) > review
        and lowered.find("next_observation", review) > review
        and linked_review
    )
    checks = {
        "review-only mode": "review-only" in lowered and "no authorization" in lowered,
        "runtime/database gate before lesson recall": _ordered(
            response,
            "check_runtime_status",
            "list_databases",
            "check_capabilities",
            "recall_lessons",
        ),
        "exact recall schema": exact_recall,
        "lesson fallback unchanged": (
            "recall_lessons" in lowered
            and "remote-disabled" in lowered
            and "unavailable" in lowered
            and "unchanged" in lowered
            and "review-only" in lowered
        ),
        "evidence before decision": _ordered(
            response, "terminal-plan-review-1", "record_decision"
        ),
        "prepare remains unlinked": (
            "prepare_plan_action" in lowered
            and "unlinked" in lowered
        ),
        "plan terminal decision links": (
            has_linked_terminal("verify_plan_action")
            and has_linked_terminal("rollback_plan_action")
            and has_linked_terminal("resolve_handoff")
        ),
        "decision contract and terminal-only review": decision_review_order,
        "correction before next hypothesis": _ordered(
            response, "correction", "counterexample", "next hypothesis"
        ),
        "typed handoff lifecycle": _ordered(
            response,
            "HandoffV1",
            "create_handoff",
            "get_handoff",
            "resolve_handoff",
        )
        and handoff_fields,
        "advisory and no authorization": (
            "advisory" in lowered
            and "no authorization" in lowered
            and "cannot activate" in lowered
        ),
        "no local memory": (
            "local ledger" in lowered
            and "install memory" in lowered
        ),
        "no mutation call": not _called_mutation(response),
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
    print("Copilot sql-plan-enforcer clean-room acceptance passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
