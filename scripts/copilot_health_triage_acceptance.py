#!/usr/bin/env python3
"""Print or validate the clean-room sql-health-triage acceptance scenario."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROMPT = """Use sql-health-triage in clean-room read-only mode.

Synthetic contract:
- check_runtime_status returned a process runtime_fingerprint, a stable
  runtime_compatibility_fingerprint, tool_schema_fingerprint, and
  sanitized_config_fingerprint values;
- list_databases returned the one database selected by the user;
- check_capabilities returned the read-only triage capability envelope; and
- recall_lessons is unavailable because learning is remote-disabled.

The only evidence reference supplied for this synthetic trace is
evidence-health-1. Do not invent a database name, query, metric, credential,
parameter value, result row, or evidence reference.

Return exactly one triage outcome and an ordered trace that:
- calls check_runtime_status, list_databases, and check_capabilities before
  recall_lessons, using exact skill=sql-health-triage, skill_version=1.0.1,
  the stable runtime_compatibility_fingerprint, and only supported
  fingerprint/identifier fields; do not pass the process runtime_fingerprint
  to recall;
- falls back unchanged to the existing read-only behavior when recall is
  unavailable, malformed, stale, incompatible, or remote-disabled;
- consumes evidence-health-1 before record_decision, including
  subject_kind, subject_fingerprint, based_on_review_ids, and both runtime
  fingerprints; passes the returned decision_id to a supported later
  analyze_db_health, collect_performance_evidence, or resolved resolve_handoff
  terminal action; and
- calls review_decision only after that action returns terminal_link_id, using
  the returned link in terminal_evidence_refs and an OutcomeReviewV1
  counterexamples/next_observation pair before the next hypothesis; the next
  decision cites the review through based_on_review_ids; and
- uses typed HandoffV1 create_handoff, get_handoff, and resolve_handoff with
  source_skill, target_skill, evidence_refs, acceptance_criteria, and
  expected_version, without granting authorization.

Do not create a local ledger, install memory, or expose hidden reasoning.
"""


def _ordered(response: str, *terms: str) -> bool:
    lowered = " ".join(response.casefold().split())
    positions = [lowered.find(term.casefold()) for term in terms]
    return all(position >= 0 for position in positions) and positions == sorted(positions)


def validate_response(response: str) -> list[str]:
    lowered = " ".join(response.casefold().split())
    outcomes = sum(
        bool(
            re.search(
                rf"(?im)^\s*(?:#+\s*)?outcome\s*:\s*{state}\b", response
            )
        )
        for state in ("healthy", "actionable", "partial", "inconclusive")
    )
    recall_call = re.search(
        r"recall_lessons\s*\((?P<args>[^)]*"
        r"skill\s*=\s*sql-health-triage[^)]*)\)",
        lowered,
        re.DOTALL,
    )
    recall_args = recall_call.group("args") if recall_call else ""
    exact_recall = (
        recall_call is not None
        and re.search(r"skill_version\s*=\s*1\.0\.1", recall_args) is not None
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

    def linked_terminal_ids(decision_id: str) -> list[str]:
        ids: list[str] = []
        match = re.search(
            rf"(?:analyze_db_health|collect_performance_evidence|resolve_handoff)"
            rf"\s*\([^)]*decision_id\s*=\s*{re.escape(decision_id)}\b[^)]*\)"
            rf"\s*(?:->|returns?)\s*terminal_link_id\s*=\s*"
            rf"([a-z0-9][a-z0-9_:-]*[a-z0-9])(?=[\s.,;)])",
            lowered,
        )
        if match:
            ids.append(match.group(1))
        return ids

    terminal_ids = linked_terminal_ids(decision_ids[0]) if decision_ids else []
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
        "exactly one outcome": outcomes == 1,
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
            and "read-only" in lowered
        ),
        "evidence before decision": _ordered(
            response, "evidence-health-1", "record_decision"
        ),
        "health terminal decision link": bool(terminal_ids),
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
        "no local memory": (
            "local ledger" in lowered
            and "install memory" in lowered
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
    print("Copilot sql-health-triage clean-room acceptance passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
