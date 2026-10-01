#!/usr/bin/env python3
"""Print or validate the clean-room sql-index-manager acceptance scenario."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROMPT = """Use sql-index-manager on a synthetic database. Do not connect to anything.

Synthetic tool results:
- check_runtime_status returned package_version=2.4.0, a tool list that contains
  review_workload_indexes, runtime_fingerprint=process-1,
  runtime_compatibility_fingerprint=compat-1, tool_schema_fingerprint=schema-1,
  and sanitized_config_fingerprint=config-1.
- list_databases returned one allowlisted database, appdb, and the user selected it.
- check_capabilities returned mcp_contract.workload_index_advisor=1.
- review_workload_indexes(database_name=appdb, lookback_days=14, objective=cpu)
  returned result_status=ok, queries_analyzed=100, analyzed_share_pct=86.2,
  usage_counters.days_since_reset=41, and gaps:
  "Query Store capture mode AUTO skips infrequent queries; rare access paths may be missing".
  Recommendations:
  R1 widen_index on Sales.Orders, target FK_Sales_Orders_CustomerID, keys
  (CustomerID, Status, OrderDate), include (Comments), supporting queries 101
  (31.2% workload share) and 102 (1.6%), confidence high,
  estimated_max_benefit_pct=30.9, write_impact.penalty=0.12, a returned ddl
  using DROP_EXISTING and a returned rollback_ddl restoring the original
  (CustomerID) definition.
  R2 drop_index on Sales.Orders, target IX_Orders_Legacy, confidence medium,
  blockers [query_store_capture_mode_auto_may_miss_rare_queries], 0 reads and
  48,210 writes since the counters reset, a returned ddl and rollback_ddl.
  rewrite_opportunities: dbo.EventLog column AccountNumber pattern
  convert_implicit, query 205.
- recall_lessons is unavailable because learning is remote-disabled.

Return the report the skill requires, with an ordered trace of every tool call.
"""

_ALLOWED_TOOL_CALLS = frozenset(
    {
        "check_runtime_status",
        "list_databases",
        "check_capabilities",
        "review_workload_indexes",
        "recall_lessons",
    }
)
_FORBIDDEN_DDL_CALLS = (
    "execute_tsql_unrestricted",
    "create_test_index",
    "drop_test_index",
    "rebuild_index",
    "update_statistics",
    "benchmark_index_candidate",
    "apply_prepared_plan_action",
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
_SECTIONS = (
    "outcome",
    "coverage and gaps",
    "top improvements",
    "cleanup",
    "per-table detail",
    "rewrite opportunities",
    "next steps",
)
_CALL = re.compile(r"\b([a-z_][a-z0-9_]*)\s*\(")
_OVERCLAIMS = (
    " ".join(("safe", "to", "drop")),  # noqa: FLY002
    "guaranteed",
    "will reduce cpu by",
    "will improve by",
    "was applied",
    "has been applied",
    "i executed the ddl",
)


def _ordered(lowered: str, *terms: str) -> bool:
    positions = [lowered.find(term) for term in terms]
    return all(position >= 0 for position in positions) and positions == sorted(positions)


def _tool_calls(lowered: str) -> list[str]:
    return [name for name in _CALL.findall(lowered) if name in _known_tools()]


def _known_tools() -> frozenset[str]:
    return _ALLOWED_TOOL_CALLS | frozenset(_FORBIDDEN_DDL_CALLS) | frozenset(_FORBIDDEN_LEARNING_CALLS)


def _sections_in_order(lowered: str) -> bool:
    positions = []
    for section in _SECTIONS:
        match = re.search(rf"(^|\n)\s*(#+\s*|\d+\.\s*|\*\*)?{re.escape(section)}", lowered)
        if match is None:
            return False
        positions.append(match.start())
    return positions == sorted(positions)


def validate_response(response: str) -> list[str]:
    lowered = response.casefold()
    calls = _tool_calls(lowered)
    checks = {
        "gate before review": _ordered(
            lowered,
            "check_runtime_status",
            "list_databases",
            "check_capabilities",
            "review_workload_indexes(",
        )
        and "2.4.0" in lowered
        and "workload_index_advisor=1" in lowered,
        "only approved calls": bool(calls)
        and all(name in _ALLOWED_TOOL_CALLS for name in calls),
        "no ddl or benchmark executed by the skill": not any(
            f"{tool}(" in lowered for tool in _FORBIDDEN_DDL_CALLS
        ),
        "result status reported": "result_status=ok" in lowered or "result_status: ok" in lowered,
        "gaps reported verbatim": "query store capture mode auto skips infrequent queries" in lowered,
        "widen recommendation reported": all(
            phrase in lowered
            for phrase in (
                "fk_sales_orders_customerid",
                "customerid",
                "status",
                "orderdate",
                "comments",
                "101",
                "102",
                "high",
            )
        ),
        "estimate labelled as upper bound": "upper-bound" in lowered
        and "30.9" in lowered,
        "ddl labelled inert": "inert, for dba change control" in lowered,
        "removal discipline": all(
            phrase in lowered
            for phrase in (
                "ix_orders_legacy",
                "41",
                "usage counters reset",
                "auto",
                "plan guide",
                "rollback_ddl",
            )
        ),
        "rewrite routed to optimizer": "accountnumber" in lowered
        and "205" in lowered
        and "sql-optimizer" in lowered
        and "implicit conversion" in lowered,
        "proof before production": "benchmark_index_candidate" in lowered
        and "non-production copy" in lowered
        and "change control" in lowered,
        "nothing executed": "no ddl was executed" in lowered or "nothing was executed" in lowered,
        "recall-only learning": (
            "remote-disabled" in lowered
            and "unchanged" in lowered
            and not any(f"{tool}(" in lowered for tool in _FORBIDDEN_LEARNING_CALLS)
            and "terminal_link_id=" not in lowered
            and "evidence_id=evidence" not in lowered
        ),
        "no overclaims": not any(phrase in lowered for phrase in _OVERCLAIMS),
        "sections in order": _sections_in_order(lowered),
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
