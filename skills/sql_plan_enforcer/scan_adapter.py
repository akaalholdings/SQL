#!/usr/bin/env python3
"""Adapt azure-sql-mcp scan-tool payloads into scan_rank candidate records.

The primary scan path (``ScanGuide.md``) calls the server's dedicated detection tools —
``detect_parameter_sniffing``, ``get_top_queries``, ``detect_regressed_queries``,
``get_forced_plans`` (or the ``plan_health_review`` bundle) — instead of hand-rolled SQL.
Each tool returns its own payload shape and units; this module normalizes them into the
candidate schema ``scan_rank.py`` ranks (``category``, ``query_id``, ``count_executions``,
``avg_duration`` in **microseconds**, plus category-specific fields). ``scan_rank.py``
stays the single ranking brain; this module never scores or filters.

Unit conversions (verified against the server source):

- ``detect_parameter_sniffing`` reports durations in **milliseconds** → ×1000 here.
  It has no stdev; the adapter synthesizes ``stdev_duration = (worst - best) / 2`` as a
  spread proxy. The resulting coefficient of variation is NOT numerically comparable to
  the custom scan's CV — candidates are tagged ``adapted_from`` so reports can say so,
  and the custom scan (ScanGuide fallback) remains the precision path.
- ``get_top_queries`` reports ``avg_duration_us`` already in microseconds; rows are per
  (query_id, plan_id) and are aggregated per query here (``distinct_plans`` counted).
- ``detect_regressed_queries`` rows are automatic-tuning recommendations with string ids.
  When ``details.planForceDetails`` carries the regressed/recommended plan CPU averages,
  a real ``regression_pct`` is computed from them; otherwise the field is omitted and the
  candidate ranks as a monitoring-only lead (enrich it via the fallback regression scan).
- ``get_forced_plans`` reports durations in **milliseconds** → ×1000 here.

Every adapted candidate carries ``adapted_from: "<tool>"`` for provenance.

Pure functions + a small CLI; no I/O beyond the CLI, so it is unit tested directly::

    python3 scan_adapter.py --input sniff.json --input top.json --input reg.json \
        --input forced.json > /tmp/candidates.json
    python3 scan_rank.py --input /tmp/candidates.json --eligible-only --limit 5
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys

SOURCES = (
    "detect_parameter_sniffing",
    "get_top_queries",
    "detect_regressed_queries",
    "get_forced_plans",
    "plan_health_review",
)

_MS_TO_US = 1000.0


def _num(value, default=None):
    if isinstance(value, bool):
        return default
    try:
        if value is None or value == "":
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _int(value, default=None):
    number = _num(value)
    if number is None or not number.is_integer():
        return default
    return int(number)


def _true_flag(value: object) -> bool:
    return value is True or (
        isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "y"}
    )


def _contains_truncation(value: object) -> bool:
    if isinstance(value, dict):
        if _true_flag(value.get("truncated")):
            return True
        return any(_contains_truncation(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_truncation(item) for item in value)
    return False


def _annotate_candidates(candidates: list[dict], payload: dict, source: str) -> list[dict]:
    """Carry database identity and scan completeness into every candidate."""
    environment = payload.get("database_name") or payload.get("environment")
    truncated = _contains_truncation(payload)
    for candidate in candidates:
        candidate.setdefault("environment", environment)
        if truncated:
            candidate["truncated"] = True
        candidate.setdefault("adapted_from", source)
    return candidates


def detect_source(payload: dict) -> str | None:
    """Identify which server tool produced a payload by its signature keys."""
    if not isinstance(payload, dict):
        return None
    if "queries" in payload and "variance_threshold" in payload:
        return "detect_parameter_sniffing"
    if "rows" in payload and "sort_by" in payload:
        return "get_top_queries"
    if "recommendations" in payload and "recommendation_count" in payload:
        return "detect_regressed_queries"
    if "forced_plans" in payload:
        return "get_forced_plans"
    if payload.get("mode") == "review" and (
        "recommended_actions" in payload
        or isinstance(payload.get("plan_enforcement"), dict)
        and "recommended_actions" in payload["plan_enforcement"]
    ):
        return "plan_health_review"
    return None


def _adapt_param_sniffing(payload: dict) -> list[dict]:
    candidates = []
    for row in payload.get("queries", []):
        best_ms = _num(row.get("best_avg_duration_ms"), 0.0)
        worst_ms = _num(row.get("worst_avg_duration_ms"), 0.0)
        avg_us = (best_ms + worst_ms) / 2.0 * _MS_TO_US
        stdev_us = max(worst_ms - best_ms, 0.0) / 2.0 * _MS_TO_US
        candidates.append({
            "category": "param_sensitive",
            "query_id": _int(row.get("query_id")),
            "current_plan_id": _int(row.get("worst_plan_id")),
            "count_executions": _num(row.get("total_executions"), 0.0),
            "avg_duration": avg_us,
            "stdev_duration": stdev_us,
            "plan_count": _int(row.get("plan_count")),
            "duration_variance_ratio": _num(row.get("duration_variance_ratio")),
            "proposed_lever": "set_hints",
            "adapted_from": "detect_parameter_sniffing",
        })
    return candidates


def _adapt_top_queries(payload: dict) -> list[dict]:
    # Rows are per (query_id, plan_id): aggregate per query, executions-weighted.
    by_query: dict[int, dict] = {}
    for row in payload.get("rows", []):
        qid = _int(row.get("query_id"))
        if qid is None:
            continue
        executions = _num(row.get("executions"), 0.0) or 0.0
        avg_us = _num(row.get("avg_duration_us"), 0.0) or 0.0
        agg = by_query.setdefault(qid, {
            "category": "top_consumer",
            "query_id": qid,
            "count_executions": 0.0,
            "total_duration": 0.0,
            "total_cpu_time": 0.0,
            "total_logical_reads": 0.0,
            "distinct_plans": 0,
            "adapted_from": "get_top_queries",
        })
        agg["count_executions"] += executions
        agg["total_duration"] += _num(row.get("total_duration_us"), executions * avg_us) or 0.0
        agg["total_cpu_time"] += _num(row.get("total_cpu_us"), 0.0) or 0.0
        agg["total_logical_reads"] += _num(row.get("total_logical_io_reads"), 0.0) or 0.0
        agg["distinct_plans"] += 1
    for agg in by_query.values():
        executions = agg["count_executions"]
        agg["avg_duration"] = (agg["total_duration"] / executions) if executions else 0.0
    return list(by_query.values())


def _adapt_regressed(payload: dict) -> list[dict]:
    candidates = []
    for row in payload.get("recommendations", []):
        qid = _int(row.get("query_id"))
        if qid is None:
            continue
        candidate = {
            "category": "regression",
            "query_id": qid,
            "current_plan_id": _int(row.get("regressed_plan_id")),
            "proposed_plan_id": _int(row.get("recommended_plan_id")),
            "count_executions": _num(row.get("recent_execution_count"), 0.0),
            "server_score": _num(row.get("score")),
            "proposed_lever": "force_plan",
            "adapted_from": "detect_regressed_queries",
            # Preserve the engine-owned recommendation state.  It is deliberately
            # not collapsed into the enforcer's own lifecycle state.
            "current_state": row.get("current_state"),
            "automatic_tuning_state": row.get("current_state"),
        }
        candidate["automatic_tuning_initiated_by"] = row.get(
            "execute_action_initiated_by"
        )
        # A recommendation from sys.dm_db_tuning_recommendations is always kept
        # out of this custom apply loop. Active can be applied by a user or by
        # FORCE_LAST_GOOD_PLAN; Verifying/Success may already be engine-owned;
        # Reverted/Expired are not valid apply candidates. Use the independent
        # fallback scan when a custom regression candidate is needed.
        candidate["automatic_tuning_review_only"] = True
        candidate["review_only"] = True
        candidate["review_reason"] = (
            "automatic-tuning DMV recommendation is review-only; establish a custom "
            "candidate from independent Query Store evidence"
        )
        # details.planForceDetails carries the per-plan CPU averages automatic tuning
        # compared — a real regression ratio, not a fabricated one.
        details = row.get("details")
        force_details = details.get("planForceDetails") if isinstance(details, dict) else None
        if isinstance(force_details, dict):
            regressed = _num(force_details.get("regressedPlanCpuTimeAverage"))
            recommended = _num(force_details.get("recommendedPlanCpuTimeAverage"))
            if regressed and recommended:
                candidate["regression_pct"] = (regressed - recommended) / recommended
                candidate["avg_duration"] = regressed  # Query Store microseconds
            bad_execs = _num(force_details.get("regressedPlanExecutionCount"))
            if bad_execs:
                candidate["count_executions"] = bad_execs
        candidates.append(candidate)
    return candidates


def _adapt_forced_plans(payload: dict) -> list[dict]:
    candidates = []
    for row in payload.get("forced_plans", []):
        failures = _num(row.get("force_failure_count"), 0.0) or 0.0
        days_stale = _int(row.get("days_since_last_exec"), 0) or 0
        # Only failing or stale forced plans are candidates; a healthy forced plan
        # executing normally is not an issue to rank.
        if failures <= 0 and days_stale <= 7:
            continue
        candidates.append({
            "category": "stale_forced",
            "query_id": _int(row.get("query_id")),
            "current_plan_id": _int(row.get("plan_id")),
            "force_failure_count": failures,
            "last_force_failure_reason": row.get("last_force_failure_reason_desc"),
            "days_since_last_exec": days_stale,
            "count_executions": _num(row.get("recent_execution_count"), 0.0),
            "avg_duration": (_num(row.get("avg_duration_ms"), 0.0) or 0.0) * _MS_TO_US,
            "proposed_lever": "unforce_plan",
            "plan_forcing_type_desc": row.get("plan_forcing_type_desc"),
            "adapted_from": "get_forced_plans",
        })
        candidate = candidates[-1]
        forcing_type = str(row.get("plan_forcing_type_desc") or "").strip().upper()
        if forcing_type != "MANUAL":
            candidate["review_only"] = True
            candidate["automatic_tuning_review_only"] = forcing_type == "AUTO"
            candidate["review_reason"] = (
                "forced-plan ownership is automatic or unknown; review only"
            )
    return candidates


def _adapt_plan_health_review(payload: dict) -> list[dict]:
    # Coarse snapshot bundle: its recommended_actions lack the Query Store metrics the
    # ranking thresholds need. Failing forced plans still rank (tier 0 by failure count);
    # force-recommendations come through as leads to enrich via the dedicated tools.
    candidates = []
    enforcement = payload.get("plan_enforcement")
    source = enforcement if isinstance(enforcement, dict) else payload
    for action in source.get("recommended_actions", []):
        qid = _int(action.get("query_id"))
        if qid is None:
            continue
        kind = str(action.get("action", "")).lower()
        if kind == "unforce":
            candidates.append({
                "category": "stale_forced",
                "query_id": qid,
                "current_plan_id": _int(action.get("plan_id")),
                "force_failure_count": (
                    _num(action.get("score"), 0.0)
                    if action.get("reason") == "forced_plan_failure" else 0.0
                ),
                "days_since_last_exec": _int(action.get("days_since_last_exec"), 0),
                "count_executions": 0.0,
                "proposed_lever": "unforce_plan",
                "adapted_from": "plan_health_review",
                "review_only": True,
                "review_reason": "snapshot does not identify forced-plan ownership",
            })
        elif kind == "force":
            candidates.append({
                "category": "regression",
                "query_id": qid,
                "proposed_plan_id": _int(action.get("plan_id")),
                "count_executions": 0.0,
                "server_score": _num(action.get("score")),
                "proposed_lever": "force_plan",
                "adapted_from": "plan_health_review",
                "review_only": True,
                "review_reason": "automatic-tuning snapshot action is review-only",
            })
    return candidates


_ADAPTERS = {
    "detect_parameter_sniffing": _adapt_param_sniffing,
    "get_top_queries": _adapt_top_queries,
    "detect_regressed_queries": _adapt_regressed,
    "get_forced_plans": _adapt_forced_plans,
    "plan_health_review": _adapt_plan_health_review,
}


def adapt(payload: dict, source: str | None = None) -> list[dict]:
    """Normalize one server-tool payload into scan_rank candidates."""
    if not isinstance(payload, dict):
        raise ValueError("tool payload must be a JSON object")
    source = source or detect_source(payload)
    if source not in _ADAPTERS:
        raise ValueError(
            "could not identify the payload's source tool; pass --source with one of: "
            + ", ".join(SOURCES)
        )
    raw_candidates = _ADAPTERS[source](payload)
    if any(not isinstance(candidate, dict) for candidate in raw_candidates):
        raise ValueError(f"{source} payload contains a non-object candidate")
    candidates = [c for c in raw_candidates if c.get("query_id") is not None]
    return _annotate_candidates(candidates, payload, source)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Adapt azure-sql-mcp scan-tool payloads into scan_rank candidates."
    )
    parser.add_argument(
        "--input", "-i", action="append", required=True,
        help="Tool payload JSON file; repeatable, or '-' for stdin (once).",
    )
    parser.add_argument(
        "--source", "-s", action="append", default=None, choices=SOURCES,
        help="Source tool per --input, positionally matched; omit to auto-detect.",
    )
    args = parser.parse_args(argv[1:])

    sources = args.source or []
    candidates: list[dict] = []
    for position, path in enumerate(args.input):
        try:
            raw = (
                sys.stdin.read()
                if path == "-"
                else pathlib.Path(path).read_text("utf-8")
            )
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"could not parse {path}: {exc}", file=sys.stderr)
            return 1
        explicit = sources[position] if position < len(sources) else None
        try:
            candidates.extend(adapt(payload, source=explicit))
        except ValueError as exc:
            print(f"{path}: {exc}", file=sys.stderr)
            return 1

    print(json.dumps({"candidates": candidates}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
