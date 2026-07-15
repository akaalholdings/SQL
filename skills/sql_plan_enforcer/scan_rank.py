#!/usr/bin/env python3
"""Rank Query Store scan results into a prioritized list of tuning candidates.

The scan queries in ``ScanGuide.md`` are run through ``azure-sql-mcp`` read tools;
each returns rows describing candidate queries. This module merges those rows, scores and
tiers them, applies eligibility thresholds (blast-radius / noise guards), and emits a single
ranked list the enforcement loop walks top-down.

Input is lenient: a top-level JSON list of candidate objects, or an object wrapping them under
``candidates`` / ``rows`` / ``data`` / ``results``. Each candidate carries at least:

    category          one of: regression | top_consumer | param_sensitive | stale_forced
    query_id          Query Store query_id (int)
    count_executions  executions over the scan window
    avg_duration      microseconds (Query Store units)

and, depending on category: ``regression_pct``, ``stdev_duration``, ``force_failure_count``,
``proposed_lever``, ``proposed_plan_id``, ``proposed_hint``.

Ranking is pure and deterministic so it is unit tested directly. It never applies anything.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import sys
from dataclasses import dataclass

CATEGORIES = ("regression", "top_consumer", "param_sensitive", "stale_forced")
LEVERS = ("force_plan", "set_hints", "unforce_plan", "handoff_optimizer")

# Lower tier = handled first. A forced plan that is actively failing to force is the most
# urgent thing in the fleet (it is broken right now), ahead of fresh regressions.
TIER_STALE_FORCED_FAILED = 0
TIER_REGRESSION = 1
TIER_PARAM_SENSITIVE = 2
TIER_STALE_FORCED_BEATEN = 2
TIER_TOP_CONSUMER = 3

DEFAULT_MIN_EXECUTIONS = 30
DEFAULT_MIN_REGRESSION_PCT = 0.50  # recent plan >= 50% slower than a known-good plan
DEFAULT_MIN_CV = 0.50  # coefficient of variation flagging parameter sensitivity
DEFAULT_MIN_AVG_DURATION = 1000.0  # microseconds; ignore trivially fast queries


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    try:
        return float(raw) if raw and raw.strip() else default
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    try:
        return int(raw) if raw and raw.strip() else default
    except ValueError:
        return default


@dataclass(frozen=True)
class Thresholds:
    min_executions: int = DEFAULT_MIN_EXECUTIONS
    min_regression_pct: float = DEFAULT_MIN_REGRESSION_PCT
    min_cv: float = DEFAULT_MIN_CV
    min_avg_duration: float = DEFAULT_MIN_AVG_DURATION

    @classmethod
    def from_env(cls) -> "Thresholds":
        return cls(
            min_executions=_env_int("SQL_PLAN_ENFORCER_MIN_EXECUTIONS", DEFAULT_MIN_EXECUTIONS),
            min_regression_pct=_env_float(
                "SQL_PLAN_ENFORCER_MIN_REGRESSION_PCT", DEFAULT_MIN_REGRESSION_PCT
            ),
            min_cv=_env_float("SQL_PLAN_ENFORCER_MIN_CV", DEFAULT_MIN_CV),
            min_avg_duration=_env_float(
                "SQL_PLAN_ENFORCER_MIN_AVG_DURATION", DEFAULT_MIN_AVG_DURATION
            ),
        )


def _num(value, default=0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def total_cost(candidate: dict) -> float:
    """Rough aggregate workload cost: how much this query hurts overall."""
    return _num(candidate.get("count_executions")) * _num(candidate.get("avg_duration"))


def _coefficient_of_variation(candidate: dict) -> float:
    avg = _num(candidate.get("avg_duration"))
    if avg <= 0:
        return 0.0
    return _num(candidate.get("stdev_duration")) / avg


def _true_flag(value: object) -> bool:
    return value is True or (
        isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "y"}
    )


def _contains_truncation(candidate: object) -> bool:
    if isinstance(candidate, dict):
        if _true_flag(candidate.get("truncated")):
            return True
        return any(_contains_truncation(value) for value in candidate.values())
    if isinstance(candidate, (list, tuple)):
        return any(_contains_truncation(value) for value in candidate)
    return False


def _default_lever(category: str, candidate: dict) -> str:
    if category == "regression":
        return "force_plan"
    if category == "param_sensitive":
        return "set_hints"
    if category == "stale_forced":
        return "unforce_plan"
    # top_consumer: only an enforcer action when a better alternate plan exists;
    # otherwise it is a rewrite job for the sql_optimizer skill.
    if _positive_int(candidate.get("proposed_plan_id")):
        return "force_plan"
    return "handoff_optimizer"


def _tier(category: str, candidate: dict) -> int:
    if category == "stale_forced":
        return (
            TIER_STALE_FORCED_FAILED
            if _num(candidate.get("force_failure_count")) > 0
            else TIER_STALE_FORCED_BEATEN
        )
    return {
        "regression": TIER_REGRESSION,
        "param_sensitive": TIER_PARAM_SENSITIVE,
        "top_consumer": TIER_TOP_CONSUMER,
    }.get(category, TIER_TOP_CONSUMER)


def _score(category: str, candidate: dict) -> float:
    cost = total_cost(candidate)
    if category == "regression":
        return _num(candidate.get("regression_pct")) * cost
    if category == "param_sensitive":
        return _coefficient_of_variation(candidate) * cost
    if category == "stale_forced":
        failures = _num(candidate.get("force_failure_count"))
        return failures if failures > 0 else cost
    return cost  # top_consumer


def _eligibility(category: str, candidate: dict, t: Thresholds) -> tuple[bool, str]:
    environment = candidate.get("environment")
    if not isinstance(environment, str) or not environment.strip() or environment.strip().lower() in {
        "unknown", "none", "null"
    }:
        return False, "environment is required for database-scoped query identity"
    query_id = candidate.get("query_id")
    if not _positive_int(query_id):
        return False, "positive integer query_id is required for database-scoped query identity"
    threshold_values = (
        _num(t.min_regression_pct, float("nan")),
        _num(t.min_cv, float("nan")),
        _num(t.min_avg_duration, float("nan")),
    )
    if (
        not _positive_int(t.min_executions)
        or any(not math.isfinite(value) or value < 0 for value in threshold_values)
    ):
        return False, "ranking thresholds are invalid"
    lever = candidate.get("proposed_lever") or _default_lever(category, candidate)
    if lever not in LEVERS:
        return False, f"unsupported proposed lever {lever!r}"
    if lever == "force_plan" and not _positive_int(candidate.get("proposed_plan_id")):
        return False, "force_plan requires a positive proposed_plan_id"
    if lever == "unforce_plan" and not _positive_int(candidate.get("current_plan_id")):
        return False, "unforce_plan requires a positive current_plan_id"
    if candidate.get("review_only") or candidate.get("automatic_tuning_review_only"):
        return False, candidate.get(
            "review_reason", "candidate is review-only and cannot compete for apply"
        )
    if _contains_truncation(candidate):
        return False, "truncated scan evidence is not eligible for enforcement"
    executions = _num(candidate.get("count_executions"))

    if category == "stale_forced":
        if _num(candidate.get("force_failure_count")) > 0:
            return True, "forced plan is failing to force"
        if executions < t.min_executions:
            return False, f"only {executions:.0f} executions (< {t.min_executions})"
        return True, "forced plan beaten by a newer plan; re-evaluate"

    if executions < t.min_executions:
        return False, f"only {executions:.0f} executions (< {t.min_executions})"

    if category == "regression":
        reg = _num(candidate.get("regression_pct"))
        if reg < t.min_regression_pct:
            return False, f"regression {reg:.0%} below {t.min_regression_pct:.0%} floor"
        return True, f"regressed {reg:.0%} vs a known-good plan"

    if category == "param_sensitive":
        cv = _coefficient_of_variation(candidate)
        if _num(candidate.get("avg_duration")) < t.min_avg_duration:
            return False, "avg duration below trivial-query floor"
        if cv < t.min_cv:
            return False, f"variation {cv:.2f} below {t.min_cv:.2f} floor"
        return True, f"high duration variation (cv {cv:.2f})"

    # top_consumer
    return True, "top aggregate resource consumer"


def normalize(candidate: dict) -> dict:
    """Annotate one candidate with category, tier, score, lever, and eligibility."""
    category = candidate.get("category")
    if category not in CATEGORIES:
        return {
            **candidate,
            "eligible": False,
            "reason": f"unknown category {category!r}",
            "tier": TIER_TOP_CONSUMER + 1,
            "score": 0.0,
        }

    eligible, reason = _eligibility(category, candidate, Thresholds())
    return {
        **candidate,
        "proposed_lever": candidate.get("proposed_lever") or _default_lever(category, candidate),
        "tier": _tier(category, candidate),
        "score": _score(category, candidate),
        "total_cost": total_cost(candidate),
        "eligible": eligible,
        "reason": reason,
    }


def rank(candidates: list, thresholds: Thresholds | None = None) -> list:
    """Return all candidates annotated and sorted most-urgent first.

    Sort key: tier ascending (0 = most urgent), then score descending. Eligible
    candidates always sort ahead of ineligible ones within the same tier so the
    enforcement loop can simply take eligible ones off the top.
    """
    thresholds = thresholds or Thresholds()
    annotated = []
    for raw in candidates:
        if not isinstance(raw, dict):
            annotated.append({
                "eligible": False,
                "reason": "candidate must be an object",
                "tier": TIER_TOP_CONSUMER + 1,
                "score": 0.0,
            })
            continue
        category = raw.get("category")
        if category in CATEGORIES:
            eligible, reason = _eligibility(category, raw, thresholds)
            item = {
                **raw,
                "proposed_lever": raw.get("proposed_lever") or _default_lever(category, raw),
                "tier": _tier(category, raw),
                "score": _score(category, raw),
                "total_cost": total_cost(raw),
                "eligible": eligible,
                "reason": reason,
            }
        else:
            item = {
                **raw,
                "eligible": False,
                "reason": f"unknown category {category!r}",
                "tier": TIER_TOP_CONSUMER + 1,
                "score": 0.0,
            }
        annotated.append(item)

    annotated.sort(key=lambda c: (c["tier"], not c["eligible"], -c["score"]))
    return annotated


def _extract_candidates(payload) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("candidates", "rows", "data", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise ValueError("input must be a JSON list of candidates or wrap them under candidates/rows/data/results")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Rank Query Store tuning candidates.")
    parser.add_argument("--input", "-i", default="-", help="Scan JSON; '-' for stdin (default).")
    parser.add_argument(
        "--limit", "-n", type=int, default=0, help="Emit only the top N eligible candidates (0 = all)."
    )
    parser.add_argument(
        "--eligible-only", action="store_true", help="Drop candidates that failed the thresholds."
    )
    args = parser.parse_args(argv[1:])

    try:
        raw = (
            sys.stdin.read()
            if args.input == "-"
            else pathlib.Path(args.input).read_text(encoding="utf-8")
        )
        payload = json.loads(raw)
        candidates = _extract_candidates(payload)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"could not parse scan input: {exc}", file=sys.stderr)
        return 1

    ranked = rank(candidates, Thresholds.from_env())
    eligible = [c for c in ranked if c["eligible"]]
    out = eligible if args.eligible_only else ranked
    if args.limit > 0:
        capped = [c for c in out if c["eligible"]][: args.limit]
        out = capped if args.eligible_only else capped + [c for c in out if not c["eligible"]]

    print(
        json.dumps(
            {"eligible_count": len(eligible), "total": len(ranked), "candidates": out},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
