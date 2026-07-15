#!/usr/bin/env python3
"""Decide keep / rollback / hold for one applied plan change.

This is the safety core of the verify-and-auto-rollback loop (see ``EnforceGuide.md``).
After a plan is forced or a hint is set, Query Store accumulates fresh runtime stats.
This module compares the **pre-change baseline** against the **post-change candidate**
metrics and returns a decision:

- ``keep``     — the change demonstrably helped; leave it in place.
- ``rollback`` — the change regressed, or did nothing useful (an unhelpful forced plan
                 is pure added risk for an autonomous loop, so we revert it).
- ``hold``     — not enough post-change executions yet to judge; watch another cycle.

Pure functions only — no I/O, no Query Store access — so the decision rule is unit
tested directly. The agent feeds it the two metric snapshots it captured via
``azure-sql-mcp`` (see ``RunGuide.md``).

Metric convention: every metric here is "lower is better" (durations are Query Store
microseconds, but the rule is ratio-based so units cancel).
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import os
import pathlib
import sys
from dataclasses import dataclass, field

# Lower is better for all of these; the primary metric drives the keep decision,
# but a bad regression on *any* of them forces a rollback.
PRIMARY_METRIC = "avg_duration"
LOWER_IS_BETTER = ("avg_duration", "avg_cpu_time", "avg_logical_io_reads")

DEFAULT_MIN_EXECUTIONS = 30
DEFAULT_MIN_IMPROVEMENT_PCT = 0.20
DEFAULT_REGRESS_TOLERANCE_PCT = 0.10
DEFAULT_REQUIRE_PROVENANCE = True


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Thresholds:
    min_executions: int = DEFAULT_MIN_EXECUTIONS
    min_improvement_pct: float = DEFAULT_MIN_IMPROVEMENT_PCT
    regress_tolerance_pct: float = DEFAULT_REGRESS_TOLERANCE_PCT
    # Retained for source compatibility with the earlier constructor.  Provenance is
    # always required; no environment setting may disable this safety boundary.
    require_provenance: bool = DEFAULT_REQUIRE_PROVENANCE

    @classmethod
    def from_env(cls) -> "Thresholds":
        return cls(
            min_executions=_env_int("SQL_PLAN_ENFORCER_MIN_EXECUTIONS", DEFAULT_MIN_EXECUTIONS),
            min_improvement_pct=_env_float(
                "SQL_PLAN_ENFORCER_MIN_IMPROVEMENT_PCT", DEFAULT_MIN_IMPROVEMENT_PCT
            ),
            regress_tolerance_pct=_env_float(
                "SQL_PLAN_ENFORCER_REGRESS_TOLERANCE_PCT", DEFAULT_REGRESS_TOLERANCE_PCT
            ),
        )


@dataclass
class Decision:
    action: str  # "keep" | "rollback" | "hold"
    reason: str
    improvement_pct: float | None = None
    regressed_metrics: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "action": self.action,
            "reason": self.reason,
            "improvement_pct": self.improvement_pct,
            "regressed_metrics": self.regressed_metrics,
        }


def improvement_pct(baseline: float | None, candidate: float | None) -> float | None:
    """Positive => candidate is better (lower). None when baseline is unusable."""
    baseline_number = _number(baseline)
    candidate_number = _number(candidate)
    if baseline_number is None or baseline_number <= 0 or candidate_number is None or candidate_number < 0:
        return None
    return (baseline_number - candidate_number) / baseline_number


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _usable_environment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value.strip().casefold() not in {"unknown", "none", "null"}
    )


def _normalized_environment(value: str) -> str:
    return value.strip().casefold()


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _field(evidence: dict, name: str):
    if name in evidence:
        return evidence[name]
    provenance = evidence.get("provenance")
    if isinstance(provenance, dict):
        return provenance.get(name)
    return None


def _has_field(evidence: dict, name: str) -> bool:
    if name in evidence:
        return True
    provenance = evidence.get("provenance")
    return isinstance(provenance, dict) and name in provenance


def _is_true_flag(value: object) -> bool:
    return value is True or (
        isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "y"}
    )


def _contains_truncation(value: object) -> bool:
    """Reject truncation whether the server put it on metrics or evidence."""
    if isinstance(value, dict):
        if _is_true_flag(value.get("truncated")):
            return True
        return any(_contains_truncation(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_truncation(item) for item in value)
    return False


def _expected_for(expected: dict, side: str) -> dict:
    side_expected = expected.get(side)
    return side_expected if isinstance(side_expected, dict) else expected


def _validate_expected(expected: object) -> str | None:
    if not isinstance(expected, dict):
        return "expected environment/query/plan provenance is required"
    for side in ("baseline", "candidate"):
        side_expected = _expected_for(expected, side)
        for field_name in ("environment", "query_id", "plan_id"):
            if field_name not in side_expected:
                return f"expected {side} provenance is missing {field_name}"
        environment = side_expected["environment"]
        if not _usable_environment(environment):
            return f"expected {side} environment is invalid"
        if not _positive_int(side_expected["query_id"]):
            return f"expected {side} query_id is invalid"
        plan_id = side_expected["plan_id"]
        if plan_id is not None and not _positive_int(plan_id):
            return f"expected {side} plan_id is invalid"
    return None


def _provenance_problem(
    baseline: dict,
    candidate: dict,
    expected: dict | None,
) -> str | None:
    """Reject mixed, truncated, or un-attributed metric windows before judging."""
    expected_problem = _validate_expected(expected)
    if expected_problem:
        return expected_problem
    baseline_evidence = baseline.get("evidence")
    candidate_evidence = candidate.get("evidence")
    if not isinstance(baseline_evidence, dict) or not isinstance(candidate_evidence, dict):
        return "verification evidence is required for both metric windows"
    if _contains_truncation(baseline) or _contains_truncation(candidate):
        return "truncated metrics or evidence cannot be used for an enforcement decision"
    if _field(baseline_evidence, "post_change") is not False:
        return "baseline evidence is not marked pre-change"
    if _field(candidate_evidence, "post_change") is not True:
        return "candidate evidence is not marked post-change"
    for label, evidence in (("baseline", baseline_evidence), ("candidate", candidate_evidence)):
        if not _field(evidence, "source"):
            return f"{label} evidence is missing its source"
        if _parse_timestamp(_field(evidence, "window_start")) is None:
            return f"{label} evidence has no valid window_start"
        if _parse_timestamp(_field(evidence, "window_end")) is None:
            return f"{label} evidence has no valid window_end"
    baseline_end = _parse_timestamp(_field(baseline_evidence, "window_end"))
    baseline_start = _parse_timestamp(_field(baseline_evidence, "window_start"))
    candidate_start = _parse_timestamp(_field(candidate_evidence, "window_start"))
    candidate_end = _parse_timestamp(_field(candidate_evidence, "window_end"))
    if (
        baseline_start is None
        or baseline_end is None
        or candidate_start is None
        or candidate_end is None
        or baseline_end <= baseline_start
        or candidate_end <= candidate_start
        or candidate_start < baseline_end
    ):
        return "baseline and candidate windows overlap or are invalid"

    baseline_buckets = _field(baseline_evidence, "parameter_buckets")
    candidate_buckets = _field(candidate_evidence, "parameter_buckets")
    if baseline_buckets is not None or candidate_buckets is not None:
        if baseline_buckets != candidate_buckets:
            return "baseline and candidate parameter buckets do not match"

    baseline_expected = _expected_for(expected, "baseline")
    candidate_expected = _expected_for(expected, "candidate")
    for label, evidence, expected_side in (
        ("baseline", baseline_evidence, baseline_expected),
        ("candidate", candidate_evidence, candidate_expected),
    ):
        for field_name in ("environment", "query_id", "plan_id"):
            if not _has_field(evidence, field_name):
                return f"{label} evidence is missing {field_name}"
            actual_value = _field(evidence, field_name)
            if field_name == "environment":
                if not _usable_environment(actual_value) or (
                    _normalized_environment(actual_value)
                    != _normalized_environment(expected_side[field_name])
                ):
                    return f"{label} evidence {field_name} does not match the expected target"
            elif field_name == "query_id":
                if not _positive_int(actual_value) or actual_value != expected_side[field_name]:
                    return f"{label} evidence {field_name} does not match the expected target"
            elif actual_value is not None and not _positive_int(actual_value):
                return f"{label} evidence {field_name} is invalid"
            elif actual_value != expected_side[field_name]:
                return f"{label} evidence {field_name} does not match the expected target"
    if _normalized_environment(_field(baseline_evidence, "environment")) != _normalized_environment(
        _field(candidate_evidence, "environment")
    ):
        return "baseline and candidate environments do not match"
    if _field(baseline_evidence, "query_id") != _field(candidate_evidence, "query_id"):
        return "baseline and candidate query_ids do not match"
    if _field(baseline_evidence, "source") != _field(candidate_evidence, "source"):
        return "baseline and candidate evidence sources do not match"
    return None


def decide(
    baseline: dict,
    candidate: dict,
    thresholds: Thresholds | None = None,
    expected: dict | None = None,
) -> Decision:
    """Compare pre-change baseline to post-change candidate metrics."""
    thresholds = thresholds or Thresholds()

    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        return Decision("hold", "baseline and candidate metrics must both be objects")

    threshold_values = (
        _number(thresholds.min_improvement_pct),
        _number(thresholds.regress_tolerance_pct),
    )
    if (
        not _positive_int(thresholds.min_executions)
        or any(value is None or value < 0 for value in threshold_values)
    ):
        return Decision("hold", "verification thresholds are invalid")

    problem = _provenance_problem(baseline, candidate, expected)
    if problem:
        return Decision("hold", problem)

    executions = _number(candidate.get("count_executions"))
    if executions is None or executions < 0:
        return Decision("hold", "candidate count_executions is invalid")
    if executions < thresholds.min_executions:
        return Decision(
            "hold",
            f"only {executions} post-change executions (< {thresholds.min_executions}); "
            "keep watching before judging",
        )

    for metric in LOWER_IS_BETTER:
        baseline_has = metric in baseline
        candidate_has = metric in candidate
        if not baseline_has and not candidate_has:
            continue
        if baseline_has != candidate_has:
            return Decision("hold", f"{metric} is missing from one verification window")
        baseline_value = _number(baseline.get(metric))
        candidate_value = _number(candidate.get(metric))
        if baseline_value is None or baseline_value <= 0:
            return Decision("hold", f"baseline {metric} is invalid")
        if candidate_value is None or candidate_value < 0:
            return Decision("hold", f"candidate {metric} is invalid")

    primary = improvement_pct(baseline.get(PRIMARY_METRIC), candidate.get(PRIMARY_METRIC))
    if primary is None:
        return Decision("hold", f"no usable baseline for {PRIMARY_METRIC}; cannot judge")

    # A material regression on ANY tracked metric is an immediate rollback.
    regressed = []
    for metric in LOWER_IS_BETTER:
        change = improvement_pct(baseline.get(metric), candidate.get(metric))
        if change is not None and change < -thresholds.regress_tolerance_pct:
            regressed.append(metric)
    if regressed:
        return Decision(
            "rollback",
            f"regressed beyond {thresholds.regress_tolerance_pct:.0%} on: {', '.join(regressed)}",
            improvement_pct=primary,
            regressed_metrics=regressed,
        )

    if primary >= thresholds.min_improvement_pct:
        return Decision(
            "keep",
            f"{PRIMARY_METRIC} improved {primary:.0%} (>= {thresholds.min_improvement_pct:.0%})",
            improvement_pct=primary,
        )

    # Within the noise band: no meaningful win. An autonomous loop does not leave a
    # control in place that earns nothing — it is added risk and maintenance.
    return Decision(
        "rollback",
        f"no meaningful improvement ({primary:.0%} < {thresholds.min_improvement_pct:.0%}); "
        "unhelpful change reverted",
        improvement_pct=primary,
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Decide keep/rollback/hold for a plan change.")
    parser.add_argument(
        "--input",
        "-i",
        default="-",
        help="JSON with {\"baseline\": {...}, \"candidate\": {...}}; '-' for stdin (default).",
    )
    args = parser.parse_args(argv[1:])

    try:
        raw = (
            sys.stdin.read()
            if args.input == "-"
            else pathlib.Path(args.input).read_text(encoding="utf-8")
        )
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"could not parse input: {exc}", file=sys.stderr)
        return 1

    if not isinstance(payload, dict):
        print("could not parse input: top-level value must be an object", file=sys.stderr)
        return 1

    decision = decide(
        payload.get("baseline", {}),
        payload.get("candidate", {}),
        Thresholds.from_env(),
        payload.get("expected"),
    )
    print(json.dumps(decision.as_dict(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
