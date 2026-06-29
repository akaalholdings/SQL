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
``query_geneva_db`` (see ``RunGuide.md``).

Metric convention: every metric here is "lower is better" (durations are Query Store
microseconds, but the rule is ratio-based so units cancel).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field

# Lower is better for all of these; the primary metric drives the keep decision,
# but a bad regression on *any* of them forces a rollback.
PRIMARY_METRIC = "avg_duration"
LOWER_IS_BETTER = ("avg_duration", "avg_cpu_time", "avg_logical_io_reads")

DEFAULT_MIN_EXECUTIONS = 30
DEFAULT_MIN_IMPROVEMENT_PCT = 0.20
DEFAULT_REGRESS_TOLERANCE_PCT = 0.10


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
    if baseline in (None, 0) or candidate is None:
        return None
    return (baseline - candidate) / baseline


def decide(baseline: dict, candidate: dict, thresholds: Thresholds | None = None) -> Decision:
    """Compare pre-change baseline to post-change candidate metrics."""
    thresholds = thresholds or Thresholds()

    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        return Decision("hold", "baseline and candidate metrics must both be objects")

    executions = candidate.get("count_executions", 0) or 0
    if executions < thresholds.min_executions:
        return Decision(
            "hold",
            f"only {executions} post-change executions (< {thresholds.min_executions}); "
            "keep watching before judging",
        )

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

    raw = sys.stdin.read() if args.input == "-" else open(args.input, encoding="utf-8").read()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"could not parse input: {exc}", file=sys.stderr)
        return 1

    decision = decide(
        payload.get("baseline", {}),
        payload.get("candidate", {}),
        Thresholds.from_env(),
    )
    print(json.dumps(decision.as_dict(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
