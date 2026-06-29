#!/usr/bin/env python3
"""Turn Query Store scan results into a read-only Plan Health Report.

This is the formatter behind **review mode** (see ``ReviewGuide.md``): monitor only, change
nothing. It classifies each candidate into an issue type + severity and renders a prioritized
report. It applies nothing, writes no ledger, and mutates no state — every "recommended
action" it shows is informational ("this is what enforce mode *would* do").

Input is the ranked candidate list (output of ``scan_rank.py``, so each item already carries
``eligible`` / ``score`` / ``reason``), including failing-forced-plan rows from the
unforce-failed detector in ``ReviewGuide.md``. Pure functions; unit tested directly.
"""

from __future__ import annotations

import argparse
import json
import sys

# Lower rank = more urgent. Failing forced plans are critical: a force that keeps failing
# can cause general failure / slower compiles (Kendra Little), so it leads the report.
SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}


def _num(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def classify(candidate: dict) -> tuple[str, str]:
    """Return (issue_type, severity) for one candidate."""
    if _num(candidate.get("force_failure_count")) > 0:
        return "failing_forced_plan", "critical"
    category = candidate.get("category")
    if category == "regression":
        return "plan_regression", "high"
    if category == "param_sensitive":
        return "parameter_sensitive", "medium"
    if category == "stale_forced":
        return "stale_forced_plan", "medium"
    if category == "top_consumer":
        return "top_consumer", "low"
    return "other", "info"


def _summary(candidate: dict, issue_type: str) -> str:
    if issue_type == "failing_forced_plan":
        reason = candidate.get("last_force_failure_reason") or "unknown reason"
        return (
            f"failing forced plan: force_failure_count="
            f"{int(_num(candidate.get('force_failure_count')))}, reason={reason}"
        )
    if issue_type == "plan_regression":
        pct = _num(candidate.get("regression_pct"))
        target = candidate.get("proposed_plan_id")
        return f"plan regression {pct:.0%} slower than plan {target}"
    if issue_type == "parameter_sensitive":
        cv = _num(candidate.get("coefficient_of_variation"))
        return f"parameter-sensitive (coefficient of variation {cv:.2f})"
    if issue_type == "stale_forced_plan":
        return "forced plan beaten by a newer plan; re-evaluate"
    if issue_type == "top_consumer":
        return f"top resource consumer (~{int(_num(candidate.get('total_cost')))} cost units)"
    return f"category {candidate.get('category')!r}"


def build_report(candidates: list) -> dict:
    """Classify and prioritize candidates into an issues report. Does not mutate input."""
    issues = []
    for candidate in candidates:
        issue_type, severity = classify(candidate)
        issue = {
            "issue_type": issue_type,
            "severity": severity,
            "query_id": candidate.get("query_id"),
            "category": candidate.get("category"),
            "eligible": candidate.get("eligible"),
            "summary": _summary(candidate, issue_type),
            "would_apply_lever": candidate.get("proposed_lever"),
            "score": _num(candidate.get("score")),
        }
        if candidate.get("unforce_command"):
            issue["unforce_command"] = candidate["unforce_command"]
        if candidate.get("last_force_failure_reason"):
            issue["last_force_failure_reason"] = candidate["last_force_failure_reason"]
        issues.append(issue)

    issues.sort(key=lambda i: (SEVERITY_RANK.get(i["severity"], 99), -i["score"]))

    by_severity: dict[str, int] = {}
    for issue in issues:
        by_severity[issue["severity"]] = by_severity.get(issue["severity"], 0) + 1

    return {
        "total_issues": len(issues),
        "by_severity": by_severity,
        "issues": issues,
    }


def render_text(report: dict) -> str:
    counts = ", ".join(
        f"{sev}: {report['by_severity'][sev]}"
        for sev in sorted(report["by_severity"], key=lambda s: SEVERITY_RANK.get(s, 99))
    )
    lines = [f"Plan Health Report — {report['total_issues']} issue(s) ({counts})", ""]

    current = None
    for issue in report["issues"]:
        if issue["severity"] != current:
            current = issue["severity"]
            lines.append(current.upper())
        marker = "" if issue.get("eligible") is not False else "  [below threshold — monitoring only]"
        lines.append(f"  • query_id {issue['query_id']} — {issue['summary']}{marker}")
        if issue.get("unforce_command"):
            lines.append(f"      would run: {issue['unforce_command']}")
        elif issue.get("would_apply_lever") and issue["would_apply_lever"] != "handoff_optimizer":
            lines.append(f"      would apply: {issue['would_apply_lever']} (not executed in review mode)")
        elif issue.get("would_apply_lever") == "handoff_optimizer":
            lines.append("      hand off to sql_optimizer (rewrite/index)")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Render a read-only Plan Health Report.")
    parser.add_argument("--input", "-i", default="-", help="Ranked candidates JSON; '-' for stdin.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON instead of text.")
    args = parser.parse_args(argv[1:])

    raw = sys.stdin.read() if args.input == "-" else open(args.input, encoding="utf-8").read()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"could not parse input: {exc}", file=sys.stderr)
        return 1
    candidates = payload.get("candidates", payload) if isinstance(payload, dict) else payload

    report = build_report(candidates)
    print(json.dumps(report, ensure_ascii=False, indent=2) if args.json else render_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
