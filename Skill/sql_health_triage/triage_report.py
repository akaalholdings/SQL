#!/usr/bin/env python3
"""Turn normalized triage findings into a read-only Triage Report.

The formatter behind ``ReportGuide.md``: the agent runs the diagnostic tools from
``TriageGuide.md``, normalizes what it saw into finding objects, and this module
classifies severity, orders them, and renders the report. It executes nothing and
recommends only — every action carries an **owner** (``sql-optimizer``,
``sql-plan-enforcer``, or ``human``) so the finding lands with whoever can act on it.

A finding is one observation::

    {
      "domain":  "resource|blocking|deadlock|tempdb|memory|compile|waits|io|connections|config|other",
      "metric":  "<what was measured, e.g. avg_cpu_percent>",
      "value":   <number>,
      "threshold": <number, optional — the limit/ceiling the value is judged against>,
      "query_id": <int, optional — when a specific query is implicated>,
      "summary": "<one-line diagnosis>",
      "recommended_action": "<what to do next>",
      "owner":   "sql-optimizer" | "sql-plan-enforcer" | "human"
    }

Severity rules are deliberately small and explicit (unit tested one by one):

- ``resource`` at >= 90% of its governance ceiling  -> **critical** (the database is
  about to hit a hard limit; everything else is noise until this is addressed)
- active ``blocking`` chain / any ``deadlock`` in window -> **high**
- ``tempdb`` at >= 80% of capacity                  -> **high**
- pending ``memory`` grants (queries waiting on RAM) -> **high**
- ``compile`` pressure or any other threshold exceeded -> **medium**
- everything else observed but in range              -> **info**

The report keeps the compatible ``findings`` list, but also separates it into
``actionable_findings``, ``observations``, and ``incomplete_evidence``. A finding
with ``evidence.truncated`` set to true is always inconclusive: it cannot have an
owner handoff or corrective action, even when its value exceeds a threshold.

Pure functions + CLI; no I/O beyond the CLI, so the rules are unit tested directly.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys

SEVERITY_RANK = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "info": 4,
    "inconclusive": 5,
}
OWNERS = ("sql-optimizer", "sql-plan-enforcer", "human")
ACTIONABLE_SEVERITIES = frozenset(("critical", "high", "medium", "low"))

RESOURCE_CRITICAL_RATIO = 0.90
TEMPDB_HIGH_RATIO = 0.80
INCONCLUSIVE_NEXT_STEP = (
    "Evidence was truncated; narrow or re-query the diagnostic evidence before "
    "drawing a conclusion or taking corrective action."
)


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


def _ratio(finding: dict) -> float | None:
    value = _num(finding.get("value"))
    threshold = _num(finding.get("threshold"))
    if value is None or threshold in (None, 0):
        return None
    return value / threshold


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


def _evidence_is_truncated(finding: dict) -> bool:
    return _contains_truncation(finding.get("evidence"))


def classify(finding: dict) -> str:
    """Severity for one finding — the tested rule table."""
    if _evidence_is_truncated(finding):
        return "inconclusive"

    domain = finding.get("domain")
    value = _num(finding.get("value"), 0.0)
    ratio = _ratio(finding)

    if domain == "resource" and ratio is not None and ratio >= RESOURCE_CRITICAL_RATIO:
        return "critical"
    if domain in ("blocking", "deadlock") and value > 0:
        return "high"
    if domain == "tempdb" and ratio is not None and ratio >= TEMPDB_HIGH_RATIO:
        return "high"
    if domain == "memory" and finding.get("metric") == "pending_memory_grants" and value > 0:
        return "high"
    if domain == "compile" and (ratio is None or ratio >= 1.0):
        return "medium"
    if ratio is not None and ratio >= 1.0:
        return "medium"
    return "info"


def validate_finding(finding: object) -> list[str]:
    """Validate the evidence contract before a finding can enter a report."""
    if not isinstance(finding, dict):
        return [f"finding must be an object, got {type(finding).__name__}"]
    errors: list[str] = []
    for field in ("domain", "metric", "summary"):
        if not isinstance(finding.get(field), str) or not finding[field].strip():
            errors.append(f"missing required field: {field}")
    if _num(finding.get("value")) is None:
        errors.append("value must be numeric")
    if "threshold" in finding and _num(finding.get("threshold")) is None:
        errors.append("threshold must be numeric")
    evidence = finding.get("evidence")
    if not isinstance(evidence, dict) or not isinstance(evidence.get("tool"), str) or not evidence["tool"].strip():
        errors.append("evidence.tool is required")
    if isinstance(evidence, dict) and "truncated" in evidence and not isinstance(evidence["truncated"], bool):
        if not (
            isinstance(evidence["truncated"], str)
            and evidence["truncated"].strip().lower()
            in {"0", "1", "false", "true", "no", "yes", "n", "y"}
        ):
            errors.append("evidence.truncated must be boolean")
    if not errors and classify(finding) in ACTIONABLE_SEVERITIES:
        action = finding.get("recommended_action")
        if not isinstance(action, str) or not action.strip():
            errors.append("missing required field: recommended_action")
    return errors


def _owner(finding: dict) -> str | None:
    if _evidence_is_truncated(finding):
        return None
    owner = finding.get("owner")
    return owner if owner in OWNERS else "human"


def _status(severity: str) -> str:
    if severity == "inconclusive":
        return "inconclusive"
    if severity in ACTIONABLE_SEVERITIES:
        return "actionable"
    return "observation"


def _issue_sort_key(issue: dict) -> tuple:
    value = _num(issue.get("value"), 0.0)
    return (
        SEVERITY_RANK.get(issue["severity"], 99),
        -(value if value is not None else 0.0),
        str(issue.get("domain", "")),
        str(issue.get("metric", "")),
        str(issue.get("query_id", "")),
        str(issue.get("summary", "")),
    )


def _count_by(items: list[dict], field: str, order: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = item.get(field)
        if value is not None:
            counts[value] = counts.get(value, 0) + 1
    return {value: counts[value] for value in order if counts.get(value)}


def build_report(findings: list, *, database_name: str | None = None,
                 mode: str = "triage", symptom: str | None = None,
                 capabilities: str | None = None) -> dict:
    """Classify and order findings. Does not mutate input."""
    if not isinstance(findings, list):
        raise ValueError("findings must be a list")
    issues = []
    for finding in findings:
        problems = validate_finding(finding)
        if problems:
            raise ValueError("invalid triage finding: " + "; ".join(problems))
        issue = {
            **finding,
            "severity": classify(finding),
            "status": "",
            "owner": _owner(finding),
        }
        issue["status"] = _status(issue["severity"])
        if issue["status"] == "inconclusive":
            # Preserve the report shape, but remove unsafe routing data. The
            # renderer uses next_step instead of the caller-provided action.
            issue["recommended_action"] = None
            issue["next_step"] = INCONCLUSIVE_NEXT_STEP
        issues.append(issue)

    issues.sort(key=_issue_sort_key)

    actionable = [issue for issue in issues if issue["status"] == "actionable"]
    observations = [issue for issue in issues if issue["status"] == "observation"]
    incomplete = [issue for issue in issues if issue["status"] == "inconclusive"]

    severity_order = tuple(SEVERITY_RANK)
    by_severity = _count_by(issues, "severity", severity_order)
    actionable_by_severity = _count_by(actionable, "severity", severity_order)
    by_owner = _count_by(issues, "owner", OWNERS)
    actionable_by_owner = _count_by(actionable, "owner", OWNERS)

    return {
        "database_name": database_name,
        "mode": mode,
        "symptom": symptom,
        "capabilities": capabilities,
        "total_findings": len(issues),
        "actionable_count": len(actionable),
        "observation_count": len(observations),
        "inconclusive_count": len(incomplete),
        "healthy": not actionable,
        "by_severity": by_severity,
        "actionable_by_severity": actionable_by_severity,
        "by_owner": by_owner,
        "actionable_by_owner": actionable_by_owner,
        "findings": issues,
        "actionable_findings": actionable,
        "observations": observations,
        "incomplete_evidence": incomplete,
    }


def _counts_text(counts: dict[str, int], *, empty: str = "none") -> str:
    if not counts:
        return empty
    return ", ".join(f"{key}: {value}" for key, value in counts.items())


def _append_evidence(lines: list[str], issue: dict, *, indent: str = "      ") -> None:
    evidence = issue["evidence"]
    evidence_text = (
        f"{evidence['tool']}; value={issue.get('value')}; "
        f"threshold={issue.get('threshold')}"
    )
    if evidence.get("window_minutes") is not None:
        evidence_text += f"; window={evidence['window_minutes']}m"
    if evidence.get("truncated"):
        evidence_text += "; truncated=true"
    lines.append(f"{indent}evidence: {evidence_text}")


def _append_actionable(lines: list[str], issue: dict) -> None:
    where = f" [query_id {issue['query_id']}]" if issue.get("query_id") is not None else ""
    lines.append(f"  • {issue.get('domain')}/{issue.get('metric')}{where} — {issue.get('summary')}")
    _append_evidence(lines, issue)
    lines.append(f"      → {issue.get('recommended_action')}  (owner: {issue['owner']})")


def _append_observation(lines: list[str], issue: dict) -> None:
    where = f" [query_id {issue['query_id']}]" if issue.get("query_id") is not None else ""
    lines.append(f"  • {issue.get('domain')}/{issue.get('metric')}{where} — {issue.get('summary')}")
    _append_evidence(lines, issue)
    lines.append("      informational only — no corrective action or owner handoff.")


def _append_inconclusive(lines: list[str], issue: dict) -> None:
    where = f" [query_id {issue['query_id']}]" if issue.get("query_id") is not None else ""
    lines.append(f"  • {issue.get('domain')}/{issue.get('metric')}{where} — {issue.get('summary')}")
    _append_evidence(lines, issue)
    lines.append(f"      → {issue['next_step']}  (no owner handoff)")


def render_text(report: dict) -> str:
    header = f"Triage Report — {report.get('database_name') or 'database'} ({report.get('mode') or 'triage'})"
    if report.get("symptom"):
        header += f" — {report['symptom']}"

    if report["healthy"]:
        lines = [
            f"{header}: healthy — no actionable findings crossed a triage threshold."
        ]
    else:
        lines = [
            f"{header} — {report['actionable_count']} actionable finding(s)",
        ]

    lines.append(
        "Summary: "
        f"actionable={report['actionable_count']} "
        f"({ _counts_text(report.get('actionable_by_severity', {})) }); "
        f"observations={report['observation_count']}; "
        f"incomplete evidence={report['inconclusive_count']}; "
        f"owners={_counts_text(report.get('actionable_by_owner', {}))}"
    )
    if report.get("capabilities"):
        lines.append(f"Capabilities: {report['capabilities']}")

    actionable = report.get("actionable_findings", [])
    if actionable:
        lines.append("")
        current = None
        for issue in actionable:
            if issue["severity"] != current:
                current = issue["severity"]
                lines.append(current.upper())
            _append_actionable(lines, issue)

    observations = report.get("observations", [])
    if observations:
        lines.extend(("", "OBSERVATIONS"))
        for issue in observations:
            _append_observation(lines, issue)

    incomplete = report.get("incomplete_evidence", [])
    if incomplete:
        lines.extend(("", "INCONCLUSIVE EVIDENCE"))
        for issue in incomplete:
            _append_inconclusive(lines, issue)

    if not report["total_findings"]:
        lines[0] += " no findings were returned."
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Render a read-only Triage Report.")
    parser.add_argument("--input", "-i", default="-", help="Findings JSON list; '-' for stdin.")
    parser.add_argument("--database", default=None, help="Database name for the header.")
    parser.add_argument("--mode", default="triage", help="Incident mode: triage or sweep.")
    parser.add_argument("--symptom", default=None, help="Reported symptom, if any.")
    parser.add_argument("--capabilities", default=None, help="Capability summary from check_capabilities.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON.")
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
    findings = payload.get("findings", payload) if isinstance(payload, dict) else payload

    try:
        report = build_report(
            findings,
            database_name=args.database,
            mode=args.mode,
            symptom=args.symptom,
            capabilities=args.capabilities,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2) if args.json else render_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
