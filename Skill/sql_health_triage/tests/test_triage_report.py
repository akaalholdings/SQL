"""Severity and evidence classification are the report's contract."""

import json

import pytest

import triage_report as tr
from triage_report import build_report, classify, render_text


def finding(**overrides):
    base = {
        "domain": "waits",
        "metric": "wait_ms",
        "value": 10.0,
        "threshold": 100.0,
        "summary": "an observation",
        "recommended_action": "look closer",
        "owner": "human",
        "evidence": {"tool": "get_wait_stats", "window_minutes": 60, "truncated": False},
    }
    base.update(overrides)
    return base


# --- one test per severity rule ---

def test_resource_at_ceiling_is_critical():
    assert classify(finding(domain="resource", metric="avg_cpu_percent",
                            value=94.0, threshold=100.0)) == "critical"


def test_resource_below_ceiling_is_not_critical():
    assert classify(finding(domain="resource", metric="avg_cpu_percent",
                            value=60.0, threshold=100.0)) == "info"


def test_active_blocking_is_high():
    assert classify(finding(domain="blocking", metric="waiting_tasks", value=14)) == "high"


def test_deadlocks_in_window_are_high():
    assert classify(finding(domain="deadlock", metric="deadlocks_24h", value=3)) == "high"


def test_zero_blocking_or_deadlocks_is_info():
    assert classify(finding(domain="blocking", metric="waiting_tasks", value=0)) == "info"
    assert classify(finding(domain="deadlock", metric="deadlocks_24h", value=0)) == "info"


def test_tempdb_at_80_percent_is_high():
    assert classify(finding(domain="tempdb", metric="used_mb",
                            value=820.0, threshold=1024.0)) == "high"
    assert classify(finding(domain="tempdb", metric="used_mb",
                            value=300.0, threshold=1024.0)) == "info"


def test_pending_memory_grants_are_high():
    assert classify(finding(domain="memory", metric="pending_memory_grants", value=2)) == "high"
    assert classify(finding(domain="memory", metric="pending_memory_grants", value=0)) == "info"


def test_compile_pressure_is_medium():
    assert classify(finding(domain="compile", metric="compiles_per_batch",
                            value=0.9, threshold=0.5)) == "medium"


def test_any_exceeded_threshold_is_at_least_medium():
    assert classify(finding(domain="io", metric="avg_write_latency_ms",
                            value=25.0, threshold=10.0)) == "medium"


def test_in_range_observation_is_info():
    assert classify(finding(domain="io", metric="avg_write_latency_ms",
                            value=2.0, threshold=10.0)) == "info"


def test_all_info_findings_are_healthy_observations():
    report = build_report([
        finding(domain="io", metric="avg_write_latency_ms", value=2.0, threshold=10.0),
        finding(domain="waits", metric="wait_ms", value=5.0, threshold=100.0),
    ], database_name="awlt_prod")

    assert report["healthy"] is True
    assert report["actionable_findings"] == []
    assert len(report["observations"]) == 2
    assert report["incomplete_evidence"] == []
    assert report["total_findings"] == 2
    assert report["actionable_by_severity"] == {}
    assert report["by_severity"] == {"info": 2}
    assert "healthy" in render_text(report)
    assert "OBSERVATIONS" in render_text(report)


def test_observation_does_not_require_a_corrective_action():
    observation = finding(value=2.0, threshold=10.0)
    observation.pop("recommended_action")
    report = build_report([observation])
    assert report["observations"][0]["status"] == "observation"


def test_actionable_finding_requires_a_corrective_action():
    actionable = finding(domain="blocking", metric="waiting_tasks", value=2)
    actionable.pop("recommended_action")
    with pytest.raises(ValueError, match="recommended_action"):
        build_report([actionable])


def test_truncated_evidence_is_inconclusive_and_cannot_route_action():
    truncated = finding(
        domain="resource",
        metric="avg_cpu_percent",
        value=99.0,
        threshold=100.0,
        owner="sql-optimizer",
        recommended_action="HANDOFF_THIS_QUERY",
        evidence={"tool": "get_resource_stats_history", "truncated": True},
    )

    assert classify(truncated) == "inconclusive"
    report = build_report([truncated], database_name="awlt_prod")
    issue = report["incomplete_evidence"][0]

    assert report["healthy"] is True
    assert report["actionable_findings"] == []
    assert issue["severity"] == "inconclusive"
    assert issue["status"] == "inconclusive"
    assert issue["owner"] is None
    assert issue["recommended_action"] is None
    assert "narrow or re-query" in issue["next_step"]
    assert report["actionable_by_owner"] == {}


def test_nested_or_string_truncation_is_inconclusive():
    candidate = finding(
        evidence={
            "tool": "get_resource_stats_history",
            "page": {"metadata": {"truncated": "true"}},
        }
    )
    assert classify(candidate) == "inconclusive"


def test_nonfinite_or_boolean_values_are_rejected():
    for value in (float("nan"), float("inf"), True):
        with pytest.raises(ValueError, match="value must be numeric"):
            build_report([finding(value=value)])


def test_threshold_is_optional_when_the_rule_does_not_need_one():
    candidate = finding(domain="blocking", metric="waiting_tasks", value=2)
    candidate.pop("threshold")
    report = build_report([candidate])
    assert report["findings"][0]["severity"] == "high"


def test_render_separates_actions_observations_and_incomplete_evidence():
    report = build_report([
        finding(
            domain="resource",
            metric="avg_cpu_percent",
            value=95.0,
            threshold=100.0,
            owner="sql-optimizer",
            summary="CPU crossed the actionable ceiling",
            recommended_action="OPTIMIZE_ACTIONABLE_QUERY",
        ),
        finding(
            domain="io",
            metric="avg_write_latency_ms",
            value=2.0,
            threshold=10.0,
            summary="IO remains within the limit",
            recommended_action="DO_NOT_RENDER_OBSERVATION_ACTION",
        ),
        finding(
            domain="blocking",
            metric="waiting_tasks",
            value=14,
            owner="human",
            summary="The truncated result cannot prove the blocking chain",
            recommended_action="DO_NOT_RENDER_TRUNCATED_ACTION",
            evidence={"tool": "get_currently_waiting_tasks", "truncated": True},
        ),
    ], database_name="awlt_prod")

    text = render_text(report)
    assert "OPTIMIZE_ACTIONABLE_QUERY" in text
    assert "OBSERVATIONS" in text
    assert "INCONCLUSIVE EVIDENCE" in text
    assert "truncated=true" in text
    assert "narrow or re-query" in text
    assert "DO_NOT_RENDER_OBSERVATION_ACTION" not in text
    assert "DO_NOT_RENDER_TRUNCATED_ACTION" not in text
    assert "no owner handoff" in text


# --- report assembly ---

def test_report_orders_critical_first_and_counts():
    report = build_report([
        finding(domain="io", metric="latency", value=25.0, threshold=10.0),        # medium
        finding(domain="resource", value=95.0, threshold=100.0),                    # critical
        finding(domain="blocking", metric="waiting_tasks", value=5),                # high
    ], database_name="awlt_prod")
    severities = [f["severity"] for f in report["findings"]]
    assert severities == ["critical", "high", "medium"]
    assert report["by_severity"] == {"critical": 1, "high": 1, "medium": 1}
    assert report["healthy"] is False


def test_unknown_owner_defaults_to_human_and_counts_by_owner():
    report = build_report([
        finding(owner="sql-optimizer", query_id=42),
        finding(owner="somebody_else"),
    ])
    owners = [f["owner"] for f in report["findings"]]
    assert set(owners) == {"sql-optimizer", "human"}
    assert report["by_owner"]["human"] == 1


def test_empty_findings_render_healthy():
    report = build_report([], database_name="awlt_prod")
    assert report["healthy"] is True
    text = render_text(report)
    assert "no findings" in text


def test_report_rejects_findings_without_tool_evidence():
    incomplete = finding()
    incomplete.pop("evidence")
    with pytest.raises(ValueError, match="evidence.tool"):
        build_report([incomplete], database_name="awlt_prod")


def test_render_includes_owner_query_id_and_severity_groups():
    report = build_report([
        finding(domain="blocking", metric="waiting_tasks", value=5,
                query_id=87, owner="human",
                summary="14 sessions behind SPID 63",
                recommended_action="KILL 63 recommended"),
    ], database_name="awlt_prod")
    text = render_text(report)
    assert "HIGH" in text
    assert "query_id 87" in text
    assert "(owner: human)" in text
    assert "KILL 63 recommended" in text


def test_cli_json_roundtrip(tmp_path, capsys):
    findings_file = tmp_path / "findings.json"
    findings_file.write_text(json.dumps([finding(domain="deadlock", value=2)]), "utf-8")
    rc = tr.main(["triage_report.py", "--input", str(findings_file),
                  "--database", "awlt_prod", "--json"])
    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["database_name"] == "awlt_prod"
    assert report["findings"][0]["severity"] == "high"


def test_cli_missing_input_is_reported_without_traceback(tmp_path, capsys):
    missing = tmp_path / "missing.json"
    assert tr.main(["triage_report.py", "--input", str(missing)]) == 1
    assert "could not parse input" in capsys.readouterr().err


def test_cli_rejects_non_list_findings_without_traceback(tmp_path, capsys):
    payload = tmp_path / "findings.json"
    payload.write_text("null", encoding="utf-8")

    assert tr.main(["triage_report.py", "--input", str(payload)]) == 1
    assert capsys.readouterr().err.strip() == "findings must be a list"
