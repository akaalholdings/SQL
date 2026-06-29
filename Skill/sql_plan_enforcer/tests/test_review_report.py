"""Review mode is monitor-only: the report must classify every issue, rank failing forced
plans (the unforce-failed condition) as the most severe, echo their exact unforce command as
*informational*, and never mutate its input (no side effects = nothing applied)."""

from review_report import build_report, classify, render_text


def failing_forced(query_id, failures=3, reason="GENERAL_FAILURE"):
    return {
        "category": "stale_forced",
        "query_id": query_id,
        "force_failure_count": failures,
        "last_force_failure_reason": reason,
        "proposed_lever": "unforce_plan",
        "eligible": True,
        "score": 3,
        "unforce_command": f"EXEC sys.sp_query_store_unforce_plan @query_id = {query_id}, @plan_id = 7;",
    }


def regression(query_id, pct=2.0, eligible=True):
    return {"category": "regression", "query_id": query_id, "regression_pct": pct,
            "proposed_plan_id": 11, "proposed_lever": "force_plan", "eligible": eligible,
            "score": pct * 1000}


def param_sensitive(query_id, cv=1.2):
    return {"category": "param_sensitive", "query_id": query_id,
            "coefficient_of_variation": cv, "proposed_lever": "set_hints",
            "eligible": True, "score": 500}


def top_consumer(query_id):
    return {"category": "top_consumer", "query_id": query_id, "total_cost": 90000,
            "proposed_lever": "handoff_optimizer", "eligible": True, "score": 90000}


def test_failing_forced_plan_is_critical_even_though_category_is_stale_forced():
    issue_type, severity = classify(failing_forced(1))
    assert issue_type == "failing_forced_plan"
    assert severity == "critical"


def test_severity_by_category():
    assert classify(regression(1))[1] == "high"
    assert classify(param_sensitive(1))[1] == "medium"
    assert classify(top_consumer(1))[1] == "low"
    assert classify({"category": "stale_forced", "query_id": 1, "force_failure_count": 0})[1] == "medium"


def test_report_orders_critical_first():
    report = build_report([top_consumer(1), regression(2), failing_forced(3)])
    assert report["issues"][0]["query_id"] == 3
    assert report["issues"][0]["severity"] == "critical"
    assert report["by_severity"] == {"critical": 1, "high": 1, "low": 1}


def test_failing_forced_carries_unforce_command_and_reason():
    report = build_report([failing_forced(42, failures=5, reason="GENERAL_FAILURE")])
    issue = report["issues"][0]
    assert "sp_query_store_unforce_plan @query_id = 42" in issue["unforce_command"]
    assert issue["last_force_failure_reason"] == "GENERAL_FAILURE"
    assert "force_failure_count=5" in issue["summary"]


def test_build_report_does_not_mutate_input():
    candidates = [regression(1), failing_forced(2)]
    snapshot = [dict(c) for c in candidates]
    build_report(candidates)
    assert candidates == snapshot  # review mode has no side effects


def test_render_text_groups_and_shows_unforce_command():
    text = render_text(build_report([failing_forced(42), regression(2)]))
    assert "CRITICAL" in text and "HIGH" in text
    assert "Plan Health Report" in text
    assert "would run: EXEC sys.sp_query_store_unforce_plan @query_id = 42" in text


def test_ineligible_item_marked_monitoring_only():
    text = render_text(build_report([regression(1, pct=0.1, eligible=False)]))
    assert "monitoring only" in text


def test_handoff_rendered_as_optimizer_handoff():
    text = render_text(build_report([top_consumer(9)]))
    assert "hand off to sql_optimizer" in text
