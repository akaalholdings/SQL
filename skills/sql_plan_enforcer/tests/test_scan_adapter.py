"""The adapter is the contract between azure-sql-mcp's scan tools and scan_rank's
candidate schema: units must convert correctly (ms→µs where the server reports ms),
string ids must coerce to ints, and adapted candidates must flow through scan_rank
unchanged. Fixture payloads mirror the exact field names the server returns
(azure_sql_mcp/query_regression.py, query_store.py, plan_enforcement.py)."""

import json

import pytest

import scan_adapter
from scan_adapter import adapt, detect_source
from scan_rank import rank

PARAM_SNIFFING = {
    "database_name": "awlt_dev",
    "window_minutes": 1440,
    "variance_threshold": 10.0,
    "affected_query_count": 1,
    "queries": [
        {
            "query_id": 42,
            "query_sql_text": "SELECT ...",
            "plan_count": 3,
            "best_avg_duration_ms": 2.0,
            "worst_avg_duration_ms": 200.0,
            "duration_variance_ratio": 100.0,
            "best_avg_cpu_ms": 1.5,
            "worst_avg_cpu_ms": 150.0,
            "total_executions": 500,
            "best_plan_id": 7,
            "worst_plan_id": 9,
        }
    ],
}

TOP_QUERIES = {
    "database_name": "awlt_dev",
    "query_store_status": {"actual_state_desc": "READ_WRITE"},
    "sort_by": "total_duration",
    "window_minutes": 10080,
    "rows": [
        {
            "query_id": 10,
            "plan_id": 100,
            "query_sql_text": "SELECT ...",
            "executions": 300,
            "total_duration_us": 3_000_000.0,
            "avg_duration_us": 10_000.0,
            "total_cpu_us": 2_000_000.0,
            "total_logical_io_reads": 90_000.0,
            "total_physical_io_reads": 10.0,
        },
        {
            "query_id": 10,
            "plan_id": 101,
            "query_sql_text": "SELECT ...",
            "executions": 100,
            "total_duration_us": 2_000_000.0,
            "avg_duration_us": 20_000.0,
            "total_cpu_us": 1_000_000.0,
            "total_logical_io_reads": 30_000.0,
            "total_physical_io_reads": 5.0,
        },
    ],
    "row_count": 2,
    "truncated": False,
}

REGRESSED = {
    "database_name": "awlt_dev",
    "window_minutes": 1440,
    "recommendation_count": 1,
    "recommendations": [
        {
            "reason": "Average query CPU time changed from 1ms to 10ms",
            "score": 83,
            "current_state": "Active",
            "query_id": "42",  # JSON_VALUE returns strings
            "regressed_plan_id": "9",
            "recommended_plan_id": "7",
            "estimated_cpu_gain": "170.5",
            "estimated_duration_gain": "120.2",
            "recent_execution_count": 250,
            "details": {
                "planForceDetails": {
                    "queryId": 42,
                    "regressedPlanId": 9,
                    "recommendedPlanId": 7,
                    "regressedPlanExecutionCount": 48,
                    "recommendedPlanExecutionCount": 300,
                    "regressedPlanCpuTimeAverage": 10_000.0,
                    "recommendedPlanCpuTimeAverage": 1_000.0,
                }
            },
        }
    ],
}

FORCED_PLANS = {
    "database_name": "awlt_dev",
    "window_minutes": 1440,
    "forced_plan_count": 3,
    "stale_count": 1,
    "failing_count": 1,
    "forced_plans": [
        {   # failing → candidate (tier 0 material)
            "plan_id": 7, "query_id": 42, "is_forced_plan": True,
            "plan_forcing_type_desc": "MANUAL",
            "force_failure_count": 12,
            "last_force_failure_reason_desc": "GENERAL_FAILURE",
            "avg_duration_ms": 50.0, "avg_cpu_ms": 40.0,
            "avg_logical_io_reads": 900, "count_executions": 400,
            "recent_execution_count": 40, "days_since_last_exec": 0,
        },
        {   # stale → candidate
            "plan_id": 8, "query_id": 43, "is_forced_plan": True,
            "plan_forcing_type_desc": "MANUAL",
            "force_failure_count": 0, "last_force_failure_reason_desc": "NONE",
            "avg_duration_ms": 5.0, "avg_cpu_ms": 4.0,
            "avg_logical_io_reads": 90, "count_executions": 10,
            "recent_execution_count": 0, "days_since_last_exec": 45,
        },
        {   # healthy → NOT a candidate
            "plan_id": 9, "query_id": 44, "is_forced_plan": True,
            "plan_forcing_type_desc": "MANUAL",
            "force_failure_count": 0, "last_force_failure_reason_desc": "NONE",
            "avg_duration_ms": 5.0, "avg_cpu_ms": 4.0,
            "avg_logical_io_reads": 90, "count_executions": 500,
            "recent_execution_count": 200, "days_since_last_exec": 0,
        },
    ],
    "warnings": [],
}

PLAN_HEALTH_REVIEW = {
    "database_name": "awlt_dev",
    "mode": "review",
    "window_minutes": 1440,
    "top_n": 20,
    "regression_recommendation_count": 1,
    "forced_plan_count": 1,
    "recommended_action_count": 2,
    "recommended_actions": [
        {"action": "force", "query_id": 42, "plan_id": 7, "reason": "query_store_regression",
         "score": 83.0, "priority": 100, "rank": 1},
        {"action": "unforce", "query_id": 50, "plan_id": 11, "reason": "forced_plan_failure",
         "score": 6.0, "priority": 90, "rank": 2,
         "last_force_failure_reason_desc": "GENERAL_FAILURE"},
    ],
    "forced_plan_warnings": [],
}


def test_detect_source_by_signature_keys():
    assert detect_source(PARAM_SNIFFING) == "detect_parameter_sniffing"
    assert detect_source(TOP_QUERIES) == "get_top_queries"
    assert detect_source(REGRESSED) == "detect_regressed_queries"
    assert detect_source(FORCED_PLANS) == "get_forced_plans"
    assert detect_source(PLAN_HEALTH_REVIEW) == "plan_health_review"
    assert detect_source({"unrelated": True}) is None


def test_unknown_payload_raises_with_source_hint():
    with pytest.raises(ValueError, match="--source"):
        adapt({"unrelated": True})


def test_explicit_source_still_rejects_non_object_payload():
    with pytest.raises(ValueError, match="JSON object"):
        adapt([], source="get_top_queries")


def test_param_sniffing_converts_ms_to_us():
    [candidate] = adapt(PARAM_SNIFFING)
    assert candidate["category"] == "param_sensitive"
    assert candidate["proposed_lever"] == "set_hints"
    assert candidate["query_id"] == 42
    assert candidate["count_executions"] == 500
    # (2 + 200)/2 ms -> 101,000 µs; spread proxy (200-2)/2 ms -> 99,000 µs
    assert candidate["avg_duration"] == pytest.approx(101_000.0)
    assert candidate["stdev_duration"] == pytest.approx(99_000.0)
    assert candidate["adapted_from"] == "detect_parameter_sniffing"


def test_top_queries_aggregates_per_query_already_in_us():
    [candidate] = adapt(TOP_QUERIES)
    assert candidate["category"] == "top_consumer"
    assert candidate["query_id"] == 10
    assert candidate["count_executions"] == 400  # 300 + 100 across plans
    assert candidate["distinct_plans"] == 2
    # executions-weighted: 5,000,000 µs total over 400 executions
    assert candidate["avg_duration"] == pytest.approx(12_500.0)
    assert candidate["total_logical_reads"] == pytest.approx(120_000.0)


def test_regressed_coerces_string_ids_and_computes_real_regression():
    [candidate] = adapt(REGRESSED)
    assert candidate["category"] == "regression"
    assert candidate["query_id"] == 42          # was the string "42"
    assert candidate["proposed_plan_id"] == 7   # was the string "7"
    assert candidate["proposed_lever"] == "force_plan"
    # (10000 - 1000) / 1000 from details.planForceDetails — not fabricated
    assert candidate["regression_pct"] == pytest.approx(9.0)
    assert candidate["count_executions"] == 48  # the regressed plan's executions


def test_regressed_without_force_details_omits_regression_pct():
    stripped = json.loads(json.dumps(REGRESSED))
    stripped["recommendations"][0].pop("details")
    [candidate] = adapt(stripped)
    assert "regression_pct" not in candidate
    assert candidate["count_executions"] == 250  # falls back to recent_execution_count


def test_regressed_unknown_automatic_tuning_state_is_review_only():
    payload = json.loads(json.dumps(REGRESSED))
    payload["recommendations"][0]["current_state"] = "future_engine_state"
    [candidate] = adapt(payload)
    assert candidate["automatic_tuning_state"] == "future_engine_state"
    assert candidate["review_only"] is True
    assert rank([candidate])[0]["eligible"] is False


def test_regressed_recommendations_remain_review_only_for_every_state():
    payload = json.loads(json.dumps(REGRESSED))
    payload["recommendations"][0]["current_state"] = "Inactive"
    [candidate] = adapt(payload)
    assert candidate["automatic_tuning_review_only"] is True
    assert rank([candidate])[0]["eligible"] is False


def test_adapter_propagates_truncation_to_rank():
    payload = json.loads(json.dumps(TOP_QUERIES))
    payload["truncated"] = True
    [candidate] = adapt(payload)
    assert candidate["truncated"] is True
    assert rank([candidate])[0]["eligible"] is False


def test_forced_plans_keeps_failing_and_stale_drops_healthy():
    candidates = adapt(FORCED_PLANS)
    by_query = {c["query_id"]: c for c in candidates}
    assert set(by_query) == {42, 43}  # healthy 44 dropped
    assert by_query[42]["force_failure_count"] == 12
    assert by_query[42]["avg_duration"] == pytest.approx(50_000.0)  # ms -> µs
    assert by_query[43]["days_since_last_exec"] == 45
    assert all(c["proposed_lever"] == "unforce_plan" for c in candidates)


def test_automatic_or_unknown_forced_plan_ownership_is_review_only():
    payload = json.loads(json.dumps(FORCED_PLANS))
    payload["forced_plans"][0]["plan_forcing_type_desc"] = "AUTO"
    payload["forced_plans"][1].pop("plan_forcing_type_desc")
    ranked = {c["query_id"]: c for c in rank(adapt(payload))}
    assert ranked[42]["eligible"] is False
    assert ranked[42]["automatic_tuning_review_only"] is True
    assert ranked[43]["eligible"] is False


def test_plan_health_review_dispatches_actions():
    candidates = adapt(PLAN_HEALTH_REVIEW)
    by_query = {c["query_id"]: c for c in candidates}
    assert by_query[42]["category"] == "regression"
    assert by_query[42]["proposed_plan_id"] == 7
    assert by_query[50]["category"] == "stale_forced"
    assert by_query[50]["force_failure_count"] == 6.0


def test_plan_health_review_accepts_current_nested_mcp_payload():
    payload = {
        "database_name": "awlt_dev",
        "mode": "review",
        "plan_enforcement": {
            "recommended_actions": [
                {"action": "force", "query_id": 42, "plan_id": 7, "reason": "query_store_regression"}
            ]
        },
        "parameter_sniffing": {"queries": []},
    }
    assert detect_source(payload) == "plan_health_review"
    candidates = adapt(payload)
    assert candidates[0]["query_id"] == 42
    assert candidates[0]["proposed_plan_id"] == 7


def test_adapted_candidates_rank_end_to_end():
    candidates = (
        adapt(PARAM_SNIFFING) + adapt(TOP_QUERIES) + adapt(REGRESSED) + adapt(FORCED_PLANS)
    )
    ranked = rank(candidates)
    assert len(ranked) == len(candidates)
    eligible = [c for c in ranked if c["eligible"]]
    # Failing forced plan (tier 0) ranks first; the real-regression and high-CV
    # param-sensitive candidates clear their thresholds; top consumer is always eligible.
    assert ranked[0]["category"] == "stale_forced"
    assert ranked[0]["force_failure_count"] == 12
    assert {c["category"] for c in eligible} >= {
        "stale_forced", "param_sensitive", "top_consumer"
    }
    regression = next(c for c in ranked if c["category"] == "regression")
    assert regression["current_state"] == "Active"
    assert regression["automatic_tuning_state"] == "Active"
    assert regression["eligible"] is False
    assert regression["review_only"] is True


def test_adapted_param_sensitive_reports_real_cv():
    # review_report must derive the CV from stdev/avg for adapted candidates
    # (the fallback scan emits a coefficient_of_variation column; the adapter doesn't).
    from review_report import build_report
    ranked = rank(adapt(PARAM_SNIFFING))
    report = build_report(ranked)
    [issue] = report["issues"]
    assert "coefficient of variation 0.98" in issue["summary"]  # 99000/101000


def test_cli_merges_multiple_inputs(tmp_path, capsys):
    a = tmp_path / "sniff.json"
    b = tmp_path / "forced.json"
    a.write_text(json.dumps(PARAM_SNIFFING), encoding="utf-8")
    b.write_text(json.dumps(FORCED_PLANS), encoding="utf-8")

    rc = scan_adapter.main(["scan_adapter.py", "--input", str(a), "--input", str(b)])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert {c["category"] for c in payload["candidates"]} == {"param_sensitive", "stale_forced"}


def test_cli_fails_closed_on_unidentifiable_payload(tmp_path, capsys):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"unrelated": True}), encoding="utf-8")
    rc = scan_adapter.main(["scan_adapter.py", "--input", str(bad)])
    assert rc == 1
    assert "--source" in capsys.readouterr().err
