"""The keep/rollback/hold rule is the auto-rollback safety net: it must keep only
demonstrable wins, revert regressions AND no-op changes, and refuse to judge on thin data."""

import json

import verify_decision as vd
from verify_decision import Thresholds, decide, improvement_pct

# Defaults: min 30 executions, +20% to keep, -10% to force rollback.
T = Thresholds()
EXPECTED = {"environment": "awlt_prod", "query_id": 42, "plan_id": 7}


def _m(duration, cpu=None, reads=None, execs=200):
    metric = {"avg_duration": duration, "count_executions": execs}
    if cpu is not None:
        metric["avg_cpu_time"] = cpu
    if reads is not None:
        metric["avg_logical_io_reads"] = reads
    metric["evidence"] = {
        "source": "query_store",
        "environment": "awlt_prod",
        "query_id": 42,
        "plan_id": 7,
        "window_start": "2026-06-28T10:00:00Z",
        "window_end": "2026-06-28T11:00:00Z",
        "post_change": duration != 1000,
    }
    if duration != 1000:
        metric["evidence"]["window_start"] = "2026-06-28T12:00:00Z"
        metric["evidence"]["window_end"] = "2026-06-28T13:00:00Z"
    return metric


def test_clear_win_is_kept():
    # 1000us -> 400us is a 60% improvement, well over the 20% keep floor.
    d = decide(_m(1000), _m(400), T, EXPECTED)
    assert d.action == "keep"
    assert d.improvement_pct == 0.6


def test_regression_is_rolled_back():
    # Forced plan made it 40% slower -> immediate rollback.
    d = decide(_m(1000), _m(1400), T, EXPECTED)
    assert d.action == "rollback"
    assert "avg_duration" in d.regressed_metrics


def test_no_meaningful_change_is_rolled_back():
    # 1000us -> 950us is only 5%: real but below the keep floor. An autonomous loop
    # does not leave an unhelpful control in place, so this reverts (not "keep").
    d = decide(_m(1000), _m(950), T, EXPECTED)
    assert d.action == "rollback"
    assert "no meaningful improvement" in d.reason


def test_insufficient_executions_holds():
    # Duration looks great but only 5 executions since apply -> too thin to judge.
    d = decide(_m(1000), _m(300, execs=5), T, EXPECTED)
    assert d.action == "hold"


def test_regression_on_secondary_metric_overrides_duration_win():
    # Duration improved, but logical reads blew up 50% -> rollback wins.
    baseline = _m(1000, cpu=500, reads=1000)
    candidate = _m(700, cpu=500, reads=1500)
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "rollback"
    assert "avg_logical_io_reads" in d.regressed_metrics


def test_unusable_baseline_holds():
    d = decide(_m(0), _m(400), T, EXPECTED)
    assert d.action == "hold"


def test_thresholds_are_tunable():
    # With a 70% keep floor, a 60% win is no longer enough to keep.
    strict = Thresholds(min_improvement_pct=0.70)
    d = decide(_m(1000), _m(400), strict, EXPECTED)
    assert d.action == "rollback"


def test_improvement_pct_helper():
    assert improvement_pct(1000, 400) == 0.6
    assert improvement_pct(0, 400) is None
    assert improvement_pct(None, 400) is None


def test_missing_provenance_holds():
    baseline = {"avg_duration": 1000, "count_executions": 200}
    candidate = {"avg_duration": 400, "count_executions": 200}
    d = decide(baseline, candidate, T)
    assert d.action == "hold"
    assert "evidence" in d.reason or "provenance" in d.reason


def test_overlapping_windows_hold():
    baseline = _m(1000)
    candidate = _m(400)
    candidate["evidence"]["window_start"] = "2026-06-28T10:30:00Z"
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "hold"
    assert "overlap" in d.reason


def test_expected_plan_mismatch_holds():
    baseline = _m(1000)
    candidate = _m(400)
    candidate["evidence"]["plan_id"] = 8
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "hold"
    assert "plan_id" in d.reason


def test_expected_environment_mismatch_on_baseline_holds():
    baseline = _m(1000)
    candidate = _m(400)
    baseline["evidence"]["environment"] = "awlt_dev"
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "hold"
    assert "baseline" in d.reason and "environment" in d.reason


def test_expected_query_mismatch_on_candidate_holds():
    baseline = _m(1000)
    candidate = _m(400)
    candidate["evidence"]["query_id"] = 99
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "hold"
    assert "query_id" in d.reason


def test_metric_level_truncation_holds():
    baseline = _m(1000)
    candidate = _m(400)
    candidate["truncated"] = True
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "hold"
    assert "truncated" in d.reason


def test_evidence_level_truncation_holds():
    baseline = _m(1000)
    candidate = _m(400)
    baseline["evidence"]["truncated"] = "true"
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "hold"
    assert "truncated" in d.reason


def test_nested_provenance_is_checked_on_both_sides():
    baseline = _m(1000)
    candidate = _m(400)
    for metric in (baseline, candidate):
        evidence = metric["evidence"]
        nested = {key: evidence.pop(key) for key in ("environment", "query_id", "plan_id")}
        evidence["provenance"] = nested
    d = decide(baseline, candidate, T, EXPECTED)
    assert d.action == "keep"


def test_nested_list_truncation_and_missing_null_plan_provenance_hold():
    baseline = _m(1000)
    candidate = _m(400)
    candidate["evidence"]["pages"] = [{"truncated": True}]
    assert decide(baseline, candidate, T, EXPECTED).action == "hold"

    baseline = _m(1000)
    candidate = _m(400)
    expected = {"environment": "awlt_prod", "query_id": 42, "plan_id": None}
    for metric in (baseline, candidate):
        metric["evidence"].pop("plan_id")
    decision = decide(baseline, candidate, T, expected)
    assert decision.action == "hold"
    assert "missing plan_id" in decision.reason


def test_environment_match_is_case_insensitive_and_invalid_numbers_hold():
    baseline = _m(1000)
    candidate = _m(400)
    baseline["evidence"]["environment"] = "AWLT_PROD"
    assert decide(baseline, candidate, T, EXPECTED).action == "keep"

    candidate = _m(400)
    candidate["count_executions"] = float("nan")
    assert decide(_m(1000), candidate, T, EXPECTED).action == "hold"

    invalid_expected = {"environment": "awlt_prod", "query_id": 0, "plan_id": 7}
    assert decide(_m(1000), _m(400), T, invalid_expected).action == "hold"


def test_present_secondary_metrics_must_be_comparable_and_finite():
    baseline = _m(1000, cpu=500)
    candidate = _m(400)
    decision = decide(baseline, candidate, T, EXPECTED)
    assert decision.action == "hold"
    assert "missing" in decision.reason

    candidate = _m(400, cpu=float("nan"))
    decision = decide(baseline, candidate, T, EXPECTED)
    assert decision.action == "hold"
    assert "avg_cpu_time" in decision.reason


def test_provenance_cannot_be_disabled_by_environment(monkeypatch):
    monkeypatch.setenv("SQL_PLAN_ENFORCER_REQUIRE_PROVENANCE", "0")
    d = decide(
        {"avg_duration": 1000, "count_executions": 200},
        {"avg_duration": 400, "count_executions": 200},
        T,
        EXPECTED,
    )
    assert d.action == "hold"


def test_cli_rejects_non_object_payload(tmp_path, capsys):
    payload = tmp_path / "payload.json"
    payload.write_text(json.dumps([]), encoding="utf-8")
    assert vd.main(["verify_decision.py", "--input", str(payload)]) == 1
    assert "top-level value must be an object" in capsys.readouterr().err
