"""Ranking decides what the autonomous loop touches first and what it ignores:
a failing forced plan must surface above everything, a big regression must beat a stable
top consumer, and sub-threshold noise must be marked ineligible (never auto-applied)."""

from scan_rank import Thresholds, normalize, rank, total_cost


def regression(query_id, reg_pct, execs=500, dur=5000):
    return {
        "category": "regression",
        "environment": "awlt_prod",
        "query_id": query_id,
        "count_executions": execs,
        "avg_duration": dur,
        "regression_pct": reg_pct,
        "proposed_plan_id": 11,
    }


def top_consumer(query_id, execs=10000, dur=8000):
    return {
        "category": "top_consumer",
        "environment": "awlt_prod",
        "query_id": query_id,
        "count_executions": execs,
        "avg_duration": dur,
    }


def param_sensitive(query_id, dur=4000, stdev=4000, execs=500):
    return {
        "category": "param_sensitive",
        "environment": "awlt_prod",
        "query_id": query_id,
        "count_executions": execs,
        "avg_duration": dur,
        "stdev_duration": stdev,
    }


def stale_forced(query_id, failures=0, execs=500, dur=3000):
    return {
        "category": "stale_forced",
        "environment": "awlt_prod",
        "query_id": query_id,
        "current_plan_id": 20 + query_id,
        "count_executions": execs,
        "avg_duration": dur,
        "force_failure_count": failures,
    }


def _by_id(ranked):
    return [c["query_id"] for c in ranked]


def test_failing_forced_plan_ranks_first():
    ranked = rank([top_consumer(1), regression(2, 2.0), stale_forced(3, failures=4)])
    assert ranked[0]["query_id"] == 3
    assert ranked[0]["tier"] == 0


def test_regression_outranks_stable_top_consumer():
    # The top consumer has higher raw cost, but a real regression is the more urgent fix.
    ranked = rank([top_consumer(1), regression(2, 2.0)])
    assert _by_id(ranked) == [2, 1]


def test_subthreshold_regression_is_ineligible():
    ranked = rank([regression(1, 0.10)])  # 10% < 50% floor
    assert ranked[0]["eligible"] is False
    assert "below" in ranked[0]["reason"]


def test_low_execution_count_is_ineligible():
    ranked = rank([top_consumer(1, execs=5)])  # below 30-execution floor
    assert ranked[0]["eligible"] is False


def test_param_sensitive_needs_real_variation_and_duration():
    stable = param_sensitive(1, dur=4000, stdev=200)  # cv 0.05 -> ineligible
    spiky = param_sensitive(2, dur=4000, stdev=4000)  # cv 1.0 -> eligible
    trivial = param_sensitive(3, dur=100, stdev=200)  # below duration floor
    annotated = {c["query_id"]: c for c in rank([stable, spiky, trivial])}
    assert annotated[1]["eligible"] is False
    assert annotated[2]["eligible"] is True
    assert annotated[3]["eligible"] is False


def test_eligible_sorts_ahead_of_ineligible_in_same_tier():
    big = regression(1, 3.0)
    small = regression(2, 0.10)  # ineligible
    ranked = rank([small, big])
    assert ranked[0]["query_id"] == 1 and ranked[0]["eligible"]
    assert ranked[1]["query_id"] == 2 and not ranked[1]["eligible"]


def test_default_levers_by_category():
    annotated = {c["query_id"]: c for c in rank(
        [regression(1, 2.0), param_sensitive(2), stale_forced(3, failures=1)]
    )}
    assert annotated[1]["proposed_lever"] == "force_plan"
    assert annotated[2]["proposed_lever"] == "set_hints"
    assert annotated[3]["proposed_lever"] == "unforce_plan"


def test_top_consumer_without_alternate_plan_hands_off_to_optimizer():
    candidate = {"category": "top_consumer", "environment": "awlt_prod", "query_id": 9, "count_executions": 9000,
                 "avg_duration": 5000}
    assert normalize(candidate)["proposed_lever"] == "handoff_optimizer"


def test_unknown_category_is_ineligible():
    ranked = rank([{"category": "mystery", "query_id": 1}])
    assert ranked[0]["eligible"] is False


def test_invalid_identity_plan_and_nested_truncation_are_ineligible():
    assert rank([top_consumer(0)])[0]["eligible"] is False
    no_plan = regression(1, 2.0)
    no_plan.pop("proposed_plan_id")
    assert "proposed_plan_id" in rank([no_plan])[0]["reason"]
    nested = top_consumer(2)
    nested["evidence"] = {"pages": [{"truncated": True}]}
    assert rank([nested])[0]["eligible"] is False
    assert rank(["not-an-object"])[0]["eligible"] is False


def test_thresholds_are_tunable():
    strict = Thresholds(min_regression_pct=3.0)
    ranked = rank([regression(1, 2.0)], strict)  # 200% < 300% strict floor
    assert ranked[0]["eligible"] is False


def test_total_cost_is_executions_times_duration():
    assert total_cost({"count_executions": 100, "avg_duration": 50}) == 5000
