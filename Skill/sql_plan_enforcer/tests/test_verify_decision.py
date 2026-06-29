"""The keep/rollback/hold rule is the auto-rollback safety net: it must keep only
demonstrable wins, revert regressions AND no-op changes, and refuse to judge on thin data."""

from verify_decision import Thresholds, decide, improvement_pct

# Defaults: min 30 executions, +20% to keep, -10% to force rollback.
T = Thresholds()


def _m(duration, cpu=None, reads=None, execs=200):
    metric = {"avg_duration": duration, "count_executions": execs}
    if cpu is not None:
        metric["avg_cpu_time"] = cpu
    if reads is not None:
        metric["avg_logical_io_reads"] = reads
    return metric


def test_clear_win_is_kept():
    # 1000us -> 400us is a 60% improvement, well over the 20% keep floor.
    d = decide(_m(1000), _m(400), T)
    assert d.action == "keep"
    assert d.improvement_pct == 0.6


def test_regression_is_rolled_back():
    # Forced plan made it 40% slower -> immediate rollback.
    d = decide(_m(1000), _m(1400), T)
    assert d.action == "rollback"
    assert "avg_duration" in d.regressed_metrics


def test_no_meaningful_change_is_rolled_back():
    # 1000us -> 950us is only 5%: real but below the keep floor. An autonomous loop
    # does not leave an unhelpful control in place, so this reverts (not "keep").
    d = decide(_m(1000), _m(950), T)
    assert d.action == "rollback"
    assert "no meaningful improvement" in d.reason


def test_insufficient_executions_holds():
    # Duration looks great but only 5 executions since apply -> too thin to judge.
    d = decide(_m(1000), _m(300, execs=5), T)
    assert d.action == "hold"


def test_regression_on_secondary_metric_overrides_duration_win():
    # Duration improved, but logical reads blew up 50% -> rollback wins.
    baseline = _m(1000, cpu=500, reads=1000)
    candidate = _m(700, cpu=500, reads=1500)
    d = decide(baseline, candidate, T)
    assert d.action == "rollback"
    assert "avg_logical_io_reads" in d.regressed_metrics


def test_unusable_baseline_holds():
    d = decide(_m(0), _m(400), T)
    assert d.action == "hold"


def test_thresholds_are_tunable():
    # With a 70% keep floor, a 60% win is no longer enough to keep.
    strict = Thresholds(min_improvement_pct=0.70)
    d = decide(_m(1000), _m(400), strict)
    assert d.action == "rollback"


def test_improvement_pct_helper():
    assert improvement_pct(1000, 400) == 0.6
    assert improvement_pct(0, 400) is None
    assert improvement_pct(None, 400) is None
