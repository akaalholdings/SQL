from datetime import datetime, timedelta, timezone

from src.decision_engine import MetricSignal, compute_decision


def test_floor_correction_has_priority() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=2,
        floor_vcores=8,
        max_vcores=16,
        step_vcores=2,
        metrics=MetricSignal(sample_count=120, up_hits=120, down_hits=0),
        last_scale_completed_utc=None,
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "floor"
    assert decision.target_vcores == 8


def test_scale_up_on_sustained_saturation() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=8,
        floor_vcores=8,
        max_vcores=16,
        step_vcores=2,
        metrics=MetricSignal(sample_count=120, up_hits=20, down_hits=0),
        last_scale_completed_utc=now - timedelta(minutes=30),
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "up"
    assert decision.target_vcores == 10


def test_scale_down_on_sustained_low_utilization() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=14,
        floor_vcores=2,
        max_vcores=16,
        step_vcores=2,
        metrics=MetricSignal(sample_count=120, up_hits=0, down_hits=120),
        last_scale_completed_utc=now - timedelta(minutes=60),
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "down"
    assert decision.target_vcores == 12


def test_cooldown_blocks_upscale() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=8,
        floor_vcores=8,
        max_vcores=16,
        step_vcores=2,
        metrics=MetricSignal(sample_count=120, up_hits=20, down_hits=0),
        last_scale_completed_utc=now - timedelta(minutes=5),
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "none"
    assert decision.reason == "up_cooldown_active"


def test_scale_up_respects_upper_bound() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=15,
        floor_vcores=8,
        max_vcores=16,
        step_vcores=2,
        metrics=MetricSignal(sample_count=120, up_hits=20, down_hits=0),
        last_scale_completed_utc=now - timedelta(minutes=30),
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "up"
    assert decision.target_vcores == 16


def test_scale_up_uses_next_allowed_vcore_value() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=8,
        floor_vcores=4,
        max_vcores=16,
        step_vcores=2,
        allowed_vcores=(2, 4, 8, 16),
        metrics=MetricSignal(sample_count=20, up_hits=20, down_hits=0),
        last_scale_completed_utc=None,
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "up"
    assert decision.target_vcores == 16


def test_scale_down_uses_next_allowed_vcore_value() -> None:
    now = datetime.now(timezone.utc)
    decision = compute_decision(
        current_vcores=16,
        floor_vcores=4,
        max_vcores=16,
        step_vcores=2,
        allowed_vcores=(2, 4, 8, 16),
        metrics=MetricSignal(sample_count=120, up_hits=0, down_hits=120),
        last_scale_completed_utc=None,
        now_utc=now,
        up_window_samples=20,
        down_window_samples=120,
        up_cooldown_min=10,
        down_cooldown_min=15,
    )
    assert decision.action == "down"
    assert decision.target_vcores == 8
