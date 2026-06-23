from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class MetricSignal:
    sample_count: int
    up_hits: int
    down_hits: int


@dataclass(frozen=True)
class Decision:
    action: str
    target_vcores: int
    reason: str


def compute_decision(
    *,
    current_vcores: int,
    floor_vcores: int,
    max_vcores: int,
    step_vcores: int,
    allowed_vcores: tuple[int, ...] | None = None,
    metrics: MetricSignal,
    last_scale_completed_utc: datetime | None,
    now_utc: datetime,
    up_window_samples: int,
    down_window_samples: int,
    up_cooldown_min: int,
    down_cooldown_min: int,
) -> Decision:
    if current_vcores < floor_vcores:
        return Decision(action="floor", target_vcores=floor_vcores, reason="below_floor")

    if _can_scale_up(current_vcores, max_vcores, metrics, up_window_samples):
        if _cooldown_satisfied(last_scale_completed_utc, now_utc, up_cooldown_min):
            target = _next_up_target(
                current_vcores=current_vcores,
                max_vcores=max_vcores,
                step_vcores=step_vcores,
                allowed_vcores=allowed_vcores,
            )
            return Decision(action="up", target_vcores=target, reason="sustained_saturation")
        return Decision(action="none", target_vcores=current_vcores, reason="up_cooldown_active")

    if _can_scale_down(current_vcores, floor_vcores, metrics, down_window_samples):
        if _cooldown_satisfied(last_scale_completed_utc, now_utc, down_cooldown_min):
            target = _next_down_target(
                current_vcores=current_vcores,
                floor_vcores=floor_vcores,
                step_vcores=step_vcores,
                allowed_vcores=allowed_vcores,
            )
            return Decision(action="down", target_vcores=target, reason="sustained_low_utilization")
        return Decision(action="none", target_vcores=current_vcores, reason="down_cooldown_active")

    return Decision(action="none", target_vcores=current_vcores, reason="no_rule_match")


def _can_scale_up(
    current_vcores: int,
    max_vcores: int,
    metrics: MetricSignal,
    up_window_samples: int,
) -> bool:
    if current_vcores >= max_vcores:
        return False

    if metrics.sample_count < up_window_samples:
        return False

    return metrics.up_hits >= up_window_samples


def _can_scale_down(
    current_vcores: int,
    floor_vcores: int,
    metrics: MetricSignal,
    down_window_samples: int,
) -> bool:
    if current_vcores <= floor_vcores:
        return False

    if metrics.sample_count < down_window_samples:
        return False

    return metrics.down_hits >= down_window_samples


def _cooldown_satisfied(
    last_scale_completed_utc: datetime | None,
    now_utc: datetime,
    cooldown_minutes: int,
) -> bool:
    if last_scale_completed_utc is None:
        return True

    now_utc = _coerce_utc(now_utc)
    last_scale_completed_utc = _coerce_utc(last_scale_completed_utc)

    return now_utc - last_scale_completed_utc >= timedelta(minutes=cooldown_minutes)


def _next_up_target(
    *,
    current_vcores: int,
    max_vcores: int,
    step_vcores: int,
    allowed_vcores: tuple[int, ...] | None,
) -> int:
    minimum_target = min(max_vcores, current_vcores + step_vcores)
    if allowed_vcores is None:
        return minimum_target

    for value in allowed_vcores:
        if value >= minimum_target and value <= max_vcores:
            return value

    return max_vcores


def _next_down_target(
    *,
    current_vcores: int,
    floor_vcores: int,
    step_vcores: int,
    allowed_vcores: tuple[int, ...] | None,
) -> int:
    maximum_target = max(floor_vcores, current_vcores - step_vcores)
    if allowed_vcores is None:
        return maximum_target

    candidates = [value for value in allowed_vcores if floor_vcores <= value <= maximum_target]
    if not candidates:
        return floor_vcores

    return max(candidates)


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
