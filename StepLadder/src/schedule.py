from __future__ import annotations

from datetime import datetime, time


def is_business_time(
    *,
    local_now: datetime,
    business_days: set[int] | frozenset[int],
    business_start: time,
    business_end: time,
) -> bool:
    if local_now.weekday() not in business_days:
        return False

    current_time = local_now.timetz().replace(tzinfo=None)

    if business_start <= business_end:
        return business_start <= current_time < business_end

    return current_time >= business_start or current_time < business_end


def determine_floor_vcores(
    *,
    local_now: datetime,
    business_days: set[int] | frozenset[int],
    business_start: time,
    business_end: time,
    min_vcores_business: int,
    min_vcores_offhours: int,
) -> int:
    if is_business_time(
        local_now=local_now,
        business_days=business_days,
        business_start=business_start,
        business_end=business_end,
    ):
        return min_vcores_business

    return min_vcores_offhours
