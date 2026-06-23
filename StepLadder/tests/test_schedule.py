from datetime import datetime, time
from zoneinfo import ZoneInfo

from src.schedule import determine_floor_vcores, is_business_time


def _dt(hour: int, minute: int = 0, day: int = 1) -> datetime:
    return datetime(2026, 6, day, hour, minute, tzinfo=ZoneInfo("America/Chicago"))


def test_is_business_time_weekday_inside_window() -> None:
    local_now = _dt(10, 0, 1)  # Monday
    assert is_business_time(
        local_now=local_now,
        business_days={0, 1, 2, 3, 4},
        business_start=time(8, 0),
        business_end=time(17, 0),
    )


def test_is_business_time_end_boundary_exclusive() -> None:
    local_now = _dt(17, 0, 1)  # Monday 5PM
    assert not is_business_time(
        local_now=local_now,
        business_days={0, 1, 2, 3, 4},
        business_start=time(8, 0),
        business_end=time(17, 0),
    )


def test_determine_floor_for_weekend() -> None:
    local_now = _dt(10, 0, 6)  # Saturday
    floor = determine_floor_vcores(
        local_now=local_now,
        business_days={0, 1, 2, 3, 4},
        business_start=time(8, 0),
        business_end=time(17, 0),
        min_vcores_business=8,
        min_vcores_offhours=2,
    )
    assert floor == 2
