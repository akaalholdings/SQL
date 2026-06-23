from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DAY_NAME_TO_INDEX = {
    "MON": 0,
    "TUE": 1,
    "WED": 2,
    "THU": 3,
    "FRI": 4,
    "SAT": 5,
    "SUN": 6,
}


class ConfigError(ValueError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    subscription_id: str
    resource_group: str
    sql_server_name: str
    sql_database_name: str
    sql_server_fqdn: str
    timezone: ZoneInfo
    business_days: frozenset[int]
    business_start: time
    business_end: time
    min_vcores_business: int
    min_vcores_offhours: int
    max_vcores: int
    step_vcores: int
    up_window_samples: int
    up_cpu_threshold: int
    up_io_threshold: int
    down_window_samples: int
    down_threshold: int
    up_cooldown_min: int
    down_cooldown_min: int
    scale_pending_timeout_min: int
    scale_request_wait_seconds: int
    dry_run: bool
    sync_maxdop: bool
    maxdop_required: bool
    managed_identity_client_id: str | None
    storage_connection_string: str
    state_table_name: str
    lock_container_name: str
    lock_lease_seconds: int
    sql_driver: str
    sql_connection_timeout_seconds: int
    sql_query_timeout_seconds: int
    allowed_vcores: tuple[int, ...] | None

    @classmethod
    def from_env(cls) -> "Config":
        subscription_id = _required("SUBSCRIPTION_ID")
        resource_group = _required("RESOURCE_GROUP")
        sql_server_name = _required("SQL_SERVER_NAME")
        sql_database_name = _required("SQL_DATABASE_NAME")
        sql_server_fqdn = os.getenv("SQL_SERVER_FQDN", f"{sql_server_name}.database.windows.net")

        timezone_name = os.getenv("TIMEZONE", "America/Chicago")
        try:
            timezone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ConfigError(f"Invalid TIMEZONE '{timezone_name}'.") from exc

        business_days = _parse_business_days(os.getenv("BUSINESS_DAYS", "MON,TUE,WED,THU,FRI"))
        business_start = _parse_hhmm(os.getenv("BUSINESS_START", "08:00"), "BUSINESS_START")
        business_end = _parse_hhmm(os.getenv("BUSINESS_END", "17:00"), "BUSINESS_END")

        min_vcores_business = _int_env("MIN_VCORES_BUSINESS", 12)
        min_vcores_offhours = _int_env("MIN_VCORES_OFFHOURS", 2)
        max_vcores = _int_env("MAX_VCORES", 16)
        step_vcores = _int_env("STEP_VCORES", 2)

        up_window_samples = _int_env("UP_WINDOW_SAMPLES", 20)
        up_cpu_threshold = _int_env("UP_CPU_THRESHOLD", 85)
        up_io_threshold = _int_env("UP_IO_THRESHOLD", 85)

        down_window_samples = _int_env("DOWN_WINDOW_SAMPLES", 120)
        down_threshold = _int_env("DOWN_THRESHOLD", 35)

        up_cooldown_min = _int_env("UP_COOLDOWN_MIN", 10)
        down_cooldown_min = _int_env("DOWN_COOLDOWN_MIN", 15)
        scale_pending_timeout_min = _int_env("SCALE_PENDING_TIMEOUT_MIN", 20)
        scale_request_wait_seconds = _int_env("SCALE_REQUEST_WAIT_SECONDS", 30)

        dry_run = _bool_env("DRY_RUN", True)
        sync_maxdop = _bool_env("SYNC_MAXDOP", True)
        maxdop_required = _bool_env("MAXDOP_REQUIRED", False)
        managed_identity_client_id = os.getenv("MANAGED_IDENTITY_CLIENT_ID") or None

        storage_connection_string = (
            os.getenv("AUTOSCALER_STORAGE_CONNECTION_STRING")
            or os.getenv("AzureWebJobsStorage")
            or ""
        )
        if not storage_connection_string:
            raise ConfigError(
                "Storage connection string is required via AUTOSCALER_STORAGE_CONNECTION_STRING "
                "or AzureWebJobsStorage."
            )

        state_table_name = os.getenv("STATE_TABLE_NAME", "sqlautoscalerstate")
        lock_container_name = os.getenv("LOCK_CONTAINER_NAME", "sqlautoscaler-locks")
        lock_lease_seconds = _int_env("LOCK_LEASE_SECONDS", 55)
        sql_driver = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
        sql_connection_timeout_seconds = _int_env("SQL_CONNECTION_TIMEOUT_SECONDS", 30)
        sql_query_timeout_seconds = _int_env("SQL_QUERY_TIMEOUT_SECONDS", 30)
        allowed_vcores = _parse_allowed_vcores(os.getenv("ALLOWED_VCORES", ""))

        _validate_ranges(
            min_vcores_business=min_vcores_business,
            min_vcores_offhours=min_vcores_offhours,
            max_vcores=max_vcores,
            step_vcores=step_vcores,
            up_window_samples=up_window_samples,
            up_cpu_threshold=up_cpu_threshold,
            up_io_threshold=up_io_threshold,
            down_window_samples=down_window_samples,
            down_threshold=down_threshold,
            up_cooldown_min=up_cooldown_min,
            down_cooldown_min=down_cooldown_min,
            scale_pending_timeout_min=scale_pending_timeout_min,
            scale_request_wait_seconds=scale_request_wait_seconds,
            lock_lease_seconds=lock_lease_seconds,
            sql_connection_timeout_seconds=sql_connection_timeout_seconds,
            sql_query_timeout_seconds=sql_query_timeout_seconds,
        )
        _validate_allowed_vcores(
            allowed_vcores=allowed_vcores,
            min_vcores_business=min_vcores_business,
            min_vcores_offhours=min_vcores_offhours,
            max_vcores=max_vcores,
        )

        return cls(
            subscription_id=subscription_id,
            resource_group=resource_group,
            sql_server_name=sql_server_name,
            sql_database_name=sql_database_name,
            sql_server_fqdn=sql_server_fqdn,
            timezone=timezone,
            business_days=business_days,
            business_start=business_start,
            business_end=business_end,
            min_vcores_business=min_vcores_business,
            min_vcores_offhours=min_vcores_offhours,
            max_vcores=max_vcores,
            step_vcores=step_vcores,
            up_window_samples=up_window_samples,
            up_cpu_threshold=up_cpu_threshold,
            up_io_threshold=up_io_threshold,
            down_window_samples=down_window_samples,
            down_threshold=down_threshold,
            up_cooldown_min=up_cooldown_min,
            down_cooldown_min=down_cooldown_min,
            scale_pending_timeout_min=scale_pending_timeout_min,
            scale_request_wait_seconds=scale_request_wait_seconds,
            dry_run=dry_run,
            sync_maxdop=sync_maxdop,
            maxdop_required=maxdop_required,
            managed_identity_client_id=managed_identity_client_id,
            storage_connection_string=storage_connection_string,
            state_table_name=state_table_name,
            lock_container_name=lock_container_name,
            lock_lease_seconds=lock_lease_seconds,
            sql_driver=sql_driver,
            sql_connection_timeout_seconds=sql_connection_timeout_seconds,
            sql_query_timeout_seconds=sql_query_timeout_seconds,
            allowed_vcores=allowed_vcores,
        )


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required setting: {name}")
    return value


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"Setting {name} must be an integer.") from exc


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise ConfigError(f"Setting {name} must be a boolean.")


def _parse_business_days(raw: str) -> frozenset[int]:
    tokens = [token.strip().upper() for token in raw.split(",") if token.strip()]
    if not tokens:
        raise ConfigError("BUSINESS_DAYS cannot be empty.")

    result = set()
    for token in tokens:
        day_index = DAY_NAME_TO_INDEX.get(token)
        if day_index is None:
            valid = ", ".join(DAY_NAME_TO_INDEX.keys())
            raise ConfigError(f"Invalid day '{token}' in BUSINESS_DAYS. Valid values: {valid}.")
        result.add(day_index)

    return frozenset(result)


def _parse_allowed_vcores(raw: str) -> tuple[int, ...] | None:
    tokens = [token.strip() for token in raw.split(",") if token.strip()]
    if not tokens:
        return None

    values = set()
    for token in tokens:
        try:
            value = int(token)
        except ValueError as exc:
            raise ConfigError("ALLOWED_VCORES must be a comma-separated list of integers.") from exc
        if value < 1:
            raise ConfigError("ALLOWED_VCORES values must be >= 1.")
        values.add(value)

    return tuple(sorted(values))


def _parse_hhmm(raw: str, setting_name: str) -> time:
    pieces = raw.split(":")
    if len(pieces) != 2:
        raise ConfigError(f"{setting_name} must use HH:MM format.")

    try:
        hour = int(pieces[0])
        minute = int(pieces[1])
        return time(hour=hour, minute=minute)
    except ValueError as exc:
        raise ConfigError(f"{setting_name} must use HH:MM format.") from exc


def _validate_ranges(**kwargs: int) -> None:
    if kwargs["min_vcores_business"] < 1 or kwargs["min_vcores_offhours"] < 1:
        raise ConfigError("Minimum vCores must be >= 1.")

    if kwargs["max_vcores"] < kwargs["min_vcores_business"]:
        raise ConfigError("MAX_VCORES must be >= MIN_VCORES_BUSINESS.")

    if kwargs["max_vcores"] < kwargs["min_vcores_offhours"]:
        raise ConfigError("MAX_VCORES must be >= MIN_VCORES_OFFHOURS.")

    if kwargs["step_vcores"] < 1:
        raise ConfigError("STEP_VCORES must be >= 1.")

    if kwargs["up_window_samples"] < 1 or kwargs["down_window_samples"] < 1:
        raise ConfigError("Window sample counts must be >= 1.")

    if kwargs["up_cpu_threshold"] < 1 or kwargs["up_cpu_threshold"] > 100:
        raise ConfigError("UP_CPU_THRESHOLD must be between 1 and 100.")

    if kwargs["up_io_threshold"] < 1 or kwargs["up_io_threshold"] > 100:
        raise ConfigError("UP_IO_THRESHOLD must be between 1 and 100.")

    if kwargs["down_threshold"] < 0 or kwargs["down_threshold"] > 100:
        raise ConfigError("DOWN_THRESHOLD must be between 0 and 100.")

    if kwargs["down_threshold"] >= min(kwargs["up_cpu_threshold"], kwargs["up_io_threshold"]):
        raise ConfigError("DOWN_THRESHOLD must be lower than both upscale thresholds.")

    if kwargs["up_cooldown_min"] < 0 or kwargs["down_cooldown_min"] < 0:
        raise ConfigError("Cooldown values must be >= 0.")

    if kwargs["scale_pending_timeout_min"] < 1:
        raise ConfigError("SCALE_PENDING_TIMEOUT_MIN must be >= 1.")

    if kwargs["scale_request_wait_seconds"] < 0:
        raise ConfigError("SCALE_REQUEST_WAIT_SECONDS must be >= 0.")

    if kwargs["lock_lease_seconds"] < 15 or kwargs["lock_lease_seconds"] > 60:
        raise ConfigError("LOCK_LEASE_SECONDS must be between 15 and 60.")

    if kwargs["sql_connection_timeout_seconds"] < 1:
        raise ConfigError("SQL_CONNECTION_TIMEOUT_SECONDS must be >= 1.")

    if kwargs["sql_query_timeout_seconds"] < 1:
        raise ConfigError("SQL_QUERY_TIMEOUT_SECONDS must be >= 1.")


def _validate_allowed_vcores(
    *,
    allowed_vcores: tuple[int, ...] | None,
    min_vcores_business: int,
    min_vcores_offhours: int,
    max_vcores: int,
) -> None:
    if allowed_vcores is None:
        return

    required = {
        "MIN_VCORES_BUSINESS": min_vcores_business,
        "MIN_VCORES_OFFHOURS": min_vcores_offhours,
        "MAX_VCORES": max_vcores,
    }
    missing = [name for name, value in required.items() if value not in allowed_vcores]
    if missing:
        joined = ", ".join(missing)
        raise ConfigError(f"ALLOWED_VCORES must include {joined}.")

    if max(allowed_vcores) > max_vcores:
        raise ConfigError("ALLOWED_VCORES cannot contain values above MAX_VCORES.")
