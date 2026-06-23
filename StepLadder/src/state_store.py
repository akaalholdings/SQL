from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode


@dataclass
class StateEntity:
    partition_key: str
    row_key: str
    last_action: str = "none"
    last_cycle_utc: datetime | None = None
    last_decision: str | None = None
    last_decision_reason: str | None = None
    last_decision_utc: datetime | None = None
    last_observed_vcores: int | None = None
    last_floor_vcores: int | None = None
    last_target_vcores: int | None = None
    last_scale_requested_utc: datetime | None = None
    last_scale_completed_utc: datetime | None = None
    pending_target_vcores: int | None = None
    pending_since_utc: datetime | None = None
    pending_deadline_utc: datetime | None = None
    last_maxdop: int | None = None
    last_maxdop_utc: datetime | None = None
    last_maxdop_error: str | None = None
    last_metrics_sample_count: int | None = None
    last_metrics_up_hits: int | None = None
    last_metrics_down_hits: int | None = None
    last_error: str | None = None
    last_error_detail: str | None = None

    @classmethod
    def from_table_entity(cls, entity: dict[str, Any]) -> "StateEntity":
        return cls(
            partition_key=entity["PartitionKey"],
            row_key=entity["RowKey"],
            last_action=entity.get("last_action", "none"),
            last_cycle_utc=_parse_datetime(entity.get("last_cycle_utc")),
            last_decision=entity.get("last_decision"),
            last_decision_reason=entity.get("last_decision_reason"),
            last_decision_utc=_parse_datetime(entity.get("last_decision_utc")),
            last_observed_vcores=_parse_int(entity.get("last_observed_vcores")),
            last_floor_vcores=_parse_int(entity.get("last_floor_vcores")),
            last_target_vcores=_parse_int(entity.get("last_target_vcores")),
            last_scale_requested_utc=_parse_datetime(entity.get("last_scale_requested_utc")),
            last_scale_completed_utc=_parse_datetime(entity.get("last_scale_completed_utc")),
            pending_target_vcores=_parse_int(entity.get("pending_target_vcores")),
            pending_since_utc=_parse_datetime(entity.get("pending_since_utc")),
            pending_deadline_utc=_parse_datetime(entity.get("pending_deadline_utc")),
            last_maxdop=_parse_int(entity.get("last_maxdop")),
            last_maxdop_utc=_parse_datetime(entity.get("last_maxdop_utc")),
            last_maxdop_error=entity.get("last_maxdop_error"),
            last_metrics_sample_count=_parse_int(entity.get("last_metrics_sample_count")),
            last_metrics_up_hits=_parse_int(entity.get("last_metrics_up_hits")),
            last_metrics_down_hits=_parse_int(entity.get("last_metrics_down_hits")),
            last_error=entity.get("last_error"),
            last_error_detail=entity.get("last_error_detail"),
        )

    def to_table_entity(self) -> dict[str, Any]:
        entity: dict[str, Any] = {
            "PartitionKey": self.partition_key,
            "RowKey": self.row_key,
            "last_action": self.last_action,
        }

        if self.last_scale_completed_utc is not None:
            entity["last_scale_completed_utc"] = _format_datetime(self.last_scale_completed_utc)

        if self.last_cycle_utc is not None:
            entity["last_cycle_utc"] = _format_datetime(self.last_cycle_utc)

        if self.last_decision is not None:
            entity["last_decision"] = self.last_decision

        if self.last_decision_reason is not None:
            entity["last_decision_reason"] = self.last_decision_reason

        if self.last_decision_utc is not None:
            entity["last_decision_utc"] = _format_datetime(self.last_decision_utc)

        if self.last_observed_vcores is not None:
            entity["last_observed_vcores"] = self.last_observed_vcores

        if self.last_floor_vcores is not None:
            entity["last_floor_vcores"] = self.last_floor_vcores

        if self.last_target_vcores is not None:
            entity["last_target_vcores"] = self.last_target_vcores

        if self.last_scale_requested_utc is not None:
            entity["last_scale_requested_utc"] = _format_datetime(self.last_scale_requested_utc)

        if self.pending_target_vcores is not None:
            entity["pending_target_vcores"] = self.pending_target_vcores

        if self.pending_since_utc is not None:
            entity["pending_since_utc"] = _format_datetime(self.pending_since_utc)

        if self.pending_deadline_utc is not None:
            entity["pending_deadline_utc"] = _format_datetime(self.pending_deadline_utc)

        if self.last_maxdop is not None:
            entity["last_maxdop"] = self.last_maxdop

        if self.last_maxdop_utc is not None:
            entity["last_maxdop_utc"] = _format_datetime(self.last_maxdop_utc)

        if self.last_maxdop_error is not None:
            entity["last_maxdop_error"] = self.last_maxdop_error

        if self.last_metrics_sample_count is not None:
            entity["last_metrics_sample_count"] = self.last_metrics_sample_count

        if self.last_metrics_up_hits is not None:
            entity["last_metrics_up_hits"] = self.last_metrics_up_hits

        if self.last_metrics_down_hits is not None:
            entity["last_metrics_down_hits"] = self.last_metrics_down_hits

        if self.last_error is not None:
            entity["last_error"] = self.last_error

        if self.last_error_detail is not None:
            entity["last_error_detail"] = self.last_error_detail

        return entity

    def set_pending(self, *, target_vcores: int, now_utc: datetime, timeout_min: int) -> None:
        self.pending_target_vcores = target_vcores
        self.pending_since_utc = _coerce_utc(now_utc)
        self.pending_deadline_utc = self.pending_since_utc + timedelta(minutes=timeout_min)

    def clear_pending(self) -> None:
        self.pending_target_vcores = None
        self.pending_since_utc = None
        self.pending_deadline_utc = None


class TableStateStore:
    def __init__(self, *, connection_string: str, table_name: str):
        self._service = TableServiceClient.from_connection_string(connection_string)
        self._table_client = self._service.get_table_client(table_name=table_name)
        try:
            self._table_client.create_table()
        except ResourceExistsError:
            pass

    def get_state(self, *, server_name: str, database_name: str) -> StateEntity:
        partition_key = server_name.strip().lower()
        row_key = database_name.strip().lower()

        try:
            entity = self._table_client.get_entity(partition_key=partition_key, row_key=row_key)
            return StateEntity.from_table_entity(entity)
        except ResourceNotFoundError:
            return StateEntity(partition_key=partition_key, row_key=row_key)

    def upsert_state(self, state: StateEntity) -> None:
        self._table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=state.to_table_entity())


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_datetime(value: datetime) -> str:
    return _coerce_utc(value).isoformat().replace("+00:00", "Z")


def _parse_datetime(raw: Any) -> datetime | None:
    if not raw:
        return None

    if isinstance(raw, datetime):
        return _coerce_utc(raw)

    if isinstance(raw, str):
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return None


def _parse_int(raw: Any) -> int | None:
    if raw is None:
        return None

    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
