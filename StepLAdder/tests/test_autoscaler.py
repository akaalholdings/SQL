from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from src.autoscaler import run_autoscaler_cycle
from src.config import Config
from src.decision_engine import MetricSignal
from src.state_store import StateEntity


def _config(**overrides) -> Config:
    base = Config(
        subscription_id="sub",
        resource_group="rg",
        sql_server_name="server",
        sql_database_name="db",
        sql_server_fqdn="server.database.windows.net",
        timezone=ZoneInfo("UTC"),
        business_days=frozenset({0, 1, 2, 3, 4}),
        business_start=time(8, 0),
        business_end=time(17, 0),
        min_vcores_business=8,
        min_vcores_offhours=2,
        max_vcores=16,
        step_vcores=2,
        up_window_samples=3,
        up_cpu_threshold=85,
        up_io_threshold=85,
        down_window_samples=3,
        down_threshold=35,
        up_cooldown_min=0,
        down_cooldown_min=0,
        scale_pending_timeout_min=20,
        scale_request_wait_seconds=1,
        dry_run=False,
        sync_maxdop=True,
        maxdop_required=False,
        managed_identity_client_id=None,
        storage_connection_string="UseDevelopmentStorage=true",
        state_table_name="state",
        lock_container_name="locks",
        lock_lease_seconds=55,
        sql_driver="ODBC Driver 18 for SQL Server",
        sql_connection_timeout_seconds=30,
        sql_query_timeout_seconds=30,
        allowed_vcores=None,
    )
    return replace(base, **overrides)


class _Compute:
    def __init__(self, vcores: int):
        self.vcores = vcores


class _SqlClient:
    def __init__(self, current_vcores: int):
        self.current = _Compute(current_vcores)
        self.requests: list[int] = []

    def get_database_compute(self) -> _Compute:
        return self.current

    def request_scale(self, *, current: _Compute, target_vcores: int, wait_seconds: int) -> None:
        del current, wait_seconds
        self.requests.append(target_vcores)


class _StateStore:
    def __init__(self, state: StateEntity | None = None):
        self.state = state or StateEntity(partition_key="server", row_key="db")
        self.upserts: list[StateEntity] = []

    def get_state(self, *, server_name: str, database_name: str) -> StateEntity:
        del server_name, database_name
        return self.state

    def upsert_state(self, state: StateEntity) -> None:
        self.upserts.append(state)


def test_maxdop_failure_does_not_block_scaling_when_not_required() -> None:
    sql_client = _SqlClient(current_vcores=8)
    state_store = _StateStore()

    result = run_autoscaler_cycle(
        config=_config(maxdop_required=False),
        state_store=state_store,
        sql_client=sql_client,
        fetch_metric_signal=lambda: MetricSignal(sample_count=3, up_hits=3, down_hits=0),
        execute_sql_statement=lambda statement: (_ for _ in ()).throw(RuntimeError(statement)),
        now_utc=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert result.status == "scale_requested"
    assert sql_client.requests == [10]
    assert state_store.state.pending_target_vcores == 10
    assert state_store.state.last_error is None
    assert "RuntimeError" in (state_store.state.last_maxdop_error or "")


def test_maxdop_failure_can_be_made_blocking() -> None:
    sql_client = _SqlClient(current_vcores=8)
    state_store = _StateStore()

    result = run_autoscaler_cycle(
        config=_config(maxdop_required=True),
        state_store=state_store,
        sql_client=sql_client,
        fetch_metric_signal=lambda: MetricSignal(sample_count=3, up_hits=3, down_hits=0),
        execute_sql_statement=lambda statement: (_ for _ in ()).throw(RuntimeError(statement)),
        now_utc=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert result.status == "error"
    assert result.error == "maxdop_sync_failed"
    assert sql_client.requests == []
    assert state_store.state.last_error == "maxdop_sync_failed"


def test_dry_run_records_decision_without_scale_request() -> None:
    sql_client = _SqlClient(current_vcores=8)
    state_store = _StateStore()

    result = run_autoscaler_cycle(
        config=_config(dry_run=True),
        state_store=state_store,
        sql_client=sql_client,
        fetch_metric_signal=lambda: MetricSignal(sample_count=3, up_hits=3, down_hits=0),
        execute_sql_statement=lambda statement: None,
        now_utc=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert result.status == "dry_run"
    assert result.target_vcores == 10
    assert sql_client.requests == []
    assert state_store.state.last_decision == "up"


def test_pending_completion_records_completion_without_fetching_metrics() -> None:
    state = StateEntity(partition_key="server", row_key="db")
    state.set_pending(
        target_vcores=10,
        now_utc=datetime(2026, 6, 1, 11, 0, tzinfo=timezone.utc),
        timeout_min=20,
    )
    state_store = _StateStore(state)

    def fail_fetch() -> MetricSignal:
        raise AssertionError("metrics should not be fetched while completing pending scale")

    result = run_autoscaler_cycle(
        config=_config(sync_maxdop=False),
        state_store=state_store,
        sql_client=_SqlClient(current_vcores=10),
        fetch_metric_signal=fail_fetch,
        execute_sql_statement=lambda statement: None,
        now_utc=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert result.status == "pending_completed"
    assert state.pending_target_vcores is None
    assert state.last_scale_completed_utc == datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
