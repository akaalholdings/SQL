from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol

from src.config import Config
from src.decision_engine import MetricSignal, compute_decision
from src.maxdop import build_maxdop_statement, compute_maxdop
from src.schedule import determine_floor_vcores
from src.state_store import StateEntity


class StateStore(Protocol):
    def get_state(self, *, server_name: str, database_name: str) -> StateEntity:
        ...

    def upsert_state(self, state: StateEntity) -> None:
        ...


class DatabaseCompute(Protocol):
    vcores: int


class SqlDatabaseClient(Protocol):
    def get_database_compute(self) -> DatabaseCompute:
        ...

    def request_scale(
        self,
        *,
        current: DatabaseCompute,
        target_vcores: int,
        wait_seconds: int,
    ) -> None:
        ...


FetchMetricSignal = Callable[[], MetricSignal]
ExecuteSqlStatement = Callable[[str], None]


@dataclass(frozen=True)
class CycleResult:
    status: str
    action: str = "none"
    target_vcores: int | None = None
    reason: str | None = None
    error: str | None = None


def run_autoscaler_cycle(
    *,
    config: Config,
    state_store: StateStore,
    sql_client: SqlDatabaseClient,
    fetch_metric_signal: FetchMetricSignal,
    execute_sql_statement: ExecuteSqlStatement,
    now_utc: datetime | None = None,
) -> CycleResult:
    now_utc = _coerce_utc(now_utc or datetime.now(timezone.utc))
    state = state_store.get_state(
        server_name=config.sql_server_name,
        database_name=config.sql_database_name,
    )
    state.last_cycle_utc = now_utc

    try:
        current_compute = sql_client.get_database_compute()
    except Exception as exc:
        _record_error(state, "compute_read_failed", exc)
        state_store.upsert_state(state)
        logging.exception("Failed to read current database compute metadata.")
        return CycleResult(status="error", error="compute_read_failed")

    current_vcores = int(current_compute.vcores)
    state.last_observed_vcores = current_vcores

    pending_result = _handle_pending_if_present(
        config=config,
        state=state,
        state_store=state_store,
        current_vcores=current_vcores,
        execute_sql_statement=execute_sql_statement,
        now_utc=now_utc,
    )
    if pending_result is not None:
        return pending_result

    if config.sync_maxdop:
        maxdop_ok = _ensure_maxdop_synced(
            config=config,
            state=state,
            current_vcores=current_vcores,
            execute_sql_statement=execute_sql_statement,
            now_utc=now_utc,
        )
        if not maxdop_ok and config.maxdop_required:
            state.last_error = "maxdop_sync_failed"
            state_store.upsert_state(state)
            return CycleResult(status="error", error="maxdop_sync_failed")

    now_local = now_utc.astimezone(config.timezone)
    floor_vcores = determine_floor_vcores(
        local_now=now_local,
        business_days=config.business_days,
        business_start=config.business_start,
        business_end=config.business_end,
        min_vcores_business=config.min_vcores_business,
        min_vcores_offhours=config.min_vcores_offhours,
    )
    state.last_floor_vcores = floor_vcores

    try:
        metrics = fetch_metric_signal()
    except Exception as exc:
        _record_error(state, "metrics_query_failed", exc)
        state_store.upsert_state(state)
        logging.exception("Failed querying sys.dm_db_resource_stats.")
        return CycleResult(status="error", error="metrics_query_failed")

    state.last_metrics_sample_count = metrics.sample_count
    state.last_metrics_up_hits = metrics.up_hits
    state.last_metrics_down_hits = metrics.down_hits

    decision = compute_decision(
        current_vcores=current_vcores,
        floor_vcores=floor_vcores,
        max_vcores=config.max_vcores,
        step_vcores=config.step_vcores,
        allowed_vcores=config.allowed_vcores,
        metrics=metrics,
        last_scale_completed_utc=state.last_scale_completed_utc,
        now_utc=now_utc,
        up_window_samples=config.up_window_samples,
        down_window_samples=config.down_window_samples,
        up_cooldown_min=config.up_cooldown_min,
        down_cooldown_min=config.down_cooldown_min,
    )
    _record_decision(state, decision.action, decision.target_vcores, decision.reason, now_utc)

    logging.info(
        "Decision=%s target=%s reason=%s current=%s floor=%s samples=%s up_hits=%s down_hits=%s",
        decision.action,
        decision.target_vcores,
        decision.reason,
        current_vcores,
        floor_vcores,
        metrics.sample_count,
        metrics.up_hits,
        metrics.down_hits,
    )

    if decision.action == "none" or decision.target_vcores == current_vcores:
        state.last_error = None
        state.last_error_detail = None
        state_store.upsert_state(state)
        return CycleResult(
            status="no_action",
            action=decision.action,
            target_vcores=decision.target_vcores,
            reason=decision.reason,
        )

    if config.dry_run:
        logging.warning(
            "DRY_RUN enabled. Would scale %s from %s to %s vCores.",
            decision.action,
            current_vcores,
            decision.target_vcores,
        )
        state.last_error = None
        state.last_error_detail = None
        state_store.upsert_state(state)
        return CycleResult(
            status="dry_run",
            action=decision.action,
            target_vcores=decision.target_vcores,
            reason=decision.reason,
        )

    try:
        sql_client.request_scale(
            current=current_compute,
            target_vcores=decision.target_vcores,
            wait_seconds=config.scale_request_wait_seconds,
        )
    except Exception as exc:
        _record_error(state, "scale_request_failed", exc)
        state_store.upsert_state(state)
        logging.exception("Scale request failed for target vCores=%s", decision.target_vcores)
        return CycleResult(status="error", action=decision.action, error="scale_request_failed")

    state.last_action = decision.action
    state.last_scale_requested_utc = now_utc
    state.set_pending(
        target_vcores=decision.target_vcores,
        now_utc=now_utc,
        timeout_min=config.scale_pending_timeout_min,
    )
    state.last_error = None
    state.last_error_detail = None
    state_store.upsert_state(state)

    logging.warning(
        "Scale requested action=%s from=%s to=%s",
        decision.action,
        current_vcores,
        decision.target_vcores,
    )
    return CycleResult(
        status="scale_requested",
        action=decision.action,
        target_vcores=decision.target_vcores,
        reason=decision.reason,
    )


def _handle_pending_if_present(
    *,
    config: Config,
    state: StateEntity,
    state_store: StateStore,
    current_vcores: int,
    execute_sql_statement: ExecuteSqlStatement,
    now_utc: datetime,
) -> CycleResult | None:
    if state.pending_target_vcores is None:
        return None

    if current_vcores == state.pending_target_vcores:
        state.last_scale_completed_utc = now_utc
        state.clear_pending()
        maxdop_ok = True
        if config.sync_maxdop:
            maxdop_ok = _ensure_maxdop_synced(
                config=config,
                state=state,
                current_vcores=current_vcores,
                execute_sql_statement=execute_sql_statement,
                now_utc=now_utc,
            )
        if not maxdop_ok and config.maxdop_required:
            state.last_error = "maxdop_sync_failed"
            state_store.upsert_state(state)
            return CycleResult(status="error", target_vcores=current_vcores, error="maxdop_sync_failed")
        state.last_error = None
        state.last_error_detail = None
        state_store.upsert_state(state)
        logging.info("Pending scale completed at %s vCores.", current_vcores)
        return CycleResult(status="pending_completed", target_vcores=current_vcores)

    if state.pending_deadline_utc and now_utc < _coerce_utc(state.pending_deadline_utc):
        logging.info(
            "Scale still pending target=%s current=%s deadline=%s",
            state.pending_target_vcores,
            current_vcores,
            state.pending_deadline_utc,
        )
        state_store.upsert_state(state)
        return CycleResult(status="pending", target_vcores=state.pending_target_vcores)

    state.clear_pending()
    state.last_error = "scale_pending_timeout"
    state.last_error_detail = None
    state_store.upsert_state(state)
    logging.error("ALERT: scale operation exceeded pending timeout; cleared pending state.")
    return None


def _ensure_maxdop_synced(
    *,
    config: Config,
    state: StateEntity,
    current_vcores: int,
    execute_sql_statement: ExecuteSqlStatement,
    now_utc: datetime,
) -> bool:
    desired_maxdop = compute_maxdop(current_vcores)
    if state.last_maxdop == desired_maxdop:
        state.last_maxdop_error = None
        return True

    if config.dry_run:
        logging.info(
            "DRY_RUN enabled. Would set MAXDOP to %s for current_vcores=%s.",
            desired_maxdop,
            current_vcores,
        )
        return True

    statement = build_maxdop_statement(desired_maxdop)
    try:
        execute_sql_statement(statement)
    except Exception as exc:
        state.last_maxdop_error = _error_detail(exc)
        logging.exception("MAXDOP update failed for value=%s", desired_maxdop)
        return False

    state.last_maxdop = desired_maxdop
    state.last_maxdop_utc = now_utc
    state.last_maxdop_error = None
    logging.info("MAXDOP set to %s for vCores=%s", desired_maxdop, current_vcores)
    return True


def _record_decision(
    state: StateEntity,
    action: str,
    target_vcores: int,
    reason: str,
    now_utc: datetime,
) -> None:
    state.last_decision = action
    state.last_decision_reason = reason
    state.last_decision_utc = now_utc
    state.last_target_vcores = target_vcores


def _record_error(state: StateEntity, error: str, exc: Exception) -> None:
    state.last_error = error
    state.last_error_detail = _error_detail(exc)


def _error_detail(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:1024]


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
