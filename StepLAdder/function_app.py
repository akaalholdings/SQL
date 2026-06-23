from __future__ import annotations

import logging

import azure.functions as func
from azure.identity import DefaultAzureCredential

from src.autoscaler import run_autoscaler_cycle
from src.azure_sql_client import AzureSqlDatabaseClient
from src.config import Config, ConfigError
from src.locking import try_acquire_singleflight_lock
from src.sql_metrics import execute_statement, fetch_metric_signal
from src.state_store import TableStateStore

app = func.FunctionApp()


@app.timer_trigger(
    arg_name="timer",
    schedule="0 */1 * * * *",
    run_on_startup=False,
    use_monitor=True,
)
def sql_vcore_autoscaler(timer: func.TimerRequest) -> None:
    del timer

    try:
        config = Config.from_env()
    except ConfigError:
        logging.exception("Configuration error. Autoscaler cycle aborted.")
        return

    lock_blob_name = f"{config.sql_server_name.lower()}-{config.sql_database_name.lower()}.lock"
    with try_acquire_singleflight_lock(
        connection_string=config.storage_connection_string,
        container_name=config.lock_container_name,
        blob_name=lock_blob_name,
        lease_seconds=config.lock_lease_seconds,
    ) as lock_acquired:
        if not lock_acquired:
            logging.info("Another autoscaler instance holds the lock. Skipping this cycle.")
            return

        credential = DefaultAzureCredential(
            managed_identity_client_id=config.managed_identity_client_id,
        )
        state_store = TableStateStore(
            connection_string=config.storage_connection_string,
            table_name=config.state_table_name,
        )
        sql_client = AzureSqlDatabaseClient(
            subscription_id=config.subscription_id,
            credential=credential,
            resource_group=config.resource_group,
            server_name=config.sql_server_name,
            database_name=config.sql_database_name,
        )

        result = run_autoscaler_cycle(
            config=config,
            state_store=state_store,
            sql_client=sql_client,
            fetch_metric_signal=lambda: fetch_metric_signal(config=config, credential=credential),
            execute_sql_statement=lambda statement: execute_statement(
                config=config,
                credential=credential,
                statement=statement,
            ),
        )
        logging.info(
            "Autoscaler cycle completed status=%s action=%s target=%s reason=%s error=%s",
            result.status,
            result.action,
            result.target_vcores,
            result.reason,
            result.error,
        )
