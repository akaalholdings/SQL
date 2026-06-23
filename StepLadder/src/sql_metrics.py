from __future__ import annotations

import struct

from azure.identity import DefaultAzureCredential

from src.config import Config
from src.decision_engine import MetricSignal

try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None

_SQL_TOKEN_SCOPE = "https://database.windows.net/.default"
_SQL_COPT_SS_ACCESS_TOKEN = 1256


def fetch_metric_signal(*, config: Config, credential: DefaultAzureCredential) -> MetricSignal:
    max_window = max(config.up_window_samples, config.down_window_samples)
    query = f"""
WITH recent AS (
    SELECT TOP ({max_window})
        end_time,
        avg_cpu_percent,
        avg_data_io_percent
    FROM sys.dm_db_resource_stats
    ORDER BY end_time DESC
),
r AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY end_time DESC) AS rn,
        avg_cpu_percent,
        avg_data_io_percent
    FROM recent
)
SELECT
    COUNT(*) AS sample_count,
    SUM(CASE
        WHEN rn <= {config.up_window_samples}
         AND (avg_cpu_percent >= {config.up_cpu_threshold}
              OR avg_data_io_percent >= {config.up_io_threshold})
        THEN 1 ELSE 0 END) AS up_hits,
    SUM(CASE
        WHEN rn <= {config.down_window_samples}
         AND avg_cpu_percent < {config.down_threshold}
         AND avg_data_io_percent < {config.down_threshold}
        THEN 1 ELSE 0 END) AS down_hits
FROM r;
"""

    with _open_connection(config=config, credential=credential) as conn:
        cursor = conn.cursor()
        cursor.timeout = config.sql_query_timeout_seconds
        cursor.execute(query)
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError("No rows returned from sys.dm_db_resource_stats query.")

    return MetricSignal(
        sample_count=int(row[0] or 0),
        up_hits=int(row[1] or 0),
        down_hits=int(row[2] or 0),
    )


def execute_statement(*, config: Config, credential: DefaultAzureCredential, statement: str) -> None:
    with _open_connection(config=config, credential=credential) as conn:
        cursor = conn.cursor()
        cursor.timeout = config.sql_query_timeout_seconds
        cursor.execute(statement)


def _open_connection(*, config: Config, credential: DefaultAzureCredential):
    if pyodbc is None:
        raise RuntimeError("pyodbc is required to connect to Azure SQL.")

    token = credential.get_token(_SQL_TOKEN_SCOPE).token
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        f"Driver={{{config.sql_driver}}};"
        f"Server=tcp:{config.sql_server_fqdn},1433;"
        f"Database={config.sql_database_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        f"Connection Timeout={config.sql_connection_timeout_seconds};"
    )

    return pyodbc.connect(
        conn_str,
        attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        autocommit=True,
    )
