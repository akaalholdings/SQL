"""Copy a large table from Azure SQL Database to Azure SQL Database with chunked bulk copy.

This script is optimized for large seed loads where a table can be copied in numeric ranges.
It uses local Parquet chunk files as a resumable handoff between source reads and destination
bulk inserts.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from uuid import UUID

try:
    import mssql_python
except ModuleNotFoundError:
    mssql_python = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError:
    pa = None
    pq = None

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")

if pa is not None:
    SQL_TO_ARROW = {
        "bit": pa.bool_(),
        "tinyint": pa.uint8(),
        "smallint": pa.int16(),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "real": pa.float32(),
        "float": pa.float64(),
        "smallmoney": pa.decimal128(10, 4),
        "money": pa.decimal128(19, 4),
        "date": pa.date32(),
        "datetime": pa.timestamp("ms"),
        "datetime2": pa.timestamp("us"),
        "smalldatetime": pa.timestamp("s"),
        "uniqueidentifier": pa.string(),
        "xml": pa.string(),
        "image": pa.binary(),
        "binary": pa.binary(),
        "varbinary": pa.binary(),
        "timestamp": pa.binary(),
    }
else:
    SQL_TO_ARROW = {}


def ensure_runtime_dependencies() -> None:
    missing: list[str] = []
    if mssql_python is None:
        missing.append("mssql-python")
    if pa is None or pq is None:
        missing.append("pyarrow")
    if load_dotenv is None:
        missing.append("python-dotenv")
    if missing:
        raise ModuleNotFoundError(
            "Missing required packages: "
            + ", ".join(missing)
            + ". Install them before running this script."
        )


@dataclass(frozen=True)
class Config:
    source_connection_string: str
    dest_connection_string: str
    source_schema: str
    source_table: str
    dest_schema: str
    dest_table: str
    chunk_column: str
    chunk_size: int
    fetch_batch_size: int
    bulkcopy_batch_size: int
    table_mode: str
    work_dir: Path
    delete_parquet_after_upload: bool


@dataclass
class State:
    source_schema: str
    source_table: str
    dest_schema: str
    dest_table: str
    chunk_column: str
    chunk_size: int
    source_min: int | None = None
    source_max: int | None = None
    source_row_count: int | None = None
    next_lower_bound: int | None = None
    chunks_completed: int = 0
    rows_downloaded: int = 0
    rows_uploaded: int = 0
    destination_prepared: bool = False
    started_at_utc: str | None = None
    updated_at_utc: str | None = None


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def validate_identifier(name: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def parse_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def load_config() -> Config:
    ensure_runtime_dependencies()
    load_dotenv()

    config = Config(
        source_connection_string=os.environ["SOURCE_CONNECTION_STRING"],
        dest_connection_string=os.environ["DEST_CONNECTION_STRING"],
        source_schema=validate_identifier(os.environ["SOURCE_SCHEMA"]),
        source_table=validate_identifier(os.environ["SOURCE_TABLE"]),
        dest_schema=validate_identifier(os.environ["DEST_SCHEMA"]),
        dest_table=validate_identifier(os.environ["DEST_TABLE"]),
        chunk_column=validate_identifier(os.environ["CHUNK_COLUMN"]),
        chunk_size=parse_int_env("CHUNK_SIZE", 1_000_000),
        fetch_batch_size=parse_int_env("FETCH_BATCH_SIZE", 50_000),
        bulkcopy_batch_size=parse_int_env("BULKCOPY_BATCH_SIZE", 50_000),
        table_mode=os.getenv("TABLE_MODE", "recreate").strip().lower(),
        work_dir=Path(os.getenv("WORK_DIR", "./work")).expanduser(),
        delete_parquet_after_upload=parse_bool("DELETE_PARQUET_AFTER_UPLOAD", True),
    )

    if config.chunk_size <= 0:
        raise ValueError("CHUNK_SIZE must be greater than 0.")
    if config.fetch_batch_size <= 0 or config.bulkcopy_batch_size <= 0:
        raise ValueError("FETCH_BATCH_SIZE and BULKCOPY_BATCH_SIZE must be greater than 0.")
    if config.table_mode not in {"recreate", "truncate", "append", "create_if_missing"}:
        raise ValueError("TABLE_MODE must be one of: recreate, truncate, append, create_if_missing.")

    return config


def sql_type_string(data_type: str, max_length: int, precision: int, scale: int) -> str:
    data_type = data_type.lower()
    if data_type in {"char", "varchar", "binary", "varbinary"}:
        length = "MAX" if max_length == -1 else str(max_length)
        return f"{data_type.upper()}({length})"
    if data_type in {"nchar", "nvarchar"}:
        length = "MAX" if max_length == -1 else str(max_length // 2)
        return f"{data_type.upper()}({length})"
    if data_type in {"decimal", "numeric"}:
        return f"{data_type.upper()}({precision},{scale})"
    if data_type in {"datetime2", "datetimeoffset", "time"}:
        return f"{data_type.upper()}({scale})"
    return data_type.upper()


def arrow_type_for_sql(data_type: str, precision: int, scale: int) -> pa.DataType:
    data_type = data_type.lower()
    if data_type in SQL_TO_ARROW:
        return SQL_TO_ARROW[data_type]
    if data_type in {"decimal", "numeric"}:
        return pa.decimal128(precision, scale)
    if data_type in {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "sysname"}:
        return pa.string()
    return pa.string()


def normalize_value(value):
    if isinstance(value, UUID):
        return str(value)
    return value


def full_table_name(schema_name: str, table_name: str) -> str:
    return f"[{schema_name}].[{table_name}]"


def get_source_schema(cursor, schema_name: str, table_name: str) -> pa.Schema:
    cursor.execute(
        """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            COALESCE(CHARACTER_MAXIMUM_LENGTH, 0),
            COALESCE(NUMERIC_PRECISION, 0),
            COALESCE(NUMERIC_SCALE, 0),
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema_name, table_name),
    )
    rows = cursor.fetchall()
    if not rows:
        raise ValueError(f"No columns found for {schema_name}.{table_name}")

    fields: list[pa.Field] = []
    for column_name, data_type, max_length, precision, scale, is_nullable in rows:
        fields.append(
            pa.field(
                column_name,
                arrow_type_for_sql(data_type, precision, scale),
                nullable=(is_nullable == "YES"),
                metadata={"sql_type": sql_type_string(data_type, max_length, precision, scale)},
            )
        )
    return pa.schema(fields)


def build_table_ddl(target_name: str, schema: pa.Schema) -> str:
    column_definitions: list[str] = []
    for field in schema:
        sql_type = field.metadata[b"sql_type"].decode()
        if sql_type == "TIMESTAMP":
            sql_type = "VARBINARY(8)"
        not_null = "" if field.nullable else " NOT NULL"
        column_definitions.append(f"[{field.name}] {sql_type}{not_null}")
    body = ",\n    ".join(column_definitions)
    return f"CREATE TABLE {target_name} (\n    {body}\n);"


def ensure_destination_schema(conn, schema_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"IF SCHEMA_ID(N'{schema_name}') IS NULL EXEC(N'CREATE SCHEMA [{schema_name}]');"
        )
    conn.commit()


def prepare_destination(conn, config: Config, schema: pa.Schema) -> None:
    ensure_destination_schema(conn, config.dest_schema)

    target_name = full_table_name(config.dest_schema, config.dest_table)
    create_sql = build_table_ddl(target_name, schema)

    with conn.cursor() as cursor:
        if config.table_mode == "recreate":
            cursor.execute(
                f"IF OBJECT_ID(N'{config.dest_schema}.{config.dest_table}', 'U') IS NOT NULL DROP TABLE {target_name};"
            )
            cursor.execute(create_sql)
        elif config.table_mode == "truncate":
            cursor.execute(f"TRUNCATE TABLE {target_name};")
        elif config.table_mode == "create_if_missing":
            cursor.execute(
                f"IF OBJECT_ID(N'{config.dest_schema}.{config.dest_table}', 'U') IS NULL BEGIN {create_sql} END"
            )
        elif config.table_mode == "append":
            cursor.execute(
                f"IF OBJECT_ID(N'{config.dest_schema}.{config.dest_table}', 'U') IS NULL BEGIN {create_sql} END"
            )
    conn.commit()


def get_source_bounds(conn, config: Config) -> tuple[int, int, int]:
    table_name = full_table_name(config.source_schema, config.source_table)
    chunk_column = f"[{config.chunk_column}]"
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                CAST(MIN({chunk_column}) AS BIGINT),
                CAST(MAX({chunk_column}) AS BIGINT),
                COUNT_BIG(*)
            FROM {table_name}
            WHERE {chunk_column} IS NOT NULL
            """
        )
        row = cursor.fetchone()

    if row is None or row[0] is None or row[1] is None:
        raise ValueError(f"Source table {table_name} has no non-null values for {chunk_column}.")

    return int(row[0]), int(row[1]), int(row[2])


def load_state(state_path: Path, config: Config) -> State | None:
    if not state_path.exists():
        return None
    payload = json.loads(state_path.read_text())
    state = State(**payload)
    if (
        state.source_schema != config.source_schema
        or state.source_table != config.source_table
        or state.dest_schema != config.dest_schema
        or state.dest_table != config.dest_table
        or state.chunk_column != config.chunk_column
        or state.chunk_size != config.chunk_size
    ):
        raise ValueError(f"State file {state_path} does not match the current configuration.")
    return state


def save_state(state_path: Path, state: State) -> None:
    state.updated_at_utc = utc_now()
    state_path.write_text(json.dumps(asdict(state), indent=2, sort_keys=True))


def chunk_file_path(parquet_dir: Path, lower_exclusive: int, upper_inclusive: int) -> Path:
    return parquet_dir / f"chunk_{lower_exclusive + 1}_{upper_inclusive}.parquet"


def row_iter_from_batch(batch: pa.RecordBatch) -> Iterable[tuple]:
    return zip(*(column.to_pylist() for column in batch.columns))


def export_chunk_to_parquet(
    conn,
    config: Config,
    schema: pa.Schema,
    parquet_path: Path,
    lower_exclusive: int,
    upper_inclusive: int,
) -> int:
    source_name = full_table_name(config.source_schema, config.source_table)
    chunk_column = f"[{config.chunk_column}]"
    row_count = 0
    started = time.perf_counter()
    writer = None

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM {source_name}
                WHERE {chunk_column} > ? AND {chunk_column} <= ?
                ORDER BY {chunk_column}
                """,
                (lower_exclusive, upper_inclusive),
            )

            while True:
                rows = cursor.fetchmany(config.fetch_batch_size)
                if not rows:
                    break

                columns = [[] for _ in range(len(schema))]
                for row in rows:
                    for index in range(len(schema)):
                        columns[index].append(normalize_value(row[index]))

                arrays = [pa.array(columns[index], type=schema.field(index).type) for index in range(len(schema))]
                batch = pa.record_batch(arrays, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(str(parquet_path), schema, compression="snappy")
                writer.write_batch(batch)
                row_count += len(rows)
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.perf_counter() - started
    rate = f"{int(row_count / elapsed):,} rows/sec" if elapsed > 0 and row_count else "n/a"
    print(
        f"Downloaded chunk ({lower_exclusive}, {upper_inclusive}] -> {parquet_path.name}: "
        f"{row_count:,} rows in {elapsed:.2f}s ({rate})"
    )
    return row_count


def bulkcopy_parquet_to_destination(conn, config: Config, parquet_path: Path) -> int:
    target_name = full_table_name(config.dest_schema, config.dest_table)
    uploaded = 0
    started = time.perf_counter()

    with pq.ParquetFile(str(parquet_path)) as parquet_file:
        with conn.cursor() as cursor:
            for batch in parquet_file.iter_batches(batch_size=config.bulkcopy_batch_size):
                cursor.bulkcopy(
                    target_name,
                    row_iter_from_batch(batch),
                    batch_size=config.bulkcopy_batch_size,
                    table_lock=True,
                    timeout=3600,
                )
                uploaded += batch.num_rows
        conn.commit()

    elapsed = time.perf_counter() - started
    rate = f"{int(uploaded / elapsed):,} rows/sec" if elapsed > 0 and uploaded else "n/a"
    print(f"Uploaded {parquet_path.name} -> {target_name}: {uploaded:,} rows in {elapsed:.2f}s ({rate})")
    return uploaded


def verify_destination_count(conn, config: Config) -> int:
    target_name = full_table_name(config.dest_schema, config.dest_table)
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {target_name}")
        return int(cursor.fetchone()[0])


def initialize_state(config: Config, state_path: Path) -> State:
    existing = load_state(state_path, config)
    if existing is not None:
        return existing

    state = State(
        source_schema=config.source_schema,
        source_table=config.source_table,
        dest_schema=config.dest_schema,
        dest_table=config.dest_table,
        chunk_column=config.chunk_column,
        chunk_size=config.chunk_size,
        started_at_utc=utc_now(),
        updated_at_utc=utc_now(),
    )
    save_state(state_path, state)
    return state


def run_copy(config: Config) -> None:
    run_dir = config.work_dir / f"{config.dest_schema}_{config.dest_table}"
    parquet_dir = run_dir / "parquet"
    state_path = run_dir / "state.json"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    state = initialize_state(config, state_path)
    total_started = time.perf_counter()

    with mssql_python.connect(config.source_connection_string) as source_conn:
        with source_conn.cursor() as cursor:
            source_schema = get_source_schema(cursor, config.source_schema, config.source_table)

        if state.source_min is None or state.source_max is None or state.source_row_count is None:
            source_min, source_max, source_row_count = get_source_bounds(source_conn, config)
            state.source_min = source_min
            state.source_max = source_max
            state.source_row_count = source_row_count
            state.next_lower_bound = source_min - 1
            save_state(state_path, state)
        else:
            source_min = state.source_min
            source_max = state.source_max
            source_row_count = state.source_row_count

        print(
            f"Source bounds for {config.source_schema}.{config.source_table}: "
            f"min={source_min:,}, max={source_max:,}, rows={source_row_count:,}"
        )

        with mssql_python.connect(config.dest_connection_string) as dest_conn:
            if not state.destination_prepared:
                prepare_destination(dest_conn, config, source_schema)
                state.destination_prepared = True
                save_state(state_path, state)

            lower_exclusive = state.next_lower_bound if state.next_lower_bound is not None else source_min - 1
            while lower_exclusive < source_max:
                upper_inclusive = min(lower_exclusive + config.chunk_size, source_max)
                parquet_path = chunk_file_path(parquet_dir, lower_exclusive, upper_inclusive)

                rows_downloaded = export_chunk_to_parquet(
                    source_conn,
                    config,
                    source_schema,
                    parquet_path,
                    lower_exclusive,
                    upper_inclusive,
                )

                rows_uploaded = 0
                if rows_downloaded > 0:
                    rows_uploaded = bulkcopy_parquet_to_destination(dest_conn, config, parquet_path)
                    if rows_uploaded != rows_downloaded:
                        raise RuntimeError(
                            f"Row mismatch for chunk ({lower_exclusive}, {upper_inclusive}]: "
                            f"downloaded {rows_downloaded}, uploaded {rows_uploaded}"
                        )

                state.rows_downloaded += rows_downloaded
                state.rows_uploaded += rows_uploaded
                state.chunks_completed += 1
                state.next_lower_bound = upper_inclusive
                save_state(state_path, state)

                if config.delete_parquet_after_upload and parquet_path.exists():
                    parquet_path.unlink()

                lower_exclusive = upper_inclusive

            destination_count = verify_destination_count(dest_conn, config)

    elapsed = time.perf_counter() - total_started
    print(
        f"Completed copy {config.source_schema}.{config.source_table} -> "
        f"{config.dest_schema}.{config.dest_table} in {elapsed:.2f}s | "
        f"downloaded={state.rows_downloaded:,}, uploaded={state.rows_uploaded:,}, "
        f"destination_count={destination_count:,}"
    )


def main() -> None:
    config = load_config()
    run_copy(config)


if __name__ == "__main__":
    main()
