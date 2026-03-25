# Azure SQL Bulk Copy Tool

This tool is a production-oriented variant of the `mssql-python` bulk copy quickstart for copying a large table from one Azure SQL Database to another.

It is designed for large one-off or bootstrap transfers, including tables in the tens of millions of rows.

## Design

- Reads from a source Azure SQL table in numeric key ranges.
- Writes each range to a local Parquet chunk file.
- Bulk loads each chunk into a destination Azure SQL table with `cursor.bulkcopy()`.
- Persists progress to a JSON state file so reruns resume from the last successful range.
- Keeps schema creation explicit and destination handling configurable.

## When To Use This

Use this when you need to seed or rebuild a large lower-environment table from Azure SQL into Azure SQL.

Do not use this as your steady-state sync engine for huge tables. For recurring refreshes, prefer the metadata-driven ADF framework and incremental/slice patterns.

## Assumptions

- The source table has a numeric chunk column suitable for range slicing.
- The chunk column is usually the clustered primary key or another indexed monotonically increasing column.
- Source and destination are reachable from the machine running the script.

## Install

Using `uv`:

```bash
uv init azure-sql-bulkcopy
uv add mssql-python python-dotenv pyarrow
```

Or using `pip`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install mssql-python python-dotenv pyarrow
```

## Configuration

Copy `.env.example` to `.env` and fill in your values.

Required values:

- `SOURCE_CONNECTION_STRING`
- `DEST_CONNECTION_STRING`
- `SOURCE_SCHEMA`
- `SOURCE_TABLE`
- `DEST_SCHEMA`
- `DEST_TABLE`
- `CHUNK_COLUMN`

Important large-table settings:

- `CHUNK_SIZE`
  Numeric range width per chunk. Start with `1000000` for large tables and adjust.
- `FETCH_BATCH_SIZE`
  Rows fetched into Arrow arrays per inner loop. Default `50000`.
- `BULKCOPY_BATCH_SIZE`
  Rows per `bulkcopy()` call. Default `50000`.
- `TABLE_MODE`
  `recreate`, `truncate`, `append`, or `create_if_missing`.
- `DELETE_PARQUET_AFTER_UPLOAD`
  `true` to keep local storage bounded.

## Example Connection Strings

Azure SQL with Entra interactive auth:

```text
Server=tcp:<server>.database.windows.net,1433;Database=<db>;Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryInteractive
```

Azure SQL with SQL auth:

```text
Server=tcp:<server>.database.windows.net,1433;Database=<db>;Encrypt=yes;TrustServerCertificate=no;Uid=<user>;Pwd=<password>
```

## Run

```bash
python azure_sql_bulkcopy.py
```

## Output

The script creates:

- a work directory at `./work/<dest_schema>_<dest_table>/`
- Parquet chunk files under `parquet/`
- a resumable state file at `state.json`

## Recommended Settings For ~80M Rows

- Use a numeric, indexed chunk column.
- Start with `CHUNK_SIZE=1000000`.
- Start with `FETCH_BATCH_SIZE=50000`.
- Start with `BULKCOPY_BATCH_SIZE=50000`.
- Use `TABLE_MODE=recreate` for a disposable lower-environment target.
- Keep the destination table as a heap during load if possible, then add indexes afterward.

## Important Caveats

- This tool recreates the table structure but not indexes, constraints, triggers, permissions, or partitioning.
- `rowversion` columns are recreated as `VARBINARY(8)` because they cannot be inserted directly.
- The script is intentionally restricted to numeric chunking for predictable large-table behavior.
