# query_geneva_db DBA tuning capabilities

These capabilities are strictly DBA-only. Every command in this file requires `--dba`; production aliases also require `--allow-prod`. Platform permissions, Azure SQL auditing, and local `audit.jsonl` remain the authoritative controls.

Use these modes to collect evidence for the `sql_optimizer` skill: original query or view definition, actual execution plan XML, index inventory, table size, statistics, Query Store history, and result-equivalence evidence.

## Production hard rules

For the production database `mid` on `ShellGeneva.database.windows.net`, use CLI alias `mid_prod` with `--manage-prod` for approved production DBA maintenance. The existing `mid` alias is the read-only replica. These operations are blocked before any read-write connection is opened:

- `KILL`
- `DROP DATABASE`
- `ALTER DATABASE`
- `DROP TABLE`
- DBCC repair options
- Multiple production maintenance statements in one command
- Anything outside the production DBA allowlist

The production DBA allowlist is intentionally narrow: `CREATE VIEW`, `CREATE OR ALTER VIEW`, `ALTER VIEW`, `DROP VIEW`, `CREATE INDEX`, `ALTER INDEX` rebuild/reorganize/resume/pause/abort actions, `DROP INDEX`, `CREATE STATISTICS`, `UPDATE STATISTICS`, `DROP STATISTICS`, selected `DBCC CHECK/SHOW` commands, and `EXEC sys.sp_updatestats`.

Example:

```text
query_geneva_db mid_prod --dba --allow-prod --manage-prod "CREATE OR ALTER VIEW dbo.MyView AS SELECT c = 1"
```

## Tuning capture

Capture an actual execution plan, result sample, row signature, elapsed time, and best-effort `STATISTICS IO/TIME` messages:

```text
query_geneva_db mid_dev --dba --tune-capture --query-file query.sql --max-rows 100
```

Artifacts are written to `~/.copilot/skills/query_geneva_db/tuning/<timestamp>` unless `--output-dir` is provided:

- `query.sql`
- `actual_plan.sqlplan`
- `result_set_N.json`
- `statistics_messages.txt` when exposed by the driver
- `tune_capture.json`

Use `--dba-exec` with `--tune-capture` when a DBA needs parameter setup or stored procedure execution for plan capture:

```text
query_geneva_db mid_dev --dba --dba-exec --tune-capture --query-file parameterized_batch.sql
```

`--dba-exec` allows guarded read-oriented batches starting with `DECLARE`, `SET`, `SELECT`, `WITH`, `EXEC`, or `EXECUTE`, and blocks explicit DML/DDL/server commands in the submitted batch.

## Metadata helpers

All metadata helpers are read-only and require `--dba`.

```text
query_geneva_db mid_dev --dba --index-inventory dbo.TableName --format json
query_geneva_db mid_dev --dba --table-size dbo.TableName
query_geneva_db mid_dev --dba --index-usage dbo.TableName
query_geneva_db mid_dev --dba --stats-info dbo.TableName
query_geneva_db mid_dev --dba --stats-info dbo.TableName --stats-name IX_TableName_Column
query_geneva_db mid_dev --dba --fragmentation dbo.TableName
query_geneva_db mid_dev --dba --object-definition dbo.ViewOrProcedureName --format json
```

These correspond to the evidence expected by `sql_optimizer`: existing index definitions, row/page counts, usage counters, statistics freshness/histogram details, fragmentation/page-density context, and exact object definitions.

## Query Store helpers

Read helpers:

```text
query_geneva_db mid_dev --dba --query-store top --query-store-metric duration --query-store-top 20
query_geneva_db mid_dev --dba --query-store regressed --query-store-metric cpu --query-store-text-like "%SomeTable%"
query_geneva_db mid_dev --dba --query-store history --query-store-query-id 123 --query-store-plan-id 456
query_geneva_db mid_dev --dba --query-store plan --query-store-plan-id 456 --format json
```

Write helpers are DBA-only and use a read-write connection. Production still requires `--allow-prod`:

```text
query_geneva_db mid_prod --dba --allow-prod --query-store-force-plan --query-store-query-id 123 --query-store-plan-id 456
query_geneva_db mid_prod --dba --allow-prod --query-store-unforce-plan --query-store-query-id 123 --query-store-plan-id 456
query_geneva_db mid_prod --dba --allow-prod --query-store-set-hints "OPTION(RECOMPILE)" --query-store-query-id 123
query_geneva_db mid_prod --dba --allow-prod --query-store-clear-hints --query-store-query-id 123
```

## Benchmark and result validation

Run original and candidate SQL on the same alias, compare result sets duplicate-safely, and report timings:

```text
query_geneva_db mid_dev --dba --benchmark --query-file original.sql --query-file2 candidate.sql --format json
```

The comparison uses canonicalized row multisets, so duplicate differences are detected. If either query is truncated by `--max-rows`, the comparison is marked as incomplete.

## Recommended SQL optimizer workflow

1. Capture the original query or view definition with `--object-definition` if needed.
2. Capture the baseline with `--tune-capture`.
3. Run `--index-inventory`, `--table-size`, `--index-usage`, and `--stats-info` for each base table from the query or plan.
4. Use Query Store helpers for historical runtime, regressions, and alternate plan XML.
5. Send the query, `actual_plan.sqlplan`, metadata outputs, and representative parameter values to `sql_optimizer`.
6. Validate the optimized query with `--benchmark`.
7. Apply any approved view/index/statistics/Query Store changes only with `--dba` and the relevant production/non-production approval flags.
