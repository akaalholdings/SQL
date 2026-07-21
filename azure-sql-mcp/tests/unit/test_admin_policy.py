from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from azure_sql_mcp.admin_policy import AdminAction
from azure_sql_mcp.admin_policy import AdminPolicy
from azure_sql_mcp.config import AccessMode
from azure_sql_mcp.config import WritePolicy
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.connection import QueryResult


def _policy(
    server_config_factory,
    tmp_path: Path,
    write_policy: WritePolicy,
    *,
    audit_full_sql: bool = False,
) -> AdminPolicy:
    config = server_config_factory(
        access_mode=AccessMode.UNRESTRICTED,
        write_policy=write_policy,
        audit_dir=str(tmp_path),
        audit_full_sql=audit_full_sql,
    )
    return AdminPolicy(config)


def _audit_events(tmp_path: Path) -> list[dict[str, Any]]:
    [path] = tmp_path.glob("*.jsonl")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _raw_action(sql: str, *, trusted_generated: bool = False) -> AdminAction:
    return AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql=sql,
        trusted_generated=trusted_generated,
    )


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE dbo.NewTable (Id int NOT NULL)",
        "ALTER TABLE dbo.NewTable ADD Name nvarchar(100)",
        "DROP TABLE dbo.NewTable",
        "CREATE VIEW dbo.NewView AS SELECT 1 AS value",
        "ALTER VIEW dbo.NewView AS SELECT 2 AS value",
        "DROP VIEW dbo.NewView",
        "CREATE PROCEDURE dbo.NewProcedure AS SELECT 1",
        "ALTER PROCEDURE dbo.NewProcedure AS SELECT 2",
        "DROP PROCEDURE dbo.NewProcedure",
        "CREATE INDEX IX_NewTable ON dbo.NewTable (Id)",
        "DROP INDEX IX_NewTable ON dbo.NewTable",
        "INSERT INTO dbo.NewTable (Id) VALUES (1)",
        "UPDATE dbo.NewTable SET Name = N'updated' WHERE Id = 1",
        "DELETE FROM dbo.NewTable WHERE Id = 1",
        "MERGE dbo.NewTable AS target USING dbo.Source AS source ON target.Id = source.Id "
        "WHEN MATCHED THEN UPDATE SET Name = source.Name;",
        "EXEC dbo.usp_refresh_reporting",
        "EXEC sys.sp_executesql N'SELECT 1', N'@value int', @value = 1",
        "DBCC CHECKDB (appdb)",
        "GRANT SELECT ON dbo.NewTable TO app_user",
        "TRUNCATE TABLE dbo.NewTable",
        "ALTER INDEX IX_NewTable ON dbo.NewTable REBUILD",
        "UPDATE STATISTICS dbo.NewTable",
        "SELECT * INTO dbo.UsersCopy FROM dbo.Users",
    ],
)
def test_raw_admin_sql_allows_dba_operations(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)

    payload = policy.preview(_raw_action(sql))

    assert payload["status"] == "dry_run"
    assert _audit_events(tmp_path)[0]["outcome"] == "preview"


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE DATABASE forbidden_db",
        "dRoP /* comment between tokens */ DATABASE forbidden_db",
        "CrEaTe/**/DATABASE forbidden_db",
        "SELECT 1; DROP\n-- comment between tokens\nDATABASE forbidden_db",
        "DROP-- comment ending with CR\rDATABASE forbidden_db",
        "CREATE PROCEDURE dbo.proc_with_forbidden_body AS BEGIN DROP DATABASE forbidden_db END",
        "EXEC(N'DROP DATABASE forbidden_db')",
        "EXECUTE(N'DROP' + N' DATABASE forbidden_db')",
        "EXEC sys.sp_executesql N'DROP DATABASE forbidden_db'",
        "sp_executesql N'DROP DATABASE forbidden_db'",
        "DECLARE @sql nvarchar(max) = N'DROP ' + N'DATABASE forbidden_db'; EXEC(@sql)",
        "EXEC(N'DROP' + SPACE(1) + N'DATABASE forbidden_db')",
        "EXEC(CONCAT(N'DR', N'OP', SPACE(1), N'DATABASE forbidden_db'))",
        "EXEC(N'DROP' + CHAR(32) + N'DATABASE forbidden_db')",
        "EXEC(N'DROP' + NCHAR(32) + N'DATABASE forbidden_db')",
        "EXEC(N'DROP' + NCHAR(0x20) + N'DATABASE forbidden_db')",
        "EXEC(CONCAT(REVERSE(N'PORD'), N' ', REVERSE(N'ESABATAD'), N' forbidden_db'))",
        "EXEC(CONCAT_WS(N' ', N'DROP', N'DATABASE forbidden_db'))",
        "EXEC(REPLACE(N'DROP_DATABASE forbidden_db', N'_', N' '))",
        "EXEC(N'DROP' + REPLICATE(N' ', 1) + N'DATABASE forbidden_db')",
        "DECLARE @verb nvarchar(10) = CONCAT(N'DR', N'OP'); "
        "DECLARE @sql nvarchar(max) = @verb + CHAR(32) + REVERSE(N'ESABATAD') "
        "+ N' forbidden_db'; EXEC(@sql)",
        "DECLARE @sql nvarchar(max) = N'DROP '; "
        "SET @sql += N'DATABASE forbidden_db'; EXEC(@sql)",
        "DECLARE @sql nvarchar(max) = "
        "STUFF(N'DXROP DATABASE forbidden_db', 2, 1, N''); EXEC(@sql)",
        "DECLARE @sql nvarchar(max); SELECT @sql = query_text FROM dbo.AdminQueue; "
        "EXEC(@sql)",
        "DECLARE @p sysname = N'sp_executesql'; "
        "EXEC @p N'DROP DATABASE forbidden_db'",
        "DECLARE @sql nvarchar(max)=N'DROP DATABASE forbidden_db'; "
        "EXEC(@sql); SET @sql=N'SELECT 1'",
        "DECLARE @sql nvarchar(max)=N'DROP DATABASE forbidden_db'; "
        "EXEC sys.sp_executesql @sql; SET @sql=N'SELECT 1'",
        "DECLARE @sql nvarchar(max); "
        "IF 1=1 SET @sql=N'DROP DATABASE forbidden_db'; "
        "IF 1=0 SET @sql=N'SELECT 1'; EXEC(@sql)",
        "DECLARE @p sysname=N'sp_executesql'; "
        "EXEC @p N'DROP DATABASE forbidden_db'; SET @p=N'dbo.safeproc'",
        "DECLARE @p sysname=N'sp_executesql', @rc int; "
        "EXEC @rc = @p N'DROP DATABASE forbidden_db'",
        "DECLARE @p sysname=N'sp_executesql', @rc int; "
        "EXEC @rc = @p N'DROP DATABASE forbidden_db'; SET @p=N'dbo.safeproc'",
        "DECLARE @p sysname, @rc int; SELECT @p = module_name FROM dbo.AdminQueue; "
        "EXEC @rc = @p N'DROP DATABASE forbidden_db'",
        "DECLARE @p sysname=N'sp_prepexec', @h int, @rc int; "
        "EXEC @rc = @p @h OUTPUT, NULL, N'DROP DATABASE forbidden_db'",
        "DECLARE @sql varchar(max)="
        "0x44524F5020444154414241534520666F7262696464656E5F6462; EXEC(@sql)",
        "DECLARE @sql nvarchar(max)=N'SELECT 1'; "
        "DECLARE c CURSOR LOCAL FAST_FORWARD FOR "
        "SELECT N'DROP DATABASE forbidden_db'; OPEN c; "
        "FETCH NEXT FROM c INTO @sql; EXEC(@sql); CLOSE c; DEALLOCATE c",
        "DECLARE @sql nvarchar(max)=N'SELECT 1'; "
        "EXEC dbo.get_command @sql OUTPUT; EXEC(@sql)",
        "DECLARE @sql nvarchar(max)=N'SELECT 1'; "
        "EXEC sys.sp_executesql "
        "N'SET @out=N''DROP DATABASE forbidden_db''', "
        "N'@out nvarchar(max) OUTPUT', @out=@sql OUTPUT; EXEC(@sql)",
        "DECLARE @h int; "
        "EXEC sys.sp_prepexec @h OUTPUT, NULL, N'DROP DATABASE forbidden_db'",
        "DECLARE @h int; "
        "EXEC sys.sp_prepexec;1 @h OUTPUT, NULL, N'DROP DATABASE forbidden_db'",
        "DECLARE @h int; "
        "EXEC sys.sp_prepare @h OUTPUT, NULL, N'CREATE DATABASE forbidden_db'; "
        "EXEC sys.sp_execute @h",
        "DECLARE @h int, @cursor int; EXEC sys.sp_cursorprepexec "
        "@h OUTPUT, @cursor OUTPUT, NULL, N'DROP DATABASE forbidden_db', 1",
        "DECLARE @cursor int; EXEC sys.sp_cursoropen "
        "@cursor OUTPUT, N'CREATE DATABASE forbidden_db', 1, 1, 1",
        "DECLARE @p sysname = N'sp_prepexec', @h int; "
        "EXEC @p @h OUTPUT, NULL, N'DROP DATABASE forbidden_db'",
        "DECLARE @p sysname = N'sp_prepexec;1', @h int; "
        "EXEC @p @h OUTPUT, NULL, N'DROP DATABASE forbidden_db'",
        "EXEC sys.sp_execute_remote N'remote', N'DROP DATABASE forbidden_db'",
        "EXEC sys.sp_sqlexec N'DROP DATABASE forbidden_db'",
        "DECLARE @sql nvarchar(max)=N'DROP DATABASE forbidden_db'; "
        "EXEC sys.sp_sqlexec @sql",
        "DECLARE @p sysname=N'sp_sqlexec'; "
        "EXEC @p N'DROP DATABASE forbidden_db'",
        "DECLARE @p sysname=N'sp_sqlexec', @rc int; "
        "EXEC @rc = @p N'DROP DATABASE forbidden_db'",
        "EXEC master.sys.sp_MSforeachdb N'DROP DATABASE [?]'",
        "EXEC master.dbo.sp_MSforeachdb @command1=N'DROP DATABASE [?]'",
        "EXEC master.dbo.sp_MSforeachdb N'SELECT 1', N'?', "
        "N'DROP DATABASE forbidden_db'",
        "EXEC sys.sp_MSforeachtable N'DROP DATABASE forbidden_db'",
        'SET QUOTED_IDENTIFIER OFF; EXEC("DROP DATABASE forbidden_db")',
        "SELECT * FROM OPENQUERY([linked], N'DROP DATABASE forbidden_db')",
        "SELECT * FROM OPENROWSET(N'provider', N'connection', "
        "N'CREATE DATABASE forbidden_db')",
    ],
)
def test_admin_policy_blocks_database_lifecycle_in_all_contexts(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)

    with pytest.raises(PermissionError, match="CREATE DATABASE and DROP DATABASE"):
        policy.preview(_raw_action(sql))

    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'DROP DATABASE forbidden_db' AS harmless_text",
        "-- DROP DATABASE forbidden_db\rSELECT 1",
        "/* CREATE DATABASE forbidden_db */ SELECT 1",
        "PRINT N'DROP DATABASE forbidden_db'",
        "EXEC(N'SELECT ''DROP DATABASE forbidden_db''')",
        "EXEC dbo.usp_log_message @message = N'DROP DATABASE forbidden_db'",
        "SELECT CONCAT(N'DROP', SPACE(1), N'DATABASE forbidden_db') AS harmless_text",
        'SELECT 1 AS "DROP DATABASE forbidden_db"',
        "SELECT * FROM OPENQUERY([linked], "
        "N'SELECT ''DROP DATABASE forbidden_db'' AS harmless_text')",
        "EXEC dbo.usp_refresh_reporting @mode = N'full'",
        "DECLARE @return_code int; EXEC @return_code = dbo.usp_refresh_reporting",
        "DECLARE @p sysname=N'dbo.usp_refresh_reporting', @return_code int; "
        "EXEC @return_code = @p @mode=N'full'",
        "DECLARE @h int; EXEC sys.sp_prepexec "
        "@h OUTPUT, NULL, N'SELECT 1'; EXEC sys.sp_unprepare @h",
        "DECLARE @h int; EXEC sys.sp_prepare "
        "@h OUTPUT, NULL, N'SELECT 1'; EXEC sys.sp_execute @h",
        "EXEC sys.sp_execute_remote N'remote', N'SELECT 1'",
        "EXEC sys.sp_sqlexec N'SELECT 1'",
        "EXEC master.sys.sp_MSforeachdb N'SELECT DB_NAME()'",
        "EXEC sys.sp_MSforeachtable @command1=N'SELECT 1', @command2=NULL",
    ],
)
def test_harmless_string_comment_and_nonexecuted_mentions_remain_allowed(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)

    payload = policy.preview(_raw_action(sql))

    assert payload["status"] == "dry_run"


@pytest.mark.parametrize("sql", ["", "   \n", "/* only a comment */ -- another comment"])
def test_admin_policy_rejects_empty_sql(
    server_config_factory,
    tmp_path: Path,
    sql: str,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)

    with pytest.raises(PermissionError, match="SQL cannot be empty"):
        policy.preview(_raw_action(sql))

    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


@pytest.mark.asyncio
async def test_trusted_generated_actions_still_run_the_lifecycle_guard_before_execution(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="update_statistics",
        database_name="appdb",
        action_type="maintenance",
        sql="DROP DATABASE forbidden_db",
        trusted_generated=True,
    )
    executor = AsyncMock()

    with pytest.raises(PermissionError, match="CREATE DATABASE and DROP DATABASE"):
        await policy.execute(action, executor, dry_run=False)

    executor.execute_non_query.assert_not_awaited()
    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


def test_admin_policy_preview_audits_without_full_sql(server_config_factory, tmp_path: Path) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="update_statistics",
        database_name="appdb",
        action_type="maintenance",
        sql="UPDATE STATISTICS [dbo].[Orders]",
        trusted_generated=True,
    )

    payload = policy.preview(action)

    assert payload["status"] == "dry_run"
    assert payload["dry_run"] is True
    events = _audit_events(tmp_path)
    assert events[0]["outcome"] == "preview"
    assert events[0]["sql_hash"]
    assert "sql" not in events[0]


@pytest.mark.asyncio
async def test_admin_policy_blocks_apply_when_write_policy_is_review(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="rebuild_index",
        database_name="appdb",
        action_type="maintenance",
        sql="ALTER INDEX [IX] ON [dbo].[Orders] REBUILD",
        trusted_generated=True,
    )
    executor = AsyncMock()

    with pytest.raises(PermissionError, match="AZURE_SQL_WRITE_POLICY=apply"):
        await policy.execute(action, executor, dry_run=False)

    assert executor.execute_non_query.await_count == 0
    assert _audit_events(tmp_path)[0]["outcome"] == "blocked"


@pytest.mark.asyncio
async def test_admin_policy_executes_generated_query_store_action_when_apply_enabled(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="apply_plan_action",
        database_name="appdb",
        action_type="query_store",
        sql="EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?",
        params=(42, 7),
        trusted_generated=True,
    )
    executor = AsyncMock()
    executor.execute_non_query = AsyncMock(return_value=0)

    payload = await policy.execute(action, executor, dry_run=False)

    executor.execute_non_query.assert_awaited_once_with(
        "appdb",
        "EXEC sp_query_store_force_plan @query_id = ?, @plan_id = ?",
        params=(42, 7),
        execution_mode=BatchExecutionMode.ADMIN,
    )
    assert payload["status"] == "completed"
    assert [event["outcome"] for event in _audit_events(tmp_path)] == [
        "apply_started",
        "apply_completed",
    ]


@pytest.mark.asyncio
async def test_raw_admin_batch_uses_admin_execution_mode(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = _raw_action("UPDATE dbo.Orders SET IsArchived = 1 WHERE OrderId = 7")
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(
        return_value=[QueryResult(columns=("value",), rows=[{"value": 1}])]
    )

    payload = await policy.execute(action, executor, dry_run=False, max_rows=10)

    executor.execute_batches.assert_awaited_once_with(
        "appdb",
        "UPDATE dbo.Orders SET IsArchived = 1 WHERE OrderId = 7",
        params=(),
        max_rows=10,
        execution_mode=BatchExecutionMode.ADMIN,
    )
    assert payload["status"] == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("query timed out near 'private value'"),
        RuntimeError("HYT00: query timeout expired near 'private value'"),
    ],
)
async def test_timeout_errors_are_audited_as_unknown_with_literals_redacted(
    server_config_factory,
    tmp_path: Path,
    error: Exception,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(side_effect=error)

    with pytest.raises(type(error)):
        await policy.execute(_raw_action("SELECT 1"), executor, dry_run=False)

    events = _audit_events(tmp_path)
    assert [event["outcome"] for event in events] == [
        "apply_started",
        "apply_outcome_unknown",
    ]
    assert "private value" not in events[-1]["error"]


@pytest.mark.asyncio
async def test_non_timeout_executor_errors_are_failed_and_sanitized(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(
        side_effect=RuntimeError(
            "Server=tcp:prod.database.windows.net;UID=sa;PWD=secret!; near 'private value'"
        )
    )

    with pytest.raises(RuntimeError):
        await policy.execute(_raw_action("SELECT 1"), executor, dry_run=False)

    events = _audit_events(tmp_path)
    assert [event["outcome"] for event in events] == ["apply_started", "apply_failed"]
    assert "secret!" not in events[-1]["error"]
    assert "prod.database.windows.net" not in events[-1]["error"]
    assert "private value" not in events[-1]["error"]


@pytest.mark.asyncio
async def test_cancellation_is_audited_as_unknown_and_reraised(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await policy.execute(
            _raw_action("UPDATE dbo.Orders SET IsArchived = 1"),
            executor,
            dry_run=False,
        )

    assert [event["outcome"] for event in _audit_events(tmp_path)] == [
        "apply_started",
        "apply_outcome_unknown",
    ]


def test_sql_previews_and_default_audit_fields_redact_literals(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.REVIEW)
    action = AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql="CREATE LOGIN test WITH PASSWORD = 'placeholder'",
        rollback_sql="EXEC dbo.restore_value N'secret rollback value'",
    )

    payload = policy.preview(action)
    event = _audit_events(tmp_path)[0]

    assert "placeholder" not in payload["sql_preview"]
    assert payload["rollback_sql"] == "EXEC dbo.restore_value N'secret rollback value'"
    assert "placeholder" not in json.dumps(event)
    assert "secret rollback value" not in json.dumps(event)
    assert "'[REDACTED]'" in payload["sql_preview"]
    assert "sql" not in event


def test_full_sql_audit_option_keeps_full_sql_but_preview_redacted(
    server_config_factory,
    tmp_path: Path,
) -> None:
    policy = _policy(
        server_config_factory,
        tmp_path,
        WritePolicy.REVIEW,
        audit_full_sql=True,
    )
    sql = "SELECT 'secret value' AS value"

    policy.preview(_raw_action(sql))
    event = _audit_events(tmp_path)[0]

    assert event["sql"] == sql
    assert "secret value" not in event["sql_preview"]


@pytest.mark.asyncio
async def test_admin_policy_query_results_are_json_safe(server_config_factory, tmp_path: Path) -> None:
    policy = _policy(server_config_factory, tmp_path, WritePolicy.APPLY)
    action = AdminAction(
        tool_name="execute_tsql_unrestricted",
        database_name="appdb",
        action_type="query",
        sql="SELECT 1 AS value",
    )
    executor = AsyncMock()
    executor.execute_batches = AsyncMock(
        return_value=[QueryResult(columns=("value",), rows=[{"value": 1}])]
    )

    payload = await policy.execute(action, executor, dry_run=False, max_rows=10)

    executor.execute_batches.assert_awaited_once_with(
        "appdb",
        "SELECT 1 AS value",
        params=(),
        max_rows=10,
        execution_mode=BatchExecutionMode.ADMIN,
    )

    assert payload["result_sets"] == [
        {
            "columns": ["value"],
            "rows": [{"value": 1}],
            "row_count": 1,
        }
    ]
