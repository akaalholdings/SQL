# Security Policy

## Supported versions

Only the latest release line is supported for security updates.

## Reporting a vulnerability

Please do not open public issues for security vulnerabilities.

Use one of these private channels:

1. Open a GitHub private security advisory for this repository.
2. Contact project maintainers privately if advisory access is unavailable.

Include:

- A clear description of the issue
- Reproduction steps or proof of concept
- Impact assessment
- Suggested remediation (if available)

We will acknowledge reports as quickly as possible, validate the issue, and provide
remediation guidance and release timelines when confirmed.

## Runtime security model

- Keep `AZURE_SQL_ACCESS_MODE=restricted` unless an operator explicitly needs admin tooling.
- `sse` and `streamable-http` require `AZURE_SQL_MCP_BEARER_TOKEN`; clients must send `Authorization: Bearer <token>`.
- Put HTTP/SSE deployments behind TLS and a private network or gateway. The bearer token is not a replacement for TLS.
- Pass secrets (`AZURE_SQL_PASSWORD`, `AZURE_CLIENT_SECRET`, `AZURE_SQL_MCP_BEARER_TOKEN`) as environment variables, not CLI flags — flags are visible in process listings.
- Remote transports do not expose apply-capable admin behavior unless `AZURE_SQL_ENABLE_REMOTE_ADMIN=1` is set.
- Write-capable tools default to dry-run review. Execution requires `dry_run=false` and `AZURE_SQL_WRITE_POLICY=apply`.
- `execute_sql` remains limited to read-only SELECT-style batches. `execute_tsql_unrestricted` accepts native DDL, DML, `EXEC`, DBCC, permission, maintenance, and destructive object operations only within an allowlisted existing database.
- `CREATE DATABASE` and `DROP DATABASE` are invariantly rejected for every admin action, including internally generated actions. SSMS `GO` separators are unsupported.
- Direct/static T-SQL, static stored-procedure calls, and statically reconstructible literal/constant dynamic SQL are accepted subject to the lifecycle guard. Runtime-opaque dynamic SQL is rejected because the MCP cannot prove the database-lifecycle invariant.
- Applied arbitrary batches use an isolated connection and execute once without automatic transient retry. Every result set is drained with its own row cap so an early `SELECT` cannot prevent a later statement from completing.
- A timeout or cancellation is recorded as `apply_outcome_unknown`; reconcile database state before any follow-up because client cancellation cannot prove whether SQL Server committed the batch.
- Audit records are written to `AZURE_SQL_AUDIT_DIR`. By default records include a SQL hash and literal-redacted preview, not full raw SQL; recorded errors also redact T-SQL string literals. Set `AZURE_SQL_AUDIT_FULL_SQL=1` only for a controlled environment that treats the audit as credential-bearing data.
- Query Store apply support is limited to reversible `sp_query_store_force_plan` / `sp_query_store_unforce_plan` actions.

## Scoped DBA principal

The MCP database-lifecycle guard is defence in depth, not the authoritative no-drop control. Run scoped DBA mode under a dedicated principal whose effective SQL Server permissions make database creation and deletion impossible.

For SQL Server, the intended principal may receive these server permissions when operationally required:

- `VIEW SERVER STATE`;
- `ALTER SERVER STATE`;
- `ALTER ANY CONNECTION`.

Inside each explicitly allowlisted database, grant only the required administration surface. The reference scope uses `ALTER`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `EXECUTE`, and `REFERENCES` with grant option, plus `VIEW DEFINITION`, `VIEW DATABASE STATE`, `SHOWPLAN`, `UNMASK`, `ADMINISTER DATABASE BULK OPERATIONS`, `ALTER ANY DATABASE SCOPED CONFIGURATION`, `BACKUP DATABASE`, `BACKUP LOG`, and `CHECKPOINT` where the engine supports them.

Keep the principal out of `sysadmin`, `dbcreator`, `db_owner`, and `db_securityadmin`, and ensure the login owns no database. For SQL Server, do not add it to any fixed server role beyond the implicit `public` role. Do not grant `CONTROL`, `TAKE OWNERSHIP`, `IMPERSONATE`, `CREATE ANY DATABASE`, or `ALTER ANY DATABASE`. Operations whose required permission is inseparable from one of those authorities remain engine-denied; preserving the no-drop guarantee takes precedence over feature completeness.

Verify the effective SQL Server identity while connected as the MCP login:

```sql
SELECT
    IS_SRVROLEMEMBER(N'sysadmin') AS is_sysadmin,
    IS_SRVROLEMEMBER(N'dbcreator') AS is_dbcreator;

SELECT role_principal.name AS fixed_server_role
FROM sys.server_role_members AS membership
JOIN sys.server_principals AS role_principal
    ON role_principal.principal_id = membership.role_principal_id
JOIN sys.server_principals AS member_principal
    ON member_principal.principal_id = membership.member_principal_id
WHERE member_principal.sid = SUSER_SID(ORIGINAL_LOGIN());

SELECT database_name.name AS owned_database
FROM sys.databases AS database_name
WHERE database_name.owner_sid = SUSER_SID(ORIGINAL_LOGIN());
```

Both role flags must be exactly `0` (not `NULL`), and both result sets must be empty. `public` is implicit and therefore does not appear in `sys.server_role_members`.

For Azure SQL Database, do not run this MCP with the provisioning principal, Microsoft Entra administrator, members of `dbmanager`, or members of `##MS_DatabaseManager##`: those identities can create or delete databases. Microsoft documents the role boundary in [Azure SQL server roles](https://learn.microsoft.com/en-us/azure/azure-sql/database/security-server-roles?view=azuresql) and the authoritative permissions in [`DROP DATABASE`](https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-database-transact-sql?view=sql-server-ver17). Use a contained user scoped only to each existing target database instead.
