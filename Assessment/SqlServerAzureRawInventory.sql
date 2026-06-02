/*
SQL Server Azure Raw Inventory Collector
Target: SQL Server 2016 SP3+
Use: run in SSMS against the source SQL Server instance and save/export result sets.

This script is read-only. It does not export table data.
*/

SET NOCOUNT ON;

DECLARE @NowUtc datetime2(0) = SYSUTCDATETIME();

------------------------------------------------------------
-- 1. SERVER PROPERTIES
------------------------------------------------------------
SELECT
    'server_properties' AS result_set,
    @NowUtc AS collection_time_utc,
    CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')) AS machine_name,
    CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')) AS server_name,
    CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')) AS instance_name,
    CONVERT(nvarchar(128), SERVERPROPERTY('Edition')) AS edition,
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')) AS product_version,
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')) AS product_level,
    CONVERT(nvarchar(128), SERVERPROPERTY('ProductUpdateLevel')) AS product_update_level,
    CONVERT(int, SERVERPROPERTY('EngineEdition')) AS engine_edition,
    CONVERT(nvarchar(128), SERVERPROPERTY('Collation')) AS server_collation,
    CONVERT(int, SERVERPROPERTY('IsClustered')) AS is_clustered,
    CONVERT(int, SERVERPROPERTY('IsHadrEnabled')) AS is_hadr_enabled,
    osi.cpu_count,
    osi.scheduler_count,
    CONVERT(decimal(18,2), osi.physical_memory_kb / 1024.0) AS physical_memory_mb,
    osi.sqlserver_start_time
FROM sys.dm_os_sys_info AS osi;

------------------------------------------------------------
-- 2. SERVER CONFIGURATION
------------------------------------------------------------
SELECT
    'server_configurations' AS result_set,
    name AS configuration_name,
    value,
    value_in_use,
    description,
    is_dynamic,
    is_advanced
FROM sys.configurations
ORDER BY name;

------------------------------------------------------------
-- 3. DATABASES
------------------------------------------------------------
SELECT
    'databases' AS result_set,
    d.name AS database_name,
    d.database_id,
    d.state_desc,
    d.user_access_desc,
    d.compatibility_level,
    d.recovery_model_desc,
    d.collation_name,
    d.containment_desc,
    d.create_date,
    d.is_read_only,
    d.is_auto_close_on,
    d.is_auto_shrink_on,
    d.page_verify_option_desc,
    d.is_broker_enabled,
    d.is_cdc_enabled,
    d.is_encrypted,
    d.is_trustworthy_on,
    SUSER_SNAME(d.owner_sid) AS owner_name
FROM sys.databases AS d
WHERE d.database_id > 4
  AND d.source_database_id IS NULL
ORDER BY d.name;

------------------------------------------------------------
-- 4. DATABASE FILES
------------------------------------------------------------
SELECT
    'database_files' AS result_set,
    DB_NAME(mf.database_id) AS database_name,
    mf.name AS logical_name,
    mf.file_id,
    mf.type_desc,
    mf.physical_name,
    CONVERT(decimal(18,2), mf.size * 8.0 / 1024.0) AS size_mb,
    CASE WHEN mf.max_size = -1 THEN -1 ELSE CONVERT(decimal(18,2), mf.max_size * 8.0 / 1024.0) END AS max_size_mb,
    CASE WHEN mf.is_percent_growth = 1
         THEN CONVERT(nvarchar(32), mf.growth) + N' percent'
         ELSE CONVERT(nvarchar(32), CONVERT(decimal(18,2), mf.growth * 8.0 / 1024.0)) + N' MB'
    END AS growth_setting,
    mf.is_percent_growth,
    mf.state_desc,
    mf.is_read_only,
    mf.is_sparse
FROM sys.master_files AS mf
JOIN sys.databases AS d
    ON mf.database_id = d.database_id
WHERE d.database_id > 4
  AND d.source_database_id IS NULL
ORDER BY d.name, mf.file_id;

------------------------------------------------------------
-- 5. LINKED SERVERS
------------------------------------------------------------
SELECT
    'linked_servers' AS result_set,
    name AS linked_server_name,
    provider,
    product,
    data_source,
    catalog,
    is_data_access_enabled,
    is_rpc_out_enabled,
    is_remote_login_enabled,
    uses_remote_collation
FROM sys.servers
WHERE is_linked = 1
ORDER BY name;

------------------------------------------------------------
-- 6. SQL AGENT JOBS
------------------------------------------------------------
SELECT
    'sql_agent_jobs' AS result_set,
    CONVERT(nvarchar(36), j.job_id) AS job_id,
    j.name AS job_name,
    j.enabled,
    SUSER_SNAME(j.owner_sid) AS owner_name,
    c.name AS category_name,
    j.date_created,
    j.date_modified,
    COUNT(s.step_id) AS step_count,
    SUM(CASE WHEN s.subsystem IN (N'CmdExec', N'PowerShell') THEN 1 ELSE 0 END) AS command_step_count,
    COUNT(DISTINCT NULLIF(s.database_name, N'')) AS referenced_database_count,
    COUNT(DISTINCT js.schedule_id) AS schedule_count
FROM msdb.dbo.sysjobs AS j
LEFT JOIN msdb.dbo.syscategories AS c
    ON j.category_id = c.category_id
LEFT JOIN msdb.dbo.sysjobsteps AS s
    ON j.job_id = s.job_id
LEFT JOIN msdb.dbo.sysjobschedules AS js
    ON j.job_id = js.job_id
GROUP BY j.job_id, j.name, j.enabled, j.owner_sid, c.name, j.date_created, j.date_modified
ORDER BY j.name;

------------------------------------------------------------
-- 7. SQL AGENT JOB STEPS - SANITIZED PREVIEW ONLY
------------------------------------------------------------
SELECT
    'sql_agent_job_steps' AS result_set,
    CONVERT(nvarchar(36), j.job_id) AS job_id,
    j.name AS job_name,
    s.step_id,
    s.step_name,
    s.subsystem,
    s.database_name,
    p.name AS proxy_name,
    CONVERT(varchar(130), sys.fn_varbintohexstr(HASHBYTES('SHA2_256', CONVERT(nvarchar(4000), s.command)))) AS command_hash,
    LEFT(
        REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(max), s.command), CHAR(13), N' '), CHAR(10), N' '), CHAR(9), N' '),
        500
    ) AS command_preview_sanitized_review_required,
    s.output_file_name
FROM msdb.dbo.sysjobs AS j
JOIN msdb.dbo.sysjobsteps AS s
    ON j.job_id = s.job_id
LEFT JOIN msdb.dbo.sysproxies AS p
    ON s.proxy_id = p.proxy_id
ORDER BY j.name, s.step_id;

------------------------------------------------------------
-- 8. HA / DR TOPOLOGY
------------------------------------------------------------
SELECT
    'server_ha_properties' AS result_set,
    CONVERT(int, SERVERPROPERTY('IsClustered')) AS is_clustered,
    CONVERT(int, SERVERPROPERTY('IsHadrEnabled')) AS is_hadr_enabled;

SELECT
    'availability_groups' AS result_set,
    ag.name AS availability_group_name,
    ar.replica_server_name,
    ar.availability_mode_desc,
    ar.failover_mode_desc,
    agl.dns_name AS listener_name
FROM sys.availability_groups AS ag
LEFT JOIN sys.availability_replicas AS ar
    ON ag.group_id = ar.group_id
LEFT JOIN sys.availability_group_listeners AS agl
    ON ag.group_id = agl.group_id
ORDER BY ag.name, ar.replica_server_name;

SELECT
    'availability_group_databases' AS result_set,
    DB_NAME(drs.database_id) AS database_name,
    ag.name AS availability_group_name,
    ar.replica_server_name,
    drs.synchronization_state_desc,
    drs.synchronization_health_desc
FROM sys.dm_hadr_database_replica_states AS drs
LEFT JOIN sys.availability_replicas AS ar
    ON drs.replica_id = ar.replica_id
LEFT JOIN sys.availability_groups AS ag
    ON ar.group_id = ag.group_id
ORDER BY DB_NAME(drs.database_id), ar.replica_server_name;

SELECT
    'log_shipping' AS result_set,
    pd.primary_database AS database_name,
    ps.secondary_server,
    ps.secondary_database
FROM msdb.dbo.log_shipping_primary_secondaries AS ps
JOIN msdb.dbo.log_shipping_primary_databases AS pd
    ON ps.primary_id = pd.primary_id
ORDER BY pd.primary_database, ps.secondary_server;

SELECT
    'database_mirroring' AS result_set,
    DB_NAME(database_id) AS database_name,
    mirroring_role_desc,
    mirroring_state_desc,
    mirroring_partner_instance
FROM sys.database_mirroring
WHERE mirroring_guid IS NOT NULL
ORDER BY DB_NAME(database_id);

------------------------------------------------------------
-- 9. SERVER SECURITY PRINCIPALS
------------------------------------------------------------
SELECT
    'server_security_principals' AS result_set,
    sp.name AS principal_name,
    sp.type_desc AS principal_type_desc,
    sl.is_disabled,
    sl.default_database_name,
    sp.create_date,
    sp.modify_date
FROM sys.server_principals AS sp
LEFT JOIN sys.sql_logins AS sl
    ON sp.principal_id = sl.principal_id
WHERE sp.type IN (N'S', N'U', N'G')
  AND sp.name NOT LIKE N'##%'
ORDER BY sp.type_desc, sp.name;

------------------------------------------------------------
-- 10. WAIT STATS SNAPSHOT
------------------------------------------------------------
SELECT TOP (200)
    'wait_stats_snapshot' AS result_set,
    @NowUtc AS sample_time_utc,
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    signal_wait_time_ms,
    wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms,
    max_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_type NOT LIKE N'SLEEP%'
  AND wait_type NOT LIKE N'BROKER%'
  AND wait_type NOT LIKE N'XE_%'
  AND wait_type NOT IN (
      N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'CHECKPOINT_QUEUE',
      N'REQUEST_FOR_DEADLOCK_SEARCH', N'SQLTRACE_BUFFER_FLUSH',
      N'WAITFOR', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT'
  )
ORDER BY wait_time_ms DESC;

------------------------------------------------------------
-- 11. IO FILE STATS SNAPSHOT
------------------------------------------------------------
SELECT
    'io_file_stats_snapshot' AS result_set,
    @NowUtc AS sample_time_utc,
    DB_NAME(vfs.database_id) AS database_name,
    mf.name AS logical_name,
    vfs.file_id,
    mf.type_desc,
    vfs.num_of_reads,
    vfs.num_of_bytes_read,
    vfs.io_stall_read_ms,
    vfs.num_of_writes,
    vfs.num_of_bytes_written,
    vfs.io_stall_write_ms,
    vfs.io_stall AS io_stall_ms,
    CONVERT(decimal(18,2), vfs.size_on_disk_bytes / 1024.0 / 1024.0) AS size_on_disk_mb
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
JOIN sys.master_files AS mf
    ON vfs.database_id = mf.database_id
   AND vfs.file_id = mf.file_id
JOIN sys.databases AS d
    ON vfs.database_id = d.database_id
WHERE d.database_id > 4
ORDER BY d.name, vfs.file_id;

------------------------------------------------------------
-- 12. DATABASE-LEVEL DEEP SCAN
------------------------------------------------------------
IF OBJECT_ID('tempdb..#database_features') IS NOT NULL DROP TABLE #database_features;
IF OBJECT_ID('tempdb..#database_dependencies') IS NOT NULL DROP TABLE #database_dependencies;
IF OBJECT_ID('tempdb..#object_feature_scan') IS NOT NULL DROP TABLE #object_feature_scan;
IF OBJECT_ID('tempdb..#query_store_summary') IS NOT NULL DROP TABLE #query_store_summary;
IF OBJECT_ID('tempdb..#database_security_principals') IS NOT NULL DROP TABLE #database_security_principals;

CREATE TABLE #database_features
(
    database_name sysname,
    feature_name nvarchar(128),
    detected bit,
    feature_value nvarchar(4000),
    evidence nvarchar(4000)
);

CREATE TABLE #database_dependencies
(
    database_name sysname,
    referencing_schema_name sysname NULL,
    referencing_object_name sysname NULL,
    referencing_object_type nvarchar(60) NULL,
    referenced_server_name sysname NULL,
    referenced_database_name sysname NULL,
    referenced_schema_name sysname NULL,
    referenced_entity_name nvarchar(256) NULL,
    dependency_type nvarchar(60),
    is_ambiguous bit NULL
);

CREATE TABLE #object_feature_scan
(
    database_name sysname,
    schema_name sysname,
    object_name sysname,
    object_type nvarchar(60),
    feature_name nvarchar(128),
    definition_hash varchar(130),
    sanitized_snippet nvarchar(500),
    azure_sql_db_impact nvarchar(4000),
    azure_sql_mi_impact nvarchar(4000),
    sql_vm_impact nvarchar(4000)
);

CREATE TABLE #query_store_summary
(
    database_name sysname,
    actual_state_desc nvarchar(60),
    desired_state_desc nvarchar(60),
    readonly_reason bigint,
    current_storage_size_mb bigint,
    max_storage_size_mb bigint,
    query_count bigint,
    plan_count bigint,
    runtime_interval_count bigint
);

CREATE TABLE #database_security_principals
(
    database_name sysname,
    principal_name sysname,
    principal_type_desc nvarchar(60),
    authentication_type_desc nvarchar(60),
    mapped_login_name sysname NULL,
    create_date datetime,
    modify_date datetime,
    roles nvarchar(max) NULL
);

DECLARE @DbName sysname;
DECLARE @Sql nvarchar(max);

DECLARE dbs CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND source_database_id IS NULL
  AND state_desc = N'ONLINE'
ORDER BY name;

OPEN dbs;
FETCH NEXT FROM dbs INTO @DbName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @Sql = N'
USE ' + QUOTENAME(@DbName) + N';

DECLARE
    @filegroup_count int = 0,
    @filestream_filegroup_count int = 0,
    @memory_optimized_table_count int = 0,
    @filetable_count int = 0,
    @external_table_count int = 0,
    @fulltext_catalog_count int = 0,
    @partition_scheme_count int = 0,
    @user_assembly_count int = 0,
    @synonym_count int = 0,
    @cross_database_reference_count int = 0,
    @service_broker_enabled int = 0,
    @cdc_enabled int = 0,
    @change_tracking_enabled int = 0,
    @tde_enabled int = 0,
    @query_store_state nvarchar(60) = N''Unavailable'',
    @largest_table nvarchar(300) = N'''',
    @largest_table_mb decimal(18,2) = 0;

SELECT @filegroup_count = COUNT(*) FROM sys.filegroups;
SELECT @filestream_filegroup_count = COUNT(*) FROM sys.filegroups WHERE type = N''FD'';

IF COL_LENGTH(N''sys.tables'', N''is_memory_optimized'') IS NOT NULL
    SELECT @memory_optimized_table_count = COUNT(*) FROM sys.tables WHERE is_memory_optimized = 1;

IF COL_LENGTH(N''sys.tables'', N''is_filetable'') IS NOT NULL
    SELECT @filetable_count = COUNT(*) FROM sys.tables WHERE is_filetable = 1;

IF OBJECT_ID(N''sys.external_tables'') IS NOT NULL
    SELECT @external_table_count = COUNT(*) FROM sys.external_tables;

IF OBJECT_ID(N''sys.fulltext_catalogs'') IS NOT NULL
    SELECT @fulltext_catalog_count = COUNT(*) FROM sys.fulltext_catalogs;

SELECT @partition_scheme_count = COUNT(*) FROM sys.partition_schemes;
SELECT @user_assembly_count = COUNT(*) FROM sys.assemblies WHERE is_user_defined = 1;
SELECT @synonym_count = COUNT(*) FROM sys.synonyms;

SELECT @cross_database_reference_count = COUNT(DISTINCT referenced_database_name)
FROM sys.sql_expression_dependencies
WHERE referenced_database_name IS NOT NULL
  AND referenced_database_name <> DB_NAME();

SELECT @service_broker_enabled = CONVERT(int, is_broker_enabled),
       @cdc_enabled = CONVERT(int, is_cdc_enabled),
       @tde_enabled = CONVERT(int, is_encrypted)
FROM sys.databases
WHERE database_id = DB_ID();

IF EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
    SET @change_tracking_enabled = 1;

IF OBJECT_ID(N''sys.database_query_store_options'') IS NOT NULL
    SELECT @query_store_state = actual_state_desc FROM sys.database_query_store_options;

SELECT TOP (1)
    @largest_table = QUOTENAME(SCHEMA_NAME(t.schema_id)) + N''.'' + QUOTENAME(t.name),
    @largest_table_mb = CONVERT(decimal(18,2), SUM(a.total_pages) * 8.0 / 1024.0)
FROM sys.tables AS t
JOIN sys.indexes AS i
    ON t.object_id = i.object_id
JOIN sys.partitions AS p
    ON i.object_id = p.object_id
   AND i.index_id = p.index_id
JOIN sys.allocation_units AS a
    ON p.partition_id = a.container_id
WHERE t.is_ms_shipped = 0
GROUP BY t.schema_id, t.name
ORDER BY SUM(a.total_pages) DESC;

INSERT #database_features(database_name, feature_name, detected, feature_value, evidence)
VALUES
(DB_NAME(), N''filegroup_count'', CASE WHEN @filegroup_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @filegroup_count), N''filegroup_count='' + CONVERT(nvarchar(32), @filegroup_count)),
(DB_NAME(), N''filestream_filegroups'', CASE WHEN @filestream_filegroup_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @filestream_filegroup_count), N''filestream_filegroup_count='' + CONVERT(nvarchar(32), @filestream_filegroup_count)),
(DB_NAME(), N''memory_optimized_tables'', CASE WHEN @memory_optimized_table_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @memory_optimized_table_count), N''memory_optimized_table_count='' + CONVERT(nvarchar(32), @memory_optimized_table_count)),
(DB_NAME(), N''filetables'', CASE WHEN @filetable_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @filetable_count), N''filetable_count='' + CONVERT(nvarchar(32), @filetable_count)),
(DB_NAME(), N''external_tables'', CASE WHEN @external_table_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @external_table_count), N''external_table_count='' + CONVERT(nvarchar(32), @external_table_count)),
(DB_NAME(), N''fulltext_catalogs'', CASE WHEN @fulltext_catalog_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @fulltext_catalog_count), N''fulltext_catalog_count='' + CONVERT(nvarchar(32), @fulltext_catalog_count)),
(DB_NAME(), N''partition_schemes'', CASE WHEN @partition_scheme_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @partition_scheme_count), N''partition_scheme_count='' + CONVERT(nvarchar(32), @partition_scheme_count)),
(DB_NAME(), N''user_assemblies'', CASE WHEN @user_assembly_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @user_assembly_count), N''user_assembly_count='' + CONVERT(nvarchar(32), @user_assembly_count)),
(DB_NAME(), N''synonyms'', CASE WHEN @synonym_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @synonym_count), N''synonym_count='' + CONVERT(nvarchar(32), @synonym_count)),
(DB_NAME(), N''cross_database_references'', CASE WHEN @cross_database_reference_count > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @cross_database_reference_count), N''cross_database_reference_count='' + CONVERT(nvarchar(32), @cross_database_reference_count)),
(DB_NAME(), N''service_broker'', CASE WHEN @service_broker_enabled > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @service_broker_enabled), N''service_broker_enabled='' + CONVERT(nvarchar(32), @service_broker_enabled)),
(DB_NAME(), N''cdc'', CASE WHEN @cdc_enabled > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @cdc_enabled), N''cdc_enabled='' + CONVERT(nvarchar(32), @cdc_enabled)),
(DB_NAME(), N''change_tracking'', CASE WHEN @change_tracking_enabled > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @change_tracking_enabled), N''change_tracking_enabled='' + CONVERT(nvarchar(32), @change_tracking_enabled)),
(DB_NAME(), N''tde'', CASE WHEN @tde_enabled > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @tde_enabled), N''tde_enabled='' + CONVERT(nvarchar(32), @tde_enabled)),
(DB_NAME(), N''query_store_state'', CASE WHEN @query_store_state <> N''Unavailable'' THEN 1 ELSE 0 END, @query_store_state, N''query_store_state='' + @query_store_state),
(DB_NAME(), N''largest_table_mb'', CASE WHEN @largest_table_mb > 0 THEN 1 ELSE 0 END, CONVERT(nvarchar(4000), @largest_table_mb), ISNULL(@largest_table, N'''') + N''='' + CONVERT(nvarchar(64), @largest_table_mb));

INSERT #database_dependencies
SELECT
    DB_NAME(),
    OBJECT_SCHEMA_NAME(d.referencing_id),
    OBJECT_NAME(d.referencing_id),
    o.type_desc,
    d.referenced_server_name,
    d.referenced_database_name,
    d.referenced_schema_name,
    d.referenced_entity_name,
    N''sql_expression_dependency'',
    d.is_ambiguous
FROM sys.sql_expression_dependencies AS d
LEFT JOIN sys.objects AS o
    ON d.referencing_id = o.object_id
WHERE d.referenced_server_name IS NOT NULL
   OR d.referenced_database_name IS NOT NULL
UNION ALL
SELECT
    DB_NAME(),
    SCHEMA_NAME(s.schema_id),
    s.name,
    N''SYNONYM'',
    PARSENAME(s.base_object_name, 4),
    PARSENAME(s.base_object_name, 3),
    PARSENAME(s.base_object_name, 2),
    PARSENAME(s.base_object_name, 1),
    N''synonym'',
    0
FROM sys.synonyms AS s;

WITH modules AS
(
    SELECT
        SCHEMA_NAME(o.schema_id) AS schema_name,
        o.name AS object_name,
        o.type_desc AS object_type,
        LOWER(CONVERT(nvarchar(max), m.definition)) AS definition_text,
        CONVERT(varchar(130), sys.fn_varbintohexstr(HASHBYTES(''SHA2_256'', CONVERT(nvarchar(4000), m.definition)))) AS definition_hash,
        LEFT(REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(max), m.definition), CHAR(13), N'' ''), CHAR(10), N'' ''), CHAR(9), N'' ''), 500) AS sanitized_snippet
    FROM sys.sql_modules AS m
    JOIN sys.objects AS o
        ON m.object_id = o.object_id
    WHERE o.is_ms_shipped = 0
)
INSERT #object_feature_scan
SELECT
    DB_NAME(),
    schema_name,
    object_name,
    object_type,
    feature_name,
    definition_hash,
    sanitized_snippet,
    azure_sql_db_impact,
    azure_sql_mi_impact,
    sql_vm_impact
FROM modules
CROSS APPLY (VALUES
    (N''xp_cmdshell'', CASE WHEN definition_text LIKE N''%xp_cmdshell%'' THEN 1 ELSE 0 END, N''Not supported in Azure SQL Database.'', N''Not supported in Azure SQL Managed Instance.'', N''Can preserve only if accepted by VM security policy.''),
    (N''bulk_insert'', CASE WHEN definition_text LIKE N''%bulk insert%'' THEN 1 ELSE 0 END, N''Requires Azure Blob data source redesign.'', N''Requires Azure Blob data source or redesign.'', N''Can preserve local/file-share bulk load if network and OS access allow.''),
    (N''openrowset'', CASE WHEN definition_text LIKE N''%openrowset%'' THEN 1 ELSE 0 END, N''Provider and file access require target validation.'', N''Non-SQL providers and non-Blob file access require redesign.'', N''Can preserve provider access if installed and allowed.''),
    (N''opendatasource'', CASE WHEN definition_text LIKE N''%opendatasource%'' THEN 1 ELSE 0 END, N''Ad hoc external provider access is not a clean Azure SQL DB fit.'', N''Validate provider and target restrictions.'', N''Can preserve provider access if installed and allowed.''),
    (N''distributed_transaction'', CASE WHEN definition_text LIKE N''%begin distributed transaction%'' THEN 1 ELSE 0 END, N''Distributed transactions require redesign for Azure SQL DB.'', N''Validate MI distributed transaction support and participants.'', N''Can preserve MSDTC with SQL VM configuration.''),
    (N''service_broker_statement'', CASE WHEN definition_text LIKE N''%begin dialog%'' OR definition_text LIKE N''%service_broker%'' THEN 1 ELSE 0 END, N''Service Broker is not supported in Azure SQL DB.'', N''Better MI fit when broker behavior must be preserved.'', N''Can preserve Service Broker behavior.''),
    (N''database_mail_statement'', CASE WHEN definition_text LIKE N''%sp_send_dbmail%'' THEN 1 ELSE 0 END, N''Database Mail is not available in Azure SQL DB.'', N''Database Mail requires MI-specific configuration.'', N''Can preserve Database Mail.''),
    (N''execute_as_login'', CASE WHEN definition_text LIKE N''%execute as login%'' THEN 1 ELSE 0 END, N''Server-scoped impersonation requires redesign.'', N''Validate EXECUTE AS limitations and login mapping.'', N''Can preserve server login impersonation.'')
) AS hits(feature_name, detected, azure_sql_db_impact, azure_sql_mi_impact, sql_vm_impact)
WHERE hits.detected = 1;

IF OBJECT_ID(N''sys.database_query_store_options'') IS NOT NULL
BEGIN
    INSERT #query_store_summary
    SELECT
        DB_NAME(),
        actual_state_desc,
        desired_state_desc,
        readonly_reason,
        current_storage_size_mb,
        max_storage_size_mb,
        ISNULL((SELECT COUNT(*) FROM sys.query_store_query), 0),
        ISNULL((SELECT COUNT(*) FROM sys.query_store_plan), 0),
        ISNULL((SELECT COUNT(*) FROM sys.query_store_runtime_stats_interval), 0)
    FROM sys.database_query_store_options;
END
ELSE
BEGIN
    INSERT #query_store_summary
    VALUES (DB_NAME(), N''Unavailable'', N''Unavailable'', 0, 0, 0, 0, 0, 0);
END;

INSERT #database_security_principals
SELECT
    DB_NAME(),
    dp.name,
    dp.type_desc,
    dp.authentication_type_desc,
    SUSER_SNAME(dp.sid),
    dp.create_date,
    dp.modify_date,
    STUFF((
        SELECT N''; '' + USER_NAME(drm.role_principal_id)
        FROM sys.database_role_members AS drm
        WHERE drm.member_principal_id = dp.principal_id
        ORDER BY USER_NAME(drm.role_principal_id)
        FOR XML PATH(N''''), TYPE
    ).value(N''.'', N''nvarchar(max)''), 1, 2, N'''')
FROM sys.database_principals AS dp
WHERE dp.type IN (N''S'', N''U'', N''G'')
  AND dp.name NOT IN (N''dbo'', N''guest'', N''INFORMATION_SCHEMA'', N''sys'');
';

    BEGIN TRY
        EXEC sys.sp_executesql @Sql;
    END TRY
    BEGIN CATCH
        SELECT
            'database_scan_error' AS result_set,
            @DbName AS database_name,
            ERROR_MESSAGE() AS error_message;
    END CATCH;

    FETCH NEXT FROM dbs INTO @DbName;
END;

CLOSE dbs;
DEALLOCATE dbs;

SELECT 'database_features' AS result_set, * FROM #database_features ORDER BY database_name, feature_name;
SELECT 'database_dependencies' AS result_set, * FROM #database_dependencies ORDER BY database_name, referencing_schema_name, referencing_object_name;
SELECT 'object_feature_scan' AS result_set, * FROM #object_feature_scan ORDER BY database_name, schema_name, object_name, feature_name;
SELECT 'query_store_summary' AS result_set, * FROM #query_store_summary ORDER BY database_name;
SELECT 'database_security_principals' AS result_set, * FROM #database_security_principals ORDER BY database_name, principal_type_desc, principal_name;

------------------------------------------------------------
-- 13. TARGET SIGNAL MATRIX
------------------------------------------------------------
SELECT
    'target_signal_matrix' AS result_set,
    database_name,
    signal_scope,
    signal_name,
    detected,
    signal_value,
    azure_sql_db_impact,
    azure_sql_mi_impact,
    sql_vm_impact,
    evidence_source
FROM
(
    SELECT
        CAST(NULL AS sysname) AS database_name,
        N'server' AS signal_scope,
        N'xp_cmdshell_enabled' AS signal_name,
        CASE WHEN EXISTS (SELECT 1 FROM sys.configurations WHERE name = N'xp_cmdshell' AND value_in_use = 1) THEN 1 ELSE 0 END AS detected,
        CONVERT(nvarchar(4000), (SELECT value_in_use FROM sys.configurations WHERE name = N'xp_cmdshell')) AS signal_value,
        N'Not supported; move OS work outside the database.' AS azure_sql_db_impact,
        N'Not supported in Azure SQL Managed Instance.' AS azure_sql_mi_impact,
        N'Can preserve only if accepted by VM security policy.' AS sql_vm_impact,
        N'server_configurations' AS evidence_source

    UNION ALL

    SELECT
        NULL,
        N'server',
        N'linked_servers',
        CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END,
        CONVERT(nvarchar(4000), COUNT(*)),
        N'Linked servers are not available in Azure SQL Database.',
        N'Validate provider support and networking for MI.',
        N'Can preserve linked servers if providers and network paths are available.',
        N'linked_servers'
    FROM sys.servers
    WHERE is_linked = 1

    UNION ALL

    SELECT
        d.name,
        N'database',
        N'compatibility_level_below_100',
        CASE WHEN d.compatibility_level < 100 THEN 1 ELSE 0 END,
        CONVERT(nvarchar(4000), d.compatibility_level),
        N'Upgrade compatibility level and regression test before Azure SQL DB.',
        N'Upgrade compatibility level and regression test before MI.',
        N'Can preserve only on compatible SQL Server versions.',
        N'databases'
    FROM sys.databases AS d
    WHERE d.database_id > 4
      AND d.source_database_id IS NULL

    UNION ALL

    SELECT
        database_name,
        N'database',
        feature_name,
        detected,
        feature_value,
        CASE feature_name
            WHEN N'filestream_filegroups' THEN N'FILESTREAM is not supported in Azure SQL Database.'
            WHEN N'filetables' THEN N'FileTable is not supported in Azure SQL Database.'
            WHEN N'cross_database_references' THEN N'Cross-database references are not a clean Azure SQL DB fit.'
            WHEN N'service_broker' THEN N'Service Broker is not supported in Azure SQL Database.'
            WHEN N'user_assemblies' THEN N'CLR assemblies require redesign or validation.'
            ELSE N'Validate feature compatibility for Azure SQL Database.'
        END,
        CASE feature_name
            WHEN N'filestream_filegroups' THEN N'FILESTREAM is not supported in Azure SQL Managed Instance.'
            WHEN N'filetables' THEN N'FileTable is not supported in Azure SQL Managed Instance.'
            ELSE N'Validate feature compatibility for Managed Instance.'
        END,
        N'Can usually preserve with SQL Server on Azure VM if OS and version dependencies are met.',
        N'database_features'
    FROM #database_features
    WHERE detected = 1

    UNION ALL

    SELECT
        database_name,
        N'object',
        feature_name,
        1,
        QUOTENAME(schema_name) + N'.' + QUOTENAME(object_name),
        azure_sql_db_impact,
        azure_sql_mi_impact,
        sql_vm_impact,
        N'object_feature_scan'
    FROM #object_feature_scan
) AS signals
ORDER BY database_name, signal_scope, signal_name;
