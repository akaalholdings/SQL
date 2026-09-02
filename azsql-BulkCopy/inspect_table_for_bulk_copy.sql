/*
    Azure SQL table inventory for designing a large ADF copy.

    Run this script in both the source and destination databases after setting
    @SchemaName and @TableName. It reads catalogue metadata only, apart from the
    final MIN/MAX query against the preferred range column. It does not count or
    modify table rows.
*/

SET NOCOUNT ON;

/* Required inputs */
DECLARE @SchemaName sysname = N'dbo';
DECLARE @TableName sysname = N'YourLargeTable';

DECLARE @QualifiedTableName nvarchar(517) =
    QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName);
DECLARE @ObjectId int = OBJECT_ID(@QualifiedTableName, N'U');

IF @ObjectId IS NULL
BEGIN
    DECLARE @ErrorMessage nvarchar(2048) =
        N'Table ' + @QualifiedTableName + N' was not found in database '
        + QUOTENAME(DB_NAME()) + N'.';

    THROW 50000, @ErrorMessage, 1;
END;

/* 1. Current Azure SQL database tier and database settings */
SELECT
    @@SERVERNAME AS logical_server_name,
    DB_NAME() AS database_name,
    CAST(SERVERPROPERTY(N'Edition') AS nvarchar(128)) AS engine_edition,
    dso.edition AS service_tier,
    dso.service_objective,
    dso.elastic_pool_name,
    d.compatibility_level,
    d.collation_name,
    d.containment_desc,
    d.is_read_committed_snapshot_on,
    d.snapshot_isolation_state_desc,
    d.is_auto_create_stats_on,
    d.is_auto_update_stats_on,
    d.is_auto_update_stats_async_on
FROM sys.databases AS d
LEFT JOIN sys.database_service_objectives AS dso
    ON dso.database_id = d.database_id
WHERE d.database_id = DB_ID();

/* 2. Table properties that can affect copy and recreation */
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.object_id,
    t.create_date,
    t.modify_date,
    t.temporal_type_desc,
    history_schema.name AS history_schema_name,
    history_table.name AS history_table_name,
    t.is_memory_optimized,
    t.durability_desc,
    t.lock_escalation_desc,
    t.is_tracked_by_cdc,
    t.is_replicated,
    t.is_node,
    t.is_edge,
    t.ledger_type_desc
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
LEFT JOIN sys.tables AS history_table
    ON history_table.object_id = t.history_table_id
LEFT JOIN sys.schemas AS history_schema
    ON history_schema.schema_id = history_table.schema_id
WHERE t.object_id = @ObjectId;

/* 3. Row and allocation estimates. DMV access can require extra permission. */
BEGIN TRY
    SELECT
        SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END)
            AS estimated_row_count,
        CAST(
            SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.reserved_page_count ELSE 0 END)
            * 8.0 / 1024.0
            AS decimal(19, 2)
        ) AS base_table_reserved_mb,
        CAST(
            SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.used_page_count ELSE 0 END)
            * 8.0 / 1024.0
            AS decimal(19, 2)
        ) AS base_table_used_mb,
        CAST(
            SUM(ps.reserved_page_count) * 8.0 / 1024.0
            AS decimal(19, 2)
        ) AS table_and_indexes_reserved_mb,
        CAST(
            SUM(ps.used_page_count) * 8.0 / 1024.0
            AS decimal(19, 2)
        ) AS table_and_indexes_used_mb,
        CAST(
            SUM(ps.lob_used_page_count) * 8.0 / 1024.0
            AS decimal(19, 2)
        ) AS lob_used_mb,
        CAST(
            SUM(ps.row_overflow_used_page_count) * 8.0 / 1024.0
            AS decimal(19, 2)
        ) AS row_overflow_used_mb
    FROM sys.dm_db_partition_stats AS ps
    WHERE ps.object_id = @ObjectId;
END TRY
BEGIN CATCH
    SELECT
        ERROR_NUMBER() AS metadata_error_number,
        ERROR_MESSAGE() AS metadata_error_message,
        N'Grant the documented database-state permission if allocation metadata is required.'
            AS metadata_guidance;
END CATCH;

/* 4. Columns, including identity, computed, default, masking and encryption metadata */
SELECT
    c.column_id,
    c.name AS column_name,
    QUOTENAME(SCHEMA_NAME(ut.schema_id))
        + N'.' + QUOTENAME(ut.name) AS declared_data_type,
    st.name AS base_data_type,
    c.max_length,
    c.[precision] AS numeric_precision,
    c.[scale] AS numeric_scale,
    c.collation_name,
    c.is_nullable,
    c.is_identity,
    identity_column.seed_value AS identity_seed,
    identity_column.increment_value AS identity_increment,
    c.is_computed,
    computed_column.[definition] AS computed_definition,
    computed_column.is_persisted,
    c.is_sparse,
    c.is_column_set,
    c.is_rowguidcol,
    c.is_hidden,
    c.generated_always_type_desc,
    c.encryption_type_desc,
    column_encryption_key.name AS column_encryption_key_name,
    CASE WHEN masked_column.column_id IS NULL THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END
        AS is_masked,
    masked_column.masking_function,
    default_constraint.name AS default_constraint_name,
    default_constraint.[definition] AS default_definition
FROM sys.columns AS c
INNER JOIN sys.types AS ut
    ON ut.user_type_id = c.user_type_id
INNER JOIN sys.types AS st
    ON st.system_type_id = c.system_type_id
   AND st.user_type_id = st.system_type_id
LEFT JOIN sys.identity_columns AS identity_column
    ON identity_column.object_id = c.object_id
   AND identity_column.column_id = c.column_id
LEFT JOIN sys.computed_columns AS computed_column
    ON computed_column.object_id = c.object_id
   AND computed_column.column_id = c.column_id
LEFT JOIN sys.default_constraints AS default_constraint
    ON default_constraint.parent_object_id = c.object_id
   AND default_constraint.parent_column_id = c.column_id
LEFT JOIN sys.masked_columns AS masked_column
    ON masked_column.object_id = c.object_id
   AND masked_column.column_id = c.column_id
LEFT JOIN sys.column_encryption_keys AS column_encryption_key
    ON column_encryption_key.column_encryption_key_id = c.column_encryption_key_id
WHERE c.object_id = @ObjectId
ORDER BY c.column_id;

/* 5. Index definitions and ordered key/include column lists */
SELECT
    i.index_id,
    i.name AS index_name,
    i.type_desc AS index_type,
    data_space.name AS data_space_name,
    key_list.key_columns,
    include_list.included_columns,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.is_disabled,
    i.is_hypothetical,
    i.auto_created,
    i.fill_factor,
    i.allow_row_locks,
    i.allow_page_locks,
    i.optimize_for_sequential_key,
    i.has_filter,
    i.filter_definition
FROM sys.indexes AS i
LEFT JOIN sys.data_spaces AS data_space
    ON data_space.data_space_id = i.data_space_id
OUTER APPLY
(
    SELECT
        STRING_AGG(
            CONVERT(
                nvarchar(max),
                QUOTENAME(c.name)
                + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
            ),
            N', '
        ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id
      AND ic.index_id = i.index_id
      AND ic.key_ordinal > 0
) AS key_list
OUTER APPLY
(
    SELECT
        STRING_AGG(
            CONVERT(nvarchar(max), QUOTENAME(c.name)),
            N', '
        ) WITHIN GROUP (ORDER BY ic.index_column_id) AS included_columns
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id
      AND ic.index_id = i.index_id
      AND ic.is_included_column = 1
) AS include_list
WHERE i.object_id = @ObjectId
ORDER BY i.index_id;

/* 6. Physical partitions, boundaries and compression by index */
SELECT
    i.index_id,
    i.name AS index_name,
    i.type_desc AS index_type,
    p.partition_number,
    p.rows AS estimated_rows,
    p.data_compression_desc,
    data_space.name AS data_space_name,
    partition_scheme.name AS partition_scheme_name,
    partition_function.name AS partition_function_name,
    partition_function.boundary_value_on_right,
    CONVERT(nvarchar(4000), lower_boundary.value) AS lower_boundary_value,
    CONVERT(nvarchar(4000), upper_boundary.value) AS upper_boundary_value
FROM sys.partitions AS p
INNER JOIN sys.indexes AS i
    ON i.object_id = p.object_id
   AND i.index_id = p.index_id
LEFT JOIN sys.data_spaces AS data_space
    ON data_space.data_space_id = i.data_space_id
LEFT JOIN sys.partition_schemes AS partition_scheme
    ON partition_scheme.data_space_id = i.data_space_id
LEFT JOIN sys.partition_functions AS partition_function
    ON partition_function.function_id = partition_scheme.function_id
LEFT JOIN sys.partition_range_values AS lower_boundary
    ON lower_boundary.function_id = partition_function.function_id
   AND lower_boundary.boundary_id = p.partition_number - 1
LEFT JOIN sys.partition_range_values AS upper_boundary
    ON upper_boundary.function_id = partition_function.function_id
   AND upper_boundary.boundary_id = p.partition_number
WHERE p.object_id = @ObjectId
ORDER BY i.index_id, p.partition_number;

/* 7. Candidate columns for ADF dynamic-range partitioning */
SELECT
    c.column_id,
    c.name AS column_name,
    st.name AS base_data_type,
    c.is_nullable,
    c.is_identity,
    identity_column.seed_value AS identity_seed,
    identity_column.increment_value AS identity_increment,
    COALESCE(index_summary.is_primary_key_lead, 0) AS is_primary_key_lead,
    COALESCE(index_summary.is_clustered_lead, 0) AS is_clustered_index_lead,
    COALESCE(index_summary.is_unique_lead, 0) AS is_unique_index_lead,
    index_summary.leading_index_names,
    CASE
        WHEN c.is_nullable = 1 THEN 90
        WHEN index_summary.is_primary_key_lead = 1 THEN 1
        WHEN index_summary.is_clustered_lead = 1 THEN 2
        WHEN index_summary.is_unique_lead = 1 THEN 3
        WHEN index_summary.leading_index_names IS NOT NULL THEN 4
        WHEN c.is_identity = 1 THEN 5
        ELSE 10
    END AS candidate_rank,
    CASE
        WHEN c.is_nullable = 1
            THEN N'Nullable: account separately for NULL rows.'
        WHEN index_summary.leading_index_names IS NULL
            THEN N'Eligible type but not the leading key of an enabled, unfiltered index.'
        ELSE N'Indexed range candidate.'
    END AS candidate_note
FROM sys.columns AS c
INNER JOIN sys.types AS st
    ON st.system_type_id = c.system_type_id
   AND st.user_type_id = st.system_type_id
LEFT JOIN sys.identity_columns AS identity_column
    ON identity_column.object_id = c.object_id
   AND identity_column.column_id = c.column_id
OUTER APPLY
(
    SELECT
        MAX(CASE WHEN i.is_primary_key = 1 THEN 1 ELSE 0 END)
            AS is_primary_key_lead,
        MAX(CASE WHEN i.type = 1 THEN 1 ELSE 0 END)
            AS is_clustered_lead,
        MAX(CASE WHEN i.is_unique = 1 THEN 1 ELSE 0 END)
            AS is_unique_lead,
        STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(i.name)), N', ')
            WITHIN GROUP (ORDER BY i.index_id) AS leading_index_names
    FROM sys.index_columns AS ic
    INNER JOIN sys.indexes AS i
        ON i.object_id = ic.object_id
       AND i.index_id = ic.index_id
    WHERE ic.object_id = c.object_id
      AND ic.column_id = c.column_id
      AND ic.key_ordinal = 1
      AND i.is_disabled = 0
      AND i.is_hypothetical = 0
      AND i.has_filter = 0
      AND i.type IN (1, 2)
) AS index_summary
WHERE c.object_id = @ObjectId
  AND c.is_computed = 0
  AND c.is_hidden = 0
  AND c.encryption_type IS NULL
  AND st.name IN
      (N'smallint', N'int', N'bigint', N'date', N'smalldatetime',
       N'datetime', N'datetime2', N'datetimeoffset')
ORDER BY
    CASE
        WHEN c.is_nullable = 1 THEN 90
        WHEN index_summary.is_primary_key_lead = 1 THEN 1
        WHEN index_summary.is_clustered_lead = 1 THEN 2
        WHEN index_summary.is_unique_lead = 1 THEN 3
        WHEN index_summary.leading_index_names IS NOT NULL THEN 4
        WHEN c.is_identity = 1 THEN 5
        ELSE 10
    END,
    c.column_id;

/*
    8. Bounds for the best non-null candidate.
       This is the only result set that reads table data. Separate scalar
       aggregates let MIN and MAX seek opposite ends of the selected leading
       index and avoid a full COUNT_BIG(*) scan.
*/
DECLARE @RangeColumn sysname;
DECLARE @RangeDataType sysname;

SELECT TOP (1)
    @RangeColumn = c.name,
    @RangeDataType = st.name
FROM sys.columns AS c
INNER JOIN sys.types AS st
    ON st.system_type_id = c.system_type_id
   AND st.user_type_id = st.system_type_id
INNER JOIN sys.index_columns AS ic
    ON ic.object_id = c.object_id
   AND ic.column_id = c.column_id
   AND ic.key_ordinal = 1
INNER JOIN sys.indexes AS i
    ON i.object_id = ic.object_id
   AND i.index_id = ic.index_id
WHERE c.object_id = @ObjectId
  AND c.is_nullable = 0
  AND c.is_computed = 0
  AND c.is_hidden = 0
  AND c.encryption_type IS NULL
  AND st.name IN
      (N'smallint', N'int', N'bigint', N'date', N'smalldatetime',
       N'datetime', N'datetime2', N'datetimeoffset')
  AND i.is_disabled = 0
  AND i.is_hypothetical = 0
  AND i.has_filter = 0
  AND i.type IN (1, 2)
ORDER BY
    CASE
        WHEN i.is_primary_key = 1 THEN 1
        WHEN i.type = 1 THEN 2
        WHEN i.is_unique = 1 THEN 3
        ELSE 4
    END,
    i.index_id,
    c.column_id;

IF @RangeColumn IS NULL
BEGIN
    SELECT
        @SchemaName AS schema_name,
        @TableName AS table_name,
        CAST(NULL AS sysname) AS selected_range_column,
        CAST(NULL AS sysname) AS range_data_type,
        CAST(NULL AS nvarchar(4000)) AS minimum_value,
        CAST(NULL AS nvarchar(4000)) AS maximum_value,
        N'No non-null, unencrypted leading index column with a supported integer/date type was found.'
            AS bounds_status;
END;
ELSE
BEGIN
    DECLARE @BoundsSql nvarchar(max) =
        N'SELECT'
        + N' @OutputSchemaName AS schema_name,'
        + N' @OutputTableName AS table_name,'
        + N' @OutputRangeColumn AS selected_range_column,'
        + N' @OutputRangeDataType AS range_data_type,'
        + N' CONVERT(nvarchar(4000), (SELECT MIN('
        + QUOTENAME(@RangeColumn) + N') FROM ' + @QualifiedTableName
        + N')) AS minimum_value,'
        + N' CONVERT(nvarchar(4000), (SELECT MAX('
        + QUOTENAME(@RangeColumn) + N') FROM ' + @QualifiedTableName
        + N')) AS maximum_value,'
        + N' N''Bounds read successfully.'' AS bounds_status;';

    EXEC sys.sp_executesql
        @BoundsSql,
        N'@OutputSchemaName sysname,
          @OutputTableName sysname,
          @OutputRangeColumn sysname,
          @OutputRangeDataType sysname',
        @OutputSchemaName = @SchemaName,
        @OutputTableName = @TableName,
        @OutputRangeColumn = @RangeColumn,
        @OutputRangeDataType = @RangeDataType;
END;

/* 9a. Primary-key and unique constraints */
SELECT
    key_constraint.name AS constraint_name,
    key_constraint.type_desc AS constraint_type,
    i.name AS backing_index_name,
    i.type_desc AS backing_index_type,
    key_columns.key_column_list
FROM sys.key_constraints AS key_constraint
INNER JOIN sys.indexes AS i
    ON i.object_id = key_constraint.parent_object_id
   AND i.index_id = key_constraint.unique_index_id
OUTER APPLY
(
    SELECT
        STRING_AGG(
            CONVERT(
                nvarchar(max),
                QUOTENAME(c.name)
                + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
            ),
            N', '
        ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_column_list
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id
      AND ic.index_id = i.index_id
      AND ic.key_ordinal > 0
) AS key_columns
WHERE key_constraint.parent_object_id = @ObjectId
ORDER BY key_constraint.type_desc, key_constraint.name;

/* 9b. Check constraints. Default constraints are included with the column inventory. */
SELECT
    check_constraint.name AS constraint_name,
    c.name AS column_name,
    check_constraint.[definition],
    check_constraint.is_disabled,
    check_constraint.is_not_trusted,
    check_constraint.is_not_for_replication,
    check_constraint.uses_database_collation
FROM sys.check_constraints AS check_constraint
LEFT JOIN sys.columns AS c
    ON c.object_id = check_constraint.parent_object_id
   AND c.column_id = check_constraint.parent_column_id
WHERE check_constraint.parent_object_id = @ObjectId
ORDER BY check_constraint.name;

/* 10. Outbound and inbound foreign keys, including ordered column mappings */
SELECT
    CASE
        WHEN fk.parent_object_id = @ObjectId
         AND fk.referenced_object_id = @ObjectId THEN N'SELF'
        WHEN fk.parent_object_id = @ObjectId THEN N'OUTBOUND'
        ELSE N'INBOUND'
    END AS relationship_direction,
    fk.name AS foreign_key_name,
    parent_schema.name AS parent_schema_name,
    parent_table.name AS parent_table_name,
    referenced_schema.name AS referenced_schema_name,
    referenced_table.name AS referenced_table_name,
    column_mapping.column_mapping,
    fk.delete_referential_action_desc,
    fk.update_referential_action_desc,
    fk.is_disabled,
    fk.is_not_trusted,
    fk.is_not_for_replication
FROM sys.foreign_keys AS fk
INNER JOIN sys.tables AS parent_table
    ON parent_table.object_id = fk.parent_object_id
INNER JOIN sys.schemas AS parent_schema
    ON parent_schema.schema_id = parent_table.schema_id
INNER JOIN sys.tables AS referenced_table
    ON referenced_table.object_id = fk.referenced_object_id
INNER JOIN sys.schemas AS referenced_schema
    ON referenced_schema.schema_id = referenced_table.schema_id
OUTER APPLY
(
    SELECT
        STRING_AGG(
            CONVERT(
                nvarchar(max),
                QUOTENAME(parent_column.name)
                + N' -> ' + QUOTENAME(referenced_column.name)
            ),
            N', '
        ) WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS column_mapping
    FROM sys.foreign_key_columns AS fkc
    INNER JOIN sys.columns AS parent_column
        ON parent_column.object_id = fkc.parent_object_id
       AND parent_column.column_id = fkc.parent_column_id
    INNER JOIN sys.columns AS referenced_column
        ON referenced_column.object_id = fkc.referenced_object_id
       AND referenced_column.column_id = fkc.referenced_column_id
    WHERE fkc.constraint_object_id = fk.object_id
) AS column_mapping
WHERE fk.parent_object_id = @ObjectId
   OR fk.referenced_object_id = @ObjectId
ORDER BY relationship_direction, fk.name;

/* 11. DML triggers and their definitions */
SELECT
    trigger_object.name AS trigger_name,
    trigger_events.event_list,
    trigger_object.is_disabled,
    trigger_object.is_instead_of_trigger,
    trigger_object.is_not_for_replication,
    CONVERT(bit, OBJECTPROPERTYEX(trigger_object.object_id, N'ExecIsInsertTrigger'))
        AS fires_for_insert,
    CONVERT(bit, OBJECTPROPERTYEX(trigger_object.object_id, N'ExecIsUpdateTrigger'))
        AS fires_for_update,
    CONVERT(bit, OBJECTPROPERTYEX(trigger_object.object_id, N'ExecIsDeleteTrigger'))
        AS fires_for_delete,
    trigger_object.create_date,
    trigger_object.modify_date,
    sql_module.uses_ansi_nulls,
    sql_module.uses_quoted_identifier,
    sql_module.[definition]
FROM sys.triggers AS trigger_object
LEFT JOIN sys.sql_modules AS sql_module
    ON sql_module.object_id = trigger_object.object_id
OUTER APPLY
(
    SELECT
        STRING_AGG(CONVERT(nvarchar(max), trigger_event.type_desc), N', ')
            WITHIN GROUP (ORDER BY trigger_event.type) AS event_list
    FROM sys.trigger_events AS trigger_event
    WHERE trigger_event.object_id = trigger_object.object_id
) AS trigger_events
WHERE trigger_object.parent_id = @ObjectId
ORDER BY trigger_object.name;
