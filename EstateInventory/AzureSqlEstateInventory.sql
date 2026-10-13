/* ---------------------------------------------------------------------------------------------
   Estate inventory: the structure of ONE database, as one flat result set.

   Read-only: SET statements and one SELECT on catalog views. No row of any table is read.
   What leaves the database: names (database, schemas, tables, columns, indexes, constraints,
   modules); the text of defaults, checks, computed columns, index and statistics filters,
   masks and security predicates; the first and last boundary value of each partition function;
   synonym targets; row counts; dates; edition and service objective. That text can hold
   literal values (a default, a list of codes, a tenant id in a filter): read the result before
   you send it. Views, procedures, functions and triggers leave as a SHA-256 hash, never as text.

   HOW TO RUN IT, once in EACH database (dev, sandbox, test, preprod, prod)
   1. Connect to the database itself, not to master (USE does not work in Azure SQL Database),
      with a login that has VIEW DEFINITION on the database (db_owner has it; db_datareader is
      not enough: the row with section = VISIBILITY then shows view_definition=0).
   2. SSMS: Query > Results To > Results to Grid, then Execute.
   3. Right-click the grid > Save Results As > file type "Text (Tab delimited) (*.txt)"; name
      the file after the database. Not CSV, not Results to Text, not Results to File, and do
      not open the file in Excel: each of them cuts or breaks values.
      VS Code (mssql extension) or Azure Data Studio: the "Save as CSV" button of the grid.
   4. Error 1222 (lock request time out period exceeded) = a schema change was running in that
      database. Nothing was returned. Run it again.

   HOW TO READ IT
   One row = one fact. Two databases are aligned on a fact when both have a row with the same
   (section, schema_name, object_name, item_name) and the same `definition`.
   `info` holds things that can differ without being a difference of structure: row counts,
   dates, the real name of a constraint that the engine named.
   A constraint that the engine named has an item_name that starts with <system-named ...>,
   made from what the constraint is and not from its name; its real name is in `info`.
   `ordinal` of a COLUMN row is the position of the column in its table (1, 2, 3 ...).
   The row with section = VISIBILITY must say view_definition=1; if not, rows are missing.

   Every text value is given the collation of the database on purpose: catalog names, the
   *_desc columns and definitions have three different collations, and mixing them is an error.
   --------------------------------------------------------------------------------------------- */
SET NOCOUNT ON;
SET LOCK_TIMEOUT 30000;

WITH col_type AS (
    SELECT c.object_id, c.column_id,
           type_text = CONVERT(nvarchar(300),
               CASE
                   WHEN t.is_user_defined = 1
                       THEN QUOTENAME(SCHEMA_NAME(t.schema_id)) COLLATE DATABASE_DEFAULT + N'.' + QUOTENAME(t.name) COLLATE DATABASE_DEFAULT
                   WHEN t.name IN (N'nvarchar', N'nchar')
                       THEN t.name COLLATE DATABASE_DEFAULT + N'(' + CASE WHEN c.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(10), c.max_length / 2) END + N')'
                   WHEN t.name IN (N'varchar', N'char', N'varbinary', N'binary')
                       THEN t.name COLLATE DATABASE_DEFAULT + N'(' + CASE WHEN c.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(10), c.max_length) END + N')'
                   WHEN t.name IN (N'decimal', N'numeric')
                       THEN t.name COLLATE DATABASE_DEFAULT + N'(' + CONVERT(nvarchar(10), c.precision) + N',' + CONVERT(nvarchar(10), c.scale) + N')'
                   WHEN t.name IN (N'datetime2', N'datetimeoffset', N'time')
                       THEN t.name COLLATE DATABASE_DEFAULT + N'(' + CONVERT(nvarchar(10), c.scale) + N')'
                   WHEN t.name = N'vector'
                       THEN t.name COLLATE DATABASE_DEFAULT + N'(max_length=' + CONVERT(nvarchar(10), c.max_length) + N')'
                   ELSE t.name COLLATE DATABASE_DEFAULT
               END)
    FROM sys.columns AS c
    JOIN sys.types AS t ON t.user_type_id = c.user_type_id
),
obj AS (  -- user tables and views, outside the schema of the deployment tool
    SELECT o.object_id,
           schema_name = s.name COLLATE DATABASE_DEFAULT,
           -- a history table that the engine named holds the object id of its table, which differs in each database
           object_name = CASE WHEN cur.object_id IS NOT NULL AND o.name LIKE N'MSSQL[_]TemporalHistoryFor[_]%'
                              THEN CONVERT(nvarchar(300), N'<history of ' + QUOTENAME(SCHEMA_NAME(cur.schema_id)) COLLATE DATABASE_DEFAULT
                                                           + N'.' + QUOTENAME(cur.name) COLLATE DATABASE_DEFAULT + N'>')
                              ELSE CONVERT(nvarchar(300), o.name COLLATE DATABASE_DEFAULT) END,
           o.type
    FROM sys.objects AS o
    JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    LEFT JOIN sys.tables AS cur ON cur.history_table_id = o.object_id
    WHERE o.is_ms_shipped = 0 AND o.type IN ('U', 'V') AND s.name <> N'azsqlcd'
      AND NOT EXISTS (SELECT 1 FROM sys.extended_properties AS ep  -- the objects of database diagrams
                      WHERE ep.class = 1 AND ep.major_id = o.object_id AND ep.minor_id = 0
                        AND ep.name = N'microsoft_database_tools_support')
),
part AS (  -- one row per index (0 = heap): rows, partitions, compression
    SELECT x.object_id, x.index_id,
           row_count = SUM(x.rows),
           partition_count = COUNT(*),
           compression = CASE WHEN MIN(x.compression_desc) = MAX(x.compression_desc)
                              THEN MIN(x.compression_desc)
                              ELSE N'MIXED(' + MIN(x.compression_desc) + N'..' + MAX(x.compression_desc) + N')' END
    FROM (SELECT p.object_id, p.index_id, p.rows,
                 compression_desc = CONVERT(nvarchar(60), p.data_compression_desc) COLLATE DATABASE_DEFAULT
          FROM sys.partitions AS p) AS x
    GROUP BY x.object_id, x.index_id
),
idx_cols AS (  -- key columns, in key order
    SELECT ic.object_id, ic.index_id,
           key_cols = STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                                                        + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END), N', ')
                      WITHIN GROUP (ORDER BY ic.key_ordinal)
    FROM sys.index_columns AS ic
    JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.key_ordinal > 0
    GROUP BY ic.object_id, ic.index_id
),
idx_part AS (  -- partitioning columns of an index or of a heap (index 0)
    SELECT ic.object_id, ic.index_id,
           part_cols = STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(c.name) COLLATE DATABASE_DEFAULT), N', ')
                       WITHIN GROUP (ORDER BY ic.partition_ordinal)
    FROM sys.index_columns AS ic
    JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.partition_ordinal > 0
    GROUP BY ic.object_id, ic.index_id
),
idx_inc AS (  -- included columns, and the column list of a nonclustered columnstore index, in name order
    SELECT ic.object_id, ic.index_id,
           inc_cols = STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(c.name) COLLATE DATABASE_DEFAULT), N', ')
                      WITHIN GROUP (ORDER BY c.name COLLATE Latin1_General_100_BIN2)
    FROM sys.index_columns AS ic
    JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.key_ordinal = 0 AND ic.is_included_column = 1
    GROUP BY ic.object_id, ic.index_id
),
fk_cols AS (
    SELECT fkc.constraint_object_id,
           cols = STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(pc.name) COLLATE DATABASE_DEFAULT), N', ')
                  WITHIN GROUP (ORDER BY fkc.constraint_column_id),
           ref_cols = STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(rc.name) COLLATE DATABASE_DEFAULT), N', ')
                      WITHIN GROUP (ORDER BY fkc.constraint_column_id)
    FROM sys.foreign_key_columns AS fkc
    JOIN sys.columns AS pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
    JOIN sys.columns AS rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
    GROUP BY fkc.constraint_object_id
),
module_hash AS (  -- a hash of the text with spaces, tabs, line ends and letter case removed; and of the exact text
    SELECT m.object_id,
           m.is_schema_bound, m.uses_ansi_nulls, m.uses_quoted_identifier,
           loose_sha256 = CONVERT(varchar(64), HASHBYTES('SHA2_256',
               UPPER(REPLACE(REPLACE(REPLACE(REPLACE(m.definition COLLATE Latin1_General_100_BIN2,
                     NCHAR(13), N''), NCHAR(10), N''), NCHAR(9), N''), N' ', N''))), 2),
           exact_sha256 = CONVERT(varchar(64), HASHBYTES('SHA2_256', m.definition), 2),
           chars = LEN(m.definition)
    FROM sys.sql_modules AS m
),
inventory AS (

    /* ---- the database --------------------------------------------------------------------- */
    SELECT section     = CONVERT(varchar(30), 'DATABASE') COLLATE DATABASE_DEFAULT,
           schema_name = CONVERT(nvarchar(128), N'') COLLATE DATABASE_DEFAULT,
           object_name = CONVERT(nvarchar(128), N'') COLLATE DATABASE_DEFAULT,
           item_name   = CONVERT(nvarchar(300), N'') COLLATE DATABASE_DEFAULT,
           ordinal     = CONVERT(int, 0),
           definition  = CONVERT(nvarchar(max), CONCAT(
               N'compatibility_level=', d.compatibility_level,
               N'; collation=', d.collation_name COLLATE DATABASE_DEFAULT,
               N'; catalog_collation=', CONVERT(nvarchar(60), d.catalog_collation_type_desc) COLLATE DATABASE_DEFAULT,
               N'; read_committed_snapshot=', d.is_read_committed_snapshot_on,
               N'; cdc=', d.is_cdc_enabled,
               N'; change_tracking=', CASE WHEN EXISTS (SELECT 1 FROM sys.change_tracking_databases AS ct WHERE ct.database_id IN (d.database_id, DB_ID())) THEN 1 ELSE 0 END)) COLLATE DATABASE_DEFAULT,
           info        = CONVERT(nvarchar(max), CONCAT(
               N'edition=', CONVERT(nvarchar(60), DATABASEPROPERTYEX(DB_NAME(), 'Edition')) COLLATE DATABASE_DEFAULT,
               N'; service_objective=', CONVERT(nvarchar(60), DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective')) COLLATE DATABASE_DEFAULT,
               N'; read_at_utc=', CONVERT(nvarchar(30), SYSUTCDATETIME(), 126))) COLLATE DATABASE_DEFAULT
    FROM sys.databases AS d
    WHERE d.name = DB_NAME()

    UNION ALL
    /* ---- schemas --------------------------------------------------------------------------- */
    SELECT 'SCHEMA', s.name COLLATE DATABASE_DEFAULT, N'', N'', 0, N'', N''
    FROM sys.schemas AS s
    WHERE s.schema_id < 16384 AND s.name NOT IN (N'sys', N'INFORMATION_SCHEMA', N'guest', N'azsqlcd')

    UNION ALL
    /* ---- tables ---------------------------------------------------------------------------- */
    SELECT 'TABLE', o.schema_name, o.object_name, N'', 0,
           CONCAT(
               N'storage=', ISNULL(CONVERT(nvarchar(60), i.type_desc) COLLATE DATABASE_DEFAULT, N'NONE'),
               N'; temporal=', CONVERT(nvarchar(60), t.temporal_type_desc) COLLATE DATABASE_DEFAULT,
               CASE WHEN t.temporal_type = 2
                    THEN N' history=' + QUOTENAME(OBJECT_SCHEMA_NAME(t.history_table_id)) COLLATE DATABASE_DEFAULT
                         + N'.' + CASE WHEN OBJECT_NAME(t.history_table_id) LIKE N'MSSQL[_]TemporalHistoryFor[_]%'
                                       THEN N'<engine-named>'
                                       ELSE QUOTENAME(OBJECT_NAME(t.history_table_id)) COLLATE DATABASE_DEFAULT END
                         + N' retention=' + ISNULL(CASE WHEN t.history_retention_period = -1 THEN N'INFINITE'
                                                        ELSE CONVERT(nvarchar(12), t.history_retention_period) + N' '
                                                             + CONVERT(nvarchar(60), t.history_retention_period_unit_desc) COLLATE DATABASE_DEFAULT END, N'?')
                    ELSE N'' END,
               N'; memory_optimized=', t.is_memory_optimized,
               CASE WHEN t.is_memory_optimized = 1 THEN N'(' + CONVERT(nvarchar(60), t.durability_desc) COLLATE DATABASE_DEFAULT + N')' ELSE N'' END,
               N'; external=', t.is_external,
               N'; graph=', CASE WHEN t.is_node = 1 THEN N'NODE' WHEN t.is_edge = 1 THEN N'EDGE' ELSE N'NO' END,
               N'; partition_scheme=', ISNULL(CONVERT(nvarchar(max), ps.name COLLATE DATABASE_DEFAULT) + N'(' + ISNULL(ip.part_cols, N'') + N')', N'NONE'),
               N'; compression=', ISNULL(p.compression, N'NONE')),
           CONCAT(
               N'rows=', ISNULL(p.row_count, 0), N'; partitions=', ISNULL(p.partition_count, 0),
               N'; created=', CONVERT(nvarchar(30), t.create_date, 126), N'; modified=', CONVERT(nvarchar(30), t.modify_date, 126),
               N'; cdc=', t.is_tracked_by_cdc,
               N'; change_tracking=', CASE WHEN EXISTS (SELECT 1 FROM sys.change_tracking_tables AS ctt WHERE ctt.object_id = t.object_id) THEN 1 ELSE 0 END,
               N'; lock_escalation=', CONVERT(nvarchar(60), t.lock_escalation_desc) COLLATE DATABASE_DEFAULT)
    FROM obj AS o
    JOIN sys.tables AS t ON t.object_id = o.object_id
    LEFT JOIN sys.indexes AS i ON i.object_id = t.object_id
         AND i.index_id = (SELECT MAX(i2.index_id) FROM sys.indexes AS i2
                           WHERE i2.object_id = t.object_id AND i2.index_id IN (0, 1) AND i2.is_hypothetical = 0)
    LEFT JOIN sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
    LEFT JOIN idx_part AS ip ON ip.object_id = i.object_id AND ip.index_id = i.index_id
    LEFT JOIN part AS p ON p.object_id = i.object_id AND p.index_id = i.index_id

    UNION ALL
    /* ---- columns of tables ----------------------------------------------------------------- */
    SELECT 'COLUMN', o.schema_name, o.object_name, c.name COLLATE DATABASE_DEFAULT,
           CONVERT(int, ROW_NUMBER() OVER (PARTITION BY c.object_id ORDER BY c.column_id)),
           CONCAT(
               CASE WHEN cc.object_id IS NOT NULL
                    THEN N'AS ' + ISNULL(CONVERT(nvarchar(max), cc.definition) COLLATE DATABASE_DEFAULT, N'NOT READABLE')
                         + CASE WHEN cc.is_persisted = 1 THEN N' PERSISTED' ELSE N'' END
                    ELSE CONVERT(nvarchar(max), ct.type_text) END,
               CASE WHEN idc.object_id IS NOT NULL
                    THEN N' IDENTITY(' + CONVERT(nvarchar(40), idc.seed_value) + N',' + CONVERT(nvarchar(40), idc.increment_value) + N')'
                    ELSE N'' END,
               CASE WHEN c.collation_name IS NOT NULL
                         AND c.collation_name COLLATE DATABASE_DEFAULT
                             <> CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation')) COLLATE DATABASE_DEFAULT
                    THEN N' COLLATE ' + c.collation_name COLLATE DATABASE_DEFAULT ELSE N'' END,
               CASE WHEN c.is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END,
               CASE WHEN c.is_ansi_padded = 0 AND c.is_computed = 0 AND c.max_length <> -1
                         AND c.system_type_id IN (165, 167, 173, 175) THEN N' ANSI_PADDING_OFF' ELSE N'' END,
               CASE WHEN c.is_rowguidcol = 1 THEN N' ROWGUIDCOL' ELSE N'' END,
               CASE WHEN c.is_sparse = 1 THEN N' SPARSE' ELSE N'' END,
               CASE WHEN c.is_masked = 1 THEN N' MASKED(' + ISNULL(CONVERT(nvarchar(4000), mc.masking_function) COLLATE DATABASE_DEFAULT, N'?') + N')' ELSE N'' END,
               CASE WHEN c.generated_always_type <> 0 THEN N' GENERATED ALWAYS ' + CONVERT(nvarchar(60), c.generated_always_type_desc) COLLATE DATABASE_DEFAULT ELSE N'' END,
               CASE WHEN c.is_hidden = 1 THEN N' HIDDEN' ELSE N'' END,
               CASE WHEN c.encryption_type IS NOT NULL THEN N' ENCRYPTED(' + CONVERT(nvarchar(60), c.encryption_type_desc) COLLATE DATABASE_DEFAULT + N')' ELSE N'' END),
           CONCAT(N'column_id=', c.column_id, N'; collation=' + c.collation_name COLLATE DATABASE_DEFAULT)
    FROM obj AS o
    JOIN sys.columns AS c ON c.object_id = o.object_id
    JOIN col_type AS ct ON ct.object_id = c.object_id AND ct.column_id = c.column_id
    LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
    LEFT JOIN sys.identity_columns AS idc ON idc.object_id = c.object_id AND idc.column_id = c.column_id
    LEFT JOIN sys.masked_columns AS mc ON mc.object_id = c.object_id AND mc.column_id = c.column_id
    WHERE o.type = 'U'

    UNION ALL
    /* ---- defaults: keyed by the column, because the engine names many of them -------------- */
    SELECT 'DEFAULT', o.schema_name, o.object_name, c.name COLLATE DATABASE_DEFAULT, c.column_id,
           ISNULL(CONVERT(nvarchar(max), dc.definition) COLLATE DATABASE_DEFAULT, N'NOT READABLE')
               + CASE WHEN dc.is_system_named = 0 THEN N'; name=' + QUOTENAME(dc.name) COLLATE DATABASE_DEFAULT ELSE N'' END,
           CONCAT(N'name=', dc.name COLLATE DATABASE_DEFAULT, N'; system_named=', dc.is_system_named)
    FROM obj AS o
    JOIN sys.default_constraints AS dc ON dc.parent_object_id = o.object_id
    JOIN sys.columns AS c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id

    UNION ALL
    /* ---- indexes, primary keys and unique constraints (tables and indexed views) ----------- */
    SELECT 'INDEX', o.schema_name, o.object_name,
           CASE WHEN kc.is_system_named = 1 AND i.is_primary_key = 1 THEN N'<system-named PRIMARY KEY>'
                WHEN kc.is_system_named = 1 THEN CONVERT(nvarchar(max), N'<system-named UNIQUE> (' + ISNULL(ic.key_cols, N'') + N')')
                WHEN i.name LIKE N'ix[_]MSSQL[_]TemporalHistoryFor[_]%' THEN N'<engine-named history index>'
                ELSE i.name COLLATE DATABASE_DEFAULT END,
           i.index_id,
           CONCAT(
               CASE WHEN i.is_primary_key = 1 THEN N'PRIMARY KEY '
                    WHEN i.is_unique_constraint = 1 THEN N'UNIQUE CONSTRAINT '
                    WHEN i.is_unique = 1 THEN N'UNIQUE INDEX ' ELSE N'INDEX ' END,
               CONVERT(nvarchar(60), i.type_desc) COLLATE DATABASE_DEFAULT,
               CASE WHEN ic.key_cols IS NOT NULL THEN N' (' + ic.key_cols + N')' ELSE N'' END,
               CASE WHEN i.type = 5 THEN N''  -- a clustered columnstore index holds every column
                    WHEN ii.inc_cols IS NOT NULL THEN N' INCLUDE (' + ii.inc_cols + N')' ELSE N'' END,
               CASE WHEN i.has_filter = 1 THEN N' WHERE ' + CONVERT(nvarchar(max), i.filter_definition) COLLATE DATABASE_DEFAULT ELSE N'' END,
               N'; partition_scheme=', ISNULL(CONVERT(nvarchar(max), ps.name COLLATE DATABASE_DEFAULT) + N'(' + ISNULL(ip.part_cols, N'') + N')', N'NONE'),
               N'; compression=', ISNULL(p.compression, N'NONE'),
               CASE WHEN i.is_disabled = 1 THEN N'; DISABLED' ELSE N'' END,
               CASE WHEN i.ignore_dup_key = 1 THEN N'; IGNORE_DUP_KEY' ELSE N'' END,
               CASE WHEN i.type IN (1, 2) AND i.allow_row_locks = 0 THEN N'; ALLOW_ROW_LOCKS=OFF' ELSE N'' END,
               CASE WHEN i.type IN (1, 2) AND i.allow_page_locks = 0 THEN N'; ALLOW_PAGE_LOCKS=OFF' ELSE N'' END,
               CASE WHEN i.optimize_for_sequential_key = 1 THEN N'; OPTIMIZE_FOR_SEQUENTIAL_KEY' ELSE N'' END),
           CONCAT(N'name=', i.name COLLATE DATABASE_DEFAULT, N'; auto_created=', i.auto_created, N'; is_padded=', i.is_padded,
                  N'; fill_factor=', i.fill_factor, N'; rows=', ISNULL(p.row_count, 0),
                  N'; on=', CASE o.type WHEN 'V' THEN N'VIEW' ELSE N'TABLE' END)
    FROM obj AS o
    JOIN sys.indexes AS i ON i.object_id = o.object_id AND i.index_id > 0 AND i.is_hypothetical = 0
    LEFT JOIN sys.key_constraints AS kc ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
    LEFT JOIN sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
    LEFT JOIN idx_cols AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    LEFT JOIN idx_inc AS ii ON ii.object_id = i.object_id AND ii.index_id = i.index_id
    LEFT JOIN idx_part AS ip ON ip.object_id = i.object_id AND ip.index_id = i.index_id
    LEFT JOIN part AS p ON p.object_id = i.object_id AND p.index_id = i.index_id

    UNION ALL
    /* ---- check constraints ----------------------------------------------------------------- */
    SELECT 'CHECK', o.schema_name, o.object_name,
           CASE WHEN ck.is_system_named = 1
                THEN N'<system-named> ' + ISNULL(LEFT(CONVERT(varchar(64), HASHBYTES('SHA2_256', ck.definition), 2), 12), N'?')
                ELSE ck.name COLLATE DATABASE_DEFAULT END, 0,
           CONCAT(ISNULL(CONVERT(nvarchar(max), ck.definition) COLLATE DATABASE_DEFAULT, N'NOT READABLE'),
                  CASE WHEN ck.is_disabled = 1 THEN N'; DISABLED' ELSE N'' END,
                  CASE WHEN ck.is_not_for_replication = 1 THEN N'; NOT FOR REPLICATION' ELSE N'' END),
           CONCAT(N'name=', ck.name COLLATE DATABASE_DEFAULT, N'; system_named=', ck.is_system_named, N'; not_trusted=', ck.is_not_trusted)
    FROM obj AS o
    JOIN sys.check_constraints AS ck ON ck.parent_object_id = o.object_id

    UNION ALL
    /* ---- foreign keys ---------------------------------------------------------------------- */
    SELECT 'FOREIGN KEY', o.schema_name, o.object_name,
           CASE WHEN fk.is_system_named = 1 THEN CONVERT(nvarchar(max), N'<system-named> (' + fc.cols + N')')
                ELSE fk.name COLLATE DATABASE_DEFAULT END, 0,
           CONCAT(N'(', fc.cols, N') REFERENCES ',
                  QUOTENAME(OBJECT_SCHEMA_NAME(fk.referenced_object_id)) COLLATE DATABASE_DEFAULT, N'.',
                  QUOTENAME(OBJECT_NAME(fk.referenced_object_id)) COLLATE DATABASE_DEFAULT,
                  N' (', fc.ref_cols, N')',
                  N' ON DELETE ', CONVERT(nvarchar(60), fk.delete_referential_action_desc) COLLATE DATABASE_DEFAULT,
                  N' ON UPDATE ', CONVERT(nvarchar(60), fk.update_referential_action_desc) COLLATE DATABASE_DEFAULT,
                  CASE WHEN fk.is_disabled = 1 THEN N'; DISABLED' ELSE N'' END,
                  CASE WHEN fk.is_not_for_replication = 1 THEN N'; NOT FOR REPLICATION' ELSE N'' END),
           CONCAT(N'name=', fk.name COLLATE DATABASE_DEFAULT, N'; system_named=', fk.is_system_named, N'; not_trusted=', fk.is_not_trusted)
    FROM obj AS o
    JOIN sys.foreign_keys AS fk ON fk.parent_object_id = o.object_id
    JOIN fk_cols AS fc ON fc.constraint_object_id = fk.object_id

    UNION ALL
    /* ---- statistics that a person made ----------------------------------------------------- */
    SELECT 'STATISTICS', o.schema_name, o.object_name, st.name COLLATE DATABASE_DEFAULT, 0,
           CONCAT(N'(',
                  (SELECT STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(c.name) COLLATE DATABASE_DEFAULT), N', ')
                              WITHIN GROUP (ORDER BY sc.stats_column_id)
                   FROM sys.stats_columns AS sc
                   JOIN sys.columns AS c ON c.object_id = sc.object_id AND c.column_id = sc.column_id
                   WHERE sc.object_id = st.object_id AND sc.stats_id = st.stats_id),
                  N')',
                  CASE WHEN st.has_filter = 1 THEN N' WHERE ' + CONVERT(nvarchar(max), st.filter_definition) COLLATE DATABASE_DEFAULT ELSE N'' END,
                  CASE WHEN st.no_recompute = 1 THEN N'; NORECOMPUTE' ELSE N'' END),
           N''
    FROM obj AS o
    JOIN sys.stats AS st ON st.object_id = o.object_id AND st.user_created = 1

    UNION ALL
    /* ---- triggers on tables and views ------------------------------------------------------ */
    SELECT 'TRIGGER', o.schema_name, o.object_name, tr.name COLLATE DATABASE_DEFAULT, 0,
           CONCAT(CASE WHEN tr.is_instead_of_trigger = 1 THEN N'INSTEAD OF ' ELSE N'AFTER ' END,
                  (SELECT STRING_AGG(CONVERT(nvarchar(max), CONVERT(nvarchar(60), te.type_desc) COLLATE DATABASE_DEFAULT), N', ')
                              WITHIN GROUP (ORDER BY te.type)
                   FROM sys.trigger_events AS te WHERE te.object_id = tr.object_id),
                  CASE WHEN tr.is_disabled = 1 THEN N'; DISABLED' ELSE N'' END,
                  N'; ansi_nulls=', ISNULL(CONVERT(varchar(1), mh.uses_ansi_nulls), '?'),
                  N'; quoted_identifier=', ISNULL(CONVERT(varchar(1), mh.uses_quoted_identifier), '?'),
                  N'; text_sha256=', ISNULL(mh.loose_sha256, 'NOT READABLE')),
           CONCAT(N'exact_sha256=', ISNULL(mh.exact_sha256, 'NOT READABLE'),
                  N'; modified=', CONVERT(nvarchar(30), tr.modify_date, 126))
    FROM obj AS o
    JOIN sys.triggers AS tr ON tr.parent_id = o.object_id AND tr.parent_class = 1 AND tr.is_ms_shipped = 0
    LEFT JOIN module_hash AS mh ON mh.object_id = tr.object_id

    UNION ALL
    /* ---- views, procedures, functions: a hash of the text, never the text ------------------- */
    SELECT CASE WHEN o.type = 'V' THEN 'VIEW' WHEN o.type IN ('P', 'PC') THEN 'PROCEDURE' ELSE 'FUNCTION' END,
           s.name COLLATE DATABASE_DEFAULT, o.name COLLATE DATABASE_DEFAULT, N'', 0,
           CONCAT(CONVERT(nvarchar(60), o.type_desc) COLLATE DATABASE_DEFAULT,
                  N'; schema_bound=', ISNULL(mh.is_schema_bound, 0),
                  N'; ansi_nulls=', ISNULL(CONVERT(varchar(1), mh.uses_ansi_nulls), '?'),
                  N'; quoted_identifier=', ISNULL(CONVERT(varchar(1), mh.uses_quoted_identifier), '?'),
                  N'; text_sha256=', ISNULL(mh.loose_sha256, 'NOT READABLE')),
           CONCAT(N'exact_sha256=', ISNULL(mh.exact_sha256, 'NOT READABLE'),
                  N'; chars=', ISNULL(mh.chars, 0),
                  N'; modified=', CONVERT(nvarchar(30), o.modify_date, 126))
    FROM sys.objects AS o
    JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    LEFT JOIN module_hash AS mh ON mh.object_id = o.object_id
    WHERE o.is_ms_shipped = 0 AND s.name <> N'azsqlcd'
      AND o.type IN ('V', 'P', 'PC', 'FN', 'IF', 'TF', 'FS', 'FT', 'AF')
      AND NOT EXISTS (SELECT 1 FROM sys.extended_properties AS ep  -- the modules of database diagrams
                      WHERE ep.class = 1 AND ep.major_id = o.object_id AND ep.minor_id = 0
                        AND ep.name = N'microsoft_database_tools_support')

    UNION ALL
    /* ---- DDL triggers of the database: they fire on the statements of a deployment ---------- */
    SELECT 'DDL TRIGGER', N'', tr.name COLLATE DATABASE_DEFAULT, N'', 0,
           CONCAT(CASE WHEN tr.is_disabled = 1 THEN N'DISABLED; ' ELSE N'' END,
                  N'ansi_nulls=', ISNULL(CONVERT(varchar(1), mh.uses_ansi_nulls), '?'),
                  N'; quoted_identifier=', ISNULL(CONVERT(varchar(1), mh.uses_quoted_identifier), '?'),
                  N'; text_sha256=', ISNULL(mh.loose_sha256, 'NOT READABLE')),
           CONCAT(N'exact_sha256=', ISNULL(mh.exact_sha256, 'NOT READABLE'),
                  N'; modified=', CONVERT(nvarchar(30), tr.modify_date, 126))
    FROM sys.triggers AS tr
    LEFT JOIN module_hash AS mh ON mh.object_id = tr.object_id
    WHERE tr.parent_class = 0 AND tr.is_ms_shipped = 0

    UNION ALL
    /* ---- sequences ------------------------------------------------------------------------- */
    SELECT 'SEQUENCE', s.name COLLATE DATABASE_DEFAULT, sq.name COLLATE DATABASE_DEFAULT, N'', 0,
           CONCAT(TYPE_NAME(sq.user_type_id) COLLATE DATABASE_DEFAULT,
                  N' INCREMENT ', CONVERT(nvarchar(40), sq.increment),
                  N' MIN ', CONVERT(nvarchar(40), sq.minimum_value), N' MAX ', CONVERT(nvarchar(40), sq.maximum_value),
                  CASE WHEN sq.is_cycling = 1 THEN N' CYCLE' ELSE N'' END,
                  CASE WHEN sq.is_cached = 1 THEN N' CACHE ' + ISNULL(CONVERT(nvarchar(20), sq.cache_size), N'DEFAULT') ELSE N' NO CACHE' END),
           CONCAT(N'start=', CONVERT(nvarchar(40), sq.start_value))
    FROM sys.sequences AS sq
    JOIN sys.schemas AS s ON s.schema_id = sq.schema_id
    WHERE s.name <> N'azsqlcd'

    UNION ALL
    /* ---- user-defined types: alias types and table types ----------------------------------- */
    SELECT 'TYPE', SCHEMA_NAME(t.schema_id) COLLATE DATABASE_DEFAULT, t.name COLLATE DATABASE_DEFAULT, N'', 0,
           CASE WHEN t.is_table_type = 1
                THEN N'TABLE (' + ISNULL(
                         (SELECT STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(c.name) COLLATE DATABASE_DEFAULT + N' ' + ct.type_text
                                                                   + CASE WHEN c.is_identity = 1 THEN N' IDENTITY' ELSE N'' END
                                                                   + CASE WHEN c.is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END), N', ')
                                     WITHIN GROUP (ORDER BY c.column_id)
                          FROM sys.table_types AS tt
                          JOIN sys.columns AS c ON c.object_id = tt.type_table_object_id
                          JOIN col_type AS ct ON ct.object_id = c.object_id AND ct.column_id = c.column_id
                          WHERE tt.user_type_id = t.user_type_id), N'') + N')'
                     + ISNULL(
                         (SELECT N'; ' + STRING_AGG(CONVERT(nvarchar(max),
                                     CASE WHEN i.is_primary_key = 1 THEN N'PRIMARY KEY ' WHEN i.is_unique = 1 THEN N'UNIQUE ' ELSE N'INDEX ' END
                                     + CONVERT(nvarchar(60), i.type_desc) COLLATE DATABASE_DEFAULT + N' (' + ISNULL(k.key_cols, N'') + N')'), N'; ')
                                     WITHIN GROUP (ORDER BY i.is_primary_key DESC, k.key_cols)
                          FROM sys.table_types AS tt
                          JOIN sys.indexes AS i ON i.object_id = tt.type_table_object_id AND i.index_id > 0
                          LEFT JOIN idx_cols AS k ON k.object_id = i.object_id AND k.index_id = i.index_id
                          WHERE tt.user_type_id = t.user_type_id), N'')
                ELSE CONVERT(nvarchar(max), CONCAT(N'FROM ', TYPE_NAME(t.system_type_id) COLLATE DATABASE_DEFAULT,
                            N' max_length=', t.max_length, N' precision=', t.precision, N' scale=', t.scale,
                            CASE WHEN t.is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END))
           END,
           N''
    FROM sys.types AS t
    WHERE t.is_user_defined = 1 AND SCHEMA_NAME(t.schema_id) <> N'azsqlcd'

    UNION ALL
    /* ---- synonyms -------------------------------------------------------------------------- */
    SELECT 'SYNONYM', s.name COLLATE DATABASE_DEFAULT, sy.name COLLATE DATABASE_DEFAULT, N'', 0,
           CONVERT(nvarchar(max), sy.base_object_name) COLLATE DATABASE_DEFAULT, N''
    FROM sys.synonyms AS sy
    JOIN sys.schemas AS s ON s.schema_id = sy.schema_id
    WHERE s.name <> N'azsqlcd'

    UNION ALL
    /* ---- partition functions and schemes --------------------------------------------------- */
    SELECT 'PARTITION FUNCTION', N'', pf.name COLLATE DATABASE_DEFAULT, N'', 0,
           CONCAT((SELECT TOP (1) TYPE_NAME(pp.system_type_id) COLLATE DATABASE_DEFAULT
                   FROM sys.partition_parameters AS pp WHERE pp.function_id = pf.function_id ORDER BY pp.parameter_id),
                  N' RANGE ', CASE WHEN pf.boundary_value_on_right = 1 THEN N'RIGHT' ELSE N'LEFT' END),
           CONCAT(N'boundaries=', pf.fanout - 1,
                  N'; first=', (SELECT TOP (1) CONVERT(nvarchar(100),
                                    CASE WHEN CONVERT(sysname, SQL_VARIANT_PROPERTY(v.value, 'BaseType')) IN (N'datetime', N'smalldatetime')
                                         THEN CONVERT(sql_variant, CONVERT(datetime2(3), v.value)) ELSE v.value END) COLLATE DATABASE_DEFAULT
                                FROM sys.partition_range_values AS v WHERE v.function_id = pf.function_id ORDER BY v.boundary_id),
                  N'; last=', (SELECT TOP (1) CONVERT(nvarchar(100),
                                    CASE WHEN CONVERT(sysname, SQL_VARIANT_PROPERTY(v.value, 'BaseType')) IN (N'datetime', N'smalldatetime')
                                         THEN CONVERT(sql_variant, CONVERT(datetime2(3), v.value)) ELSE v.value END) COLLATE DATABASE_DEFAULT
                               FROM sys.partition_range_values AS v WHERE v.function_id = pf.function_id ORDER BY v.boundary_id DESC))
    FROM sys.partition_functions AS pf

    UNION ALL
    SELECT 'PARTITION SCHEME', N'', ps.name COLLATE DATABASE_DEFAULT, N'', 0,
           CONCAT(N'function=', pf.name COLLATE DATABASE_DEFAULT), N''
    FROM sys.partition_schemes AS ps
    JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id

    UNION ALL
    /* ---- row-level security ---------------------------------------------------------------- */
    SELECT 'SECURITY POLICY', s.name COLLATE DATABASE_DEFAULT, sp.name COLLATE DATABASE_DEFAULT,
           ISNULL(QUOTENAME(OBJECT_SCHEMA_NAME(pr.target_object_id)) COLLATE DATABASE_DEFAULT + N'.'
                  + QUOTENAME(OBJECT_NAME(pr.target_object_id)) COLLATE DATABASE_DEFAULT,
                  CONCAT(N'<object ', pr.target_object_id, N' NOT READABLE>'))
               + N' ' + CONVERT(nvarchar(60), pr.predicate_type_desc) COLLATE DATABASE_DEFAULT
               + ISNULL(N' ' + CONVERT(nvarchar(60), pr.operation_desc) COLLATE DATABASE_DEFAULT, N''),
           pr.security_predicate_id,
           CONCAT(CONVERT(nvarchar(60), pr.predicate_type_desc) COLLATE DATABASE_DEFAULT, N' ',
                  ISNULL(CONVERT(nvarchar(60), pr.operation_desc) COLLATE DATABASE_DEFAULT, N''), N' ',
                  CONVERT(nvarchar(max), pr.predicate_definition) COLLATE DATABASE_DEFAULT,
                  CASE WHEN sp.is_enabled = 1 THEN N'' ELSE N'; DISABLED' END),
           N''
    FROM sys.security_policies AS sp
    JOIN sys.schemas AS s ON s.schema_id = sp.schema_id
    JOIN sys.security_predicates AS pr ON pr.object_id = sp.object_id

    UNION ALL
    /* ---- what this login can read: with view_definition=0, rows and texts are missing ------- */
    SELECT 'VISIBILITY', N'', N'', N'', 0,
           CONCAT(N'view_definition=', ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION'), 0)),
           CONCAT(N'modules_without_text=', (SELECT COUNT(*) FROM sys.sql_modules AS m WHERE m.definition IS NULL),
                  N'; defaults_without_text=', (SELECT COUNT(*) FROM sys.default_constraints AS dc WHERE dc.definition IS NULL),
                  N'; checks_without_text=', (SELECT COUNT(*) FROM sys.check_constraints AS ck WHERE ck.definition IS NULL),
                  N'; computed_without_text=', (SELECT COUNT(*) FROM sys.computed_columns AS cc WHERE cc.definition IS NULL),
                  N'; columns_without_type=', (SELECT COUNT(*) FROM sys.columns AS c
                                               WHERE NOT EXISTS (SELECT 1 FROM sys.types AS t WHERE t.user_type_id = c.user_type_id)))
)
/* Tabs and line ends inside a value become one space, so that every row is one line in any export. */
SELECT database_name = DB_NAME(),
       inv.section,
       schema_name = REPLACE(REPLACE(REPLACE(inv.schema_name, NCHAR(13), N' '), NCHAR(10), N' '), NCHAR(9), N' '),
       object_name = REPLACE(REPLACE(REPLACE(inv.object_name, NCHAR(13), N' '), NCHAR(10), N' '), NCHAR(9), N' '),
       item_name   = REPLACE(REPLACE(REPLACE(inv.item_name, NCHAR(13), N' '), NCHAR(10), N' '), NCHAR(9), N' '),
       inv.ordinal,
       definition = REPLACE(REPLACE(REPLACE(inv.definition, NCHAR(13), N' '), NCHAR(10), N' '), NCHAR(9), N' '),
       info       = REPLACE(REPLACE(REPLACE(inv.info, NCHAR(13), N' '), NCHAR(10), N' '), NCHAR(9), N' ')
FROM inventory AS inv
ORDER BY inv.section, inv.schema_name, inv.object_name, inv.ordinal, inv.item_name;

SET LOCK_TIMEOUT -1;  -- back to the default, for the next statement in this window
