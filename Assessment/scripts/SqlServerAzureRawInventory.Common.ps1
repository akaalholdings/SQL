Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'SqlServerAzureMigrationAssessment.Common.ps1')

$script:RawServerPropertiesColumns = @(
    'sql_instance',
    'collection_time_utc',
    'machine_name',
    'server_name',
    'instance_name',
    'edition',
    'product_version',
    'product_level',
    'product_update_level',
    'engine_edition',
    'server_collation',
    'is_clustered',
    'is_hadr_enabled',
    'cpu_count',
    'scheduler_count',
    'physical_memory_mb',
    'sqlserver_start_time_utc',
    'collection_error'
)

$script:RawServerConfigurationsColumns = @(
    'sql_instance',
    'configuration_name',
    'value',
    'value_in_use',
    'description',
    'is_dynamic',
    'is_advanced',
    'collection_error'
)

$script:RawDatabasesColumns = @(
    'sql_instance',
    'database_name',
    'database_id',
    'state_desc',
    'user_access_desc',
    'compatibility_level',
    'recovery_model_desc',
    'collation_name',
    'containment_desc',
    'create_date',
    'is_read_only',
    'is_auto_close_on',
    'is_auto_shrink_on',
    'page_verify_option_desc',
    'is_broker_enabled',
    'is_cdc_enabled',
    'is_encrypted',
    'is_trustworthy_on',
    'owner_name',
    'collection_error'
)

$script:RawDatabaseFilesColumns = @(
    'sql_instance',
    'database_name',
    'logical_name',
    'file_id',
    'type_desc',
    'physical_name',
    'size_mb',
    'max_size_mb',
    'growth_setting',
    'is_percent_growth',
    'state_desc',
    'is_read_only',
    'is_sparse',
    'collection_error'
)

$script:RawDatabaseFeaturesColumns = @(
    'sql_instance',
    'database_name',
    'feature_name',
    'detected',
    'feature_value',
    'evidence',
    'collection_error'
)

$script:RawObjectFeatureScanColumns = @(
    'sql_instance',
    'database_name',
    'schema_name',
    'object_name',
    'object_type',
    'feature_name',
    'match_count',
    'definition_hash',
    'sanitized_snippet',
    'azure_sql_db_impact',
    'azure_sql_mi_impact',
    'sql_vm_impact',
    'collection_error'
)

$script:RawDatabaseDependenciesColumns = @(
    'sql_instance',
    'database_name',
    'referencing_schema_name',
    'referencing_object_name',
    'referencing_object_type',
    'referenced_server_name',
    'referenced_database_name',
    'referenced_schema_name',
    'referenced_entity_name',
    'dependency_type',
    'is_ambiguous',
    'collection_error'
)

$script:RawSqlAgentJobsColumns = @(
    'sql_instance',
    'job_id',
    'job_name',
    'enabled',
    'owner_name',
    'category_name',
    'date_created',
    'date_modified',
    'step_count',
    'command_step_count',
    'referenced_database_count',
    'schedule_count',
    'collection_error'
)

$script:RawSqlAgentJobStepsColumns = @(
    'sql_instance',
    'job_id',
    'job_name',
    'step_id',
    'step_name',
    'subsystem',
    'database_name',
    'proxy_name',
    'command_hash',
    'sanitized_command_preview',
    'output_file_name',
    'collection_error'
)

$script:RawLinkedServersColumns = @(
    'sql_instance',
    'linked_server_name',
    'provider',
    'product',
    'data_source',
    'catalog',
    'is_data_access_enabled',
    'is_rpc_out_enabled',
    'is_remote_login_enabled',
    'uses_remote_collation',
    'collection_error'
)

$script:RawHaDrTopologyColumns = @(
    'sql_instance',
    'database_name',
    'ha_dr_component',
    'component_name',
    'replica_server_name',
    'availability_mode',
    'failover_mode',
    'listener_name',
    'role_desc',
    'health_desc',
    'evidence',
    'paas_impact',
    'mi_impact',
    'vm_impact',
    'collection_error'
)

$script:RawSecurityPrincipalsColumns = @(
    'sql_instance',
    'scope',
    'database_name',
    'principal_name',
    'principal_type_desc',
    'authentication_type_desc',
    'mapped_login_name',
    'is_disabled',
    'default_database_name',
    'create_date',
    'modify_date',
    'roles',
    'collection_error'
)

$script:RawQueryStoreSummaryColumns = @(
    'sql_instance',
    'database_name',
    'actual_state_desc',
    'desired_state_desc',
    'readonly_reason',
    'current_storage_size_mb',
    'max_storage_size_mb',
    'query_count',
    'plan_count',
    'runtime_interval_count',
    'collection_error'
)

$script:RawWaitStatsSnapshotColumns = @(
    'sql_instance',
    'sample_time_utc',
    'wait_type',
    'waiting_tasks_count',
    'wait_time_ms',
    'signal_wait_time_ms',
    'resource_wait_time_ms',
    'max_wait_time_ms',
    'collection_error'
)

$script:RawIoFileStatsSnapshotColumns = @(
    'sql_instance',
    'sample_time_utc',
    'database_name',
    'logical_name',
    'file_id',
    'type_desc',
    'num_of_reads',
    'num_of_bytes_read',
    'io_stall_read_ms',
    'num_of_writes',
    'num_of_bytes_written',
    'io_stall_write_ms',
    'io_stall_ms',
    'size_on_disk_mb',
    'collection_error'
)

$script:RawWorkloadSamplesColumns = @(
    'sql_instance',
    'sample_id',
    'sample_time_utc',
    'metric_scope',
    'database_name',
    'metric_name',
    'metric_value',
    'metric_unit',
    'collection_error'
)

$script:RawTargetSignalMatrixColumns = @(
    'sql_instance',
    'database_name',
    'signal_scope',
    'signal_name',
    'detected',
    'signal_value',
    'azure_sql_db_impact',
    'azure_sql_mi_impact',
    'sql_vm_impact',
    'evidence_source'
)

$script:RawCollectionErrorsColumns = @(
    'sql_instance',
    'collector_name',
    'database_name',
    'error_message',
    'collection_time_utc'
)

$script:RawInventoryCsvContracts = [ordered]@{
    'server_properties.csv'    = $script:RawServerPropertiesColumns
    'server_configurations.csv' = $script:RawServerConfigurationsColumns
    'databases.csv'            = $script:RawDatabasesColumns
    'database_files.csv'       = $script:RawDatabaseFilesColumns
    'database_features.csv'    = $script:RawDatabaseFeaturesColumns
    'object_feature_scan.csv'  = $script:RawObjectFeatureScanColumns
    'database_dependencies.csv' = $script:RawDatabaseDependenciesColumns
    'sql_agent_jobs.csv'       = $script:RawSqlAgentJobsColumns
    'sql_agent_job_steps.csv'  = $script:RawSqlAgentJobStepsColumns
    'linked_servers.csv'       = $script:RawLinkedServersColumns
    'ha_dr_topology.csv'       = $script:RawHaDrTopologyColumns
    'security_principals.csv'  = $script:RawSecurityPrincipalsColumns
    'query_store_summary.csv'  = $script:RawQueryStoreSummaryColumns
    'wait_stats_snapshot.csv'  = $script:RawWaitStatsSnapshotColumns
    'io_file_stats_snapshot.csv' = $script:RawIoFileStatsSnapshotColumns
    'target_signal_matrix.csv' = $script:RawTargetSignalMatrixColumns
    'collection_errors.csv'    = $script:RawCollectionErrorsColumns
}

$script:RawInventoryOptionalCsvContracts = [ordered]@{
    'workload_samples.csv' = $script:RawWorkloadSamplesColumns
}

function Get-RawInventoryUtcNow {
    return (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
}

function ConvertTo-RawInventorySafeName {
    param([Parameter(Mandatory = $true)][string]$Value)

    $safe = (Get-Text $Value) -replace '[^A-Za-z0-9_.-]+', '-'
    $safe = $safe.Trim('-')
    if (Is-EmptyText $safe) { return 'sql-instance' }
    return $safe
}

function ConvertTo-SanitizedInventoryText {
    param(
        [object]$Value,
        [int]$MaxLength = 240
    )

    if ($null -eq $Value) { return '' }

    $text = $Value.ToString()
    $text = $text -replace '[\r\n\t]+', ' '
    $text = $text -replace '\s+', ' '
    $text = $text -replace "(?i)(password|pwd|secret|token|apikey|api_key|accesskey|access_key|accountkey|account_key)\s*=\s*'[^']*'", '$1=<redacted>'
    $text = $text -replace '(?i)(password|pwd|secret|token|apikey|api_key|accesskey|access_key|accountkey|account_key)\s*=\s*[^;,\s]+', '$1=<redacted>'
    $text = $text -replace '(?i)(sig|signature)=([^&\s]+)', '$1=<redacted>'
    $text = $text.Trim()

    if ($MaxLength -gt 0 -and $text.Length -gt $MaxLength) {
        return $text.Substring(0, $MaxLength)
    }

    return $text
}

function Test-RawInventorySamplingOptions {
    param(
        [switch]$EnableWorkloadSampling,
        [int]$SampleIntervalSeconds = 60,
        [int]$SampleDurationSeconds = 0
    )

    if (-not $EnableWorkloadSampling.IsPresent) { return $true }
    if ($SampleIntervalSeconds -lt 1) { throw 'SampleIntervalSeconds must be 1 or greater when workload sampling is enabled.' }
    if ($SampleDurationSeconds -lt 1) { throw 'SampleDurationSeconds must be 1 or greater when workload sampling is enabled.' }
    if ($SampleDurationSeconds -lt $SampleIntervalSeconds) { throw 'SampleDurationSeconds must be greater than or equal to SampleIntervalSeconds.' }

    return $true
}

function New-RawCollectionErrorRow {
    param(
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [Parameter(Mandatory = $true)][string]$CollectorName,
        [string]$DatabaseName = '',
        [Parameter(Mandatory = $true)][string]$ErrorMessage
    )

    return [PSCustomObject]@{
        sql_instance        = $SqlInstance
        collector_name      = $CollectorName
        database_name       = $DatabaseName
        error_message       = ConvertTo-SanitizedInventoryText -Value $ErrorMessage -MaxLength 1000
        collection_time_utc = Get-RawInventoryUtcNow
    }
}

function New-RawCollectionErrorList {
    $rows = New-Object 'System.Collections.Generic.List[object]'
    $rows.Add([PSCustomObject]@{
        sql_instance        = ''
        collector_name      = '__collector_state__'
        database_name       = ''
        error_message       = ''
        collection_time_utc = ''
    }) | Out-Null
    return $rows
}

function Get-VisibleRawCollectionErrorRows {
    param([object[]]$Rows = @())

    return @($Rows | Where-Object { (Get-ObjectText $_ 'collector_name') -ne '__collector_state__' })
}

function Add-RawCollectionError {
    param(
        [object]$ErrorRows,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [Parameter(Mandatory = $true)][string]$CollectorName,
        [string]$DatabaseName = '',
        [Parameter(Mandatory = $true)][string]$ErrorMessage
    )

    $ErrorRows.Add((New-RawCollectionErrorRow `
        -SqlInstance $SqlInstance `
        -CollectorName $CollectorName `
        -DatabaseName $DatabaseName `
        -ErrorMessage $ErrorMessage)) | Out-Null
}

function Invoke-RawInventoryQuery {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$Query,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [Parameter(Mandatory = $true)][string]$CollectorName,
        [object]$ErrorRows,
        [string]$Database = 'master',
        [string]$DatabaseNameForError = '',
        [int]$CommandTimeoutSeconds = 120
    )

    $result = Invoke-OptionalAssessmentSqlQuery `
        -ConnectionString $ConnectionString `
        -Query $Query `
        -Database $Database `
        -CollectorName $CollectorName `
        -CommandTimeoutSeconds $CommandTimeoutSeconds

    if (-not (Is-EmptyText $result.Error)) {
        Add-RawCollectionError `
            -ErrorRows $ErrorRows `
            -SqlInstance $SqlInstance `
            -CollectorName $CollectorName `
            -DatabaseName $DatabaseNameForError `
            -ErrorMessage $result.Error
    }

    return @($result.Rows)
}

function Export-RawInventoryCsv {
    param(
        [object]$Rows = @(),
        [Parameter(Mandatory = $true)][string[]]$Columns,
        [Parameter(Mandatory = $true)][string]$OutputPath
    )

    Ensure-Directory -Path (Split-Path -Parent $OutputPath)

    $normalizedRows = @()
    foreach ($row in @($Rows)) {
        $ordered = [ordered]@{}
        foreach ($column in $Columns) {
            $ordered[$column] = Get-ObjectValue -InputObject $row -PropertyName $column -Default ''
        }
        $normalizedRows += [PSCustomObject]$ordered
    }

    if (@($normalizedRows).Count -eq 0) {
        Set-Content -LiteralPath $OutputPath -Value (ConvertTo-AssessmentCsvHeader -Columns $Columns) -Encoding UTF8
        return
    }

    $normalizedRows | Export-Csv -LiteralPath $OutputPath -NoTypeInformation -Encoding UTF8
}

function Resolve-RawInventoryOutputRoot {
    param([string]$OutputRoot = './outputs')

    if ([System.IO.Path]::IsPathRooted($OutputRoot)) { return $OutputRoot }
    return Join-Path (Get-RepoRoot) $OutputRoot
}

function Resolve-RawInventoryTargets {
    param(
        [string]$SqlInstance = '',
        [string]$InstanceListCsv = '',
        [string]$DatabaseName = ''
    )

    $hasSqlInstance = -not (Is-EmptyText $SqlInstance)
    $hasInstanceList = -not (Is-EmptyText $InstanceListCsv)

    if ($hasSqlInstance -eq $hasInstanceList) {
        throw 'Specify exactly one of SqlInstance or InstanceListCsv.'
    }

    if ($hasSqlInstance) {
        return @([PSCustomObject]@{
            sql_instance  = $SqlInstance
            database_name = $DatabaseName
        })
    }

    $csvPath = Resolve-PathFromRepo -RelativeOrAbsolutePath $InstanceListCsv
    $rows = @(Import-Csv -LiteralPath $csvPath)
    $targets = New-Object 'System.Collections.Generic.List[object]'

    foreach ($row in $rows) {
        $instance = Get-ObjectText -InputObject $row -PropertyName 'sql_instance'
        if (Is-EmptyText $instance) { $instance = Get-ObjectText -InputObject $row -PropertyName 'instance_name' }
        if (Is-EmptyText $instance) { $instance = Get-ObjectText -InputObject $row -PropertyName 'server_name' }
        if (Is-EmptyText $instance) {
            throw "Instance list row is missing sql_instance, instance_name, or server_name: $($row | ConvertTo-Json -Compress)"
        }

        $targetDatabase = Get-ObjectText -InputObject $row -PropertyName 'database_name'
        if (Is-EmptyText $targetDatabase) { $targetDatabase = $DatabaseName }

        $targets.Add([PSCustomObject]@{
            sql_instance  = $instance
            database_name = $targetDatabase
        }) | Out-Null
    }

    return @($targets)
}

function Get-RawDatabaseNames {
    param(
        [object[]]$DatabaseRows,
        [bool]$OnlineOnly = $true
    )

    $names = New-Object 'System.Collections.Generic.List[string]'
    foreach ($db in @($DatabaseRows)) {
        $name = Get-ObjectText -InputObject $db -PropertyName 'database_name'
        $state = Get-ObjectText -InputObject $db -PropertyName 'state_desc'
        if (Is-EmptyText $name) { continue }
        if ($OnlineOnly -and $state -ne 'ONLINE') { continue }
        if (-not $names.Contains($name)) { $names.Add($name) | Out-Null }
    }

    return @($names)
}

function Get-RawServerPropertiesRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    $collectionTime = Get-RawInventoryUtcNow
    $query = @"
SELECT
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
    CONVERT(decimal(18, 2), osi.physical_memory_kb / 1024.0) AS physical_memory_mb,
    CONVERT(nvarchar(30), osi.sqlserver_start_time, 126) AS sqlserver_start_time_utc
FROM sys.dm_os_sys_info AS osi;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'server_properties' `
        -ErrorRows $ErrorRows

    $outRows = New-Object 'System.Collections.Generic.List[object]'
    foreach ($row in @($rows)) {
        $outRows.Add([PSCustomObject]@{
            sql_instance             = $SqlInstance
            collection_time_utc      = $collectionTime
            machine_name             = Get-ObjectText $row 'machine_name'
            server_name              = Get-ObjectText $row 'server_name'
            instance_name            = Get-ObjectText $row 'instance_name'
            edition                  = Get-ObjectText $row 'edition'
            product_version          = Get-ObjectText $row 'product_version'
            product_level            = Get-ObjectText $row 'product_level'
            product_update_level     = Get-ObjectText $row 'product_update_level'
            engine_edition           = Get-ObjectInt $row 'engine_edition'
            server_collation         = Get-ObjectText $row 'server_collation'
            is_clustered             = Get-ObjectInt $row 'is_clustered'
            is_hadr_enabled          = Get-ObjectInt $row 'is_hadr_enabled'
            cpu_count                = Get-ObjectInt $row 'cpu_count'
            scheduler_count          = Get-ObjectInt $row 'scheduler_count'
            physical_memory_mb       = Get-ObjectDouble $row 'physical_memory_mb'
            sqlserver_start_time_utc = Get-ObjectText $row 'sqlserver_start_time_utc'
            collection_error         = ''
        }) | Out-Null
    }

    return @($outRows)
}

function Get-RawServerConfigurationsRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    $query = @"
SELECT
    CONVERT(nvarchar(128), name) AS configuration_name,
    value,
    value_in_use,
    CONVERT(nvarchar(4000), description) AS description,
    CONVERT(int, is_dynamic) AS is_dynamic,
    CONVERT(int, is_advanced) AS is_advanced
FROM sys.configurations
ORDER BY name;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'server_configurations' `
        -ErrorRows $ErrorRows

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance       = $SqlInstance
            configuration_name = Get-ObjectText $_ 'configuration_name'
            value              = Get-ObjectInt $_ 'value'
            value_in_use       = Get-ObjectInt $_ 'value_in_use'
            description        = Get-ObjectText $_ 'description'
            is_dynamic         = Get-ObjectInt $_ 'is_dynamic'
            is_advanced        = Get-ObjectInt $_ 'is_advanced'
            collection_error   = ''
        }
    })
}

function Get-RawDatabasesRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [string]$DatabaseName = ''
    )

    $databaseFilter = if (Is-EmptyText $DatabaseName) { 'NULL' } else { Quote-SqlLiteral -Value $DatabaseName }
    $query = @"
DECLARE @DatabaseName sysname = $databaseFilter;

SELECT
    CONVERT(nvarchar(128), d.name) AS database_name,
    d.database_id,
    CONVERT(nvarchar(60), d.state_desc) AS state_desc,
    CONVERT(nvarchar(60), d.user_access_desc) AS user_access_desc,
    d.compatibility_level,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(128), d.collation_name) AS collation_name,
    CONVERT(nvarchar(60), d.containment_desc) AS containment_desc,
    CONVERT(nvarchar(30), d.create_date, 126) AS create_date,
    CONVERT(int, d.is_read_only) AS is_read_only,
    CONVERT(int, d.is_auto_close_on) AS is_auto_close_on,
    CONVERT(int, d.is_auto_shrink_on) AS is_auto_shrink_on,
    CONVERT(nvarchar(60), d.page_verify_option_desc) AS page_verify_option_desc,
    CONVERT(int, d.is_broker_enabled) AS is_broker_enabled,
    CONVERT(int, d.is_cdc_enabled) AS is_cdc_enabled,
    CONVERT(int, d.is_encrypted) AS is_encrypted,
    CONVERT(int, d.is_trustworthy_on) AS is_trustworthy_on,
    CONVERT(nvarchar(256), SUSER_SNAME(d.owner_sid)) AS owner_name
FROM sys.databases AS d
WHERE d.database_id > 4
  AND d.source_database_id IS NULL
  AND (@DatabaseName IS NULL OR d.name = @DatabaseName)
ORDER BY d.name;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'databases' `
        -ErrorRows $ErrorRows

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance            = $SqlInstance
            database_name           = Get-ObjectText $_ 'database_name'
            database_id             = Get-ObjectInt $_ 'database_id'
            state_desc              = Get-ObjectText $_ 'state_desc'
            user_access_desc        = Get-ObjectText $_ 'user_access_desc'
            compatibility_level     = Get-ObjectInt $_ 'compatibility_level'
            recovery_model_desc     = Get-ObjectText $_ 'recovery_model_desc'
            collation_name          = Get-ObjectText $_ 'collation_name'
            containment_desc        = Get-ObjectText $_ 'containment_desc'
            create_date             = Get-ObjectText $_ 'create_date'
            is_read_only            = Get-ObjectInt $_ 'is_read_only'
            is_auto_close_on        = Get-ObjectInt $_ 'is_auto_close_on'
            is_auto_shrink_on       = Get-ObjectInt $_ 'is_auto_shrink_on'
            page_verify_option_desc = Get-ObjectText $_ 'page_verify_option_desc'
            is_broker_enabled       = Get-ObjectInt $_ 'is_broker_enabled'
            is_cdc_enabled          = Get-ObjectInt $_ 'is_cdc_enabled'
            is_encrypted            = Get-ObjectInt $_ 'is_encrypted'
            is_trustworthy_on       = Get-ObjectInt $_ 'is_trustworthy_on'
            owner_name              = Get-ObjectText $_ 'owner_name'
            collection_error        = ''
        }
    })
}

function Get-RawDatabaseFilesRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [string]$DatabaseName = ''
    )

    $databaseFilter = if (Is-EmptyText $DatabaseName) { 'NULL' } else { Quote-SqlLiteral -Value $DatabaseName }
    $query = @"
DECLARE @DatabaseName sysname = $databaseFilter;

SELECT
    CONVERT(nvarchar(128), DB_NAME(mf.database_id)) AS database_name,
    CONVERT(nvarchar(128), mf.name) AS logical_name,
    mf.file_id,
    CONVERT(nvarchar(60), mf.type_desc) AS type_desc,
    CONVERT(nvarchar(520), mf.physical_name) AS physical_name,
    CONVERT(decimal(18, 2), mf.size * 8.0 / 1024.0) AS size_mb,
    CASE WHEN mf.max_size = -1 THEN -1 ELSE CONVERT(decimal(18, 2), mf.max_size * 8.0 / 1024.0) END AS max_size_mb,
    CASE WHEN mf.is_percent_growth = 1 THEN CONVERT(nvarchar(32), mf.growth) + N' percent' ELSE CONVERT(nvarchar(32), CONVERT(decimal(18, 2), mf.growth * 8.0 / 1024.0)) + N' MB' END AS growth_setting,
    CONVERT(int, mf.is_percent_growth) AS is_percent_growth,
    CONVERT(nvarchar(60), mf.state_desc) AS state_desc,
    CONVERT(int, mf.is_read_only) AS is_read_only,
    CONVERT(int, mf.is_sparse) AS is_sparse
FROM sys.master_files AS mf
JOIN sys.databases AS d
    ON mf.database_id = d.database_id
WHERE d.database_id > 4
  AND d.source_database_id IS NULL
  AND (@DatabaseName IS NULL OR d.name = @DatabaseName)
ORDER BY d.name, mf.file_id;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'database_files' `
        -ErrorRows $ErrorRows

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = Get-ObjectText $_ 'database_name'
            logical_name      = Get-ObjectText $_ 'logical_name'
            file_id           = Get-ObjectInt $_ 'file_id'
            type_desc         = Get-ObjectText $_ 'type_desc'
            physical_name     = Get-ObjectText $_ 'physical_name'
            size_mb           = Get-ObjectDouble $_ 'size_mb'
            max_size_mb       = Get-ObjectDouble $_ 'max_size_mb'
            growth_setting    = Get-ObjectText $_ 'growth_setting'
            is_percent_growth = Get-ObjectInt $_ 'is_percent_growth'
            state_desc        = Get-ObjectText $_ 'state_desc'
            is_read_only      = Get-ObjectInt $_ 'is_read_only'
            is_sparse         = Get-ObjectInt $_ 'is_sparse'
            collection_error  = ''
        }
    })
}

function New-RawDatabaseFeatureRowsFromDesign {
    param(
        [Parameter(Mandatory = $true)][object[]]$DatabaseDesignRows
    )

    $rows = New-Object 'System.Collections.Generic.List[object]'
    $countFeatures = @(
        @{ Name = 'data_file_count'; Column = 'data_file_count' },
        @{ Name = 'log_file_count'; Column = 'log_file_count' },
        @{ Name = 'filegroup_count'; Column = 'filegroup_count' },
        @{ Name = 'filestream_filegroups'; Column = 'filestream_filegroup_count' },
        @{ Name = 'memory_optimized_tables'; Column = 'memory_optimized_table_count' },
        @{ Name = 'filetables'; Column = 'filetable_count' },
        @{ Name = 'external_tables'; Column = 'external_table_count' },
        @{ Name = 'fulltext_catalogs'; Column = 'fulltext_catalog_count' },
        @{ Name = 'partition_schemes'; Column = 'partition_scheme_count' },
        @{ Name = 'user_assemblies'; Column = 'user_assembly_count' },
        @{ Name = 'synonyms'; Column = 'synonym_count' },
        @{ Name = 'cross_database_references'; Column = 'cross_database_reference_count' },
        @{ Name = 'largest_table_mb'; Column = 'largest_table_mb' },
        @{ Name = 'sql_agent_jobsteps'; Column = 'sql_agent_jobstep_count' },
        @{ Name = 'sql_agent_cmdexec_or_powershell_steps'; Column = 'sql_agent_cmdexec_step_count' }
    )
    $booleanFeatures = @(
        @{ Name = 'service_broker'; Column = 'service_broker_enabled' },
        @{ Name = 'cdc'; Column = 'cdc_enabled' },
        @{ Name = 'change_tracking'; Column = 'change_tracking_enabled' },
        @{ Name = 'tde'; Column = 'tde_enabled' }
    )

    foreach ($db in @($DatabaseDesignRows)) {
        $sqlInstance = Get-ObjectText $db 'sql_instance'
        $databaseName = Get-ObjectText $db 'database_name'

        foreach ($feature in $countFeatures) {
            $value = Get-ObjectDouble -InputObject $db -PropertyName $feature.Column
            $rows.Add([PSCustomObject]@{
                sql_instance     = $sqlInstance
                database_name    = $databaseName
                feature_name     = $feature.Name
                detected         = ($value -gt 0).ToString().ToLowerInvariant()
                feature_value    = $value
                evidence         = "$($feature.Column)=$value"
                collection_error = ''
            }) | Out-Null
        }

        foreach ($feature in $booleanFeatures) {
            $detected = Test-ObjectBool -InputObject $db -PropertyName $feature.Column
            $rows.Add([PSCustomObject]@{
                sql_instance     = $sqlInstance
                database_name    = $databaseName
                feature_name     = $feature.Name
                detected         = $detected.ToString().ToLowerInvariant()
                feature_value    = $detected.ToString().ToLowerInvariant()
                evidence         = "$($feature.Column)=$detected"
                collection_error = ''
            }) | Out-Null
        }

        $queryStoreState = Get-ObjectText $db 'query_store_state'
        $rows.Add([PSCustomObject]@{
            sql_instance     = $sqlInstance
            database_name    = $databaseName
            feature_name     = 'query_store_state'
            detected         = (-not (Is-EmptyText $queryStoreState) -and $queryStoreState -ne 'Unavailable').ToString().ToLowerInvariant()
            feature_value    = $queryStoreState
            evidence         = "query_store_state=$queryStoreState"
            collection_error = ''
        }) | Out-Null
    }

    return @($rows)
}

function Get-RawDatabaseFeaturesRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [string]$DatabaseName = ''
    )

    try {
        $designRows = @(Get-DatabaseDesignEvidence `
            -ConnectionString $ConnectionString `
            -SqlInstance $SqlInstance `
            -DatabaseName $DatabaseName)
    }
    catch {
        Add-RawCollectionError `
            -ErrorRows $ErrorRows `
            -SqlInstance $SqlInstance `
            -CollectorName 'database_features' `
            -DatabaseName $DatabaseName `
            -ErrorMessage $_.Exception.Message
        return @()
    }

    foreach ($row in @($designRows)) {
        $notes = Get-ObjectText $row 'collection_notes'
        if (-not (Is-EmptyText $notes) -and $notes -ne 'None') {
            Add-RawCollectionError `
                -ErrorRows $ErrorRows `
                -SqlInstance $SqlInstance `
                -CollectorName 'database_features' `
                -DatabaseName (Get-ObjectText $row 'database_name') `
                -ErrorMessage $notes
        }
    }

    return New-RawDatabaseFeatureRowsFromDesign -DatabaseDesignRows $designRows
}

function Get-RawObjectFeatureScanRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [object[]]$DatabaseRows
    )

    $outRows = New-Object 'System.Collections.Generic.List[object]'
    $databaseNames = Get-RawDatabaseNames -DatabaseRows $DatabaseRows -OnlineOnly $true

    $query = @"
WITH modules AS (
    SELECT
        CONVERT(nvarchar(128), SCHEMA_NAME(o.schema_id)) AS schema_name,
        CONVERT(nvarchar(128), o.name) AS object_name,
        CONVERT(nvarchar(60), o.type_desc) AS object_type,
        LOWER(CONVERT(nvarchar(max), m.definition)) AS definition_text,
        CONVERT(varchar(130), sys.fn_varbintohexstr(HASHBYTES('SHA2_256', CONVERT(nvarchar(4000), m.definition)))) AS definition_hash,
        LEFT(CONVERT(nvarchar(max), m.definition), 500) AS raw_snippet
    FROM sys.sql_modules AS m
    JOIN sys.objects AS o
        ON m.object_id = o.object_id
    WHERE o.is_ms_shipped = 0
)
SELECT
    schema_name,
    object_name,
    object_type,
    feature_name,
    1 AS match_count,
    definition_hash,
    raw_snippet,
    azure_sql_db_impact,
    azure_sql_mi_impact,
    sql_vm_impact
FROM modules
CROSS APPLY (VALUES
    (N'xp_cmdshell', CASE WHEN definition_text LIKE N'%xp_cmdshell%' THEN 1 ELSE 0 END, N'Not supported in Azure SQL Database.', N'Not supported in Azure SQL Managed Instance.', N'Can preserve only if accepted by VM security policy.'),
    (N'bulk_insert', CASE WHEN definition_text LIKE N'%bulk insert%' THEN 1 ELSE 0 END, N'Requires Azure Blob data source redesign.', N'Requires Azure Blob data source or redesign.', N'Can preserve local/file-share bulk load if network and OS access allow.'),
    (N'openrowset', CASE WHEN definition_text LIKE N'%openrowset%' THEN 1 ELSE 0 END, N'Provider and file access require target validation.', N'Non-SQL providers and non-Blob file access require redesign.', N'Can preserve provider access if installed and allowed.'),
    (N'opendatasource', CASE WHEN definition_text LIKE N'%opendatasource%' THEN 1 ELSE 0 END, N'Ad hoc external provider access is not a clean Azure SQL DB fit.', N'Validate provider and target restrictions.', N'Can preserve provider access if installed and allowed.'),
    (N'distributed_transaction', CASE WHEN definition_text LIKE N'%begin distributed transaction%' THEN 1 ELSE 0 END, N'Distributed transactions require redesign for Azure SQL DB.', N'Validate MI distributed transaction support and participants.', N'Can preserve MSDTC with SQL VM configuration.'),
    (N'service_broker_statement', CASE WHEN definition_text LIKE N'%service_broker%' OR definition_text LIKE N'%begin dialog%' THEN 1 ELSE 0 END, N'Service Broker is not supported in Azure SQL DB.', N'Better MI fit when broker behavior must be preserved.', N'Can preserve Service Broker behavior.'),
    (N'database_mail_statement', CASE WHEN definition_text LIKE N'%sp_send_dbmail%' THEN 1 ELSE 0 END, N'Database Mail is not available in Azure SQL DB.', N'Database Mail requires MI-specific configuration.', N'Can preserve Database Mail.'),
    (N'legacy_raiserror', CASE WHEN definition_text LIKE N'%raiserror%' THEN 1 ELSE 0 END, N'Legacy syntax should be reviewed before modernization.', N'Legacy syntax should be reviewed before migration.', N'Can preserve but should still be reviewed.'),
    (N'fastfirstrow_hint', CASE WHEN definition_text LIKE N'%fastfirstrow%' THEN 1 ELSE 0 END, N'Discontinued query hint requires replacement.', N'Discontinued query hint requires replacement.', N'Can preserve only on compatible engine versions.'),
    (N'disable_def_cnst_chk', CASE WHEN definition_text LIKE N'%disable_def_cnst_chk%' THEN 1 ELSE 0 END, N'Discontinued SET option is not supported.', N'Discontinued SET option is not supported.', N'Can preserve only on compatible engine versions.'),
    (N'cryptographic_provider', CASE WHEN definition_text LIKE N'%cryptographic provider%' THEN 1 ELSE 0 END, N'Cryptographic provider file access is not a clean Azure SQL DB fit.', N'Cryptographic provider file access is not supported.', N'Can preserve with OS/file access if configured.'),
    (N'execute_as_login', CASE WHEN definition_text LIKE N'%execute as login%' THEN 1 ELSE 0 END, N'Server-scoped impersonation requires redesign.', N'Validate EXECUTE AS limitations and login mapping.', N'Can preserve server login impersonation.')
) AS hits(feature_name, detected, azure_sql_db_impact, azure_sql_mi_impact, sql_vm_impact)
WHERE hits.detected = 1
ORDER BY schema_name, object_name, feature_name;
"@

    foreach ($databaseName in @($databaseNames)) {
        $rows = Invoke-RawInventoryQuery `
            -ConnectionString $ConnectionString `
            -Query $query `
            -SqlInstance $SqlInstance `
            -CollectorName 'object_feature_scan' `
            -ErrorRows $ErrorRows `
            -Database $databaseName `
            -DatabaseNameForError $databaseName

        foreach ($row in @($rows)) {
            $outRows.Add([PSCustomObject]@{
                sql_instance        = $SqlInstance
                database_name       = $databaseName
                schema_name         = Get-ObjectText $row 'schema_name'
                object_name         = Get-ObjectText $row 'object_name'
                object_type         = Get-ObjectText $row 'object_type'
                feature_name        = Get-ObjectText $row 'feature_name'
                match_count         = Get-ObjectInt $row 'match_count'
                definition_hash     = Get-ObjectText $row 'definition_hash'
                sanitized_snippet   = ConvertTo-SanitizedInventoryText -Value (Get-ObjectText $row 'raw_snippet') -MaxLength 240
                azure_sql_db_impact = Get-ObjectText $row 'azure_sql_db_impact'
                azure_sql_mi_impact = Get-ObjectText $row 'azure_sql_mi_impact'
                sql_vm_impact       = Get-ObjectText $row 'sql_vm_impact'
                collection_error    = ''
            }) | Out-Null
        }
    }

    return @($outRows)
}

function Get-RawDatabaseDependencyRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [object[]]$DatabaseRows
    )

    $outRows = New-Object 'System.Collections.Generic.List[object]'
    $databaseNames = Get-RawDatabaseNames -DatabaseRows $DatabaseRows -OnlineOnly $true
    $query = @"
SELECT
    CONVERT(nvarchar(128), OBJECT_SCHEMA_NAME(d.referencing_id)) AS referencing_schema_name,
    CONVERT(nvarchar(128), OBJECT_NAME(d.referencing_id)) AS referencing_object_name,
    CONVERT(nvarchar(60), o.type_desc) AS referencing_object_type,
    CONVERT(nvarchar(128), d.referenced_server_name) AS referenced_server_name,
    CONVERT(nvarchar(128), d.referenced_database_name) AS referenced_database_name,
    CONVERT(nvarchar(128), d.referenced_schema_name) AS referenced_schema_name,
    CONVERT(nvarchar(256), d.referenced_entity_name) AS referenced_entity_name,
    CONVERT(nvarchar(60), N'sql_expression_dependency') AS dependency_type,
    CONVERT(int, d.is_ambiguous) AS is_ambiguous
FROM sys.sql_expression_dependencies AS d
LEFT JOIN sys.objects AS o
    ON d.referencing_id = o.object_id
WHERE d.referenced_server_name IS NOT NULL
   OR d.referenced_database_name IS NOT NULL
UNION ALL
SELECT
    CONVERT(nvarchar(128), SCHEMA_NAME(s.schema_id)) AS referencing_schema_name,
    CONVERT(nvarchar(128), s.name) AS referencing_object_name,
    CONVERT(nvarchar(60), N'SYNONYM') AS referencing_object_type,
    CONVERT(nvarchar(128), PARSENAME(s.base_object_name, 4)) AS referenced_server_name,
    CONVERT(nvarchar(128), PARSENAME(s.base_object_name, 3)) AS referenced_database_name,
    CONVERT(nvarchar(128), PARSENAME(s.base_object_name, 2)) AS referenced_schema_name,
    CONVERT(nvarchar(256), PARSENAME(s.base_object_name, 1)) AS referenced_entity_name,
    CONVERT(nvarchar(60), N'synonym') AS dependency_type,
    CONVERT(int, 0) AS is_ambiguous
FROM sys.synonyms AS s
ORDER BY referencing_schema_name, referencing_object_name, dependency_type;
"@

    foreach ($databaseName in @($databaseNames)) {
        $rows = Invoke-RawInventoryQuery `
            -ConnectionString $ConnectionString `
            -Query $query `
            -SqlInstance $SqlInstance `
            -CollectorName 'database_dependencies' `
            -ErrorRows $ErrorRows `
            -Database $databaseName `
            -DatabaseNameForError $databaseName

        foreach ($row in @($rows)) {
            $outRows.Add([PSCustomObject]@{
                sql_instance            = $SqlInstance
                database_name           = $databaseName
                referencing_schema_name = Get-ObjectText $row 'referencing_schema_name'
                referencing_object_name = Get-ObjectText $row 'referencing_object_name'
                referencing_object_type = Get-ObjectText $row 'referencing_object_type'
                referenced_server_name  = Get-ObjectText $row 'referenced_server_name'
                referenced_database_name = Get-ObjectText $row 'referenced_database_name'
                referenced_schema_name  = Get-ObjectText $row 'referenced_schema_name'
                referenced_entity_name  = Get-ObjectText $row 'referenced_entity_name'
                dependency_type         = Get-ObjectText $row 'dependency_type'
                is_ambiguous            = Get-ObjectInt $row 'is_ambiguous'
                collection_error        = ''
            }) | Out-Null
        }
    }

    return @($outRows)
}

function Get-RawSqlAgentJobsRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    $query = @"
SELECT
    CONVERT(nvarchar(36), j.job_id) AS job_id,
    CONVERT(nvarchar(256), j.name) AS job_name,
    CONVERT(int, j.enabled) AS enabled,
    CONVERT(nvarchar(256), SUSER_SNAME(j.owner_sid)) AS owner_name,
    CONVERT(nvarchar(256), c.name) AS category_name,
    CONVERT(nvarchar(30), j.date_created, 126) AS date_created,
    CONVERT(nvarchar(30), j.date_modified, 126) AS date_modified,
    COUNT(s.step_id) AS step_count,
    SUM(CASE WHEN s.subsystem IN (N'CmdExec', N'PowerShell') THEN 1 ELSE 0 END) AS command_step_count,
    COUNT(DISTINCT NULLIF(s.database_name, N'')) AS referenced_database_count,
    COUNT(DISTINCT js.schedule_id) AS schedule_count
FROM dbo.sysjobs AS j
LEFT JOIN dbo.syscategories AS c
    ON j.category_id = c.category_id
LEFT JOIN dbo.sysjobsteps AS s
    ON j.job_id = s.job_id
LEFT JOIN dbo.sysjobschedules AS js
    ON j.job_id = js.job_id
GROUP BY j.job_id, j.name, j.enabled, j.owner_sid, c.name, j.date_created, j.date_modified
ORDER BY j.name;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'sql_agent_jobs' `
        -ErrorRows $ErrorRows `
        -Database 'msdb'

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance              = $SqlInstance
            job_id                    = Get-ObjectText $_ 'job_id'
            job_name                  = Get-ObjectText $_ 'job_name'
            enabled                   = Get-ObjectInt $_ 'enabled'
            owner_name                = Get-ObjectText $_ 'owner_name'
            category_name             = Get-ObjectText $_ 'category_name'
            date_created              = Get-ObjectText $_ 'date_created'
            date_modified             = Get-ObjectText $_ 'date_modified'
            step_count                = Get-ObjectInt $_ 'step_count'
            command_step_count        = Get-ObjectInt $_ 'command_step_count'
            referenced_database_count = Get-ObjectInt $_ 'referenced_database_count'
            schedule_count            = Get-ObjectInt $_ 'schedule_count'
            collection_error          = ''
        }
    })
}

function Get-RawSqlAgentJobStepsRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    $query = @"
SELECT
    CONVERT(nvarchar(36), j.job_id) AS job_id,
    CONVERT(nvarchar(256), j.name) AS job_name,
    s.step_id,
    CONVERT(nvarchar(256), s.step_name) AS step_name,
    CONVERT(nvarchar(60), s.subsystem) AS subsystem,
    CONVERT(nvarchar(128), s.database_name) AS database_name,
    CONVERT(nvarchar(256), p.name) AS proxy_name,
    CONVERT(varchar(130), sys.fn_varbintohexstr(HASHBYTES('SHA2_256', CONVERT(nvarchar(4000), s.command)))) AS command_hash,
    LEFT(CONVERT(nvarchar(max), s.command), 500) AS command_preview,
    CONVERT(nvarchar(520), s.output_file_name) AS output_file_name
FROM dbo.sysjobs AS j
JOIN dbo.sysjobsteps AS s
    ON j.job_id = s.job_id
LEFT JOIN dbo.sysproxies AS p
    ON s.proxy_id = p.proxy_id
ORDER BY j.name, s.step_id;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'sql_agent_job_steps' `
        -ErrorRows $ErrorRows `
        -Database 'msdb'

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance              = $SqlInstance
            job_id                    = Get-ObjectText $_ 'job_id'
            job_name                  = Get-ObjectText $_ 'job_name'
            step_id                   = Get-ObjectInt $_ 'step_id'
            step_name                 = Get-ObjectText $_ 'step_name'
            subsystem                 = Get-ObjectText $_ 'subsystem'
            database_name             = Get-ObjectText $_ 'database_name'
            proxy_name                = Get-ObjectText $_ 'proxy_name'
            command_hash              = Get-ObjectText $_ 'command_hash'
            sanitized_command_preview = ConvertTo-SanitizedInventoryText -Value (Get-ObjectText $_ 'command_preview') -MaxLength 240
            output_file_name          = ConvertTo-SanitizedInventoryText -Value (Get-ObjectText $_ 'output_file_name') -MaxLength 240
            collection_error          = ''
        }
    })
}

function Get-RawLinkedServersRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    $query = @"
SELECT
    CONVERT(nvarchar(128), name) AS linked_server_name,
    CONVERT(nvarchar(128), provider) AS provider,
    CONVERT(nvarchar(128), product) AS product,
    CONVERT(nvarchar(4000), data_source) AS data_source,
    CONVERT(nvarchar(128), catalog) AS catalog,
    CONVERT(int, is_data_access_enabled) AS is_data_access_enabled,
    CONVERT(int, is_rpc_out_enabled) AS is_rpc_out_enabled,
    CONVERT(int, is_remote_login_enabled) AS is_remote_login_enabled,
    CONVERT(int, uses_remote_collation) AS uses_remote_collation
FROM sys.servers
WHERE is_linked = 1
ORDER BY name;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'linked_servers' `
        -ErrorRows $ErrorRows

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance             = $SqlInstance
            linked_server_name       = Get-ObjectText $_ 'linked_server_name'
            provider                 = Get-ObjectText $_ 'provider'
            product                  = Get-ObjectText $_ 'product'
            data_source              = Get-ObjectText $_ 'data_source'
            catalog                  = Get-ObjectText $_ 'catalog'
            is_data_access_enabled   = Get-ObjectInt $_ 'is_data_access_enabled'
            is_rpc_out_enabled       = Get-ObjectInt $_ 'is_rpc_out_enabled'
            is_remote_login_enabled  = Get-ObjectInt $_ 'is_remote_login_enabled'
            uses_remote_collation    = Get-ObjectInt $_ 'uses_remote_collation'
            collection_error         = ''
        }
    })
}

function Get-RawHaDrTopologyRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    try {
        $rows = @(Get-ClusterDesignEvidence -ConnectionString $ConnectionString -SqlInstance $SqlInstance)
    }
    catch {
        Add-RawCollectionError `
            -ErrorRows $ErrorRows `
            -SqlInstance $SqlInstance `
            -CollectorName 'ha_dr_topology' `
            -ErrorMessage $_.Exception.Message
        return @()
    }

    foreach ($row in @($rows)) {
        $error = Get-ObjectText $row 'collection_error'
        if (-not (Is-EmptyText $error)) {
            Add-RawCollectionError `
                -ErrorRows $ErrorRows `
                -SqlInstance $SqlInstance `
                -CollectorName 'ha_dr_topology' `
                -DatabaseName (Get-ObjectText $row 'database_name') `
                -ErrorMessage $error
        }
    }

    return @($rows)
}

function Get-RawSecurityPrincipalsRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [object[]]$DatabaseRows
    )

    $outRows = New-Object 'System.Collections.Generic.List[object]'
    $serverQuery = @"
SELECT
    CONVERT(nvarchar(256), sp.name) AS principal_name,
    CONVERT(nvarchar(60), sp.type_desc) AS principal_type_desc,
    CONVERT(nvarchar(60), N'INSTANCE') AS authentication_type_desc,
    CONVERT(nvarchar(256), sp.name) AS mapped_login_name,
    CONVERT(int, ISNULL(sl.is_disabled, 0)) AS is_disabled,
    CONVERT(nvarchar(128), sl.default_database_name) AS default_database_name,
    CONVERT(nvarchar(30), sp.create_date, 126) AS create_date,
    CONVERT(nvarchar(30), sp.modify_date, 126) AS modify_date
FROM sys.server_principals AS sp
LEFT JOIN sys.sql_logins AS sl
    ON sp.principal_id = sl.principal_id
WHERE sp.type IN (N'S', N'U', N'G', N'E', N'X')
  AND sp.name NOT LIKE N'##%'
ORDER BY sp.type_desc, sp.name;
"@

    $serverRows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $serverQuery `
        -SqlInstance $SqlInstance `
        -CollectorName 'security_principals:server' `
        -ErrorRows $ErrorRows

    foreach ($row in @($serverRows)) {
        $outRows.Add([PSCustomObject]@{
            sql_instance             = $SqlInstance
            scope                    = 'server'
            database_name            = ''
            principal_name           = Get-ObjectText $row 'principal_name'
            principal_type_desc      = Get-ObjectText $row 'principal_type_desc'
            authentication_type_desc = Get-ObjectText $row 'authentication_type_desc'
            mapped_login_name        = Get-ObjectText $row 'mapped_login_name'
            is_disabled              = Get-ObjectInt $row 'is_disabled'
            default_database_name    = Get-ObjectText $row 'default_database_name'
            create_date              = Get-ObjectText $row 'create_date'
            modify_date              = Get-ObjectText $row 'modify_date'
            roles                    = ''
            collection_error         = ''
        }) | Out-Null
    }

    $databaseQuery = @"
SELECT
    CONVERT(nvarchar(256), dp.name) AS principal_name,
    CONVERT(nvarchar(60), dp.type_desc) AS principal_type_desc,
    CONVERT(nvarchar(60), dp.authentication_type_desc) AS authentication_type_desc,
    CONVERT(nvarchar(256), SUSER_SNAME(dp.sid)) AS mapped_login_name,
    CONVERT(nvarchar(30), dp.create_date, 126) AS create_date,
    CONVERT(nvarchar(30), dp.modify_date, 126) AS modify_date,
    STUFF((
        SELECT N'; ' + USER_NAME(drm.role_principal_id)
        FROM sys.database_role_members AS drm
        WHERE drm.member_principal_id = dp.principal_id
        ORDER BY USER_NAME(drm.role_principal_id)
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 2, N'') AS roles
FROM sys.database_principals AS dp
WHERE dp.type IN (N'S', N'U', N'G', N'E', N'X')
  AND dp.name NOT IN (N'dbo', N'guest', N'INFORMATION_SCHEMA', N'sys')
ORDER BY dp.type_desc, dp.name;
"@

    foreach ($databaseName in @(Get-RawDatabaseNames -DatabaseRows $DatabaseRows -OnlineOnly $true)) {
        $dbRows = Invoke-RawInventoryQuery `
            -ConnectionString $ConnectionString `
            -Query $databaseQuery `
            -SqlInstance $SqlInstance `
            -CollectorName 'security_principals:database' `
            -ErrorRows $ErrorRows `
            -Database $databaseName `
            -DatabaseNameForError $databaseName

        foreach ($row in @($dbRows)) {
            $outRows.Add([PSCustomObject]@{
                sql_instance             = $SqlInstance
                scope                    = 'database'
                database_name            = $databaseName
                principal_name           = Get-ObjectText $row 'principal_name'
                principal_type_desc      = Get-ObjectText $row 'principal_type_desc'
                authentication_type_desc = Get-ObjectText $row 'authentication_type_desc'
                mapped_login_name        = Get-ObjectText $row 'mapped_login_name'
                is_disabled              = ''
                default_database_name    = ''
                create_date              = Get-ObjectText $row 'create_date'
                modify_date              = Get-ObjectText $row 'modify_date'
                roles                    = Get-ObjectText $row 'roles'
                collection_error         = ''
            }) | Out-Null
        }
    }

    return @($outRows)
}

function Get-RawQueryStoreSummaryRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [object[]]$DatabaseRows
    )

    $outRows = New-Object 'System.Collections.Generic.List[object]'
    $query = @"
DECLARE @actual_state_desc nvarchar(60) = N'Unavailable',
        @desired_state_desc nvarchar(60) = N'Unavailable',
        @readonly_reason bigint = 0,
        @current_storage_size_mb bigint = 0,
        @max_storage_size_mb bigint = 0,
        @query_count bigint = 0,
        @plan_count bigint = 0,
        @runtime_interval_count bigint = 0;

IF OBJECT_ID(N'sys.database_query_store_options') IS NOT NULL
BEGIN
    SELECT
        @actual_state_desc = CONVERT(nvarchar(60), actual_state_desc),
        @desired_state_desc = CONVERT(nvarchar(60), desired_state_desc),
        @readonly_reason = readonly_reason,
        @current_storage_size_mb = current_storage_size_mb,
        @max_storage_size_mb = max_storage_size_mb
    FROM sys.database_query_store_options;

    IF OBJECT_ID(N'sys.query_store_query') IS NOT NULL
        SELECT @query_count = COUNT(*) FROM sys.query_store_query;

    IF OBJECT_ID(N'sys.query_store_plan') IS NOT NULL
        SELECT @plan_count = COUNT(*) FROM sys.query_store_plan;

    IF OBJECT_ID(N'sys.query_store_runtime_stats_interval') IS NOT NULL
        SELECT @runtime_interval_count = COUNT(*) FROM sys.query_store_runtime_stats_interval;
END;

SELECT
    @actual_state_desc AS actual_state_desc,
    @desired_state_desc AS desired_state_desc,
    @readonly_reason AS readonly_reason,
    @current_storage_size_mb AS current_storage_size_mb,
    @max_storage_size_mb AS max_storage_size_mb,
    @query_count AS query_count,
    @plan_count AS plan_count,
    @runtime_interval_count AS runtime_interval_count;
"@

    foreach ($databaseName in @(Get-RawDatabaseNames -DatabaseRows $DatabaseRows -OnlineOnly $true)) {
        $rows = Invoke-RawInventoryQuery `
            -ConnectionString $ConnectionString `
            -Query $query `
            -SqlInstance $SqlInstance `
            -CollectorName 'query_store_summary' `
            -ErrorRows $ErrorRows `
            -Database $databaseName `
            -DatabaseNameForError $databaseName

        foreach ($row in @($rows)) {
            $outRows.Add([PSCustomObject]@{
                sql_instance              = $SqlInstance
                database_name             = $databaseName
                actual_state_desc         = Get-ObjectText $row 'actual_state_desc'
                desired_state_desc        = Get-ObjectText $row 'desired_state_desc'
                readonly_reason           = Get-ObjectInt $row 'readonly_reason'
                current_storage_size_mb   = Get-ObjectInt $row 'current_storage_size_mb'
                max_storage_size_mb       = Get-ObjectInt $row 'max_storage_size_mb'
                query_count               = Get-ObjectInt $row 'query_count'
                plan_count                = Get-ObjectInt $row 'plan_count'
                runtime_interval_count    = Get-ObjectInt $row 'runtime_interval_count'
                collection_error          = ''
            }) | Out-Null
        }
    }

    return @($outRows)
}

function Get-RawWaitStatsSnapshotRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows
    )

    $sampleTime = Get-RawInventoryUtcNow
    $query = @"
SELECT TOP (200)
    CONVERT(nvarchar(120), wait_type) AS wait_type,
    waiting_tasks_count,
    wait_time_ms,
    signal_wait_time_ms,
    wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms,
    max_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP',
    N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
    N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'DBMIRROR_DBM_EVENT',
    N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD',
    N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC',
    N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
    N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE',
    N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE', N'KSOURCE_WAKEUP',
    N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'MEMORY_ALLOCATION_EXT',
    N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_DRAIN_WORKER',
    N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST',
    N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK',
    N'PREEMPTIVE_OS_FLUSHFILEBUFFERS', N'PREEMPTIVE_XE_GETTARGETSTATE',
    N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
    N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_ASYNC_QUEUE',
    N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'REQUEST_FOR_DEADLOCK_SEARCH',
    N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH',
    N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY',
    N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP',
    N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', N'SLEEP_TEMPDBSTARTUP',
    N'SNI_HTTP_ACCEPT', N'SP_SERVER_DIAGNOSTICS_SLEEP',
    N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
    N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS', N'WAITFOR',
    N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
    N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', N'XE_DISPATCHER_WAIT',
    N'XE_TIMER_EVENT'
)
ORDER BY wait_time_ms DESC;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'wait_stats_snapshot' `
        -ErrorRows $ErrorRows

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance          = $SqlInstance
            sample_time_utc       = $sampleTime
            wait_type             = Get-ObjectText $_ 'wait_type'
            waiting_tasks_count   = Get-ObjectInt $_ 'waiting_tasks_count'
            wait_time_ms          = Get-ObjectInt $_ 'wait_time_ms'
            signal_wait_time_ms   = Get-ObjectInt $_ 'signal_wait_time_ms'
            resource_wait_time_ms = Get-ObjectInt $_ 'resource_wait_time_ms'
            max_wait_time_ms      = Get-ObjectInt $_ 'max_wait_time_ms'
            collection_error      = ''
        }
    })
}

function Get-RawIoFileStatsSnapshotRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [string]$DatabaseName = ''
    )

    $sampleTime = Get-RawInventoryUtcNow
    $databaseFilter = if (Is-EmptyText $DatabaseName) { 'NULL' } else { Quote-SqlLiteral -Value $DatabaseName }
    $query = @"
DECLARE @DatabaseName sysname = $databaseFilter;

SELECT
    CONVERT(nvarchar(128), DB_NAME(vfs.database_id)) AS database_name,
    CONVERT(nvarchar(128), mf.name) AS logical_name,
    vfs.file_id,
    CONVERT(nvarchar(60), mf.type_desc) AS type_desc,
    vfs.num_of_reads,
    vfs.num_of_bytes_read,
    vfs.io_stall_read_ms,
    vfs.num_of_writes,
    vfs.num_of_bytes_written,
    vfs.io_stall_write_ms,
    vfs.io_stall AS io_stall_ms,
    CONVERT(decimal(18, 2), vfs.size_on_disk_bytes / 1024.0 / 1024.0) AS size_on_disk_mb
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
JOIN sys.master_files AS mf
    ON vfs.database_id = mf.database_id
   AND vfs.file_id = mf.file_id
JOIN sys.databases AS d
    ON vfs.database_id = d.database_id
WHERE d.database_id > 4
  AND (@DatabaseName IS NULL OR d.name = @DatabaseName)
ORDER BY d.name, vfs.file_id;
"@

    $rows = Invoke-RawInventoryQuery `
        -ConnectionString $ConnectionString `
        -Query $query `
        -SqlInstance $SqlInstance `
        -CollectorName 'io_file_stats_snapshot' `
        -ErrorRows $ErrorRows

    return @($rows | ForEach-Object {
        [PSCustomObject]@{
            sql_instance          = $SqlInstance
            sample_time_utc       = $sampleTime
            database_name         = Get-ObjectText $_ 'database_name'
            logical_name          = Get-ObjectText $_ 'logical_name'
            file_id               = Get-ObjectInt $_ 'file_id'
            type_desc             = Get-ObjectText $_ 'type_desc'
            num_of_reads          = Get-ObjectInt $_ 'num_of_reads'
            num_of_bytes_read     = Get-ObjectValue -InputObject $_ -PropertyName 'num_of_bytes_read' -Default 0
            io_stall_read_ms      = Get-ObjectInt $_ 'io_stall_read_ms'
            num_of_writes         = Get-ObjectInt $_ 'num_of_writes'
            num_of_bytes_written  = Get-ObjectValue -InputObject $_ -PropertyName 'num_of_bytes_written' -Default 0
            io_stall_write_ms     = Get-ObjectInt $_ 'io_stall_write_ms'
            io_stall_ms           = Get-ObjectInt $_ 'io_stall_ms'
            size_on_disk_mb       = Get-ObjectDouble $_ 'size_on_disk_mb'
            collection_error      = ''
        }
    })
}

function Get-RawWorkloadSampleRows {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [object]$ErrorRows,
        [switch]$EnableWorkloadSampling,
        [int]$SampleIntervalSeconds = 60,
        [int]$SampleDurationSeconds = 0
    )

    if (-not $EnableWorkloadSampling.IsPresent) { return @() }
    Test-RawInventorySamplingOptions `
        -EnableWorkloadSampling `
        -SampleIntervalSeconds $SampleIntervalSeconds `
        -SampleDurationSeconds $SampleDurationSeconds | Out-Null

    $sampleCount = [math]::Max(1, [int][math]::Ceiling($SampleDurationSeconds / [double]$SampleIntervalSeconds))
    $outRows = New-Object 'System.Collections.Generic.List[object]'
    $query = @"
SELECT N'server' AS metric_scope, N'' AS database_name, N'cpu_ticks' AS metric_name, CONVERT(decimal(38, 0), cpu_ticks) AS metric_value, N'ticks' AS metric_unit
FROM sys.dm_os_sys_info
UNION ALL
SELECT N'server', N'', N'ms_ticks', CONVERT(decimal(38, 0), ms_ticks), N'milliseconds'
FROM sys.dm_os_sys_info
UNION ALL
SELECT N'server', N'', N'active_requests', CONVERT(decimal(38, 0), COUNT(*)), N'count'
FROM sys.dm_exec_requests
WHERE session_id <> @@SPID
UNION ALL
SELECT N'waits', N'', N'total_wait_time_ms', CONVERT(decimal(38, 0), SUM(wait_time_ms)), N'milliseconds'
FROM sys.dm_os_wait_stats
UNION ALL
SELECT N'waits', N'', N'total_waiting_tasks', CONVERT(decimal(38, 0), SUM(waiting_tasks_count)), N'count'
FROM sys.dm_os_wait_stats
UNION ALL
SELECT N'io', N'', N'total_reads', CONVERT(decimal(38, 0), SUM(num_of_reads)), N'count'
FROM sys.dm_io_virtual_file_stats(NULL, NULL)
UNION ALL
SELECT N'io', N'', N'total_writes', CONVERT(decimal(38, 0), SUM(num_of_writes)), N'count'
FROM sys.dm_io_virtual_file_stats(NULL, NULL)
UNION ALL
SELECT N'io', N'', N'total_io_stall_ms', CONVERT(decimal(38, 0), SUM(io_stall)), N'milliseconds'
FROM sys.dm_io_virtual_file_stats(NULL, NULL);
"@

    for ($sampleId = 1; $sampleId -le $sampleCount; $sampleId++) {
        $sampleTime = Get-RawInventoryUtcNow
        $rows = Invoke-RawInventoryQuery `
            -ConnectionString $ConnectionString `
            -Query $query `
            -SqlInstance $SqlInstance `
            -CollectorName 'workload_samples' `
            -ErrorRows $ErrorRows

        foreach ($row in @($rows)) {
            $outRows.Add([PSCustomObject]@{
                sql_instance     = $SqlInstance
                sample_id        = $sampleId
                sample_time_utc  = $sampleTime
                metric_scope     = Get-ObjectText $row 'metric_scope'
                database_name    = Get-ObjectText $row 'database_name'
                metric_name      = Get-ObjectText $row 'metric_name'
                metric_value     = Get-ObjectValue -InputObject $row -PropertyName 'metric_value' -Default 0
                metric_unit      = Get-ObjectText $row 'metric_unit'
                collection_error = ''
            }) | Out-Null
        }

        if ($sampleId -lt $sampleCount) {
            Start-Sleep -Seconds $SampleIntervalSeconds
        }
    }

    return @($outRows)
}

function Add-RawTargetSignal {
    param(
        [object]$Rows,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [string]$DatabaseName = '',
        [Parameter(Mandatory = $true)][string]$SignalScope,
        [Parameter(Mandatory = $true)][string]$SignalName,
        [Parameter(Mandatory = $true)][bool]$Detected,
        [object]$SignalValue = '',
        [Parameter(Mandatory = $true)][string]$AzureSqlDbImpact,
        [Parameter(Mandatory = $true)][string]$AzureSqlMiImpact,
        [Parameter(Mandatory = $true)][string]$SqlVmImpact,
        [Parameter(Mandatory = $true)][string]$EvidenceSource
    )

    $Rows.Add([PSCustomObject]@{
        sql_instance        = $SqlInstance
        database_name       = $DatabaseName
        signal_scope        = $SignalScope
        signal_name         = $SignalName
        detected            = $Detected.ToString().ToLowerInvariant()
        signal_value        = Get-Text $SignalValue
        azure_sql_db_impact = $AzureSqlDbImpact
        azure_sql_mi_impact = $AzureSqlMiImpact
        sql_vm_impact       = $SqlVmImpact
        evidence_source     = $EvidenceSource
    }) | Out-Null
}

function Get-RawFeatureValue {
    param(
        [object[]]$DatabaseFeatureRows,
        [string]$DatabaseName,
        [string]$FeatureName
    )

    $row = @($DatabaseFeatureRows | Where-Object {
        (Get-ObjectText $_ 'database_name') -eq $DatabaseName -and
        (Get-ObjectText $_ 'feature_name') -eq $FeatureName
    } | Select-Object -First 1)

    if (@($row).Count -eq 0) { return 0 }

    $rowValue = @($row)[0]
    $numericValue = Get-ObjectDouble -InputObject $rowValue -PropertyName 'feature_value' -Default ([double]::NaN)
    if (-not [double]::IsNaN($numericValue)) { return $numericValue }

    if ((Get-ObjectText $rowValue 'detected') -eq 'true') { return 1 }
    return 0
}

function New-TargetSignalRows {
    param(
        [object[]]$ServerPropertiesRows = @(),
        [object[]]$ServerConfigurationRows = @(),
        [object[]]$DatabaseRows = @(),
        [object[]]$DatabaseFileRows = @(),
        [object[]]$DatabaseFeatureRows = @(),
        [object[]]$ObjectFeatureScanRows = @(),
        [object[]]$SqlAgentJobStepRows = @(),
        [object[]]$LinkedServerRows = @(),
        [object[]]$SecurityPrincipalRows = @()
    )

    $signals = New-Object 'System.Collections.Generic.List[object]'
    $signals.Add([PSCustomObject]@{
        sql_instance        = ''
        database_name       = ''
        signal_scope        = '__collector_state__'
        signal_name         = '__collector_state__'
        detected            = 'false'
        signal_value        = ''
        azure_sql_db_impact = ''
        azure_sql_mi_impact = ''
        sql_vm_impact       = ''
        evidence_source     = ''
    }) | Out-Null
    $server = if (@($ServerPropertiesRows).Count -gt 0) { @($ServerPropertiesRows)[0] } else { $null }
    $sqlInstance = Get-ObjectText $server 'sql_instance'
    if (Is-EmptyText $sqlInstance -and @($ServerConfigurationRows).Count -gt 0) {
        $sqlInstance = Get-ObjectText (@($ServerConfigurationRows)[0]) 'sql_instance'
    }
    if (Is-EmptyText $sqlInstance -and @($DatabaseRows).Count -gt 0) {
        $sqlInstance = Get-ObjectText (@($DatabaseRows)[0]) 'sql_instance'
    }
    if (Is-EmptyText $sqlInstance -and @($LinkedServerRows).Count -gt 0) {
        $sqlInstance = Get-ObjectText (@($LinkedServerRows)[0]) 'sql_instance'
    }
    if (Is-EmptyText $sqlInstance -and @($SecurityPrincipalRows).Count -gt 0) {
        $sqlInstance = Get-ObjectText (@($SecurityPrincipalRows)[0]) 'sql_instance'
    }

    $xpCmdshell = @($ServerConfigurationRows | Where-Object {
        (Get-ObjectText $_ 'configuration_name') -eq 'xp_cmdshell' -and
        (Get-ObjectInt $_ 'value_in_use') -gt 0
    }).Count -gt 0
    Add-RawTargetSignal `
        -Rows $signals `
        -SqlInstance $sqlInstance `
        -SignalScope 'server' `
        -SignalName 'xp_cmdshell_enabled' `
        -Detected $xpCmdshell `
        -SignalValue $xpCmdshell `
        -AzureSqlDbImpact 'Not supported; move OS work outside the database.' `
        -AzureSqlMiImpact 'Not supported in Azure SQL Managed Instance.' `
        -SqlVmImpact 'Can preserve only if accepted by VM security policy.' `
        -EvidenceSource 'server_configurations.csv'

    $linkedServerCount = @($LinkedServerRows).Count
    Add-RawTargetSignal `
        -Rows $signals `
        -SqlInstance $sqlInstance `
        -SignalScope 'server' `
        -SignalName 'linked_servers' `
        -Detected ($linkedServerCount -gt 0) `
        -SignalValue $linkedServerCount `
        -AzureSqlDbImpact 'Linked server functionality is not available in Azure SQL Database.' `
        -AzureSqlMiImpact 'Validate provider support and networking; non-SQL providers are not a clean MI fit.' `
        -SqlVmImpact 'Can preserve linked servers if providers and network paths are available.' `
        -EvidenceSource 'linked_servers.csv'

    $windowsLoginCount = @($SecurityPrincipalRows | Where-Object {
        (Get-ObjectText $_ 'principal_type_desc') -in @('WINDOWS_LOGIN', 'WINDOWS_GROUP')
    }).Count
    Add-RawTargetSignal `
        -Rows $signals `
        -SqlInstance $sqlInstance `
        -SignalScope 'server' `
        -SignalName 'windows_auth_principals' `
        -Detected ($windowsLoginCount -gt 0) `
        -SignalValue $windowsLoginCount `
        -AzureSqlDbImpact 'Map users to contained SQL or Microsoft Entra authentication.' `
        -AzureSqlMiImpact 'Plan Active Directory to Microsoft Entra identity mapping.' `
        -SqlVmImpact 'Can preserve Windows authentication with domain connectivity.' `
        -EvidenceSource 'security_principals.csv'

    foreach ($db in @($DatabaseRows)) {
        $databaseName = Get-ObjectText $db 'database_name'
        $compatibilityLevel = Get-ObjectInt $db 'compatibility_level'
        Add-RawTargetSignal `
            -Rows $signals `
            -SqlInstance (Get-ObjectText $db 'sql_instance' $sqlInstance) `
            -DatabaseName $databaseName `
            -SignalScope 'database' `
            -SignalName 'compatibility_level_below_100' `
            -Detected ($compatibilityLevel -gt 0 -and $compatibilityLevel -lt 100) `
            -SignalValue $compatibilityLevel `
            -AzureSqlDbImpact 'Compatibility level below 100 needs upgrade and regression testing.' `
            -AzureSqlMiImpact 'Compatibility level below 100 needs upgrade and regression testing.' `
            -SqlVmImpact 'Can preserve older compatibility only on compatible SQL Server versions.' `
            -EvidenceSource 'databases.csv'

        $logFileCount = @($DatabaseFileRows | Where-Object {
            (Get-ObjectText $_ 'database_name') -eq $databaseName -and
            (Get-ObjectText $_ 'type_desc') -eq 'LOG'
        }).Count
        Add-RawTargetSignal `
            -Rows $signals `
            -SqlInstance (Get-ObjectText $db 'sql_instance' $sqlInstance) `
            -DatabaseName $databaseName `
            -SignalScope 'database' `
            -SignalName 'multiple_log_files' `
            -Detected ($logFileCount -gt 1) `
            -SignalValue $logFileCount `
            -AzureSqlDbImpact 'Validate migration tooling and simplify log file layout where needed.' `
            -AzureSqlMiImpact 'Managed Instance supports only one log file per database for restore compatibility.' `
            -SqlVmImpact 'Can preserve multiple log files, though simplification is usually preferred.' `
            -EvidenceSource 'database_files.csv'

        foreach ($feature in @(
            @{ Name = 'filestream_filegroups'; Db = 'Not supported in Azure SQL Database.'; Mi = 'Not supported in Azure SQL Managed Instance.'; Vm = 'Use SQL VM if FILESTREAM cannot be externalized.' },
            @{ Name = 'filetables'; Db = 'Not supported in Azure SQL Database.'; Mi = 'Not supported in Azure SQL Managed Instance.'; Vm = 'Use SQL VM if FileTable cannot be externalized.' },
            @{ Name = 'cross_database_references'; Db = 'Cross-database three-part references are not a clean Azure SQL DB fit.'; Mi = 'MI can preserve colocated database patterns better.'; Vm = 'Can preserve cross-database references.' },
            @{ Name = 'sql_agent_jobsteps'; Db = 'SQL Server Agent is not available in Azure SQL Database.'; Mi = 'MI SQL Agent can preserve many T-SQL jobs; validate unsupported subsystems.'; Vm = 'Can preserve SQL Server Agent jobs.' },
            @{ Name = 'sql_agent_cmdexec_or_powershell_steps'; Db = 'OS command job steps cannot run in Azure SQL Database.'; Mi = 'Command and PowerShell job steps are not a clean MI fit.'; Vm = 'Can preserve command steps if accepted by OS/security policy.' },
            @{ Name = 'user_assemblies'; Db = 'CLR assemblies are not a clean Azure SQL Database fit.'; Mi = 'Validate CLR assemblies and permission sets.'; Vm = 'Can preserve CLR behavior on compatible SQL Server editions.' },
            @{ Name = 'service_broker'; Db = 'Service Broker is not supported in Azure SQL Database.'; Mi = 'MI is usually a better fit when broker semantics must be preserved.'; Vm = 'Can preserve Service Broker.' },
            @{ Name = 'external_tables'; Db = 'External data sources need Azure SQL target validation or redesign.'; Mi = 'Validate external table/data virtualization support.'; Vm = 'Can preserve if external dependencies remain reachable.' }
        )) {
            $value = Get-RawFeatureValue -DatabaseFeatureRows $DatabaseFeatureRows -DatabaseName $databaseName -FeatureName $feature.Name
            Add-RawTargetSignal `
                -Rows $signals `
                -SqlInstance (Get-ObjectText $db 'sql_instance' $sqlInstance) `
                -DatabaseName $databaseName `
                -SignalScope 'database' `
                -SignalName $feature.Name `
                -Detected ($value -gt 0) `
                -SignalValue $value `
                -AzureSqlDbImpact $feature.Db `
                -AzureSqlMiImpact $feature.Mi `
                -SqlVmImpact $feature.Vm `
                -EvidenceSource 'database_features.csv'
        }
    }

    foreach ($objectHit in @($ObjectFeatureScanRows)) {
        Add-RawTargetSignal `
            -Rows $signals `
            -SqlInstance (Get-ObjectText $objectHit 'sql_instance' $sqlInstance) `
            -DatabaseName (Get-ObjectText $objectHit 'database_name') `
            -SignalScope 'object' `
            -SignalName (Get-ObjectText $objectHit 'feature_name') `
            -Detected $true `
            -SignalValue "$(Get-ObjectText $objectHit 'schema_name').$(Get-ObjectText $objectHit 'object_name')" `
            -AzureSqlDbImpact (Get-ObjectText $objectHit 'azure_sql_db_impact') `
            -AzureSqlMiImpact (Get-ObjectText $objectHit 'azure_sql_mi_impact') `
            -SqlVmImpact (Get-ObjectText $objectHit 'sql_vm_impact') `
            -EvidenceSource 'object_feature_scan.csv'
    }

    foreach ($jobStep in @($SqlAgentJobStepRows | Where-Object { (Get-ObjectText $_ 'subsystem') -in @('CmdExec', 'PowerShell') })) {
        Add-RawTargetSignal `
            -Rows $signals `
            -SqlInstance (Get-ObjectText $jobStep 'sql_instance' $sqlInstance) `
            -DatabaseName (Get-ObjectText $jobStep 'database_name') `
            -SignalScope 'job_step' `
            -SignalName 'sql_agent_command_step' `
            -Detected $true `
            -SignalValue (Get-ObjectText $jobStep 'job_name') `
            -AzureSqlDbImpact 'SQL Agent command steps cannot run in Azure SQL Database.' `
            -AzureSqlMiImpact 'Command and PowerShell job steps are not a clean MI fit.' `
            -SqlVmImpact 'Can preserve command steps if accepted by OS/security policy.' `
            -EvidenceSource 'sql_agent_job_steps.csv'
    }

    return @($signals | Where-Object { (Get-ObjectText $_ 'signal_scope') -ne '__collector_state__' })
}

function New-CodexEvidencePackMarkdown {
    param(
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [Parameter(Mandatory = $true)][string]$OutputPath,
        [object[]]$ServerPropertiesRows = @(),
        [object[]]$DatabaseRows = @(),
        [object[]]$TargetSignalRows = @(),
        [object[]]$CollectionErrorRows = @(),
        [string[]]$OutputFiles = @(),
        [bool]$WorkloadSamplingEnabled = $false
    )

    $server = if (@($ServerPropertiesRows).Count -gt 0) { @($ServerPropertiesRows)[0] } else { $null }
    $detectedSignals = @($TargetSignalRows | Where-Object { (Get-ObjectText $_ 'detected') -eq 'true' })
    $lines = New-Object 'System.Collections.Generic.List[string]'

    $lines.Add('# SQL Server Azure Migration Raw Evidence Pack') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add("Generated at: $(Get-RawInventoryUtcNow)") | Out-Null
    $lines.Add("SQL instance: $SqlInstance") | Out-Null
    $lines.Add("Output path: $OutputPath") | Out-Null
    $lines.Add("Workload sampling enabled: $($WorkloadSamplingEnabled.ToString().ToLowerInvariant())") | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('## Instance Summary') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add("- SQL version: $(Get-ObjectText $server 'product_version') $(Get-ObjectText $server 'product_level') $(Get-ObjectText $server 'product_update_level')") | Out-Null
    $lines.Add("- Edition: $(Get-ObjectText $server 'edition')") | Out-Null
    $lines.Add("- CPU count: $(Get-ObjectText $server 'cpu_count')") | Out-Null
    $lines.Add("- Physical memory MB: $(Get-ObjectText $server 'physical_memory_mb')") | Out-Null
    $lines.Add("- User databases collected: $(@($DatabaseRows).Count)") | Out-Null
    $lines.Add("- Detected target-fit signals: $(@($detectedSignals).Count)") | Out-Null
    $lines.Add("- Collection errors: $(@($CollectionErrorRows).Count)") | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('## Detected Signals') | Out-Null
    $lines.Add('') | Out-Null

    if (@($detectedSignals).Count -eq 0) {
        $lines.Add('- No target-fit blockers or caution signals were detected from the collected evidence.') | Out-Null
    }
    else {
        foreach ($signal in @($detectedSignals | Sort-Object database_name, signal_scope, signal_name | Select-Object -First 50)) {
            $dbName = Get-ObjectText $signal 'database_name'
            if (Is-EmptyText $dbName) { $dbName = '<server>' }
            $lines.Add("- ${dbName}: $(Get-ObjectText $signal 'signal_name') = $(Get-ObjectText $signal 'signal_value')") | Out-Null
        }
        if (@($detectedSignals).Count -gt 50) {
            $lines.Add("- Additional signals omitted from this summary; see target_signal_matrix.csv.") | Out-Null
        }
    }

    $lines.Add('') | Out-Null
    $lines.Add('## Raw Output Files') | Out-Null
    $lines.Add('') | Out-Null
    foreach ($file in @($OutputFiles | Sort-Object)) {
        $lines.Add("- $file") | Out-Null
    }

    $lines.Add('') | Out-Null
    $lines.Add('## Notes For Codex Review') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('- Use target_signal_matrix.csv as the first-pass target-fit evidence for Azure SQL Database, Azure SQL Managed Instance, or SQL Server on Azure VM.') | Out-Null
    $lines.Add('- Use object_feature_scan.csv and sql_agent_job_steps.csv for sanitized object/job evidence. Full definitions and full command text are intentionally not exported.') | Out-Null
    $lines.Add('- Use wait_stats_snapshot.csv, io_file_stats_snapshot.csv, and workload_samples.csv if present for sizing context. A final SKU still needs representative workload history.') | Out-Null

    return ($lines -join [Environment]::NewLine)
}

function Export-RawInventoryManifest {
    param(
        [Parameter(Mandatory = $true)][string]$OutputPath,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [string]$DatabaseName = '',
        [Parameter(Mandatory = $true)][string]$StartedUtc,
        [Parameter(Mandatory = $true)][string]$CompletedUtc,
        [string[]]$OutputFiles,
        [bool]$WorkloadSamplingEnabled = $false,
        [int]$SampleIntervalSeconds = 0,
        [int]$SampleDurationSeconds = 0,
        [int]$CollectionErrorCount = 0
    )

    $manifest = [ordered]@{
        sql_instance              = $SqlInstance
        database_name             = $DatabaseName
        started_utc               = $StartedUtc
        completed_utc             = $CompletedUtc
        sanitizer                 = 'metadata-only with redacted/truncated command and module snippets'
        azure_api_calls           = $false
        workload_sampling_enabled = $WorkloadSamplingEnabled
        sample_interval_seconds   = $SampleIntervalSeconds
        sample_duration_seconds   = $SampleDurationSeconds
        collection_error_count    = $CollectionErrorCount
        output_files              = @($OutputFiles | Sort-Object)
    }

    $manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $OutputPath -Encoding UTF8
}

function Invoke-SqlServerAzureRawInventory {
    param(
        [string]$SqlInstance = '',
        [string]$InstanceListCsv = '',
        [string]$OutputRoot = './outputs',
        [string]$DatabaseName = '',
        [bool]$UseIntegratedSecurity = $true,
        [string]$SqlUsername = '',
        [securestring]$SqlPassword,
        [int]$ConnectionTimeoutSeconds = 15,
        [bool]$Encrypt = $true,
        [bool]$TrustServerCertificate = $true,
        [switch]$EnableWorkloadSampling,
        [int]$SampleIntervalSeconds = 60,
        [int]$SampleDurationSeconds = 0
    )

    Test-RawInventorySamplingOptions `
        -EnableWorkloadSampling:$EnableWorkloadSampling.IsPresent `
        -SampleIntervalSeconds $SampleIntervalSeconds `
        -SampleDurationSeconds $SampleDurationSeconds | Out-Null

    $targets = Resolve-RawInventoryTargets `
        -SqlInstance $SqlInstance `
        -InstanceListCsv $InstanceListCsv `
        -DatabaseName $DatabaseName

    $resolvedOutputRoot = Resolve-RawInventoryOutputRoot -OutputRoot $OutputRoot
    $results = New-Object 'System.Collections.Generic.List[object]'

    foreach ($target in @($targets)) {
        $targetInstance = Get-ObjectText $target 'sql_instance'
        $targetDatabase = Get-ObjectText $target 'database_name'
        $startedUtc = Get-RawInventoryUtcNow
        $runStamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
        $instanceOutputRoot = Join-Path $resolvedOutputRoot "$(ConvertTo-RawInventorySafeName -Value $targetInstance)-raw-inventory"
        $runOutputRoot = Join-Path $instanceOutputRoot $runStamp
        Ensure-Directory -Path $runOutputRoot

        Write-Host "Collecting raw migration inventory from $targetInstance..."

        $connectionString = New-AssessmentConnectionString `
            -SqlInstance $targetInstance `
            -UseIntegratedSecurity $UseIntegratedSecurity `
            -SqlUsername $SqlUsername `
            -SqlPassword $SqlPassword `
            -ConnectionTimeoutSeconds $ConnectionTimeoutSeconds `
            -Encrypt $Encrypt `
            -TrustServerCertificate $TrustServerCertificate

        $errorRows = New-RawCollectionErrorList

        $serverPropertiesRows = @(Get-RawServerPropertiesRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $serverConfigurationRows = @(Get-RawServerConfigurationsRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $databaseRows = @(Get-RawDatabasesRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseName $targetDatabase)
        $databaseFileRows = @(Get-RawDatabaseFilesRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseName $targetDatabase)
        $databaseFeatureRows = @(Get-RawDatabaseFeaturesRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseName $targetDatabase)
        $objectFeatureRows = @(Get-RawObjectFeatureScanRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseRows $databaseRows)
        $databaseDependencyRows = @(Get-RawDatabaseDependencyRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseRows $databaseRows)
        $sqlAgentJobRows = @(Get-RawSqlAgentJobsRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $sqlAgentJobStepRows = @(Get-RawSqlAgentJobStepsRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $linkedServerRows = @(Get-RawLinkedServersRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $haDrRows = @(Get-RawHaDrTopologyRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $securityPrincipalRows = @(Get-RawSecurityPrincipalsRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseRows $databaseRows)
        $queryStoreRows = @(Get-RawQueryStoreSummaryRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseRows $databaseRows)
        $waitStatsRows = @(Get-RawWaitStatsSnapshotRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows)
        $ioFileStatsRows = @(Get-RawIoFileStatsSnapshotRows -ConnectionString $connectionString -SqlInstance $targetInstance -ErrorRows $errorRows -DatabaseName $targetDatabase)
        $workloadSampleRows = @(Get-RawWorkloadSampleRows `
            -ConnectionString $connectionString `
            -SqlInstance $targetInstance `
            -ErrorRows $errorRows `
            -EnableWorkloadSampling:$EnableWorkloadSampling.IsPresent `
            -SampleIntervalSeconds $SampleIntervalSeconds `
            -SampleDurationSeconds $SampleDurationSeconds)

        $visibleErrorRows = @(Get-VisibleRawCollectionErrorRows -Rows @($errorRows))

        $targetSignalRows = @(New-TargetSignalRows `
            -ServerPropertiesRows $serverPropertiesRows `
            -ServerConfigurationRows $serverConfigurationRows `
            -DatabaseRows $databaseRows `
            -DatabaseFileRows $databaseFileRows `
            -DatabaseFeatureRows $databaseFeatureRows `
            -ObjectFeatureScanRows $objectFeatureRows `
            -SqlAgentJobStepRows $sqlAgentJobStepRows `
            -LinkedServerRows $linkedServerRows `
            -SecurityPrincipalRows $securityPrincipalRows)

        $rowSets = [ordered]@{
            'server_properties.csv'    = $serverPropertiesRows
            'server_configurations.csv' = $serverConfigurationRows
            'databases.csv'            = $databaseRows
            'database_files.csv'       = $databaseFileRows
            'database_features.csv'    = $databaseFeatureRows
            'object_feature_scan.csv'  = $objectFeatureRows
            'database_dependencies.csv' = $databaseDependencyRows
            'sql_agent_jobs.csv'       = $sqlAgentJobRows
            'sql_agent_job_steps.csv'  = $sqlAgentJobStepRows
            'linked_servers.csv'       = $linkedServerRows
            'ha_dr_topology.csv'       = $haDrRows
            'security_principals.csv'  = $securityPrincipalRows
            'query_store_summary.csv'  = $queryStoreRows
            'wait_stats_snapshot.csv'  = $waitStatsRows
            'io_file_stats_snapshot.csv' = $ioFileStatsRows
            'target_signal_matrix.csv' = $targetSignalRows
            'collection_errors.csv'    = $visibleErrorRows
        }

        $outputFiles = New-Object 'System.Collections.Generic.List[string]'
        foreach ($fileName in $rowSets.Keys) {
            Export-RawInventoryCsv `
                -Rows @($rowSets[$fileName]) `
                -Columns $script:RawInventoryCsvContracts[$fileName] `
                -OutputPath (Join-Path $runOutputRoot $fileName)
            $outputFiles.Add($fileName) | Out-Null
        }

        if ($EnableWorkloadSampling.IsPresent) {
            Export-RawInventoryCsv `
                -Rows $workloadSampleRows `
                -Columns $script:RawWorkloadSamplesColumns `
                -OutputPath (Join-Path $runOutputRoot 'workload_samples.csv')
            $outputFiles.Add('workload_samples.csv') | Out-Null
        }

        $completedUtc = Get-RawInventoryUtcNow
        Export-RawInventoryManifest `
            -OutputPath (Join-Path $runOutputRoot 'assessment_manifest.json') `
            -SqlInstance $targetInstance `
            -DatabaseName $targetDatabase `
            -StartedUtc $startedUtc `
            -CompletedUtc $completedUtc `
            -OutputFiles @(@($outputFiles) + @('assessment_manifest.json', 'codex_evidence_pack.md')) `
            -WorkloadSamplingEnabled $EnableWorkloadSampling.IsPresent `
            -SampleIntervalSeconds $SampleIntervalSeconds `
            -SampleDurationSeconds $SampleDurationSeconds `
            -CollectionErrorCount @($visibleErrorRows).Count
        $outputFiles.Add('assessment_manifest.json') | Out-Null

        $evidencePack = New-CodexEvidencePackMarkdown `
            -SqlInstance $targetInstance `
            -OutputPath $runOutputRoot `
            -ServerPropertiesRows $serverPropertiesRows `
            -DatabaseRows $databaseRows `
            -TargetSignalRows $targetSignalRows `
            -CollectionErrorRows $visibleErrorRows `
            -OutputFiles @(@($outputFiles) + @('codex_evidence_pack.md')) `
            -WorkloadSamplingEnabled $EnableWorkloadSampling.IsPresent
        Set-Content -LiteralPath (Join-Path $runOutputRoot 'codex_evidence_pack.md') -Value $evidencePack -Encoding UTF8
        $outputFiles.Add('codex_evidence_pack.md') | Out-Null

        $results.Add([PSCustomObject]@{
            sql_instance           = $targetInstance
            database_name          = $targetDatabase
            output_root            = $runOutputRoot
            database_count         = @($databaseRows).Count
            detected_signal_count  = @($targetSignalRows | Where-Object { (Get-ObjectText $_ 'detected') -eq 'true' }).Count
            collection_error_count = @($visibleErrorRows).Count
        }) | Out-Null

        Write-Host "Raw migration inventory complete for $targetInstance. Output root: $runOutputRoot"
    }

    return @($results)
}
