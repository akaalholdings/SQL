Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'Common.ps1')

try {
    Add-Type -AssemblyName System.Data -ErrorAction Stop
}
catch {
    # PowerShell 7 can load these types lazily; connection creation will fail clearly if SqlClient is unavailable.
}

$script:ServerDesignColumns = @(
    'sql_instance',
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
    'max_server_memory_mb',
    'tempdb_total_size_mb',
    'tempdb_data_file_count',
    'tempdb_log_file_count',
    'linked_server_count',
    'sql_agent_job_count',
    'sql_agent_cmdexec_step_count',
    'database_mail_profile_count',
    'credential_count',
    'endpoint_count',
    'server_trigger_count',
    'availability_group_count',
    'resource_governor_user_pool_count',
    'trace_flag_count',
    'xp_cmdshell_enabled',
    'clr_enabled',
    'external_scripts_enabled',
    'polybase_enabled',
    'collection_notes'
)

$script:ClusterDesignColumns = @(
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

$script:DatabaseDesignColumns = @(
    'sql_instance',
    'database_name',
    'state_desc',
    'compatibility_level',
    'recovery_model_desc',
    'collation_name',
    'total_size_gb',
    'data_size_gb',
    'log_size_gb',
    'data_file_count',
    'log_file_count',
    'filegroup_count',
    'filestream_filegroup_count',
    'memory_optimized_table_count',
    'filetable_count',
    'external_table_count',
    'fulltext_catalog_count',
    'partition_scheme_count',
    'user_assembly_count',
    'synonym_count',
    'cross_database_reference_count',
    'service_broker_enabled',
    'cdc_enabled',
    'change_tracking_enabled',
    'tde_enabled',
    'query_store_state',
    'largest_table',
    'largest_table_mb',
    'sql_agent_jobstep_count',
    'sql_agent_cmdexec_step_count',
    'collection_notes'
)

$script:FeatureUsageColumns = @(
    'sql_instance',
    'database_name',
    'scope',
    'feature_name',
    'detected',
    'evidence',
    'azure_sql_db_impact',
    'azure_sql_mi_impact',
    'sql_vm_impact',
    'collection_error'
)

$script:RecommendationColumns = @(
    'sql_instance',
    'database_name',
    'recommended_target',
    'service_tier',
    'compute_band',
    'storage_band',
    'sizing_confidence',
    'confidence',
    'paas_blockers',
    'mi_blockers',
    'vm_reason',
    'remediation_required',
    'migration_route_hint',
    'evidence_summary'
)

$script:RemediationColumns = @(
    'sql_instance',
    'database_name',
    'recommended_target',
    'blocker_type',
    'blocker',
    'why_it_matters',
    'required_action',
    'priority'
)

function Get-ObjectValue {
    param(
        [object]$InputObject,
        [Parameter(Mandatory = $true)][string]$PropertyName,
        [object]$Default = ''
    )

    if ($null -eq $InputObject) { return $Default }

    $property = $InputObject.PSObject.Properties[$PropertyName]
    if ($null -eq $property -or $null -eq $property.Value) { return $Default }

    return $property.Value
}

function Get-ObjectText {
    param(
        [object]$InputObject,
        [Parameter(Mandatory = $true)][string]$PropertyName,
        [string]$Default = ''
    )

    return Get-Text (Get-ObjectValue -InputObject $InputObject -PropertyName $PropertyName -Default $Default)
}

function Get-ObjectInt {
    param(
        [object]$InputObject,
        [Parameter(Mandatory = $true)][string]$PropertyName,
        [int]$Default = 0
    )

    return To-Int (Get-ObjectValue -InputObject $InputObject -PropertyName $PropertyName -Default $Default) -Default $Default
}

function Get-ObjectDouble {
    param(
        [object]$InputObject,
        [Parameter(Mandatory = $true)][string]$PropertyName,
        [double]$Default = 0.0
    )

    return To-Double (Get-ObjectValue -InputObject $InputObject -PropertyName $PropertyName -Default $Default) -Default $Default
}

function Test-ObjectBool {
    param(
        [object]$InputObject,
        [Parameter(Mandatory = $true)][string]$PropertyName
    )

    return To-Bool (Get-ObjectValue -InputObject $InputObject -PropertyName $PropertyName -Default $false)
}

function Add-UniqueText {
    param(
        [Parameter(Mandatory = $true)][System.Collections.Generic.List[string]]$List,
        [string]$Value
    )

    $text = Get-Text $Value
    if ($text -ne '' -and -not $List.Contains($text)) {
        $List.Add($text) | Out-Null
    }
}

function Join-AssessmentList {
    param(
        [System.Collections.Generic.List[string]]$Values,
        [string]$Default = 'None'
    )

    if ($null -eq $Values -or $Values.Count -eq 0) { return $Default }
    return ($Values | Where-Object { -not (Is-EmptyText $_) } | Select-Object -Unique) -join '; '
}

function Quote-SqlLiteral {
    param([object]$Value)
    if ($null -eq $Value) { return 'NULL' }

    $escaped = $Value.ToString().Replace("'", "''")
    return "N'$escaped'"
}

function ConvertFrom-SecureStringToPlainText {
    param([Parameter(Mandatory = $true)][securestring]$SecureString)

    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureString)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        if ($bstr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        }
    }
}

function New-AssessmentConnectionString {
    param(
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [bool]$UseIntegratedSecurity = $true,
        [string]$SqlUsername = '',
        [securestring]$SqlPassword,
        [int]$ConnectionTimeoutSeconds = 15,
        [bool]$Encrypt = $true,
        [bool]$TrustServerCertificate = $true
    )

    $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
    $builder['Data Source'] = $SqlInstance
    $builder['Initial Catalog'] = 'master'
    $builder['Application Name'] = 'Peabody Azure SQL Migration Assessment'
    $builder['Connect Timeout'] = $ConnectionTimeoutSeconds
    $builder['Encrypt'] = $Encrypt
    $builder['TrustServerCertificate'] = $TrustServerCertificate

    if ($UseIntegratedSecurity) {
        $builder['Integrated Security'] = $true
    }
    else {
        if (Is-EmptyText $SqlUsername -or $null -eq $SqlPassword) {
            throw 'SqlUsername and SqlPassword are required when UseIntegratedSecurity is false.'
        }

        $builder['User ID'] = $SqlUsername
        $builder['Password'] = ConvertFrom-SecureStringToPlainText -SecureString $SqlPassword
    }

    return $builder.ConnectionString
}

function Invoke-AssessmentSqlQuery {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$Query,
        [string]$Database = 'master',
        [int]$CommandTimeoutSeconds = 120
    )

    $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder $ConnectionString
    $builder['Initial Catalog'] = $Database

    $connection = New-Object System.Data.SqlClient.SqlConnection $builder.ConnectionString
    $command = $connection.CreateCommand()
    $command.CommandText = $Query
    $command.CommandTimeout = $CommandTimeoutSeconds

    $table = New-Object System.Data.DataTable
    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command

    try {
        $connection.Open()
        [void]$adapter.Fill($table)
    }
    finally {
        $connection.Dispose()
        $command.Dispose()
        $adapter.Dispose()
    }

    $rows = @()
    foreach ($dataRow in $table.Rows) {
        $values = [ordered]@{}
        foreach ($column in $table.Columns) {
            if ($dataRow.IsNull($column)) {
                $values[$column.ColumnName] = $null
            }
            else {
                $values[$column.ColumnName] = $dataRow[$column]
            }
        }

        $rows += [PSCustomObject]$values
    }

    return @($rows)
}

function Invoke-OptionalAssessmentSqlQuery {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$Query,
        [string]$Database = 'master',
        [string]$CollectorName = 'collector',
        [int]$CommandTimeoutSeconds = 120
    )

    try {
        $rows = Invoke-AssessmentSqlQuery `
            -ConnectionString $ConnectionString `
            -Query $Query `
            -Database $Database `
            -CommandTimeoutSeconds $CommandTimeoutSeconds

        return [PSCustomObject]@{
            Rows  = @($rows)
            Error = ''
        }
    }
    catch {
        return [PSCustomObject]@{
            Rows  = @()
            Error = "$CollectorName failed: $($_.Exception.Message)"
        }
    }
}

function Get-SingleValueCount {
    param(
        [object[]]$Rows,
        [string]$PropertyName = 'item_count'
    )

    if ($null -eq $Rows -or @($Rows).Count -eq 0) { return 0 }
    return Get-ObjectInt -InputObject @($Rows)[0] -PropertyName $PropertyName
}

function Get-SqlServerDesignEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance
    )

    $notes = New-Object 'System.Collections.Generic.List[string]'

    $serverPropertiesQuery = @"
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
    CONVERT(int, SERVERPROPERTY('IsHadrEnabled')) AS is_hadr_enabled;
"@

    $serverProperties = Invoke-AssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -Query $serverPropertiesQuery
    if (@($serverProperties).Count -eq 0) {
        throw 'Could not collect SQL Server SERVERPROPERTY evidence.'
    }

    $base = @($serverProperties)[0]

    $osResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'os_sys_info' -Query @"
SELECT
    cpu_count,
    scheduler_count,
    CONVERT(decimal(18, 2), physical_memory_kb / 1024.0) AS physical_memory_mb
FROM sys.dm_os_sys_info;
"@
    if (-not (Is-EmptyText $osResult.Error)) { Add-UniqueText -List $notes -Value $osResult.Error }
    $os = if (@($osResult.Rows).Count -gt 0) { @($osResult.Rows)[0] } else { $null }

    $configResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'sys_configurations' -Query @"
SELECT name, value_in_use
FROM sys.configurations
WHERE name IN (
    N'max server memory (MB)',
    N'xp_cmdshell',
    N'clr enabled',
    N'external scripts enabled',
    N'polybase enabled'
);
"@
    if (-not (Is-EmptyText $configResult.Error)) { Add-UniqueText -List $notes -Value $configResult.Error }

    $configMap = @{}
    foreach ($row in @($configResult.Rows)) {
        $configMap[(Get-ObjectText $row 'name').ToLowerInvariant()] = Get-ObjectInt $row 'value_in_use'
    }

    $tempdbResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'tempdb_files' -Query @"
SELECT
    CONVERT(decimal(18, 2), SUM(size) * 8.0 / 1024.0) AS tempdb_total_size_mb,
    SUM(CASE WHEN type_desc = N'ROWS' THEN 1 ELSE 0 END) AS tempdb_data_file_count,
    SUM(CASE WHEN type_desc = N'LOG' THEN 1 ELSE 0 END) AS tempdb_log_file_count
FROM sys.master_files
WHERE database_id = 2;
"@
    if (-not (Is-EmptyText $tempdbResult.Error)) { Add-UniqueText -List $notes -Value $tempdbResult.Error }
    $tempdb = if (@($tempdbResult.Rows).Count -gt 0) { @($tempdbResult.Rows)[0] } else { $null }

    $linkedServers = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'linked_servers' -Query "SELECT COUNT(*) AS item_count FROM sys.servers WHERE is_linked = 1;"
    $agentJobs = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'msdb' -CollectorName 'sql_agent_jobs' -Query "SELECT COUNT(*) AS item_count FROM dbo.sysjobs;"
    $agentCommandSteps = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'msdb' -CollectorName 'sql_agent_command_steps' -Query "SELECT COUNT(*) AS item_count FROM dbo.sysjobsteps WHERE subsystem IN (N'CmdExec', N'PowerShell');"
    $mailProfiles = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'msdb' -CollectorName 'database_mail_profiles' -Query "SELECT COUNT(*) AS item_count FROM dbo.sysmail_profile;"
    $credentials = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'credentials' -Query "SELECT COUNT(*) AS item_count FROM sys.credentials;"
    $endpoints = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'endpoints' -Query "SELECT COUNT(*) AS item_count FROM sys.endpoints WHERE type_desc <> N'SERVICE_BROKER';"
    $triggers = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'server_triggers' -Query "SELECT COUNT(*) AS item_count FROM sys.server_triggers WHERE is_disabled = 0;"
    $availabilityGroups = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'availability_groups' -Query "SELECT COUNT(*) AS item_count FROM sys.availability_groups;"
    $resourceGovernor = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'resource_governor' -Query "SELECT COUNT(*) AS item_count FROM sys.resource_governor_resource_pools WHERE pool_id > 2;"
    $traceFlags = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'trace_flags' -Query "DBCC TRACESTATUS(-1) WITH NO_INFOMSGS;"

    foreach ($result in @($linkedServers, $agentJobs, $agentCommandSteps, $mailProfiles, $credentials, $endpoints, $triggers, $availabilityGroups, $resourceGovernor, $traceFlags)) {
        if (-not (Is-EmptyText $result.Error)) {
            Add-UniqueText -List $notes -Value $result.Error
        }
    }

    return @([PSCustomObject]@{
        sql_instance                      = $SqlInstance
        machine_name                      = Get-ObjectText $base 'machine_name'
        server_name                       = Get-ObjectText $base 'server_name'
        instance_name                     = Get-ObjectText $base 'instance_name'
        edition                           = Get-ObjectText $base 'edition'
        product_version                   = Get-ObjectText $base 'product_version'
        product_level                     = Get-ObjectText $base 'product_level'
        product_update_level              = Get-ObjectText $base 'product_update_level'
        engine_edition                    = Get-ObjectInt $base 'engine_edition'
        server_collation                  = Get-ObjectText $base 'server_collation'
        is_clustered                      = Get-ObjectInt $base 'is_clustered'
        is_hadr_enabled                   = Get-ObjectInt $base 'is_hadr_enabled'
        cpu_count                         = Get-ObjectInt $os 'cpu_count'
        scheduler_count                   = Get-ObjectInt $os 'scheduler_count'
        physical_memory_mb                = Get-ObjectDouble $os 'physical_memory_mb'
        max_server_memory_mb              = if ($configMap.ContainsKey('max server memory (mb)')) { $configMap['max server memory (mb)'] } else { 0 }
        tempdb_total_size_mb              = Get-ObjectDouble $tempdb 'tempdb_total_size_mb'
        tempdb_data_file_count            = Get-ObjectInt $tempdb 'tempdb_data_file_count'
        tempdb_log_file_count             = Get-ObjectInt $tempdb 'tempdb_log_file_count'
        linked_server_count               = Get-SingleValueCount -Rows $linkedServers.Rows
        sql_agent_job_count               = Get-SingleValueCount -Rows $agentJobs.Rows
        sql_agent_cmdexec_step_count      = Get-SingleValueCount -Rows $agentCommandSteps.Rows
        database_mail_profile_count       = Get-SingleValueCount -Rows $mailProfiles.Rows
        credential_count                  = Get-SingleValueCount -Rows $credentials.Rows
        endpoint_count                    = Get-SingleValueCount -Rows $endpoints.Rows
        server_trigger_count              = Get-SingleValueCount -Rows $triggers.Rows
        availability_group_count          = Get-SingleValueCount -Rows $availabilityGroups.Rows
        resource_governor_user_pool_count = Get-SingleValueCount -Rows $resourceGovernor.Rows
        trace_flag_count                  = @($traceFlags.Rows).Count
        xp_cmdshell_enabled               = if ($configMap.ContainsKey('xp_cmdshell')) { $configMap['xp_cmdshell'] } else { 0 }
        clr_enabled                       = if ($configMap.ContainsKey('clr enabled')) { $configMap['clr enabled'] } else { 0 }
        external_scripts_enabled          = if ($configMap.ContainsKey('external scripts enabled')) { $configMap['external scripts enabled'] } else { 0 }
        polybase_enabled                  = if ($configMap.ContainsKey('polybase enabled')) { $configMap['polybase enabled'] } else { 0 }
        collection_notes                  = Join-AssessmentList -Values $notes -Default 'None'
    })
}

function Get-ClusterDesignEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance
    )

    $rows = New-Object 'System.Collections.Generic.List[object]'

    $serverHaResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'server_ha_properties' -Query @"
SELECT
    CONVERT(int, SERVERPROPERTY('IsClustered')) AS is_clustered,
    CONVERT(int, SERVERPROPERTY('IsHadrEnabled')) AS is_hadr_enabled;
"@

    if (@($serverHaResult.Rows).Count -gt 0) {
        $ha = @($serverHaResult.Rows)[0]
        $evidence = "IsClustered=$(Get-ObjectInt $ha 'is_clustered'); IsHadrEnabled=$(Get-ObjectInt $ha 'is_hadr_enabled')"
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'server_properties'
            component_name    = 'SQL Server HA properties'
            replica_server_name = ''
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = $evidence
            paas_impact       = 'Built-in Azure SQL HA replaces SQL Server cluster control plane.'
            mi_impact         = 'Built-in MI HA replaces SQL Server cluster control plane; validate listener and failover expectations.'
            vm_impact         = 'SQL VM can preserve WSFC/AG-style architecture if required.'
            collection_error  = ''
        }) | Out-Null
    }
    elseif (-not (Is-EmptyText $serverHaResult.Error)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'server_properties'
            component_name    = 'SQL Server HA properties'
            replica_server_name = ''
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = ''
            paas_impact       = ''
            mi_impact         = ''
            vm_impact         = ''
            collection_error  = $serverHaResult.Error
        }) | Out-Null
    }

    $clusterNodeResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'cluster_nodes' -Query @"
SELECT
    CONVERT(nvarchar(128), NodeName) AS replica_server_name,
    CONVERT(nvarchar(60), status_description) AS health_desc
FROM sys.dm_os_cluster_nodes;
"@
    foreach ($node in @($clusterNodeResult.Rows)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'cluster_node'
            component_name    = 'WSFC node'
            replica_server_name = Get-ObjectText $node 'replica_server_name'
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = Get-ObjectText $node 'health_desc'
            evidence          = 'Source SQL Server is installed on a Windows failover cluster node.'
            paas_impact       = 'Cluster node topology is replaced by Azure SQL platform HA.'
            mi_impact         = 'Cluster node topology is replaced by MI built-in HA.'
            vm_impact         = 'Can preserve cluster-node architecture with SQL Server on Azure VMs if required.'
            collection_error  = ''
        }) | Out-Null
    }

    if (-not (Is-EmptyText $clusterNodeResult.Error) -and (Get-ObjectInt -InputObject (@($serverHaResult.Rows)[0]) -PropertyName 'is_clustered') -gt 0) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'cluster_node'
            component_name    = 'WSFC node'
            replica_server_name = ''
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = ''
            paas_impact       = ''
            mi_impact         = ''
            vm_impact         = ''
            collection_error  = $clusterNodeResult.Error
        }) | Out-Null
    }

    $agResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'availability_replicas' -Query @"
SELECT
    CONVERT(nvarchar(128), ag.name) AS component_name,
    CONVERT(nvarchar(128), ar.replica_server_name) AS replica_server_name,
    CONVERT(nvarchar(60), ar.availability_mode_desc) AS availability_mode,
    CONVERT(nvarchar(60), ar.failover_mode_desc) AS failover_mode,
    CONVERT(nvarchar(256), agl.dns_name) AS listener_name
FROM sys.availability_groups ag
LEFT JOIN sys.availability_replicas ar
    ON ag.group_id = ar.group_id
LEFT JOIN sys.availability_group_listeners agl
    ON ag.group_id = agl.group_id;
"@
    if (-not (Is-EmptyText $agResult.Error)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'availability_group'
            component_name    = ''
            replica_server_name = ''
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = ''
            paas_impact       = ''
            mi_impact         = ''
            vm_impact         = ''
            collection_error  = $agResult.Error
        }) | Out-Null
    }
    foreach ($ag in @($agResult.Rows)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'availability_group'
            component_name    = Get-ObjectText $ag 'component_name'
            replica_server_name = Get-ObjectText $ag 'replica_server_name'
            availability_mode = Get-ObjectText $ag 'availability_mode'
            failover_mode     = Get-ObjectText $ag 'failover_mode'
            listener_name     = Get-ObjectText $ag 'listener_name'
            role_desc         = ''
            health_desc       = ''
            evidence          = 'Source uses Always On Availability Groups.'
            paas_impact       = 'Do not lift AG control plane; use platform HA and validate connection/listener behavior.'
            mi_impact         = 'Do not lift AG control plane; use built-in MI HA or MI link where appropriate.'
            vm_impact         = 'Can preserve AG architecture on SQL Server VMs if cluster-level control is required.'
            collection_error  = ''
        }) | Out-Null
    }

    $agDbResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'availability_group_databases' -Query @"
SELECT
    CONVERT(nvarchar(128), DB_NAME(drs.database_id)) AS database_name,
    CONVERT(nvarchar(128), ag.name) AS component_name,
    CONVERT(nvarchar(128), ar.replica_server_name) AS replica_server_name,
    CONVERT(nvarchar(60), drs.synchronization_state_desc) AS role_desc,
    CONVERT(nvarchar(60), drs.synchronization_health_desc) AS health_desc
FROM sys.dm_hadr_database_replica_states drs
LEFT JOIN sys.availability_replicas ar
    ON drs.replica_id = ar.replica_id
LEFT JOIN sys.availability_groups ag
    ON ar.group_id = ag.group_id;
"@
    if (-not (Is-EmptyText $agDbResult.Error)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'availability_group_database'
            component_name    = ''
            replica_server_name = ''
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = ''
            paas_impact       = ''
            mi_impact         = ''
            vm_impact         = ''
            collection_error  = $agDbResult.Error
        }) | Out-Null
    }
    foreach ($agDb in @($agDbResult.Rows)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = Get-ObjectText $agDb 'database_name'
            ha_dr_component   = 'availability_group_database'
            component_name    = Get-ObjectText $agDb 'component_name'
            replica_server_name = Get-ObjectText $agDb 'replica_server_name'
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = Get-ObjectText $agDb 'role_desc'
            health_desc       = Get-ObjectText $agDb 'health_desc'
            evidence          = 'Database participates in Always On Availability Group.'
            paas_impact       = 'Replace with Azure SQL platform HA and test failover behavior.'
            mi_impact         = 'Replace with MI built-in HA or use MI link during migration.'
            vm_impact         = 'Can preserve AG participation if SQL VM is selected.'
            collection_error  = ''
        }) | Out-Null
    }

    $logShippingResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'msdb' -CollectorName 'log_shipping' -Query @"
SELECT
    CONVERT(nvarchar(128), primary_database) AS database_name,
    CONVERT(nvarchar(128), secondary_server) AS replica_server_name,
    CONVERT(nvarchar(128), secondary_database) AS component_name
FROM dbo.log_shipping_primary_secondaries;
"@
    foreach ($ls in @($logShippingResult.Rows)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = Get-ObjectText $ls 'database_name'
            ha_dr_component   = 'log_shipping'
            component_name    = Get-ObjectText $ls 'component_name'
            replica_server_name = Get-ObjectText $ls 'replica_server_name'
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = 'Source uses log shipping.'
            paas_impact       = 'Replace with DMS, backup/restore, or platform PITR/geo-restore patterns.'
            mi_impact         = 'Replace with DMS, backup/restore, MI link, or platform PITR/geo-restore patterns.'
            vm_impact         = 'Can preserve log shipping if operationally required.'
            collection_error  = ''
        }) | Out-Null
    }

    $mirroringResult = Invoke-OptionalAssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -CollectorName 'database_mirroring' -Query @"
SELECT
    CONVERT(nvarchar(128), DB_NAME(database_id)) AS database_name,
    CONVERT(nvarchar(60), mirroring_role_desc) AS role_desc,
    CONVERT(nvarchar(60), mirroring_state_desc) AS health_desc,
    CONVERT(nvarchar(128), mirroring_partner_instance) AS replica_server_name
FROM sys.database_mirroring
WHERE mirroring_guid IS NOT NULL;
"@
    foreach ($mirror in @($mirroringResult.Rows)) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = Get-ObjectText $mirror 'database_name'
            ha_dr_component   = 'database_mirroring'
            component_name    = 'Database mirroring'
            replica_server_name = Get-ObjectText $mirror 'replica_server_name'
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = Get-ObjectText $mirror 'role_desc'
            health_desc       = Get-ObjectText $mirror 'health_desc'
            evidence          = 'Source uses database mirroring.'
            paas_impact       = 'Database mirroring is not lifted to Azure SQL DB; replace with platform HA and migration tooling.'
            mi_impact         = 'Database mirroring is not lifted to MI; replace with platform HA and migration tooling.'
            vm_impact         = 'Can preserve SQL Server-level HA pattern only on SQL VM, though AG is usually preferred.'
            collection_error  = ''
        }) | Out-Null
    }

    if ($rows.Count -eq 0) {
        $rows.Add([PSCustomObject]@{
            sql_instance      = $SqlInstance
            database_name     = ''
            ha_dr_component   = 'none_detected'
            component_name    = 'No HA/DR evidence collected'
            replica_server_name = ''
            availability_mode = ''
            failover_mode     = ''
            listener_name     = ''
            role_desc         = ''
            health_desc       = ''
            evidence          = 'No cluster, AG, log shipping, or mirroring rows were collected.'
            paas_impact       = 'No source HA control plane to preserve.'
            mi_impact         = 'No source HA control plane to preserve.'
            vm_impact         = 'No source HA control plane to preserve.'
            collection_error  = ''
        }) | Out-Null
    }

    return @($rows)
}

function Get-DatabaseFeatureFacts {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$DatabaseName
    )

    $query = @"
DECLARE @filegroup_count int = 0,
        @filestream_filegroup_count int = 0,
        @memory_optimized_table_count int = 0,
        @filetable_count int = 0,
        @external_table_count int = 0,
        @fulltext_catalog_count int = 0,
        @partition_scheme_count int = 0,
        @user_assembly_count int = 0,
        @synonym_count int = 0,
        @cross_database_reference_count int = 0,
        @change_tracking_enabled int = 0,
        @query_store_state nvarchar(60) = N'Unavailable',
        @largest_table nvarchar(300) = N'',
        @largest_table_mb decimal(18, 2) = 0;

SELECT @filegroup_count = COUNT(*) FROM sys.filegroups;
SELECT @filestream_filegroup_count = COUNT(*) FROM sys.filegroups WHERE type = N'FD';

IF COL_LENGTH(N'sys.tables', N'is_memory_optimized') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @value = COUNT(*) FROM sys.tables WHERE is_memory_optimized = 1',
        N'@value int OUTPUT',
        @value = @memory_optimized_table_count OUTPUT;

IF COL_LENGTH(N'sys.tables', N'is_filetable') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @value = COUNT(*) FROM sys.tables WHERE is_filetable = 1',
        N'@value int OUTPUT',
        @value = @filetable_count OUTPUT;

IF OBJECT_ID(N'sys.external_tables') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @value = COUNT(*) FROM sys.external_tables',
        N'@value int OUTPUT',
        @value = @external_table_count OUTPUT;

IF OBJECT_ID(N'sys.fulltext_catalogs') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @value = COUNT(*) FROM sys.fulltext_catalogs',
        N'@value int OUTPUT',
        @value = @fulltext_catalog_count OUTPUT;

SELECT @partition_scheme_count = COUNT(*) FROM sys.partition_schemes;
SELECT @user_assembly_count = COUNT(*) FROM sys.assemblies WHERE is_user_defined = 1;
SELECT @synonym_count = COUNT(*) FROM sys.synonyms;
SELECT @cross_database_reference_count = COUNT(DISTINCT referenced_database_name)
FROM sys.sql_expression_dependencies
WHERE referenced_database_name IS NOT NULL
  AND referenced_database_name <> DB_NAME();

IF EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
    SET @change_tracking_enabled = 1;

IF OBJECT_ID(N'sys.database_query_store_options') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @value = CONVERT(nvarchar(60), actual_state_desc) FROM sys.database_query_store_options',
        N'@value nvarchar(60) OUTPUT',
        @value = @query_store_state OUTPUT;

SELECT TOP (1)
    @largest_table = QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name),
    @largest_table_mb = CONVERT(decimal(18, 2), SUM(a.total_pages) * 8.0 / 1024.0)
FROM sys.tables t
JOIN sys.indexes i
    ON t.object_id = i.object_id
JOIN sys.partitions p
    ON i.object_id = p.object_id
   AND i.index_id = p.index_id
JOIN sys.allocation_units a
    ON p.partition_id = a.container_id
WHERE t.is_ms_shipped = 0
GROUP BY t.schema_id, t.name
ORDER BY SUM(a.total_pages) DESC;

SELECT
    @filegroup_count AS filegroup_count,
    @filestream_filegroup_count AS filestream_filegroup_count,
    @memory_optimized_table_count AS memory_optimized_table_count,
    @filetable_count AS filetable_count,
    @external_table_count AS external_table_count,
    @fulltext_catalog_count AS fulltext_catalog_count,
    @partition_scheme_count AS partition_scheme_count,
    @user_assembly_count AS user_assembly_count,
    @synonym_count AS synonym_count,
    @cross_database_reference_count AS cross_database_reference_count,
    @change_tracking_enabled AS change_tracking_enabled,
    @query_store_state AS query_store_state,
    @largest_table AS largest_table,
    @largest_table_mb AS largest_table_mb;
"@

    $result = Invoke-OptionalAssessmentSqlQuery `
        -ConnectionString $ConnectionString `
        -Database $DatabaseName `
        -CollectorName "database_features:$DatabaseName" `
        -Query $query

    return $result
}

function Get-AgentJobStepEvidenceForDatabase {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$DatabaseName
    )

    $databaseLiteral = Quote-SqlLiteral -Value $DatabaseName
    $query = @"
DECLARE @DatabaseName sysname = $databaseLiteral;

SELECT
    COUNT(*) AS sql_agent_jobstep_count,
    SUM(CASE WHEN subsystem IN (N'CmdExec', N'PowerShell') THEN 1 ELSE 0 END) AS sql_agent_cmdexec_step_count
FROM dbo.sysjobsteps
WHERE database_name = @DatabaseName
   OR command LIKE N'%' + @DatabaseName + N'%';
"@

    return Invoke-OptionalAssessmentSqlQuery `
        -ConnectionString $ConnectionString `
        -Database 'msdb' `
        -CollectorName "sql_agent_jobsteps:$DatabaseName" `
        -Query $query
}

function Get-DatabaseDesignEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$SqlInstance,
        [string]$DatabaseName = ''
    )

    $databaseFilter = if (Is-EmptyText $DatabaseName) { 'NULL' } else { Quote-SqlLiteral -Value $DatabaseName }
    $databaseListQuery = @"
DECLARE @DatabaseName sysname = $databaseFilter;

SELECT
    CONVERT(nvarchar(128), d.name) AS database_name,
    CONVERT(nvarchar(60), d.state_desc) AS state_desc,
    d.compatibility_level,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(128), d.collation_name) AS collation_name,
    CONVERT(decimal(18, 2), SUM(mf.size) * 8.0 / 1024.0 / 1024.0) AS total_size_gb,
    CONVERT(decimal(18, 2), SUM(CASE WHEN mf.type_desc = N'ROWS' THEN mf.size ELSE 0 END) * 8.0 / 1024.0 / 1024.0) AS data_size_gb,
    CONVERT(decimal(18, 2), SUM(CASE WHEN mf.type_desc = N'LOG' THEN mf.size ELSE 0 END) * 8.0 / 1024.0 / 1024.0) AS log_size_gb,
    SUM(CASE WHEN mf.type_desc = N'ROWS' THEN 1 ELSE 0 END) AS data_file_count,
    SUM(CASE WHEN mf.type_desc = N'LOG' THEN 1 ELSE 0 END) AS log_file_count,
    CONVERT(int, d.is_broker_enabled) AS service_broker_enabled,
    CONVERT(int, d.is_cdc_enabled) AS cdc_enabled,
    CONVERT(int, d.is_encrypted) AS tde_enabled
FROM sys.databases d
LEFT JOIN sys.master_files mf
    ON d.database_id = mf.database_id
WHERE d.database_id > 4
  AND d.source_database_id IS NULL
  AND (@DatabaseName IS NULL OR d.name = @DatabaseName)
GROUP BY
    d.name,
    d.state_desc,
    d.compatibility_level,
    d.recovery_model_desc,
    d.collation_name,
    d.is_broker_enabled,
    d.is_cdc_enabled,
    d.is_encrypted
ORDER BY d.name;
"@

    $databaseRows = Invoke-AssessmentSqlQuery -ConnectionString $ConnectionString -Database 'master' -Query $databaseListQuery

    if (-not (Is-EmptyText $DatabaseName) -and @($databaseRows).Count -eq 0) {
        throw "Database not found or not accessible: $DatabaseName"
    }

    $outRows = New-Object 'System.Collections.Generic.List[object]'

    foreach ($db in @($databaseRows)) {
        $dbName = Get-ObjectText $db 'database_name'
        $state = Get-ObjectText $db 'state_desc'
        $notes = New-Object 'System.Collections.Generic.List[string]'

        $featureFacts = $null
        $agentFacts = $null

        if ($state -eq 'ONLINE') {
            $featureResult = Get-DatabaseFeatureFacts -ConnectionString $ConnectionString -DatabaseName $dbName
            if (-not (Is-EmptyText $featureResult.Error)) { Add-UniqueText -List $notes -Value $featureResult.Error }
            if (@($featureResult.Rows).Count -gt 0) { $featureFacts = @($featureResult.Rows)[0] }

            $agentResult = Get-AgentJobStepEvidenceForDatabase -ConnectionString $ConnectionString -DatabaseName $dbName
            if (-not (Is-EmptyText $agentResult.Error)) { Add-UniqueText -List $notes -Value $agentResult.Error }
            if (@($agentResult.Rows).Count -gt 0) { $agentFacts = @($agentResult.Rows)[0] }
        }
        else {
            Add-UniqueText -List $notes -Value "Database skipped for in-database feature checks because state is $state."
        }

        $outRows.Add([PSCustomObject]@{
            sql_instance                   = $SqlInstance
            database_name                  = $dbName
            state_desc                     = $state
            compatibility_level            = Get-ObjectInt $db 'compatibility_level'
            recovery_model_desc            = Get-ObjectText $db 'recovery_model_desc'
            collation_name                 = Get-ObjectText $db 'collation_name'
            total_size_gb                  = Get-ObjectDouble $db 'total_size_gb'
            data_size_gb                   = Get-ObjectDouble $db 'data_size_gb'
            log_size_gb                    = Get-ObjectDouble $db 'log_size_gb'
            data_file_count                = Get-ObjectInt $db 'data_file_count'
            log_file_count                 = Get-ObjectInt $db 'log_file_count'
            filegroup_count                = Get-ObjectInt $featureFacts 'filegroup_count'
            filestream_filegroup_count     = Get-ObjectInt $featureFacts 'filestream_filegroup_count'
            memory_optimized_table_count   = Get-ObjectInt $featureFacts 'memory_optimized_table_count'
            filetable_count                = Get-ObjectInt $featureFacts 'filetable_count'
            external_table_count           = Get-ObjectInt $featureFacts 'external_table_count'
            fulltext_catalog_count         = Get-ObjectInt $featureFacts 'fulltext_catalog_count'
            partition_scheme_count         = Get-ObjectInt $featureFacts 'partition_scheme_count'
            user_assembly_count            = Get-ObjectInt $featureFacts 'user_assembly_count'
            synonym_count                  = Get-ObjectInt $featureFacts 'synonym_count'
            cross_database_reference_count = Get-ObjectInt $featureFacts 'cross_database_reference_count'
            service_broker_enabled         = Get-ObjectInt $db 'service_broker_enabled'
            cdc_enabled                    = Get-ObjectInt $db 'cdc_enabled'
            change_tracking_enabled        = Get-ObjectInt $featureFacts 'change_tracking_enabled'
            tde_enabled                    = Get-ObjectInt $db 'tde_enabled'
            query_store_state              = Get-ObjectText $featureFacts 'query_store_state' 'Unavailable'
            largest_table                  = Get-ObjectText $featureFacts 'largest_table'
            largest_table_mb               = Get-ObjectDouble $featureFacts 'largest_table_mb'
            sql_agent_jobstep_count        = Get-ObjectInt $agentFacts 'sql_agent_jobstep_count'
            sql_agent_cmdexec_step_count   = Get-ObjectInt $agentFacts 'sql_agent_cmdexec_step_count'
            collection_notes               = Join-AssessmentList -Values $notes -Default 'None'
        }) | Out-Null
    }

    return @($outRows)
}

function New-FeatureUsageRow {
    param(
        [string]$SqlInstance,
        [string]$DatabaseName,
        [string]$Scope,
        [string]$FeatureName,
        [bool]$Detected,
        [string]$Evidence,
        [string]$AzureSqlDbImpact,
        [string]$AzureSqlMiImpact,
        [string]$SqlVmImpact,
        [string]$CollectionError = ''
    )

    return [PSCustomObject]@{
        sql_instance        = $SqlInstance
        database_name       = $DatabaseName
        scope               = $Scope
        feature_name        = $FeatureName
        detected            = $Detected.ToString().ToLowerInvariant()
        evidence            = $Evidence
        azure_sql_db_impact = $AzureSqlDbImpact
        azure_sql_mi_impact = $AzureSqlMiImpact
        sql_vm_impact       = $SqlVmImpact
        collection_error    = $CollectionError
    }
}

function New-FeatureUsageRows {
    param(
        [object[]]$ServerDesignRows,
        [object[]]$ClusterDesignRows,
        [object[]]$DatabaseDesignRows
    )

    $rows = New-Object 'System.Collections.Generic.List[object]'
    $server = if (@($ServerDesignRows).Count -gt 0) { @($ServerDesignRows)[0] } else { $null }
    $sqlInstance = Get-ObjectText $server 'sql_instance'

    $serverFeatureSpecs = @(
        @{ Name = 'linked_servers'; Column = 'linked_server_count'; DbImpact = 'Not supported as SQL Server linked servers; replace with app integration, elastic query alternatives, or data movement.'; MiImpact = 'Supported with target restrictions; validate remote providers and networking.'; VmImpact = 'Can preserve existing linked server model if providers and network paths are available.' },
        @{ Name = 'sql_agent_jobs'; Column = 'sql_agent_job_count'; DbImpact = 'SQL Server Agent is not available; replace with Elastic Jobs, Azure Automation, Functions, or app scheduler.'; MiImpact = 'SQL Agent is available with limitations; validate job steps, proxies, operators, alerts, and mail profile.'; VmImpact = 'Can preserve SQL Server Agent behavior.' },
        @{ Name = 'sql_agent_cmdexec_or_powershell_steps'; Column = 'sql_agent_cmdexec_step_count'; DbImpact = 'OS command job steps cannot run in Azure SQL DB.'; MiImpact = 'Command shell style job steps are not a clean MI fit; rewrite to Azure Automation/Functions or use VM.'; VmImpact = 'Can preserve command job steps if OS/security policy allows.' },
        @{ Name = 'database_mail'; Column = 'database_mail_profile_count'; DbImpact = 'Database Mail is not available in Azure SQL DB; move notifications outside the database.'; MiImpact = 'Supported with MI-specific configuration requirements.'; VmImpact = 'Can preserve Database Mail.' },
        @{ Name = 'credentials'; Column = 'credential_count'; DbImpact = 'Server-scoped credentials need redesign to database-scoped credentials or managed identity patterns.'; MiImpact = 'Validate credential type and file/external access behavior.'; VmImpact = 'Can preserve compatible credential usage.' },
        @{ Name = 'server_endpoints'; Column = 'endpoint_count'; DbImpact = 'Server endpoint control plane is not available.'; MiImpact = 'Database mirroring/AG endpoints are not user-managed in MI.'; VmImpact = 'Can preserve endpoint-dependent architecture.' },
        @{ Name = 'server_triggers'; Column = 'server_trigger_count'; DbImpact = 'Server-level triggers are not supported.'; MiImpact = 'Validate server trigger behavior; redesign where MI does not support required scope.'; VmImpact = 'Can preserve server trigger behavior.' },
        @{ Name = 'availability_groups'; Column = 'availability_group_count'; DbImpact = 'Source AG control plane is replaced by platform HA.'; MiImpact = 'Source AG control plane is replaced by MI HA or MI link migration pattern.'; VmImpact = 'Can preserve AG architecture if required.' },
        @{ Name = 'resource_governor'; Column = 'resource_governor_user_pool_count'; DbImpact = 'Resource Governor is not supported.'; MiImpact = 'Resource Governor is not supported.'; VmImpact = 'Can preserve Resource Governor.' },
        @{ Name = 'trace_flags'; Column = 'trace_flag_count'; DbImpact = 'Server-level trace flags need review and removal/replacement.'; MiImpact = 'Most server-level trace flags need review and are not a clean MI dependency.'; VmImpact = 'Can preserve trace flags where supported.' },
        @{ Name = 'xp_cmdshell'; Column = 'xp_cmdshell_enabled'; DbImpact = 'Not supported; move OS work outside the database.'; MiImpact = 'Not a clean MI fit; remove OS command dependency or use VM.'; VmImpact = 'Can preserve if accepted by security policy.' },
        @{ Name = 'clr'; Column = 'clr_enabled'; DbImpact = 'CLR requires redesign for Azure SQL DB unless workload uses supported alternatives.'; MiImpact = 'CLR may be viable but requires compatibility validation.'; VmImpact = 'Can preserve CLR behavior.' },
        @{ Name = 'external_scripts'; Column = 'external_scripts_enabled'; DbImpact = 'External script runtimes are not a clean Azure SQL DB fit.'; MiImpact = 'Requires validation or redesign to external compute services.'; VmImpact = 'Can preserve Machine Learning Services/runtime where installed.' },
        @{ Name = 'polybase'; Column = 'polybase_enabled'; DbImpact = 'SQL Server PolyBase usage requires redesign to Azure-native external data patterns.'; MiImpact = 'Validate against MI data virtualization support and storage targets.'; VmImpact = 'Can preserve SQL Server PolyBase where installed.' }
    )

    foreach ($spec in $serverFeatureSpecs) {
        $count = Get-ObjectInt -InputObject $server -PropertyName $spec.Column
        $rows.Add((New-FeatureUsageRow `
            -SqlInstance $sqlInstance `
            -DatabaseName '' `
            -Scope 'server' `
            -FeatureName $spec.Name `
            -Detected ($count -gt 0) `
            -Evidence "$($spec.Column)=$count" `
            -AzureSqlDbImpact $spec.DbImpact `
            -AzureSqlMiImpact $spec.MiImpact `
            -SqlVmImpact $spec.VmImpact)) | Out-Null
    }

    $dbFeatureSpecs = @(
        @{ Name = 'filestream_filegroups'; Column = 'filestream_filegroup_count'; DbImpact = 'FILESTREAM is not supported in Azure SQL DB.'; MiImpact = 'FILESTREAM is not supported in MI.'; VmImpact = 'Use SQL VM if FILESTREAM cannot be removed or externalized.' },
        @{ Name = 'filetables'; Column = 'filetable_count'; DbImpact = 'FileTable is not supported in Azure SQL DB.'; MiImpact = 'FileTable is not supported in MI.'; VmImpact = 'Use SQL VM if FileTable cannot be removed or externalized.' },
        @{ Name = 'memory_optimized_tables'; Column = 'memory_optimized_table_count'; DbImpact = 'Requires compatible Azure SQL DB tier, typically Business Critical for in-memory OLTP.'; MiImpact = 'Requires MI Business Critical, not General Purpose.'; VmImpact = 'Can preserve with a compatible SQL Server edition.' },
        @{ Name = 'external_tables'; Column = 'external_table_count'; DbImpact = 'External table behavior must be redesigned or validated against Azure SQL support.'; MiImpact = 'Validate external table/data virtualization targets.'; VmImpact = 'Can preserve SQL Server external table pattern if dependencies are available.' },
        @{ Name = 'full_text'; Column = 'fulltext_catalog_count'; DbImpact = 'Full-text is supported in Azure SQL DB but needs migration validation.'; MiImpact = 'Full-text is supported but needs migration validation.'; VmImpact = 'Can preserve full-text behavior.' },
        @{ Name = 'partitioning'; Column = 'partition_scheme_count'; DbImpact = 'Partitioning is generally supported; validate filegroup and maintenance assumptions.'; MiImpact = 'Partitioning is generally supported; validate filegroup and maintenance assumptions.'; VmImpact = 'Can preserve filegroup-level layout.' },
        @{ Name = 'clr_assemblies'; Column = 'user_assembly_count'; DbImpact = 'CLR assemblies are not a clean Azure SQL DB fit.'; MiImpact = 'CLR may be viable but requires validation.'; VmImpact = 'Can preserve CLR behavior.' },
        @{ Name = 'synonyms'; Column = 'synonym_count'; DbImpact = 'Synonyms may hide cross-database or linked-server dependencies; validate before Azure SQL DB.'; MiImpact = 'Usually easier to preserve than Azure SQL DB, but validate targets.'; VmImpact = 'Can preserve existing synonym targets if network/security permits.' },
        @{ Name = 'cross_database_references'; Column = 'cross_database_reference_count'; DbImpact = 'Cross-database three-part references are not a clean Azure SQL DB fit.'; MiImpact = 'MI supports instance-level database colocation patterns better.'; VmImpact = 'Can preserve cross-database references.' },
        @{ Name = 'sql_agent_database_steps'; Column = 'sql_agent_jobstep_count'; DbImpact = 'Database-specific SQL Agent steps need scheduler redesign.'; MiImpact = 'MI SQL Agent may preserve T-SQL job steps; validate unsupported subsystems.'; VmImpact = 'Can preserve SQL Agent steps.' },
        @{ Name = 'sql_agent_command_steps'; Column = 'sql_agent_cmdexec_step_count'; DbImpact = 'Command or PowerShell job steps cannot run in Azure SQL DB.'; MiImpact = 'Command or PowerShell job steps are not a clean MI fit.'; VmImpact = 'Can preserve command job steps if accepted by OS/security policy.' }
    )

    foreach ($db in @($DatabaseDesignRows)) {
        $dbName = Get-ObjectText $db 'database_name'
        foreach ($spec in $dbFeatureSpecs) {
            $count = Get-ObjectInt -InputObject $db -PropertyName $spec.Column
            $rows.Add((New-FeatureUsageRow `
                -SqlInstance (Get-ObjectText $db 'sql_instance' $sqlInstance) `
                -DatabaseName $dbName `
                -Scope 'database' `
                -FeatureName $spec.Name `
                -Detected ($count -gt 0) `
                -Evidence "$($spec.Column)=$count" `
                -AzureSqlDbImpact $spec.DbImpact `
                -AzureSqlMiImpact $spec.MiImpact `
                -SqlVmImpact $spec.VmImpact)) | Out-Null
        }

        foreach ($booleanFeature in @(
            @{ Name = 'service_broker'; Column = 'service_broker_enabled'; DbImpact = 'Requires Azure SQL DB compatibility validation and often redesign.'; MiImpact = 'Better MI fit when Service Broker behavior is required.'; VmImpact = 'Can preserve Service Broker behavior.' },
            @{ Name = 'cdc'; Column = 'cdc_enabled'; DbImpact = 'Validate CDC support and downstream consumers.'; MiImpact = 'Validate CDC support and downstream consumers.'; VmImpact = 'Can preserve CDC behavior.' },
            @{ Name = 'change_tracking'; Column = 'change_tracking_enabled'; DbImpact = 'Generally supported; validate retention and consumer behavior.'; MiImpact = 'Generally supported; validate retention and consumer behavior.'; VmImpact = 'Can preserve change tracking.' },
            @{ Name = 'tde'; Column = 'tde_enabled'; DbImpact = 'Plan key/certificate handling and target encryption model.'; MiImpact = 'Plan key/certificate handling and target encryption model.'; VmImpact = 'Can preserve TDE with certificate/key migration.' }
        )) {
            $detected = Test-ObjectBool -InputObject $db -PropertyName $booleanFeature.Column
            $rows.Add((New-FeatureUsageRow `
                -SqlInstance (Get-ObjectText $db 'sql_instance' $sqlInstance) `
                -DatabaseName $dbName `
                -Scope 'database' `
                -FeatureName $booleanFeature.Name `
                -Detected $detected `
                -Evidence "$($booleanFeature.Column)=$detected" `
                -AzureSqlDbImpact $booleanFeature.DbImpact `
                -AzureSqlMiImpact $booleanFeature.MiImpact `
                -SqlVmImpact $booleanFeature.VmImpact)) | Out-Null
        }
    }

    foreach ($cluster in @($ClusterDesignRows)) {
        $component = Get-ObjectText $cluster 'ha_dr_component'
        if ($component -ne '' -and $component -ne 'server_properties' -and $component -ne 'none_detected') {
            $rows.Add((New-FeatureUsageRow `
                -SqlInstance (Get-ObjectText $cluster 'sql_instance' $sqlInstance) `
                -DatabaseName (Get-ObjectText $cluster 'database_name') `
                -Scope 'cluster' `
                -FeatureName $component `
                -Detected $true `
                -Evidence (Get-ObjectText $cluster 'evidence') `
                -AzureSqlDbImpact (Get-ObjectText $cluster 'paas_impact') `
                -AzureSqlMiImpact (Get-ObjectText $cluster 'mi_impact') `
                -SqlVmImpact (Get-ObjectText $cluster 'vm_impact') `
                -CollectionError (Get-ObjectText $cluster 'collection_error'))) | Out-Null
        }
    }

    return @($rows)
}

function Get-ComputeBand {
    param(
        [double]$SizeGb,
        [int]$CpuCount,
        [string]$Target
    )

    $baseBand = '2-4 vCores'
    if ($SizeGb -gt 4000) { $baseBand = '32+ vCores' }
    elseif ($SizeGb -gt 1000) { $baseBand = '16-32 vCores' }
    elseif ($SizeGb -gt 250) { $baseBand = '8-16 vCores' }
    elseif ($SizeGb -gt 50) { $baseBand = '4-8 vCores' }

    if ($Target -eq 'SqlOnAzureVm') {
        if ($CpuCount -gt 0) {
            return "$baseBand equivalent SQL VM band; compare against source $CpuCount logical CPUs, memory pressure, and IO baseline before choosing VM family."
        }

        return "$baseBand equivalent SQL VM band; shortlist VM family only after CPU, memory, and IO baseline."
    }

    if ($CpuCount -gt 0) {
        return "$baseBand initial band; compare against source $CpuCount logical CPUs and measured workload."
    }

    return "$baseBand initial band; collect CPU/IO history before SKU commitment."
}

function Get-StorageBand {
    param([double]$SizeGb)

    $headroom = [math]::Ceiling($SizeGb * 1.3)
    if ($headroom -lt 32) { $headroom = 32 }

    return "Current allocated $([math]::Round($SizeGb, 2)) GB; plan at least $headroom GB including 30 percent headroom."
}

function Get-RecommendedServiceTier {
    param(
        [string]$Target,
        [double]$SizeGb,
        [string]$BusinessCriticality,
        [int]$MemoryOptimizedTableCount,
        [double]$LargestTableMb
    )

    if ($Target -eq 'AzureSqlDatabase') {
        if ($SizeGb -gt 4000) { return 'Hyperscale' }
        if ($BusinessCriticality -in @('High', 'Critical') -or $MemoryOptimizedTableCount -gt 0 -or $LargestTableMb -gt 102400) {
            return 'Business Critical'
        }

        return 'General Purpose'
    }

    if ($Target -eq 'AzureSqlManagedInstance') {
        if ($BusinessCriticality -in @('High', 'Critical') -or $MemoryOptimizedTableCount -gt 0) {
            return 'Business Critical'
        }
        if ($SizeGb -gt 8192) { return 'Next-gen General Purpose' }

        return 'General Purpose'
    }

    return 'SQL Server on Azure VM: choose VM family after perf counter and IO baseline.'
}

function New-AzureMigrationRecommendation {
    param(
        [object[]]$ServerDesignRows,
        [object[]]$ClusterDesignRows,
        [object[]]$DatabaseDesignRows,
        [object[]]$FeatureUsageRows
    )

    $server = if (@($ServerDesignRows).Count -gt 0) { @($ServerDesignRows)[0] } else { $null }
    $sqlInstance = Get-ObjectText $server 'sql_instance'
    $cpuCount = Get-ObjectInt $server 'cpu_count'

    $serverLinkedServers = Get-ObjectInt $server 'linked_server_count'
    $serverCmdSteps = Get-ObjectInt $server 'sql_agent_cmdexec_step_count'
    $serverResourceGovernor = Get-ObjectInt $server 'resource_governor_user_pool_count'
    $serverXpCmdShell = Get-ObjectInt $server 'xp_cmdshell_enabled'
    $serverExternalScripts = Get-ObjectInt $server 'external_scripts_enabled'
    $serverPolyBase = Get-ObjectInt $server 'polybase_enabled'
    $serverMailProfiles = Get-ObjectInt $server 'database_mail_profile_count'
    $serverTraceFlags = Get-ObjectInt $server 'trace_flag_count'
    $availabilityGroupCount = Get-ObjectInt $server 'availability_group_count'

    $recommendations = New-Object 'System.Collections.Generic.List[object]'

    foreach ($db in @($DatabaseDesignRows)) {
        $paasBlockers = New-Object 'System.Collections.Generic.List[string]'
        $miBlockers = New-Object 'System.Collections.Generic.List[string]'
        $remediations = New-Object 'System.Collections.Generic.List[string]'
        $evidence = New-Object 'System.Collections.Generic.List[string]'

        $databaseName = Get-ObjectText $db 'database_name'
        $dbSqlInstance = Get-ObjectText $db 'sql_instance' $sqlInstance
        $sizeGb = Get-ObjectDouble $db 'total_size_gb'
        $compatibilityLevel = Get-ObjectInt $db 'compatibility_level'
        $state = Get-ObjectText $db 'state_desc'
        $memoryOptimized = Get-ObjectInt $db 'memory_optimized_table_count'
        $largestTableMb = Get-ObjectDouble $db 'largest_table_mb'
        $agentSteps = Get-ObjectInt $db 'sql_agent_jobstep_count'
        $dbCmdSteps = Get-ObjectInt $db 'sql_agent_cmdexec_step_count'
        $crossDbRefs = Get-ObjectInt $db 'cross_database_reference_count'
        $synonyms = Get-ObjectInt $db 'synonym_count'
        $filestream = Get-ObjectInt $db 'filestream_filegroup_count'
        $filetables = Get-ObjectInt $db 'filetable_count'
        $clrAssemblies = Get-ObjectInt $db 'user_assembly_count'
        $externalTables = Get-ObjectInt $db 'external_table_count'
        $serviceBroker = Get-ObjectInt $db 'service_broker_enabled'
        $queryStoreState = Get-ObjectText $db 'query_store_state'
        $collectionNotes = Get-ObjectText $db 'collection_notes'

        Add-UniqueText -List $evidence -Value "size_gb=$([math]::Round($sizeGb, 2))"
        Add-UniqueText -List $evidence -Value "compatibility_level=$compatibilityLevel"
        Add-UniqueText -List $evidence -Value "query_store_state=$queryStoreState"

        if ($state -ne 'ONLINE') {
            Add-UniqueText -List $paasBlockers -Value "Database is not ONLINE ($state)"
            Add-UniqueText -List $miBlockers -Value "Database is not ONLINE ($state)"
            Add-UniqueText -List $remediations -Value 'Bring database online or assess from restored copy before target commitment.'
        }

        if ($compatibilityLevel -gt 0 -and $compatibilityLevel -lt 100) {
            Add-UniqueText -List $paasBlockers -Value "Compatibility level $compatibilityLevel is below supported Azure SQL baseline"
            Add-UniqueText -List $miBlockers -Value "Compatibility level $compatibilityLevel is below supported MI baseline"
            Add-UniqueText -List $remediations -Value 'Upgrade database compatibility level to 100 or higher and regression test application behavior.'
        }

        if ($filestream -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'FILESTREAM filegroups detected'
            Add-UniqueText -List $miBlockers -Value 'FILESTREAM filegroups detected'
            Add-UniqueText -List $remediations -Value 'Move FILESTREAM data to Azure Blob/app storage or keep the workload on SQL Server VM.'
        }

        if ($filetables -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'FileTable objects detected'
            Add-UniqueText -List $miBlockers -Value 'FileTable objects detected'
            Add-UniqueText -List $remediations -Value 'Replace FileTable with external document storage or keep the workload on SQL Server VM.'
        }

        if ($agentSteps -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'SQL Agent job steps reference this database'
            Add-UniqueText -List $remediations -Value 'Move SQL Agent scheduling to Elastic Jobs, Azure Automation, Functions, or validate MI SQL Agent compatibility.'
        }

        if ($dbCmdSteps -gt 0 -or $serverCmdSteps -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'SQL Agent CmdExec or PowerShell job steps detected'
            Add-UniqueText -List $miBlockers -Value 'SQL Agent command-shell style job steps detected'
            Add-UniqueText -List $remediations -Value 'Rewrite OS command job steps to Azure Automation, Functions, Logic Apps, or keep on SQL Server VM.'
        }

        if ($crossDbRefs -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'Cross-database references detected'
            Add-UniqueText -List $remediations -Value 'Co-locate dependent databases on MI/VM or refactor references behind application/data integration boundaries.'
        }

        if ($serverLinkedServers -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'Server linked servers detected'
            Add-UniqueText -List $remediations -Value 'Inventory linked server targets and replace with application integration, ETL, MI-compatible linked servers, or SQL VM.'
        }

        if ($synonyms -gt 0) {
            Add-UniqueText -List $evidence -Value "synonym_count=$synonyms"
        }

        if ($serviceBroker -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'Service Broker enabled'
            Add-UniqueText -List $remediations -Value 'Validate Service Broker usage; prefer MI when broker semantics must be preserved.'
        }

        if ($clrAssemblies -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'User CLR assemblies detected'
            Add-UniqueText -List $remediations -Value 'Validate CLR assemblies for MI or move code to application/Azure Functions before Azure SQL DB.'
        }

        if ($externalTables -gt 0 -or $serverPolyBase -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'External table or PolyBase usage detected'
            Add-UniqueText -List $remediations -Value 'Validate external data source support on target or redesign to Azure-native data integration.'
        }

        if ($serverXpCmdShell -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'xp_cmdshell enabled'
            Add-UniqueText -List $miBlockers -Value 'xp_cmdshell enabled'
            Add-UniqueText -List $remediations -Value 'Remove database-to-OS shell dependency or use SQL Server VM with explicit security controls.'
        }

        if ($serverExternalScripts -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'External scripts enabled'
            Add-UniqueText -List $miBlockers -Value 'External script runtime dependency needs target validation'
            Add-UniqueText -List $remediations -Value 'Move R/Python execution to Azure ML, Functions, or SQL VM unless MI support is explicitly validated.'
        }

        if ($serverResourceGovernor -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'Resource Governor user pools detected'
            Add-UniqueText -List $miBlockers -Value 'Resource Governor user pools detected'
            Add-UniqueText -List $remediations -Value 'Remove Resource Governor dependency or keep workload on SQL Server VM.'
        }

        if ($serverMailProfiles -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'Database Mail configured'
            Add-UniqueText -List $remediations -Value 'Move mail notifications outside Azure SQL DB or configure MI Database Mail profile.'
        }

        if ($serverTraceFlags -gt 0) {
            Add-UniqueText -List $paasBlockers -Value 'Server trace flags detected'
            Add-UniqueText -List $remediations -Value 'Review trace flags; remove dependency or validate SQL VM requirement.'
        }

        if ($availabilityGroupCount -gt 0) {
            Add-UniqueText -List $evidence -Value 'source_uses_availability_groups=true'
            Add-UniqueText -List $remediations -Value 'Validate application connection strings, listener dependency, failover expectations, and target HA model.'
        }

        if ($memoryOptimized -gt 0) {
            Add-UniqueText -List $evidence -Value "memory_optimized_table_count=$memoryOptimized"
            Add-UniqueText -List $remediations -Value 'Use Business Critical tier for in-memory OLTP support or validate VM sizing.'
        }

        if ($collectionNotes -ne '' -and $collectionNotes -ne 'None') {
            Add-UniqueText -List $evidence -Value "collection_notes=$collectionNotes"
        }

        $target = 'AzureSqlDatabase'
        $confidence = 0.86
        $vmReason = 'None'

        if ($miBlockers.Count -gt 0) {
            $target = 'SqlOnAzureVm'
            $confidence = 0.72
            $vmReason = 'One or more required features need SQL Server/OS/instance-level control that is not a clean PaaS or MI fit.'
        }
        elseif ($paasBlockers.Count -gt 0) {
            $target = 'AzureSqlManagedInstance'
            $confidence = 0.80
        }

        if ($target -eq 'AzureSqlManagedInstance' -and $sizeGb -gt 32768) {
            $target = 'SqlOnAzureVm'
            $confidence = 0.70
            Add-UniqueText -List $miBlockers -Value 'Database size exceeds conservative MI planning envelope'
            Add-UniqueText -List $remediations -Value 'Split/archive data, validate current MI regional limits, or use SQL Server VM.'
            $vmReason = 'Database size exceeds conservative MI planning envelope.'
        }

        if ($queryStoreState -eq 'Unavailable' -or $queryStoreState -eq '') {
            Add-UniqueText -List $remediations -Value 'Collect perf counters, Query Store or DMV workload history for 24h-7d before final SKU commitment.'
        }

        $serviceTier = Get-RecommendedServiceTier `
            -Target $target `
            -SizeGb $sizeGb `
            -BusinessCriticality '' `
            -MemoryOptimizedTableCount $memoryOptimized `
            -LargestTableMb $largestTableMb

        $routeHint = switch ($target) {
            'AzureSqlDatabase' { 'Use Azure Database Migration Service for controlled migrations; use SqlPackage/BACPAC only for small low-criticality databases after compatibility checks.' }
            'AzureSqlManagedInstance' { 'Use Azure Database Migration Service, native backup to URL/restore, or MI link depending downtime and SQL version.' }
            default { 'Use native backup/restore, log shipping, or AG seeding depending downtime tolerance and HA design.' }
        }

        $recommendations.Add([PSCustomObject]@{
            sql_instance         = $dbSqlInstance
            database_name        = $databaseName
            recommended_target   = $target
            service_tier         = $serviceTier
            compute_band         = Get-ComputeBand -SizeGb $sizeGb -CpuCount $cpuCount -Target $target
            storage_band         = Get-StorageBand -SizeGb $sizeGb
            sizing_confidence    = 'Low'
            confidence           = [math]::Round($confidence, 2)
            paas_blockers        = Join-AssessmentList -Values $paasBlockers -Default 'None'
            mi_blockers          = Join-AssessmentList -Values $miBlockers -Default 'None'
            vm_reason            = $vmReason
            remediation_required = Join-AssessmentList -Values $remediations -Default 'None'
            migration_route_hint = $routeHint
            evidence_summary     = Join-AssessmentList -Values $evidence -Default 'No notable feature evidence.'
        }) | Out-Null
    }

    return @($recommendations)
}

function Get-BlockerExplanation {
    param([string]$Blocker)

    if ($Blocker -like '*FILESTREAM*') { return 'FILESTREAM relies on SQL Server file system integration that is not available in Azure SQL DB or MI.' }
    if ($Blocker -like '*FileTable*') { return 'FileTable exposes file system semantics through SQL Server and is not available in Azure SQL DB or MI.' }
    if ($Blocker -like '*xp_cmdshell*') { return 'xp_cmdshell is an OS command execution dependency, which PaaS targets intentionally do not expose.' }
    if ($Blocker -like '*command*job*' -or $Blocker -like '*CmdExec*') { return 'Command job steps depend on the host OS and SQL Agent subsystems that are not clean PaaS dependencies.' }
    if ($Blocker -like '*Cross-database*') { return 'Azure SQL DB isolates databases behind separate logical database boundaries.' }
    if ($Blocker -like '*linked server*') { return 'Linked servers depend on instance-level providers and network paths that Azure SQL DB does not preserve.' }
    if ($Blocker -like '*Resource Governor*') { return 'Resource Governor is an instance-level workload management feature not available in Azure SQL DB or MI.' }
    if ($Blocker -like '*Compatibility level*') { return 'Azure SQL targets require supported database compatibility levels.' }
    if ($Blocker -like '*not ONLINE*') { return 'A database that is not online cannot be fully assessed or migrated with confidence.' }

    return 'The feature changes target compatibility or operational behavior and must be remediated or accepted before migration.'
}

function Get-BlockerAction {
    param([string]$Blocker)

    if ($Blocker -like '*FILESTREAM*') { return 'Externalize binary data to Azure Blob/application storage, then retest migration compatibility.' }
    if ($Blocker -like '*FileTable*') { return 'Replace FileTable with application-managed document storage or keep workload on SQL Server VM.' }
    if ($Blocker -like '*xp_cmdshell*') { return 'Remove OS shell execution from SQL Server or move it to Azure Automation/Functions.' }
    if ($Blocker -like '*command*job*' -or $Blocker -like '*CmdExec*') { return 'Rewrite command job steps outside SQL Server or place the workload on SQL Server VM.' }
    if ($Blocker -like '*Cross-database*') { return 'Co-locate databases on MI/VM or refactor data access boundaries.' }
    if ($Blocker -like '*linked server*') { return 'Inventory remote targets and replace with app/API/data movement integration or MI-compatible linked servers.' }
    if ($Blocker -like '*Resource Governor*') { return 'Remove Resource Governor dependency or select SQL Server VM.' }
    if ($Blocker -like '*Compatibility level*') { return 'Upgrade compatibility level to 100 or higher and regression test.' }
    if ($Blocker -like '*not ONLINE*') { return 'Bring database online or assess a restored copy.' }

    return 'Assign DBA/application owner to validate and remediate before target sign-off.'
}

function New-RemediationPlanRows {
    param([object[]]$RecommendationRows)

    $rows = New-Object 'System.Collections.Generic.List[object]'

    foreach ($recommendation in @($RecommendationRows)) {
        foreach ($field in @(
            @{ Name = 'paas_blockers'; Type = 'Azure SQL DB blocker' },
            @{ Name = 'mi_blockers'; Type = 'Azure SQL MI blocker' }
        )) {
            $text = Get-ObjectText $recommendation $field.Name
            if ($text -eq '' -or $text -eq 'None') { continue }

            foreach ($blocker in @($text.Split(';') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' -and $_ -ne 'None' })) {
                $rows.Add([PSCustomObject]@{
                    sql_instance       = Get-ObjectText $recommendation 'sql_instance'
                    database_name      = Get-ObjectText $recommendation 'database_name'
                    recommended_target = Get-ObjectText $recommendation 'recommended_target'
                    blocker_type       = $field.Type
                    blocker            = $blocker
                    why_it_matters     = Get-BlockerExplanation -Blocker $blocker
                    required_action    = Get-BlockerAction -Blocker $blocker
                    priority           = if ($field.Type -eq 'Azure SQL MI blocker') { 'High' } else { 'Medium' }
                }) | Out-Null
            }
        }
    }

    return @($rows)
}

function ConvertTo-AssessmentCsvHeader {
    param([string[]]$Columns)

    return ($Columns | ForEach-Object { '"' + ($_.Replace('"', '""')) + '"' }) -join ','
}

function Export-AssessmentCsv {
    param(
        [object[]]$Rows,
        [Parameter(Mandatory = $true)][string]$OutputPath,
        [Parameter(Mandatory = $true)][string[]]$Columns
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

function New-AssessmentSummaryMarkdown {
    param(
        [object[]]$ServerDesignRows,
        [object[]]$DatabaseDesignRows,
        [object[]]$RecommendationRows,
        [object[]]$RemediationRows
    )

    $server = if (@($ServerDesignRows).Count -gt 0) { @($ServerDesignRows)[0] } else { $null }
    $totalDatabases = @($DatabaseDesignRows).Count
    $dbTargetCount = @($RecommendationRows | Where-Object { (Get-ObjectText $_ 'recommended_target') -eq 'AzureSqlDatabase' }).Count
    $miTargetCount = @($RecommendationRows | Where-Object { (Get-ObjectText $_ 'recommended_target') -eq 'AzureSqlManagedInstance' }).Count
    $vmTargetCount = @($RecommendationRows | Where-Object { (Get-ObjectText $_ 'recommended_target') -eq 'SqlOnAzureVm' }).Count
    $blockedCount = @($RecommendationRows | Where-Object { (Get-ObjectText $_ 'paas_blockers') -ne 'None' -or (Get-ObjectText $_ 'mi_blockers') -ne 'None' }).Count

    $lines = New-Object 'System.Collections.Generic.List[string]'
    $lines.Add('# Azure SQL Migration Assessment Summary') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add("Generated at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')") | Out-Null
    $lines.Add("SQL instance: $(Get-ObjectText $server 'sql_instance')") | Out-Null
    $lines.Add("SQL version: $(Get-ObjectText $server 'product_version') $(Get-ObjectText $server 'product_level') $(Get-ObjectText $server 'product_update_level')") | Out-Null
    $lines.Add("Edition: $(Get-ObjectText $server 'edition')") | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('## Executive Summary') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add("- Databases assessed: $totalDatabases") | Out-Null
    $lines.Add("- Azure SQL Database candidates: $dbTargetCount") | Out-Null
    $lines.Add("- Azure SQL Managed Instance candidates: $miTargetCount") | Out-Null
    $lines.Add("- SQL Server on Azure VM candidates: $vmTargetCount") | Out-Null
    $lines.Add("- Databases with blockers/remediation: $blockedCount") | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('## Target Recommendations') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('| Database | Target | Tier | Confidence | Key blockers |') | Out-Null
    $lines.Add('| --- | --- | --- | --- | --- |') | Out-Null
    foreach ($row in @($RecommendationRows | Sort-Object database_name)) {
        $blockers = Get-ObjectText $row 'paas_blockers'
        if ($blockers -eq 'None') { $blockers = Get-ObjectText $row 'mi_blockers' }
        if ($blockers -eq '') { $blockers = 'None' }
        $lines.Add("| $(Get-ObjectText $row 'database_name') | $(Get-ObjectText $row 'recommended_target') | $(Get-ObjectText $row 'service_tier') | $(Get-ObjectText $row 'confidence') | $blockers |") | Out-Null
    }
    $lines.Add('') | Out-Null
    $lines.Add('## Required Remediation') | Out-Null
    $lines.Add('') | Out-Null
    if (@($RemediationRows).Count -eq 0) {
        $lines.Add('- No target blockers were identified from the collected evidence.') | Out-Null
    }
    else {
        foreach ($row in @($RemediationRows | Sort-Object database_name, priority, blocker)) {
            $lines.Add("- $(Get-ObjectText $row 'database_name'): $(Get-ObjectText $row 'blocker') - $(Get-ObjectText $row 'required_action')") | Out-Null
        }
    }
    $lines.Add('') | Out-Null
    $lines.Add('## Sizing Caveat') | Out-Null
    $lines.Add('') | Out-Null
    $lines.Add('This one-off assessment can estimate target tier and sizing bands from metadata, but final SKU selection needs workload evidence such as perf counters, Query Store runtime data, wait stats, and IO latency/throughput over 24h-7d.') | Out-Null

    return ($lines -join [Environment]::NewLine)
}

function Export-AzureMigrationAssessmentOutputs {
    param(
        [Parameter(Mandatory = $true)][string]$OutputRoot,
        [object[]]$ServerDesignRows,
        [object[]]$ClusterDesignRows,
        [object[]]$DatabaseDesignRows,
        [object[]]$FeatureUsageRows,
        [object[]]$RecommendationRows,
        [object[]]$RemediationRows
    )

    Ensure-Directory -Path $OutputRoot

    Export-AssessmentCsv -Rows $ServerDesignRows -OutputPath (Join-Path $OutputRoot 'server_design.csv') -Columns $script:ServerDesignColumns
    Export-AssessmentCsv -Rows $ClusterDesignRows -OutputPath (Join-Path $OutputRoot 'cluster_design.csv') -Columns $script:ClusterDesignColumns
    Export-AssessmentCsv -Rows $DatabaseDesignRows -OutputPath (Join-Path $OutputRoot 'database_design.csv') -Columns $script:DatabaseDesignColumns
    Export-AssessmentCsv -Rows $FeatureUsageRows -OutputPath (Join-Path $OutputRoot 'feature_usage.csv') -Columns $script:FeatureUsageColumns
    Export-AssessmentCsv -Rows $RecommendationRows -OutputPath (Join-Path $OutputRoot 'azure_target_recommendations.csv') -Columns $script:RecommendationColumns
    Export-AssessmentCsv -Rows $RemediationRows -OutputPath (Join-Path $OutputRoot 'remediation_plan.csv') -Columns $script:RemediationColumns

    $summary = New-AssessmentSummaryMarkdown `
        -ServerDesignRows $ServerDesignRows `
        -DatabaseDesignRows $DatabaseDesignRows `
        -RecommendationRows $RecommendationRows `
        -RemediationRows $RemediationRows

    Set-Content -LiteralPath (Join-Path $OutputRoot 'assessment_summary.md') -Value $summary -Encoding UTF8
}
