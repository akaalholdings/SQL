. (Join-Path $PSScriptRoot '../scripts/SqlServerAzureRawInventory.Common.ps1')

Describe 'SQL Server Azure raw inventory collector' {
    It 'contains the raw inventory entrypoints' {
        Test-Path (Join-Path $PSScriptRoot '../scripts/Invoke-SqlServerAzureRawInventory.ps1') | Should -BeTrue
        Test-Path (Join-Path $PSScriptRoot '../scripts/SqlServerAzureRawInventory.Common.ps1') | Should -BeTrue
    }

    It 'defines stable schemas for every required output file' {
        foreach ($fileName in @(
            'server_properties.csv',
            'server_configurations.csv',
            'databases.csv',
            'database_files.csv',
            'database_features.csv',
            'object_feature_scan.csv',
            'database_dependencies.csv',
            'sql_agent_jobs.csv',
            'sql_agent_job_steps.csv',
            'linked_servers.csv',
            'ha_dr_topology.csv',
            'security_principals.csv',
            'query_store_summary.csv',
            'wait_stats_snapshot.csv',
            'io_file_stats_snapshot.csv',
            'target_signal_matrix.csv',
            'collection_errors.csv'
        )) {
            $script:RawInventoryCsvContracts.Keys -contains $fileName | Should -BeTrue
            @($script:RawInventoryCsvContracts[$fileName]).Count | Should -BeGreaterThan 2
        }

        $script:RawInventoryOptionalCsvContracts.Keys -contains 'workload_samples.csv' | Should -BeTrue
        $script:RawTargetSignalMatrixColumns -contains 'azure_sql_db_impact' | Should -BeTrue
        $script:RawTargetSignalMatrixColumns -contains 'azure_sql_mi_impact' | Should -BeTrue
        $script:RawTargetSignalMatrixColumns -contains 'sql_vm_impact' | Should -BeTrue
    }

    It 'sanitizes sensitive command text and truncates previews' {
        $text = "EXEC dbo.DoThing @Password='abc123'; token=tok987; url='https://x/?sig=sig987'; SELECT 1"
        $sanitized = ConvertTo-SanitizedInventoryText -Value $text -MaxLength 70

        $sanitized | Should -Not -Match 'abc123|tok987|sig987'
        $sanitized | Should -Match '<redacted>'
        $sanitized.Length | Should -BeLessOrEqual 70
    }

    It 'validates optional workload sampling settings' {
        Test-RawInventorySamplingOptions | Should -BeTrue
        { Test-RawInventorySamplingOptions -EnableWorkloadSampling -SampleIntervalSeconds 0 -SampleDurationSeconds 60 } | Should -Throw
        { Test-RawInventorySamplingOptions -EnableWorkloadSampling -SampleIntervalSeconds 60 -SampleDurationSeconds 30 } | Should -Throw
    }

    It 'builds target signals from sanitized raw evidence' {
        $serverProperties = @([PSCustomObject]@{
            sql_instance = 'onprem-sql-01'
        })
        $serverConfigurations = @([PSCustomObject]@{
            sql_instance       = 'onprem-sql-01'
            configuration_name = 'xp_cmdshell'
            value_in_use       = 1
        })
        $databases = @([PSCustomObject]@{
            sql_instance        = 'onprem-sql-01'
            database_name       = 'FinanceCore'
            compatibility_level = 90
        })
        $databaseFiles = @(
            [PSCustomObject]@{ sql_instance = 'onprem-sql-01'; database_name = 'FinanceCore'; type_desc = 'LOG' },
            [PSCustomObject]@{ sql_instance = 'onprem-sql-01'; database_name = 'FinanceCore'; type_desc = 'LOG' }
        )
        $databaseFeatures = @(
            [PSCustomObject]@{ sql_instance = 'onprem-sql-01'; database_name = 'FinanceCore'; feature_name = 'filestream_filegroups'; detected = 'true'; feature_value = 1 },
            [PSCustomObject]@{ sql_instance = 'onprem-sql-01'; database_name = 'FinanceCore'; feature_name = 'service_broker'; detected = 'true'; feature_value = 'true' }
        )
        $objectHits = @([PSCustomObject]@{
            sql_instance        = 'onprem-sql-01'
            database_name       = 'FinanceCore'
            schema_name         = 'dbo'
            object_name         = 'LegacyProc'
            feature_name        = 'xp_cmdshell'
            azure_sql_db_impact = 'Not supported in Azure SQL Database.'
            azure_sql_mi_impact = 'Not supported in Azure SQL Managed Instance.'
            sql_vm_impact       = 'Can preserve only if accepted.'
        })
        $jobSteps = @([PSCustomObject]@{
            sql_instance  = 'onprem-sql-01'
            database_name = 'FinanceCore'
            subsystem     = 'PowerShell'
            job_name      = 'Finance Load'
        })
        $linkedServers = @([PSCustomObject]@{
            sql_instance       = 'onprem-sql-01'
            linked_server_name = 'OracleFinance'
        })
        $securityPrincipals = @([PSCustomObject]@{
            sql_instance        = 'onprem-sql-01'
            principal_type_desc = 'WINDOWS_LOGIN'
        })

        $signals = @(New-TargetSignalRows `
            -ServerPropertiesRows $serverProperties `
            -ServerConfigurationRows $serverConfigurations `
            -DatabaseRows $databases `
            -DatabaseFileRows $databaseFiles `
            -DatabaseFeatureRows $databaseFeatures `
            -ObjectFeatureScanRows $objectHits `
            -SqlAgentJobStepRows $jobSteps `
            -LinkedServerRows $linkedServers `
            -SecurityPrincipalRows $securityPrincipals)

        ($signals | Where-Object { $_.signal_name -eq 'xp_cmdshell_enabled' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_name -eq 'compatibility_level_below_100' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_name -eq 'multiple_log_files' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_name -eq 'filestream_filegroups' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_name -eq 'service_broker' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_name -eq 'linked_servers' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_name -eq 'windows_auth_principals' -and $_.detected -eq 'true' }).Count | Should -Be 1
        ($signals | Where-Object { $_.signal_scope -eq 'job_step' -and $_.signal_name -eq 'sql_agent_command_step' }).Count | Should -Be 1
    }
}
