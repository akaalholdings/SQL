. (Join-Path $PSScriptRoot '../scripts/SqlServerAzureMigrationAssessment.Common.ps1')

Describe 'SQL Server Azure migration assessment recommendation rules' {
    It 'defines stable output schemas for every generated artifact' {
        $script:ServerDesignColumns -contains 'sql_instance' | Should -BeTrue
        $script:ServerDesignColumns -contains 'linked_server_count' | Should -BeTrue
        $script:ClusterDesignColumns -contains 'ha_dr_component' | Should -BeTrue
        $script:DatabaseDesignColumns -contains 'cross_database_reference_count' | Should -BeTrue
        $script:FeatureUsageColumns -contains 'azure_sql_db_impact' | Should -BeTrue
        $script:RecommendationColumns -contains 'remediation_required' | Should -BeTrue
        $script:RemediationColumns -contains 'required_action' | Should -BeTrue
    }

    It 'keeps the recommendation output schema stable' {
        $serverRows = @([PSCustomObject]@{
            sql_instance                      = 'onprem-sql-01'
            cpu_count                         = 8
            linked_server_count               = 0
            sql_agent_cmdexec_step_count      = 0
            resource_governor_user_pool_count = 0
            xp_cmdshell_enabled               = 0
            external_scripts_enabled          = 0
            polybase_enabled                  = 0
            database_mail_profile_count       = 0
            trace_flag_count                  = 0
            availability_group_count          = 0
        })

        $databaseRows = @([PSCustomObject]@{
            sql_instance                   = 'onprem-sql-01'
            database_name                  = 'CleanDb'
            state_desc                     = 'ONLINE'
            compatibility_level            = 150
            total_size_gb                  = 25
            memory_optimized_table_count   = 0
            largest_table_mb               = 512
            sql_agent_jobstep_count        = 0
            sql_agent_cmdexec_step_count   = 0
            cross_database_reference_count = 0
            synonym_count                  = 0
            filestream_filegroup_count     = 0
            filetable_count                = 0
            user_assembly_count            = 0
            external_table_count           = 0
            service_broker_enabled         = 0
            query_store_state              = 'READ_WRITE'
            collection_notes               = 'None'
        })

        $rows = New-AzureMigrationRecommendation `
            -ServerDesignRows $serverRows `
            -ClusterDesignRows @() `
            -DatabaseDesignRows $databaseRows `
            -FeatureUsageRows @()

        $columns = @($rows[0].PSObject.Properties.Name)
        foreach ($expectedColumn in @(
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
        )) {
            $columns -contains $expectedColumn | Should -BeTrue
        }
    }

    It 'recommends Azure SQL Database for a clean isolated database' {
        $serverRows = @([PSCustomObject]@{
            sql_instance                      = 'onprem-sql-01'
            cpu_count                         = 8
            linked_server_count               = 0
            sql_agent_cmdexec_step_count      = 0
            resource_governor_user_pool_count = 0
            xp_cmdshell_enabled               = 0
            external_scripts_enabled          = 0
            polybase_enabled                  = 0
            database_mail_profile_count       = 0
            trace_flag_count                  = 0
            availability_group_count          = 0
        })

        $databaseRows = @([PSCustomObject]@{
            sql_instance                   = 'onprem-sql-01'
            database_name                  = 'CleanDb'
            state_desc                     = 'ONLINE'
            compatibility_level            = 150
            total_size_gb                  = 10
            memory_optimized_table_count   = 0
            largest_table_mb               = 128
            sql_agent_jobstep_count        = 0
            sql_agent_cmdexec_step_count   = 0
            cross_database_reference_count = 0
            synonym_count                  = 0
            filestream_filegroup_count     = 0
            filetable_count                = 0
            user_assembly_count            = 0
            external_table_count           = 0
            service_broker_enabled         = 0
            query_store_state              = 'READ_WRITE'
            collection_notes               = 'None'
        })

        $rows = New-AzureMigrationRecommendation `
            -ServerDesignRows $serverRows `
            -ClusterDesignRows @() `
            -DatabaseDesignRows $databaseRows `
            -FeatureUsageRows @()

        $rows[0].recommended_target | Should -Be 'AzureSqlDatabase'
        $rows[0].paas_blockers | Should -Be 'None'
    }

    It 'routes SQL Agent and cross-database dependencies to Managed Instance when MI blockers are absent' {
        $serverRows = @([PSCustomObject]@{
            sql_instance                      = 'onprem-sql-01'
            cpu_count                         = 16
            linked_server_count               = 1
            sql_agent_cmdexec_step_count      = 0
            resource_governor_user_pool_count = 0
            xp_cmdshell_enabled               = 0
            external_scripts_enabled          = 0
            polybase_enabled                  = 0
            database_mail_profile_count       = 1
            trace_flag_count                  = 0
            availability_group_count          = 0
        })

        $databaseRows = @([PSCustomObject]@{
            sql_instance                   = 'onprem-sql-01'
            database_name                  = 'IntegratedDb'
            state_desc                     = 'ONLINE'
            compatibility_level            = 140
            total_size_gb                  = 120
            memory_optimized_table_count   = 0
            largest_table_mb               = 2048
            sql_agent_jobstep_count        = 4
            sql_agent_cmdexec_step_count   = 0
            cross_database_reference_count = 2
            synonym_count                  = 2
            filestream_filegroup_count     = 0
            filetable_count                = 0
            user_assembly_count            = 0
            external_table_count           = 0
            service_broker_enabled         = 0
            query_store_state              = 'READ_WRITE'
            collection_notes               = 'None'
        })

        $rows = New-AzureMigrationRecommendation `
            -ServerDesignRows $serverRows `
            -ClusterDesignRows @() `
            -DatabaseDesignRows $databaseRows `
            -FeatureUsageRows @()

        $rows[0].recommended_target | Should -Be 'AzureSqlManagedInstance'
        $rows[0].paas_blockers | Should -Match 'SQL Agent'
        $rows[0].paas_blockers | Should -Match 'Cross-database'
        $rows[0].mi_blockers | Should -Be 'None'
    }

    It 'routes unsupported MI and control-plane features to SQL Server on Azure VM' {
        $serverRows = @([PSCustomObject]@{
            sql_instance                      = 'onprem-sql-01'
            cpu_count                         = 16
            linked_server_count               = 0
            sql_agent_cmdexec_step_count      = 1
            resource_governor_user_pool_count = 1
            xp_cmdshell_enabled               = 1
            external_scripts_enabled          = 0
            polybase_enabled                  = 0
            database_mail_profile_count       = 0
            trace_flag_count                  = 0
            availability_group_count          = 0
        })

        $databaseRows = @([PSCustomObject]@{
            sql_instance                   = 'onprem-sql-01'
            database_name                  = 'FileSystemDb'
            state_desc                     = 'ONLINE'
            compatibility_level            = 150
            total_size_gb                  = 80
            memory_optimized_table_count   = 0
            largest_table_mb               = 1024
            sql_agent_jobstep_count        = 1
            sql_agent_cmdexec_step_count   = 1
            cross_database_reference_count = 0
            synonym_count                  = 0
            filestream_filegroup_count     = 1
            filetable_count                = 0
            user_assembly_count            = 0
            external_table_count           = 0
            service_broker_enabled         = 0
            query_store_state              = 'READ_WRITE'
            collection_notes               = 'None'
        })

        $rows = New-AzureMigrationRecommendation `
            -ServerDesignRows $serverRows `
            -ClusterDesignRows @() `
            -DatabaseDesignRows $databaseRows `
            -FeatureUsageRows @()

        $rows[0].recommended_target | Should -Be 'SqlOnAzureVm'
        $rows[0].mi_blockers | Should -Match 'FILESTREAM'
        $rows[0].mi_blockers | Should -Match 'xp_cmdshell'
        $rows[0].vm_reason | Should -Not -Be 'None'
    }

    It 'marks one-off sizing confidence as low when workload history is missing' {
        $serverRows = @([PSCustomObject]@{
            sql_instance                      = 'onprem-sql-01'
            cpu_count                         = 0
            linked_server_count               = 0
            sql_agent_cmdexec_step_count      = 0
            resource_governor_user_pool_count = 0
            xp_cmdshell_enabled               = 0
            external_scripts_enabled          = 0
            polybase_enabled                  = 0
            database_mail_profile_count       = 0
            trace_flag_count                  = 0
            availability_group_count          = 0
        })

        $databaseRows = @([PSCustomObject]@{
            sql_instance                   = 'onprem-sql-01'
            database_name                  = 'NoHistoryDb'
            state_desc                     = 'ONLINE'
            compatibility_level            = 150
            total_size_gb                  = 200
            memory_optimized_table_count   = 0
            largest_table_mb               = 8192
            sql_agent_jobstep_count        = 0
            sql_agent_cmdexec_step_count   = 0
            cross_database_reference_count = 0
            synonym_count                  = 0
            filestream_filegroup_count     = 0
            filetable_count                = 0
            user_assembly_count            = 0
            external_table_count           = 0
            service_broker_enabled         = 0
            query_store_state              = 'Unavailable'
            collection_notes               = 'None'
        })

        $rows = New-AzureMigrationRecommendation `
            -ServerDesignRows $serverRows `
            -ClusterDesignRows @() `
            -DatabaseDesignRows $databaseRows `
            -FeatureUsageRows @()

        $rows[0].sizing_confidence | Should -Be 'Low'
        $rows[0].remediation_required | Should -Match 'Collect perf counters'
    }
}
