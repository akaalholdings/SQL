$commonScriptPath = Join-Path $PSScriptRoot '../scripts/cost-optimization/AzureSqlCost.Common.ps1'
. $commonScriptPath

function New-TestEstateRow {
    param(
        [string]$ResourceKind = 'AzureSqlDatabase',
        [string]$ResourceName = 'sql-prod/appdb',
        [string]$ResourceId = '/subscriptions/sub-001/resourceGroups/rg-sql/providers/Microsoft.Sql/servers/sql-prod/databases/appdb',
        [string]$LicenseType = 'LicenseIncluded',
        [string]$ComputeModel = 'Provisioned'
    )

    return New-CostEstateRow `
        -SubscriptionId 'sub-001' `
        -ResourceGroup 'rg-sql' `
        -ResourceKind $ResourceKind `
        -ResourceName $ResourceName `
        -ResourceId $ResourceId `
        -Location 'uksouth' `
        -ServerName 'sql-prod' `
        -DatabaseName 'appdb' `
        -SkuName 'GP_Gen5_8' `
        -ServiceTier 'GeneralPurpose' `
        -ServiceObjective 'GP_Gen5_8' `
        -ComputeModel $ComputeModel `
        -VCores 8 `
        -LicenseType $LicenseType `
        -Tags @{ costCentre = 'sql' }
}

function New-TestMetricRow {
    param(
        [Parameter(Mandatory = $true)]$EstateRow,
        [Parameter(Mandatory = $true)][string]$MetricName,
        [double]$Average = 20,
        [double]$P95 = 30,
        [double]$Maximum = 50,
        [bool]$Missing = $false
    )

    return [PSCustomObject]@{
        subscription_id     = $EstateRow.subscription_id
        resource_group      = $EstateRow.resource_group
        resource_kind       = $EstateRow.resource_kind
        resource_name       = $EstateRow.resource_name
        resource_id         = $EstateRow.resource_id
        metric_name         = $MetricName
        metric_display_name = $MetricName
        unit                = 'Percent'
        aggregation         = 'Average'
        start_utc           = '2026-05-01T00:00:00'
        end_utc             = '2026-05-22T00:00:00'
        sample_count        = if ($Missing) { 0 } else { 100 }
        average             = if ($Missing) { $null } else { $Average }
        p95                 = if ($Missing) { $null } else { $P95 }
        maximum             = if ($Missing) { $null } else { $Maximum }
        minimum             = if ($Missing) { $null } else { 1 }
        missing             = $Missing
        notes               = if ($Missing) { 'No metric samples returned.' } else { '' }
    }
}

Describe 'Azure SQL cost optimization scripts' {
    It 'contains the cost optimization entrypoints' {
        Test-Path (Join-Path $PSScriptRoot '../scripts/cost-optimization/AzureSqlCost.Common.ps1') | Should -BeTrue
        Test-Path (Join-Path $PSScriptRoot '../scripts/cost-optimization/Invoke-AzureSqlCostAssessment.ps1') | Should -BeTrue
        Test-Path (Join-Path $PSScriptRoot '../scripts/cost-optimization/Invoke-AzureSqlCostSnapshot.ps1') | Should -BeTrue
    }

    It 'creates estate rows with the required output contract for each target type' {
        $rows = @(
            New-TestEstateRow -ResourceKind 'AzureSqlDatabase'
            New-TestEstateRow -ResourceKind 'AzureSqlElasticPool' -ResourceName 'sql-prod/pool-a' -ResourceId '/subscriptions/sub-001/resourceGroups/rg-sql/providers/Microsoft.Sql/servers/sql-prod/elasticPools/pool-a'
            New-TestEstateRow -ResourceKind 'AzureSqlManagedInstance' -ResourceName 'mi-prod' -ResourceId '/subscriptions/sub-001/resourceGroups/rg-sql/providers/Microsoft.Sql/managedInstances/mi-prod'
            New-TestEstateRow -ResourceKind 'SqlOnAzureVm' -ResourceName 'sqlvm-prod' -ResourceId '/subscriptions/sub-001/resourceGroups/rg-sql/providers/Microsoft.Compute/virtualMachines/sqlvm-prod' -LicenseType 'PAYG'
        )

        $rows.Count | Should -Be 4
        foreach ($column in $script:EstateColumns) {
            $rows[0].PSObject.Properties.Name -contains $column | Should -BeTrue
        }

        @($rows.resource_kind | Sort-Object -Unique).Count | Should -Be 4
    }

    It 'calculates nearest-rank percentile and metric summaries' {
        Get-CostPercentile -Values ([double[]](1..100)) -Percentile 95 | Should -Be 95

        $summary = Measure-MetricSeries -Values @('10', '20', '30', '40')
        $summary.sample_count | Should -Be 4
        $summary.average | Should -Be 25
        $summary.p95 | Should -Be 40
        $summary.maximum | Should -Be 40
        $summary.missing | Should -BeFalse
    }

    It 'records missing metrics without throwing' {
        $summary = Measure-MetricSeries -Values @() -MissingNote 'metric missing'
        $summary.sample_count | Should -Be 0
        $summary.missing | Should -BeTrue
        $summary.notes | Should -Be 'metric missing'
    }

    It 'flags low-utilization resources for scale-down review and licensing review' {
        $estate = New-TestEstateRow
        $metrics = @(
            New-TestMetricRow -EstateRow $estate -MetricName 'cpu_percent' -P95 22 -Maximum 45
            New-TestMetricRow -EstateRow $estate -MetricName 'physical_data_read_percent' -P95 18 -Maximum 38
            New-TestMetricRow -EstateRow $estate -MetricName 'log_write_percent' -P95 20 -Maximum 40
            New-TestMetricRow -EstateRow $estate -MetricName 'storage_percent' -P95 45 -Maximum 55
            New-TestMetricRow -EstateRow $estate -MetricName 'workers_percent' -P95 10 -Maximum 20
            New-TestMetricRow -EstateRow $estate -MetricName 'sessions_percent' -P95 12 -Maximum 22
        )

        $recommendations = @(New-CostRecommendations -EstateRows @($estate) -MetricRows $metrics -LookbackDays 21)

        ($recommendations.recommendation_type -contains 'ScaleDownReview') | Should -BeTrue
        ($recommendations.recommendation_type -contains 'LicensingReview') | Should -BeTrue
        ($recommendations | Where-Object { $_.recommendation_type -eq 'ScaleDownReview' } | Select-Object -First 1).confidence | Should -Be 'High'
    }

    It 'flags resources with material headroom risk' {
        $estate = New-TestEstateRow -LicenseType 'BasePrice'
        $metrics = @(
            New-TestMetricRow -EstateRow $estate -MetricName 'cpu_percent' -P95 72 -Maximum 91
            New-TestMetricRow -EstateRow $estate -MetricName 'storage_percent' -P95 50 -Maximum 60
        )

        $recommendations = @(New-CostRecommendations -EstateRows @($estate) -MetricRows $metrics -LookbackDays 14)

        ($recommendations.recommendation_type -contains 'ScaleRisk') | Should -BeTrue
        ($recommendations | Where-Object { $_.recommendation_type -eq 'ScaleRisk' } | Select-Object -First 1).confidence | Should -Be 'Medium'
    }

    It 'keeps sizing confidence low when critical metrics are absent' {
        $estate = New-TestEstateRow
        $recommendations = @(New-CostRecommendations -EstateRows @($estate) -MetricRows @() -LookbackDays 21)

        ($recommendations.recommendation_type -contains 'InsufficientMetrics') | Should -BeTrue
        ($recommendations | Where-Object { $_.recommendation_type -eq 'InsufficientMetrics' } | Select-Object -First 1).confidence | Should -Be 'Low'
    }

    It 'exports empty CSVs with a stable header' {
        $outputPath = Join-Path $TestDrive 'empty-recommendations.csv'
        Export-CostCsv -Rows @() -Columns $script:RecommendationColumns -OutputPath $outputPath

        $header = Get-Content -LiteralPath $outputPath -Raw -Encoding UTF8
        $header.Trim() | Should -Be ($script:RecommendationColumns -join ',')
    }
}
