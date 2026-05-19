Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path (Split-Path -Parent $PSScriptRoot) 'Common.ps1')

$script:EstateColumns = @(
    'subscription_id',
    'resource_group',
    'resource_kind',
    'resource_name',
    'resource_id',
    'location',
    'server_name',
    'database_name',
    'elastic_pool_name',
    'sku_name',
    'service_tier',
    'service_objective',
    'compute_model',
    'vcores',
    'dtu',
    'max_size_gb',
    'storage_size_gb',
    'license_type',
    'zone_redundant',
    'tags'
)

$script:MetricColumns = @(
    'subscription_id',
    'resource_group',
    'resource_kind',
    'resource_name',
    'resource_id',
    'metric_name',
    'metric_display_name',
    'unit',
    'aggregation',
    'start_utc',
    'end_utc',
    'sample_count',
    'average',
    'p95',
    'maximum',
    'minimum',
    'missing',
    'notes'
)

$script:RecommendationColumns = @(
    'subscription_id',
    'resource_group',
    'resource_kind',
    'resource_name',
    'resource_id',
    'recommendation_type',
    'recommendation',
    'confidence',
    'current_sku',
    'suggested_target',
    'evidence',
    'requires_manual_approval',
    'source'
)

$script:CostColumns = @(
    'subscription_id',
    'resource_id',
    'usage_date',
    'service_name',
    'resource_type',
    'cost',
    'currency'
)

$script:AdvisorColumns = @(
    'subscription_id',
    'resource_id',
    'resource_group',
    'impact',
    'short_description',
    'recommendation_type_id',
    'annual_savings_amount',
    'currency',
    'source'
)

$script:ReservationColumns = @(
    'subscription_id',
    'scope',
    'resource_type',
    'location',
    'sku_name',
    'term',
    'lookback_period',
    'recommended_quantity',
    'net_savings',
    'currency',
    'source'
)

function Get-CostObjectProperty {
    param(
        [Parameter(Mandatory = $true)][object]$InputObject,
        [Parameter(Mandatory = $true)][string[]]$Names,
        [object]$Default = $null
    )

    foreach ($name in $Names) {
        $property = $InputObject.PSObject.Properties[$name]
        if ($null -ne $property -and $null -ne $property.Value) {
            return $property.Value
        }
    }

    return $Default
}

function ConvertTo-CostDouble {
    param([object]$Value)

    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace($Value.ToString())) {
        return $null
    }

    $result = 0.0
    if ([double]::TryParse($Value.ToString(), [ref]$result)) {
        return $result
    }

    return $null
}

function Convert-BytesToGiB {
    param([object]$Value)

    $number = ConvertTo-CostDouble -Value $Value
    if ($null -eq $number) { return $null }
    return [math]::Round($number / 1GB, 2)
}

function Convert-MbToGiB {
    param([object]$Value)

    $number = ConvertTo-CostDouble -Value $Value
    if ($null -eq $number) { return $null }
    return [math]::Round($number / 1024, 2)
}

function Get-CostPercentile {
    param(
        [Parameter(Mandatory = $true)][double[]]$Values,
        [Parameter(Mandatory = $true)][ValidateRange(1, 99)][int]$Percentile
    )

    $cleanValues = @($Values | Where-Object { $null -ne $_ } | Sort-Object)
    if ($cleanValues.Count -eq 0) { return $null }

    $rank = [math]::Ceiling(($Percentile / 100.0) * $cleanValues.Count) - 1
    if ($rank -lt 0) { $rank = 0 }
    if ($rank -ge $cleanValues.Count) { $rank = $cleanValues.Count - 1 }

    return [math]::Round([double]$cleanValues[$rank], 2)
}

function Measure-MetricSeries {
    param(
        [Parameter(Mandatory = $true)][object[]]$Values,
        [string]$MissingNote = 'No metric samples returned.'
    )

    $numericValues = @()
    foreach ($value in $Values) {
        $number = ConvertTo-CostDouble -Value $value
        if ($null -ne $number) {
            $numericValues += [double]$number
        }
    }

    if ($numericValues.Count -eq 0) {
        return [PSCustomObject]@{
            sample_count = 0
            average      = $null
            p95          = $null
            maximum      = $null
            minimum      = $null
            missing      = $true
            notes        = $MissingNote
        }
    }

    $average = ($numericValues | Measure-Object -Average).Average
    $maximum = ($numericValues | Measure-Object -Maximum).Maximum
    $minimum = ($numericValues | Measure-Object -Minimum).Minimum

    return [PSCustomObject]@{
        sample_count = $numericValues.Count
        average      = [math]::Round([double]$average, 2)
        p95          = Get-CostPercentile -Values $numericValues -Percentile 95
        maximum      = [math]::Round([double]$maximum, 2)
        minimum      = [math]::Round([double]$minimum, 2)
        missing      = $false
        notes        = ''
    }
}

function New-CostEstateRow {
    param(
        [Parameter(Mandatory = $true)][string]$SubscriptionId,
        [Parameter(Mandatory = $true)][string]$ResourceGroup,
        [Parameter(Mandatory = $true)][string]$ResourceKind,
        [Parameter(Mandatory = $true)][string]$ResourceName,
        [Parameter(Mandatory = $true)][string]$ResourceId,
        [string]$Location = '',
        [string]$ServerName = '',
        [string]$DatabaseName = '',
        [string]$ElasticPoolName = '',
        [string]$SkuName = '',
        [string]$ServiceTier = '',
        [string]$ServiceObjective = '',
        [string]$ComputeModel = '',
        [object]$VCores = $null,
        [object]$Dtu = $null,
        [object]$MaxSizeGb = $null,
        [object]$StorageSizeGb = $null,
        [string]$LicenseType = '',
        [object]$ZoneRedundant = $null,
        [object]$Tags = $null
    )

    $tagText = ''
    if ($null -ne $Tags) {
        $tagPairs = @()
        if ($Tags -is [hashtable]) {
            foreach ($tag in $Tags.GetEnumerator()) {
                $tagPairs += "$($tag.Key)=$($tag.Value)"
            }
        }
        else {
            foreach ($tag in $Tags.PSObject.Properties) {
                $tagPairs += "$($tag.Name)=$($tag.Value)"
            }
        }
        $tagText = ($tagPairs | Sort-Object) -join ';'
    }

    return [PSCustomObject]@{
        subscription_id    = $SubscriptionId
        resource_group     = $ResourceGroup
        resource_kind      = $ResourceKind
        resource_name      = $ResourceName
        resource_id        = $ResourceId
        location           = $Location
        server_name        = $ServerName
        database_name      = $DatabaseName
        elastic_pool_name  = $ElasticPoolName
        sku_name           = $SkuName
        service_tier       = $ServiceTier
        service_objective  = $ServiceObjective
        compute_model      = $ComputeModel
        vcores             = $VCores
        dtu                = $Dtu
        max_size_gb        = $MaxSizeGb
        storage_size_gb    = $StorageSizeGb
        license_type       = $LicenseType
        zone_redundant     = $ZoneRedundant
        tags               = $tagText
    }
}

function New-MetricSummaryRow {
    param(
        [Parameter(Mandatory = $true)]$EstateRow,
        [Parameter(Mandatory = $true)][string]$MetricName,
        [string]$MetricDisplayName = '',
        [string]$Unit = '',
        [string]$Aggregation = 'Average',
        [Parameter(Mandatory = $true)][datetime]$StartUtc,
        [Parameter(Mandatory = $true)][datetime]$EndUtc,
        [object[]]$Values = @(),
        [string]$MissingNote = 'No metric samples returned.'
    )

    $summary = Measure-MetricSeries -Values $Values -MissingNote $MissingNote

    return [PSCustomObject]@{
        subscription_id     = $EstateRow.subscription_id
        resource_group      = $EstateRow.resource_group
        resource_kind       = $EstateRow.resource_kind
        resource_name       = $EstateRow.resource_name
        resource_id         = $EstateRow.resource_id
        metric_name         = $MetricName
        metric_display_name = $MetricDisplayName
        unit                = $Unit
        aggregation         = $Aggregation
        start_utc           = $StartUtc.ToUniversalTime().ToString('s')
        end_utc             = $EndUtc.ToUniversalTime().ToString('s')
        sample_count        = $summary.sample_count
        average             = $summary.average
        p95                 = $summary.p95
        maximum             = $summary.maximum
        minimum             = $summary.minimum
        missing             = $summary.missing
        notes               = $summary.notes
    }
}

function Get-CostMetricProfile {
    param([Parameter(Mandatory = $true)][string]$ResourceKind)

    switch ($ResourceKind) {
        'AzureSqlDatabase' {
            return @(
                @{ Name = 'cpu_percent'; DisplayName = 'CPU percentage'; Unit = 'Percent' },
                @{ Name = 'dtu_consumption_percent'; DisplayName = 'DTU percentage'; Unit = 'Percent' },
                @{ Name = 'physical_data_read_percent'; DisplayName = 'Data IO percentage'; Unit = 'Percent' },
                @{ Name = 'log_write_percent'; DisplayName = 'Log IO percentage'; Unit = 'Percent' },
                @{ Name = 'workers_percent'; DisplayName = 'Workers percentage'; Unit = 'Percent' },
                @{ Name = 'sessions_percent'; DisplayName = 'Sessions percentage'; Unit = 'Percent' },
                @{ Name = 'storage_percent'; DisplayName = 'Data space used percent'; Unit = 'Percent' },
                @{ Name = 'app_cpu_percent'; DisplayName = 'Serverless app CPU percentage'; Unit = 'Percent' },
                @{ Name = 'app_memory_percent'; DisplayName = 'Serverless app memory percentage'; Unit = 'Percent' },
                @{ Name = 'app_cpu_billed'; DisplayName = 'Serverless app CPU billed'; Unit = 'Count'; Aggregation = 'Total' }
            )
        }
        'AzureSqlElasticPool' {
            return @(
                @{ Name = 'cpu_percent'; DisplayName = 'CPU percentage'; Unit = 'Percent' },
                @{ Name = 'dtu_consumption_percent'; DisplayName = 'DTU percentage'; Unit = 'Percent' },
                @{ Name = 'physical_data_read_percent'; DisplayName = 'Data IO percentage'; Unit = 'Percent' },
                @{ Name = 'log_write_percent'; DisplayName = 'Log IO percentage'; Unit = 'Percent' },
                @{ Name = 'workers_percent'; DisplayName = 'Workers percentage'; Unit = 'Percent' },
                @{ Name = 'sessions_percent'; DisplayName = 'Sessions percentage'; Unit = 'Percent' },
                @{ Name = 'storage_percent'; DisplayName = 'Data space used percent'; Unit = 'Percent' },
                @{ Name = 'allocated_data_storage_percent'; DisplayName = 'Allocated data storage percent'; Unit = 'Percent' }
            )
        }
        'AzureSqlManagedInstance' {
            return @(
                @{ Name = 'avg_cpu_percent'; DisplayName = 'Average CPU percentage'; Unit = 'Percent' },
                @{ Name = 'io_bytes_read'; DisplayName = 'IO bytes read'; Unit = 'Bytes' },
                @{ Name = 'io_bytes_written'; DisplayName = 'IO bytes written'; Unit = 'Bytes' },
                @{ Name = 'io_requests'; DisplayName = 'IO requests'; Unit = 'Count' },
                @{ Name = 'reserved_storage_mb'; DisplayName = 'Storage space reserved'; Unit = 'Count' },
                @{ Name = 'storage_space_used_mb'; DisplayName = 'Storage space used'; Unit = 'Count' },
                @{ Name = 'virtual_core_count'; DisplayName = 'Virtual core count'; Unit = 'Count' }
            )
        }
        'SqlOnAzureVm' {
            return @(
                @{ Name = 'Percentage CPU'; DisplayName = 'Percentage CPU'; Unit = 'Percent' },
                @{ Name = 'Available Memory Percentage'; DisplayName = 'Available Memory Percentage'; Unit = 'Percent' },
                @{ Name = 'Data Disk IOPS Consumed Percentage'; DisplayName = 'Data Disk IOPS Consumed Percentage'; Unit = 'Percent' },
                @{ Name = 'Data Disk Bandwidth Consumed Percentage'; DisplayName = 'Data Disk Bandwidth Consumed Percentage'; Unit = 'Percent' },
                @{ Name = 'Data Disk Latency'; DisplayName = 'Data Disk Latency'; Unit = 'Milliseconds' },
                @{ Name = 'Disk Read Operations/Sec'; DisplayName = 'Disk Read Operations/Sec'; Unit = 'CountPerSecond' },
                @{ Name = 'Disk Write Operations/Sec'; DisplayName = 'Disk Write Operations/Sec'; Unit = 'CountPerSecond' },
                @{ Name = 'Disk Read Bytes'; DisplayName = 'Disk Read Bytes'; Unit = 'Bytes'; Aggregation = 'Total' },
                @{ Name = 'Disk Write Bytes'; DisplayName = 'Disk Write Bytes'; Unit = 'Bytes'; Aggregation = 'Total' }
            )
        }
        default {
            return @()
        }
    }
}

function Get-MetricValuesFromAzMetric {
    param(
        [Parameter(Mandatory = $true)]$MetricResult,
        [string]$Aggregation = 'Average'
    )

    $values = @()
    $points = @()

    $dataProperty = $MetricResult.PSObject.Properties['Data']
    if ($null -ne $dataProperty -and $null -ne $dataProperty.Value) {
        $points += @($dataProperty.Value)
    }

    $seriesProperty = $MetricResult.PSObject.Properties['Timeseries']
    if ($null -ne $seriesProperty -and $null -ne $seriesProperty.Value) {
        foreach ($series in @($seriesProperty.Value)) {
            $seriesData = $series.PSObject.Properties['Data']
            if ($null -ne $seriesData -and $null -ne $seriesData.Value) {
                $points += @($seriesData.Value)
            }
        }
    }

    foreach ($point in $points) {
        $property = $point.PSObject.Properties[$Aggregation]
        if ($null -eq $property -or $null -eq $property.Value) {
            $property = $point.PSObject.Properties['Average']
        }

        if ($null -ne $property -and $null -ne $property.Value) {
            $values += $property.Value
        }
    }

    return $values
}

function Export-CostCsv {
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string[]]$Columns,
        [Parameter(Mandatory = $true)][string]$OutputPath
    )

    Ensure-Directory -Path (Split-Path -Parent $OutputPath)

    if ($Rows.Count -eq 0) {
        Set-Content -LiteralPath $OutputPath -Encoding UTF8 -Value ($Columns -join ',')
        return
    }

    $Rows | Select-Object $Columns | Export-Csv -LiteralPath $OutputPath -NoTypeInformation -Encoding UTF8
}

function Import-AzModule {
    param([Parameter(Mandatory = $true)][string[]]$ModuleName)

    foreach ($module in $ModuleName) {
        if (-not (Get-Module -ListAvailable -Name $module)) {
            throw "Required PowerShell module '$module' is not installed."
        }

        Import-Module $module -ErrorAction Stop
    }
}

function Set-CostAssessmentAzContext {
    param([Parameter(Mandatory = $true)][string]$SubscriptionId)

    Import-AzModule -ModuleName @('Az.Accounts')

    $context = Get-AzContext
    if ($null -eq $context) {
        throw 'No Azure context found. Run Connect-AzAccount before this assessment.'
    }

    Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
}

function Get-AzureSqlEstate {
    param(
        [Parameter(Mandatory = $true)][string]$SubscriptionId,
        [string[]]$ResourceGroupName = @()
    )

    Import-AzModule -ModuleName @('Az.Resources', 'Az.Sql', 'Az.Compute', 'Az.SqlVirtualMachine')
    $estate = @()
    $resourceGroupFilter = @($ResourceGroupName | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

    $servers = @(Get-AzSqlServer)
    foreach ($server in $servers) {
        if ($resourceGroupFilter.Count -gt 0 -and -not ($resourceGroupFilter -contains $server.ResourceGroupName)) {
            continue
        }

        $databases = @(Get-AzSqlDatabase -ResourceGroupName $server.ResourceGroupName -ServerName $server.ServerName)
        foreach ($database in $databases) {
            if ($database.DatabaseName -eq 'master') { continue }

            $estate += New-CostEstateRow `
                -SubscriptionId $SubscriptionId `
                -ResourceGroup $database.ResourceGroupName `
                -ResourceKind 'AzureSqlDatabase' `
                -ResourceName "$($database.ServerName)/$($database.DatabaseName)" `
                -ResourceId $database.ResourceId `
                -Location $database.Location `
                -ServerName $database.ServerName `
                -DatabaseName $database.DatabaseName `
                -ElasticPoolName (Get-CostObjectProperty -InputObject $database -Names @('ElasticPoolName') -Default '') `
                -SkuName (Get-CostObjectProperty -InputObject $database -Names @('SkuName', 'CurrentServiceObjectiveName') -Default '') `
                -ServiceTier (Get-CostObjectProperty -InputObject $database -Names @('Edition') -Default '') `
                -ServiceObjective (Get-CostObjectProperty -InputObject $database -Names @('CurrentServiceObjectiveName', 'RequestedServiceObjectiveName') -Default '') `
                -ComputeModel (Get-CostObjectProperty -InputObject $database -Names @('ComputeModel') -Default '') `
                -VCores (Get-CostObjectProperty -InputObject $database -Names @('Capacity', 'VCore') -Default $null) `
                -Dtu (Get-CostObjectProperty -InputObject $database -Names @('Dtu', 'Capacity') -Default $null) `
                -MaxSizeGb (Convert-BytesToGiB -Value (Get-CostObjectProperty -InputObject $database -Names @('MaxSizeBytes') -Default $null)) `
                -LicenseType (Get-CostObjectProperty -InputObject $database -Names @('LicenseType') -Default '') `
                -ZoneRedundant (Get-CostObjectProperty -InputObject $database -Names @('ZoneRedundant') -Default $null) `
                -Tags (Get-CostObjectProperty -InputObject $database -Names @('Tags') -Default $null)
        }

        $elasticPools = @(Get-AzSqlElasticPool -ResourceGroupName $server.ResourceGroupName -ServerName $server.ServerName)
        foreach ($pool in $elasticPools) {
            $estate += New-CostEstateRow `
                -SubscriptionId $SubscriptionId `
                -ResourceGroup $pool.ResourceGroupName `
                -ResourceKind 'AzureSqlElasticPool' `
                -ResourceName "$($pool.ServerName)/$($pool.ElasticPoolName)" `
                -ResourceId $pool.ResourceId `
                -Location $pool.Location `
                -ServerName $pool.ServerName `
                -ElasticPoolName $pool.ElasticPoolName `
                -SkuName (Get-CostObjectProperty -InputObject $pool -Names @('SkuName', 'Edition') -Default '') `
                -ServiceTier (Get-CostObjectProperty -InputObject $pool -Names @('Edition') -Default '') `
                -ServiceObjective (Get-CostObjectProperty -InputObject $pool -Names @('Edition') -Default '') `
                -ComputeModel (Get-CostObjectProperty -InputObject $pool -Names @('ComputeModel') -Default '') `
                -VCores (Get-CostObjectProperty -InputObject $pool -Names @('Capacity', 'VCore') -Default $null) `
                -Dtu (Get-CostObjectProperty -InputObject $pool -Names @('Dtu', 'Capacity') -Default $null) `
                -MaxSizeGb (Convert-BytesToGiB -Value (Get-CostObjectProperty -InputObject $pool -Names @('MaxSizeBytes') -Default $null)) `
                -LicenseType (Get-CostObjectProperty -InputObject $pool -Names @('LicenseType') -Default '') `
                -ZoneRedundant (Get-CostObjectProperty -InputObject $pool -Names @('ZoneRedundant') -Default $null) `
                -Tags (Get-CostObjectProperty -InputObject $pool -Names @('Tags') -Default $null)
        }
    }

    $managedInstances = @(Get-AzSqlInstance)
    foreach ($instance in $managedInstances) {
        if ($resourceGroupFilter.Count -gt 0 -and -not ($resourceGroupFilter -contains $instance.ResourceGroupName)) {
            continue
        }

        $sku = Get-CostObjectProperty -InputObject $instance -Names @('Sku') -Default $null
        $skuName = ''
        $tier = ''
        if ($null -ne $sku) {
            $skuName = Get-CostObjectProperty -InputObject $sku -Names @('Name') -Default ''
            $tier = Get-CostObjectProperty -InputObject $sku -Names @('Tier') -Default ''
        }

        $estate += New-CostEstateRow `
            -SubscriptionId $SubscriptionId `
            -ResourceGroup $instance.ResourceGroupName `
            -ResourceKind 'AzureSqlManagedInstance' `
            -ResourceName $instance.ManagedInstanceName `
            -ResourceId $instance.Id `
            -Location $instance.Location `
            -ServerName $instance.ManagedInstanceName `
            -SkuName $skuName `
            -ServiceTier $tier `
            -ServiceObjective $tier `
            -ComputeModel (Get-CostObjectProperty -InputObject $instance -Names @('ComputeGeneration') -Default '') `
            -VCores (Get-CostObjectProperty -InputObject $instance -Names @('VCores') -Default $null) `
            -StorageSizeGb (Get-CostObjectProperty -InputObject $instance -Names @('StorageSizeInGB') -Default $null) `
            -LicenseType (Get-CostObjectProperty -InputObject $instance -Names @('LicenseType') -Default '') `
            -ZoneRedundant (Get-CostObjectProperty -InputObject $instance -Names @('ZoneRedundant') -Default $null) `
            -Tags (Get-CostObjectProperty -InputObject $instance -Names @('Tags') -Default $null)
    }

    $sqlVmResources = @()
    $sqlVmCommand = Get-Command -Name Get-AzSqlVM -ErrorAction SilentlyContinue
    if ($null -ne $sqlVmCommand) {
        try {
            $sqlVmResources = @(Get-AzSqlVM)
        }
        catch {
            Write-Warning "Get-AzSqlVM failed; falling back to generic resource discovery: $($_.Exception.Message)"
            $sqlVmResources = @(Get-AzResource -ResourceType 'Microsoft.SqlVirtualMachine/sqlVirtualMachines')
        }
    }
    else {
        $sqlVmResources = @(Get-AzResource -ResourceType 'Microsoft.SqlVirtualMachine/sqlVirtualMachines')
    }

    foreach ($sqlVm in $sqlVmResources) {
        $resourceGroup = Get-CostObjectProperty -InputObject $sqlVm -Names @('ResourceGroupName') -Default ''
        if ($resourceGroupFilter.Count -gt 0 -and -not ($resourceGroupFilter -contains $resourceGroup)) {
            continue
        }

        $vmResourceId = Get-CostObjectProperty -InputObject $sqlVm -Names @('VirtualMachineResourceId') -Default ''
        $vmName = Get-CostObjectProperty -InputObject $sqlVm -Names @('Name', 'SqlVirtualMachineName') -Default ''
        $vmResourceIdFromProperties = Get-CostObjectProperty -InputObject (Get-CostObjectProperty -InputObject $sqlVm -Names @('Properties') -Default ([PSCustomObject]@{})) -Names @('virtualMachineResourceId') -Default ''
        if ([string]::IsNullOrWhiteSpace($vmResourceId)) {
            $vmResourceId = $vmResourceIdFromProperties
        }

        $vm = $null
        if (-not [string]::IsNullOrWhiteSpace($vmResourceId)) {
            $vm = Get-AzResource -ResourceId $vmResourceId -ErrorAction SilentlyContinue
        }

        $metricResourceId = if ($null -ne $vm) { $vm.ResourceId } else { Get-CostObjectProperty -InputObject $sqlVm -Names @('Id', 'ResourceId') -Default '' }
        $vmSize = ''
        if ($null -ne $vm) {
            $vmDetails = Get-AzVM -ResourceGroupName $vm.ResourceGroupName -Name $vm.Name -ErrorAction SilentlyContinue
            if ($null -ne $vmDetails) {
                $vmSize = $vmDetails.HardwareProfile.VmSize
            }
        }

        $estate += New-CostEstateRow `
            -SubscriptionId $SubscriptionId `
            -ResourceGroup $resourceGroup `
            -ResourceKind 'SqlOnAzureVm' `
            -ResourceName $vmName `
            -ResourceId $metricResourceId `
            -Location (Get-CostObjectProperty -InputObject $sqlVm -Names @('Location') -Default '') `
            -SkuName $vmSize `
            -ServiceTier 'VirtualMachine' `
            -ServiceObjective $vmSize `
            -LicenseType (Get-CostObjectProperty -InputObject $sqlVm -Names @('LicenseType', 'SqlServerLicenseType') -Default '') `
            -Tags (Get-CostObjectProperty -InputObject $sqlVm -Names @('Tags') -Default $null)
    }

    return $estate
}

function Get-AzureMetricSummary {
    param(
        [Parameter(Mandatory = $true)][object[]]$EstateRows,
        [Parameter(Mandatory = $true)][datetime]$StartUtc,
        [Parameter(Mandatory = $true)][datetime]$EndUtc,
        [TimeSpan]$TimeGrain = ([TimeSpan]::FromMinutes(15))
    )

    Import-AzModule -ModuleName @('Az.Monitor')
    $rows = @()

    foreach ($resource in $EstateRows) {
        $profile = @(Get-CostMetricProfile -ResourceKind $resource.resource_kind)
        foreach ($metric in $profile) {
            $aggregation = if ($metric.ContainsKey('Aggregation')) { $metric.Aggregation } else { 'Average' }
            try {
                $metricResult = Get-AzMetric `
                    -ResourceId $resource.resource_id `
                    -MetricName $metric.Name `
                    -TimeGrain $TimeGrain `
                    -StartTime $StartUtc `
                    -EndTime $EndUtc `
                    -AggregationType $aggregation `
                    -ErrorAction Stop

                $values = Get-MetricValuesFromAzMetric -MetricResult $metricResult -Aggregation $aggregation
                $rows += New-MetricSummaryRow -EstateRow $resource -MetricName $metric.Name -MetricDisplayName $metric.DisplayName -Unit $metric.Unit -Aggregation $aggregation -StartUtc $StartUtc -EndUtc $EndUtc -Values $values
            }
            catch {
                $rows += New-MetricSummaryRow -EstateRow $resource -MetricName $metric.Name -MetricDisplayName $metric.DisplayName -Unit $metric.Unit -Aggregation $aggregation -StartUtc $StartUtc -EndUtc $EndUtc -Values @() -MissingNote $_.Exception.Message
            }
        }
    }

    return Add-DerivedMetricRows -MetricRows $rows -EstateRows $EstateRows -StartUtc $StartUtc -EndUtc $EndUtc
}

function Add-DerivedMetricRows {
    param(
        [Parameter(Mandatory = $true)][object[]]$MetricRows,
        [Parameter(Mandatory = $true)][object[]]$EstateRows,
        [Parameter(Mandatory = $true)][datetime]$StartUtc,
        [Parameter(Mandatory = $true)][datetime]$EndUtc
    )

    $rows = @($MetricRows)

    foreach ($resource in $EstateRows) {
        $resourceMetrics = @($MetricRows | Where-Object { $_.resource_id -eq $resource.resource_id })

        $miUsed = $resourceMetrics | Where-Object { $_.metric_name -eq 'storage_space_used_mb' -and $_.missing -eq $false } | Select-Object -First 1
        $miReserved = $resourceMetrics | Where-Object { $_.metric_name -eq 'reserved_storage_mb' -and $_.missing -eq $false } | Select-Object -First 1
        $miUsedMax = if ($null -ne $miUsed) { ConvertTo-CostDouble -Value $miUsed.maximum } else { $null }
        $miReservedMax = if ($null -ne $miReserved) { ConvertTo-CostDouble -Value $miReserved.maximum } else { $null }
        if ($null -ne $miUsedMax -and $null -ne $miReservedMax -and $miReservedMax -gt 0) {
            $percent = [math]::Round(($miUsedMax / $miReservedMax) * 100, 2)
            $rows += New-MetricSummaryRow -EstateRow $resource -MetricName 'storage_percent_derived' -MetricDisplayName 'Storage used percent derived' -Unit 'Percent' -Aggregation 'Maximum' -StartUtc $StartUtc -EndUtc $EndUtc -Values @($percent)
        }

        $availableMemory = $resourceMetrics | Where-Object { $_.metric_name -eq 'Available Memory Percentage' -and $_.missing -eq $false } | Select-Object -First 1
        $availableMemoryMinimum = if ($null -ne $availableMemory) { ConvertTo-CostDouble -Value $availableMemory.minimum } else { $null }
        if ($null -ne $availableMemoryMinimum) {
            $memoryUsed = [math]::Round(100 - $availableMemoryMinimum, 2)
            $rows += New-MetricSummaryRow -EstateRow $resource -MetricName 'memory_used_percent_derived' -MetricDisplayName 'Memory used percent derived' -Unit 'Percent' -Aggregation 'Maximum' -StartUtc $StartUtc -EndUtc $EndUtc -Values @($memoryUsed)
        }
    }

    return $rows
}

function Get-ResourceMetricsByName {
    param(
        [Parameter(Mandatory = $true)][object[]]$MetricRows,
        [Parameter(Mandatory = $true)][string]$ResourceId,
        [Parameter(Mandatory = $true)][string[]]$MetricNames
    )

    return @($MetricRows | Where-Object { $_.resource_id -eq $ResourceId -and $MetricNames -contains $_.metric_name -and $_.missing -eq $false })
}

function Get-AssessmentConfidence {
    param(
        [int]$LookbackDays,
        [bool]$CriticalMetricsMissing
    )

    if ($CriticalMetricsMissing) { return 'Low' }
    if ($LookbackDays -ge 21) { return 'High' }
    if ($LookbackDays -ge 7) { return 'Medium' }
    return 'Low'
}

function New-CostRecommendations {
    param(
        [Parameter(Mandatory = $true)][object[]]$EstateRows,
        [Parameter(Mandatory = $true)][object[]]$MetricRows,
        [Parameter(Mandatory = $true)][int]$LookbackDays
    )

    $recommendations = @()
    $bottleneckMetrics = @(
        'cpu_percent',
        'dtu_consumption_percent',
        'physical_data_read_percent',
        'log_write_percent',
        'app_cpu_percent',
        'app_memory_percent',
        'avg_cpu_percent',
        'Percentage CPU',
        'Data Disk IOPS Consumed Percentage',
        'Data Disk Bandwidth Consumed Percentage',
        'memory_used_percent_derived'
    )
    $storageMetrics = @('storage_percent', 'allocated_data_storage_percent', 'storage_percent_derived')
    $workerSessionMetrics = @('workers_percent', 'sessions_percent')

    foreach ($resource in $EstateRows) {
        $resourceMetrics = @($MetricRows | Where-Object { $_.resource_id -eq $resource.resource_id })
        $bottlenecks = @(Get-ResourceMetricsByName -MetricRows $MetricRows -ResourceId $resource.resource_id -MetricNames $bottleneckMetrics)
        $storage = @(Get-ResourceMetricsByName -MetricRows $MetricRows -ResourceId $resource.resource_id -MetricNames $storageMetrics)
        $workerSession = @(Get-ResourceMetricsByName -MetricRows $MetricRows -ResourceId $resource.resource_id -MetricNames $workerSessionMetrics)

        $criticalMissing = ($bottlenecks.Count -eq 0)
        $storageRequired = $resource.resource_kind -in @('AzureSqlDatabase', 'AzureSqlElasticPool', 'AzureSqlManagedInstance')
        $workerSessionRequired = $resource.resource_kind -in @('AzureSqlDatabase', 'AzureSqlElasticPool')
        $storageUnknown = ($storageRequired -and $storage.Count -eq 0)
        $workerSessionUnknown = ($workerSessionRequired -and $workerSession.Count -eq 0)
        $confidence = Get-AssessmentConfidence -LookbackDays $LookbackDays -CriticalMetricsMissing $criticalMissing

        $bottleneckP95 = 0.0
        $bottleneckMax = 0.0
        if ($bottlenecks.Count -gt 0) {
            $bottleneckP95 = [double](($bottlenecks | Where-Object { $null -ne (ConvertTo-CostDouble $_.p95) } | Measure-Object -Property p95 -Maximum).Maximum)
            $bottleneckMax = [double](($bottlenecks | Where-Object { $null -ne (ConvertTo-CostDouble $_.maximum) } | Measure-Object -Property maximum -Maximum).Maximum)
        }

        $storageMax = 0.0
        if ($storage.Count -gt 0) {
            $storageMax = [double](($storage | Where-Object { $null -ne (ConvertTo-CostDouble $_.maximum) } | Measure-Object -Property maximum -Maximum).Maximum)
        }

        $workerSessionMax = 0.0
        if ($workerSession.Count -gt 0) {
            $workerSessionMax = [double](($workerSession | Where-Object { $null -ne (ConvertTo-CostDouble $_.maximum) } | Measure-Object -Property maximum -Maximum).Maximum)
        }

        $evidence = "p95_bottleneck=$bottleneckP95; max_bottleneck=$bottleneckMax; max_storage=$storageMax; max_worker_or_session=$workerSessionMax; storage_unknown=$storageUnknown; worker_session_unknown=$workerSessionUnknown; lookback_days=$LookbackDays"
        $currentSku = (($resource.sku_name, $resource.service_tier, $resource.service_objective) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique) -join '/'

        if ($criticalMissing) {
            $recommendations += [PSCustomObject]@{
                subscription_id          = $resource.subscription_id
                resource_group           = $resource.resource_group
                resource_kind            = $resource.resource_kind
                resource_name            = $resource.resource_name
                resource_id              = $resource.resource_id
                recommendation_type      = 'InsufficientMetrics'
                recommendation           = 'Collect additional Azure Monitor evidence before making sizing decisions.'
                confidence               = 'Low'
                current_sku              = $currentSku
                suggested_target         = 'No sizing change'
                evidence                 = $evidence
                requires_manual_approval = $true
                source                   = 'LocalMetricRules'
            }
        }
        elseif ($bottleneckP95 -le 35 -and $bottleneckMax -le 70 -and $storageMax -lt 80 -and $workerSessionMax -lt 60 -and -not $storageUnknown -and -not $workerSessionUnknown) {
            $recommendations += [PSCustomObject]@{
                subscription_id          = $resource.subscription_id
                resource_group           = $resource.resource_group
                resource_kind            = $resource.resource_kind
                resource_name            = $resource.resource_name
                resource_id              = $resource.resource_id
                recommendation_type      = 'ScaleDownReview'
                recommendation           = 'Candidate for one-step compute scale-down review in the same service family or VM size family.'
                confidence               = $confidence
                current_sku              = $currentSku
                suggested_target         = 'Review next lower SKU/vCore/DTU setting; validate with workload owner before change.'
                evidence                 = $evidence
                requires_manual_approval = $true
                source                   = 'LocalMetricRules'
            }
        }
        elseif ($bottleneckP95 -ge 65 -or $bottleneckMax -ge 85 -or $storageMax -ge 85 -or $workerSessionMax -ge 75) {
            $recommendations += [PSCustomObject]@{
                subscription_id          = $resource.subscription_id
                resource_group           = $resource.resource_group
                resource_kind            = $resource.resource_kind
                resource_name            = $resource.resource_name
                resource_id              = $resource.resource_id
                recommendation_type      = 'ScaleRisk'
                recommendation           = 'Do not scale down without deeper workload review; observed utilization leaves limited headroom.'
                confidence               = $confidence
                current_sku              = $currentSku
                suggested_target         = 'No scale-down'
                evidence                 = $evidence
                requires_manual_approval = $true
                source                   = 'LocalMetricRules'
            }
        }
        else {
            $recommendations += [PSCustomObject]@{
                subscription_id          = $resource.subscription_id
                resource_group           = $resource.resource_group
                resource_kind            = $resource.resource_kind
                resource_name            = $resource.resource_name
                resource_id              = $resource.resource_id
                recommendation_type      = 'KeepCurrentSize'
                recommendation           = 'Usage does not clearly justify a scale-down under conservative thresholds.'
                confidence               = $confidence
                current_sku              = $currentSku
                suggested_target         = 'No sizing change'
                evidence                 = $evidence
                requires_manual_approval = $true
                source                   = 'LocalMetricRules'
            }
        }

        $licenseType = (Get-Text $resource.license_type).ToUpperInvariant()
        $computeModel = (Get-Text $resource.compute_model).ToUpperInvariant()
        if ($licenseType -in @('LICENSEINCLUDED', 'PAYG', '') -and $computeModel -ne 'SERVERLESS') {
            $licenseRecommendation = 'Review Azure Hybrid Benefit eligibility with licensing owner; do not change unless the organization has matching Software Assurance/subscription rights.'
            if ($resource.resource_kind -eq 'SqlOnAzureVm') {
                $licenseRecommendation = 'Review SQL VM license type. PAYG may be valid, AHUB needs eligible SQL Server licenses, and DR applies only to passive HA/DR replicas.'
            }

            $recommendations += [PSCustomObject]@{
                subscription_id          = $resource.subscription_id
                resource_group           = $resource.resource_group
                resource_kind            = $resource.resource_kind
                resource_name            = $resource.resource_name
                resource_id              = $resource.resource_id
                recommendation_type      = 'LicensingReview'
                recommendation           = $licenseRecommendation
                confidence               = 'Medium'
                current_sku              = $currentSku
                suggested_target         = 'Review BasePrice/AHUB/DR eligibility; no automatic change.'
                evidence                 = "license_type=$($resource.license_type); compute_model=$($resource.compute_model)"
                requires_manual_approval = $true
                source                   = 'LocalLicensingRules'
            }
        }

        if (($resourceMetrics | Where-Object { $_.missing -eq $true }).Count -gt 0 -and -not $criticalMissing) {
            $recommendations += [PSCustomObject]@{
                subscription_id          = $resource.subscription_id
                resource_group           = $resource.resource_group
                resource_kind            = $resource.resource_kind
                resource_name            = $resource.resource_name
                resource_id              = $resource.resource_id
                recommendation_type      = 'MetricCoverageReview'
                recommendation           = 'Some optional metrics were unavailable; review diagnostic coverage before high-impact decisions.'
                confidence               = 'Low'
                current_sku              = $currentSku
                suggested_target         = 'Improve metric coverage'
                evidence                 = "missing_metrics=$((@($resourceMetrics | Where-Object { $_.missing -eq $true }).metric_name) -join ';')"
                requires_manual_approval = $true
                source                   = 'LocalMetricRules'
            }
        }
    }

    return $recommendations
}

function Get-AzureSqlCostActuals {
    param(
        [Parameter(Mandatory = $true)][string]$SubscriptionId,
        [Parameter(Mandatory = $true)][datetime]$StartUtc,
        [Parameter(Mandatory = $true)][datetime]$EndUtc,
        [string[]]$ResourceId = @()
    )

    Import-AzModule -ModuleName @('Az.CostManagement')
    $scope = "/subscriptions/$SubscriptionId"
    $aggregation = @{
        totalCost = @{
            name     = 'Cost'
            function = 'Sum'
        }
    }
    $grouping = @(
        @{ type = 'Dimension'; name = 'ResourceId' },
        @{ type = 'Dimension'; name = 'ServiceName' },
        @{ type = 'Dimension'; name = 'ResourceType' }
    )

    $result = Invoke-AzCostManagementQuery `
        -Scope $scope `
        -Timeframe Custom `
        -TimePeriodFrom $StartUtc `
        -TimePeriodTo $EndUtc `
        -Type ActualCost `
        -DatasetGranularity Daily `
        -DatasetAggregation $aggregation `
        -DatasetGrouping $grouping

    $resultColumns = @(Get-CostObjectProperty -InputObject $result -Names @('Column', 'Columns') -Default @())
    $resultRows = @(Get-CostObjectProperty -InputObject $result -Names @('Row', 'Rows') -Default @())
    $columns = @($resultColumns | ForEach-Object { $_.Name })
    $knownResourceIds = @($ResourceId | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $rows = @()
    foreach ($row in $resultRows) {
        $map = @{}
        for ($i = 0; $i -lt $columns.Count; $i++) {
            $map[$columns[$i]] = $row[$i]
        }

        $resourceId = Get-CostObjectProperty -InputObject ([PSCustomObject]$map) -Names @('ResourceId') -Default ''
        $resourceType = Get-CostObjectProperty -InputObject ([PSCustomObject]$map) -Names @('ResourceType') -Default ''
        if ($knownResourceIds.Count -gt 0 -and -not ($knownResourceIds -contains $resourceId)) {
            continue
        }

        if ($resourceType -notmatch 'Microsoft.Sql|Microsoft.SqlVirtualMachine|Microsoft.Compute/virtualMachines') {
            continue
        }

        $rows += [PSCustomObject]@{
            subscription_id = $SubscriptionId
            resource_id     = $resourceId
            usage_date      = Get-CostObjectProperty -InputObject ([PSCustomObject]$map) -Names @('UsageDate') -Default ''
            service_name    = Get-CostObjectProperty -InputObject ([PSCustomObject]$map) -Names @('ServiceName') -Default ''
            resource_type   = $resourceType
            cost            = Get-CostObjectProperty -InputObject ([PSCustomObject]$map) -Names @('Cost', 'PreTaxCost', 'totalCost') -Default ''
            currency        = Get-CostObjectProperty -InputObject ([PSCustomObject]$map) -Names @('Currency') -Default ''
        }
    }

    return $rows
}

function Invoke-CostRestGet {
    param([Parameter(Mandatory = $true)][string]$Path)

    $items = @()
    $nextPath = $Path
    while (-not [string]::IsNullOrWhiteSpace($nextPath)) {
        $response = Invoke-AzRestMethod -Method GET -Path $nextPath
        if ([string]::IsNullOrWhiteSpace($response.Content)) {
            break
        }

        $payload = $response.Content | ConvertFrom-Json -Depth 60
        if ($null -ne $payload.value) {
            $items += @($payload.value)
        }

        $nextLink = Get-CostObjectProperty -InputObject $payload -Names @('nextLink') -Default ''
        if ([string]::IsNullOrWhiteSpace($nextLink)) {
            $nextPath = ''
        }
        else {
            $nextPath = $nextLink.Replace('https://management.azure.com', '')
        }
    }

    return $items
}

function Get-AzureAdvisorCostRecommendations {
    param(
        [Parameter(Mandatory = $true)][string]$SubscriptionId,
        [string[]]$ResourceId = @()
    )

    Import-AzModule -ModuleName @('Az.Accounts')
    $filter = [System.Uri]::EscapeDataString("Category eq 'Cost'")
    $path = "/subscriptions/$SubscriptionId/providers/Microsoft.Advisor/recommendations?api-version=2025-01-01&`$filter=$filter"
    $items = Invoke-CostRestGet -Path $path
    $knownResourceIds = @($ResourceId | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $rows = @()

    foreach ($item in $items) {
        $properties = Get-CostObjectProperty -InputObject $item -Names @('properties') -Default ([PSCustomObject]@{})
        $shortDescription = Get-CostObjectProperty -InputObject $properties -Names @('shortDescription') -Default ([PSCustomObject]@{})
        $extended = Get-CostObjectProperty -InputObject $properties -Names @('extendedProperties') -Default ([PSCustomObject]@{})
        $resourceMetadata = Get-CostObjectProperty -InputObject $properties -Names @('resourceMetadata') -Default ([PSCustomObject]@{})
        $resourceId = Get-CostObjectProperty -InputObject $resourceMetadata -Names @('resourceId') -Default ''
        if ($knownResourceIds.Count -gt 0 -and -not [string]::IsNullOrWhiteSpace($resourceId) -and -not ($knownResourceIds -contains $resourceId)) {
            continue
        }

        $rows += [PSCustomObject]@{
            subscription_id         = $SubscriptionId
            resource_id             = $resourceId
            resource_group          = Get-CostObjectProperty -InputObject $properties -Names @('resourceGroup') -Default ''
            impact                  = Get-CostObjectProperty -InputObject $properties -Names @('impact') -Default ''
            short_description       = Get-CostObjectProperty -InputObject $shortDescription -Names @('problem', 'solution') -Default ''
            recommendation_type_id  = Get-CostObjectProperty -InputObject $properties -Names @('recommendationTypeId') -Default ''
            annual_savings_amount   = Get-CostObjectProperty -InputObject $extended -Names @('annualSavingsAmount', 'savingsAmount') -Default ''
            currency                = Get-CostObjectProperty -InputObject $extended -Names @('savingsCurrency', 'currency') -Default ''
            source                  = 'AzureAdvisor'
        }
    }

    return $rows
}

function Get-ReservationLookbackPeriod {
    param([Parameter(Mandatory = $true)][int]$LookbackDays)

    if ($LookbackDays -le 7) { return 'Last7Days' }
    if ($LookbackDays -le 30) { return 'Last30Days' }
    return 'Last60Days'
}

function Get-AzureReservationRecommendations {
    param(
        [Parameter(Mandatory = $true)][string]$SubscriptionId,
        [Parameter(Mandatory = $true)][int]$LookbackDays
    )

    Import-AzModule -ModuleName @('Az.Accounts')
    $lookback = Get-ReservationLookbackPeriod -LookbackDays $LookbackDays
    $filter = [System.Uri]::EscapeDataString("properties/scope eq 'Single' AND properties/lookBackPeriod eq '$lookback'")
    $path = "/subscriptions/$SubscriptionId/providers/Microsoft.Consumption/reservationRecommendations?api-version=2024-08-01&`$filter=$filter"
    $items = Invoke-CostRestGet -Path $path
    $rows = @()

    foreach ($item in $items) {
        $properties = Get-CostObjectProperty -InputObject $item -Names @('properties') -Default ([PSCustomObject]@{})
        $netSavings = Get-CostObjectProperty -InputObject $properties -Names @('netSavings') -Default ''
        $currency = ''
        if ($null -ne $netSavings -and $netSavings -isnot [string] -and $netSavings.PSObject.Properties['value']) {
            $currency = Get-CostObjectProperty -InputObject $netSavings -Names @('currency') -Default ''
            $netSavings = Get-CostObjectProperty -InputObject $netSavings -Names @('value') -Default ''
        }

        $rows += [PSCustomObject]@{
            subscription_id          = Get-CostObjectProperty -InputObject $properties -Names @('subscriptionId') -Default $SubscriptionId
            scope                    = Get-CostObjectProperty -InputObject $properties -Names @('scope') -Default ''
            resource_type            = Get-CostObjectProperty -InputObject $properties -Names @('resourceType') -Default ''
            location                 = Get-CostObjectProperty -InputObject $properties -Names @('location') -Default (Get-CostObjectProperty -InputObject $item -Names @('location') -Default '')
            sku_name                 = Get-CostObjectProperty -InputObject $properties -Names @('skuName') -Default (Get-CostObjectProperty -InputObject $item -Names @('sku') -Default '')
            term                     = Get-CostObjectProperty -InputObject $properties -Names @('term') -Default ''
            lookback_period          = Get-CostObjectProperty -InputObject $properties -Names @('lookBackPeriod') -Default $lookback
            recommended_quantity     = Get-CostObjectProperty -InputObject $properties -Names @('recommendedQuantity') -Default ''
            net_savings              = $netSavings
            currency                 = $currency
            source                   = 'AzureConsumptionReservationRecommendations'
        }
    }

    return $rows
}

function New-CostExecutiveSummary {
    param(
        [Parameter(Mandatory = $true)][object[]]$EstateRows,
        [Parameter(Mandatory = $true)][object[]]$RecommendationRows,
        [Parameter(Mandatory = $true)][object[]]$AdvisorRows,
        [Parameter(Mandatory = $true)][object[]]$ReservationRows,
        [Parameter(Mandatory = $true)][int]$LookbackDays,
        [Parameter(Mandatory = $true)][datetime]$StartUtc,
        [Parameter(Mandatory = $true)][datetime]$EndUtc
    )

    $scaleDown = @($RecommendationRows | Where-Object { $_.recommendation_type -eq 'ScaleDownReview' })
    $scaleRisk = @($RecommendationRows | Where-Object { $_.recommendation_type -eq 'ScaleRisk' })
    $licensing = @($RecommendationRows | Where-Object { $_.recommendation_type -eq 'LicensingReview' })

    return @"
# Azure SQL Cost Optimization Summary

Generated at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
Metric window: $($StartUtc.ToString('s'))Z to $($EndUtc.ToString('s'))Z ($LookbackDays days)

## Estate

- Resources assessed: $($EstateRows.Count)
- Azure SQL databases: $((@($EstateRows | Where-Object { $_.resource_kind -eq 'AzureSqlDatabase' })).Count)
- Azure SQL elastic pools: $((@($EstateRows | Where-Object { $_.resource_kind -eq 'AzureSqlElasticPool' })).Count)
- Azure SQL managed instances: $((@($EstateRows | Where-Object { $_.resource_kind -eq 'AzureSqlManagedInstance' })).Count)
- SQL Server on Azure VMs: $((@($EstateRows | Where-Object { $_.resource_kind -eq 'SqlOnAzureVm' })).Count)

## Findings

- Scale-down review candidates: $($scaleDown.Count)
- Scale-risk / no scale-down findings: $($scaleRisk.Count)
- Licensing review candidates: $($licensing.Count)
- Azure Advisor cost recommendations captured: $($AdvisorRows.Count)
- Reservation recommendations captured: $($ReservationRows.Count)

## Operating rule

This assessment is read-only. It does not resize, stop, relicense, or otherwise modify Azure resources. Treat every recommendation as a review queue item that requires workload owner approval and change control.
"@
}

function New-CostTechnicalFindings {
    param(
        [Parameter(Mandatory = $true)][object[]]$RecommendationRows,
        [Parameter(Mandatory = $true)][object[]]$MetricRows
    )

    $topRecommendations = @($RecommendationRows | Select-Object -First 25)
    $missingMetrics = @($MetricRows | Where-Object { $_.missing -eq $true } | Select-Object -First 25)

    $recommendationLines = if ($topRecommendations.Count -eq 0) {
        '- No local recommendations generated.'
    }
    else {
        ($topRecommendations | ForEach-Object { "- [$($_.recommendation_type)] $($_.resource_kind) $($_.resource_name): $($_.recommendation) Evidence: $($_.evidence)" }) -join "`n"
    }

    $missingMetricLines = if ($missingMetrics.Count -eq 0) {
        '- No missing metrics detected.'
    }
    else {
        ($missingMetrics | ForEach-Object { "- $($_.resource_kind) $($_.resource_name) metric $($_.metric_name): $($_.notes)" }) -join "`n"
    }

    return @"
# Azure SQL Cost Optimization Technical Findings

## Local Recommendations

$recommendationLines

## Missing Metrics

$missingMetricLines

## Thresholds

- Scale-down review requires p95 bottleneck utilization <= 35%, max bottleneck utilization <= 70%, max storage < 80%, and max worker/session pressure < 60%.
- Scale-risk is flagged when p95 bottleneck utilization >= 65%, max bottleneck utilization >= 85%, max storage >= 85%, or max worker/session pressure >= 75%.
- Licensing findings are review-only and must be validated against SQL Server license entitlements before changing Azure Hybrid Benefit settings.
"@
}
