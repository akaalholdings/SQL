param(
    [Parameter(Mandatory = $true)][string[]]$SubscriptionId,
    [string[]]$ResourceGroupName = @(),
    [string]$OutputRoot = './outputs/cost-optimization',
    [ValidateRange(15, 1440)][int]$WindowMinutes = 60,
    [TimeSpan]$TimeGrain = ([TimeSpan]::FromMinutes(5))
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'AzureSqlCost.Common.ps1')

$repoRoot = Get-RepoRoot
$snapshotRoot = Join-Path (Join-Path $repoRoot $OutputRoot) 'snapshots'
Ensure-Directory -Path $snapshotRoot

$runStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runPath = Join-Path $snapshotRoot $runStamp
Ensure-Directory -Path $runPath

$endUtc = (Get-Date).ToUniversalTime()
$startUtc = $endUtc.AddMinutes(-1 * $WindowMinutes)

$estateRows = @()
$metricRows = @()

foreach ($subscription in $SubscriptionId) {
    Write-Host "Collecting cost snapshot for subscription: $subscription"
    Set-CostAssessmentAzContext -SubscriptionId $subscription

    $subscriptionEstate = @(Get-AzureSqlEstate -SubscriptionId $subscription -ResourceGroupName $ResourceGroupName)
    $estateRows += $subscriptionEstate

    if ($subscriptionEstate.Count -gt 0) {
        $metricRows += @(Get-AzureMetricSummary -EstateRows $subscriptionEstate -StartUtc $startUtc -EndUtc $endUtc -TimeGrain $TimeGrain)
    }
}

Export-CostCsv -Rows $estateRows -Columns $script:EstateColumns -OutputPath (Join-Path $runPath 'azure_sql_estate.csv')
Export-CostCsv -Rows $metricRows -Columns $script:MetricColumns -OutputPath (Join-Path $runPath 'usage_metrics.csv')

$indexPath = Join-Path $snapshotRoot 'snapshot_index.csv'
$indexRow = [PSCustomObject]@{
    run_stamp          = $runStamp
    collected_at_utc   = $endUtc.ToString('s')
    window_minutes     = $WindowMinutes
    subscription_count = $SubscriptionId.Count
    resource_count     = $estateRows.Count
    metric_row_count   = $metricRows.Count
    snapshot_path      = $runPath
}

if (Test-Path -LiteralPath $indexPath) {
    $indexRow | Export-Csv -LiteralPath $indexPath -NoTypeInformation -Append -Encoding UTF8
}
else {
    $indexRow | Export-Csv -LiteralPath $indexPath -NoTypeInformation -Encoding UTF8
}

Write-Host "Azure SQL cost snapshot complete. Snapshot: $runPath"
