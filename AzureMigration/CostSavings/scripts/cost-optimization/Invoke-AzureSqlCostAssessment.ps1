param(
    [Parameter(Mandatory = $true)][string[]]$SubscriptionId,
    [string[]]$ResourceGroupName = @(),
    [ValidateRange(1, 60)][int]$LookbackDays = 21,
    [string]$OutputRoot = './outputs/cost-optimization',
    [TimeSpan]$TimeGrain = ([TimeSpan]::FromMinutes(15)),
    [switch]$IncludeCostManagement,
    [switch]$IncludeAdvisor,
    [switch]$IncludeReservationRecommendations
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'AzureSqlCost.Common.ps1')

$repoRoot = Get-RepoRoot
$outputPath = Join-Path $repoRoot $OutputRoot
Ensure-Directory -Path $outputPath

$endUtc = (Get-Date).ToUniversalTime()
$startUtc = $endUtc.AddDays(-1 * $LookbackDays)

$estateRows = @()
$metricRows = @()
$costRows = @()
$advisorRows = @()
$reservationRows = @()

foreach ($subscription in $SubscriptionId) {
    Write-Host "Assessing subscription: $subscription"
    Set-CostAssessmentAzContext -SubscriptionId $subscription

    $subscriptionEstate = @(Get-AzureSqlEstate -SubscriptionId $subscription -ResourceGroupName $ResourceGroupName)
    $estateRows += $subscriptionEstate

    if ($subscriptionEstate.Count -eq 0) {
        Write-Warning "No Azure SQL resources discovered in subscription $subscription."
        continue
    }

    Write-Host "Collecting Azure Monitor metrics for $($subscriptionEstate.Count) resources."
    $metricRows += @(Get-AzureMetricSummary -EstateRows $subscriptionEstate -StartUtc $startUtc -EndUtc $endUtc -TimeGrain $TimeGrain)

    if ($IncludeCostManagement) {
        try {
            Write-Host "Collecting Cost Management actuals."
            $costRows += @(Get-AzureSqlCostActuals -SubscriptionId $subscription -StartUtc $startUtc -EndUtc $endUtc -ResourceId @($subscriptionEstate.resource_id))
        }
        catch {
            Write-Warning "Cost Management data unavailable for subscription $($subscription): $($_.Exception.Message)"
        }
    }

    if ($IncludeAdvisor) {
        try {
            Write-Host "Collecting Azure Advisor cost recommendations."
            $advisorRows += @(Get-AzureAdvisorCostRecommendations -SubscriptionId $subscription -ResourceId @($subscriptionEstate.resource_id))
        }
        catch {
            Write-Warning "Azure Advisor recommendations unavailable for subscription $($subscription): $($_.Exception.Message)"
        }
    }

    if ($IncludeReservationRecommendations) {
        try {
            Write-Host "Collecting reservation recommendations."
            $reservationRows += @(Get-AzureReservationRecommendations -SubscriptionId $subscription -LookbackDays $LookbackDays)
        }
        catch {
            Write-Warning "Reservation recommendations unavailable for subscription $($subscription): $($_.Exception.Message)"
        }
    }
}

$recommendationRows = @(New-CostRecommendations -EstateRows $estateRows -MetricRows $metricRows -LookbackDays $LookbackDays)

Export-CostCsv -Rows $estateRows -Columns $script:EstateColumns -OutputPath (Join-Path $outputPath 'azure_sql_estate.csv')
Export-CostCsv -Rows $metricRows -Columns $script:MetricColumns -OutputPath (Join-Path $outputPath 'usage_metrics.csv')
Export-CostCsv -Rows $costRows -Columns $script:CostColumns -OutputPath (Join-Path $outputPath 'cost_actuals.csv')
Export-CostCsv -Rows $advisorRows -Columns $script:AdvisorColumns -OutputPath (Join-Path $outputPath 'advisor_cost_recommendations.csv')
Export-CostCsv -Rows $reservationRows -Columns $script:ReservationColumns -OutputPath (Join-Path $outputPath 'reservation_recommendations.csv')
Export-CostCsv -Rows $recommendationRows -Columns $script:RecommendationColumns -OutputPath (Join-Path $outputPath 'recommendations.csv')

$summary = New-CostExecutiveSummary -EstateRows $estateRows -RecommendationRows $recommendationRows -AdvisorRows $advisorRows -ReservationRows $reservationRows -LookbackDays $LookbackDays -StartUtc $startUtc -EndUtc $endUtc
Set-Content -LiteralPath (Join-Path $outputPath 'executive_summary.md') -Value $summary -Encoding UTF8

$technicalFindings = New-CostTechnicalFindings -RecommendationRows $recommendationRows -MetricRows $metricRows
Set-Content -LiteralPath (Join-Path $outputPath 'technical_findings.md') -Value $technicalFindings -Encoding UTF8

Write-Host "Azure SQL cost optimization assessment complete. Output root: $outputPath"
