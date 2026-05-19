# Azure SQL Cost Optimization Workflow

This workflow is read-only. It discovers Azure SQL resources, collects Azure Monitor usage metrics, and writes review-ready recommendations. It does not resize, stop, relicense, or modify any Azure resource.

Run commands from the package root:

```powershell
Set-Location ./AzureMigration/CostSavings
```

## Prerequisites

- PowerShell 7+ (`pwsh`).
- Azure PowerShell modules:
  - `Az.Accounts`
  - `Az.Resources`
  - `Az.Monitor`
  - `Az.Sql`
  - `Az.SqlVirtualMachine`
  - `Az.Compute`
  - `Az.CostManagement`
- Azure permissions:
  - Reader
  - Monitoring Reader
  - Advisor Reader, if collecting Advisor recommendations
  - Cost Management Reader, if collecting cost actuals or reservation recommendations

Connect before running:

```powershell
Connect-AzAccount
```

## Full Assessment

Run a 21-day assessment:

```powershell
pwsh ./scripts/cost-optimization/Invoke-AzureSqlCostAssessment.ps1 `
  -SubscriptionId "<subscription-id>" `
  -LookbackDays 21 `
  -OutputRoot ./outputs/cost-optimization `
  -IncludeCostManagement `
  -IncludeAdvisor `
  -IncludeReservationRecommendations
```

Limit discovery to known resource groups:

```powershell
pwsh ./scripts/cost-optimization/Invoke-AzureSqlCostAssessment.ps1 `
  -SubscriptionId "<subscription-id>" `
  -ResourceGroupName "rg-sql-prod","rg-sql-nonprod" `
  -LookbackDays 14
```

## Scheduled Snapshots

Use snapshots when you want locally archived evidence over several weeks:

```powershell
pwsh ./scripts/cost-optimization/Invoke-AzureSqlCostSnapshot.ps1 `
  -SubscriptionId "<subscription-id>" `
  -WindowMinutes 60 `
  -OutputRoot ./outputs/cost-optimization
```

Schedule that command hourly or daily using your preferred scheduler. Each run writes to `outputs/cost-optimization/snapshots/<timestamp>/` and appends to `snapshot_index.csv`.

## Outputs

- `azure_sql_estate.csv`: discovered SQL DB, elastic pool, Managed Instance, and SQL VM resources.
- `usage_metrics.csv`: summarized metric evidence with average, p95, max, min, sample count, and missing status.
- `cost_actuals.csv`: Cost Management actuals for discovered resources, when enabled and permitted.
- `advisor_cost_recommendations.csv`: Azure Advisor cost recommendations, when enabled and permitted.
- `reservation_recommendations.csv`: Azure Consumption reservation recommendations, when enabled and permitted.
- `recommendations.csv`: local rule-based findings.
- `executive_summary.md`: management-readable rollup.
- `technical_findings.md`: evidence and missing metric details.

## Recommendation Rules

Scale-down review is deliberately conservative. A resource is only flagged when:

- p95 bottleneck utilization is `<= 35%`
- maximum bottleneck utilization is `<= 70%`
- maximum storage pressure is `< 80%`
- maximum worker/session pressure is `< 60%`

Scale-risk is flagged when:

- p95 bottleneck utilization is `>= 65%`
- maximum bottleneck utilization is `>= 85%`
- maximum storage pressure is `>= 85%`
- maximum worker/session pressure is `>= 75%`

Licensing recommendations are review-only. Do not change Azure Hybrid Benefit, PAYG, BasePrice, AHUB, or DR settings until the licensing owner confirms entitlement and intended use.

## Manual Validation

Before any platform change:

1. Compare `azure_sql_estate.csv` resource counts with the Azure Portal for each subscription/resource group.
2. Spot-check at least one Azure SQL Database, one Managed Instance, and one SQL VM metric against Azure Monitor.
3. Compare `cost_actuals.csv` totals with Azure Cost Management for the same date range.
4. Review the top 10 rows in `recommendations.csv` with the workload owner.
5. For licensing findings, confirm eligible SQL Server licenses and Software Assurance/subscription rights before changing settings.
6. For scale-down findings, apply normal change control and monitor after any approved change.

## Tests

The Pester tests are fixture-based and do not require Azure access:

```powershell
Invoke-Pester ./tests/azure-sql-cost-optimization.contract.tests.ps1
```
