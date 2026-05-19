# SQL Server Azure Migration Assessment

`scripts/Invoke-SqlServerAzureMigrationAssessment.ps1` runs a one-off assessment against one on-prem SQL Server instance and writes evidence-based Azure target recommendations.

## Usage

Run these commands from the `AzureMigration/Assessment` folder.

Integrated security:

```powershell
pwsh ./scripts/Invoke-SqlServerAzureMigrationAssessment.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -OutputRoot './outputs/onprem-sql-01-assessment'
```

Single database:

```powershell
pwsh ./scripts/Invoke-SqlServerAzureMigrationAssessment.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -DatabaseName 'FinanceCore' `
  -OutputRoot './outputs/onprem-sql-01-financecore-assessment'
```

SQL authentication:

```powershell
$password = Read-Host -AsSecureString 'SQL password'
pwsh ./scripts/Invoke-SqlServerAzureMigrationAssessment.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -UseIntegratedSecurity:$false `
  -SqlUsername 'assessment_user' `
  -SqlPassword $password `
  -OutputRoot './outputs/onprem-sql-01-assessment'
```

## Outputs

- `server_design.csv`: SQL version, edition, CPU/memory metadata, tempdb shape, SQL Agent, linked servers, Database Mail, CLR, trace flags, Resource Governor, and other server-level evidence.
- `cluster_design.csv`: WSFC/Always On/log shipping/mirroring evidence where visible.
- `database_design.csv`: size, files, compatibility level, recovery model, filegroups, FILESTREAM/FileTable, memory-optimized objects, cross-database references, SQL Agent references, Query Store state, and largest table.
- `feature_usage.csv`: normalized compatibility matrix for server, database, and cluster features.
- `azure_target_recommendations.csv`: recommended target, service tier, compute/storage sizing band, confidence, blockers, remediation, migration route hint, and evidence summary.
- `remediation_plan.csv`: blocker-by-blocker action list.
- `assessment_summary.md`: executive-readable summary.

## Interpretation

The script recommends:

- `AzureSqlDatabase` when the database appears isolated and no instance-level blockers are detected.
- `AzureSqlManagedInstance` when SQL Server compatibility is needed but no MI blockers are detected.
- `SqlOnAzureVm` when the workload needs SQL Server/OS/instance-level control that is not a clean PaaS or MI fit.

Sizing is intentionally conservative. A one-off metadata run cannot replace workload baselining, so `sizing_confidence` remains `Low` until CPU, memory, IO, waits, Query Store, and latency evidence has been collected over a representative 24h-7d window.

The script does not call Azure APIs or pricing APIs. It produces target-fit and sizing-band guidance from the SQL Server evidence it can collect locally.

## Permissions

Run with an account that can read server and database metadata. Some collectors need elevated visibility such as `VIEW SERVER STATE`, `msdb` metadata access, and access to each assessed user database. If a collector is blocked by permissions, the output records the collection error and continues where possible.
