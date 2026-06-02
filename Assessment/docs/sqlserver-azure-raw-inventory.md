# SQL Server Azure Raw Inventory Collector

`scripts/Invoke-SqlServerAzureRawInventory.ps1` collects sanitized raw evidence from one or more on-prem SQL Server instances for Azure SQL Database, Azure SQL Managed Instance, and SQL Server on Azure VM target-fit review.

The collector is read-only. It does not query Azure APIs and does not export table data, full module definitions, or full SQL Agent command text.

## Usage

Single instance with integrated security:

```powershell
pwsh ./scripts/Invoke-SqlServerAzureRawInventory.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -OutputRoot './outputs'
```

Single database:

```powershell
pwsh ./scripts/Invoke-SqlServerAzureRawInventory.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -DatabaseName 'FinanceCore' `
  -OutputRoot './outputs'
```

Instance list:

```powershell
pwsh ./scripts/Invoke-SqlServerAzureRawInventory.ps1 `
  -InstanceListCsv './inputs/instances.csv' `
  -OutputRoot './outputs'
```

SQL authentication:

```powershell
$password = Read-Host -AsSecureString 'SQL password'
pwsh ./scripts/Invoke-SqlServerAzureRawInventory.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -UseIntegratedSecurity:$false `
  -SqlUsername 'assessment_user' `
  -SqlPassword $password `
  -OutputRoot './outputs'
```

Optional workload sampling:

```powershell
pwsh ./scripts/Invoke-SqlServerAzureRawInventory.ps1 `
  -SqlInstance 'onprem-sql-01' `
  -EnableWorkloadSampling `
  -SampleIntervalSeconds 60 `
  -SampleDurationSeconds 3600 `
  -OutputRoot './outputs'
```

## Outputs

Each run writes to:

```text
outputs/<instance>-raw-inventory/<timestamp>/
```

Primary files:

- `assessment_manifest.json`
- `server_properties.csv`
- `server_configurations.csv`
- `databases.csv`
- `database_files.csv`
- `database_features.csv`
- `object_feature_scan.csv`
- `database_dependencies.csv`
- `sql_agent_jobs.csv`
- `sql_agent_job_steps.csv`
- `linked_servers.csv`
- `ha_dr_topology.csv`
- `security_principals.csv`
- `query_store_summary.csv`
- `wait_stats_snapshot.csv`
- `io_file_stats_snapshot.csv`
- `target_signal_matrix.csv`
- `collection_errors.csv`
- `codex_evidence_pack.md`
- `workload_samples.csv` when sampling is enabled

Start with `codex_evidence_pack.md` and `target_signal_matrix.csv` when preparing a presentable migration recommendation.

## Permissions

The collector continues when individual collectors fail and records failures in `collection_errors.csv`.

Best results require:

- Metadata visibility on the instance and user databases.
- `VIEW SERVER STATE` for waits, IO, requests, and server-level DMV evidence.
- `VIEW ANY DEFINITION` or equivalent object definition visibility for object feature scans.
- Read access to `msdb` for SQL Agent jobs and job steps.

## Sanitization

The collector exports object names, counts, hashes, feature hits, and short redacted snippets. It intentionally avoids full definitions and full job commands. Snippets redact common secret tokens such as password, token, API key, account key, and URL signatures.
