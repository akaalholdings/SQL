# AkaalHoldings SQL

Private Azure SQL assessment, migration, performance, and operations tooling.
This repository is a monorepo: choose the smallest path that matches the work.

## Repository map

| Path | Purpose | Primary output or handoff |
| --- | --- | --- |
| [`azure-sql-mcp/`](azure-sql-mcp/) | Azure SQL MCP server for bounded queries, diagnostics, schema comparison, and audited administration | Structured evidence, plans, diffs, and MCP artifacts |
| [`skills/`](skills/) | Active `sql_health_triage`, `sql_optimizer`, and `sql_plan_enforcer` skills | Triage findings, optimization packs, and verified Query Store actions |
| [`Assessment/`](Assessment/) | Read-only raw SQL Server inventory collector | Sanitized inventory and target-fit evidence |
| [`AzureMigration/Assessment/`](AzureMigration/Assessment/) | One-off on-prem SQL Server Azure target assessment | Target recommendations, sizing bands, blockers, and remediation plan |
| [`AzureMigration/CostSavings/`](AzureMigration/CostSavings/) | Read-only Azure SQL estate, usage, cost, and Advisor assessment | Cost findings and review-ready recommendations |
| [`SchemaCompare/`](SchemaCompare/) | Schema comparison dashboard redesign mockup and notes | Design handoff; no runnable service is currently defined here |
| [`StepLadder/`](StepLadder/) | Azure Function that scales an Azure SQL Database from workload signals | Controlled vCore decisions and state |
| [`azsql-BulkCopy/`](azsql-BulkCopy/) | Chunked Azure SQL to Azure SQL bulk-copy utility | Resumable Parquet-backed seed transfer |
| [`connector/`](connector/) | Standalone read-only SQL Server/Synapse and Databricks inspection connector | Query, shape, plan, and cross-engine comparison evidence |
| [`legacy/`](legacy/) | Retired material retained for history and reference | Not part of the active execution path |

`query_geneva_db` is deprecated and lives under [`legacy/query_geneva_db/`](legacy/query_geneva_db/). Do not install, invoke, or present it as an active skill; use `azure-sql-mcp`, `skills/`, or `connector/` instead.

## Choose your path

| Need | Start here | Database or Azure mutation |
| --- | --- | --- |
| Inventory an on-prem SQL Server before migration | [`Assessment/`](Assessment/) | None; read-only collection |
| Recommend Azure SQL Database, Managed Instance, or SQL VM | [`AzureMigration/Assessment/`](AzureMigration/Assessment/) | None; local assessment only |
| Review Azure SQL cost and utilization | [`AzureMigration/CostSavings/`](AzureMigration/CostSavings/) | None; read-only Azure discovery and metrics |
| Inspect Azure SQL schema, plans, waits, Query Store, or health | [`azure-sql-mcp/`](azure-sql-mcp/) | Restricted mode is read-only by default |
| Run the performance loop | [`skills/`](skills/) | Triage is read-only; optimizer and enforcer changes are separately gated |
| Compare SQL Server/Synapse with Databricks table evidence | [`connector/`](connector/) | Read-only connector; permissions remain the real boundary |
| Seed a large lower-environment table | [`azsql-BulkCopy/`](azsql-BulkCopy/) | Writes to the configured destination; use change control |
| Automate Azure SQL vCore scaling | [`StepLadder/`](StepLadder/) | Azure resource changes only when `DRY_RUN=false` |
| Review the schema comparison UI direction | [`SchemaCompare/dashboard-redesign/`](SchemaCompare/dashboard-redesign/) | Design-only |

## Architecture and handoff flow

The tools are intentionally composable rather than one automatic migration pipeline. Evidence is the handoff contract; an owner reviews each recommendation before a change-capable path is opened.

```text
on-prem SQL Server
        │
        ├── Assessment ────────────────┐
        └── AzureMigration/Assessment ─┤  target fit, blockers, sizing bands
                                       │
Azure SQL ── azure-sql-mcp ──► structured evidence, plans, health, schema diffs
                                       │
                         ┌─────────────┴─────────────┐
                         │                           │
                 skills/ performance loop     review/change queue
              triage → optimize → enforce       owner approval + audit
                         │                           │
                         ├── verified query/plan handoff
                         ├── StepLadder operations handoff
                         └── azsql-BulkCopy seed handoff

AzureMigration/CostSavings and connector produce separate evidence packs;
SchemaCompare is currently a design handoff, not a connected runtime.
```

The active skills use `azure-sql-mcp` as their database execution channel. The handoff queue and audit/ledger files make ownership, evidence, and rollback intent explicit; they do not grant permission to change a database.

## Quick start: Azure SQL MCP

Requires Python 3.12+ and [`uv`](https://github.com/astral-sh/uv). Run from the MCP directory:

```bash
cd azure-sql-mcp
uv sync

# Prefer Entra ID / managed identity. Configure only the non-secret values needed locally.
export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="appdb"
export AZURE_SQL_ALLOWED_DATABASES="appdb"
export AZURE_SQL_AUTH_MODE="entra-default"
export AZURE_SQL_ACCESS_MODE="restricted"

uv run azure-sql-mcp
```

The default transport is stdio. For a remote transport, also set `AZURE_SQL_MCP_BEARER_TOKEN`, put TLS and an access-controlled proxy in front of it, and do not expose the process directly to the public internet. See [`azure-sql-mcp/README.md`](azure-sql-mcp/README.md) for tool groups, authentication modes, limits, client configuration, and Docker usage.

## Quick start: active skills

The maintained bundle is under `skills/`; the former `Skill/` path is no longer active.

```bash
cd skills
python3 install_all.py --dest ~/.copilot/skills
python3 check_installed_parity.py
```

The three skills are:

- [`sql_health_triage`](skills/sql_health_triage/SKILL.md): read-only detection and incident handoff.
- [`sql_optimizer`](skills/sql_optimizer/SKILL.md): evidence-based query and index experiments, with disposable test-index gates.
- [`sql_plan_enforcer`](skills/sql_plan_enforcer/SKILL.md): Query Store review and reversible plan controls with a kill switch, allowlist, audit ledger, and verification loop.

## Safety and credential policy

- `azure-sql-mcp` defaults to restricted access. Queries are parsed and bounded; the configured database allowlist, row limit, timeout, and transport authentication are separate controls.
- Write-capable MCP tools require unrestricted access, `AZURE_SQL_WRITE_POLICY=apply`, `dry_run=false`, and the tool-specific safeguards. Keep `AZURE_SQL_TEST_INDEX_DATABASES` limited to disposable sandboxes.
- `skills/sql_health_triage` is read-only. Optimizer test indexes and enforcer plan controls are explicitly gated and audited; production DDL remains a reviewed script or approved operational change.
- `Assessment/` and `AzureMigration/` assessment workflows are read-only. `AzureMigration/CostSavings/` does not resize, stop, relicense, or modify Azure resources.
- `azsql-BulkCopy` writes to its configured destination. Treat `recreate`, `truncate`, and `append` as change-controlled operations.
- Keep `.env`, passwords, access tokens, connection strings, private endpoints, and generated evidence containing secrets out of Git. Commit `.env.example` files only. Prefer `az login`, `entra-default`, managed identity, or a local secret store; inject secrets at runtime rather than placing them in command history, reports, or client configuration checked into the repository.
- A read-only client flag or `ApplicationIntent=ReadOnly` is not a security boundary. Enforce least privilege in SQL and Azure, then use the application-level guards as defense in depth.

## Development and test commands

These commands are local checks. They do not require a live database unless an integration test is explicitly configured with database settings.

### MCP

```bash
cd azure-sql-mcp
uv sync
uv run ruff check src tests
uv run pyright
uv run python -m compileall -q src tests
uv run pytest -q
uv build
```

### Skills

```bash
cd skills
uv run --with pytest pytest sql_optimizer/tests sql_plan_enforcer/tests sql_health_triage/tests tests
python3 -m compileall -q sql_optimizer sql_plan_enforcer sql_health_triage install_all.py check_installed_parity.py
python3 check_installed_parity.py
```

### PowerShell assessment and operations paths

```powershell
Invoke-Pester ./Assessment/tests/sqlserver.azure-raw-inventory.tests.ps1
Invoke-Pester ./AzureMigration/Assessment/tests/sqlserver.azure-migration-assessment.tests.ps1
Invoke-Pester ./AzureMigration/CostSavings/tests/azure-sql-cost-optimization.contract.tests.ps1
```

For StepLadder, run `pytest -q` from [`StepLadder/`](StepLadder/) after installing its development requirements. For connector and bulk-copy usage, follow [`azsql-BulkCopy/README.md`](azsql-BulkCopy/README.md) and the `uv run connector/connector.py --help` entry point; neither command is a substitute for database-side permission controls.

## Local, pushed, CI, and deployed are different states

| State | Meaning | Evidence |
| --- | --- | --- |
| Local | Files exist in this working tree and local checks may have run | `git status`, command output, local artifacts |
| Pushed | A commit exists on a remote branch | Remote branch or commit URL |
| CI | GitHub has executed the relevant root workflow and reported a result | Workflow run URL and job status |
| Deployed | A runtime or Azure resource has been updated and smoke-tested | Deployment record, live endpoint/logs, and runtime verification |

GitHub reads workflows only from the root [`.github/workflows/`](.github/workflows/) directory. [`sql-skills.yml`](.github/workflows/sql-skills.yml) runs for `skills/**`; [`azure-sql-mcp.yml`](.github/workflows/azure-sql-mcp.yml) runs for `azure-sql-mcp/**`. A local edit or pushed commit does not imply CI success, deployment, or a database connection. These workflows validate code; they do not deploy it or connect to a database.
