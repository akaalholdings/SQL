# AkaalHoldings SQL

Public monorepo for Azure SQL assessment, migration, performance, and operations work. Start with the smallest active path that matches the job.

## Repository map

| Path | Use it for | Output |
| --- | --- | --- |
| [`azure-sql-mcp/`](azure-sql-mcp/) | Bounded queries, schema metadata, diagnostics, Query Store evidence, and gated administration | Structured evidence, plans, diffs, and MCP artifacts |
| [`skills/`](skills/) | `sql_health_triage`, `sql_optimizer`, and `sql_plan_enforcer` workflows | Triage findings, optimization packs, and reviewed plan actions |
| [`Assessment/`](Assessment/) | Read-only SQL Server inventory before migration | Sanitized inventory and target-fit evidence |
| [`AzureMigration/Assessment/`](AzureMigration/Assessment/) | On-premises SQL Server target assessment | Target recommendations, sizing bands, blockers, and remediation plans |
| [`AzureMigration/CostSavings/`](AzureMigration/CostSavings/) | Azure SQL estate, usage, cost, and Advisor review | Cost findings and review-ready recommendations |
| [`SchemaCompare/`](SchemaCompare/) | Schema comparison UI direction | Design handoff only |
| [`StepLadder/`](StepLadder/) | Azure Function vCore scaling decisions | Controlled scaling state |
| [`azsql-BulkCopy/`](azsql-BulkCopy/) | Chunked Azure SQL to Azure SQL seed transfer | Resumable Parquet-backed transfer |

Retired execution paths must remain absent. The repository integrity check fails if they reappear or are referenced by active documentation and code.

## Choose the operating path

| Need | Start here | Mutation boundary |
| --- | --- | --- |
| Inventory an on-premises SQL Server | [`Assessment/`](Assessment/) | Read-only collection |
| Recommend Azure SQL Database, Managed Instance, or SQL VM | [`AzureMigration/Assessment/`](AzureMigration/Assessment/) | Local assessment only |
| Review Azure SQL cost and utilization | [`AzureMigration/CostSavings/`](AzureMigration/CostSavings/) | Read-only Azure discovery and metrics |
| Inspect Azure SQL schema, plans, waits, Query Store, or health | [`azure-sql-mcp/`](azure-sql-mcp/) | Restricted MCP mode is read-only by default |
| Run the performance loop | [`skills/`](skills/) | Triage is read-only; experiments and enforcement are separately gated |
| Seed a lower-environment table | [`azsql-BulkCopy/`](azsql-BulkCopy/) | Destination writes require change control |
| Automate Azure SQL vCore scaling | [`StepLadder/`](StepLadder/) | Azure resource changes only when `DRY_RUN=false` |

The active skills use `azure-sql-mcp` as their database execution channel. Evidence, owner review, rollback intent, and audit records are handoff requirements; they do not grant database permission.

## Quick start: Azure SQL MCP

Requires Python 3.12+ and [`uv`](https://docs.astral.sh/uv/). These commands use placeholders only and do not connect to a database until you run the server with real local configuration.

```bash
cd azure-sql-mcp
uv sync --dev

export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DEFAULT_DATABASE="your-database"
export AZURE_SQL_ALLOWED_DATABASES="your-database"
export AZURE_SQL_AUTH_MODE="entra-default"
export AZURE_SQL_ACCESS_MODE="restricted"
export AZURE_SQL_WRITE_POLICY="disabled"
export AZURE_SQL_PROFILE="triage"
export AZURE_SQL_TOOL_GROUPS="core,performance"

# Use an Azure CLI login, managed identity, or another DefaultAzureCredential source.
uv run azure-sql-mcp
```

The default transport is local stdio. For VS Code Copilot setup, profiles, database policy, and troubleshooting, read [`azure-sql-mcp/docs/09-operations.md`](azure-sql-mcp/docs/09-operations.md) and [`azure-sql-mcp/README.md`](azure-sql-mcp/README.md).

## Quick start: active skills

Install the maintained bundle into the local Copilot skill directory:

```bash
cd skills
python3 install_all.py --dest ~/.copilot/skills
python3 check_installed_parity.py --dest ~/.copilot/skills
```

The three active skills are:

- [`sql_health_triage`](skills/sql_health_triage/SKILL.md): read-only health and incident triage.
- [`sql_optimizer`](skills/sql_optimizer/SKILL.md): query rewrites and index experiments with disposable-test gates.
- [`sql_plan_enforcer`](skills/sql_plan_enforcer/SKILL.md): Query Store review and reversible plan controls.

## Local database policy

- Do not apply a database change from an uncommitted checkout. Generate the preview or script, review it, and execute it from an approved change surface.
- A local write is allowed only against an explicitly disposable sandbox with a clear owner, rollback, audit record, and cleanup step.
- Test indexes must use the MCP test-index gates and an allowlisted sandbox database. Remove them after the experiment and retain the evidence, not the database mutation as the handoff.
- CI, documentation checks, and normal unit tests do not access a database. Integration tests are opt-in and must be run only against an approved non-production target.

## Safety and credentials

- MCP restricted mode is the default. It validates and bounds read-only SQL, enforces the database allowlist, and hides admin tools.
- Unrestricted mode exposes the administration surface but still requires the tool call, `dry_run`, write-policy, audit, and target-database gates.
- Keep passwords, tokens, client secrets, connection strings, private endpoints, and SQL text containing sensitive literals out of Git and command history. `.env.example` files contain placeholders only.
- A read-only client flag is not a SQL security boundary. Enforce least privilege in Azure SQL and use application guards as defense in depth.

## Verification

Run checks from the relevant directory. None of the normal commands below require database credentials.

### MCP

```bash
cd azure-sql-mcp
uv sync --dev
uv run ruff check src tests
uv run pyright
uv run python -m compileall -q src tests
uv run pytest -q
uv build
```

### Skills

```bash
cd skills
python3 -m pytest -q sql_optimizer/tests sql_plan_enforcer/tests sql_health_triage/tests tests
python3 -m compileall -q sql_optimizer sql_plan_enforcer sql_health_triage install_all.py check_installed_parity.py
tmp_dir="$(mktemp -d)"
python3 install_all.py --dest "$tmp_dir"
python3 check_installed_parity.py --dest "$tmp_dir"
rm -rf "$tmp_dir"
```

### Repository integrity

```bash
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_markdown_links.py
python3 scripts/check_retired_paths.py
python3 scripts/scan_content_secrets.py
```

The combined workflow is [`sql-integrity.yml`](.github/workflows/sql-integrity.yml). It runs for changes to the public documentation, active skills, MCP package, workflows, integrity scripts, and retired-path deletions. It does not deploy or connect to a database.

## State reporting

| State | Evidence |
| --- | --- |
| Local | `git status` and local command output |
| Pushed | A commit exists on a remote branch |
| CI | GitHub reports a result for the relevant workflow run |
| Deployed | A runtime or Azure resource changed and a live smoke check passed |

These states are separate. A local edit or push does not imply CI success, deployment, or database access.
