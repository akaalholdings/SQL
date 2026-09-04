# AkaalHoldings SQL

Public monorepo for Azure SQL assessment, migration, performance, and operations work. Start with the smallest active path that matches the job.

## Repository map

| Path | Use it for | Output |
| --- | --- | --- |
| [`azure-sql-mcp`](https://github.com/akaalholdings/azure-sql-mcp) | Bounded queries, schema metadata, diagnostics, Query Store evidence, and gated administration | Structured evidence, plans, diffs, and MCP artifacts |
| [`skills/`](skills/) | `sql_health_triage`, `sql_optimizer`, `sql_plan_enforcer`, and `sql_index_manager` workflows | Triage findings, optimization packs, index portfolio reviews, and reviewed plan actions |
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
| Inspect Azure SQL schema, plans, waits, Query Store, or health | [`azure-sql-mcp`](https://github.com/akaalholdings/azure-sql-mcp) | Restricted MCP mode is read-only by default |
| Run the performance loop | [`skills/`](skills/) | Triage is read-only; experiments and enforcement are separately gated |
| Seed a lower-environment table | [`azsql-BulkCopy/`](azsql-BulkCopy/) | Destination writes require change control |
| Automate Azure SQL vCore scaling | [`StepLadder/`](StepLadder/) | Azure resource changes only when `DRY_RUN=false` |

The active skills use `azure-sql-mcp` as their database execution channel. Evidence, owner review, rollback intent, and audit records are handoff requirements; they do not grant database permission.

## Quick start: Azure SQL MCP

Requires Python 3.12+ and [`uv`](https://docs.astral.sh/uv/). These commands use placeholders only and do not connect to a database until you run the server with real local configuration.

```bash
git clone https://github.com/akaalholdings/azure-sql-mcp.git
cd azure-sql-mcp
uv sync --dev --locked

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

The default transport is local stdio. The optimizer's complete VS Code Copilot configuration, profile gates, synthetic policy, clean-room acceptance procedure, and Azure SQL tuning knowledge are self-contained in [`skills/README.md`](skills/README.md) and the installed skill.

After connecting, call `check_capabilities`. Measured optimization requires
`mcp_contract.performance_tuning=1`; restart-safe view workflows also require
`mcp_contract.durable_view_change=1`. These contracts are published by
`azure-sql-mcp` 2.1.0 and newer. An older MCP remains usable for static rewrite
analysis only.

## Quick start: active skills

Install the maintained bundle into the local Copilot skill directory:

```bash
cd skills
python3 install_all.py --dest ~/.copilot/skills
python3 check_installed_parity.py --dest ~/.copilot/skills
```

The collection installer backs up prior managed bundles and retires obsolete copies across known user-level Copilot, Claude, Agents, and Codex skill roots. Parity checks those same discovery surfaces and the retired PATH wrapper.

The four active skills are:

- [`sql_health_triage`](skills/sql_health_triage/SKILL.md): read-only health and incident triage.
- [`sql_optimizer`](skills/sql_optimizer/SKILL.md): query rewrites and single-query sandbox index experiments with disposable-test gates.
- [`sql_index_manager`](skills/sql_index_manager/SKILL.md): inventory, review, and recheck of Azure SQL index portfolios, with human change-control routing.
- [`sql_plan_enforcer`](skills/sql_plan_enforcer/SKILL.md): Query Store review and reversible plan controls.

The index manager is recommend-only. Its portfolio review returns the exact
artifact filenames `index-review.json`, `index-review.md`,
`create-candidates.sql`, `consolidation-candidates.sql`, `drop-candidates.sql`,
`rollback.sql`, and `validation.sql` when available. Snapshot, review,
as-of-run, and run ids remain opaque portfolio tracking identifiers, not
artifacts or learning evidence refs; V1 returns `evidence_id=None`. It uses
exact recurring-request, overlap, protection, stable-epoch, no-gap, and human
change-control gates; it never executes index DDL.

Run index review through an operator-owned local stdio MCP process configured
for the currently signed-in Entra identity. The server and skill contain no
fixed user principal name. Per-caller Entra delegation for a shared remote MCP
service is out of scope. The workflow uses existing effective database
permissions and does not create or require an additional database user or role.
Review requires `SELECT` on both history tables. Capture requires `SELECT` and
`INSERT` on both. Broader effective permissions, including `dbo`, do not fail
the contract probe. The restricted profile, database allowlist, and
`allow_index_history_write` are application-layer controls; they do not reduce
the signed-in identity's SQL permissions outside MCP.

## Local database policy

- Do not apply a database change from an uncommitted checkout. Generate the preview or script, review it, and execute it from an approved change surface.
- The general no-write rule for the restricted MCP and index-review path has one narrowly gated append-only exception: `capture_index_review_snapshot` may append only to the manually installed dbatools history tables after the returned policy gates pass. This exception does not permit arbitrary SQL, DDL, `UPDATE`, `DELETE`, schema creation, index apply, or production index changes.
- A local write in the separately scoped optimizer sandbox is allowed only against an explicitly disposable sandbox with a clear owner, rollback, audit record, and cleanup step.
- Test indexes must use the MCP test-index gates and an allowlisted sandbox database. Remove them after the experiment and retain the evidence, not the database mutation as the handoff.
- CI, documentation checks, and normal unit tests do not access a database. Integration tests are opt-in and must be run only against an approved non-production target.

## Safety and credentials

- MCP restricted mode is the default. It validates and bounds read-only SQL, enforces the database allowlist, and hides admin tools. The separate index-review profile is restricted and permits only its one policy-gated append-only snapshot-history write.
- Unrestricted mode exposes the administration surface but still requires the tool call, `dry_run`, write-policy, audit, and target-database gates.
- Keep passwords, tokens, client secrets, connection strings, private endpoints, and SQL text containing sensitive literals out of Git and command history. `.env.example` files contain placeholders only.
- A read-only client flag is not a SQL security boundary. Existing Azure SQL
  permissions remain authoritative; use application guards as defence in depth.

## Verification

Run checks from the relevant directory. None of the normal commands below require database credentials.

### MCP

From this repository root, keep the companion checkout as a sibling directory:

```bash
git clone https://github.com/akaalholdings/azure-sql-mcp.git ../azure-sql-mcp
(
  cd ../azure-sql-mcp
  uv sync --dev --locked
  uv run ruff check src tests scripts
  uv run pyright
  uv run python -m compileall -q src tests scripts
  uv run pytest -q
  uv build
  uv run python scripts/check_markdown_links.py
  uv run python scripts/verify_repository_content.py
)
```

### Skills

```bash
(
  cd skills
  python3 -m pytest -q sql_optimizer/tests sql_plan_enforcer/tests sql_health_triage/tests sql_index_manager/tests tests
  python3 -m compileall -q sql_optimizer sql_plan_enforcer sql_health_triage sql_index_manager install_all.py check_installed_parity.py
  python3 -m ruff check . ../scripts
  tmp_dir="$(mktemp -d)"
  HOME="$tmp_dir/home" python3 install_all.py \
    --dest "$tmp_dir/skills" \
    --backup-root "$tmp_dir/backups" \
    --retired-wrapper "$tmp_dir/bin/obsolete-wrapper"
  HOME="$tmp_dir/home" python3 check_installed_parity.py \
    --dest "$tmp_dir/skills" \
    --retired-wrapper "$tmp_dir/bin/obsolete-wrapper"
  rm -rf "$tmp_dir"
)
```

### Repository integrity

```bash
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_markdown_links.py
python3 scripts/check_retired_paths.py
python3 scripts/scan_content_secrets.py
```

The combined workflow is [`sql-integrity.yml`](.github/workflows/sql-integrity.yml). It runs for changes to the public documentation, active skills, workflows, integrity scripts, and retired-path deletions. MCP package CI now runs in the standalone repository. Neither workflow deploys or connects to a database.

## State reporting

| State | Evidence |
| --- | --- |
| Local | `git status` and local command output |
| Pushed | A commit exists on a remote branch |
| CI | GitHub reports a result for the relevant workflow run |
| Deployed | A runtime or Azure resource changed and a live smoke check passed |

These states are separate. A local edit or push does not imply CI success, deployment, or database access.
