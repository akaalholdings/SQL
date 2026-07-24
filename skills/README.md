# Azure SQL performance skills for VS Code Copilot

This folder contains the three maintained Copilot skills:

- `sql-health-triage`: read-only incident and health diagnosis.
- `sql-optimizer`: iterative single-query rewriting, equivalence checking, benchmarking, and sandbox index experiments.
- `sql-plan-enforcer`: reviewed Query Store plan controls with verification and exact rollback.

Each installed bundle contains one authoritative file: `SKILL.md`. Database execution, durable cases, tuning sessions, index leases, and plan-action intents belong to `azure-sql-mcp`.

## Choose the skill

| Need | Skill | MCP profile |
| --- | --- | --- |
| Diagnose broad slowness, blocking, waits, resource pressure, regressions, or an incident | `sql-health-triage` | `triage` |
| Rewrite and tune one SELECT query without database writes | `sql-optimizer` | `optimizer` |
| Test a temporary index in an approved non-production database | `sql-optimizer` | `sandbox` |
| Review Query Store plan stability without changes | `sql-plan-enforcer` | `enforcer-review` |
| Apply one explicitly authorized prepared plan action | `sql-plan-enforcer` | `enforcer-apply` |

Do not use the plan-enforcement skill for query rewrites or index changes. Do not use the optimizer for a broad incident before triage identifies the query-shaped problem.

## Prerequisites

- VS Code with GitHub Copilot Chat.
- Python 3.12 or newer.
- A local checkout of this repository.
- A separate local checkout of `azure-sql-mcp` 2.1.0 or newer for measured
  tuning, multi-hour budgets, leased index tests, and durable view changes.
- Azure SQL connection settings supplied locally, outside Git.
- For active benchmarks or writes, a local database policy file that explicitly permits the target database and operation.

The skills still work without MCP for static analysis. In that mode the optimizer must return concrete unmeasured rewrites and must not invent performance metrics.

## Install for VS Code Copilot

From the repository root:

```bash
python3 skills/install_all.py --dest "$HOME/.copilot/skills"
python3 skills/check_installed_parity.py --dest "$HOME/.copilot/skills"
```

The collection installer stages all three skills, replaces each managed bundle transactionally, removes stale files from earlier skill versions, and archives obsolete payloads found under the selected destination or known user-level Copilot, Claude, Agents, and Codex skill roots. It changes no unrelated skill directory.

Reload the VS Code window after installation so Copilot refreshes skill metadata.

To install one skill intentionally:

```bash
python3 skills/sql_optimizer/install.py --dest "$HOME/.copilot/skills"
```

A single-skill install does not synchronize the other two. Use `install_all.py` for normal upgrades.

## Configure MCP locally

Run `azure-sql-mcp` through local stdio from VS Code. Create `.vscode/mcp.json` in the workspace, replace only the placeholders, and keep the file uncommitted:

```json
{
  "servers": {
    "azure-sql-optimizer": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/azure-sql-mcp",
        "run",
        "azure-sql-mcp"
      ],
      "env": {
        "AZURE_SQL_SERVER": "your-server.database.windows.net",
        "AZURE_SQL_DEFAULT_DATABASE": "your-database",
        "AZURE_SQL_ALLOWED_DATABASES": "your-database",
        "AZURE_SQL_AUTH_MODE": "entra-default",
        "AZURE_SQL_ACCESS_MODE": "restricted",
        "AZURE_SQL_WRITE_POLICY": "disabled",
        "AZURE_SQL_PROFILE": "optimizer",
        "AZURE_SQL_TOOL_GROUPS": "core,performance",
        "AZURE_SQL_DATABASE_POLICY_FILE": "/protected/path/azure-sql-policy.json"
      }
    }
  }
}
```

Use an Azure CLI login or managed identity for `entra-default`. Keep server names, database names, tenant information, usernames, passwords, tokens, and policy paths local. On first use, reload VS Code, enable the server in Copilot Chat, call `list_databases`, then call `check_capabilities`. Measured tuning requires `azure-sql-mcp` 2.1.0 or newer and `mcp_contract.performance_tuning=1`; restart-safe view work also requires `mcp_contract.durable_view_change=1`. Version 2.1.0 sizes the outer workflow timeout from the local per-request execution ceiling and the query timeout, then bounds it by the durable session deadline, so a policy-authorized multi-hour campaign is not cancelled by the old one-query wrapper. If either contract is missing, update `azure-sql-mcp` or stay in the optimizer's static, unmeasured mode. Select only a returned database that is in the configured allowlist.

For static rewrites, MCP and the policy file are optional. For measured rewrites, the selected policy entry must allow reads and benchmarks. For a disposable index or view test, change to a separate local server entry with `AZURE_SQL_PROFILE=sandbox`, `AZURE_SQL_TOOL_GROUPS=core,performance,admin`, local stdio, `AZURE_SQL_ACCESS_MODE=unrestricted`, `AZURE_SQL_WRITE_POLICY=apply`, and a non-production database policy. View apply also requires `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`; index testing does not. Never use that entry for production.

## Database policy

Profiles limit the exposed workflow; the local policy limits what a particular database may do. Both must allow an operation.

A policy entry controls:

- read access;
- repeated benchmarks;
- disposable test indexes;
- prepared sandbox view changes;
- prepared plan actions;
- maximum benchmark executions;
- environment classification.

Keep this synthetic policy outside Git and set its path in `AZURE_SQL_DATABASE_POLICY_FILE`:

```json
{
  "version": 1,
  "databases": {
    "your-sandbox-database": {
      "environment": "sandbox",
      "allow_read": true,
      "allow_benchmark": true,
      "allow_test_indexes": true,
      "allow_view_apply": true,
      "allow_plan_apply": false,
      "max_benchmark_executions": 80,
      "max_tuning_candidates": 60,
      "max_tuning_session_executions": 2000,
      "max_tuning_session_minutes": 360
    },
    "your-production-database": {
      "environment": "production",
      "allow_read": true,
      "allow_benchmark": false,
      "allow_test_indexes": false,
      "allow_view_apply": false,
      "allow_plan_apply": false,
      "max_benchmark_executions": 0,
      "max_tuning_candidates": 0,
      "max_tuning_session_executions": 0,
      "max_tuning_session_minutes": 0
    }
  }
}
```

Unknown databases fail closed for benchmarks, temporary indexes, and plan apply. Production should remain read-only unless a reviewed exception is deliberately configured. The policy file is local and uncommitted.

## Use in Copilot Chat

Name the skill and give it the smallest useful scope.

### Read-only incident triage

```text
Use sql-health-triage. Diagnose why requests timed out in the last 30 minutes.
Stay read-only, use the configured database I select, and show evidence gaps.
```

Expected behavior:

- starts a shared performance case;
- records collection windows, units, availability, truncation, provenance, and stable identities;
- returns exactly `healthy`, `actionable`, `partial`, or `inconclusive`;
- hands query work to the optimizer or plan review by case id;
- changes nothing.

### Static rewrite before a plan exists

```text
Use sql-optimizer. Rewrite this Azure SQL query now from the text alone.
Mark it unmeasured, preserve duplicates, NULLs, ordering and ties, then tell me what evidence to collect.
<synthetic query>
```

Expected behavior:

- states the semantic contract;
- returns concrete SQL when a safe rewrite is possible;
- inspects all six candidate families;
- treats missing plan evidence as lower confidence, not a reason to stop.

### Iterative measured tuning

```text
Use sql-optimizer with the optimizer profile. Open a tuning session for this SELECT.
Test one change at a time across common, rare, NULL and boundary parameters.
Use up to 360 minutes, 60 candidates, and 2000 executions if the local policy permits.
Continue after losing candidates and return the full leaderboard and winning SQL.
<synthetic query>
```

Expected behavior:

- uses the requested tuning budget; 10 candidates, 80 executions, and 20 minutes are compatibility defaults rather than a ceiling;
- reads `check_capabilities.local_tuning_policy` and, for an open-ended “fastest version” request, uses the largest useful policy-authorized campaign instead of silently choosing 20 minutes;
- can run a policy-authorized multi-hour campaign and stops only at the configured limit, exhausted credible candidates, or an evidence-based diminishing-return point;
- executes each measured query once per sample;
- binds every parameter value with its exact SQL type through `sp_executesql`;
- uses a one-snapshot, duplicate- and order-aware full-result comparison for finalists;
- records every candidate as improved, neutral, regressed, equivalence_failed, inconclusive, or cleanup_required;
- does not let a slower index end the session.

Rewrite screening costs six executions per parameter case and normally defers the full comparison. Finalist validation costs twelve per case, including the two-query comparison. Four finalist cases therefore cost 48 executions. Use a representative screening subset so the configured session budget explores several ideas before promoting finalists.

### Sandbox index test

```text
Use sql-optimizer with the sandbox profile on the approved non-production database.
Benchmark this disposable index candidate, enforce the lease, and confirm cleanup.
<synthetic query and candidate definition>
```

The sandbox profile and database policy must both permit temporary indexes. Screening uses A-B-A measurements at nine executions per case; a four-case finalist costs 60. The SQL is unchanged, so MCP checks complete result stability across the three DDL-separated phases and verifies that the expected index was used. Cleanup failure blocks another test and returns the lease plus rollback instructions.

### Sandbox view test

```text
Use sql-optimizer. Prepare this view definition, show dependencies and exact rollback,
then apply it only through the approved sandbox profile and verify the definition.
<synthetic view body>
```

`prepare_view_change` is read-only in both profiles, but an optimizer preparation is preview-only and cannot cross into another MCP process. Re-prepare in the local sandbox with the same stable idempotency key, policy `allow_view_apply=true`, and `AZURE_SQL_PERSIST_VIEW_SQL_STATE=true`. That explicit opt-in stores the exact target and prior view definitions in the permission-restricted MCP state database so apply, verify, and rollback can recover after a restart. After an interruption, verify the existing durable change id; do not prepare against the possibly changed view. Changing a production view remains a separate owner-approved deployment.

### Plan review

```text
Use sql-plan-enforcer in review mode. Review Query Store regressions for the selected database.
Do not prepare or apply anything.
```

### Prepared plan action

```text
Use sql-plan-enforcer. Prepare the reviewed action for this evidence id.
Show the exact prior-state reference and verification contract; do not apply yet.
```

Applying is a separate user-authorized step through a local stdio `enforcer-apply` server using unrestricted/apply posture, `AZURE_SQL_TOOL_GROUPS=core,performance,admin`, a database policy that permits the action, an open apply kill switch, and a prepared intent id. Direct force, hint, unrestricted SQL, and raw apply paths are not valid skill workflows.

## Inline/edit context

Use inline or edit mode only for static SQL rewriting in an open file. Ask the optimizer to preserve the recorded semantic contract and return the edit as unmeasured. Move to Copilot Chat or an agent/task context for MCP evidence, repeated benchmarks, index leases, or prepared plan actions so the complete session and results remain visible.

## Agent/task context

Use an agent/task for multi-step triage or tuning when Copilot must call several MCP tools. Set the profile when starting the MCP server, not inside SQL. Keep one performance case and one tuning session per problem; opening replacement sessions must not bypass budgets.

Plan apply requires explicit authorization for each prepared intent. A long-running task does not broaden that authorization.

## Upgrade safely

1. Pull the intended repository revision.
2. Run the collection installer against the same destination used previously.
3. Run parity.
4. Reload VS Code.
5. Run the clean-room optimizer acceptance prompt before work use.

Print the synthetic prompt, paste it into a new Copilot Chat with no prior SQL
context, save the response outside the repository, then validate it:

```bash
python3 scripts/copilot_optimizer_acceptance.py --print-prompt
python3 scripts/copilot_optimizer_acceptance.py --response /tmp/copilot-response.md
```

The validator extracts only fenced `sql`/`tsql` blocks and validates the actual
synthetic rewritten query: complete SELECT shape, the typed SARGable lower and
upper bounds, and removal of the wrapped date conversion. Prose, a non-SQL
fence, or the unchanged source query cannot satisfy the SQL requirement. It
also requires `unmeasured`, the semantic contract, the slower index as
`regressed`, and explicit continuation. It prints requirement names only on
failure and never echoes the response.

```bash
git pull --ff-only
python3 skills/install_all.py --dest "$HOME/.copilot/skills"
python3 skills/check_installed_parity.py --dest "$HOME/.copilot/skills"
```

Before replacement, the installer archives prior managed bundles as well as retired skill/wrapper payloads across `~/.copilot/skills`, `~/.claude/skills`, `~/.agents/skills`, and `~/.codex/skills`. Parity checks the same discovery roots. The protected archive defaults under `~/.azure-sql-mcp/backups/retired-skills/`. It may contain historical private state; keep its permissions restricted and do not commit it.

## Verify source and release behavior

From the repository root:

```bash
uv run --with pytest pytest -q skills scripts/tests
uv run --with ruff ruff check skills scripts
python3 scripts/check_markdown_links.py
python3 scripts/check_retired_paths.py
python3 scripts/scan_content_secrets.py
```

Verify a clean isolated install:

```bash
tmp="$(mktemp -d)"
HOME="$tmp/home" python3 skills/install_all.py \
  --dest "$tmp/skills" \
  --backup-root "$tmp/backups" \
  --retired-wrapper "$tmp/bin/obsolete-wrapper"
HOME="$tmp/home" python3 skills/check_installed_parity.py \
  --dest "$tmp/skills" \
  --retired-wrapper "$tmp/bin/obsolete-wrapper"
rm -rf "$tmp"
```

The placeholder wrapper path in this isolated command is intentionally nonexistent.

## Troubleshooting

### Copilot still follows old instructions

Confirm the install destination, run parity, then reload the VS Code window. Parity must report exactly one `SKILL.md` in each managed bundle.

### The optimizer asks only for a plan

Confirm the installed `sql_optimizer/SKILL.md` matches source. Its first-response rule requires a concrete static rewrite whenever one is safely possible. Run the clean-room acceptance prompt after parity.

### A benchmark is policy denied

Check the active profile and the local database policy. The `optimizer` profile does not grant database permission by itself. Do not widen production policy to make a test pass; use static analysis or an approved non-production database.

### A temporary index was not cleaned up

Stop new index experiments. Use the MCP lease id and rollback action to reconcile cleanup. The candidate remains `cleanup_required` until MCP confirms removal.

### Plan apply is blocked

Check that review produced a prepared intent, the evidence and prior-state hashes still match, the kill switch is open, ownership is manual, the server runs `enforcer-apply`, and the database policy permits plan apply. Do not fall back to a direct mutation tool.

### Triage reports partial

Read the evidence-gap section. Missing permissions, Query Store coverage, truncated rows, mismatched windows, or missing parameter buckets correctly prevent a healthy/actionable conclusion. Fix only the narrow evidence gap and recollect the same case.
