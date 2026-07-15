# DEPRECATED: `query_geneva_db`

**Status: archived and unsupported. Do not use for new work.**

This directory is preserved under `legacy/query_geneva_db/` for historical reference only.
`query_geneva_db` is not an active skill and is deliberately excluded from the maintained
skill installer, installed-copy parity checks, active CI, and current workflow guidance.
There is no compatibility, security, or operational support contract for this archive.
The former discovery entrypoint is retained as `SKILL.deprecated.md`; there is deliberately
no `SKILL.md` in this directory.

## Replacement routing

All new Azure SQL work must use the maintained collection and the
[`azure-sql-mcp`](../../azure-sql-mcp/README.md) server as its execution
channel:

- Health symptoms, incident diagnosis, or a database health sweep ->
  [`skills/sql_health_triage/SKILL.md`](../../skills/sql_health_triage/SKILL.md)
- One query, actual-plan review, rewrite, or index recommendation ->
  [`skills/sql_optimizer/SKILL.md`](../../skills/sql_optimizer/SKILL.md)
- Fleet-wide Query Store plan review or reversible plan controls ->
  [`skills/sql_plan_enforcer/SKILL.md`](../../skills/sql_plan_enforcer/SKILL.md)
- Collection installation, handoffs, prerequisites, and safety gates ->
  [`skills/README.md`](../../skills/README.md)

The maintained skills do not use this CLI. They use `azure-sql-mcp` and preserve the separate
read-only, optimization, and plan-enforcement boundaries documented above.

## Archival execution warning

Do not run, install, package, or connect with the code in this directory. Its historical CLI
may use old Azure CLI/ODBC authentication, database aliases, environment variables, and
connection behavior that are not maintained or reviewed for current targets. Running it can
attempt database access with whatever local configuration is available. The archive is not a
safe fallback when the MCP server is unavailable.

If historical behavior must be inspected, review the files offline and isolate any experiment
from live credentials and databases. Do not treat a successful archival execution as support
or evidence for the maintained SQL workflow.

This notice intentionally contains no installation or usage instructions. Route new work
through `azure-sql-mcp` and the three maintained skills instead.
