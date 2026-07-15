---
name: query_geneva_db
description: CLI tool for safe, read-only Geneva SQL execution and metadata-driven NL2SQL mapping using GitHub Copilot workflows.
---

# Geneva DB Query Skill (Copilot + Terminal)

## Purpose
Use `query_geneva_db` to:

1. Execute a safe, single read-only SQL statement.
2. Or convert natural language to SQL using the Geneva reference dataset catalog query, then execute.

This is designed for environments without OpenAI API access.

## Tool usage

### Direct SQL mode
```bash
query_geneva_db <db_alias> --mode sql "SELECT TOP (50) ...;"
```

### Natural language mode
```bash
query_geneva_db <db_alias> --mode nl "show shipments delayed today"
```

### Auto mode
```bash
query_geneva_db <db_alias> --mode auto "<sql-or-natural-language>"
```

### Long input
```bash
query_geneva_db <db_alias> -f request_or_query.txt
cat request_or_query.txt | query_geneva_db <db_alias> --stdin
```

## Guardrails
- Only one statement.
- Must be `SELECT` or `WITH`.
- No write or DDL keywords.
- No `SELECT *`.
- Output is sampled (`--preview-rows`, default 50).

## NL2SQL behavior
In `--mode nl`, the tool:

1. Executes reference catalog SQL to gather dataset + column metadata.
2. Scores candidate datasets against request keywords.
3. Picks top dataset and likely columns.
4. Generates `SELECT TOP (N)` SQL.
5. Shows the generated SQL and candidate mapping summary.

Use `--dry-run` and `--save-query` to review/edit SQL before running.

## Reference catalog query
Default:
- `src/query_geneva_db/resources/reference_catalog.sql`

Override:
```bash
query_geneva_db <db_alias> --mode nl --reference-query-file custom_reference.sql "request"
```

## Recommended Copilot workflow
1. Ask Copilot for a domain question breakdown.
2. Use `query_geneva_db --mode nl --dry-run` to generate SQL candidate.
3. Review SQL (`--show-query` / `--save-query`).
4. Execute once validated.
