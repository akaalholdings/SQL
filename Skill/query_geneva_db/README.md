# query_geneva_db

Terminal CLI for Geneva data access with:

- read-only SQL execution (Azure CLI auth + ODBC),
- safety guardrails (single `SELECT`/`WITH`, no DML/DDL, no `SELECT *`),
- NL2SQL mode without OpenAI APIs (uses metadata catalog query to map tables/columns),
- sampled output for large result sets,
- generated SQL preview/save for user validation.

## 1. Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 2. Configure DB aliases

Copy and edit the template:

```bash
cp db_environments.example.json db_environments.json
```

Then fill real `server`, `database`, `description`, and `type` values.

The CLI reads aliases from:

1. `--db-config <path>` (highest priority)
2. `QUERY_GENEVA_DB_CONFIG` env var
3. `./db_environments.json`

## 3. Azure login prerequisite

```bash
az login
```

Azure SQL server prerequisites (one-time):

- Configure an Entra admin on the SQL server.
- Allow client IP in SQL server firewall rules.

## 4. Usage

Show help and aliases:

```bash
query_geneva_db --help
```

Run SQL directly:

```bash
query_geneva_db mid_dev "SELECT TOP (50) [DATASET_KEY] FROM [cns_glb_reference].[DATASET];"
```

Run from file:

```bash
query_geneva_db mid_dev -f query.sql
```

Run NL request (auto-generated SQL from reference catalog):

```bash
query_geneva_db mid_dev --mode nl "show active projects updated today"
```

Auto mode (`sql` vs `nl` detection):

```bash
query_geneva_db mid_dev --mode auto "show orders above 1000 today"
```

Preview-only (no execution):

```bash
query_geneva_db mid_dev --mode nl --dry-run --save-query generated.sql "orders above 1000 today"
```

## 5. Reference catalog query

NL2SQL mode uses the built-in query in:

- `src/query_geneva_db/resources/reference_catalog.sql`

Override with your own:

```bash
query_geneva_db mid_dev --mode nl --reference-query-file my_reference.sql "request text"
```

## 6. Output truncation behavior

The CLI prints at most `--preview-rows` rows (default `50`).

- This keeps terminal output reviewable.
- When results exceed the limit, the CLI prints a truncation notice.
- Increase rows with `--preview-rows N` or refine filters.

## 7. Copilot skill install

Install bundled skill markdown:

```bash
query_geneva_db --install-skill
```

Destination:

- `~/.copilot/skills/query_geneva_db/SKILL.md`
