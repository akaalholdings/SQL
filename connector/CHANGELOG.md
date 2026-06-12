# connector.py — Changes vs the Original `query_geneva_db` Skill

The original was a single-purpose tool: Azure SQL only, regex safety filter, dump-everything-to-stdout. It evolved in four rounds into the current connector: a read-only query, comparison, and inspection interface across Azure SQL and Databricks Unity Catalog.

## Round 1 — Security & operational hardening

- **TLS fixed**: removed `TrustServerCertificate=yes` (which disabled certificate validation) and made `Encrypt=yes` unconditional — the original left ODBC Driver 17 connections unencrypted.
- **Read-only enforcement repositioned**: the regex filter is documented as defense-in-depth only; the real boundary is database-side permissions (`db_datareader` via an Entra ID group — setup SQL in the module docstring). `ApplicationIntent=ReadOnly` is flagged as a routing hint, not a security control.
- **Stronger query gate**: extended forbidden keywords (`WAITFOR`, `DECLARE`, `SET`, `OPENROWSET`, `DBCC`, …) and added detection of semicolon-less batches (`SELECT 1 SELECT 2` is now rejected; `UNION`/`EXCEPT`/`INTERSECT` still allowed).
- **Row cap**: `--max-rows` (default 500) with `fetchmany()` and truncation markers — the original `fetchall()` could exhaust memory or flood an LLM's context window.
- **Timeouts**: `--timeout` statement timeout plus a connection timeout (the original had none).
- **Output formats**: `--format table|csv|json` with proper NULL rendering, tab/newline escaping, and datetime/Decimal/bytes serialization (the original printed raw `str()` TSV with NULLs as `None`).
- **Plan capture**: `--plan estimated|actual` emits execution plan XML for evidence-based optimization workflows.
- **Audit log**: a JSONL record per run (timestamp, alias, query, rows, duration, status) written to `~/.copilot/skills/query_geneva_db/audit.jsonl`; row count and elapsed time printed per run. Disable with `--no-audit`.
- **Production gating with teeth**: a `"production"` config flag plus a required `--allow-prod` to proceed (the original printed a hardcoded warning and continued anyway).
- **Bug fixed**: `extract_sql_block` sliced CTEs at their inner SELECT, so `;WITH … SELECT` inputs produced broken SQL even in the original.
- Smaller items: friendly hints for the common Azure errors (firewall IP rejection, missing Entra ID contained user, timeouts), numeric ODBC driver sorting, one-line UTF-16LE token encoding, and clean `--query-file` error handling.

## Round 2 — Databricks Unity Catalog + data compare

- **Second engine**: `"type": "Databricks"` aliases (hostname, `http_path`, default catalog/schema), authenticated through the same `az login` session (or a `DATABRICKS_TOKEN` PAT), with `databricks-sql-connector` imported lazily so SQL-only users don't need it.
- **Engine-aware safety**: backtick identifier masking, `SHOW`/`DESCRIBE` allowed as read statements on Databricks only, and Delta/UC write-and-maintenance commands blocked (`COPY`, `VACUUM`, `OPTIMIZE`, `CACHE`, `CLONE`, …).
- **`--compare SECOND_ALIAS`**: runs the query on both engines and diffs the result sets — order-insensitive multiset comparison with cross-engine value canonicalization (`Decimal("1.50")` equals `1.5`, ISO datetimes, NULL handling), `--query2`/`--query-file2` for dialect differences, sample mismatch rows in the output, and exit code 2 when differences are found.

## Round 3 — Dependency installers

- **PEP 723 inline metadata** so `uv run connector.py …` auto-resolves all dependencies into an isolated environment.
- **`--install-deps`** flag pip-installs `pyodbc`, `azure-identity`, and `databricks-sql-connector` into the current interpreter (`sys.executable -m pip`, so it can't hit the wrong Python).
- **Lazy imports with friendly errors** naming the exact missing package and all install routes — the original crashed on `--help` if `pyodbc` wasn't installed.

## Round 4 — Table shape inspection

- **`--describe TABLE`**: returns a normalized table shape (columns, raw types, engine-neutral type buckets, nullability, row count) from `INFORMATION_SCHEMA.COLUMNS` on SQL Server/Synapse and `information_schema.columns` on Unity Catalog, with the alias's default catalog/schema applied to partial names. Ambiguous unqualified SQL names produce an error listing candidate schemas instead of guessing.
- **`--describe` + `--compare` (+ `--describe2`)**: cross-engine shape diff with type normalization — `nvarchar(100)` vs `STRING`, `bit` vs `BOOLEAN`, `money` vs `DECIMAL` count as matches, while real renames, type changes, nullability flips, and row-count drift are reported individually. Exit code 2 on mismatch; structure and row-count differences are called out separately.
- **Usage rule codified** in the docstring: this tool is a trusted inspection interface for *evidence* (legacy SQL checks, Delta table facts, migration validation) — not a data-access layer for pipeline or runtime code.

## Verification

All changes were regression-tested (~35 cases): the safety gate on both engines, value canonicalization, shape fetchers via fake cursors, table-reference parsing (including bracket/backtick-quoted dotted names), and injection hygiene on table references.

## Deployment prerequisites

- Database-side read-only setup: `db_datareader` (Azure SQL) and `GRANT USE CATALOG / USE SCHEMA / SELECT` (Unity Catalog) for the reader group — see the module docstring.
- `SKILL.md` should document the current flags (`--compare`, `--describe`, `--plan`, `--install-deps`, `--allow-prod`, and the exit-code-2 convention) for Copilot-driven use.
- Keep real `DB_ENVIRONMENTS` entries (server names, warehouse paths) out of this public repository.
