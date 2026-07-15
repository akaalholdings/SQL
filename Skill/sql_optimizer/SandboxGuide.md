# Sandbox Guide (database-clone testing)

How to run index tests and DML tuning against a **clone** of the production database instead of production itself. A clone carries production data, statistics, and Query Store history — so measurements transfer — while making every risk local: a bad test index, a runaway UPDATE, or a locked table on the clone costs nothing.

Use a sandbox whenever: testing candidate indexes (scenario 3), tuning `INSERT`/`UPDATE`/`DELETE`/`MERGE` statements (the only way — see DML below), running parameter buckets that would hammer production, or the user wants standing approval instead of per-index sign-off.

## 1. Create the clone (operator action, once)

Azure SQL Database copies are transactionally consistent, statistics included. The **operator** creates it — this skill never runs database-level DDL. Two equivalent routes:

```sql
-- T-SQL, connected to master on the logical server:
CREATE DATABASE awlt_tuning AS COPY OF awlt_prod;
```

```bash
# or Azure CLI:
az sql db copy --resource-group <rg> --server <server> --name awlt_prod --dest-name awlt_tuning
```

Cost control: copy to a smaller service objective (`--service-objective`) or serverless with auto-pause — tuning workloads are intermittent. The copy is point-in-time; refresh it (drop + re-copy) when production drifts enough to matter (schema changes, large data skew shifts).

## 2. Expose the clone to the server (operator action)

Add the clone to the running `azure-sql-mcp` server's `AZURE_SQL_ALLOWED_DATABASES`, restart it, confirm with `list_databases`. For a fully autonomous sandbox session the server also needs `AZURE_SQL_ACCESS_MODE=unrestricted` and `AZURE_SQL_WRITE_POLICY=apply` — acceptable *because* the target is a clone.

**Keep the blast wall:** if production must stay reachable read-only in the same session, remember the server's write gate is global, not per-database. The clean setup is a dedicated server instance (or session) whose `AZURE_SQL_ALLOWED_DATABASES` lists **only** the clone when apply is on. State that setup in the response before any write work.

## 3. Work on the clone

- **Standing approval applies.** On a database the user has designated as a sandbox clone, test-index create/drop (`RunGuide.md` step 4) and approved DML runs do not need per-action sign-off — the designation *is* the approval. Confirm once at session start: "treating `awlt_tuning` as the sandbox, standing approval for test DDL — correct?"
- **Measurements transfer with caveats, state them:** the clone's service objective (if smaller), cold buffer pool on first runs (warm up per the repetition rule), and no concurrent production workload. Logical reads and plan shape transfer well; wall-clock duration transfers only between same-SLO databases.
- **Query Store on the clone** starts as a copy of production's — history-based evidence (`tune_query`, `get_top_queries`) reflects production behavior at copy time.

## DML and stored-procedure tuning (the sandbox is the only home)

Write statements cannot be benchmarked on production through this skill (read-only validator; and you never benchmark a live `DELETE` anyway). On the clone:

1. Baseline: run the original DML via `execute_tsql_unrestricted`? **No** — DML is hard-denylisted there too. The operator runs DML baselines directly against the clone (SSMS/sqlcmd) and pastes timings, or wraps the statement in a transaction with `SET STATISTICS TIME/IO ON` and rolls back. This skill captures the *plan* read-only via `explain_query(analyze=false)` (estimated — actual-plan capture would execute the write).
2. Rewrite per `queryguide.md` §1.4 (batching, SARGable predicates, MERGE decomposition), and compare estimated plans + operator-run timings.
3. Equivalence for DML = same rows affected + same end-state checksum on the touched table(s), verified by the operator between resets (re-copy or transaction rollback).

State plainly in the response which measurements were operator-run — the skill orchestrates and analyzes; the clone plus the operator execute.

## Teardown

```sql
DROP DATABASE awlt_tuning;   -- operator, when the tuning engagement ends
```

Remove it from `AZURE_SQL_ALLOWED_DATABASES`. Nothing in this skill's state needs cleanup — audit records reference the clone by name and stay valid history.
