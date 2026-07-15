# SQL

Private Azure SQL Database tooling and agent skills:

- `Skill/`
  Canonical source for the `sql-health-triage`, `sql-optimizer`, and
  `sql-plan-enforcer` skills, including install/parity tooling and tests.
- `azsql-BulkCopy/`
  Chunked Azure SQL Database to Azure SQL Database bulk copy tool for large table seed loads.

## Quick Start

```bash
cd azsql-BulkCopy
cp .env.example .env
python azure_sql_bulkcopy.py
```

See [azsql-BulkCopy/README.md](azsql-BulkCopy/README.md) for configuration and tuning guidance.

See [Skill/README.md](Skill/README.md) for the
Azure SQL performance skill workflow and installation commands.
