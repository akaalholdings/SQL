---
name: query_geneva_db
description: CLI tool for running safe, read-only SQL queries against Geneva databases (SQL Server and Synapse) using Azure CLI auth. Designed for GitHub Copilot (VS Code) workflows where Copilot performs NL2SQL and this tool executes SQL.
---

# Geneva DB Query Skill (Copilot NL2SQL + CLI Execution)

## What this skill does
This skill supports a **GitHub Copilot in VS Code** workflow where:

1) A user asks a question in natural language (NL).  
2) **Copilot converts NL → SQL** using the metadata query below and Geneva naming conventions.  
3) The `query_geneva_db` CLI executes the resulting **single read-only SELECT** against the chosen database alias.

This avoids guessing schema details and reduces broken SQL and hallucinated columns.

---

## Primary tool: `query_geneva_db`

### Basic usage
```bash
query_geneva_db <db_alias> "<sql_query>"
```

### Copilot-friendly usage (recommended for long queries)
```bash
query_geneva_db <db_alias> -f path/to/query.sql
```

```bash
cat path/to/query.sql | query_geneva_db <db_alias> --stdin
```

### Important constraints (enforced)
- Only **one** statement is allowed.
- Query must be **read-only**: **SELECT** or **CTE starting with WITH** (the tool also tolerates `;WITH`).
- No DML/DDL keywords (INSERT/UPDATE/DELETE/MERGE/CREATE/DROP/ALTER/TRUNCATE/EXEC…)
- No `SELECT ... INTO` (write operation).
- No `SELECT *` (strongly discouraged; Copilot should name columns explicitly).
- Results should be bounded (use `TOP (N)` where returning rows).

### Input cleanup (handled by the CLI)
The CLI is designed to tolerate common Copilot output:
- Removes Markdown fences like ```sql ... ```
- Removes SSMS batch separators like `GO`
- Removes SQL comments
- If the input contains preamble text plus SQL, it attempts to extract the SQL block

Even though the tool cleans these up, **Copilot should still output raw SQL only** when possible.

---

## How to choose the right database (aliases)
Run:
```bash
query_geneva_db --help
```
to see the up-to-date list of supported aliases grouped by database type.

Geneva data lives in two platforms:
- **SQL Server (Azure SQL)**: operational and reference data, plus some consumption views
- **Synapse (DNA platform)**: large-scale curated and consumption tables, especially for domain pipelines

When you’re unsure where a table lives, start with `INFORMATION_SCHEMA.TABLES` on a likely alias.

---

## Geneva Data Architecture (Medallion Flow)
Geneva uses a tiered “Medallion” architecture:

1. **RAW (`raw_*` schemas)**: ingested source format  
   - Example: `raw_cpo_mid.Shell_AD_data`

2. **STAGE & DELTA (`trf_*_stage`, `trf_*_delta`)**: temporary transformation + incremental checks  
   - Example: `trf_cpo_mid_stage.SHELL_AD_PERSON_DL`

3. **CURATED (`trf_*`)**: cleaned/validated/deduped; source-oriented naming  
   - Examples: `trf_cpo_mid`, `trf_cpo_mid_capacity`

4. **CONSUMPTION (`cns_*`)**: modeled for analytics/reporting (often star-schema-ish)  
   - Examples: `cns_glb_reference`, `cns_cpo_mid_dim`
   - Common patterns:
     - `D_*` = dimensions (filters/descriptive)
     - `F_*` = facts (measures/aggregates)

Utility schemas:
- `refresh` (environment sync / copy-back history)
- `process_control` (ETL telemetry/execution logs)

---

## Schema discovery playbook
### Basic discovery
```sql
SELECT TOP 50 TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

### SQL Server common schemas scan
```sql
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN (
   'cns_glb_reference',
   'cns_cpo_mid_reference',
   'cns_cpo_mid_dim',
   'cns_cpo_mid_capacity',
   'cns_cpo_mid_nonstd',
   'trf_cpo_mid_capacity',
   'process_control',
   'refresh'
)
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

### Synapse common schemas scan
```sql
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN (
   'cns_glb_reference',
   'cns_cpo_mid',
   'cns_cpo_mid_capacity',
   'cns_cpo_mid_dim',
   'trf_cpo_mid',
   'trf_cpo_mid_delta',
   'raw_cpo_mid'
)
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

---

## Metadata-driven NL2SQL workflow (Copilot)
When a user asks a question in natural language, Copilot should:

1) **Run the Reference Dataset Catalog Query** (below) to learn datasets, columns, descriptions, relationships.  
2) Build a mental map:
   - dataset technical names (likely views/tables)
   - column names, meanings, types
   - relationship hints (“Ref. Technical Name”, dropdown filters)
3) Generate a **single** SQL Server style query that:
   - starts with `SELECT` or `WITH`
   - uses explicit columns (no `*`)
   - uses `TOP (N)` for row outputs
   - uses appropriate `WHERE` filters (prefer catalog columns flagged for filtering)
   - joins using `_KEY` patterns + referenced dataset hints
4) Execute via:
```bash
query_geneva_db <alias> "<sql>"
```

### Key heuristics
- Columns ending in `_KEY` are often join keys.
- Prefer pairs like `*_CD` (code) + `*_NM` (name) for readability.
- For “today”: find the appropriate domain date column and filter using `CAST(col AS date) = CAST(GETDATE() AS date)` (SQL Server style), unless a better domain rule exists.

---

## Reference Dataset Catalog Query (SQL Server style)
Run this query to get the dataset/column catalog.

```sql
SELECT
    DC.[DATASET_COLUMN_KEY] AS 'Key',
    DP.data_provider_abbr AS 'Data Provider',
    DS.dataset_nm AS 'Dataset Name',
    DS.DATASET_TECHNICAL_NM AS 'Technical Name',
    DSP.dataset_purpose_type_cd AS 'Purpose',
    DC.[COLUMN_NM] AS 'Column Name',
    DC.[COLUMN_DESC] AS 'Description',
    DC.[COLUMN_BUSINESS_NM] AS 'Business Name',
    DC.[COLUMN_LABEL_NM] AS 'Label Name',
    DT.DATA_TYPE_CD as 'Data Type',
    RDP.data_provider_abbr AS 'Ref. Provider',
    RDS.dataset_nm AS 'Ref. Dataset Name',
    RDS.DATASET_TECHNICAL_NM AS 'Ref. Technical Name',
    DC.[API_INCLUDED_IND] AS 'In API?',
    DC.[DISPLAY_SEQUENCE_NUM] AS 'Display Seq',
    DC.[FILTER_USAGE_IND] as 'Filter?',
    DC.[MANDATORY_IND] as 'Mandatory?',
    DD.COLUMN_NM AS 'Dropdown Filter',
    DC.[STANDARD_COLUMN_IND] AS 'Std?',
    DE.DATA_ELEMENT_TECHNICAL_NM AS 'Data Element',
    DC.DATA_ELEMENT_IN_CONTEXT_IND as 'In Context?',
    DC.DATASET_COLUMN_GROUP_NUM AS 'Group Num',
    DF.DISPLAY_FORMAT_NM AS 'Display Format',
    DC.DISPLAY_DEFAULT_IND AS 'Display Default?',
    DC.[DELETE_IND] AS 'Del?',
    DC.[META_QUALITY_CD] AS 'META Quality',
    DC.[META_ACTION_CD] AS 'META Action',
    DC.[META_CREATED_DTTM] AS 'META Created',
    DC.[META_CREATOR_NM] AS 'META Creator',
    DC.[META_CHANGED_DTTM] AS 'META Changed',
    DC.[META_CHANGED_BY_NM] AS 'META Changed By',
    DC.[RECORD_ENTRY_DTTM] AS 'Entry Date'
FROM [cns_glb_reference].[DATASET_COLUMN] DC
LEFT OUTER JOIN [cns_glb_reference].[dataset] DS
    ON DC.DATASET_KEY = DS.dataset_key
LEFT OUTER JOIN [cns_eis_controls].[data_provider] DP
    ON DS.data_provider_key = DP.data_provider_key
LEFT OUTER JOIN [cns_glb_reference].[DATA_TYPE] DT
    ON DC.DATA_TYPE_KEY = DT.DATA_TYPE_KEY
LEFT OUTER JOIN [cns_glb_reference].[dataset] RDS
    ON DC.REFERENCED_DATASET_KEY = RDS.dataset_key
LEFT OUTER JOIN [cns_eis_controls].[data_provider] RDP
    ON RDS.data_provider_key = RDP.data_provider_key
LEFT OUTER JOIN [cns_glb_reference].[dataset_purpose_type] DSP
    ON DS.DATASET_PURPOSE_TYPE_KEY = DSP.dataset_purpose_type_key
LEFT OUTER JOIN [cns_glb_reference].[DATASET_COLUMN] DD
    ON DC.DROPDOWN_FILTER_KEY = DD.DATASET_COLUMN_KEY
LEFT OUTER JOIN [cns_glb_reference].[DATA_ELEMENT] DE
    ON DC.DATA_ELEMENT_KEY = DE.DATA_ELEMENT_KEY
LEFT OUTER JOIN [cns_glb_reference].[DISPLAY_FORMAT] DF
    ON DC.DISPLAY_FORMAT_KEY = DF.DISPLAY_FORMAT_KEY
ORDER BY
    DP.data_provider_abbr,
    DS.dataset_nm,
    DC.DISPLAY_SEQUENCE_NUM;
```

> Note: Don’t include `GO` when executing programmatically; the CLI will strip it if present.

---

## Example (NL2SQL via Copilot → run via CLI)

User asks:
> “Show me orders processed today where value > 1000.”

Copilot produces SQL (example; real names must come from metadata):
```sql
SELECT TOP (100)
    o.ORDER_ID,
    o.ORDER_PROCESSED_DTTM,
    o.ORDER_VALUE
FROM cns_some_schema.F_ORDERS o
WHERE
    CAST(o.ORDER_PROCESSED_DTTM AS date) = CAST(GETDATE() AS date)
    AND o.ORDER_VALUE > 1000
ORDER BY
    o.ORDER_PROCESSED_DTTM DESC;
```

Then run:
```bash
query_geneva_db mid -f query.sql
```

---

## What not to do
- Don’t pass pure natural language into the CLI. Copilot must generate SQL first.
- Don’t run unbounded queries on large tables.
- Don’t use `SELECT *`.
- Don’t use multiple statements (no internal semicolons).
- Don’t use `GO`, `USE`, `DECLARE`, `SET`, temp tables, or any write operations.
