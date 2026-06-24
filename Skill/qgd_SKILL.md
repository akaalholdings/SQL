---
name: query_geneva_db
description: CLI tool for running read-only SQL queries against Geneva SQL Server, Synapse, and Databricks Unity Catalog using Azure CLI auth.
---

To query Geneva databases using Azure CLI authentication, use the `query_geneva_db` command-line tool. This tool allows read-only SQL inspection against Geneva aliases across SQL Server, Synapse, and Databricks Unity Catalog.

1. The first argument is the database alias (e.g., SQL Server: `mid`, `mid_sandbox`, `mid_dev`, `mid_test`; Synapse: `synapse_dev`, `synapse_test`, `synapse_prod`; Unity Catalog: `uc_dev`, `uc_test`, `uc_preprod`, `uc_prod`).
2. The second argument is the SQL query you want to execute, enclosed in quotes. You can also use `--query-file`, `--stdin`, `--describe`, `--compare`, and `--format json`.

Before running this tool for querying, first run it with `--help` to view the up to date list of supported aliases grouped by database type.

### Database Types and Where They Live

Geneva data lives in two main platforms:

- **SQL Server (Azure SQL)**: Operational and reference data, plus some consumption views.
- **Synapse (DNA platform)**: Large-scale curated and consumption tables, especially for domain pipelines.
- **Databricks Unity Catalog / Data Catalog**: Delta table facts for new pipeline layers. Use this for read-only evidence and shape inspection, not as runtime pipeline code.

### Unity Catalog / Data Catalog

Four EDP Databricks aliases are available, each pointing to a different **compute endpoint** (workspace + SQL warehouse). Unity Catalog is independent of the workspace — any catalog you have permission on is accessible from any of these aliases. The alias you choose determines *where the query runs*, not *which catalogs are available*.

| Alias | Workspace | SQL Warehouse |
|-------|-----------|---------------|
| `uc_dev` | `adb-3856895995337257.17.azuredatabricks.net` | `/sql/1.0/warehouses/d9d27d8edb4ff825` |
| `uc_test` | `adb-1275967750467369.9.azuredatabricks.net` | `/sql/1.0/warehouses/cc46d81a37a2aa72` |
| `uc_preprod` | `adb-1677585743409209.9.azuredatabricks.net` | `/sql/1.0/warehouses/aa3db777ad19955b` |
| `uc_prod` | `adb-4018826223110734.14.azuredatabricks.net` | `/sql/1.0/warehouses/3a939c17c170c4f6` |

Always fully qualify table names as `catalog.schema.table`. No default catalog or schema is set — these must be specified in the query or via environment variables:

```powershell
$env:QUERY_GENEVA_DB_UC_CATALOG = "<catalog>"
$env:QUERY_GENEVA_DB_UC_SCHEMA = "<schema>"
```

Find the HTTP path in Databricks under **SQL Warehouses -> your warehouse -> Connection details -> HTTP Path**. Authentication uses `az login` by default; if `DATABRICKS_TOKEN` is set, that token is used instead.

For Unity Catalog discovery:

```sql
SHOW CATALOGS
SHOW SCHEMAS IN <catalog>
SHOW TABLES IN <catalog>.<schema>
```

For table shape and row-count evidence:

```powershell
query_geneva_db uc_dev --describe <catalog>.<schema>.<table> --format json
query_geneva_db uc_test --describe <catalog>.<schema>.<table> --format json
```

To compare legacy SQL evidence with Unity Catalog facts:

```powershell
query_geneva_db mid_dev "SELECT COUNT_BIG(*) AS row_count FROM cns_glb_reference.dataset" --compare uc_dev --query2 "SELECT COUNT(*) AS row_count FROM <catalog>.<schema>.<delta_table>"
```

When you are unsure where a table lives, start with `INFORMATION_SCHEMA.TABLES` for the target alias to discover schemas and table names.

Example discovery query:

```sql
SELECT TOP 50 TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA, TABLE_NAME
```

### Geneva Data Architecture (Medallion Flow)

Geneva uses a tiered "Medallion" architecture to process data from source to reporting. Understanding these layers helps you choose the correct schema for your query:

1. **RAW Layer (`raw_*` Schemas)**: Contains ingested data in its original source format. Useful for debugging ingestion issues.
   - *Example*: `raw_cpo_mid.Shell_AD_data`

2. **STAGE & DELTA Layers (`trf_*_stage`, `trf_*_delta` Schemas)**: Temporary areas for transformation and incremental checks. 
   - *Example*: `trf_cpo_mid_stage.SHELL_AD_PERSON_DL`

3. **CURATED Layer (`trf_*` Schemas)**: This is the **Curated** tier. Data here is cleaned, validated, and deduplicated, but still follows source-oriented naming conventions. 
   - *Physical Schemas*: `TRF_CPO_MID`, `trf_cpo_mid_capacity`.
   - *Tip*: These often contain `SOURCE_*` columns for auditing.
   - *Example*: `trf_cpo_mid_capacity.PERSON`

4. **CONSUMPTION Layer (`cns_*` Schemas)**: This is the **Consumption** (Analytics) tier. Data is modeled for performance and cross-domain reporting (Star Schema).
   - *Physical Schemas*: `CNS_CPO_MID_DIM`, `cns_glb_reference`.
   - **`D_*` Tables**: Dimensions (Descriptive data: e.g., `D_ASSET`, `D_PORT_FACILITY`). Use for filters.
   - **`F_*` Tables**: Facts (Measurable data: e.g., `F_AIRCRAFT_FLIGHT`). Use for aggregates.
   - *Example*: `cns_glb_reference.PERSON`

### Utility Schemas

- **`refresh`**: Logs for database environment synchronization and "Copy Back" history.
- **`process_control`**: Execution logs and telemetry for ETL pipelines.

### SQL Server Schema Map

For SQL Server (Azure SQL), common schemas used across Geneva include:

- **Reference**: `cns_glb_reference`, `cns_cpo_mid_reference`
- **Consumption (analytics)**: `cns_cpo_mid_dim`, `cns_cpo_mid_capacity`, `cns_cpo_mid_nonstd`
- **Curated (domain)**: `trf_cpo_mid_capacity` and other `trf_*` schemas
- **Operational / telemetry**: `process_control`, `refresh`

If you need a quick scan on SQL Server:

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
ORDER BY TABLE_SCHEMA, TABLE_NAME
```

### Synapse-Specific Schema Map

For Synapse (DNA platform), the Geneva medallion layers typically map as:

- **Raw**: `raw_cpo_mid`
- **Stage**: `trf_cpo_mid_stage`
- **Delta**: `trf_cpo_mid_delta`
- **Curated**: `trf_cpo_mid` (plus curated subdomains like `trf_cpo_mid_capacity`)
- **Consumption**: `cns_cpo_mid`, `cns_cpo_mid_dim`, `cns_glb_reference`

Use these schemas to translate questions into SQL quickly:

- "raw" or "ingested" data -> `raw_cpo_mid`
- "curated" or "cleaned" -> `trf_cpo_mid` / `trf_cpo_mid_capacity`
- "analytics" / "dashboard" / "reporting" -> `cns_*` schemas

### Pipeline Domain Clues (What to Search First)

These hints help map user questions to likely schemas/tables:

- **Vessels / Kpler / Vortexa / AIS**: Often in `cns_cpo_mid` with `dragons_*` tables and curated inputs in `trf_cpo_mid_capacity`.
- **Aviation / OAG schedules and actuals**: Often in `cns_cpo_mid` with `AVI_*`, `OAG_*`, and `JEFF_*` tables.
- **Demand / Jet fuel**: Look for `DEMAND_*`, `F_JET_FUEL_*` in `cns_cpo_mid`.
- **Reference / metadata**: Use `cns_glb_reference.*` and `cns_cpo_mid_reference.*` for lookup tables and hierarchies.

If the user asks about "what tables exist" or "where is X", start with a schema filter query in Synapse:

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
ORDER BY TABLE_SCHEMA, TABLE_NAME
```
