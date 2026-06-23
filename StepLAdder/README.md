# Stepladder

Azure Function that scales an Azure SQL Database vCore SKU up or down from workload signals.

## What It Does

- Runs every minute from a timer trigger.
- Reads recent CPU and data IO from `sys.dm_db_resource_stats`.
- Enforces a business-hours/off-hours vCore floor.
- Scales up after sustained saturation.
- Scales down after sustained low utilization.
- Uses Azure Table Storage for state and pending-operation tracking.
- Uses a blob lease so only one function instance acts at a time.
- Optionally syncs database scoped `MAXDOP` to `min(8, vCores / 2)`.

## Runtime Requirements

- Azure Functions v4, Python 3.11 recommended.
- Remote build enabled, or deploy with prebuilt `.python_packages`.
- ODBC Driver 18 for SQL Server available in the hosting image.
- Managed identity enabled on the Function App.
- Azure SQL Database using a provisioned vCore SKU.

## Required App Settings

| Setting | Purpose |
| --- | --- |
| `AzureWebJobsStorage` or `AUTOSCALER_STORAGE_CONNECTION_STRING` | Timer host storage, state table, and blob lease. |
| `SUBSCRIPTION_ID` | Azure subscription containing the database. |
| `RESOURCE_GROUP` | Resource group containing the SQL server/database. |
| `SQL_SERVER_NAME` | Logical SQL server name, not FQDN. |
| `SQL_DATABASE_NAME` | Target database name. |
| `DRY_RUN` | Keep `true` until logs show the expected decisions. Set `false` to scale. |

## Important Optional Settings

| Setting | Default | Notes |
| --- | --- | --- |
| `MANAGED_IDENTITY_CLIENT_ID` | empty | Set for user-assigned identity. Leave empty for system-assigned identity. |
| `TIMEZONE` | `America/Chicago` | Used for business-hours floor. |
| `MIN_VCORES_BUSINESS` | `12` | Minimum vCores during business window. |
| `MIN_VCORES_OFFHOURS` | `2` | Minimum vCores outside business window. |
| `MAX_VCORES` | `16` | Upper scaling bound. |
| `STEP_VCORES` | `2` | Minimum step when `ALLOWED_VCORES` is unset. |
| `ALLOWED_VCORES` | empty | Comma-separated SKU capacities, e.g. `2,4,8,16`. If set, floors and max must be included. |
| `UP_CPU_THRESHOLD` | `85` | Upscale CPU threshold percentage. |
| `UP_IO_THRESHOLD` | `85` | Upscale data IO threshold percentage. |
| `DOWN_THRESHOLD` | `35` | Downscale threshold; both CPU and IO must be below it. |
| `SYNC_MAXDOP` | `true` | Sync database scoped MAXDOP when possible. |
| `MAXDOP_REQUIRED` | `false` | If `true`, MAXDOP failure blocks scaling. Keep `false` unless required by policy. |
| `SCALE_REQUEST_WAIT_SECONDS` | `30` | How long to wait for ARM validation/completion before tracking as pending. |

## Azure Permissions

Assign the Function App managed identity an Azure RBAC role that can read and update the target database. A narrow custom role should include at least:

- `Microsoft.Sql/servers/databases/read`
- `Microsoft.Sql/servers/databases/write`

For first deployment, `SQL DB Contributor` scoped to the database or resource group is the simplest validation path. Replace with a custom least-privilege role after the app is proven.

## SQL Permissions

Run [`sql/001_grants.sql`](sql/001_grants.sql) in the target database as a Microsoft Entra admin or an account that can create Entra users.

The function identity needs:

- `VIEW DATABASE STATE` for `sys.dm_db_resource_stats`.
- `ALTER ANY DATABASE SCOPED CONFIGURATION` only if `SYNC_MAXDOP=true`.

If `CREATE USER ... FROM EXTERNAL PROVIDER` fails because the managed identity display name is ambiguous, set `@principal_object_id` in the grant script and use the `WITH OBJECT_ID` branch.

## Deployment

From this directory:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
./scripts/package.sh
```

Deploy with Azure Functions remote build so `requirements.txt` is installed in Azure:

```bash
az functionapp config appsettings set \
  --name <function-app-name> \
  --resource-group <function-app-rg> \
  --settings SCM_DO_BUILD_DURING_DEPLOYMENT=true ENABLE_ORYX_BUILD=true

func azure functionapp publish <function-app-name> --python
```

Alternatively deploy `sqlautoscaler.zip` with a zip-deploy workflow that performs remote build.

## Smoke Test Checklist

1. Deploy with `DRY_RUN=true`.
2. Confirm timer invocation appears in Application Insights.
3. Confirm state table row is created for `<server>/<database>`.
4. Confirm logs include `Decision=... current=... floor=... samples=...`.
5. Confirm `last_error` is empty in the state table.
6. If `last_maxdop_error` is populated but decisions work, either fix SQL grant or set `SYNC_MAXDOP=false`.
7. Set `DRY_RUN=false`.
8. Confirm a scale request writes `pending_target_vcores`.
9. Confirm a later cycle clears pending and writes `last_scale_completed_utc`.

## Failure Signals

| State error | Meaning |
| --- | --- |
| `compute_read_failed` | Azure RBAC, subscription/resource names, or SQL ARM API issue. |
| `metrics_query_failed` | SQL connectivity, managed identity SQL user, firewall, ODBC driver, or `VIEW DATABASE STATE`. |
| `scale_request_failed` | Invalid SKU/capacity, RBAC write failure, quota, or Azure SQL management failure. |
| `scale_pending_timeout` | ARM accepted the request but the observed vCore count did not reach target before timeout. |
| `maxdop_sync_failed` | MAXDOP sync failed and `MAXDOP_REQUIRED=true`. |
