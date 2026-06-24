# Worked Example — Response Format Reference

This file demonstrates the response formats defined in the main instructions. Mirror the relevant structure: the first example shows the fallback when **no execution plan is supplied**; the second example shows the preferred Shell workflow when the user supplies a query plus actual execution plan XML.

Note: the user's identifier names (PascalCase here) are always preserved — StyleGuide naming conventions apply only to new code you author.

## User input

```sql
SELECT DISTINCT o.OrderID, o.OrderDate, o.TotalDue, c.CustomerName
FROM Orders o WITH (NOLOCK)
JOIN dbo.Customers c ON c.CustomerID = o.CustomerID
WHERE YEAR(o.OrderDate) = 2025
AND c.Region = N'EMEA'
AND o.OrderID NOT IN (SELECT r.OrderID FROM dbo.Refunds r)
ORDER BY o.OrderDate DESC;
```

## Expected response

---

**Schema check:** Schemas identified: `dbo` (`Customers`, `Refunds`) — all references preserved exactly as written. `Orders` is referenced without a schema and stays unqualified per the schema immutability rule; schema-qualifying it (a change for you to make) would improve plan-cache reuse. This optimization assumes the unqualified name resolves to a single object.

**Optimized query**

```sql
SELECT DISTINCT
    o.OrderID,
    o.OrderDate,
    o.TotalDue,
    c.CustomerName
FROM Orders AS o
JOIN dbo.Customers AS c
  ON c.CustomerID = o.CustomerID
WHERE o.OrderDate >= '20250101'
AND   o.OrderDate < '20260101'
AND   c.Region = N'EMEA'
AND   o.OrderID NOT IN
(
    SELECT
        r.OrderID
    FROM dbo.Refunds AS r
)
ORDER BY
    o.OrderDate DESC;
```

**Index recommendations**

This DDL is a recommendation script. Do not execute it until the user has approved the change and the target environment is clear.

```sql
/*
Supports the date-range seek and covers the Orders columns in this query.
Schema-qualify the table name to match your Orders table before approval/execution.
Assumes OrderID is the clustered primary key (and therefore carried in the
nonclustered index automatically); if not, add OrderID to the INCLUDE list.
*/
CREATE INDEX
    orders_orderdate_covering
ON Orders
(
    OrderDate
)
INCLUDE
(
    CustomerID,
    TotalDue
)
WITH
    (ONLINE = ON);
```

No execution plan or existing index definitions were provided, so this recommendation cannot be checked for overlap with existing indexes — share your index inventory (script in the main instructions) and an existing index may be extendable instead.

**What changed and why**

- `YEAR(o.OrderDate) = 2025` → half-open date range (`>= '20250101' AND < '20260101'`): the function on the column made the predicate non-SARGable, forcing a scan; the range form permits an index seek (queryguide Rule 1). Estimated impact: scan → seek against the recommended index.
- Removed `WITH (NOLOCK)`: Azure SQL Database uses READ COMMITTED SNAPSHOT by default, so readers don't block writers; the hint adds dirty-read/double-read risk with no benefit (queryguide Rule 4). Verify with: `SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME();`
- Reformatted to the styling guide (table aliases with `AS`, qualified columns, `yyyymmdd` date literals, layout). Formatting only — no behavior change.

*Optional changes (not applied — please confirm):*

- `NOT IN` → `NOT EXISTS`: equivalent **only** if `Refunds.OrderID` is `NOT NULL`. If it is nullable and ever contains a NULL, the current query returns zero rows — which may itself be a latent bug worth checking. If the column is `NOT NULL`, this form typically produces a more efficient anti-semi-join:
  ```sql
  AND NOT EXISTS
  (
      SELECT
          1
      FROM dbo.Refunds AS r
      WHERE r.OrderID = o.OrderID
  )
  ```
- `DISTINCT` removal: if `OrderID` is unique in `Orders` and `CustomerID` is the key of `dbo.Customers`, the join cannot multiply rows and `DISTINCT` is pure cost (a needless distinct sort or hash). Confirm both and it can be dropped.

**Azure SQL Database notes**

- No execution plan was provided, so all impact statements above are estimates. Capture an actual plan (SSMS: "Include Actual Execution Plan", or `SET STATISTICS XML ON;`) and resubmit to verify — especially estimated vs actual row counts.
- If `Customers.Region` is `varchar`, the `N'EMEA'` literal forces an implicit conversion of the **column** (nvarchar has higher type precedence), preventing an index seek on Region. Confirm the column type; if it is `varchar`, change the literal to `'EMEA'`. If the column is `nvarchar`, it is correct as written.

---

## Plan-based Shell example

## User input

```sql
SELECT
    s.ShipmentID,
    s.OrderID,
    s.ShipDate,
    s.StatusCode,
    c.CustomerName
FROM dbo.Shipments AS s
JOIN dbo.Customers AS c
  ON c.CustomerID = s.CustomerID
WHERE CONVERT(date, s.ShipDate) = @ShipDate
AND   s.StatusCode IN ('DELAYED', 'HELD')
ORDER BY
    s.ShipDate DESC;
```

The user also supplies the actual execution plan XML. The XML shows:

```xml
<RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimatedRows="42.1">
  <RunTimeInformation>
    <RunTimeCountersPerThread ActualRows="184221" ActualLogicalReads="82114" />
  </RunTimeInformation>
  <Warnings>
    <PlanAffectingConvert ConvertIssue="Seek Plan" />
  </Warnings>
</RelOp>
<RelOp PhysicalOp="Key Lookup" EstimateRows="42.1">
  <RunTimeInformation>
    <RunTimeCountersPerThread ActualExecutions="184221" />
  </RunTimeInformation>
</RelOp>
```

## Expected response

---

**Schema check:** Schema identified: `dbo` (`Shipments`, `Customers`) — all references preserved exactly as written. No unqualified objects found.

**Environment for validation**

Before direct database validation, choose the Shell environment: `mid` (prod), `mid_preprod` (preprod), `mid_test` (test), `mid_dev` (dev), or `mid_sandbox` (sandbox). If you do not specify one, I will use `mid_dev` and state that assumption.

For direct read-only inspection/validation, use `query_geneva_db` after the environment is chosen:

```bash
query_geneva_db mid_dev -f /tmp/shipments_index_inventory.sql --mode sql --preview-rows 500
query_geneva_db mid_dev -f /tmp/shipments_baseline_sample.sql --mode sql --preview-rows 100
```

`query_geneva_db` is only for direct database reads, metadata/index inspection, and validation queries. Do not use it to run the view or index DDL below.

**Plan findings**

- `dbo.Shipments` is scanned: the plan shows a clustered index scan with `ActualRows=184221` and `ActualLogicalReads=82114`.
- `CONVERT(date, s.ShipDate) = @ShipDate` is non-SARGable and the XML reports `PlanAffectingConvert ConvertIssue="Seek Plan"`.
- The key lookup executes once per qualifying shipment row (`ActualExecutions=184221`), so the index used by the plan is not covering the selected/joined columns.
- Estimated rows (`42.1`) vs actual rows (`184221`) shows a severe cardinality miss; check parameter sensitivity and statistics on `ShipDate` and `StatusCode`.

**Optimized query or view**

```sql
SELECT
    s.ShipmentID,
    s.OrderID,
    s.ShipDate,
    s.StatusCode,
    c.CustomerName
FROM dbo.Shipments AS s
JOIN dbo.Customers AS c
  ON c.CustomerID = s.CustomerID
WHERE s.ShipDate >= @ShipDate
AND   s.ShipDate < DATEADD(day, 1, @ShipDate)
AND   s.StatusCode IN ('DELAYED', 'HELD')
ORDER BY
    s.ShipDate DESC;
```

**Index recommendations**

Run the index inventory first and compare overlap before applying:

```sql
SELECT
    schema_name = SCHEMA_NAME(t.schema_id),
    table_name = t.name,
    index_name = i.name,
    index_type = i.type_desc,
    column_name = c.name,
    ic.key_ordinal,
    ic.is_included_column
FROM sys.indexes AS i
JOIN sys.index_columns AS ic
  ON  ic.object_id = i.object_id
  AND ic.index_id = i.index_id
JOIN sys.columns AS c
  ON  c.object_id = ic.object_id
  AND c.column_id = ic.column_id
JOIN sys.tables AS t
  ON t.object_id = i.object_id
WHERE i.object_id = OBJECT_ID(N'dbo.Shipments')
ORDER BY
    i.index_id,
    ic.is_included_column,
    ic.key_ordinal;
```

If no existing index can be amended, prepare this candidate script for the approved environment:

```sql
CREATE INDEX
    shipments_shipdate_status_covering
ON dbo.Shipments
(
    ShipDate,
    StatusCode
)
INCLUDE
(
    ShipmentID,
    OrderID,
    CustomerID
)
WITH
    (ONLINE = ON);
```

Justification: supports a seek on the half-open `ShipDate` range, applies the `StatusCode` filter in index order, and covers the selected/joined shipment columns to remove the repeated key lookup. Do not create this if the inventory shows an existing equivalent index that can be amended instead.

**Three test scenarios**

1. Baseline test:
   - Run the original query with representative `@ShipDate`.
   - Capture actual plan XML, duration, CPU time, logical reads, row count, spills, memory grant, and lookup executions.
   - Save the baseline result set or exact comparison signature.

2. Optimized view/query test:
   - Run the optimized query without new indexes.
   - Validate exact result equivalence:
     ```sql
     WITH original_result AS
     (
         /* original query, without ORDER BY */
     ),
     optimized_result AS
     (
         /* optimized query, without ORDER BY */
     )
     SELECT issue = 'original_except_optimized', *
     FROM original_result
     EXCEPT
     SELECT issue = 'original_except_optimized', *
     FROM optimized_result
     UNION ALL
     SELECT issue = 'optimized_except_original', *
     FROM optimized_result
     EXCEPT
     SELECT issue = 'optimized_except_original', *
     FROM original_result;
     ```
   - If duplicates are possible, add duplicate-preserving row numbering over all projected columns before comparing.

3. Optimized view/query + indexes test:
   - Apply the approved index script only after explicit approval.
   - Re-run the optimized query and exact comparison.
   - Confirm the new plan removes the scan/key lookup pattern or explain why it does not.
   - Compare logical reads, duration, CPU time, spills, and memory grant against baseline.

**What changed and why**

- `CONVERT(date, s.ShipDate) = @ShipDate` changed to a half-open range. This preserves the same calendar-day filter while making `ShipDate` seekable.
- No join type, filter value, projection, grouping, or ordering semantics changed.
- The index recommendation is tied to the actual scan, row-estimate miss, and key lookup evidence in the supplied XML.

*Optional changes (not applied — please confirm):*

- If `StatusCode` is highly selective and `ShipDate` is not, test `(StatusCode, ShipDate)` as the key order. Pick the winner from measured logical reads and actual plan shape, not guesswork.

**Azure SQL Database notes**

- Validate in `mid_dev` by default unless you choose another Shell environment.
- Generated DDL is a script, not an instruction to execute automatically. Apply it only after approval in the chosen environment.
