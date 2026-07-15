# Worked Example — Response Format Reference

This file demonstrates the response formats defined in the main instructions. Mirror the relevant structure: the first example shows the fallback when **no execution plan is supplied**; the second example shows the preferred live-database workflow when the user supplies a query plus actual execution plan XML. A third section gives compact rewrite-pattern examples for `queryguide.md` Rules 11–16 — a real run still executes the full three-scenario benchmark and result-equivalence proof per `RunGuide.md`; those sections are omitted there for brevity.

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
FROM Orders AS o WITH (NOLOCK)
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
- Remove `WITH (NOLOCK)`: Azure SQL Database uses READ COMMITTED SNAPSHOT by default, so readers do not block writers; the hint adds dirty-read/double-read risk. This is a recommended correctness change, not applied in the guaranteed equivalent rewrite because it can change behavior during concurrent writes. Verify with: `SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME();`

**Azure SQL Database notes**

- No execution plan was provided, so all impact statements above are estimates. Capture an actual plan (SSMS: "Include Actual Execution Plan", or `SET STATISTICS XML ON;`) and resubmit to verify — especially estimated vs actual row counts.
- If `Customers.Region` is `varchar`, the `N'EMEA'` literal forces an implicit conversion of the **column** (nvarchar has higher type precedence), preventing an index seek on Region. Confirm the column type; if it is `varchar`, change the literal to `'EMEA'`. If the column is `nvarchar`, it is correct as written.

---

## Plan-based live-database example

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

Before direct database validation, call `list_databases` and ask which configured database to target — there is no fixed alias list; the available databases are whatever the running `azure-sql-mcp` server's `AZURE_SQL_ALLOWED_DATABASES` configures. If you do not specify one, I will ask rather than assume a default.

For direct read-only inspection/validation, use `azure-sql-mcp` after the database is chosen:

```
get_object_details(schema_name="dbo", object_name="Shipments", object_type="table")
explain_query(sql=<contents of shipments_baseline.sql>, analyze=true)
```

`azure-sql-mcp` is for this query's evidence: direct database reads, metadata/index inspection, actual-plan capture, benchmarking, and result-validation queries. `execute_sql`/`explain_query` are always read-only; the test-index DDL below runs through the gated `create_test_index`/`drop_test_index` tools after approval (see `RunGuide.md` step 4).

**Plan findings**

- `dbo.Shipments` is scanned: the plan shows a clustered index scan with `ActualRows=184221` and `ActualLogicalReads=82114`.
- `CONVERT(date, s.ShipDate) = @ShipDate` is non-SARGable and the XML reports `PlanAffectingConvert ConvertIssue="Seek Plan"`.
- The key lookup executes once per qualifying shipment row (`ActualExecutions=184221`), so the index used by the plan is not covering the selected/joined columns.
- Estimated rows (`42.1`) vs actual rows (`184221`) shows a severe cardinality miss; check parameter sensitivity and statistics on `ShipDate` and `StatusCode`.

**Optimized query**

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

**Three-scenario results**

Same `@ShipDate` and parameter values across all three runs. Row count is identical (312) in every scenario — that is the equivalence anchor.

| Scenario | Duration ms | CPU ms | Logical Reads | Physical Reads | Rows | Plan Notes |
|---|---:|---:|---:|---:|---:|---|
| Baseline | 1,240 | 1,100 | 82,114 | 90 | 312 | Clustered index scan; non-SARGable `CONVERT`; est 42 vs actual 184,221; key lookup ×184,221 |
| Optimized | 980 | 870 | 61,030 | 40 | 312 | Range now SARGable; seeks an existing `ShipDate` index but key lookup remains (not covering) |
| Optimized + indexes | 45 | 30 | 980 | 0 | 312 | Single seek on the test covering index; key lookup gone; est ≈ actual |

How each scenario was run:

1. **Baseline** — read-only, on the chosen database. Captured the actual plan, duration, CPU, logical reads, row count, spills, memory grant, and lookup executions.
   ```
   explain_query(sql=<contents of shipments_baseline.sql>, analyze=true)
   execute_sql(sql=<contents of shipments_baseline.sql>)
   ```

2. **Optimized** — read-only, same database, no new indexes. The SARGable range cut reads, but without a covering index the key lookup remains; exact equivalence is proven server-side below.
   ```
   explain_query(sql=<contents of shipments_optimized.sql>, analyze=true)
   execute_sql(sql=<contents of shipments_optimized.sql>)
   ```

3. **Optimized + indexes** — after DDL approval, via the gated test-index tools (`RunGuide.md` step 4). The plan collapsed to a single seek with no key lookup.
   ```
   # create (tool enforces the IX_Testing_ prefix; response carries the rollback DROP)
   create_test_index(schema_name="dbo", table_name="Shipments",
                     index_name="IX_Testing_BS_Shipments_ShipDate_StatusCode_a1b2c3d4",
                     key_columns=["ShipDate", "StatusCode"], include_columns=["CustomerID", "TotalDue"],
                     dry_run=false)
   # capture the optimized query with the index present
   explain_query(sql=<contents of shipments_optimized.sql>, analyze=true)
   # drop (rollback)
   drop_test_index(schema_name="dbo", table_name="Shipments",
                   index_name="IX_Testing_BS_Shipments_ShipDate_StatusCode_a1b2c3d4", dry_run=false)
   ```

**Result equivalence** — run through `execute_sql` as a single statement (CTEs are fully supported); both `EXCEPT` directions returned zero rows (duplicate-sensitive; add row numbering over all projected columns when duplicates are possible). Drop `ORDER BY` for the set comparison itself.

```sql
WITH baseline_result AS
(
    /* original query, without ORDER BY */
),
optimized_result AS
(
    /* optimized query, without ORDER BY */
)
SELECT issue = 'baseline_except_optimized', *
FROM baseline_result
EXCEPT
SELECT issue = 'baseline_except_optimized', *
FROM optimized_result
UNION ALL
SELECT issue = 'optimized_except_baseline', *
FROM optimized_result
EXCEPT
SELECT issue = 'optimized_except_baseline', *
FROM baseline_result;
```

**What changed and why**

- `CONVERT(date, s.ShipDate) = @ShipDate` changed to a half-open range. This preserves the same calendar-day filter while making `ShipDate` seekable.
- No join type, filter value, projection, grouping, or ordering semantics changed.
- The index recommendation is tied to the actual scan, row-estimate miss, and key lookup evidence in the supplied XML.

*Optional changes (not applied — please confirm):*

- If `StatusCode` is highly selective and `ShipDate` is not, test `(StatusCode, ShipDate)` as the key order. Pick the winner from measured logical reads and actual plan shape, not guesswork.

**Azure SQL Database notes**

- Validate read-only evidence on whichever database you choose via `list_databases` — there is no default; ask if unspecified. Test-index DDL runs through `create_test_index`/`drop_test_index` with explicit approval (standing approval on a sandbox clone — `SandboxGuide.md`).
- Production deployment DDL remains a script for the user; only disposable test-prefixed indexes are ever tool-executed, and every create carries its rollback DROP (`RunGuide.md` step 4).

## Rewrite-pattern examples (Rules 11–16)

Compact examples for the `queryguide.md` §1.2 rewrite rules 11–16. Each shows the anti-pattern, the rewrite, and the load-bearing caveats. A real run still executes the full three-scenario benchmark and proves result equivalence per `RunGuide.md` — those sections are omitted here for brevity, never in practice.

### Rule 11 — kitchen-sink optional parameters

**User input** (plan symptom: clustered index scan and ~30%-of-table estimates regardless of which parameters are supplied)

```sql
SELECT o.order_id, o.order_date, o.total_due, o.status_code
FROM dbo.orders AS o
WHERE (@customer_id IS NULL OR o.customer_id = @customer_id)
AND (@status_code IS NULL OR o.status_code = @status_code)
AND (@order_date_from IS NULL OR o.order_date >= @order_date_from);
```

**Expected response**

**Schema check:** `dbo.orders` qualified; preserved as written.

**Plan findings**

- One cached plan serves all 8 filter combinations: the optimizer compiled a defensive scan because no single plan can seek `customer_id` when supplied and skip it when NULL.
- Estimates reflect the density average, not the actual filter combination — wrong for almost every execution.

**Optimized query** (low-frequency path — semantics identical; NULL branches fold away at compile time)

```sql
SELECT
    o.order_id,
    o.order_date,
    o.total_due,
    o.status_code
FROM dbo.orders AS o
WHERE (@customer_id IS NULL OR o.customer_id = @customer_id)
AND   (@status_code IS NULL OR o.status_code = @status_code)
AND   (@order_date_from IS NULL OR o.order_date >= @order_date_from)
OPTION (RECOMPILE);
```

**What changed and why**

- `OPTION (RECOMPILE)` compiles a plan for exactly the predicates present each execution (Rule 11). Cost: compile CPU per execution — stated per §3.1 hint governance.

*Optional changes (not applied — please confirm):*

- Hot-path form: parameterized dynamic SQL appending only present predicates (Rule 9 discipline — values always as parameters):

```sql
DECLARE
    @sql nvarchar(max) = N'
SELECT
    o.order_id,
    o.order_date,
    o.total_due,
    o.status_code
FROM dbo.orders AS o
WHERE 1 = 1';

IF @customer_id IS NOT NULL
BEGIN
    SET @sql += N'
AND   o.customer_id = @customer_id';
END;

IF @status_code IS NOT NULL
BEGIN
    SET @sql += N'
AND   o.status_code = @status_code';
END;

IF @order_date_from IS NOT NULL
BEGIN
    SET @sql += N'
AND   o.order_date >= @order_date_from';
END;

EXECUTE sys.sp_executesql
    @sql,
    N'@customer_id integer, @status_code varchar(10), @order_date_from date',
    @customer_id,
    @status_code,
    @order_date_from;
```

  Up to 2^3 = 8 distinct plans cache — deliberately, one per filter combination.

**Azure SQL Database notes**

- Benchmark buckets must include the all-NULL combination and each single-filter case (`get_query_parameter_buckets` + §4.1). Equivalence proof per `RunGuide.md` before recommending.

### Rule 12 — RBAR cursor to set-based

**User input** (plan symptom: one UPDATE plus one correlated aggregate probe per customer; thousands of tiny statements)

```sql
DECLARE @customer_id int;
DECLARE customer_cursor CURSOR FOR
SELECT customer_id FROM dbo.customers;
OPEN customer_cursor;
FETCH NEXT FROM customer_cursor INTO @customer_id;
WHILE @@FETCH_STATUS = 0
BEGIN
    UPDATE dbo.customers
    SET lifetime_value = (SELECT SUM(o.total_due) FROM dbo.orders AS o
                          WHERE o.customer_id = @customer_id)
    WHERE customer_id = @customer_id;
    FETCH NEXT FROM customer_cursor INTO @customer_id;
END;
CLOSE customer_cursor;
DEALLOCATE customer_cursor;
```

**Expected response**

**Schema check:** `dbo.customers`, `dbo.orders` qualified; preserved.

**Plan findings**

- Per-row work the engine can do as one set: each iteration pays parse/execute, a keyed UPDATE, and a correlated `SUM` probe — customers × 2 statements.
- This is RBAR (Rule 12), not the §1.4 batching pattern: batching chunks one set operation to bound transaction size; this loop does per-row work.

**Optimized query**

```sql
UPDATE
    c
SET
    c.lifetime_value = s.customer_total
FROM dbo.customers AS c
LEFT JOIN
(
    SELECT
        o.customer_id,
        customer_total = SUM(o.total_due)
    FROM dbo.orders AS o
    GROUP BY
        o.customer_id
) AS s
  ON s.customer_id = c.customer_id;
```

**What changed and why**

- One set-based UPDATE replaces customers × 2 statements (Rule 12).
- `LEFT JOIN`, not `JOIN`: the cursor updates **every** customer, and `SUM` over zero orders yields NULL — customers with no orders must get `lifetime_value = NULL`. An inner join would silently skip them (different result).

*Optional changes (not applied — please confirm):*

- If `dbo.customers` is very large, batch this single UPDATE per §1.4 (`UPDATE TOP (n)` keyed loop). That loop is chunked set work, not RBAR.

**Azure SQL Database notes**

- DML tuning belongs on a sandbox clone (`SandboxGuide.md`). Equivalence is proven on post-update table state, not row counts (Phase 4).

### Rule 13 — OR across different columns

**User input** (plan symptom: full scan; neither single-column index can be seeked through the disjunction)

```sql
SELECT o.order_id, o.order_date, o.customer_id, o.sales_rep_id, o.total_due
FROM dbo.orders AS o
WHERE o.customer_id = @customer_id OR o.sales_rep_id = @sales_rep_id;
```

**Expected response**

**Schema check:** `dbo.orders` qualified; preserved.

**Plan findings**

- The OR spans two different columns; the optimizer cannot seek both indexes for one disjunction and falls back to a scan (Rule 13; same-column OR would seek fine).

**Optimized query** (exactly row-equivalent because the unique `order_id` is projected)

```sql
SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.sales_rep_id,
    o.total_due
FROM dbo.orders AS o
WHERE o.customer_id = @customer_id

UNION ALL

SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.sales_rep_id,
    o.total_due
FROM dbo.orders AS o
WHERE o.sales_rep_id = @sales_rep_id
AND   (o.customer_id <> @customer_id OR o.customer_id IS NULL);
```

**Index recommendations**

```sql
CREATE NONCLUSTERED INDEX IX_orders_customer_id
    ON dbo.orders (customer_id)
    INCLUDE (order_date, sales_rep_id, total_due)
    WITH (ONLINE = ON);

CREATE NONCLUSTERED INDEX IX_orders_sales_rep_id
    ON dbo.orders (sales_rep_id)
    INCLUDE (order_date, customer_id, total_due)
    WITH (ONLINE = ON);
```

**What changed and why**

- Each branch seeks its own index; the mutual-exclusion predicate keeps both-match rows from duplicating (Rule 13).
- The `IS NULL` arm is mandatory while `customer_id` is nullable: `NULL <> @customer_id` is UNKNOWN and would silently drop rows the original OR returned.

*Optional changes (not applied — please confirm):*

- Plain `UNION` is simpler but pays a dedup sort and collapses legitimate source duplicates — only equivalent here because `order_id` is unique.
- If either parameter can be NULL at call time, this is also a Rule 11 pattern — handle that first.

**Azure SQL Database notes**

- Both `EXCEPT` directions must return zero rows before recommending (Phase 4 / `RunGuide.md`).

### Rule 14 — correlated per-row subqueries to window function

**User input** (plan symptom: two Nested Loops branches each executing once per customer; the orders index probed 2 × customers times)

```sql
SELECT c.customer_id, c.customer_name,
    (SELECT TOP 1 o.order_date FROM dbo.orders AS o
     WHERE o.customer_id = c.customer_id ORDER BY o.order_date DESC) AS last_order_date,
    (SELECT TOP 1 o.total_due FROM dbo.orders AS o
     WHERE o.customer_id = c.customer_id ORDER BY o.order_date DESC) AS last_order_total
FROM dbo.customers AS c;
```

**Expected response**

**Schema check:** `dbo.customers`, `dbo.orders` qualified; preserved.

**Plan findings**

- Two correlated subqueries against the same table: `dbo.orders` is probed twice per customer where one windowed read suffices (Rule 14).
- Latent bug in the original: the two independent `TOP 1` probes can return `last_order_date` from one tied order and `last_order_total` from a different one when `order_date` ties.

**Optimized query**

```sql
SELECT
    c.customer_id,
    c.customer_name,
    last_order_date = lo.order_date,
    last_order_total = lo.total_due
FROM dbo.customers AS c
LEFT JOIN
(
    SELECT
        o.customer_id,
        o.order_date,
        o.total_due,
        order_rank = ROW_NUMBER() OVER
            (
                PARTITION BY
                    o.customer_id
                ORDER BY
                    o.order_date DESC,
                    o.order_id DESC
            )
    FROM dbo.orders AS o
) AS lo
  ON  lo.customer_id = c.customer_id
  AND lo.order_rank = 1;
```

**Index recommendations**

```sql
CREATE NONCLUSTERED INDEX IX_orders_customer_id_order_date
    ON dbo.orders (customer_id, order_date DESC)
    INCLUDE (total_due)
    WITH (ONLINE = ON);
```

**What changed and why**

- One windowed read of `dbo.orders` replaces two probes per customer (Rule 14). `LEFT JOIN` preserves customers with no orders (the original returns NULLs for them).
- Both output columns now come from the **same** order row, and the `o.order_id DESC` tiebreaker makes the pick deterministic.

*Optional changes (not applied — please confirm):*

- The tiebreaker is a semantic pin the original did not guarantee — confirm it, or use `RANK() = 1` if all tied rows are wanted (Rule 14 ties caveat).

**Azure SQL Database notes**

- The index keys (partition column, then window ORDER BY) remove the Sort feeding the window — see IndexingGuide "GROUP BY, TOP, ORDER BY, and Window Functions". Equivalence proof per Phase 4.

### Rule 15 — multi-statement TVF row source

`dbo.fn_customer_order_totals()` is a multi-statement TVF aggregating `dbo.orders` per customer into a declared return table (`customer_id integer, order_count bigint, total_due money`).

**User input** (plan symptom: TVF operator estimated 100 rows, actual 1,240,000; Nested Loops join chosen; interleaved execution did not engage)

```sql
SELECT c.customer_id, c.customer_name, r.order_count, r.total_due
FROM dbo.customers AS c
JOIN dbo.fn_customer_order_totals() AS r ON r.customer_id = c.customer_id
WHERE r.total_due > @minimum_total;
```

**Expected response**

**Schema check:** `dbo.customers`, `dbo.fn_customer_order_totals` qualified; preserved.

**Plan findings**

- The multi-statement TVF gets a fixed 100-row estimate (compatibility level 140+) against 1.24M actual rows — the Nested Loops choice downstream is built on that guess (Rule 15).
- Estimated exactly 100 with far higher actuals = interleaved execution did not engage for this shape.

**Optimized query** (function body inlined as a derived table)

```sql
SELECT
    c.customer_id,
    c.customer_name,
    r.order_count,
    r.total_due
FROM dbo.customers AS c
JOIN
(
    SELECT
        o.customer_id,
        order_count = COUNT_BIG(*),
        total_due = SUM(o.total_due)
    FROM dbo.orders AS o
    GROUP BY
        o.customer_id
) AS r
  ON r.customer_id = c.customer_id
WHERE r.total_due > @minimum_total;
```

**What changed and why**

- The optimizer now sees the aggregation directly: real estimates, sane join choice (Rule 15). `COUNT_BIG` matches the declared `bigint` return column.

*Optional changes (not applied — please confirm):*

- Convert the shared function to an inline TVF so every caller benefits (shared-object change — audit all callers first; return column types/nullability must match exactly):

```sql
CREATE OR ALTER FUNCTION
    dbo.fn_customer_order_totals ()
RETURNS table
AS
RETURN
(
    SELECT
        o.customer_id,
        order_count = COUNT_BIG(*),
        total_due = SUM(o.total_due)
    FROM dbo.orders AS o
    GROUP BY
        o.customer_id
);
```

- If the body cannot be one statement, materialize into a `#temptable` and join to it — real statistics (§1.3).

**Azure SQL Database notes**

- Interleaved execution (compatibility 140+) fixes some MSTVF estimates without code change, but not data-modification uses — always check estimate vs actual on the TVF operator. Equivalence proof per Phase 4.

### Rule 16 — nested view stack

`dbo.v_order_enriched` selects from `dbo.v_order_details` (which joins `dbo.orders`, `dbo.customers`, `dbo.addresses`, `dbo.sales_reps` under a `DISTINCT`) and adds `dbo.shipping_status`.

**User input** (plan symptom: operators touch six base tables for a three-column result; the inner view's `DISTINCT` blocked join pruning)

```sql
SELECT v.order_id, v.order_date, v.customer_name
FROM dbo.v_order_enriched AS v
WHERE v.order_date >= @order_date_from;
```

**Expected response**

**Schema check:** `dbo.v_order_enriched` qualified; preserved. Base objects traced per §1.1: `orders`, `customers`, `addresses`, `sales_reps`, `shipping_status`.

**Plan findings**

- The plan joins six tables; the final projection needs two (`orders`, `customers`). The inner view's `DISTINCT` prevented the optimizer from pruning the unused joins (Rule 16).

**Optimized query** (flattened to base tables — recommended redesign, since the views serve other consumers)

```sql
SELECT
    o.order_id,
    o.order_date,
    customer_name = c.customer_name
FROM dbo.orders AS o
JOIN dbo.customers AS c
  ON c.customer_id = o.customer_id
WHERE o.order_date >= @order_date_from;
```

**Index recommendations**

```sql
CREATE NONCLUSTERED INDEX IX_orders_order_date
    ON dbo.orders (order_date)
    INCLUDE (customer_id)
    WITH (ONLINE = ON);
```

**What changed and why**

- The hot query reads only what it projects (Rule 16). Equivalence rests on two proofs, both stated: (1) the view's `DISTINCT` was pure cost — `order_id` is unique in the projection and `customers` is joined on its key (Rule 5); (2) each removed inner join was non-filtering — trusted foreign keys on `NOT NULL` columns. Verify both before approving.

*Optional changes (not applied — please confirm):*

- This is a per-call-site redesign (Rule 10 convention): the views remain the API for other consumers.

**Azure SQL Database notes**

- Do not reach for indexed views or `SCHEMABINDING` casually — IndexingGuide's "Partitioning and Indexed Views" gate applies; index the base tables instead. Equivalence proof per Phase 4.

## Field examples

Real before/afters promoted from the audit corpus by the `ImproveGuide.md` review pass ("Promoting field examples"): anonymized to the neutral `dbo` example domain, shapes preserved exactly, measured numbers verbatim, each tagged with its corpus run id. Capped at **5** — a new candidate must displace the weakest current entry.

*None promoted yet. Candidates come from your own runs: record with `SQL_OPTIMIZER_AUDIT_FULL_SQL=1`, then run the `ImproveGuide.md` review pass once the corpus has enough runs to show recurrence. Equivalence failures and regressions outrank routine wins.*
