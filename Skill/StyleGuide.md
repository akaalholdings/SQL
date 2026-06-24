# Balwinder Singh's T-SQL Styling Guide

Contact: Balwinder.singh@shell.com
Adapted from Erik Darling's (Darling Data, LLC) public T-SQL style conventions, trimmed and corrected for Azure SQL Database.

This document defines what "clean" T-SQL looks like. Every query returned by this Space must follow these conventions.

## Scope and Precedence

- **Style is formatting-only.** Applying this guide must never change what a query returns or how it executes: never add query hints, never change isolation levels, never convert join types, and never add, remove, or alter schema qualifiers (SchemaGuide governs schemas and wins every conflict).
- Preferences in this guide that would alter output types or semantics (e.g., `COUNT(*)` → `COUNT_BIG(*)`) are **recommendation-only** when cleaning a user's query — mention them, don't apply them. They apply fully when authoring new code such as index scripts and examples.

## General Formatting

- **Keywords**: All SQL keywords in UPPERCASE (SELECT, FROM, WHERE, JOIN, etc.); never abbreviate them (EXECUTE not EXEC, TRANSACTION not TRAN, PROCEDURE not PROC)
- **Functions**: All SQL functions in UPPERCASE (CONVERT, ISNULL, OBJECT_ID, etc.)
- **Data types**:
  - Never abbreviate (integer, not int)
  - Always lowercase, including length, precision, and scale: nvarchar(max), decimal(38,2)
  - Use sysname for variables holding SQL object names (table names, column names, index names, etc.) rather than nvarchar(128)
- **Indentation**: 4 spaces per level (never tabs)
- **Line breaks**: Each statement on a new line; empty line between logical code blocks (maximum two)
- **Spacing**: Consistent spacing around operators (=, <, >, etc.)
- **Quotes**: Single quotes for string literals; N-prefix for Unicode strings (N'string')
- **TOP syntax**: Always parenthesized — TOP (100), not TOP 100
- **Object creation**: Use CREATE OR ALTER instead of DROP/CREATE
- **Table aliases**: Every table gets an alias, even in single-table queries, always with AS: `FROM dbo.orders AS o`
- **Column references**: Always qualified with the table alias
- **Commas**: Trailing commas, always
- **Semicolons**: Terminate every statement (after any query hints, at the very end)

## Comments

- Use block comments /* ... */, never double-dash (--)
- Prefix logical sections with a short comment describing what the section does
- Comment complex expressions, non-obvious logic, and the purpose of temp tables
- No decorative headers, ASCII art, or boilerplate

## Naming Conventions

- **Parameters and variables**: @ prefix with snake_case (@database_name, @debug)
- **Temporary tables**: # prefix with descriptive snake_case (#filtered_objects)
- **Aliases**: Short, meaningful, lowercase (o, ol, c)
- **Column aliases**: Always the pattern `alias_name = expression`:
  ```sql
  some_date = DATEADD(DAY, 1, GETDATE())
  ```

## Query Structure

- **SELECT statements**:
  - SELECT keyword on the first line
  - Column list starts on the next line, indented four spaces, one column per line
  - FROM on a new line at the same indent level as SELECT

- **Table references**:
  - Preserve the user's schema qualifiers exactly as written — never add or remove them (see SchemaGuide)
  - In new code you author (index scripts, examples), schema-qualify everything except temporary objects: `FROM dbo.orders`, `FROM #temp_table`

- **Window functions**: OVER on the same line as the function; PARTITION BY and ORDER BY indented on separate lines; parentheses on their own lines:
  ```sql
  SELECT
      n = ROW_NUMBER() OVER
          (
              PARTITION BY
                  column_name
              ORDER BY
                  other_column
          )
  ```

- **JOIN syntax**:
  - Modern ANSI joins only (JOIN ... ON); flag any old-style comma joins for rewrite
  - JOIN keyword on a new line at the same indent level as FROM
  - ON indented two spaces, AND conditions aligned beneath it; the most recently referenced table comes first in the ON clause:
  ```sql
  FROM dbo.table_a AS a
  JOIN dbo.table_b AS b
    ON  b.col = a.col
    AND b.other_col = a.other_col
  ```

- **Clauses**:
  - GROUP BY, ORDER BY, and HAVING each begin on a new line, with their columns indented four spaces on subsequent lines
  - WHERE with AND conditions aligned:
  ```sql
  WHERE a.col = 1
  AND   b.col = 2
  ```

- **EXISTS / NOT EXISTS**: parenthesized block with SELECT 1:
  ```sql
  WHERE EXISTS
  (
      SELECT
          1
      FROM dbo.other_table AS ot
      WHERE ot.col = t.col
  )
  ```

- **Subqueries**: never one-liners — new lines with proper indentation:
  ```sql
  SELECT
      column_name =
      (
          SELECT
              ot.column_name
          FROM dbo.other_table AS ot
          WHERE ot.condition = 1
      )
  ```

- **APPLY operators**:
  ```sql
  FROM dbo.a_table AS y
  CROSS APPLY
  (
      SELECT
          x.columns
      FROM dbo.table_name AS x
      WHERE x.col = y.col
  ) AS x
  ```

- **Set operations**: operator between statements with blank lines around it:
  ```sql
  SELECT
      a.columns
  FROM dbo.a_table AS a

  EXCEPT

  SELECT
      b.columns
  FROM dbo.b_table AS b;
  ```

- **Table-valued constructors (VALUES)**:
  ```sql
  FROM
  (
      VALUES
          (1, 2, 3)
  ) AS v (named_columns)
  ```

- **CTEs**: WITH on its own line; CTE name indented; optional column list parenthesized on its own lines; AS on its own line; multiple CTEs separated by trailing commas:
  ```sql
  WITH
      first_cte AS
  (
      SELECT
          o.column_name
      FROM dbo.orders AS o
  ),
      second_cte AS
  (
      SELECT
          c.column_name
      FROM dbo.customers AS c
  )
  ```

## DML Formatting

- **INSERT statements**: always INSERT INTO with an explicit column list:
  ```sql
  INSERT INTO
      dbo.table_name
  (
      column1,
      column2
  )
  VALUES
  (
      value1,
      value2
  );
  ```

- **Temporary table inserts**: use the TABLOCK hint:
  ```sql
  INSERT
      #table_name
  WITH
      (TABLOCK)
  (
      column_list
  )
  ```

- **UPDATE statements**:
  ```sql
  UPDATE
      alias
  SET
      alias.col1 = value1,
      alias.col2 = value2
  FROM dbo.table_name AS alias
  WHERE alias.condition = 1;
  ```

- **DELETE statements**:
  ```sql
  DELETE
      alias
  FROM dbo.table_name AS alias
  WHERE alias.condition = 1;
  ```

## DDL Formatting

- **Table creation**: every column explicitly NULL or NOT NULL; DEFAULT constraints on the same line as the column:
  ```sql
  CREATE TABLE
      dbo.table_name
  (
      column_name bigint NOT NULL,
      another_column varchar(50) NULL DEFAULT 'value',
      third_column datetime2(7) NOT NULL DEFAULT SYSDATETIME()
  );
  ```

- **Index creation** (the format for this Space's index recommendations):
  ```sql
  CREATE INDEX
      index_name
  ON dbo.table_name
  (
      column1,
      column2
  )
  INCLUDE
  (
      column3,
      column4
  )
  WITH
      (ONLINE = ON);
  ```
  Single-column indexes may use a compact form with `(column1)` indented on one line.

## Control Flow and Code Blocks

- **CASE expressions**: each condition on a new line:
  ```sql
  CASE
      WHEN thing
      AND  other_thing
      THEN stuff
      ELSE result
  END
  ```

- **IF/ELSE and WHILE**: BEGIN/END on their own lines, contents indented four spaces:
  ```sql
  IF condition
  BEGIN
      /*logic*/
  END;
  ELSE
  BEGIN
      /*logic*/
  END;
  ```

- **DECLARE blocks**: one variable per line; declare and initialize together for static values (taking care not to introduce logical flaws with NULL checks):
  ```sql
  DECLARE
      @t1 integer = 1,
      @t2 integer = 2;
  ```

- **Error handling**: TRY/CATCH with rollback and THROW:
  ```sql
  BEGIN TRY
      /*work*/
  END TRY
  BEGIN CATCH
      IF @@TRANCOUNT > 0
      BEGIN
          ROLLBACK;
      END;

      THROW;
  END CATCH;
  ```

- **Dynamic SQL**: always parameterized with sys.sp_executesql — values go in as parameters, never concatenated into the string; QUOTENAME is for identifiers only. Concatenating user values into dynamic SQL is an injection risk and must be flagged:
  ```sql
  DECLARE
      @table_name sysname = N'orders',
      @cutoff_date date = '20260101',
      @sql nvarchar(max) = N'';

  SET @sql = N'
  SELECT
      order_count = COUNT_BIG(*)
  FROM dbo.' + QUOTENAME(@table_name) + N' AS t
  WHERE t.order_date >= @cutoff_date;
  ';

  EXECUTE sys.sp_executesql
      @sql,
      N'@cutoff_date date',
      @cutoff_date;
  ```

## T-SQL Practices (Azure SQL Database)

- Always use IS NULL / IS NOT NULL for NULL comparisons — never = NULL or != NULL
- Use ISNULL() for simple value replacement; COALESCE when multiple fallbacks are needed
- Date literals in yyyymmdd format (e.g., '20260101') — unambiguous under any language or DATEFORMAT setting
- Use STRING_SPLIT for splitting and STRING_AGG for building delimited strings — always available on Azure SQL Database; do not use legacy XML-based splitting/concatenation tricks
- CONCAT / CONCAT_WS are fine for string building and handle NULLs gracefully; + is acceptable when NULL propagation is intended
- Prefer CONVERT for explicit conversions; TRY_CONVERT when the input may not be valid
- Avoid FORMAT() in queries — it is dramatically slower than CONVERT-based alternatives; number/date presentation belongs in the application layer
- Avoid MERGE — use separate INSERT / UPDATE / DELETE statements unless MERGE is functionally required
- Temp tables vs table variables: follow queryguide section 1.3 (deferred compilation at compatibility level 150+ changes the old "always temp tables" rule)
- Prefer COUNT_BIG() / ROWCOUNT_BIG() where counts could exceed the integer range (recommendation-only on user queries — it changes the output column type)
- Cursors: prefer set-based rewrites (see queryguide); where a cursor must remain, use cursor variables, which need no explicit CLOSE/DEALLOCATE
- Do not drop temp tables at the end of stored procedures — they are cleaned up automatically
- Stored procedures should start with SET NOCOUNT, XACT_ABORT ON; never SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED (RCSI is on by default in Azure SQL Database — see queryguide Rule 4)
- Query hints (RECOMPILE, MAXDOP, OPTIMIZE FOR, etc.) are never a style matter — they may only be recommended with evidence, per the main instructions

## Reference Example

A query formatted to this guide:

```sql
WITH
    recent_orders AS
(
    SELECT
        customer_id = o.customer_id,
        order_id = o.order_id,
        order_total = o.order_total,
        order_rank = ROW_NUMBER() OVER
            (
                PARTITION BY
                    o.customer_id
                ORDER BY
                    o.order_date DESC
            )
    FROM dbo.orders AS o
    WHERE o.order_date >= '20260101'
    AND   o.order_status = N'COMPLETE'
)
SELECT
    c.customer_id,
    customer_name = c.display_name,
    ro.order_id,
    ro.order_total,
    order_band =
        CASE
            WHEN ro.order_total >= 1000
            THEN N'LARGE'
            ELSE N'STANDARD'
        END
FROM dbo.customers AS c
JOIN recent_orders AS ro
  ON  ro.customer_id = c.customer_id
  AND ro.order_rank = 1
WHERE EXISTS
(
    SELECT
        1
    FROM dbo.order_lines AS ol
    WHERE ol.order_id = ro.order_id
    AND   ol.line_status = N'SHIPPED'
)
ORDER BY
    ro.order_total DESC;
```
