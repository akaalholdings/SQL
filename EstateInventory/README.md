# Azure SQL Estate Inventory

`AzureSqlEstateInventory.sql` returns the structure of one Azure SQL Database as one flat result set. Run it in each environment of an application (for example dev, test and prod), and compare the results to see where the databases agree and where they have drifted apart.

The query is read-only. It runs `SET` statements and one `SELECT` on catalog views. It reads no row of any table.

## What leaves the database

- Names: the database, schemas, tables, columns, indexes, constraints and modules.
- The text of defaults, check constraints, computed columns, index and statistics filters, masks and security predicates.
- The first and last boundary value of each partition function, and synonym targets.
- Row counts, dates, the edition and the service objective.

That text can hold literal values, such as a default, a list of codes or a tenant id in a filter. Read the result before you send it to anyone.

Views, procedures, functions and triggers leave as a SHA-256 hash of their text, never as the text.

## Run it

Run it once in each database.

1. Connect to the database itself, not to `master`. Use a login that has `VIEW DEFINITION` on the database. `db_datareader` alone is not enough: the catalog then hides rows without an error, and the row with `section = VISIBILITY` shows `view_definition=0`.
2. SQL Server Management Studio: set **Query > Results To > Results to Grid**, then execute the file.
3. Right-click the grid, choose **Save Results As**, and pick the file type **Text (Tab delimited) (\*.txt)**. Name the file after the database.
4. If the query stops with error 1222 (lock request time out period exceeded), a schema change was running in that database. Nothing was returned. Run it again.

Do not save as CSV from Management Studio, do not use Results to Text or Results to File, and do not open the file in Excel. Each of them cuts or breaks values.

In the `mssql` extension of VS Code, use the **Save as CSV** button of the result grid. Its CSV quotes values correctly.

Without a graphical client:

```powershell
Invoke-Sqlcmd -ServerInstance '<server>.database.windows.net' -Database '<database>' `
  -InputFile './AzureSqlEstateInventory.sql' -MaxCharLength 2147483647 |
  Export-Csv './<database>.csv' -NoTypeInformation -Encoding UTF8
```

Add the authentication parameters that your environment uses.

## Read the result

Each row is one fact.

| Column | Meaning |
| --- | --- |
| `database_name` | The database that the row came from |
| `section` | The kind of fact: see the list below |
| `schema_name`, `object_name` | The table, view, module, sequence or type |
| `item_name` | The column, index or constraint, when the fact is about one |
| `ordinal` | For a column: its position in the table (1, 2, 3 ...). For an index: its index id. Otherwise 0 |
| `definition` | The structure of the fact, as one line of text |
| `info` | Things that can differ without being a difference of structure: row counts, dates, the real name of a constraint that the engine named |

Two databases are aligned on a fact when both have a row with the same `section`, `schema_name`, `object_name` and `item_name`, and the same `definition`. `info` is not part of the comparison.

The engine gives some things a name that differs in each database. Such a row has an `item_name` made from what the thing is, not from its name:

- `<system-named PRIMARY KEY>`, `<system-named UNIQUE> ([columns])` for a key constraint;
- `<system-named> ([columns])` for a foreign key, `<system-named> <12 hex digits>` for a check constraint;
- `<history of [schema].[table]>` and `<engine-named history index>` for the history table of a temporal table that was made without a name.

The real name is in `info`. A default is keyed by its column; its name is part of `definition` only when a person named it.

Column order: compare `ordinal` of the `COLUMN` rows of a table. It is not part of `definition`, so that one missing column does not show every later column as different.

`info` holds flags that are not compared but that change what a deployment can do: `auto_created=1` on an index (automatic tuning made it), `not_trusted=1` on a constraint, `cdc=1` and `change_tracking=1` on a table. It also holds the start value of a sequence and the boundaries of a partition function, because a reseed or a sliding window moves them without a change of design.

If the `DATABASE` rows of two databases show different collations, compare the columns on `collation=` in `info`: `definition` names the collation of a column only when it differs from the default of its own database.

A file whose `VISIBILITY` row does not say `view_definition=1` is not complete. Do not compare it.

## Sections

| Section | One row for each |
| --- | --- |
| `DATABASE` | The database: compatibility level, collation, snapshot isolation, change data capture, change tracking |
| `VISIBILITY` | What the login could read |
| `SCHEMA` | Schema |
| `TABLE` | Table: storage, temporal state, partition scheme, compression |
| `COLUMN` | Column of a table: type, nullability, identity, computed text, mask |
| `DEFAULT` | Default constraint, keyed by its column |
| `INDEX` | Index, primary key or unique constraint of a table or a view |
| `CHECK` | Check constraint |
| `FOREIGN KEY` | Foreign key |
| `STATISTICS` | Statistics object that a person created |
| `TRIGGER` | Trigger on a table or a view |
| `DDL TRIGGER` | Trigger of the database, which fires on the statements of a deployment |
| `VIEW`, `PROCEDURE`, `FUNCTION` | Module: a hash of its text, and the `ANSI_NULLS` and `QUOTED_IDENTIFIER` settings that it is stored with |
| `SEQUENCE` | Sequence |
| `TYPE` | Alias type, or table type with its columns, keys and indexes |
| `SYNONYM` | Synonym |
| `PARTITION FUNCTION`, `PARTITION SCHEME` | Partition function or scheme |
| `SECURITY POLICY` | Predicate of a row-level security policy |

## Limits

- The hash in `definition` of a module is made from the text with spaces, tabs, line ends and letter case removed. Two modules with the same hash have the same text apart from layout. Two modules with different hashes can still do the same thing: a comment, a bracket around a name or a trailing semicolon changes the hash. `info` holds the hash of the exact text.
- The query does not return the columns of views, permissions, extended properties, the order of triggers, or the details of columnstore, XML and spatial indexes. For other indexes it returns the keys, the included columns, the filter, the compression, the partition scheme and the options that are not the default.
- The schema `azsqlcd` is left out: it holds the state tables of the azsqlcd deployment tool. The objects of database diagrams are left out too.
- The query was run against one Azure SQL Database at compatibility level 130. That database has no triggers on tables, synonyms, partition functions, security policies or user-created statistics, so those sections were compiled there and returned no rows. It has no table that the engine named the history table of, and no constraint that the engine named.
