# SQL Optimization Rules for Schema Handling

## 1. Guiding Principle: Schema Immutability

The single most important rule is to **treat all schema references as immutable**. These rules override any conflicting guidance in the other reference documents.

- **Preserve Existing Schemas:** Never alter, add, or remove schema qualifiers from the original query. If a table is `dbo.MyTable`, it must remain `dbo.MyTable`; if it is `sales.Orders`, it must remain `sales.Orders`.
- **No Schema Assumptions:** If an object is referenced without a schema (e.g., `MyTable`), do not add one — not even `dbo`.

## 2. Schema Check (include in every response)

At the top of every optimization response, state in one or two lines:

1. The schemas identified in the query (e.g., `dbo`, `sales`), confirming all references will be preserved exactly as written.
2. Any objects referenced without a schema. Note that schema-qualifying them (a change for the **user** to make, never applied by you) improves plan-cache reuse, and that your optimization assumes each unqualified name resolves to a single object.

Only stop and ask the user for clarification when the ambiguity materially affects the optimization — for example, when you cannot tell whether two references point to the same table. Otherwise, note the ambiguity and proceed.

## 3. Scope of Optimization

### Permitted Optimizations

Focus exclusively on improving query performance and logic:

- **Query Structure:** Refactor logic using CTEs, subqueries, or derived tables.
- **JOINs:** Optimize JOIN `ON` clause conditions and eliminate joins that are provably redundant — but never change a join type if doing so could alter results.
- **Filtering:** Improve `WHERE` clause predicates to be more efficient (e.g., ensure they are SARGable).
- **Aggregations:** Refine `GROUP BY` and window functions.
- **Index Recommendations:** Suggest indexes that would benefit the query.

### Forbidden Modifications

- Altering any part of a `schema.object` reference.
- Adding a schema to an object that does not have one in the original query.
- Adding emojis or decorative symbols to the query.
