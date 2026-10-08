# Updating Results from Join Queries

Editing cells in multi-table result sets.

## Why

A Pine expression like `user | document` returns a result set that mixes columns from two tables. You
might want to edit a document's `name` field directly in that result. A plain
`UPDATE document SET name = ?` doesn't know which document row you edited. It needs the key of that
row.

A row's key is its table's **primary key**. That can be one column, such as `id`, or several, such as
`(group_code, member_code)`. A table without a primary key, such as a view, has no key, and its rows
can't be edited this way.

Pine uses the key in two places:

- **Hidden key columns**: for each table in the query, Pine adds a hidden column for each primary key
  column, named `__<alias>__<column>`. They are never displayed but are always in the result, so the UI
  can tell which row a cell belongs to.
- **`update!`**: generates one UPDATE statement per table. Each finds its rows by the table's primary
  key, through a subquery that keeps the expression's joins and conditions.

## Syntax

```
<expression> | update! <column> = <value>
<expression> | u! <column> = <value>
```

Multiple assignments, optionally targeting different tables:

```
user as u | document | u! u.name = 'Alice', title = 'Passport'
```

## Examples

### Single-table update

```
company | where: id = 42 | u! name = 'Acme Corp'
```

```sql
UPDATE "company" SET "name" = ? WHERE "id" IN (
  SELECT "c_0"."id" FROM "company" AS "c_0" WHERE "c_0"."id" = 42
)
```

### A key of several columns

`membership` is keyed on `(group_code, member_code)`. The key is matched as a row:

```
membership | where: role = 'guest' | u! role = 'member'
```

```sql
UPDATE "membership" SET "role" = ? WHERE ("group_code", "member_code") IN (
  SELECT "m_0"."group_code", "m_0"."member_code" FROM "membership" AS "m_0" WHERE "m_0"."role" = ?
)
```

### Multi-table update

```
user as u | document | u! u.name = 'Alice', title = 'Passport'
```

Pine generates one UPDATE per table:

```sql
UPDATE "user" SET "name" = ? WHERE "id" IN (
  SELECT "u"."id" FROM "user" AS "u" JOIN "document" AS "d_0" ON ...
)

UPDATE "document" SET "title" = ? WHERE "id" IN (
  SELECT "d_0"."id" FROM "user" AS "u" JOIN "document" AS "d_0" ON ...
)
```

Each subquery selects only its own table's key, narrowed by the full join condition.

### Hidden key columns in the result

```
user as u | document
```

The generated SQL includes the hidden key columns alongside the visible data:

```sql
SELECT "u"."id" AS "__u__id",
       "d_0"."id" AS "__d_0__id",
       "d_0".*
FROM "user" AS "u"
JOIN "document" AS "d_0" ON "u"."id" = "d_0"."user_id"
LIMIT 250
```

`__u__id` and `__d_0__id` are hidden in the grid but available when a cell is edited.

### Inline cell edit

When you edit a cell directly in the result grid, the UI writes an `update!` expression for you:

1. The edited cell belongs to column index N.
2. `colIndexToAliasLookup[N]` gives the table alias, for example `"d_0"`.
3. `aliasToKeyLookup["d_0"]` gives that table's key columns and where each one is in the row.
4. The UI reads each key value from the current row.
5. Pine evaluates `<original expression> | from: d_0 | where: id = <id> | u! <column> = <value>`,
   with one `where:` step for each key column.

## How it works

- **The key** is the table's declared primary key, read from the database when the connection is
  indexed (`pine.db.references/primary-key`). An unqualified table uses the key of the schema it
  resolves to, the same schema its columns come from. When several schemas have the table and none of
  them is `public`, there is no key.
- **Hidden key columns** are added for every real table that has a primary key. They are marked
  `hidden: true` (not shown in the grid) and `auto-id: true` (so the UI can find them). Named results
  are excluded: they have no primary key.
- **Column qualification**: unqualified columns (e.g. `name`) default to the last table in the
  expression. Qualified columns (`u.name`) target the specified alias.
- **One UPDATE per table**: assignments are grouped by target alias. Each group becomes an independent
  UPDATE with a subquery to identify the rows.
- **Subquery for row targeting**: Pine uses `WHERE <key> IN (SELECT <key> FROM ... JOIN ...)` rather
  than a direct `WHERE id = ?`. This keeps the update within the full join condition.

## Constraints

- A table without a primary key gets no hidden key columns, and `update!` refuses it with
  `error-type: "write-refused"`. This includes views, and tables that have an `id` column but no
  primary key.
- Under an access policy, a table gets no hidden key columns when the policy hides any column of its
  key, so a hidden value is never sent as a key. Its rows can't be edited from the grid. `id` is always
  shown.
- If the user explicitly selects a key column (e.g. `s: id, name`), the hidden column is still added
  under its `__alias__column` name, so the two don't clash.
- `update!` on a named result is refused. A named result has no physical table to update.

---

## Implementation

### Indexing keys (`db/postgres.clj`, `db/mysql.clj`, `db/references.clj`)

Each dialect reads primary key columns in key order: Postgres from `pg_constraint` (`contype = 'p'`),
MySQL from `information_schema.key_column_usage` (`constraint_name = 'PRIMARY'`). `index-references`
files them under `[:schema s :table t :primary-key]`, and `resolve-bare-tables` copies the resolved
schema's key to `[:table t :primary-key]`.

### Row keys and hidden columns (`ast/select.clj`)

`add-row-keys` runs in `post-handle` and records each real table's key as `:row-keys {alias [column
...]}`. `update!` reads it from there, since `:references` is dropped from the final state.

`add-auto-id-columns` runs next. For each key column of each table, it appends:

```clojure
{ :column       "group_code"
  :alias        "m_0"
  :column-alias "__m_0__group_code"
  :hidden       true
  :auto-id      true
  :operation-index N }
```

### Frontend column metadata (`default.plugin.tsx`)

After a query runs, the UI builds lookup maps from the response's column metadata:

- `colIndexToAliasLookup`: column position to table alias
- `aliasToKeyLookup`: table alias to its key columns and their positions in the row, from the columns
  marked `auto-id`

### SQL generation (`eval/build-update-queries`)

Assignments are grouped by target alias. For each group, `build-single-update-query` produces the
UPDATE. Its subquery is built by replacing the state's column list with the table's key columns and
calling `build-select-query`, so it keeps the full JOIN and WHERE conditions of the original
expression.

### Parsing (`pine.bnf`, `parser.clj`)

`update!` / `u!` are parsed as `:update-action`. Each assignment carries
`{:column {:alias ... :column ...} :value {...}}`. Partial typing (`u! col`) is parsed as
`:update-partial` for hint generation.
