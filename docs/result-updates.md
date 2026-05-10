# Updating Results from Join Queries

Editing cells in multi-table result sets.

## Why

A Pine expression like `user | document` returns a result set that mixes columns from two tables. You might want to edit a document's `name` field directly in that result. The challenge is that a naive `UPDATE document SET name = ?` doesn't know which specific document row to target — it needs the `id` of the document for the row you edited, not just any document.

Pine solves this with two mechanisms working together:

- **Auto-id columns**: for each table in the query, Pine silently appends a hidden column `alias.id AS "__alias__id"`. These are never displayed but are always present in the result, so the UI can look up the exact row ID for any cell.
- **`update!`**: generates separate UPDATE statements per table, each scoped to the correct row via a subquery that inherits the full join condition.

Together they make any join result editable, not just readable.

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
UPDATE "company" SET "name" = ? WHERE id IN (
  SELECT "c_0"."id" FROM "company" AS "c_0" WHERE "c_0"."id" = 42
)
```

### Multi-table update

```
user as u | document | u! u.name = 'Alice', title = 'Passport'
```

Pine generates one UPDATE per table:

```sql
UPDATE "user" SET "name" = ? WHERE id IN (
  SELECT "u"."id" FROM "user" AS "u" JOIN "document" AS "d_0" ON ...
)

UPDATE "document" SET "title" = ? WHERE id IN (
  SELECT "d_0"."id" FROM "user" AS "u" JOIN "document" AS "d_0" ON ...
)
```

Each subquery targets only the relevant table's `id`, scoped by the full join condition.

### Auto-id columns in the result

```
user as u | document
```

The generated SQL includes hidden auto-id columns alongside the visible data:

```sql
SELECT "u".id AS "__u__id",
       "d_0".id AS "__d_0__id",
       "d_0".*
FROM "user" AS "u"
JOIN "document" AS "d_0" ON "u"."id" = "d_0"."user_id"
LIMIT 250
```

`__u__id` and `__d_0__id` are hidden in the grid but available when a cell is edited.

### Inline cell edit

When you edit a cell directly in the result grid, the UI constructs an `update!` expression automatically:

1. The edited cell belongs to column index N.
2. `colIndexToAliasLookup[N]` → table alias (e.g. `"d_0"`).
3. `aliasToIdLookup["d_0"]` → index of the `__d_0__id` column in the row.
4. The ID value is read from that column in the current row.
5. Pine evaluates: `<original expression> | where: d_0.id = <id> | u! <column> = <value>`

## How it works

- **Auto-id columns** are appended automatically for every real table that has an `id` column. They are marked `hidden: true` (not shown in the grid) and `auto-id: true` (so internal logic can identify them). Variable/CTE tables are excluded — they don't have a stable `id`.
- **Column qualification**: unqualified columns (e.g. `name`) default to the last table in the expression. Qualified columns (`u.name`) target the specified alias.
- **One UPDATE per table**: assignments are grouped by target alias. Each group becomes an independent UPDATE with a subquery to identify the rows.
- **Subquery for row targeting**: Pine uses `WHERE id IN (SELECT id FROM ... JOIN ...)` rather than a direct `WHERE id = ?`. This ensures the update respects the full join condition.

## Constraints

- Only tables with an `id` column in the DB schema get an auto-id. Tables without `id` cannot be targeted by `update!`.
- If the user explicitly selects `id` (e.g. `s: id, name`), the auto-id column is still added with its `__alias__id` alias to avoid ambiguity.
- Auto-id columns are not added inside CTE bodies — `SELECT *` already includes `id`, and a second `id` would cause an ambiguous-column error.
- `update!` on a variable/CTE table is not supported — the CTE has no physical table to update.

---

## Implementation

### Auto-id columns (`ast/select.clj`)

`add-auto-id-columns` runs in `post-handle` after all operations are applied. For each table alias whose underlying table has an `id` column (checked via `has-id-column?`), it appends:

```clojure
{ :column       "id"
  :alias        alias
  :column-alias "__alias__id"
  :hidden       true
  :auto-id      true
  :operation-index N }
```

Variable tables are skipped: if the table name for an alias is a key in `state :variables`, no auto-id is added.

### CTE body deduplication (`eval.clj`)

`build-cte-body` generates the inner SQL for a variable's CTE. Before calling `build-bare-select`, it checks whether user-specified columns already cover the current table. If so, the auto-id column is dropped to avoid `SELECT id, ..., id` duplication.

### Frontend column metadata (`default.plugin.tsx`)

After a query runs, the UI builds two lookup maps from the response column metadata:

- `colIndexToAliasLookup` — column position → table alias
- `aliasToIdLookup` — table alias → position of its auto-id column in the row

### Frontend cell editing (`Result.tsx`)

`processRowUpdate` is called by MUI DataGrid when a cell is committed. It uses the two lookup maps to find the row ID, builds an update expression via `createUpdateExpression`, and evaluates it in a virtual session before refreshing the main session.

### SQL generation (`eval/build-update-queries`)

Assignments are grouped by target alias. For each group, `build-single-update-query` produces the UPDATE with a subquery built by temporarily replacing the state's column list with `[{:column "id" :alias update-alias}]` and calling `build-select-query` — so it inherits the full JOIN and WHERE conditions from the original expression.

### Parsing (`pine.bnf`, `parser.clj`)

`update!` / `u!` are parsed as `:update-action`. Each assignment carries `{:column {:alias ... :column ...} :value {...}}`. Partial typing (`u! col`) is parsed as `:update-partial` for hint generation.
