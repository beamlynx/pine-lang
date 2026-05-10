# Auto-ID Columns

Hidden columns Pine adds to track the primary key of each table in a result set.

## Why

When a Pine expression joins multiple tables, each row in the result contains data from more than one table. If you want to update or delete a cell in that row, Pine needs to know two things: which table that column belongs to, and what that table's row ID is. Without this, a join result is read-only — you can see the data but can't target a specific row for mutation.

Auto-id columns carry this information. For each table in the query, Pine appends a hidden column `table_alias.id AS "__alias__id"`. The UI reads these to build the correct `WHERE id = ?` clause when a cell is edited.

## Example

```
user as u | document
```

The result includes two visible tables and two hidden auto-id columns:

```sql
SELECT "u".id AS "__u__id",
       "d_0".id AS "__d_0__id",
       "d_0".*
FROM "user" AS "u"
JOIN "document" AS "d_0" ON "u"."id" = "d_0"."user_id"
LIMIT 250
```

`__u__id` tracks the `user` row, `__d_0__id` tracks the `document` row. Both are hidden in the UI by default but available to the update logic.

## How it works

- For every table in the query that has an `id` column, Pine appends a hidden auto-id column: `alias.id AS "__alias__id"`.
- The column is marked `hidden: true` so the UI doesn't display it in the grid.
- The column is marked `auto-id: true` so internal logic can identify and handle it separately from user-selected columns.
- The UI maintains a `colIndexToAliasLookup` and `aliasToIdLookup` map. When a cell is edited, the UI finds the table alias for that column, looks up the corresponding auto-id column index, reads the ID value from that row, and constructs the update expression.
- Variable tables (CTEs) are excluded — they don't have a stable `id` and their underlying table's auto-id is already tracked separately. See [variables.md](variables.md).

## Constraints

- Only tables with an `id` column in the DB schema get an auto-id. Tables without `id` are skipped.
- If the user explicitly selects `id` (e.g. `s: id, name`), the auto-id column is still added but with its `__alias__id` alias to avoid ambiguity.
- Auto-id columns are not included in CTE bodies — adding them when `SELECT *` is already in the CTE would create a duplicate `id` column, causing a SQL error.

---

## Implementation

### Adding auto-id columns (`ast/select.clj`)

`add-auto-id-columns` runs in `post-handle` after all operations are applied. It iterates over all table aliases in the state and, for each alias whose underlying table has an `id` column (checked via `has-id-column?` against the references map), appends a column entry:

```clojure
{ :column       "id"
  :alias        alias
  :column-alias "__alias__id"
  :hidden       true
  :auto-id      true
  :operation-index N }
```

Variable tables are excluded: if the table name for an alias is a key in `state :variables`, no auto-id is added.

### CTE body deduplication (`eval.clj`)

`build-cte-body` generates the inner SQL for a variable's CTE using `build-bare-select`. Before doing so, it checks whether the user's explicit columns already include a column for the current table. If they do, the auto-id column (which would generate `alias.id`) is dropped to avoid `SELECT id, ..., id` duplication.

### Frontend column metadata (`default.plugin.tsx`, `Result.tsx`)

After a query runs, the UI builds two lookup maps from the column metadata:

- `colIndexToAliasLookup` — maps column position → table alias
- `aliasToIdLookup` — maps table alias → position of its auto-id column

When a cell is edited, `Result.tsx` uses these to find the right ID value in the current row and constructs an `update!` expression that Pine evaluates against the database. See [result-updates.md](result-updates.md).
