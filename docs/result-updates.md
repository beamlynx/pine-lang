# Updating Results from Join Queries

Editing cells in multi-table result sets.

## Why

A Pine expression like `user | document` returns a result set that mixes columns from two tables. You might want to edit a document's `name` field directly in that result. The challenge is that a naive `UPDATE document SET name = ?` doesn't know which specific document row to target — it needs the `id` of the document for the row you edited.

Pine solves this with two mechanisms working together: auto-id columns that track each table's primary key in every result row, and `update!` which generates separate UPDATE statements per table. This means you can edit any column in a join result and Pine will route the update to the correct table automatically.

## Syntax

```
<expression> | update! <column> = <value>
<expression> | u! <column> = <value>
```

Multiple assignments, optionally across different tables:

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

### Inline cell edit

When you edit a cell directly in the result grid, the UI constructs an `update!` expression automatically:

1. The edited cell belongs to column index N.
2. `colIndexToAliasLookup[N]` → table alias (e.g. `"d_0"`).
3. `aliasToIdLookup["d_0"]` → index of `__d_0__id` in the row.
4. The ID value is read from the current row at that index.
5. Pine evaluates: `<original expression> | where: d_0.id = <id> | u! <column> = <value>`

## How it works

- **Column qualification**: unqualified columns (e.g. `name`) default to the last table in the expression. Qualified columns (`u.name`) target the specified alias.
- **One UPDATE per table**: assignments are grouped by target alias. Each group becomes an independent UPDATE with a subquery to identify the rows.
- **Subquery for row targeting**: rather than a direct `WHERE id = ?`, Pine uses `WHERE id IN (SELECT id FROM ... JOIN ...)`. This ensures the update respects the full join condition, not just a naked ID lookup.
- **Auto-id columns enable inline editing**: hidden `__alias__id` columns in every result row carry the primary key for each table. The UI uses them to route cell edits to the right UPDATE. See [auto-id.md](auto-id.md).

## Constraints

- The target tables must have an `id` column. Tables without `id` cannot be targeted by `update!`.
- Only one value per column per `update!` expression.
- `update!` on a variable/CTE table is not supported — the CTE has no physical table to update.

---

## Implementation

### Parsing (`pine.bnf`, `parser.clj`)

`update!` and `u!` are parsed as `:update-action` operations. Each assignment in the value list carries a `{:column {:alias ... :column ...} :value {...}}` map. Partial typing (`u! col`) is parsed as `:update-partial` for hint generation.

### AST (`ast/update-action.clj`)

`handle` records the assignments in `state :update`. Column aliases default to the current table alias when not explicitly qualified.

### SQL generation (`eval/build-update-queries`)

Assignments are grouped by their target alias. For each group, `build-single-update-query` produces:

```sql
UPDATE "schema"."table" SET "col" = ?
WHERE id IN ( SELECT "alias"."id" FROM ... )
```

The subquery is built by temporarily replacing the state's column list with just `[{:column "id" :alias update-alias}]` and calling `build-select-query`, so it inherits the full JOIN and WHERE conditions from the original expression.

### Frontend cell editing (`Result.tsx`)

`processRowUpdate` is called by MUI DataGrid when a cell is committed. It:

1. Finds the changed field (column index).
2. Looks up the table alias via `colIndexToAliasLookup`.
3. Looks up the auto-id column index via `aliasToIdLookup`.
4. Reads the row ID from the current row data.
5. Builds an update expression via `createUpdateExpression` and evaluates it in a virtual session, then refreshes the main session.
