# Variables

Variables let you name and reuse intermediate query results across expressions. They compile to CTEs (`WITH` clauses) in the generated SQL.

## Syntax

```
<expression> |= <name>
```

Append `|= name` at the end of any pipe chain to assign the result to a variable.

## Example

```
company | where: active = true |= active_companies

active_companies | employee
```

The first expression defines `active_companies`. The second expression uses it as a table — Pine expands it into a CTE:

```sql
WITH "active_companies" AS (
  SELECT "c_0".* FROM "company" AS "c_0" WHERE "c_0"."active" = true
)
SELECT "e_1".id AS "__e_1__id", "e_1".*
FROM "active_companies" AS "ac_0"
JOIN "employee" AS "e_1" ON "ac_0"."id" = "e_1"."company_id"
LIMIT 250
```

## Multi-expression input

Expressions are separated by blank lines. Each expression is evaluated in order; earlier expressions provide context (as CTEs) for later ones. Only the last expression is executed and returned.

```
company | where: active = true |= active_companies

active_companies | l: 10 |= small_active

small_active
```

This produces two CTEs (`active_companies`, `small_active`) and selects from the last.

## How it works

- **Assignment** (`|= name`): the expression is stored as a variable. Its SQL becomes the body of a CTE named `name`.
- **Usage**: when `name` appears as a table in a later expression, Pine substitutes the CTE. Joins through variable tables resolve using the schema of the underlying real table(s).
- **Scoping**: variables accumulate left-to-right. Each expression sees all variables defined before it.
- **Auto-id columns**: Pine normally adds a hidden `id` column per table for internal row tracking. Variable tables (CTEs) are excluded — only real database tables get auto-id columns.
- **CTE body**: the CTE body is generated without a `LIMIT` (limits only apply to the outer query).

## Constraints

- Variable names must be valid Pine identifiers (letters, digits, underscores).
- A variable used but not defined in a preceding expression will cause a parse/table-not-found error.
- Circular references are not supported.
