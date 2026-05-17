# Checkpoints

Automatically seal a GROUP or LIMIT result into an anonymous CTE so subsequent table operations compose on top of it.

## Why

GROUP and LIMIT produce a final, bounded result set — further joins appended after them produce malformed SQL (a GROUP BY inside a subquery that is then joined, or a LIMIT applied before the join filter). Checkpoints solve this by detecting when a table operation follows a GROUP or LIMIT and implicitly wrapping the preceding query in a CTE, then continuing the pipeline on top of that CTE.

Without checkpoints, `company | l: 10 | employee` would attempt to build a single query with LIMIT and a JOIN, which contradicts the intent (get 10 companies, then navigate to their employees).

## Syntax

No syntax change is required. Checkpoints fire automatically when a TABLE operation follows a GROUP or LIMIT:

```
<expression> | (group:|limit:) ... | <table> [| more-ops...]
```

Optionally assign the auto-generated CTE a name by placing `|= name` between the checkpoint op and the table:

```
<expression> | (group:|limit:) ... |= <name> | <table> [| more-ops...]
```

## Examples

### LIMIT checkpoint (auto-named)

```
x.company | l: 10 | employee
```

Pine seals `x.company | l: 10` as an anonymous CTE `__pine_0__` and joins `employee` to it:

```sql
WITH "__pine_0__" AS (
  SELECT "c_0".* FROM "x"."company" AS "c_0" LIMIT 10
)
SELECT "e_1".id AS "__e_1__id", "e_1".*
FROM "__pine_0__" AS "__pine_0__"
JOIN "employee" AS "e_1" ON "__pine_0__"."id" = "e_1"."company_id"
LIMIT 250
```

### LIMIT checkpoint (user-named via `|=`)

```
x.company | l: 10 |= pg | employee
```

Same result but the CTE is named `pg`:

```sql
WITH "pg" AS (
  SELECT "c_0".* FROM "x"."company" AS "c_0" LIMIT 10
)
SELECT "e_1".id AS "__e_1__id", "e_1".*
FROM "pg" AS "pg"
JOIN "employee" AS "e_1" ON "pg"."id" = "e_1"."company_id"
LIMIT 250
```

### GROUP checkpoint

```
x.company | group: id => count | employee
```

The GROUP result is sealed as a CTE and employee is joined to it:

```sql
WITH "__pine_0__" AS (
  SELECT "c_0"."id", COUNT(1) AS "count"
  FROM "x"."company" AS "c_0"
  GROUP BY "c_0"."id"
)
SELECT "e_1".id AS "__e_1__id", "e_1".*
FROM "__pine_0__" AS "__pine_0__"
JOIN "employee" AS "e_1" ON "__pine_0__"."id" = "e_1"."company_id"
LIMIT 250
```

### No checkpoint when non-table op follows

```
company | limit: 100 | count:
```

`count:` is not a table op — no checkpoint fires. The pipeline remains a single query and COUNT wraps it normally:

```sql
WITH x AS ( SELECT "c_0".* FROM "company" AS "c_0" LIMIT 100 )
SELECT COUNT(*) FROM x
```

## How it works

- **Detection**: after processing a GROUP or LIMIT op, `handle-ops` sets `:pending-checkpoint {:needs-assign true}` on the state.
- **Fire on table**: at the start of each `handle-ops` iteration, `flush-checkpoint` checks whether a checkpoint is pending and whether the incoming op is a TABLE op. If both are true, the current state is snapshotted into `:pending-assignments` under an auto-generated name (`__pine_0__`, `__pine_1__`, ...), the references map is seeded for that name (same FK propagation used by variables), and the state is reset. The CTE name is then injected as the first table so subsequent ops join to it.
- **User-named CTE**: if an `|= name` op appears between the checkpoint op and the table op, `flush-checkpoint` records the name and waits for the table. `assign/handle` stores the snapshot under `name` as normal. When the table op arrives, `activate-checkpoint-cte` seeds references for the named CTE and resets state.
- **Hold for non-table ops**: if the op after GROUP/LIMIT is something other than a table or assign (e.g. `count:`, `where:`), the checkpoint stays pending and the op is processed normally without firing.
- **SQL generation**: because the CTE table has an `:ast` entry in `:aliases`, `collect-ctes` automatically picks it up and emits the `WITH` clause. No changes to `eval.clj` were needed.

## Constraints

- Checkpoint op types are GROUP and LIMIT. ORDER does not create a checkpoint.
- Only one table-level composition step is supported per checkpoint; chaining `l: 10 | employee | document` creates one CTE for `l: 10` and then navigates normally through employee to document.
- Auto-generated CTE names (`__pine_0__`, etc.) are numbered per expression and are not exposed to the user.
- Checkpoint CTEs do not receive Pine's auto-id columns; those are suppressed for all CTE-backed tables.

See also: [variables.md](variables.md) for cross-expression named CTEs.

---

## Implementation

### State fields (`ast/main.clj`)

| Field | Default | Purpose |
|---|---|---|
| `:auto-cte-count` | `0` | Counter for generating `__pine_0__`, `__pine_1__`, etc. |
| `:pending-checkpoint` | `nil` | `{:needs-assign true}` after a checkpoint op; `{:name n :needs-table true}` after an explicit `|=` |

### Core functions (`ast/main.clj`)

- `checkpoint-op-types` — set `#{:group :limit}`
- `reset-for-cte` — clears all query-building fields (tables, columns, joins, where, order, group, limit) while preserving references, variables, and pending-assignments
- `seal-as-cte` — the single shared action: stores the snapshot under `cname` in `:pending-assignments`, seeds FK references for `cname` (same three passes used by cross-expression variables in `pre-handle`), resets query-building state, then injects `cname` as the first table via `handle-op`; `table/handle` resolves it via `:pending-assignments`
- `flush-checkpoint` — state-machine dispatch called at the start of each `handle-ops` reduce iteration; calls `seal-as-cte` when a TABLE op follows the checkpoint, using either a freshly-snapshotted state (auto-named) or the snapshot already stored by `assign/handle` (user-named)

### `table/handle` (`ast/table.clj`)

Updated to check `:pending-assignments` alongside `:variables` when looking up the CTE for a table name:

```clojure
var-ast (or (get-in state [:variables table])
            (get-in state [:pending-assignments table]))
```

### `add-auto-id-columns` (`ast/select.clj`)

Changed the predicate from checking `variables` membership to checking `(:ast (get aliases %))`. Any alias that carries an `:ast` entry is a CTE-backed table and should not receive an auto-id column.
