# Variables

Name and reuse intermediate query results across expressions.

## Why

Complex queries often need to build up results in steps — filter a set of rows, then join that filtered
set to another table, then aggregate. Without variables, the only way to do this is with nested subqueries,
which are hard to read and hard to iterate on in a REPL-style tool. Variables let you give each step a
name, treat it as a table, and compose them incrementally.

## Syntax

```
<expression> |= <name> [| more-ops...]
```

Place `|= name` anywhere in a pipe chain to snapshot the query state at that point under `name`.
The pipeline continues after the assignment — operations after `|=` refine the current expression's
output while the snapshot remains frozen. Use `name` as a table in any later expression.

## Examples

### Basic

```
company | where: active = true |= active_companies

active_companies | employee
```

`active_companies` becomes a CTE. The second expression joins through it.
`|=` can appear at the end (as here) or mid-pipeline:

```sql
WITH "active_companies" AS (
  SELECT "c_0".* FROM "company" AS "c_0" WHERE "c_0"."active" = true
)
SELECT "e_1".id AS "__e_1__id", "e_1".*
FROM "active_companies" AS "ac_0"
JOIN "employee" AS "e_1" ON "ac_0"."id" = "e_1"."company_id"
LIMIT 250
```

### Mid-pipeline assign

```
company |= all_companies | where: active = true

all_companies
```

`all_companies` is the full unfiltered company snapshot — the snapshot was taken before `where:`.
The current expression still returns only active companies.

### Chained

Expressions are separated by blank lines and evaluated in order. Each one can build on the last.

```
company | where: active = true |= active_companies

active_companies | l: 10 |= small_active

small_active
```

This produces two CTEs and selects from the final one.

### Grouped

```
tenant as t | public.company .tenantId | g: t.title |= x

x
```

`x` exposes only the columns the CTE actually produces — here `title` and `count` — so hints for
`x | s:` show just those two columns.

Neither `tenant` nor `company` survives as a join source, though: grouping by `t.title` doesn't keep
`tenant`'s `id`, so `x | company` (or any other table) shows no join hints at all — see
[get-source-tables](#join-resolution-through-variables) below. Grouping by `t.id, t.title` instead would
keep `tenant` joinable.

## How it works

- **Assignment** (`|= name`): snapshots the pipeline state at that point. The snapshot becomes the CTE body
  when `name` is used in a later expression. Operations after `|=` continue refining the current expression's
  output — they do not affect the snapshot.
- **Usage**: when `name` appears as a table in a later expression, Pine substitutes the CTE. Joins resolve
  in both directions — `variable | table`, `table | variable`, and `variable | variable` — using the FK
  schema of the underlying real tables.
- **Scoping**: variables accumulate left-to-right. Each expression sees all variables defined before it.
  Mid-pipeline assignments within the current expression are NOT visible to that same expression.
- **Column hints**: when a variable has explicit output columns (from `s:`, `g:`, etc.), hints reflect
  those columns, not the full schema of the source table. Variables using `*` (no explicit selection)
  inherit the source table's columns.
- **No LIMIT in CTE body**: limits only apply to the outer query.
- **No auto-id columns**: Pine's hidden `id` tracking columns are not added for variable tables.
  See [result-updates.md](result-updates.md).

## Constraints

- Variable names must be valid Pine identifiers (letters, digits, underscores).
- A variable used but not defined in a preceding expression causes a table-not-found error.
- Circular references are not supported.

See also: [checkpoints.md](checkpoints.md) for same-expression anonymous CTEs after GROUP/LIMIT.

---

## Implementation

### Grammar and parsing

`|= name` is parsed as a regular pipe operation of type `:assign` in `pine.bnf`. The ASSIGN rule sits
inside OPERATION alongside TABLE, SELECT, WHERE, etc., so it participates in the normal pipeline.
`parser/parse` returns `{:result [ops...]}` with the assign op included in the list.

### State threading

The API receives an array of expressions. Each expression is parsed and evaluated in order:

1. `parser/parse(expr)` returns `{:result [ops...]}`. The operation list may include `:assign` ops.
2. `ast/generate(ops, ..., variables)` runs the full pipeline. When a `:assign` op is encountered,
   `assign/handle` snapshots the current state into `:pending-assignments` under the variable name.
3. After `ast/generate` returns, the caller merges `:pending-assignments` from the returned state into
   the running `variables` map.
4. The updated `variables` map is passed to `ast/generate` for every subsequent expression.

This is done purely in memory within the request — there is no persistence layer.

### CTE generation (`eval.clj`)

When `build-select-query` runs on a state that has variables, it calls `collect-ctes` to walk the variable
map and build `WITH name AS ( ... )` clauses. Each CTE body is generated by `build-cte-body`, which calls
`build-bare-select` (no LIMIT). WHERE params from inside CTEs are accumulated and merged into the outer
query's params list.

### Join resolution through variables

All three passes below build on one question, answered by the shared helper `get-source-tables`:
**which real tables can this variable actually be joined to?** A table only counts as a source if its
own `id` column is present among the CTE's *actual* output columns — including hidden, internally-added
ones, not just what was explicitly typed.

**Why a raw table join doesn't need this, but a sealed variable does**: `t | c` (no variable involved)
compiles to `... JOIN "c" ON "t"."id" = "c"."tenantId"` — a `JOIN ... ON` clause can reference any column
of a real table in `FROM`/`JOIN`, regardless of what's in `SELECT`. A CTE is different in kind, not degree:
once state is sealed into `WITH "x" AS ( SELECT ... )`, the outer query can no longer see `x`'s underlying
real tables at all — only whatever `x`'s own `:columns` produced. So a table stays a valid join source
*through a variable* only if its id actually survived into that snapshot.

This has nothing to do with `select/add-auto-id-columns` (which exists purely so the UI can identify which
row to update — see [result-updates.md](result-updates.md) — an unrelated concern that happens to also add
an id column, for different reasons, at a different time). It's handled by its own mechanism instead,
`assign/preserve-join-keys`, called from `assign/snapshot` — the one shared entry point for sealing state
into a CTE, used identically by a `|=` assignment (`assign/handle`) and a checkpoint's auto-named seal
(`flush-checkpoint` in `ast/main.clj`, the other place this happens). Its columns are marked `:hidden`
(excluded from column hints, same treatment as auto-id columns) but deliberately never `:auto-id`, so nothing
downstream can conflate "has a join key" with "is tracked for row updates." `preserve-join-keys` is a no-op
for `:group` — an unaggregated id column can't be silently added to a `GROUP BY` without changing what's
grouped by — which is what makes `get-source-tables` behave differently depending on how the CTE's columns
came to be:

- **No explicit columns** (`*`, e.g. a bare `where:`/`limit:` with no `s:`/`g:`): the CTE implicitly
  selects everything, which always includes `id` regardless of operation type. The variable's `:current`
  table is the sole, always-safe source.
- **Non-`GROUP` explicit columns** (`s:`): `preserve-join-keys` adds a join-key column for every real table
  in `:tables`, so every table referenced by an explicit column stays a valid source.
- **`GROUP`'s grouped columns**: no join-key is ever added — a table is a source here *only* if the user
  explicitly grouped by that table's `id`. `group: name` alone therefore resolves to **zero** sources
  (nothing joinable, no join hints should appear); `group: id, name` resolves to one; `group: t.id, c.id`
  can resolve to more than one — the same multi-table join support a plain, variable-free pipeline already
  has when it references more than one table.

With that settled, the three passes propagate joins for whichever source tables `get-source-tables`
returned, inside `pre-handle` (`ast/main.clj`):

**Pass 1 — `seed-variable-references`**: For each variable V, for each of its source tables S, copies S's
FK reference entry into the references map under V's name. This enables `V | table` and `table | V` when
the join helper can find `table[:referred-by][V]` — but only if that entry already exists, which it
doesn't yet after pass 1 alone. A variable with zero source tables (see above) has nothing to seed and
stays unjoinable to anything, correctly.

**Pass 2 — `patch-variable-relations`** (runs `patch-direction` once per direction): Builds a reverse
index from the references map — `source-table -> entities that already relate to it` — once per direction,
instead of scanning every entity per variable. For each variable V, for each source S, looks up S in that
index to find every entity T where `T[:referred-by][S]` exists, and registers `T[:referred-by][V]` with the
same relation data. The index is kept in sync with entries added mid-pass, so a variable-of-variable (V2
wrapping V1, where V1 is itself a variable processed earlier in the same pass) still picks up V1's freshly
patched relations. This propagates the relationship bidirectionally:

- `T | V` and `V | T` (real table ↔ variable)
- `V | W` and `W | V` (variable ↔ variable)

**Pass 3 — `patch-same-source-variable-joins`**: Groups variables by source table first, so only variables
that actually share a source table are ever paired. For each ordered pair of distinct variables (V1, V2)
within a group, and sharing a common source table with an `id` column, registers a synthetic `id = id`
join at `refs[:table V1 :referred-by V2]`. This makes `V1 | V2` resolve even when the source table has no
self-referential FK. Only adds entries where no join path already exists — existing FK-based propagation
(e.g. two employee-wrapping variables joined via `reports_to`) is not overridden.

### Column hints for variables

`seed-variable-references` also overrides the column list for the variable entry in the references map:

- **Explicit columns** (`s:`, `g:`, etc.): the column list is built from the user-selected columns. The
  name used is `column-alias` if set, otherwise `column`. For a GROUP-sourced CTE this naturally includes
  the aggregate — group.clj folds it directly into `:columns`, so it's read here like any other column
  rather than appended separately. (`=> count` is optional in the *grammar*, but parser.clj defaults
  `:functions` to `["count"]` whenever it's omitted — there's no way to write a truly aggregate-less
  GROUP, so this case always has a `count` entry in practice.)
- **No explicit columns** (`*`): the source table's full column list is inherited.

### Alias disambiguation

Each table gets an alias derived from its name initials (`active_companies` → `ac`). Because `make-alias ""`
(empty input) also returns `"x"` as a fallback, the empty-expression guard in `build-query` checks the
actual table name in the aliases map rather than the alias string, to avoid false matches on a variable
named `x`.

### Duplicate column guard in CTE body

When the user selects explicit columns that include `id` (e.g. `s: id, name`), the CTE body must not
also emit the auto-id column, since `SELECT id, id` is ambiguous. `build-cte-body` detects whether
user-specified columns already cover the current table and drops the auto-id column from the CTE body
if so.
