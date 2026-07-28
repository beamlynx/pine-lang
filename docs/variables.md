# Variables

Name and reuse intermediate query results across expressions.

## Why

Complex queries often need to build up results in steps — filter a set of rows, then join that filtered
set to another table, then aggregate. Without variables, the only way to do this is with nested subqueries,
which are hard to read and hard to iterate on in a REPL-style tool. Variables let you give each step a
name, treat it as a table, and build on it later.

## Syntax

```
<expression> |= <name> [| more-ops...]
```

Place `|= name` anywhere in a pipe chain to save the query state at that point under `name`. The pipeline
keeps going after the assignment — anything after `|=` keeps refining the current expression's output, but
the saved snapshot doesn't change. Use `name` as a table in any later expression.

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

`all_companies` is the full unfiltered company snapshot — it was saved before `where:` ran.
The current expression still returns only active companies.

### Chained

```
company | where: active = true |= active_companies

active_companies | l: 10 |= small_active

small_active
```

Expressions are separated by blank lines and run in order, each one able to build on the last. This
produces two CTEs and selects from the final one.

### Grouped

```
tenant as t | public.company .tenantId | g: t.title |= x

x
```

`x` exposes only the columns the CTE actually produces — here `title` and `count` — so hints for
`x | s:` show just those two columns.

Neither `tenant` nor `company` survives as a join source, though: grouping by `t.title` doesn't keep
`tenant`'s `id`, so `x | company` (or any other table) shows no join hints at all — see
[Join resolution through variables](#join-resolution-through-variables) below. Grouping by
`t.id, t.title` instead would keep `tenant` joinable.

### Same-source joins (not a general self-join)

Pine does not support self-joins on raw tables — `customer | customer` does not resolve to a usable join,
since there's no FK from a table to itself, no way yet to tell the two occurrences apart (no `t | t as t2`
self-aliasing), and so no fallback for that case. A **variable** wrapping the same source table as the
other side is a narrow, deliberate exception to that, since a variable is already a distinct, named
snapshot — unambiguous in a way two bare references to the same table aren't:

```
customer |= x

customer |= y

x | y
```

```sql
WITH "x" AS ( SELECT "c_0".* FROM "customer" AS "c_0" ),
     "y" AS ( SELECT "c_0".* FROM "customer" AS "c_0" )
SELECT "y".* FROM "x" AS "x" JOIN "y" AS "y" ON "x"."id" = "y"."id" LIMIT 250
```

This isn't limited to two variables, either — a real table joined to a variable that traces back to that
same table works exactly the same way, since it's the same underlying situation (one side just isn't
sealed into a CTE):

```
customer | s: id as c_id |= x

customer | x
```

```sql
WITH "x" AS ( SELECT "c_0"."id" AS "c_id" FROM "customer" AS "c_0" )
SELECT "c_0".id AS "__c_0__id", "x".* FROM "customer" AS "c_0" JOIN "x" AS "x" ON "c_0"."id" = "x"."c_id" LIMIT 250
```

`x | x` (or `customer | customer` with no variable on *either* side) is **not** the same thing and will not
resolve — at least one side must actually be a variable, and it can't be a variable joined to itself.

This is a consequence of how variable join resolution works (see
[Join resolution through variables](#join-resolution-through-variables) below), not general self-join
support — Pine may add that separately in the future, most likely via an explicit way to alias a second
occurrence of the same real table (`t | t as t2`), at which point this fallback would just be one case of
that more general mechanism rather than a separate one.

## How it works

- **Assignment** (`|= name`): saves the pipeline state at that point. The snapshot becomes the CTE body
  when `name` is used in a later expression. Operations after `|=` continue refining the current
  expression's output — they do not affect the snapshot.
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
- A variable cannot be joined to itself (`x | x`), matching Pine's general lack of self-join support
  on raw tables. Two [distinct variables wrapping the same source table](#same-source-variables-not-a-general-self-join)
  (`t |= x`, `t |= y`, then `x | y`) is the one narrow exception.

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
   `assign/handle` saves the current state into `:pending-assignments` under the variable name.
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

**A note on the letters used below** — they show up a lot in this section, so here's what each one means:

- **S** — the real, physical table a variable is built from (its "source table").
- **X** and **Y** — two variable names, matching the `x`/`y` used in the examples above.
- **T** — some *other* table already known to Pine. Could be a real table, or could itself be another
  variable. It's whatever might already have a relationship to `S`.
- **`refs[:table A :referred-by B]`** — this is literal Clojure, not just notation for this doc. It's how
  the code reads "the references map's entry for `A`, recording that `B` refers to it." `:refers-to` is
  the mirror: "`A`'s own entry recording that it refers to `B`."

The references map itself only ever describes **real** tables — it's built once, at connection time,
straight from the DB schema (`db/postgres.clj`). A variable is never faked into it as a pretend table.
Instead, join resolution figures out, live, which real table a variable actually stands in for, and reads
the real map directly. Two pieces make that work:

**`:source` (`ast/select.clj`)**: every column a variable exposes carries `:source` — the real table it
ultimately traces back to. Computed once, when the column is created, by a single rule with no walk or
recursion: selecting from a real table → `:source` is that table; selecting from a variable → `:source` is
copied forward from *that* variable's own already-resolved `:source` for the matching column. Since a
variable must be defined before it can be referenced, whatever it's built from has already gone through
this same step — so by the time `Y` is built on top of `X`, `X`'s columns are already fully resolved to a
real table, and `Y` just copies that value rather than re-deriving it. A variable with no explicit columns
at all (`*`) has nothing to tag per-column, so `resolve-table` (below) applies the same rule one level up
instead, at the whole variable's own `:current` table.

A table only counts as a source if its own `id` column is explicitly among the variable's output columns —
Pine never adds one on its own. **Why a plain table join doesn't have this restriction, but a variable
does**: `t | c` (no variable involved) compiles to `... JOIN "c" ON "t"."id" = "c"."tenantId"` — a real
`JOIN ... ON` clause can reference any column of `c`, selected or not, since the whole table still exists in
the database. A variable removes that safety net: once its query is sealed into `WITH "x" AS ( SELECT ... )`,
the outer query can't see `x`'s original table at all — only the exact columns `SELECT` produced. That's why
a table only stays joinable *through* a variable if its `id` was explicitly selected — nothing else survives
the CTE boundary.

- **No explicit columns** (`*`): the variable implicitly selects everything, `id` included — its `:current`
  table is the only source.
- **Explicit columns** (`s:`, or `group:`'s grouped columns): a table is a source only if `id` is literally
  among the selected columns. `s: name` alone gives **zero** sources; `s: id, name` gives one; `s: t.id,
  c.id` can give more than one, each with its own independent `:source` — the same multi-table join support
  a plain, variable-free pipeline already has when it references more than one table.

**`resolve-table` (`ast/table.clj`)**: given an alias — real or variable — returns the real source table(s)
it resolves to for a join, each paired with a `:rename` map (`{raw-column -> exposed-column}`) so a column
found via the real table's schema can be translated back to whatever name the variable actually exposes it
under. A real table resolves to itself, with an empty rename. This is the *only* place a variable's own
name ever gets swapped out for a real table name — `join-helper` and hints never see a variable's name as a
schema-lookup key at all.

- **`X | T` and `T | X`** (real table on either side of a variable): `join-tables` resolves both sides
  through `resolve-table` before doing the schema lookup, so it's really `S | T`/`T | S` under the hood —
  which the references map already answers both directions for, exactly like it does for two real tables.
  No pre-seeding needed; whichever direction was typed just resolves against the real schema live.
- **`X | Y`, and just as much `T | X`/`X | T`** when `T` shares `X`'s source with no real FK connecting
  them (e.g. two variables both wrapping `customer`, or `customer` itself joined to a variable wrapping
  it): falls through to a small fallback inside `join-tables` — if both sides resolve to the *same* real
  source with an `id` column, and no real relationship connects them, synthesize an `id = id` join, each
  side using its own exposed name for it. Requires at least one side to actually be a variable — two raw
  references to the same table still don't resolve, since there's no way yet to tell which occurrence is
  which — and never fires for a variable joined to itself. See
  [Same-source joins](#same-source-joins-not-a-general-self-join) above. The corresponding hint is tagged
  `:resolution "synthetic"` — see [joins.md](joins.md#hint-facing-resolution-asthintsclj) for the full set
  of resolution values a hint can carry.
- **Hints** (`ast/hints.clj`): `relation-hints` resolves the context the same way, then also has to
  enumerate *other* variables as candidates — something a real table's own schema entry can't express,
  since "some variable also happens to wrap this table" isn't a fact about the table. It checks every
  variable currently in scope for whether it resolves (via `resolve-table`) to a relevant real table, either
  as an ordinary join target (`T`) or via the same-source fallback (`Y`). When the *context* itself is a
  restricted variable, the source table it resolves to is suggested back too (e.g. `aggregate | tenant`,
  the mirror image of `tenant | aggregate`) — the same fallback, just the other direction.

### Column hints for variables

`generate-all-column-hints` (`ast/hints.clj`) shows a variable's own exposed columns:

- **Explicit columns** (`s:`, `g:`, etc.): the columns the user actually selected — using the alias if one
  was given, otherwise the plain column name. A `GROUP`-built variable already includes its aggregate
  column here too; `group.clj` folds it directly in, so it's treated like any other column rather than
  added separately. (`=> count` is optional to type, but the parser fills in `"count"` whenever it's left
  out — there's no way to write a GROUP with no aggregate at all, so this case is always present in
  practice.)
- **No explicit columns** (`*`): resolved the same one-hop way as everything else above, straight to the
  real underlying table's own full column list (names *and* types) — which is also what
  `data_types.clj`'s `get-column-type` relies on for coercing `where:`/`update!` literal values through a
  variable (e.g. `x | where: tmp_id = 5`).

### Alias disambiguation

Each table gets a short alias from its name's initials (`active_companies` → `ac`). That same naming
scheme happens to return `"x"` as a fallback for empty input too — so the code that guards against an
empty first table checks the *actual table name*, not the alias string, to avoid a false match whenever a
variable happens to be named `x`.

### Duplicate column guard in CTE body

If the user selects `id` explicitly (`s: id, name`), the CTE body must not *also* emit Pine's own hidden
id-tracking column — `SELECT id, id` isn't valid SQL. `build-cte-body` checks whether the user's own
selected columns already cover it, and skips adding the hidden one if so.
