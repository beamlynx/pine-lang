# Checkpoints

Automatically seal a GROUP or LIMIT result into a CTE (a named subquery in a `WITH` clause) so subsequent operations compose on top of it instead of corrupting it.

## Why

GROUP and LIMIT produce a final, bounded result set — further joins, or further filtering/ordering/selecting, appended after them produce malformed SQL if just tacked onto the same query (a GROUP BY inside a subquery that is then joined, a LIMIT applied before the join filter, or — for select:/where:/order: — the query builder falling back to treating the pipeline as an ordinary ungrouped select, since it decides how to build the SQL from the *last* operation's type, not from whether a GROUP happened earlier). Checkpoints solve this by detecting when such an operation follows a GROUP or LIMIT and implicitly wrapping the preceding query in a CTE, then continuing on top of that CTE.

Without checkpoints, `company | l: 10 | employee` would attempt to build a single query with LIMIT and a JOIN, which contradicts the intent (get 10 companies, then navigate to their employees). Similarly, `company | group: name | o: name desc` would resolve ORDER BY against the pre-group table instead of the grouped result.

## Terminology

"Checkpoint" and "seal" name two different things, not two names for one thing:

- A **checkpoint** is the *pending state* — the noun. After a GROUP or LIMIT, the pipeline has a checkpoint pending (`:pending-checkpoint`) until something resolves it.
- **Sealing** is the *action* that resolves a pending checkpoint — the verb. `seal-as-cte` is the one function that does it: snapshot the state, seed its references, and inject it as a CTE.

So a checkpoint is *pending*, and gets *sealed* into a CTE — the same relationship as a transaction being pending and then committed. The doc and public feature name is "checkpoints"; "seal" only ever refers to the specific act of materializing one into a CTE.

## Syntax

No syntax change is required. Checkpoints fire automatically when a TABLE operation, or a select:/where:/order: (complete or partial), follows a GROUP or LIMIT:

```
<expression> | (group:|limit:) ... | <table> [| more-ops...]
```

Optionally give the CTE your own name by placing `|= name` between the checkpoint op and the table:

```
<expression> | (group:|limit:) ... |= <name> | <table> [| more-ops...]
```

## Examples

### LIMIT checkpoint (auto-named)

```
x.company | l: 10 | employee
```

Pine seals `x.company | l: 10` as a CTE named `result_1` and joins `employee` to it:

```sql
WITH "result_1" AS (
  SELECT "c_0".* FROM "x"."company" AS "c_0" LIMIT 10
)
SELECT "e_1"."id" AS "__e_1__id", "e_1".*
FROM "result_1" AS "result_1"
JOIN "employee" AS "e_1" ON "result_1"."id" = "e_1"."company_id"
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
SELECT "e_1"."id" AS "__e_1__id", "e_1".*
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
WITH "result_1" AS (
  SELECT "c_0"."id", COUNT(1) AS "count"
  FROM "x"."company" AS "c_0"
  GROUP BY "c_0"."id"
)
SELECT "e_1"."id" AS "__e_1__id", "e_1".*
FROM "result_1" AS "result_1"
JOIN "employee" AS "e_1" ON "result_1"."id" = "e_1"."company_id"
LIMIT 250
```

### ORDER checkpoint

```
company as c | employee .company_id | group: c.name | o: name desc
```

`o:` is not a table op, but it still fires the pending GROUP checkpoint — otherwise `build-query` would dispatch on `:order` (the last op) instead of `:group`, and silently build an ungrouped query with `ORDER BY` resolved against the stale pre-group table alias. Sealing first makes this identical to the sealed cross-expression form (`group: c.name |= x` in one expression, `x | o: name desc` in the next):

```sql
WITH "result_1" AS (
  SELECT "c"."name", COUNT(1) AS "count"
  FROM "company" AS "c"
  JOIN "employee" AS "e_1" ON "c"."id" = "e_1"."company_id"
  GROUP BY "c"."name"
)
SELECT "result_1".* FROM "result_1" AS "result_1" ORDER BY "result_1"."name" DESC LIMIT 250
```

The same applies to `select:`/`where:`, and to their `-partial` forms (e.g. `o: name desc,` with a dangling trailing comma) — a partial with a value already carries whatever was fully typed before the comma, so it seals exactly like the complete form.

### No checkpoint when an action op follows

```
company | limit: 100 | count:
```

`count:` (like `delete!`/`update!`) builds its own wrapper query generically regardless of what came before it, so it doesn't need the checkpoint sealed first — no checkpoint fires, and the pipeline remains a single query with COUNT wrapping it normally:

```sql
WITH x AS ( SELECT "c_0".* FROM "company" AS "c_0" LIMIT 100 )
SELECT COUNT(*) FROM x
```

## How it works

- **Detection**: after processing a GROUP or LIMIT op, `handle-ops` sets `:pending-checkpoint {:needs-assign true}` on the state.
- **Fire on table, checkpoint, or checkpoint-consuming op**: at the start of each `handle-ops` iteration, `flush-checkpoint` checks whether a checkpoint is pending and whether the incoming op is a TABLE op, another checkpoint op (GROUP or LIMIT), or a checkpoint-*consuming* op (`select`/`select-partial`/`where`/`where-partial`/`order`/`order-partial`). If so, the current state is snapshotted into `:pending-assignments` under an auto-generated name (`result_1`, `result_2`, ..., see [Auto-generated names](#auto-generated-names)), the references map is seeded for that name (same FK propagation used by variables), and the state is reset. The CTE name is then injected as the first table so subsequent ops compose on top of it. A LIMIT following a GROUP therefore fires the GROUP checkpoint, then applies the limit to the outer query.
- **User-named CTE**: if an `|= name` op appears between the checkpoint op and the following op, `flush-checkpoint` records the name and waits. `assign/handle` stores the snapshot under `name` as normal. When the next firing op arrives, `seal-as-cte` activates it.
- **Hold for non-triggering ops**: if the op after GROUP/LIMIT is something that has its own query-building path — `count:`, `delete!`, `update!` — the checkpoint stays pending and that op is processed without firing the checkpoint.
- **SQL generation**: because the CTE table has an `:ast` entry in `:aliases`, `collect-ctes` automatically picks it up and emits the `WITH` clause. No changes to `eval.clj` were needed.

## Constraints

- Checkpoint op types (what *creates* a pending checkpoint) are GROUP and LIMIT. LIMIT includes `limit: N per <columns>` ([limit-per.md](limit-per.md)) and its partial form, so steps after it work on the rows kept per group.
- Checkpoint-consuming op types (what *fires*/seals an already-pending checkpoint, alongside TABLE) are `select`/`select-partial`/`where`/`where-partial`/`order`/`order-partial`. `count:`/`delete!`/`update!` deliberately do not fire — see above.
- Only one table-level composition step is supported per checkpoint; chaining `l: 10 | employee | document` creates one CTE for `l: 10` and then navigates normally through employee to document.
- Checkpoint CTEs do not receive Pine's auto-id columns; those are suppressed for all CTE-backed tables.

## What /build reports

`/build`'s `ast.checkpoints` lists the checkpoints of the expression in pipe order. beamlynx-ui draws each one as a divider with a node named after it. Once a checkpoint is sealed, nothing else in the AST says what it was, so the list carries that.

```
company as c | employee .company_id | o: created_at desc | l: 1 per c.id | where: id = 1
```

```json
[{"name": "result_1",
  "sealed": true,
  "index": 3,
  "kind": "limit",
  "limit": 1,
  "per": [{"alias": "c", "column": "id"}],
  "tables": ["c", "e_1"],
  "columns": ["id", "company_id", "reports_to"]}]
```

| Field | What it is |
|---|---|
| `name` | The CTE's name: the `|= name`, or the auto name (`result_1`, ...). |
| `sealed` | `false` only for a checkpoint still pending at the end of the expression. Its `name` is the one sealing it will give it. |
| `index` | The operation index of its `group:` or `limit:` step, the same index as in `ast.ranges`. |
| `kind` | `"limit"` or `"group"`. |
| `limit`, `per` | For a limit: N, and the columns after `per` if any. Each column has its `alias`, `column`, and a JSON `path` if it has one. |
| `group`, `aggregates` | For a group: the grouped columns, and the aggregates' names, such as `["count"]`. |
| `tables` | The aliases of the tables it seals, in pipe order. A later checkpoint lists the earlier one's name here. |
| `columns` | The names of the columns its result has: what it selects, or every column of its current table. After `group: t.title`, that is `title` and `count`. |

A trailing checkpoint is listed even when nothing follows it, and when only `count:`, `delete!` or `update!` follows it. Those keep it pending.

To draw what a sealed checkpoint seals, look up its `name` in `ast.pending-assignments`. Each entry there has the stage's `tables`, `joins`, `columns`, `where`, `order` and `group`. The tables after the last checkpoint are in the top-level AST, as before.

## Auto-generated names

A checkpoint sealed without `|= name` is named `result_1`, `result_2`, and so on. The canvas shows this name on the node. Because it is a normal name, a later expression can use it like any `|=` name, for example `result_1 | s: name`.

Pine picks the smallest number whose name is free. A name is not free if it is:

- a named result from this or an earlier expression, including earlier auto-names;
- a table alias or `|= name` anywhere in this expression, even after the checkpoint;
- the name of a real table in the connected database, in any schema.

Each rule prevents a different clash:

- Two CTEs with the same name would make the SQL generator keep only one of them.
- The CTE is also its own alias (`FROM "result_1" AS "result_1"`), so it can't share a name with another alias.
- A CTE hides a real table of the same name, so an unqualified read of that table would silently read the CTE instead.

Names are compared without regard to case. The same expression, earlier named results and schema always give the same names.

For example, `x.company | l: 10 | employee as result_1` names the checkpoint `result_2`, because `result_1` is already an alias.

See also: [named-results.md](named-results.md) for cross-expression named CTEs, and [limit-per.md](limit-per.md) for top N per group.

---

## Implementation

### State fields (`ast/main.clj`)

| Field | Default | Purpose |
|---|---|---|
| `:reserved-cte-names` | `nil` | Every table alias and `|=` name in the expression, collected by `handle-ops` before any op runs. `next-auto-cte-name` skips these. |
| `:pending-checkpoint` | `nil` | `{:needs-assign true :index i}` after a checkpoint op; `{:name n :needs-table true :index i}` after an explicit `|=`. `:index` is the checkpoint op's operation index. |
| `:checkpoints` | `[]` | One entry per checkpoint, for `ast.checkpoints`. `reset-for-cte` keeps it. |

### Core functions (`ast/main.clj`)

- `checkpoint-op-types` — set `#{:group :limit :limit-partial}`; ops that *create* a pending checkpoint
- `checkpoint-consuming-op-types` — set `#{:select :select-partial :where :where-partial :order :order-partial}`; ops that *fire* (seal) an already-pending checkpoint, alongside a TABLE op or another checkpoint op. Partial and complete forms are treated identically — a partial with a value has already fully typed whatever precedes a dangling trailing comma.
- `reset-for-cte` — clears all query-building fields (tables, columns, joins, where, order, group, limit) while preserving references, variables, and pending-assignments
- `seal-as-cte` — the single shared action: stores the snapshot under `cname` in `:pending-assignments`, seeds FK references for `cname` (same three passes used by cross-expression variables in `pre-handle`), resets query-building state, then injects `cname` as the first table via `handle-op`; `table/handle` resolves it via `:pending-assignments`
- `checkpoint-entry` — builds one `ast.checkpoints` entry from a checkpoint's snapshot. `seal-as-cte` adds one for every sealed checkpoint. `add-pending-checkpoint`, run by `generate` after `handle-ops`, adds the trailing one. Both run before `post-handle` drops `:references`, which `result-columns` needs to list a `*` result's columns.
- `next-auto-cte-name` — picks `result_N` for an auto-named checkpoint (see [Auto-generated names](#auto-generated-names))
- `flush-checkpoint` — state-machine dispatch called at the start of each `handle-ops` reduce iteration; calls `seal-as-cte` when a TABLE op, another checkpoint op, or a checkpoint-consuming op follows the pending checkpoint, using either a freshly-snapshotted state (auto-named) or the snapshot already stored by `assign/handle` (user-named)

### `table/handle` (`ast/table.clj`)

Updated to check `:pending-assignments` alongside `:variables` when looking up the CTE for a table name:

```clojure
var-ast (or (get-in state [:variables table])
            (get-in state [:pending-assignments table]))
```

### `add-auto-id-columns` (`ast/select.clj`)

Changed the predicate from checking `variables` membership to checking `(:ast (get aliases %))`. Any alias that carries an `:ast` entry is a CTE-backed table and should not receive an auto-id column.
