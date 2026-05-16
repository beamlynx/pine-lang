# Expressions

A unit of Pine input — one pipe chain — and how multiple expressions compose.

## Why

Pine is a REPL-style tool. A session naturally builds up state: filter a set, name it, join through it,
aggregate. A single pipe chain can do a lot, but some queries need intermediate steps that are cleaner as
separate named expressions than as deeply nested subqueries. Multiple expressions, evaluated in order,
give you that incremental composition while keeping each step readable on its own.

## Syntax

A single expression is one pipe chain:

```
company | where: active = true | employee | s: name
```

Multiple expressions are separated by blank lines:

```
company | where: active = true |= active_companies

active_companies | employee | s: name
```

The API receives them as an array of strings — one string per expression, blank lines already stripped.

## Operations

Each pipe segment maps to a typed operation. Operations are applied left to right to build the query state:

| Pipe segment     | What it does                                                                |
|------------------|-----------------------------------------------------------------------------|
| `table`          | Registers the table, resolves FK joins, tracks current context              |
| `select` / `s:`  | Records which columns to include                                            |
| `where` / `w:`   | Records filter conditions and parameters                                    |
| `group` / `g:`   | Records group-by columns and aggregate functions                            |
| `order` / `o:`   | Records sort columns and direction                                          |
| `limit` / `l:`   | Records the row limit                                                       |
| `count:`         | Switches operation mode to COUNT                                            |
| `update!` / `u!` | Records column assignments for UPDATE                                       |
| `delete!`        | Marks the operation as DELETE                                               |
| `= name`         | Snapshots the current state into `:pending-assignments`; pipeline continues |

See [pipeline.md](pipeline.md) for how these operations are processed internally.

## How evaluation works

Expressions are evaluated left to right. Only the **last expression** produces output (query or result
rows). Earlier expressions exist solely to define variables that the last expression can use.

```
expr₁  →  variables₁
expr₂(variables₁)  →  variables₁₂
expr₃(variables₁₂)  →  SQL / rows   ← this is what's returned
```

Each expression passes through the full parse → generate → eval pipeline independently. Variables
accumulate: every `|= name` op in an expression adds a named snapshot to the variable map, which is
merged into the map passed to the next expression.

If any expression fails — parse error or generate error — evaluation stops immediately and the error is
returned. The last expression is never run.

## Constraints

- Only the last expression's SQL is built or executed. Earlier expressions are only evaluated to collect
  variables — they do not produce query output.
- A variable used but not defined in a preceding expression causes a table-not-found error.
- Expressions are stateless per request. No state carries over between API calls.
- Circular variable references are not supported.

---

## Implementation

### API entry points (`api.clj`)

`api-build` and `api-eval` both receive an `expressions` array. Both split it the same way:

- **context expressions** — all but the last, passed to `evaluate-expressions`
- **last expression** — processed separately with the accumulated variable map

`api-build` returns the query string and AST (used by the UI to show hints and the SQL preview).
`api-eval` runs the query and returns rows.

### Variable threading (`evaluate-expressions`, `api.clj`)

```clojure
(reduce (fn [{:keys [variables]} expression]
          (let [{:keys [result error]} (generate-state expression nil connection-id variables)]
            (if error
              (reduced {:error error})          ;; short-circuit on first error
              {:variables (merge variables (:pending-assignments result))
               :last-state result})))
        {:variables {} :last-state nil}
        expressions)
```

Each expression's `:pending-assignments` map (keyed by variable name, valued by state snapshots from
`|=` ops) is merged into the running `variables` map. `reduced` short-circuits the reduce on error so
later expressions are never evaluated.

### Per-expression pipeline

Each call to `generate-state` runs:

1. `parser/parse` — tokenises the expression into a typed operation list.
2. `ast/generate` — folds operations into a state map, seeding the DB schema and any in-scope variables
   at the start. `|=` ops produce `:pending-assignments` entries in the returned state.
3. The caller merges `:pending-assignments` into `variables` for the next expression.

The last expression goes through the same `generate-state` call and then into `build-query` or
`run-query` depending on the endpoint.

### Hints for the last expression

`api-build` calls `generate-state` on the last expression **twice**: once with the full expression
(for the AST and query), and once with the trimmed expression (for the `query` field in the response).
Both calls receive the same `variables` map so hints reflect variables defined by earlier expressions.
