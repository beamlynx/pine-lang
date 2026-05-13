# Evaluation Pipeline

How a Pine expression goes from text to SQL results.

## Why

Pine is a pipe-based DSL that compiles to SQL. Understanding the pipeline helps when debugging unexpected
query output, tracing where hints come from, or extending the language with new operations.

## Overview

Every expression passes through three stages:

```
text  →  parse  →  generate  →  eval
```

**Parse** turns the text into a list of typed operations.
**Generate** folds those operations into a state map, using the DB schema to resolve joins and compute hints.
**Eval** translates the state map into SQL, and optionally runs it against the database.

## Stages

### 1. Parse

The expression is tokenised against the Pine grammar and turned into a flat list of typed operations —
one per pipe segment.

Input:
```
user as u | document name="passport" | s: u.email, id | l: 10
```

Output:
```
[ table(user as u),  table(document name="passport"),  select(u.email, id),  limit(10) ]
```

Each operation has a type and a value. If the expression ends with `|= name`, that name is extracted
separately and carried alongside the list.

### 2. Generate

Each operation is applied left to right to a state map, using the appropriate handler. Before the first
operation runs, the DB schema is loaded so that join paths and column lists are available.

| Pipe segment | What it adds to state |
|---|---|
| `table` | Registers the table, resolves FK joins, tracks current context |
| `select` / `s:` | Records which columns to include |
| `where` / `w:` | Records filter conditions and parameters |
| `group` / `g:` | Records group-by columns and aggregate functions |
| `order` / `o:` | Records sort columns and direction |
| `limit` / `l:` | Records the row limit |
| `count:` | Switches operation mode to COUNT |
| `update!` / `u!` | Records column assignments for UPDATE |
| `delete!` | Marks the operation as DELETE |

After all operations are applied, a **post-processing** step runs: autocomplete hints are computed,
hidden auto-id columns are appended for row tracking, and a prettified form of the expression is attached.

The result is a single **state map** that fully describes the query.

### 3. Eval

The state map is handed to the eval layer, which has two paths:

**Build** — translates the state into a SQL string and parameter list. No database call. Used to show
the query in the UI as the user types.

**Run** — builds the SQL then executes it against the database, returning rows.

## How it works

- The pipeline is stateless per request — each call to the API runs the full parse → generate → eval
  cycle from scratch.
- Hints are computed inside `generate`, using a second pass over the expression truncated at the cursor
  position. This gives hints that reflect what the user has typed so far, not the full completed expression.
- When multiple expressions are present (separated by blank lines), they are evaluated left to right.
  Each assigned expression (`|= name`) stores its state as a variable, threaded into subsequent calls.
  See [variables.md](variables.md).

## Constraints

- The pipeline is synchronous and single-pass — there is no incremental or lazy evaluation.
- All schema information is resolved at generate time. A table or column that doesn't exist in the DB
  schema will fail at that stage, not at SQL execution time.

---

## Implementation

### Parse (`parser/parse`, `pine.bnf`)

Instaparse runs the BNF grammar over the input string. The raw parse tree is normalised into a vector of
`{:type <keyword> :value <data>}` maps. `|= name` is extracted as `:assign` and stripped from the
operation list so the rest of the pipeline is unaware of it.

### Generate (`ast/generate`, `ast/main.clj`)

`generate` orchestrates three sub-steps:

**`pre-handle`** — seeds the initial state with the DB schema via `db/init-references` (cached per
connection). If variables are in scope, `seed-variable-references` merges their underlying FK references
so join resolution works through CTEs.

**`handle-ops`** — calls `reduce` over the operation list, dispatching each operation to its handler
module (`ast/table`, `ast/select`, `ast/where`, `ast/group`, `ast/order`, `ast/limit`, `ast/count`,
`ast/delete-action`, `ast/update-action`). The operation index (`i`) is threaded through so later
stages know the order columns were introduced.

**`post-handle`** — runs after all operations:
- `ast/hints/handle` computes autocomplete hints using the truncated-at-cursor state.
- `ast/select/add-auto-id-columns` appends hidden `id` columns for each real table.
  See [result-updates.md](result-updates.md).
- `add-prettify` attaches a formatted expression and per-operation character ranges for cursor highlighting.

The final state map:

```clojure
{ :tables    [ {:table "user" :alias "u"}
               {:table "document" :alias "d_1"} ]
  :aliases   { "u" {:table "user"} "d_1" {:table "document"} }
  :current   "d_1"
  :context   "u"
  :columns   [ {:alias "u" :column "email"} {:alias "d_1" :column "id"} ]
  :joins     [ ["u" "d_1" <relation> nil] ]
  :where     []
  :limit     10
  :operation {:type :select}
  :hints     {:table [...] :select [...] :where [...]} }
```

### Eval (`eval/build-query`, `eval/run-query`)

`build-query` dispatches on operation type:

| Type | Builder |
|---|---|
| `:select` (default) | `build-select-query` |
| `:count` | `build-count-query` |
| `:group` | `build-group-query` |
| `:update-action` / `:update-partial` | `build-update-queries` |
| `:delete-action` | `build-delete-query` |

`build-select-query` checks for variables in scope and, if present, prepends CTE clauses via `collect-ctes`.
`run-query` calls `build-query` then executes against the connection pool via `db/run-query`.

### Empty-expression guard

`build-query` checks whether the current table name (looked up via the aliases map) is an empty string.
This handles blank input — returns `{:query "" :params nil}` rather than generating a malformed SELECT.
