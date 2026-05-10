# Evaluation Pipeline

A Pine expression goes through three stages before results come back: **parse**, **generate**, and **eval**.

## Example expression

```
user as u | document name="passport" | s: u.email, id | l: 10
```

---

## Stage 1 — Parse

The text is tokenised against the Pine grammar and turned into a flat list of typed operations — one per pipe segment:

```
[ table(user as u),  table(document name="passport"),  select(u.email, id),  limit(10) ]
```

This list is the input to the next stage. If the expression contains `|= name`, the assigned name is extracted separately and carried alongside the operation list.

---

## Stage 2 — Generate

Each operation is folded over a state map, left to right. Before the first operation runs, the DB schema is loaded so that join paths and column lists are available. Each operation handler adds its contribution to the state:

| Pipe segment | What it contributes |
|---|---|
| `table` | Registers the table, resolves joins, tracks current context |
| `select` / `s:` | Records which columns to include |
| `where` / `w:` | Records filter conditions |
| `group` / `g:` | Records group-by columns and aggregate functions |
| `order` / `o:` | Records sort columns and direction |
| `limit` / `l:` | Records the row limit |
| `count:` | Switches operation to COUNT mode |
| `update!` | Records assignments for an UPDATE |
| `delete!` | Marks as a DELETE operation |

After all operations are processed, the stage produces a **state map** — a single structure that fully describes the query: tables, joins, column list, filters, limit, and so on. Hints for autocomplete are also computed here, using the DB schema and the current cursor position.

---

## Stage 3 — Eval

The state map is handed to the eval layer, which has two independent paths:

**Build** — translates the state map into a SQL string and a parameter list. No database call. Used by the UI to display the query as the user types.

**Run** — builds the SQL and then executes it against the database, returning rows.

---

## Multi-expression flow (variables)

When the input contains multiple expressions separated by blank lines, they are evaluated left to right. An expression ending in `|= name` stores its state as a named variable. All subsequent expressions can use that name as a table — Pine injects it as a CTE in the generated SQL.

```
company | where: active = true |= active_co     ← defines active_co

active_co | employee                             ← uses active_co as a CTE
```

Each expression in the sequence sees all variables defined before it.

---

## Technical implementation

### Parse (`parser/parse`, `pine.bnf`)

Instaparse runs the BNF grammar over the input string. The raw parse tree is normalized into a vector of `{:type <keyword> :value <data>}` maps. `|= name` is handled separately — it is extracted as `:assign` and stripped from the operation list so downstream code is unaware of it.

### Generate (`ast/generate`, `ast/main.clj`)

`generate` orchestrates three sub-steps:

1. **`pre-handle`** — seeds the state with the DB schema via `db/init-references` (cached per connection). If variables are in scope, `seed-variable-references` merges their underlying FK references into the schema map so join resolution works through CTEs.

2. **`handle-ops`** — calls `reduce` over the operation list, dispatching each operation to its handler module (`ast/table`, `ast/select`, `ast/where`, `ast/group`, `ast/order`, `ast/limit`, `ast/count`, `ast/delete-action`, `ast/update-action`). Each handler returns a modified state. The operation index (`i`) is threaded through so later stages (hints, auto-id columns) know the order in which columns were introduced.

3. **`post-handle`** — runs after all operations:
   - `ast/hints/handle` computes autocomplete hints (column hints for `select:`, `where:`, etc.; table hints for join suggestions) using the truncated-at-cursor state so hints reflect what the user has typed so far, not the full expression.
   - `ast/select/add-auto-id-columns` appends hidden `id` columns for each real (non-variable) table that has one, used by the UI for row identity tracking.
   - `add-prettify` attaches a formatted version of the expression and per-operation character ranges for cursor-based highlighting.

The final state map shape:

```clojure
{ :tables   [ {:table "user" :alias "u" :schema nil}
              {:table "document" :alias "d_1" :schema nil} ]
  :aliases  { "u"   {:table "user"}
              "d_1" {:table "document"} }
  :current  "d_1"          ; alias of the last table in the chain
  :context  "u"            ; alias of the table before the last
  :columns  [ {:alias "u"   :column "email"}
              {:alias "d_1" :column "id"} ]
  :joins    [ ["u" "d_1" <relation> nil] ]
  :where    []
  :limit    10
  :operation {:type :select}
  :hints    {:table [...] :select [...] :where [...]}
  :prettified "user as u\n | document name=\"passport\"\n | s: u.email, id\n | l: 10"
  :assign   nil }
```

### Eval (`eval/build-query`, `eval/run-query`)

`build-query` dispatches on `(-> state :operation :type)`:

| Type | Builder |
|---|---|
| `:select` (default) | `build-select-query` |
| `:count` | `build-count-query` |
| `:group` | `build-group-query` |
| `:update-action` / `:update-partial` | `build-update-queries` |
| `:delete-action` | `build-delete-query` |

`build-select-query` checks whether the state has variables in scope and, if so, prepends CTE clauses produced by `collect-ctes`. Each CTE body is built by `build-cte-body` using `build-bare-select` (no LIMIT). WHERE params from inside CTEs are collected and merged with the outer query's params.

`run-query` calls `build-query` then hands the result to `db/run-query`, which executes it against the connection pool and returns rows as a vector of maps.

### Truncated-state and cursor hints

To generate accurate autocomplete hints, `generate` builds a second, truncated version of the state by re-parsing the expression cut off at the cursor position. This truncated state is passed to `hints/handle` so the hint context reflects what the user has typed so far — not the full completed expression.

### Empty-expression guard

`build-query` checks whether the current table name (looked up from the aliases map) is an empty string. This handles the case where the input is blank — no parse output, no table — and returns `{:query "" :params nil}` rather than attempting to generate a SELECT with a missing table.
