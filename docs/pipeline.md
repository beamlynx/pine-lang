# Evaluation Pipeline

How a Pine expression goes from text to SQL results.

```
user as u | document name="passport" | s: u.email, id | l: 10
```

---

## 1. Parse  (`parser/parse`)

The expression is tokenised and parsed against the BNF grammar (`pine.bnf`).
The output is a flat vector of typed operations:

```
[ {:type :table,  :value {:table "user"  :alias "u"}}
  {:type :table,  :value {:table "document" ...}}
  {:type :select, :value [{:column "email" :alias "u"} {:column "id"}]}
  {:type :limit,  :value 10} ]
```

Each operation maps 1-to-1 with a pipe segment. Assignment (`|= name`) is
extracted separately and returned as `:assign` alongside the operation list.

---

## 2. Generate  (`ast/generate`)

The parse result is fed through `ast/generate`, which:

1. **`pre-handle`** — loads DB schema via `db/init-references` (cached per
   connection). If variables are in scope, their FK references are merged in
   via `seed-variable-references` so join resolution works.

2. **`handle-ops`** — folds each operation over an accumulator state, calling
   the appropriate handler:

   | Operation | Handler | What it adds to state |
   |---|---|---|
   | `:table` | `ast/table` | `tables`, `aliases`, `joins`, `context` |
   | `:select` | `ast/select` | `columns` |
   | `:limit` | `ast/limit` | `limit` |
   | `:where` | `ast/where` | `where` |
   | `:group` | `ast/group` | `columns`, `group` |
   | `:order` | `ast/order` | `order` |
   | `:count` | `ast/count` | `operation` |
   | `:delete-action` | `ast/delete-action` | `operation` |
   | `:update-action` | `ast/update-action` | `update` |

3. **`post-handle`** — runs after all operations:
   - Generates column and table hints (`ast/hints`)
   - Appends hidden auto-id columns for row tracking (`ast/select/add-auto-id-columns`)
   - Attaches prettified expression and cursor ranges

The result is a **state map**:

```clojure
{ :tables   [ {:table "user" :alias "u"} {:table "document" :alias "d_1"} ]
  :aliases  { "u" {:table "user"} "d_1" {:table "document"} }
  :context  "d_1"
  :current  "d_1"
  :columns  [ {:alias "u" :column "email"} {:alias "d_1" :column "id"} ]
  :limit    10
  :joins    [ ... ]
  :where    []
  :hints    { :table [...] :select [...] }
  ... }
```

---

## 3. Eval  (`eval/build-query` and `eval/run-query`)

The state map is passed to the eval layer, which has two independent paths:

### Build only  (`eval/build-query`)

Produces `{:query "SELECT ..." :params [...]}` without hitting the database.
Used by the `/api/v1/build` endpoint to return the SQL and AST to the UI.

The query builder dispatches on operation type:

| Operation type | Builder |
|---|---|
| default (select) | `build-select-query` |
| `:count` | `build-count-query` |
| `:group` | `build-group-query` |
| `:update-action` | `build-update-queries` |
| `:delete-action` | `build-delete-query` |

For expressions with variables in scope, `build-select-query` prepends CTE
clauses via `collect-ctes` before the main `SELECT`.

### Run  (`eval/run-query`)

Calls `build-query` internally, then executes the SQL against the database via
`db/run-query`. Used by the `/api/v1/eval` endpoint.

---

## Full flow (single expression)

```
Pine text
    │
    ▼
parser/parse          ── BNF grammar → [ ops... ]
    │
    ▼
ast/generate
    ├── db/init-references   ── loads schema from DB (cached)
    ├── handle-ops           ── folds ops → state map
    └── post-handle          ── hints, auto-id columns, prettify
    │
    ▼
state map  { tables, aliases, columns, joins, where, limit, ... }
    │
    ├──► eval/build-query    ── state → { :query "..." :params [...] }
    │
    └──► eval/run-query      ── build-query + db/run-query → rows
```

---

## Multi-expression flow (variables)

When multiple expressions are separated by blank lines, the API evaluates them
left-to-right. Each assigned expression (`|= name`) has its state stored in a
`variables` map. Subsequent expressions receive that map and treat each entry
as a CTE.

```
expr 1: company | where: active = true |= active_co
    │
    ▼  ast/generate  →  state-1  (stored as variables["active_co"])
    │
expr 2: active_co | employee
    │
    ▼  ast/generate (variables = {"active_co": state-1})
       └── pre-handle: seeds active_co FK refs from company's schema
       └── handle-ops: resolves join active_co → employee
    │
    ▼  eval/build-query
       └── collect-ctes: WITH "active_co" AS ( SELECT ... FROM company )
       └── main SELECT with JOIN
```
