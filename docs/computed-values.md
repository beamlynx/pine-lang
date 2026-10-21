# Computed values

A value that isn't just a column: `coalesce(reference_id, concat('att:', a.id))`, `id in edited`,
`if x is null then 'a' else 'b'`, `created_at => month`, or an aggregate like
`a.id => min where a.type = 'sharepoint'`.

```
company_document as d
 | s: d.id, coalesce(reference_id, concat('doc:', id)) as dedupe_key, id in edited as was_edited
 | o: was_edited desc, created_at desc
 | l: 1 per company_id, dedupe_key
```

## Why

A query often needs a value the table doesn't store as such: a fallback when a column is empty, a flag that
says whether a row is in some set, a label chosen by a condition. In SQL these are expressions, and an AI
agent's SQL is full of them. Without them, Pine couldn't say such a query, and SQL to Pine had to refuse it.

## Syntax

A computed value is one of these, and they nest freely:

| Kind | Example |
|---|---|
| A column, with an optional cast | `a.id`, `a.id::text`, `a.id ::text` |
| A literal | `'att:'`, `42`, `-1.5`, `true`, `false`, `null`, `'2024-01-01'` |
| A variable | `$prefix` |
| A function call | `coalesce(a, b)`, `concat('att:', a.id)`, `now()`, `month(created_at)` |
| A function applied with the arrow | `created_at => month`, `title => coalesce('untitled')` |
| A condition, as true or false | `type = 'sharepoint'`, `deleted_at is null`, `id in edited`, `id in (1, 2)`, `name like 'A%' or name is null` |
| A choice | `if x is null then 'none' else x` |
| Parentheses | `(e.document_id is not null)` |

A cast is `::text` or `::uuid`, as in `where:`. A cast after a call applies to its result:
`coalesce(a, b)::text`.

A condition is anything `where:` accepts, including a [named result](named-results.md) after `in` and a
`$variable`. Several conditions joined with `or` are one value.

### The arrow

`x => f` is `f(x)`, and `x => f(a, b)` is `f(x, a, b)`: the value on the left is the function's first
argument. Both forms mean the same, everywhere a value goes.

```
created_at => month                       month(created_at)
reference_id => coalesce(name, 'none')    coalesce(reference_id, name, 'none')
name => coalesce('x') => concat('!')      concat(coalesce(name, 'x'), '!')
a.id::text => min                         min(a.id::text)
```

The arrow is for exploring: you have a value, you see it needs a function, and you append `=> f` instead of
going back to wrap it. Steps chain left to right.

- A cast binds tighter than the arrow: `a.id::text => min` casts, then takes the minimum.
- A condition before an arrow goes in parentheses, `(type = 'x') => any`, or is written as a call,
  `any(type = 'x')`. Without them, `type = 'x' => any` is a parse error that says so.
- `now()` and `count` take no value, so they have no arrow form.

**Which form Pine writes.** When Pine writes Pine, a function of exactly one value uses the arrow:
`created_at => month`, `a.id => min`. Anything else uses the call: `coalesce(a, b)` keeps every argument
in order, and `any(type = 'x')` keeps a condition whole. This covers SQL to Pine, the `text` that `/build`
reports, and the examples in the docs. The formatter never rewrites what a person typed.

### select:

A computed value in `select:` must have a name, given with `as`:

```
attachment as a
 | s: a.id, coalesce(a.name, concat('att:', a.id)) as label, a.type = 'sharepoint' as on_sharepoint
```

A select with only computed and named columns returns those, not every column of the table, as with plain
columns.

### Functions

| Function | What it gives |
|---|---|
| `coalesce(a, b, ...)` | The first argument that isn't NULL. Two or more arguments. |
| `concat(a, b, ...)` | The arguments as text, one after another. NULL when any of them is NULL, like SQL's `\|\|`. Two or more arguments. |
| `now()` | The current date and time. |
| `minute(x)`, `hour(x)`, `day(x)`, `week(x)`, `month(x)`, `year(x)` | A date as text, cut to that unit: `month` gives `'2024-05'`. A week starts on Monday. |
| `jsonb_typeof(x)` | The type of a JSON value: `object`, `array`, `string`, `number`, `boolean` or `null`. NULL when there is no value, such as a missing key. |
| `jsonb_array_length(x)` | The number of items in a JSON array. See [JSON values](#json-values). |

In `select:`, a date bucket on a column needs no `as`: `s: created_at => month` is named `month`.

### JSON values

`jsonb_typeof()` and `jsonb_array_length()` read their argument as JSON. They are named after Postgres's own
functions, so SQL written for Postgres converts as it is, but they work on every database.

```
customer | s: data.tags => jsonb_array_length as tag_count
customer | s: jsonb_array_length(if data.kind = 'list' then data else data.items) as n
```

- A key inside a JSON column is its JSON value here, not its text. Elsewhere, `data.tags` is shown as text.
- An `if` passes this on to its branches, so `data` and `data.items` are both JSON.
- A string literal is JSON text: `jsonb_typeof('[1, 2]')` is `array`.
- `jsonb_array_length` of a value that isn't an array is an error on Postgres, as Postgres's own function is.
  MySQL and SQLite give NULL. Check the type first when the value may not be an array.

### if

```
if <condition> then <value> [else <value>]
```

- With no `else`, the value is NULL when the condition isn't true.
- Several branches are a chain: `if a = 1 then 'one' else if a = 2 then 'two' else 'many'`. It becomes one
  `CASE WHEN ... WHEN ... ELSE ... END`.
- An `if` right after `then` goes in parentheses: `if a then (if b then 1 else 2) else 3`. Without them, an
  `else` could belong to either `if`.

### Using a computed name later

A later step can use a computed column's name like a column:

```
company_document
 | s: id, company_id, coalesce(reference_id, concat('doc:', id)) as dedupe_key, id in edited as was_edited
 | where: was_edited = true
 | o: created_at desc
 | l: 1 per company_id, dedupe_key
```

This works in `where:`, `order:`, after `per` (see [limit-per.md](limit-per.md)), in `group:`, and inside a
later computed value, even one in the same `select:`.

- The name wins over a table column with the same name. The column is still reachable with its alias, like
  `d.status`.
- A condition's name is compared with `true` or `false`: `where: was_edited = true`.
- After a [checkpoint](checkpoints.md), the name is an ordinary column of the checkpoint's result.
- Hints offer computed names in `where:`, `order:` and after `per`.

### Aggregates in group:

`group:` lists its keys, then `with` and the aggregates. Without `with`, each group is counted:
`group: status` is `group: status with count`. An aggregate other than `count` takes a value:

```
company_document as d
 | company_document_attachment :left .company_document_id = .id
 | attachment as a :left .id = .attachment_id
 | group: d.id, d.reference_id with count, a.id::text => min where a.type = 'sharepoint' as sp_attachment,
          a.id::text => min as any_attachment, any(a.type = 'sharepoint') as on_sharepoint
```

| Aggregate | What it gives for each group |
|---|---|
| `count` | The number of rows. Written bare. |
| `x => min`, `x => max` | The smallest, or largest, value. Also `min(x)`, `max(x)`. |
| `x => sum`, `x => avg` | The total, or the average. Also `sum(x)`, `avg(x)`. |
| `any(condition)` | True when the condition is true for at least one row. |
| `all(condition)` | True when the condition is true for every row. |

Every aggregate, `count` included, can take a filter: `a.id => min where a.type = 'sharepoint'` only reads
the rows where the condition is true.

`=>` used to divide the keys from the aggregates: `group: status => count`. That form is now a parse error
that names the new one.

Without `as`, an aggregate is named after the function and the first column it reads: `min(a.id)` is
`min_id`, and `any(a.type = 'sharepoint')` is `any_type`. `count` is `count`. Two aggregates with the same
name are an error that asks for `as`.

To keep only some groups, add a `where:` after the `group:`. It filters the groups by the aggregates' names,
as SQL's `HAVING` does:

```
... | group: d.id with any(a.type = 'sharepoint') as on_sharepoint | where: on_sharepoint = true
```

An aggregate can't be used inside a function. To combine two, name them in `group:` and combine their names
in a `select:` after it: `| s: id, coalesce(sp_attachment, any_attachment) as attachment`.

### now() in where: and update!

`now()` can follow a comparison in `where:`, and be the value of an `update!`:

```
session | where: expires_at < now() | update! closed_at = now()
```

There is no date arithmetic yet, such as `now() - interval '1 day'`.

### A cast in a join

Either column of an explicit join pair can take a cast:

```
document as d | activity_resource .resourceableId = .id ::text
```

The cast belongs to the column it follows, here `d.id`: `ON "d"."id"::text = "ar_1"."resourceableId"`.

## How it works

### Inlined, not named

SQL can't use a `SELECT` list name in `WHERE`, in `GROUP BY` reliably, or inside a window's `PARTITION BY` and
`ORDER BY`. So a computed value is written out again wherever its name is used:

```
report | s: coalesce(title, 'x') as k | where: k = 'y' | o: k desc
```

```sql
SELECT COALESCE("r_0"."title", ?) AS "k" FROM "report" AS "r_0"
WHERE COALESCE("r_0"."title", ?) = ? ORDER BY COALESCE("r_0"."title", ?) DESC LIMIT 250
```

Its parameters repeat with it, each in the order its `?` appears. Grouping by a computed name in one query uses
the column's position instead, `GROUP BY 1`: Postgres doesn't treat two copies of an expression with
parameters as the same expression.

Sealing a [checkpoint](checkpoints.md) after each `select:` was the other option. It would add a CTE the
person never asked for, and `update!` refuses to change a named result, so a `select:` with a computed value
couldn't end in `update!`.

### On each database

| Pine | Postgres | MySQL | SQLite |
|---|---|---|---|
| `concat(a, b)` | `((a)::text \|\| (b)::text)` | `CONCAT(a, b)` | `(a \|\| b)` |
| `now()` | `NOW()` | `NOW()` | `CURRENT_TIMESTAMP` |
| `any(c)` | `BOOL_OR(c)` | `MAX(c)` | `MAX(c)` |
| `all(c)` | `BOOL_AND(c)` | `MIN(c)` | `MIN(c)` |
| `min(x) where c` | `MIN(x) FILTER (WHERE c)` | `MIN(CASE WHEN c THEN x END)` | `MIN(x) FILTER (WHERE c)` |
| `count where c` | `COUNT(1) FILTER (WHERE c)` | `COUNT(CASE WHEN c THEN 1 END)` | `COUNT(1) FILTER (WHERE c)` |
| a condition | `(c)`, true or false | `(c)`, 1 or 0 | `(c)`, 1 or 0 |
| `jsonb_typeof(x)` | `jsonb_typeof(x)` | `JSON_TYPE(x)`, renamed | `json_type(x)`, renamed |
| `jsonb_array_length(x)` | `jsonb_array_length(x)` | `JSON_LENGTH(x)` when `x` is an array | `json_array_length(x)` when `x` is an array |
| a key, inside these two | `jsonb_extract_path(c::jsonb, ...)` | `JSON_EXTRACT(c, ...)` | `(c -> ...)` |

- `concat` is NULL when any argument is, on every database. Postgres's and SQLite's own `concat()` skip NULLs,
  so `||` is used there. Postgres has no `||` for two numbers, so each argument is cast to text. MySQL's
  `CONCAT()` is already NULL on a NULL argument, and its `||` means OR.
- On MySQL and SQLite a condition is 1, 0 or NULL, so `MAX` and `MIN` give the same as `BOOL_OR` and
  `BOOL_AND`, NULL over only NULLs included.
- MySQL and SQLite name JSON types differently: `INTEGER`, `DOUBLE`, `true`, `text`. Pine maps them to
  Postgres's names, so `jsonb_typeof` gives the same text everywhere. SQLite's `json_extract` gives a string's
  text, not its JSON, so a key is read there with `->`.
- Numbers, `true`, `false` and `null` are written into the SQL. Strings and dates are parameters, as
  everywhere else.
- A string beside a column takes the column's type, as in `where:`. `coalesce(uuid_col, '...')` sends a uuid,
  which Postgres needs: it can't pick between a uuid and a varchar.

### Access policy

Under an access policy, a computed column is shown only when every column it reads is visible. Otherwise its
whole value is `xxxxx`, so a condition on a hidden column can't leak through a true or false result. A value
that reads no column, such as `now()`, is shown. The same applies to an aggregate and its filter.

A computed name used in `where:`, `order:`, `group:` or after `per` reads the columns inside it. If one is
hidden, the query is refused, as for the column itself.

### What /build reports

`ast.columns` describes a computed column for the canvas:

```json
{"computed": true,
 "column-alias": "dedupe_key",
 "alias": "d",
 "text": "coalesce(d.reference_id, concat('doc:', d.id))",
 "uses": [{"alias": "d", "column": "reference_id"}, {"alias": "d", "column": "id"}]}
```

| Field | What it is |
|---|---|
| `computed` | `true` for a computed column. |
| `column-alias` | Its name. |
| `alias` | The table that was current when it was selected. |
| `text` | The value written as Pine, with every column's alias. |
| `uses` | The columns it reads, each once. |
| `aggregate` | `true` for an aggregate of `group:`. |

An `order:`, `group:` or `where:` entry that uses a computed name has the same `computed`, `text` and `uses`,
next to the name in `column`. A computed value on the right of a condition, like `now()`, is a `value` with
`type: "computed"`, `text` and `uses`.

## Constraints

- A computed value in `select:` needs `as`.
- The functions are `coalesce`, `concat` and `now`; the aggregates `count`, `min`, `max`, `sum`, `avg`, `any`
  and `all`. Any other name is an error that lists them.
- An aggregate only goes after `with` in `group:`, and not inside a function there.
- `where:` takes a function only on the right of a comparison: `created_at > now()`.
- `update!` can't write a function into a key of a JSON column.
- No arithmetic (`+`, `-`), and no intervals.

---

## Implementation

| File | What it does |
|---|---|
| `src/pine/pine.bnf` | `computed-column`, `value`, `call`, `if-value`, `literal`; `aggregate-function`; `call` after a comparison and in `update!`; a cast in `explicit-pair`. |
| `src/pine/computed.clj` | The node shapes, the one table of functions, the columns a value reads, and the printer that writes a value back as Pine. No dependencies on the rest of Pine. |
| `src/pine/parser.clj` | `normalize-value` turns a parsed value into nodes; `normalize-aggregate` does the same after `with`. An `arrow-value` becomes a call with the value as its first argument. |
| `src/pine/ast/value.clj` | `resolve-node` resolves a value against the state: each column's alias, computed names, conditions as `where:` does them, and literals typed by a column beside them. |
| `src/pine/ast/path.clj` | `computed-ref` reads a bare computed name as its value. |
| `src/pine/eval.clj` | `render-value` writes a node as SQL for the dialect. |
| `src/pine/access_policy.clj` | A computed column is hidden when a column it reads is; `check-references` walks into computed values. |
| `src/pine/api.clj` | `describe-computed` gives the client `text` and `uses` instead of the tree. |

### Nodes

```clojure
{:kind :column :alias "a" :column "id" :path [...] :cast "text" :source {...}}
{:kind :literal :value {:type :string :value "att:"}}        ; also a $variable, as {:type :variable ...}
{:kind :call :fn "coalesce" :args [...] :cast "text"}
{:kind :condition :conditions [<where: condition> ...]}      ; joined with or
{:kind :if :when <condition node> :then <node> :else <node or nil>}
{:kind :aggregate :fn "min" :arg <node or nil> :filter <condition node or nil>}
```

The parser keeps conditions as `[column operator value]` triples, as it does for `where:`. `resolve-node` runs
them through `pine.ast.where/resolve-conditions`, so they get the same typing, enum casts, JSON paths and
named-result CTEs. A `$variable` is a literal whose value is the variable, so `pine.variables/bind` replaces
it with no special case.

A computed column in `:columns` is `{:computed <node> :column-alias "k" :alias <current table>}`. An aggregate
adds `:aggregate true`. A where:, order:, group: or per entry that names one carries `:computed <node>` beside
its `:column`, and the evaluator renders the node there.

### Adding a function

Add an entry to `pine.computed/functions`, with its arity and a `:render` function per dialect. The grammar
accepts any name before `(`, and `check-call` reads the same table for errors. Mark an aggregate with
`:aggregate true`; its `:render` gets the argument's and the filter's SQL.
Mark a function that reads JSON with `:json-args true`. Its arguments are rendered as JSON (`render-json` in
`pine.eval`), and its `:render` gets `[sql params]` pairs and returns one, so it can write an argument twice.

### The terminal group:

A query that ends in `group:` reads from an inner query (`x_N`). An aggregate's columns are added to the inner
query under their own names, `__pine_a0__`, and the outer aggregate reads them through `eval/*column-ref*`. A
`group:` sealed into a checkpoint is one query, so there the aggregate reads the table columns directly.
