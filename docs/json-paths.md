# JSON paths

A key inside a `json` or `jsonb` column, used wherever a column can be: `customer | where: data.country = 'SE'`.

## Why

Many tables keep part of their data in a JSON column. Pine could show such a column, and compare the whole value, but
not reach inside it. To pick out a city, filter on a plan or count customers per plan, a user had to leave Pine and
write `->>`, `#>>` or `JSON_EXTRACT` by hand, which differ between Postgres and MySQL.

A path is written with dots, like a column of an alias. Pine works out what each name means, writes the SQL for the
database in use, and sends every key as a parameter.

## Syntax

A column followed by keys and array indexes:

| Form | Example |
|---|---|
| A key | `data.plan`, `data.address.city` |
| A key that isn't a Pine name | `data.'home address'`, `data.'0'`. An apostrophe inside is written twice: `data.'it''s'` |
| An array index, from 0 | `data.tags[0]` |
| Through an alias | `c.data.plan` |

A key may contain `-`, as any Pine name can: `data.first-name`.

A path can go in `select:`, `where:`, `order:` and `group:`.

## Examples

### Basic

```
customer | s: data.address.city
```

```sql
SELECT jsonb_extract_path_text("c_0"."data"::jsonb, ?::text, ?::text) AS "data.address.city", ...
FROM "customer" AS "c_0" LIMIT 250
-- params: 'address', 'city'
```

The result column is named after the path. `as` names it something else: `s: data.plan as tier`.

### Filtering

```
customer | where: data.country = 'SE' | where: data.seats > 10
```

```sql
WHERE jsonb_extract_path("c_0"."data"::jsonb, ?::text) = ?::jsonb
  AND (jsonb_typeof(jsonb_extract_path("c_0"."data"::jsonb, ?::text)) = 'number'
       AND jsonb_extract_path("c_0"."data"::jsonb, ?::text) > ?::jsonb)
-- params: 'country', '"SE"', 'seats', 'seats', '10'
```

- `=`, `!=`, `<` and `>` compare JSON values. The literal is sent as JSON: `10` is the number 10, `'SE'` the string
  `"SE"`, `true` the JSON `true`. So `data.seats > 10` compares numbers. No cast is written.
- `<` and `>` only compare values of the literal's own type. Both databases order values of different JSON types
  instead of refusing to compare them: on Postgres, `true > 10` and `{"a":1} > 10` are true. With the type check, a
  row whose `seats` is `"20"`, `true` or an object doesn't match `> 10`.
- `like`, `ilike`, `in` and `is null` compare the value as text.

### Missing keys and JSON null

```
customer | where: data.cancelled_at is null
```

Matches a row without the key and a row where it is `null`. On MySQL, which turns a JSON null into the text `'null'`,
this is written with `JSON_TYPE`:

```sql
WHERE COALESCE(JSON_TYPE(JSON_EXTRACT(`p_0`.`config`, ?)), 'NULL') = 'NULL'
```

### Sorting and grouping

```
customer | o: data.signup.score desc
customer | group: data.plan => count
```

`order:` sorts JSON values, so numbers sort as numbers. Values of different JSON types sort by type first.
`group:` names each path in its inner query and groups by that name, so two paths into one column stay apart.

### MySQL

```
product | s: config.a.b
```

```sql
SELECT JSON_UNQUOTE(JSON_EXTRACT(`p_0`.`config`, ?)) AS `config.a.b`, ... FROM `product` AS `p_0`
-- params: '$."a"."b"'
```

MySQL takes the whole path as one parameter. Each key is quoted as a JSON string, so a key can hold any character.

## How it works

- **Grammar.** A column may end in path steps: `.key`, `.'quoted key'`, `[n]`. The grammar doesn't know the aliases,
  so `a.b` always parses as alias `a` and column `b`. After a bare name, a path starts with a quoted key or an index.
  Every name has exactly one parse.
- **Parser.** One helper reads every column node into `{:alias :column}`, with `:path` (keys as strings, indexes as
  numbers) and `:column-function` when present. The alias is only what was written before the first dot.
- **AST.** `pine.ast.path/resolve-column` decides what a dotted name means, now that the aliases are known:
  - If the first name is an alias in scope (a table alias or a named result), the second is its column. The rest is
    the path.
  - Otherwise the first name is a column of the current table, and the rest is the path.
  - **When it is both, the alias wins.** A migration that adds a column can't change an existing expression, and the
    column is still reachable through its own table's alias: `e.data.plan`. The column is marked
    `:alias-hides-column` so the canvas can show it.
  - A path column gets `:column-alias`, the path as typed, unless `as` named it.
  - After a named result, a path names the column the named result selected under that name. After
    `customer | s: data.plan |= p`, `p | s: data.plan` is `p`'s column `data.plan`.
- **Literals.** `pine.ast.where/json-literal` turns a literal compared with `=`, `!=`, `<` or `>` into a `:jsonb`
  value with its `:json-type`.
- **SQL.** `pine.eval/json-extract` writes the extraction as JSON (to compare and sort) or as text (to show and
  match). Each clause builder returns its SQL and its parameters together, in the order of their `?`.

## Constraints

- **Read only.** `update!` refuses a path. A result column with `path` is computed, so beamlynx-ui doesn't edit it.
- **No column function on a path.** `data.created => month` is refused: the value is text.
- **The value must be on the right.** `where: data.plan = other_column` and a path on the right are refused.
- **Whole numbers only.** `number` is `[0-9]+` in all of Pine, so `> 4.5` and `> -1` don't parse yet.
- **A path on a column that isn't JSON** is an error: "`uuid_col` is not a JSON column". A column whose type Pine
  can't trace (some named-result columns) is let through, and the database reports it.
- **A name that is neither an alias nor a column** of the current table is an error. It used to reach the database.
- **No key hints.** Keys aren't in the schema. After `c.data.`, hints are empty.
- **Access policy.** A hidden JSON column hides every key inside it. Selecting a key shows the redacted value;
  filtering, sorting or grouping on one is refused.

---

## Implementation

### Grammar and parsing (`pine.bnf`, `parser.clj`)

```
<qualified-symbol> := symbol | symbol json-start json-step* | alias <"."> symbol json-step*
<json-start>       := <"."> json-quoted-key | json-index
<json-step>        := <"."> json-key | json-index
partial-alias      := alias <"."> | alias <"."> symbol json-step* <"."> | symbol json-start json-step* <".">
```

`column-info` reads a `[:column ...]` node. It replaces the per-shape `core.match` patterns of select, order, where,
group and update, which also keeps the compiled class names short. `parse-partial-alias` marks a name that stops inside
a path (`c.data.`) as `{:column "data" :alias "c" :path [] :json-partial true}`.

### AST (`ast/path.clj`, `ast/select.clj`, `ast/where.clj`, `ast/order.clj`, `ast/group.clj`, `ast/update_action.clj`)

`resolve-column` is called by each handler for every column it receives. A where condition keeps `:path`:

```clojure
{:alias "c_0" :column "data" :path ["seats"] :cast nil :operator ">"
 :value {:type :jsonb :value "10" :json-type "number"}}
```

Because `:column` stays the real column, type lookup, `access-policy/sensitive-column?` and
`access-policy/check-references` work unchanged. `hints/exclude-columns` ignores path columns, and `hints/handle`
returns no hints while `in-json-path?`.

### SQL generation (`eval.clj`)

| Function | Role |
|---|---|
| `json-path-params` | One `?::text` parameter per step on Postgres; one `$."a"[0]` path on MySQL |
| `json-extract` | `jsonb_extract_path(_text)(col::jsonb, ...)` or `JSON_(UNQUOTE(JSON_)EXTRACT(col, ?)` |
| `json-type-check` | `jsonb_typeof(x) = 'number'` or `JSON_TYPE(x) IN (...)`, for `<` and `>` |
| `render-path-condition` | A condition on a path, as `[sql params]` |
| `column-sql`, `build-columns-clause`, `build-order-clause`, `build-where-clause` | Return SQL with its params |

`build-bare-select` joins the params as SELECT, then WHERE, then ORDER BY: the order their `?` appear.
`build-inner-select-for-group` does the same for a group query. `q` doubles a quote inside an identifier, because a
path column is named after keys that can hold any character.
