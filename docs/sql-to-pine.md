# SQL to Pine

Turn one SQL `SELECT`, `UPDATE` or `DELETE` into a Pine expression that picks the same rows.

## Why

People arrive with SQL: from logs, dashboards, an ORM's debug output, a colleague's message. AI
agents write SQL well too. beamlynx
draws a Pine expression on its canvas, with its tables, joins and conditions, and lets you edit it by
clicking. Converting the SQL to Pine gives a pasted query the same view.

That matters most for SQL that changes data. An agent's `UPDATE` with three `WITH` queries is hard to
check by reading. Drawn as Pine, each step is a block on the canvas, and a person can see which rows
it would change before deciding to run it.

The conversion is exact or it refuses. It covers the SQL Pine can say and names everything else, with
its line. It never guesses, because a query that converts to something subtly different is worse than
one that doesn't convert.

Two differences are accepted, both caps on how many rows come back. Pine adds `LIMIT 250` to a query
with no `limit:`, so a query without `LIMIT` returns at most 250 rows. A query that ends in `group:`
returns at most 10 000 groups. A converted `COUNT(*)` has no cap: it counts every row.

## API

```
POST /api/v1/sql-to-pine
{"query": "SELECT ..."}
```

```json
{"expression": "company as c\n | employee as e .company_id = .id\n | where: name = 'Acme'",
 "warnings": []}
```

A statement with `WITH` queries becomes several blocks, separated by blank lines. Each `WITH` query
is a block that ends in `|= name`, and the statement's own block comes last. The canvas draws the
earlier blocks as nested queries.

On failure, `error` says what and where, and `error-type` says which kind:

| `error-type` | Meaning |
|---|---|
| `sql-unsupported` | It is SQL, but Pine can't say it: `Line 3: OFFSET isn't in Pine.` |
| `sql-parse-error` | The text isn't SQL this reads. |
| `internal` | The converter produced Pine that doesn't parse, or doesn't build. That is a bug here; the message includes the Pine and Pine's reason. |

Nothing runs, so no connection is needed and no access policy applies. The request may carry
`connection-id`; without it, the selected connection is used. A connection that can't be read is
skipped, never an error. With a connection:

- Names take the database's spelling (see [Names and case](#names-and-case)).
- The converted Pine is built against it, block by block, as a check. Nothing runs. Pine that
  doesn't build is an `internal` error, never a conversion that fails later.
- On MySQL, `||` is refused: there it means OR. `CONCAT()` converts instead.

AI agents use it too. beamlynx-desktop's MCP tool `run_sql_as_pine` accepts SQL, which beamlynx-ui
converts here and then runs as Pine through `/eval`, with the connection's access policy and writes
refused. SQL never runs, so every query still passes the AST, where the policy applies. The agent
gets the Pine back, not SQL.

An `UPDATE` or `DELETE` converts to Pine that ends in `update!` or `delete!`. Converting runs
nothing. The agent's run still refuses it: beamlynx-ui always sends `allow-writes: false` for an
agent, and `/eval` refuses an expression that writes before it runs (see
[side-effects.md](side-effects.md)). Only a person can run the converted write, from their own tab.

## What converts

| SQL | Pine |
|---|---|
| `SELECT * FROM company` | `company` |
| `SELECT * FROM public.company AS c` | `public.company as c` |
| `SELECT name, id AS i FROM company` | `company \| select: name, id as i` |
| `JOIN employee e ON e.company_id = c.id` | `\| employee as e .company_id = .id` |
| `LEFT JOIN`, `RIGHT JOIN` (with or without `OUTER`) | `:left`, `:right` |
| `ON a.x = b.y AND a.z = b.w` | `.y = .x, .w = .z` |
| `JOIN employee USING (company_id)` | `.company_id = .company_id` |
| `WHERE a = 1 AND b = 'x'` | `\| where: a = 1 \| where: b = 'x'` |
| `WHERE a = 1 OR b = 'x'` | `\| where: a = 1 or b = 'x'` |
| `<>`, `!=`, `<`, `>`, `<=`, `>=` | the same, `<>` as `!=`. Two columns compare with `=` and `!=` only |
| `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE` | the same |
| `IS NULL`, `IS NOT NULL` | the same |
| `IN ('a', 1)`, `NOT IN (...)` | the same |
| `x BETWEEN 1 AND 9` | `where: x >= 1 \| where: x <= 9` |
| `x NOT BETWEEN 1 AND 9` | `where: x < 1 or x > 9` |
| `WHERE active`, `WHERE NOT active` | `where: active = true`, `where: active = false` |
| `5 < x` | `x > 5` |
| `x::text = '1'`, `CAST(x AS uuid) = '...'` | `x = '1' ::text`, `x = '...' ::uuid` |
| `'2024-01-01'::timestamp` | `'2024-01-01'`: Pine types a value by its column |
| `SELECT COUNT(*) FROM ... WHERE ...` | `... \| count:` |
| `SELECT COUNT(*) AS n FROM t` | `t \| group: with count as n`: `count:` can't be named |
| `SELECT COUNT(*) AS n, SUM(x) AS s FROM t` | `t \| group: with count as n, x => sum as s`: one group, the whole table |
| `SELECT a, COUNT(*) FROM t GROUP BY a` | `t \| group: a with count` |
| `SELECT a, COUNT(*) AS n FROM t GROUP BY a` | `t \| group: a with count as n` |
| `SELECT a FROM t GROUP BY a` | `t \| group: a \| select: a`: Pine's count is left out |
| `ORDER BY a, b DESC` | `order: a asc, b desc` |
| `ORDER BY a NULLS FIRST`, `b DESC NULLS LAST` | `order: a asc nulls first, b desc nulls last` |
| `LIMIT 10` | `limit: 10` |
| `$name` | `$name`, a Pine variable |
| `x IS DISTINCT FROM 'v'` | `where: x != 'v' or x is null` |
| `x IS NOT DISTINCT FROM 'v'` | `where: x = 'v' \| where: x is not null` |
| `x IS DISTINCT FROM NULL` | `where: x is not null` |
| `x > NOW()`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP` | `where: x > now()` |
| `x = COALESCE(y, 1)`, `x = y \|\| 'a'` | `where: x = coalesce(y, 1)`, `where: x = concat(y, 'a')` |
| `WITH a AS (...) SELECT ... FROM a` | a block ending in `\|= a`, then `a \| ...` |
| `FROM (SELECT ...) x` | a block ending in `\|= x`, then `x \| ...` |
| `x IN (SELECT c FROM t ...)` | a block for the subquery, then `where: x in <block>` |
| `LEFT JOIN k ON k.c = t.x WHERE k.c IS NULL` | `where: x not in k` |
| `EXISTS (SELECT 1 FROM k WHERE k.c = t.x AND ...)` | a block for `k`, then `where: x in <block>` (see [EXISTS](#exists)) |
| `NOT EXISTS (...)`, the same | `where: x not in <block>`, guarded against NULLs |
| `ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn`, kept with `rn = 1` or `rn <= N` | `order: o \| limit: N per p` |
| `SELECT DISTINCT ON (p) ... ORDER BY p, o` | `order: p, o \| limit: 1 per p` |
| `COALESCE(a, b)`, `a \|\| b`, `NOW()` | `coalesce(a, b)`, `concat(a, b)`, `now()` (see [Computed values](#computed-values)) |
| `CASE WHEN a THEN x WHEN b THEN y ELSE z END AS k` | `select: if a then x else if b then y else z as k` |
| `CASE x WHEN 1 THEN 'a' ELSE 'b' END AS k` | `select: if x = 1 then 'a' else 'b' as k` |
| `(x IS NOT NULL) AS k`, `x IS NOT NULL AS k` | `select: x is not null as k`, a true or false value |
| `(a IN (SELECT b FROM k)) AS m` | a block for the subquery, then `select: a in <block> as m` |
| `a::text AS k`, `CAST(a AS text) AS k` | `select: a::text as k` |
| `t.data->>'plan' AS p`, `t.data->'tags'->>0 AS tag` | `select: data.plan as p, data.tags[0] as tag` (see [JSON keys](#json-keys)) |
| `WHERE t.data->>'plan' = 'pro'`, `IN (...)`, `LIKE`, `IS NULL` | `where: data.plan = 'pro'` (see [JSON keys](#json-keys)) |
| `CROSS JOIN LATERAL jsonb_array_elements(a.data) e` | `\| unnest: a.data as e` (see [JSON arrays as rows](#json-arrays-as-rows)) |
| `e->>'k'`, `e` of those elements | `e.k`, `e` |
| `jsonb_typeof(x)`, `jsonb_array_length(x)` | the same, in Pine |
| `jsonb_typeof(x) = 'array'`, in `WHERE` or `CASE WHEN` | `jsonb_typeof(x) = 'array'` |
| `SELECT s.*, COALESCE(...) AS k FROM s` | `s as s \| select: s.*, coalesce(...) as k` |
| `MIN(x) AS m ... GROUP BY a` | `group: a with x => min as m`; also `MAX`, `SUM`, `AVG` |
| `MIN(x)` without `AS` | `min(x) as min`, the name Postgres gives it |
| `MAX(x)::text AS m` | `group: ... with x => max as max_x \| select: ..., max_x::text as m` |
| `MIN(x) FILTER (WHERE c)`, `COUNT(*) FILTER (WHERE c)` | `min(x) where c`, `count where c` |
| `COUNT(x) ... GROUP BY a` | `group: a with count where x is not null`: SQL counts the rows where `x` isn't NULL |
| `SELECT COUNT(x) FROM t` | `t \| where: x is not null \| count:` |
| `BOOL_OR(c)`, `BOOL_AND(c)` | `any(c)`, `all(c)` |
| `GROUP BY 1, 2`, `ORDER BY 2 DESC` | the selected columns they number; `ORDER BY` uses the item's name when it has one |
| `GROUP BY a HAVING ...` | `group: a with ... \| where: ...`, by the aggregates' names |
| `LEFT JOIN k ON k.c = t.x`, k distinct values, tested with `k.c IS NOT NULL` | `x in k` (see [Tests on a LEFT JOIN](#tests-on-a-left-join)) |
| `ar."resourceableId" = g.id::text` in `ON` | `.resourceableId = .id ::text` |
| `UPDATE t SET c = 'v' WHERE ...` | `t \| where: ... \| update! c = 'v'` |
| `SET c = COALESCE(c, 'x')`, `SET c = c \|\| 'x'` | `update! c = coalesce(c, 'x')`, `update! c = concat(c, 'x')` |
| `DELETE FROM t WHERE ...` | `t \| where: ... \| delete! .id`, by the table's primary key |
| `BEGIN; ...; COMMIT;` | the statement inside, with a warning |

### Order

Pine's `order:` sorts descending when no direction is given, and SQL ascending. So every `order:`
column is written with its direction.

### Where the operations go

Tables and joins first, in the SQL's order. Then one `where:` for each AND-ed condition. Then `select:`,
`group:` or `count:`. Then `order:` and `limit:`. Pine builds one SQL `SELECT` with every join and
every condition in it, so a `where:` after the joins means the same as SQL's single `WHERE`.

### Computed values

A value in the select list that isn't a column becomes a [computed value](computed-values.md), named
with the SQL's `AS`:

```sql
SELECT s.*, COALESCE(s.reference_id, 'att:' || s.any_attachment_id) AS dedupe_key FROM sp_docs s
```

```
sp_docs as s
 | select: s.*, coalesce(reference_id, concat('att:', any_attachment_id)) as dedupe_key
```

- `COALESCE` becomes `coalesce()`, `NOW()` becomes `now()`, and a cast to `text` or `uuid` stays.
  `CURRENT_TIMESTAMP` and `LOCALTIMESTAMP` are `now()` too. `CURRENT_DATE`, `CURRENT_TIME` and
  `CURRENT_USER` are refused by name.
- `a || b || c` becomes `concat(a, b, c)`. Both are NULL when any part is. A cast to text on a part is
  left out: Pine's `concat()` makes text of each part. SQL's own `CONCAT()` is refused on Postgres,
  where it skips NULLs. On MySQL it is NULL when a part is, so it converts, and `||` is refused, since
  MySQL reads it as OR.
- `CAST(x AS t)` is `x::t`, whatever `x` is.
- `CASE WHEN a THEN x WHEN b THEN y ELSE z END` becomes `if a then x else if b then y else z`. A `CASE`
  right after `THEN` goes in parentheses.
- A condition used as a value, `(x IS NOT NULL)`, becomes the condition. Its `NOT`s are pushed down as in
  `WHERE`. Pine joins the conditions of one value with `or` only, so a value with `AND` in it is refused.
  So are `BETWEEN` and `IS [NOT] DISTINCT FROM`, which are two conditions joined with AND; the
  message names them.
- A computed value needs `AS`: Pine names every one. `ORDER BY` its name works, and so does `ORDER BY`
  the same expression.
- A column named `true`, `false`, `null`, `if`, `then` or `else` is refused inside a computed value, and
  `true`, `false` and `null` on the right of a condition or in `select:`: Pine reads them as keywords
  there.
- `jsonb_typeof()` and `jsonb_array_length()` keep their names. Pine has them on every database.
- A function on the left of a comparison, `jsonb_typeof(x) = 'array'` or `COALESCE(a, b) = 'x'`, stays
  there. Pine conditions can start with a function. It is compared with a value, not with a column.
- Any other function is refused by name: `Functions like lower() aren't in Pine.`

### JSON keys

`->` and `->>` become a [JSON path](json-paths.md) in a selected value or a function's argument:

```sql
SELECT t.template->>'kind' AS kind, t.template->'nodes'->0 AS first FROM company_structure_template t
```

```
company_structure_template as t
 | select: template.kind as kind, template.nodes[0] as first
```

- A whole-number key, `->0`, is an index, `[0]`. A key that isn't a Pine name is quoted: `data.'a b'`.
- On MySQL and SQLite, `->` takes a path: `t.data->>'$.a."b c"[0]'` is `data.a.'b c'[0]`. On Postgres
  `'$.a'` is a key with that name. Without the connection the database isn't known, so a key starting with
  `$` is refused. A wildcard or `last` in a path is refused.
- In `select:`, Pine shows a key as text, as `->>` does. `->` gives JSON in SQL, so a string keeps its
  quotes there. The conversion warns about that difference.
- Inside `jsonb_typeof()` and `jsonb_array_length()`, Pine reads the key as JSON, as `->` gives it. A
  `CASE` there passes this on to its branches, so `CASE WHEN ... THEN t.d ELSE t.d->'nodes' END` is
  exact. Any other function between them is refused.
- When a column's bare name is also a table's alias, its first key is quoted: `data.'k'`. Pine would
  read `data.k` as the column `k` of the table `data`.
- A `CASE` or `COALESCE` outside those functions that mixes a `->` key with a column or a function is
  refused. Pine shows the key as text and the column as JSON, and Postgres can't put both in one column.
  Keys and literals alone are fine.
- `ORDER BY` a `->` key, or the name it is selected as, sorts by the path: SQL and Pine both sort it as
  JSON. `ORDER BY` a `->>` key is refused: SQL sorts it as text and Pine as JSON, so `'10'` and `9` swap.
- In `WHERE`, a `->>` key converts where Pine's test means the same as SQL's:
  - `LIKE`, `IN` with strings, and `IS NULL` compare text in both.
  - `=` and `!=` compare text in SQL and JSON values in Pine. For a string they agree, so `data->>'plan' = 'pro'`
    converts. A string that is also how a number, `true`, `false`, `null`, an object or an array reads as text is
    refused: `data->>'n' = '10'` matches the number 10 in SQL, and not in Pine.
  - On Postgres and SQLite, `->>` gives NULL for a JSON `null`, so SQL's `!=` drops that row. Pine's `!=` keeps it, so
    `!=` also gets `where: data.plan is not null`. On MySQL, `->>` gives the text `'null'`, and `!=` stays as it is.
  - On MySQL, `IS NULL` on a key is refused: `->>` gives the text `'null'` for a JSON `null`, and Pine's `is null`
    counts a JSON `null` as NULL.
  - `<`, `>`, a number on the right, a cast, and a `->` key are refused.
- With `GROUP BY`, a key is refused. `#>` and `#>>` are refused too.

### JSON arrays as rows

`jsonb_array_elements` in `FROM` gives one row for each element of an array. Pine says it with
[`unnest:`](json-paths.md#arrays-as-rows):

```sql
SELECT a.id, e->>'field' AS field
FROM activity a CROSS JOIN LATERAL jsonb_array_elements(a.data->'changes') e
WHERE e->>'field' IN ('legalTypes', 'giin')
```

```
activity as a
 | unnest: a.data.changes as e
 | where: e.field in ('legalTypes', 'giin')
 | select: id, e.field as field
```

- `CROSS JOIN LATERAL`, a comma, and `JOIN LATERAL ... ON true` all convert. `LATERAL` may be left out.
  `json_array_elements` and the `_text` forms convert too.
- `e` is the element, `e.value` and `e(x)`'s `x` name it too, and `e->>'k'` is a key of it. A selected element is
  named as SQL names it: `e.value` becomes `e as value`.
- `jsonb_array_elements` gives JSON, so a string element keeps its quotes in SQL. Pine shows it as text, as
  `jsonb_array_elements_text` does, and warns.
- An element of the `_text` forms compares as a `->>` key does. An element of the JSON forms is refused in `WHERE`.
- `PARTITION BY e->>'k'` partitions by the key's text, as Pine does. `PARTITION BY e->'k'` is refused.
- Refused: `LEFT JOIN LATERAL`, which keeps rows whose array is empty, and `->>` inside the function, which gives text.
  Any other function in `FROM`, like `generate_series`, is refused as a cross join.

The SQL fails on a row whose value isn't an array. Pine skips that row.

### Aggregates and HAVING

Every `GROUP BY` converts in steps whose result has exactly the SQL's columns, under its names:

```sql
SELECT d.id, COALESCE(MIN(a.id::text) FILTER (WHERE a.type = 'sharepoint'), MIN(a.id::text)) AS att
FROM document d LEFT JOIN attachment a ON a.document_id = d.id
GROUP BY d.id, d.reference_id
HAVING BOOL_OR(a.type = 'sharepoint') OR d.reference_id IS NOT NULL
```

```
document as d
 | attachment as a :left .document_id = .id
 | group: d.id, d.reference_id with id::text => min where type = 'sharepoint' as min_id, id::text => min as min_id_2, any(type = 'sharepoint') as any_type
 | where: any_type = true or reference_id is not null
 | select: id, coalesce(min_id, min_id_2) as att
```

1. `group:` lists the grouped columns and every aggregate once. An aggregate selected on its own takes
   the SQL's name for it: its `AS`, or else its function's name, as Postgres names it (`count`, `min`).
   The others take Pine's default name (`min_id`), with `_2`, `_3`, ... when it is taken. When two SQL
   columns have one name, like `COUNT(*), MIN(x) AS count`, the second is renamed with a warning; two
   with the same `AS` are refused.
2. `where:` is the `HAVING`. It follows the `group:`, so a [checkpoint](checkpoints.md) seals the groups
   first, and it filters them by the names. A boolean aggregate on its own, like `BOOL_OR(...)`, is
   compared with `true`.
3. `order:` sorts by the group's columns when the `ORDER BY` names one the `SELECT` leaves out, and
   comes before the `select:`. Otherwise it sorts by the SQL's names, after it. `ORDER BY` an aggregate
   uses the aggregate's name.
4. `select:` lists the SQL's columns in its order, under its names: a grouped column with its `AS`,
   and a cast after an aggregate (`MAX(x)::text`). It is left out when the `group:` already returns
   exactly those. A `group:` with no aggregate adds a count, which the `select:` leaves out.

`MIN`, `MAX`, `SUM` and `AVG` convert, `BOOL_OR` to `any()` and `BOOL_AND` to `all()`, each with an
optional `FILTER (WHERE ...)`. `COUNT(*)` is `count`, and `COUNT(*) FILTER (WHERE c)` is
`count where c`. Any other function, like `STRING_AGG`, is refused by name.

### Tests on a LEFT JOIN

This SQL asks, for each row of `g`, whether `edited` has its id:

```sql
WITH edited AS (SELECT DISTINCT ... AS document_id FROM ...)
SELECT g.*, (e.document_id IS NOT NULL) AS has_activity
FROM grouped g LEFT JOIN edited e ON e.document_id = g.id
```

In Pine that is `id in edited`, with no join: `grouped as g | select: g.*, id in edited as has_activity`.
It converts that way when all of these hold:

- The `ON` is that one pair, and the joined query holds distinct values of the one column it selects
  (`SELECT DISTINCT c`). Otherwise the join could repeat rows of `g`, which `in` doesn't.
- The joined query is used nowhere else, and every other mention of it is a test `k.c IS NULL` or
  `IS NOT NULL`.

`IS NOT NULL` becomes `in` and `IS NULL` becomes `not in`. When the schema doesn't show both columns are
`NOT NULL`, `in` could be unknown where the SQL's test is false. So a value is written
`if id in edited then true else false`, which is never NULL. In a condition, where unknown counts as
false, plain `in` is exact. A negated test there that could be unknown is refused.

A `LEFT JOIN k ... WHERE k.c IS NULL` on its own is still [rows with no match](#rows-with-no-match).

### WHERE becomes several where: steps

Inside one `where:`, Pine joins conditions with `or` only. Several `where:` steps are AND-ed. So the
condition is put into conjunctive normal form:

1. `NOT` is pushed down until it applies to single comparisons, flipping each: `=` and `!=`, `<` and
   `>=`, `like` and `not like`, `is null` and `is not null`, `in` and `not in`. `NOT (a AND b)` becomes
   `NOT a OR NOT b`.
2. `OR` is distributed over `AND`: `(a AND b) OR c` becomes `(a OR c) AND (b OR c)`.

Each AND-ed part is one `where:`. Both steps keep their meaning under SQL's NULL rules, where a
comparison with NULL is neither true nor false.

Distributing can multiply: `(a AND b) OR (c AND d)` is four `where:` steps. Past 16 the conversion
refuses and asks for a simpler mix.

### IS DISTINCT FROM

`x IS DISTINCT FROM 'v'` is true when x is NULL, unlike `x != 'v'`. So it becomes
`x != 'v' or x is null`. `IS NOT DISTINCT FROM 'v'` becomes `x = 'v'` and `x is not null`. Both
forms are true or false for every x, NULL included, as the SQL is, so `NOT` in front of them flips
them exactly. Against another column it is refused.

### WITH queries and subqueries

Each `WITH` query becomes a block of its own, ending in `|= name`. A later query uses it as a table,
by its name, as SQL does:

```sql
WITH active AS (SELECT id, name FROM company WHERE active)
SELECT * FROM active WHERE name LIKE 'A%'
```

```
company
 | where: active = true
 | select: id, name |= active

active
 | where: name like 'A%'
```

A subquery in `FROM` or `JOIN` works the same, named by its alias. A subquery after `IN` becomes a
block too, named after its table and column, like `company_ids`. When it is just `SELECT c FROM w`,
and `w` is a `WITH` query used for nothing else, `w` itself is used and selects only `c`.

A subquery after `IN` inside a selected value, `(a IN (SELECT b FROM k)) AS m`, becomes a block in the
same way, and the value `a in <block> as m`.

A name Pine makes up for a block, like `company_ids`, is never a table the SQL reads: then it is
`company_ids_2`. A `WITH` query named like a table the statement also reads with its schema
(`WITH company AS (...) ... FROM public.company`) is refused, since Pine would read the `WITH` query for
both.

With the schema, a bare column in a subquery after `IN` must be a column of the subquery's own tables.
In SQL it may be the outer query's column; Pine would read it as the subquery's, so it is refused.

`DISTINCT` doesn't change which values a set holds, so it is dropped from a query that is only used
after `IN`, unless it has a `LIMIT`: the `LIMIT` counts distinct values. Anywhere else it is refused.

A `WITH` query's columns are what a later query sees. So a conversion that would change them, such as
renaming a second column of one name, is refused inside a `WITH` query.

### Rows with no match

This SQL keeps the rows of `t` that have no row in `k`:

```sql
SELECT t.* FROM t LEFT JOIN k ON k.c = t.x WHERE k.c IS NULL
```

In Pine that is `where: x not in k`, with `k` selecting only `c`. It converts that way when the
`ON` is that one pair, the `IS NULL` is on `k.c` and stands on its own in the `WHERE`, and nothing
else uses `k`. A bare column that could be `k`'s counts as a use, unless the schema says it isn't.
Otherwise the `LEFT JOIN` converts as a join.

When `k` is a `WITH` query with a `LIMIT`, it isn't narrowed to `c`: a new block reads it, so the
`LIMIT` still applies before `c is not null`.

`NOT IN` treats NULL differently from the join. When `t.x` is NULL, the join keeps the row and
`NOT IN` drops it. When `k.c` holds a NULL, `NOT IN` matches nothing at all. So unless the schema
says both columns are `NOT NULL`, the condition is `x is null or x not in k`, and `k` leaves out
its NULLs with `where: c is not null`. The schema comes with the connection. Through `WITH`
queries, a column is `NOT NULL` when the table column it comes from is, and no outer join makes it
NULL.

A `NOT IN (SELECT ...)` written in the SQL converts as it is: it has the same NULL rules as Pine's
`not in`.

### EXISTS

`EXISTS` asks whether a row has a related row. Pine asks that with `in`:

```sql
SELECT * FROM "user" u
WHERE EXISTS (SELECT 1 FROM user_tenant_role r WHERE r."userId" = u.id AND r.type = 'counterparty')
```

```
user_tenant_role as r
 | where: type = 'counterparty'
 | select: userId |= user_tenant_role_userIds

user as u
 | where: id in user_tenant_role_userIds
```

It converts when the subquery is tied to the outer query by one equality, `k.c = t.x` in either
order, joined to the rest of its `WHERE` with `AND`. The subquery may not refer to the outer query
anywhere else. A column qualified by none of the subquery's tables is the outer query's, so an
alias the subquery reuses is its own, as in SQL. What the subquery selects doesn't matter, so
`SELECT 1` and `SELECT *` both convert. Its `ORDER BY` and `LIMIT` are left out: they don't change
whether a row exists. `GROUP BY`, `HAVING` and `DISTINCT ON` in it are refused, and so is an
`EXISTS` inside another.

`EXISTS` is true or false, never NULL. Pine's `in` is NULL when `x` is NULL, or when `x` isn't in
the block and the block holds a NULL. Where a NULL drops the row just as false would, nothing more
is needed. Under a `NOT`, it isn't: `NOT EXISTS` keeps a row whose `x` is NULL. So under a `NOT`,
unless the schema says the columns are `NOT NULL`, the condition is `x is null or x not in k`, and
`k` leaves out its NULLs with `where: c is not null`, as for [rows with no match](#rows-with-no-match).

`EXISTS` converts only in `WHERE`, of a `SELECT`, `UPDATE` or `DELETE`.

### Top N per group

A `ROW_NUMBER()` numbers the rows of each group. A later query that keeps the first ones is Pine's
`limit: N per` (see [limit-per.md](limit-per.md)):

```sql
WITH ranked AS (
  SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.company_id ORDER BY d.created_at DESC) AS rn
  FROM document d
)
SELECT id FROM ranked WHERE rn = 1
```

```
document as d
 | order: created_at desc
 | limit: 1 per company_id |= ranked

ranked
 | select: id
```

- Every query that uses the numbered rows must keep the first N, with `rn = 1`, `rn <= N` or
  `rn < N`, and all of them the same N. That test leaves the later query; `limit: N per` does it.
- `rn` can't be used any other way. `rn = 2` and `rn > 1` are refused by name.
- An expression in `PARTITION BY` or `ORDER BY`, such as `(x IS NOT NULL)` or a `CASE`, is selected as
  a computed column before the `order:`, and used by its name. A selected value with the same expression
  is used by its own name. Otherwise the new column is called `rn_order_1`, `rn_partition_1`, ..., after
  `rn` and its place. It is a column the SQL doesn't have, which a warning says when it reaches a result.
- A `ROW_NUMBER()` that no later query uses, and no `*` returns, is left out with a warning. So a second,
  unused one doesn't stop the first from converting.
- `rn` isn't a column of the result in Pine. A `SELECT *` over the numbered rows gets a warning.
- The numbered rows can't be joined with `LEFT JOIN` or `RIGHT JOIN`, or read twice in one query.
- With no `ORDER BY`, the SQL keeps any row of each group, and Pine the first in primary key order. A
  warning says so. The same goes for `DISTINCT ON` with no `ORDER BY`.
- A column Pine adds for an expression never takes the name of a column a `*` brings in, when the
  schema says what those are.

Postgres's `SELECT DISTINCT ON (p) ... ORDER BY p, o` keeps the first row of each `p`. It becomes
`order: p, o | limit: 1 per p`. Sorting by `p` first changes no group's first row, and keeps the
order the SQL returns rows in. The `ORDER BY` must start with the `DISTINCT ON` columns, as Postgres
requires.

### UPDATE and DELETE

An `UPDATE` becomes the Pine that picks the rows it changes, ending in `update!`. A `DELETE` ends in
`delete!`.

```sql
UPDATE employee e SET active = false
FROM company c
WHERE e.company_id = c.id AND c.name = 'Acme'
```

```
employee as e
 | company as c .id = .company_id
 | where: name = 'Acme'
 | from: e
 | update! active = false
```

- The `WHERE` conditions that tie the `FROM` table to the changed one are its join. The rest are
  `where:` steps. `DELETE ... USING` works the same.
- When the pipe ends on another table, `from:` goes back to the changed one before `update!`.
- `SET` takes values: a string, a number, true, false, NULL, a `$variable`, `NOW()`,
  `COALESCE(...)` or `a || b` of the changed table's columns. Another function, or another column on
  its own, is refused.
- `delete!` names rows by the table's primary key, so converting a `DELETE` needs the connection.
  A table without a primary key is refused, and with the connection, so is an `UPDATE` of one.
- A statement with no condition of its own is refused, as Pine refuses to change every row.

### Scripts

A script is split on `;`, but not on one inside a string, a quoted name, a comment or a
dollar-quoted body (`$$ ... $$`). `BEGIN`, `COMMIT` and `ROLLBACK` are left out, with a warning that
names them as written (`BEGIN WORK`, `END`): Pine runs each `update!` and `delete!` in a transaction of
its own. A script that ends in `ROLLBACK` and changes
data is refused, since the SQL undoes its change and Pine wouldn't.

Of the other statements, the one that changes data converts, or the first one when none does. Each
statement left out gets a warning with its line, such as a `SELECT COUNT(*)` that checks the result
afterwards. A script with two statements that change data is refused. Only a `SELECT` counts as a
statement that doesn't change data: a `CALL`, a `DO` block or an `EXPLAIN ANALYZE` might.

### Joins

A Pine table joins the table piped before it. So each `JOIN`'s `ON` must pair columns of the new
table with columns of one earlier table. When that table isn't the last one piped, a `from:` comes
first:

```sql
SELECT * FROM company c
JOIN employee e ON e.company_id = c.id
JOIN invoice i ON i.company_id = c.id
```

```
company as c
 | employee as e .company_id = .id
 | from: c
 | invoice as i .company_id = .id
 | select: c.*, e.*, i.*
```

Joins are written with explicit columns, `.new_column = .earlier_column`. That form joins on exactly
those columns, whatever the foreign keys say, and the canvas shows it as a manual join.

In an inner join, any other `ON` condition (`AND e.active = true`) means the same in `WHERE`, so it
moves to a `where:` with a warning. In a left or right join it doesn't, so it is refused.

### Columns and aliases

A table keeps the alias the SQL gave it. A table without one gets one, its own name, only when
something refers to it: `company as company`.

A column of the table the pipe ends on is written bare, since Pine reads a bare column as that
table's. Any other column is written `alias.column`.

`SELECT *` over a join selects every table's columns: `select: c.*, e.*`. Pine with no `select:`
would return only the last table's.

Pine also returns every column of the table the pipe ends on unless `select:` names one of them.
So a select list of only earlier tables' columns ends with `from:` the first of them:
`SELECT c.name FROM company c JOIN employee e ON ...` converts to
`company as c | employee as e ... | select: c.name | from: c`. Without it, every column of
`employee` would come back too.

### Names and case

Keywords may be in any case. A quoted identifier keeps its case: `"Users"` in Postgres or
`` `Users` `` in MySQL. A double-quoted name is always an identifier, never a string, as in Postgres
and standard SQL. A name Pine can't write, like `"my table"`, is refused.

A bare identifier takes the database's spelling, when there is a connection. For each bare table,
schema or column name:

1. If the database has the name in lowercase, that is the one. Postgres reads an unquoted name this
   way.
2. Otherwise, if exactly one name matches ignoring case, that is the one. `c.tenantId` becomes
   `.tenantId` when `company` has a `tenantId` column.
3. If several match, like `Name` and `NAME`, the conversion refuses and asks for the name in quotes.
4. If none match, the name stays lowercase.

Step 2 is more lenient than Postgres. There, `c.tenantId` means `tenantid`, and fails when the column
is `"tenantId"`. The converter takes the column the SQL most likely means. In MySQL and SQLite, where
column names ignore case, it is what the database does.

Without a connection, a bare identifier is folded to lowercase: `FROM Users` is the table `users`.

### Pine's own SQL

The SQL preview beamlynx shows for a Pine expression converts back. Its hidden key columns
(`"c_0"."id" AS "__c_0__id"`) are left out, with a warning, since Pine adds them itself. Only that exact
form is: an alias that merely looks like one is kept. Its `LIMIT 250` becomes `limit: 250`. Computed
values convert back, including the casts Pine writes around each part of a `concat()`. A preview that
starts with `WITH`, which Pine writes for `count:`, `group:`, `|=` names and checkpoints, converts to a
block for each `WITH` query.

Two exceptions:

- A preview of `limit: N per` doesn't convert yet.
- A `group:` with an aggregate over a value converts back with the internal names Pine gives the
  aggregated columns: `select: name, id as __pine_a0__ |= x_1`, then
  `group: name with __pine_a0__ => min as m`. It means the same, but isn't the Pine you started from.

### Warnings

A conversion can succeed with warnings. Each says how the Pine differs from the SQL without
changing which rows come back:

- A second SQL column of one name, renamed: `count_2`. In a `WITH` query this and the next are
  refused instead, since a later query may use the columns.
- An inner join's `ON` condition moved to `where:`.
- `ROW_NUMBER()` or `DISTINCT ON` with no `ORDER BY`: which row of each group Pine keeps.
- Pine's hidden columns left out.
- `rn` from `ROW_NUMBER()` isn't a column of a `SELECT *` result.
- A `ROW_NUMBER()` that no later query uses is left out.
- A column added for an expression in `ROW_NUMBER()`, when it reaches a result.
- `BEGIN`, `COMMIT` and `ROLLBACK` left out.
- `->` in a selected value: Pine shows the value as text.
- Another statement in the script left out, with its line.

## What doesn't convert

Each is refused with its line.

| SQL | Why |
|---|---|
| `INSERT`, `MERGE`, `CALL`, `DO`, DDL and other statements | Only `SELECT`, `UPDATE` and `DELETE` convert. |
| Two statements that change data | One at a time. |
| `ROLLBACK` after a change | Pine's `update!` keeps its change. |
| `DISTINCT`, other than in a set used after `IN` without `LIMIT` | Not in Pine. |
| `SELECT` without `FROM` | Pine starts from a table. |
| A column list after a `WITH` name, `w(a, b)` | Name the columns with `AS` in its query. |
| A string with a line break | Pine's blocks are separated by blank lines. |
| A time with a zone (`'...Z'`, `'...+02'`), or `'... 10:00'::date` | Pine refuses the zone, and would keep the time. |
| `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_USER` | Not in Pine. |
| Two columns compared with `<`, `>`, `<=` or `>=` | Pine compares two columns with `=` and `!=`. |
| SQL nested too deeply | Simplify it. |
| `OFFSET`, `FETCH`, `UNION`, `INTERSECT`, `EXCEPT` | Not in Pine. |
| `HAVING` without `GROUP BY` | Not converted. |
| `WITH RECURSIVE`, `MATERIALIZED`, `RETURNING` | Not in Pine. |
| A subquery other than in `FROM`, `JOIN` or after `IN` | Not in Pine. |
| A function other than `COALESCE`, `NOW()`, `jsonb_typeof()`, `jsonb_array_length()` and the aggregates above; arithmetic | Not in Pine. |
| `->` or `->>` in `WHERE` or with `GROUP BY`; `ORDER BY` a `->>` key; `#>`, `#>>`; a negative index; a `$` path without the connection; a `CASE` or `COALESCE` mixing a `->` key with a column | See [JSON keys](#json-keys). |
| `EXISTS` outside `WHERE`, or not tied to the outer query by one equality | See [EXISTS](#exists). |
| `CONCAT()` on Postgres; `\|\|` on MySQL | Postgres's `CONCAT()` skips NULLs: write `a \|\| b`. MySQL reads `\|\|` as OR. |
| `AND`, `BETWEEN` or `IS [NOT] DISTINCT FROM` inside a value | Pine joins a value's conditions with `or`. |
| A computed value without `AS`, other than an aggregate | Pine names every computed value. |
| A cast after an aggregate to other than `text` or `uuid` | Pine casts to those two. |
| An aggregate inside another aggregate | Not in Pine. |
| `ORDER BY` an expression that isn't selected, outside `ROW_NUMBER()` | Select it with a name, and sort by the name. |
| A window function other than `ROW_NUMBER()`, or one not kept with `rn <= N` | Not in Pine. |
| `rn = 2`, `rn > 1`, or `rn` used any other way | Pine keeps the first N rows of each group. |
| `LIMIT` after `DISTINCT ON` | Remove it. |
| A function other than `NOW()` and `COALESCE` in `SET` | `update!` writes a value, `coalesce(...)`, `a \|\| b` or `now()`. |
| A column on its own in `SET`, or another table's column | Not converted. |
| `UPDATE` of a table with no primary key, with the connection | `update!` finds rows by the key. |
| `UPDATE` or `DELETE` with no condition of its own | Pine refuses to change every row. |
| `DELETE` without a connection, or on a table with no primary key | `delete!` names rows by the key. |
| `IS DISTINCT FROM` another column | Compare with a value. |
| `COUNT(DISTINCT ...)` | Pine's count counts rows. |
| `COUNT(x) FILTER (WHERE ...)` | It needs `x is not null` and the filter, and a count's filter joins its conditions with `or`. |
| A column next to an aggregate without `GROUP BY` | Needs `GROUP BY`. |
| `ORDER BY` or `LIMIT` on a single `COUNT(*)` | Remove them. |
| A selected column that isn't grouped | Not in SQL either. |
| `ORDER BY 1` or `GROUP BY 1` after a `*`, `GROUP BY` a position that numbers a value | Name the column. |
| `FULL`, `CROSS` or `NATURAL` join, a comma between tables | Pine joins inner, left or right, on named columns. |
| A join tied to two earlier tables | Pine joins a table to one other. |
| `USING` after more than one table | It can't say which table it joins to. Write it with `ON`. |
| A column in a join that isn't qualified, when more than one table has it, or the schema isn't known | It could belong to more than one table. |
| `= NULL` | Never true in SQL. Use `IS NULL`. |
| A cast other than to `text` or `uuid`, in a condition, a value or a join's `ON` | Pine casts to those two. |
| A comparison with no column | Pine conditions start with a column, or a function of one. |
| A function compared with a column | Pine compares a function with a value. |
| `$1`, `?` | Write the value, or a `$name` variable. |
| `LIMIT` over 10 000 | Pine's maximum. |

---

## Implementation

| File | What it does |
|---|---|
| `src/pine/sql/sql.bnf` | The SQL subset, as an Instaparse grammar: one statement. |
| `src/pine/sql/parser.clj` | Splits a script on `;`, reads a statement into plain maps, and names an unsupported word when the reading stops on one. |
| `src/pine/sql/transpile.clj` | Picks the statement, rewrites `WITH` queries and subqueries into blocks, maps each to Pine operations, renders them, and checks each block parses and, with a connection, builds. |
| `src/pine/api.clj` | `POST /api/v1/sql-to-pine`. |

### Reading (`pine.sql.parser`)

Keywords are case-insensitive regexes ending in `\b`, so `selected` is never `select` followed by
`ed`. Every keyword, including ones the grammar never uses, like `OFFSET`, is kept out of bare
identifiers. Without that, `FROM t OFFSET 5` would read `OFFSET` as the table's alias, and the reading
would stop on `5`.

When the reading stops, the word at that point picks the message: `OFFSET` gives "OFFSET isn't in
Pine", a `(` after a name gives "Functions like lower() aren't in Pine", and so on. Otherwise the
message quotes the text where it stopped.

Each node a later refusal may point at carries `:at`, its offset in the text, so the message can name
the line.

The grammar must read every statement exactly one way. Instaparse doesn't fail on an ambiguous
grammar, it picks one reading, so `test/pine/sql/parser_test.clj` checks `insta/parses` returns one
tree for every statement in its corpus.

A script is split on each `;` outside a string, a quoted name or a comment (`split-script`). The
statement that converts is read from the whole text with everything else blanked to spaces (`mask`),
so offsets and lines still point into what was pasted.

When the reading stops inside `ROW_NUMBER() OVER (...)`, the message quotes the `PARTITION BY` or
`ORDER BY` item it stopped in: "ORDER BY d.created_at + 1 inside ROW_NUMBER() isn't in Pine:
arithmetic isn't."

### Converting (`pine.sql.transpile`)

`script-statement` picks the statement and warns about the rest. `transpile-statement` turns it
into entries, `{:name :query}`, one per block: the `WITH` queries, the subqueries, and the statement
itself last, with no name. An `UPDATE` or `DELETE` becomes the query that picks its rows, with an
`:action` (`write->query`). Then, in order:

1. `flatten-query` makes each subquery in `FROM`, `JOIN` or after `IN` an entry. After `IN`, the
   condition becomes `{:op :in-set}`, Pine's `in` with a named result. `EXISTS` first becomes an
   `IN` (`exists->in`), and its `:in-set` is marked `:exists`.
2. `exists-guards` adds NULL guards to each `:exists` set under a `NOT`, unless `not-null?` finds
   the columns `NOT NULL`.
3. `narrow-in-sets` uses a `WITH` query itself after `IN`, when that is all it's used for.
4. `semi-joins` turns a `LEFT JOIN` to distinct values, tested only with `IS [NOT] NULL`, into `in`,
   marking each test `:guard` when `not-null?` can't show both columns are `NOT NULL`.
5. `anti-joins` turns `LEFT JOIN k ... WHERE k.c IS NULL` into `not in`, with NULL guards unless
   `not-null?` finds both columns `NOT NULL` in the schema.
6. `drop-unused-windows` leaves out a `ROW_NUMBER()` no later query uses.
7. `plan-windows` finds what every user of a `ROW_NUMBER()` keeps (`rn-limit`), removes that test
   from them, and records N on the numbered entry.

Each entry is then one block (`transpile-query`), with the earlier entries' names and columns in
`*ctes*`, so they read as tables:

1. `plan-tables` checks each name, finds each join's anchor table, and turns its `ON` into column
   pairs.
2. `table-ops` lays out the table steps, adding `from:` where a join's anchor isn't the current table.
3. `plan-where` turns `WHERE`, and any `ON` conditions moved out of inner joins, into clauses: `nnf`
   pushes `NOT` down, `cnf` distributes `OR`.
4. `plan-select` chooses `select:`, `group:` or `count:`, then `order:` and `limit:`.
   `plan-aggregated` does every `GROUP BY`. `plan-plain` selects an expression in `ROW_NUMBER()` as a
   computed column.
5. `expr-pine` writes a SQL value as a Pine computed value, and `conditions-pine` a condition used in
   one. Both work on the tables in scope; after the `group:` of `plan-aggregated`, on its result.
6. `render` writes Pine text. `pine.parser/prettify` lays it out one operation per line, and
   `pine.parser/parse` checks it reads back. A named block gets `|= name` after its last operation.
   With a connection, `build-error` builds each block, after the ones before it, as `/build` would.

A statement nested too deeply overflows the stack, in the reader or here. That is caught and refused.

A column is `{:t <table index> :name <column>}` until it is rendered, so whether it needs an alias is
decided once, at the end.

### Tests

`test/pine/sql/transpile_test.clj`:

- Each case in the tables above, compared as parsed Pine, so layout doesn't matter.
- Each refusal's message.
- `WITH`, subqueries, top N per group, rows with no match, `UPDATE`, `DELETE` and scripts, each on
  its own.
- The script that motivated them: an agent's `UPDATE` that soft-deletes duplicate documents. It
  converts in full, as written, with a warning for `BEGIN`/`COMMIT`, the check after it, and an unused
  `ROW_NUMBER()`.
- `test/pine/sql/postgres_equivalence_test.clj` runs that script on a real Postgres when
  `PINE_TEST_POSTGRES_URL` names a server. It creates a database of its own with a fixture for every
  rule (duplicates by reference and by attachment, edits that count and ones that don't, archived and
  already deleted documents, another tenant), and checks that the SQL's `UPDATE` and the Pine pick the
  same ids. It drops the database after.
- That an agent's read-only run refuses a converted `UPDATE`.
- A fixed point: for a list of Pine expressions, computed values among them, Pine's SQL converts back
  to Pine that produces the same SQL. Pine's SQL with `WITH` is checked to convert back and build.
- `test/pine/sql/review_test.clj`: each finding of a review of the converter, with the reviewer's
  input.
- The normal form under SQL's three-valued logic: random `AND`/`OR`/`NOT`/`IS DISTINCT FROM` trees,
  evaluated before and after, for every combination of true, false and NULL.

## Later

Planned in `beamlynx-plans/pending/2026-10-08-sql-to-pine.md`:

- With a connection: resolve an unqualified column in a join from the schema, and write a join that
  matches a foreign key as `.column` instead of explicit pairs. Names already take the database's
  spelling.
- `NOW() - INTERVAL '1 minute'` and other date arithmetic.
- More functions, and `ORDER BY` an expression that isn't selected.
- Date buckets (`DATE_TRUNC`) as `=> month` and the like.
