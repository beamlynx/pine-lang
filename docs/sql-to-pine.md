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
| `internal` | The converter produced Pine that doesn't parse. That is a bug here; the message includes the Pine. |

Nothing runs, so no connection is needed and no access policy applies. With one, names take the
database's spelling (see [Names and case](#names-and-case)). The request may carry `connection-id`;
without it, the selected connection is used. A connection that can't be read is skipped, never an
error.

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
| `<>`, `!=`, `<`, `>`, `<=`, `>=` | the same, `<>` as `!=` |
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
| `SELECT a, COUNT(*) FROM t GROUP BY a` | `t \| group: a => count` |
| `ORDER BY a, b DESC` | `order: a asc, b desc` |
| `LIMIT 10` | `limit: 10` |
| `$name` | `$name`, a Pine variable |
| `x IS DISTINCT FROM 'v'` | `where: x != 'v' or x is null` |
| `x IS NOT DISTINCT FROM 'v'` | `where: x = 'v' \| where: x is not null` |
| `x IS DISTINCT FROM NULL` | `where: x is not null` |
| `WITH a AS (...) SELECT ... FROM a` | a block ending in `\|= a`, then `a \| ...` |
| `FROM (SELECT ...) x` | a block ending in `\|= x`, then `x \| ...` |
| `x IN (SELECT c FROM t ...)` | a block for the subquery, then `where: x in <block>` |
| `LEFT JOIN k ON k.c = t.x WHERE k.c IS NULL` | `where: x not in k` |
| `ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn`, kept with `rn = 1` or `rn <= N` | `order: o \| limit: N per p` |
| `SELECT DISTINCT ON (p) ... ORDER BY p, o` | `order: p, o \| limit: 1 per p` |
| `UPDATE t SET c = 'v' WHERE ...` | `t \| where: ... \| update! c = 'v'` |
| `DELETE FROM t WHERE ...` | `t \| where: ... \| delete! .id`, by the table's primary key |
| `BEGIN; ...; COMMIT;` | the statement inside, with a warning |

### Order

Pine's `order:` sorts descending when no direction is given, and SQL ascending. So every `order:`
column is written with its direction.

### Where the operations go

Tables and joins first, in the SQL's order. Then one `where:` for each AND-ed condition. Then `select:`,
`group:` or `count:`. Then `order:` and `limit:`. Pine builds one SQL `SELECT` with every join and
every condition in it, so a `where:` after the joins means the same as SQL's single `WHERE`.

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

`DISTINCT` doesn't change which values a set holds, so it is dropped from a query that is only used
after `IN`. Anywhere else it is refused.

A `WITH` query's columns are what a later query sees. So a conversion that would change them, the
ones that are warnings in the statement's own query (`COUNT(*) AS n` named `count`, the count a
`group:` adds), is refused inside a `WITH` query.

### Rows with no match

This SQL keeps the rows of `t` that have no row in `k`:

```sql
SELECT t.* FROM t LEFT JOIN k ON k.c = t.x WHERE k.c IS NULL
```

In Pine that is `where: x not in k`, with `k` selecting only `c`. It converts that way when the
`ON` is that one pair, the `IS NULL` is on `k.c` and stands on its own in the `WHERE`, and nothing
else uses `k`. Otherwise the `LEFT JOIN` converts as a join.

`NOT IN` treats NULL differently from the join. When `t.x` is NULL, the join keeps the row and
`NOT IN` drops it. When `k.c` holds a NULL, `NOT IN` matches nothing at all. So unless the schema
says both columns are `NOT NULL`, the condition is `x is null or x not in k`, and `k` leaves out
its NULLs with `where: c is not null`. The schema comes with the connection. Through `WITH`
queries, a column is `NOT NULL` when the table column it comes from is, and no outer join makes it
NULL.

A `NOT IN (SELECT ...)` written in the SQL converts as it is: it has the same NULL rules as Pine's
`not in`.

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
- `PARTITION BY` and `ORDER BY` take columns. An expression, such as `(x IS NOT NULL)` or a `CASE`,
  is refused with its line.
- `rn` isn't a column of the result in Pine. A `SELECT *` over the numbered rows gets a warning.
- The numbered rows can't be joined with `LEFT JOIN` or `RIGHT JOIN`.

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
- `SET` takes values: a string, a number, true, false, NULL or a `$variable`. A function, like
  `NOW()`, or another column is refused.
- `delete!` names rows by the table's primary key, so converting a `DELETE` needs the connection.
  A table without a primary key is refused.
- A statement with no condition of its own is refused, as Pine refuses to change every row.

### Scripts

A script is split on `;`, but not on one inside a string, a quoted name, a comment or a
dollar-quoted body (`$$ ... $$`). `BEGIN`, `COMMIT` and `ROLLBACK` are left out, with a warning: Pine runs
each `update!` and `delete!` in a transaction of its own. A script that ends in `ROLLBACK` and changes
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
(`AS "__c_0__id"`) are left out, with a warning, since Pine adds them itself. Its `LIMIT 250` becomes
`limit: 250`. A preview that starts with `WITH`, which Pine writes for `count:`, `group:`, `|=`
names and checkpoints, converts to a block for each `WITH` query. A preview of `limit: N per` doesn't
convert yet.

### Warnings

A conversion can succeed with warnings. Each says how the Pine differs from the SQL without
changing which rows come back:

- `COUNT(*) AS n`: Pine names the count column `count`. In a `WITH` query, this and the next three
  are refused instead, since a later query may use the columns.
- `GROUP BY` without `COUNT(*)`, or a grouped column left out of the `SELECT`: Pine's `group:` returns
  the grouped columns and a count.
- A grouped column's alias is dropped.
- An inner join's `ON` condition moved to `where:`.
- Pine's hidden columns left out.
- `rn` from `ROW_NUMBER()` isn't a column of a `SELECT *` result.
- `BEGIN`, `COMMIT` and `ROLLBACK` left out.
- Another statement in the script left out, with its line.

## What doesn't convert

Each is refused with its line.

| SQL | Why |
|---|---|
| `INSERT`, `MERGE`, `CALL`, `DO`, DDL and other statements | Only `SELECT`, `UPDATE` and `DELETE` convert. |
| Two statements that change data | One at a time. |
| `ROLLBACK` after a change | Pine's `update!` keeps its change. |
| `DISTINCT`, other than in a set used after `IN` | Not in Pine. |
| `OFFSET`, `FETCH`, `HAVING`, `UNION`, `INTERSECT`, `EXCEPT` | Not in Pine. |
| `WITH RECURSIVE`, `MATERIALIZED`, `RETURNING` | Not in Pine. |
| A subquery other than in `FROM`, `JOIN` or after `IN` | Not in Pine. |
| A function, `CASE`, `EXISTS`, arithmetic | Not in Pine. |
| A window function other than `ROW_NUMBER()`, or one not kept with `rn <= N` | Not in Pine. |
| An expression in `PARTITION BY` or `ORDER BY` inside `ROW_NUMBER()` | Not yet. |
| `rn = 2`, `rn > 1`, or `rn` used any other way | Pine keeps the first N rows of each group. |
| `LIMIT` after `DISTINCT ON` | Remove it. |
| `NOW()` or another function in `SET` | Not yet. `update!` writes a value. |
| A column in `SET` | Not converted. |
| `UPDATE` or `DELETE` with no condition of its own | Pine refuses to change every row. |
| `DELETE` without a connection, or on a table with no primary key | `delete!` names rows by the key. |
| `IS DISTINCT FROM` another column | Compare with a value. |
| A cast in a join's `ON` | Pine joins on columns as they are. |
| `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(column)`, `COUNT(DISTINCT ...)` | Pine counts rows only. |
| A column next to `COUNT(*)` without `GROUP BY` | Needs `GROUP BY`. |
| `ORDER BY` or `LIMIT` on a single `COUNT(*)` | Remove them. |
| A selected column that isn't grouped | Pine selects the grouped columns and the count. |
| `ORDER BY 1`, `NULLS FIRST` | Name the column; no null ordering. |
| `FULL`, `CROSS` or `NATURAL` join, a comma between tables | Pine joins inner, left or right, on named columns. |
| A join tied to two earlier tables | Pine joins a table to one other. |
| `USING` after more than one table | It can't say which table it joins to. Write it with `ON`. |
| A column in a join that isn't qualified | It could belong to more than one table. |
| `= NULL` | Never true in SQL. Use `IS NULL`. |
| A cast other than to `text` or `uuid` | Pine casts a column to those two. |
| A comparison with no column | Pine conditions start with a column. |
| `$1`, `?` | Write the value, or a `$name` variable. |
| `LIMIT` over 10 000 | Pine's maximum. |

---

## Implementation

| File | What it does |
|---|---|
| `src/pine/sql/sql.bnf` | The SQL subset, as an Instaparse grammar: one statement. |
| `src/pine/sql/parser.clj` | Splits a script on `;`, reads a statement into plain maps, and names an unsupported word when the reading stops on one. |
| `src/pine/sql/transpile.clj` | Picks the statement, rewrites `WITH` queries and subqueries into blocks, maps each to Pine operations, renders them, and checks each block parses. |
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
`ORDER BY` item it stopped in: "ORDER BY (e.document_id IS NOT NULL) inside ROW_NUMBER() isn't in
Pine yet."

### Converting (`pine.sql.transpile`)

`script-statement` picks the statement and warns about the rest. `transpile-statement` turns it
into entries, `{:name :query}`, one per block: the `WITH` queries, the subqueries, and the statement
itself last, with no name. An `UPDATE` or `DELETE` becomes the query that picks its rows, with an
`:action` (`write->query`). Then, in order:

1. `flatten-query` makes each subquery in `FROM`, `JOIN` or after `IN` an entry. After `IN`, the
   condition becomes `{:op :in-set}`, Pine's `in` with a named result.
2. `narrow-in-sets` uses a `WITH` query itself after `IN`, when that is all it's used for.
3. `anti-joins` turns `LEFT JOIN k ... WHERE k.c IS NULL` into `not in`, with NULL guards unless
   `not-null?` finds both columns `NOT NULL` in the schema.
4. `plan-windows` finds what every user of a `ROW_NUMBER()` keeps (`rn-limit`), removes that test
   from them, and records N on the numbered entry.

Each entry is then one block (`transpile-query`), with the earlier entries' names and columns in
`*ctes*`, so they read as tables:

1. `plan-tables` checks each name, finds each join's anchor table, and turns its `ON` into column
   pairs.
2. `table-ops` lays out the table steps, adding `from:` where a join's anchor isn't the current table.
3. `plan-where` turns `WHERE`, and any `ON` conditions moved out of inner joins, into clauses: `nnf`
   pushes `NOT` down, `cnf` distributes `OR`.
4. `plan-select` chooses `select:`, `group: ... => count` or `count:`, then `order:` and `limit:`.
5. `render` writes Pine text. `pine.parser/prettify` lays it out one operation per line, and
   `pine.parser/parse` checks it reads back. A named block gets `|= name` after its last operation.

A column is `{:t <table index> :name <column>}` until it is rendered, so whether it needs an alias is
decided once, at the end.

### Tests

`test/pine/sql/transpile_test.clj`:

- Each case in the tables above, compared as parsed Pine, so layout doesn't matter.
- Each refusal's message.
- `WITH`, subqueries, top N per group, rows with no match, `UPDATE`, `DELETE` and scripts, each on
  its own.
- The script that motivated them: an agent's `UPDATE` that soft-deletes duplicate documents. The
  test asserts its first refusal, and that a version without the unsupported parts converts to the
  Pine a person would write.
- That an agent's read-only run refuses a converted `UPDATE`.
- A fixed point: for a list of Pine expressions, Pine's SQL converts back to Pine that produces the
  same SQL.
- The normal form under SQL's three-valued logic: random `AND`/`OR`/`NOT`/`IS DISTINCT FROM` trees,
  evaluated before and after, for every combination of true, false and NULL.

## Later

Planned in `beamlynx-plans/pending/2026-10-08-sql-to-pine.md`:

- With a connection: resolve an unqualified column in a join from the schema, and write a join that
  matches a foreign key as `.column` instead of explicit pairs. Names already take the database's
  spelling.
- Functions in `SET`, like `NOW()`, and expressions in `ROW_NUMBER()`'s `ORDER BY`.
- `COALESCE`, `FILTER`, `BOOL_OR` and `HAVING`.
- Date buckets (`DATE_TRUNC`) as `=> month` and the like.
