# SQL to Pine

Turn one SQL `SELECT` into a Pine expression that returns the same rows.

## Why

People arrive with SQL: from logs, dashboards, an ORM's debug output, a colleague's message. AI
agents write SQL well too. beamlynx
draws a Pine expression on its canvas, with its tables, joins and conditions, and lets you edit it by
clicking. Converting the SQL to Pine gives a pasted query the same view.

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
`limit: 250`. A preview that starts with `WITH`, which Pine writes for `count:`, `group:` and
checkpoints, doesn't convert yet.

### Warnings

A conversion can succeed with warnings. Each says how the Pine differs from the SQL without
changing which rows come back:

- `COUNT(*) AS n`: Pine names the count column `count`.
- `GROUP BY` without `COUNT(*)`, or a grouped column left out of the `SELECT`: Pine's `group:` returns
  the grouped columns and a count.
- A grouped column's alias is dropped.
- An inner join's `ON` condition moved to `where:`.
- Pine's hidden columns left out.

## What doesn't convert

Each is refused with its line.

| SQL | Why |
|---|---|
| `INSERT`, `UPDATE`, `DELETE`, DDL | Only a `SELECT` converts. Write `update!` and `delete!` in Pine. |
| More than one statement | One at a time. |
| `DISTINCT`, `OFFSET`, `FETCH`, `HAVING`, `UNION`, `INTERSECT`, `EXCEPT` | Not in Pine. |
| `WITH`, a subquery | Not yet. |
| A function, `CASE`, `EXISTS`, arithmetic, a window function | Not in Pine. |
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
| `src/pine/sql/sql.bnf` | The SQL subset, as an Instaparse grammar. |
| `src/pine/sql/parser.clj` | Reads SQL into plain maps. Names an unsupported word when the reading stops on one. |
| `src/pine/sql/transpile.clj` | Maps to Pine operations, renders them, and checks the result parses. |
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

### Converting (`pine.sql.transpile`)

1. `plan-tables` checks each name, finds each join's anchor table, and turns its `ON` into column
   pairs.
2. `table-ops` lays out the table steps, adding `from:` where a join's anchor isn't the current table.
3. `plan-where` turns `WHERE`, and any `ON` conditions moved out of inner joins, into clauses: `nnf`
   pushes `NOT` down, `cnf` distributes `OR`.
4. `plan-select` chooses `select:`, `group: ... => count` or `count:`, then `order:` and `limit:`.
5. `render` writes Pine text. `pine.parser/prettify` lays it out one operation per line, and
   `pine.parser/parse` checks it reads back.

A column is `{:t <table index> :name <column>}` until it is rendered, so whether it needs an alias is
decided once, at the end.

### Tests

`test/pine/sql/transpile_test.clj`:

- Each case in the tables above, compared as parsed Pine, so layout doesn't matter.
- Each refusal's message.
- A fixed point: for a list of Pine expressions, Pine's SQL converts back to Pine that produces the
  same SQL.
- The normal form under SQL's three-valued logic: random `AND`/`OR`/`NOT` trees, evaluated before
  and after, for every combination of true, false and NULL.

## Later

Planned in `beamlynx-plans/pending/2026-10-08-sql-to-pine.md`:

- With a connection: resolve an unqualified column in a join from the schema, and write a join that
  matches a foreign key as `.column` instead of explicit pairs. Names already take the database's
  spelling.
- Subqueries and `WITH` as named results.
- Date buckets (`DATE_TRUNC`) as `=> month` and the like.
