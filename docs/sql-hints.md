# SQL hints

What beamlynx's SQL editor completes from, while SQL is typed: table and column names, and the joins
after `JOIN`.

## Tables and columns

```
POST /api/v1/sql-schema
{"connection-id": "..."}
```

```json
{"dialect": "postgres",
 "default-schema": "public",
 "tables": [{"schema": "public", "table": "company", "columns": ["id", "name"]}]}
```

`connection-id` is optional. Without it, the selected connection is used. With no connection, or one
whose schema can't be read, `tables` is empty. That isn't an error, because the editor asks as soon
as it opens.

- `dialect` is `postgres`, `mysql` or `sqlite`. The editor uses it to quote names: a name like
  `tenantId` needs quotes in Postgres.
- `default-schema` is the schema an unqualified table is in: `public` in Postgres, and the one schema
  of a MySQL or SQLite connection. The editor offers those tables without a schema.
- Columns are in the table's order.

The endpoint is internal to beamlynx. It isn't on the MCP surface.

## Joins

```
POST /api/v1/sql-join-hints
{"query": "select * from company c join ", "cursor": 29, "connection-id": "..."}
```

```json
{"joins": [{"sql": "employee e ON e.company_id = c.id",
            "schema": "public", "table": "employee", "resolution": "fk"}]}
```

When the SQL before `cursor` ends in `JOIN` and a partial name, these are the tables that join to the
tables already in the query, with the `ON` clause written. `sql` is the text that goes after `JOIN`.
Anywhere else, `joins` is empty.

`cursor` is an offset into `query`, counted the way JavaScript and Java count string length. With no
connection, `joins` is empty.

- **Which joins:** Pine's own join hints. `resolution` is `fk` for a foreign key, and `heuristic`
  for a join guessed from names like `company_id`. A key of several columns is joined on all of them.
- **Which tables:** every table in scope at the cursor, the last one first: after `FROM`, `JOIN` or
  `UPDATE`, before the cursor, in the same query as the cursor. A table inside a subquery the cursor
  isn't in doesn't count, and neither does one in the query around it. The `ON` clause refers to each
  by its alias, or by its name when it has none.
- **Names:**
  - The new table gets an alias of its words' initials (`line_item` is `li`), with a number when
    that is taken.
  - A name is quoted when the database needs it to be: mixed case or a reserved word in Postgres,
    like `"order"`. MySQL uses backquotes.
  - The schema is left out when it's the default one.

### How it reads the SQL

The SQL is unfinished while it is typed, so it is scanned, not parsed. Comments and strings are
blanked out first, so `JOIN` inside them doesn't count. A table is the name after `FROM`, `JOIN` or
`UPDATE`, with an optional schema and alias. A word like `ON`, `WHERE` or `SET` is never taken as an
alias, and the `FROM` of `IS DISTINCT FROM` names no table. Parentheses mark the queries: a table
counts only when it is inside the same parentheses as the cursor.

For each table, the endpoint builds the Pine `schema.table | ` and asks for its join hints. That is
the same code that suggests joins in the Pine editor. It writes each hint back as SQL.

The implementation is in `src/pine/sql/hints.clj`.
