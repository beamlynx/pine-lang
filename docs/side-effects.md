# Side effects

Which operations change data, and how a caller refuses to run them.

## Why

Most of Pine reads. Two operations write: `delete!` and `update!`. The grammar
marks them with `!`, which tells a person reading an expression what it does —
but only a person. A program that needed the same answer had to match the
expression text itself.

That is fragile in a way that matters, because the callers who need this answer
are exactly the ones where being wrong is expensive. beamlynx's MCP server runs
Pine on behalf of an AI agent, and an agent must not change the database. It used
to enforce that with a regular expression looking for `delete!` — which silently
missed `update!`, and missed the `d!` and `u!` short forms too.

So the engine answers the question instead. It parses the expression anyway; it
already knows which operations are in it.

## What counts as a write

| Operation | Short form |
| --- | --- |
| `delete!` | `d!` |
| `update!` | `u!` |

That is the whole list. Everything else — `select:`, `where:`, `order:`,
`limit:`, `group:`, `count:`, `from:`, `? table`, a join, a `|= name`
assignment — only reads.

`delete!` and `update!` are the operations that carry a `!`, and that is not a
coincidence: the mark and the meaning are the same fact. If an operation writes,
it has a `!`; if it has a `!`, it writes.

### Which rows `delete!` removes

`delete!` names the column (or columns) that identify the rows to remove, and
the DELETE matches them against the same columns selected by the expression it
is piped onto:

```
company | where: id = 1 | employee | delete! .id
```

```sql
DELETE FROM "employee" WHERE "id" IN (
  SELECT "e_1"."id" FROM "company" AS "c_0"
    JOIN "employee" AS "e_1" ON "c_0"."id" = "e_1"."company_id"
  WHERE "c_0"."id" = ? )
```

Name several columns, comma-separated, and they are matched as a row:

```
k.case | where: id = 1 | k.case_ref | delete! .case_id, .search_id
```

```sql
DELETE FROM "k"."case_ref" WHERE ("case_id", "search_id") IN (
  SELECT "cr_1"."case_id", "cr_1"."search_id" FROM "k"."case" AS "c_0"
    JOIN "k"."case_ref" AS "cr_1"
      ON "c_0"."id" = "cr_1"."case_id" AND "c_0"."search_id" = "cr_1"."search_id"
  WHERE "c_0"."id" = ? )
```

This is what a table with a composite key needs. `case_ref` has no single
column that picks out one of its rows: `case_id` alone matches every reference
belonging to that case, and `search_id` alone matches references belonging to
*other* cases entirely. Either one deletes rows nobody asked to delete, and says
nothing about it. Naming both columns deletes exactly the rows the expression
selected.

At least one column is required — a bare `delete!` has nothing to match on and
does not parse.

### Which rows `update!` changes

`update!` has no equivalent. It always scopes by a single `id` column:

```sql
UPDATE "employee" SET "name" = ? WHERE id IN ( SELECT "e_1"."id" FROM ... )
```

So a table whose primary key is not a column called `id` — including one keyed
on several columns — cannot be updated through Pine. This is a separate
limitation from the one `delete!` just lost, and it is about the target's own
primary key rather than the foreign key it was reached by.

## Finding out: `writes`

Every `/api/v1/eval` response carries a `writes` boolean.

```json
{ "connection-id": "...", "writes": false, "result": [...], "columns": [...] }
```

It is true if **any** operation in the expression writes, not only the last one.
The last operation is what decides which query gets built, so
`company | delete! .id | select: name` builds a `SELECT` today and the `delete!`
does nothing. `writes` still reports `true`. A caller refusing writes should not
have to depend on that staying true.

## Refusing: `allow-writes`

Send `allow-writes: false` and the endpoint refuses a writing expression instead
of running it.

```json
POST /api/v1/eval
{
  "expressions": ["company | where: id = 1 | delete! .id"],
  "connection-id": "...",
  "allow-writes": false
}
```

```json
{
  "error-type": "write-refused",
  "error": "Refusing to run an expression that changes data: delete-action. This caller asked for read-only evaluation.",
  "writes": true
}
```

Nothing is executed. The refusal happens after the expression is built and before
the query runs, so the database is untouched.

**Leaving the field out means writes are allowed.** Every existing caller keeps
working, and a person running `delete!` in their own editor is doing it on
purpose. Only an explicit `false` turns writes off — a missing or malformed value
never reads as "refuse everything" for a caller that does not send it.

## What this does not do

It does not make a connection read-only. `allow-writes: false` is a property of
one request, not of the connection, so it protects the caller that asks for it
and nothing else.

If you want a connection that genuinely cannot be written to — and for a
connection an agent can reach, you probably do — give it a database role with no
`INSERT`/`UPDATE`/`DELETE` grant. Schema introspection reads `pg_catalog`, which
every role can read regardless of grants, so hints and completion still work
fully on a role with only `SELECT`.
