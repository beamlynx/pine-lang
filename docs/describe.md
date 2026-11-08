# Describe

`describe:` lists a table's columns, the tables it joins to and the tables that join to it. It reads the schema Pine already holds for the connected database. Nothing runs on the database.

## Why

To write a query, you need to know a table's columns and how it connects to other tables. Hints give that one step at a time, while typing. `describe:` gives it all at once, as a result you can read, sort and copy.

It is also how an AI agent learns a table. An agent connected through Beamlynx's MCP server runs `user | describe:` the way it runs any other query, instead of asking for one hint after another.

## Syntax

```
table | describe:
table | other_table | describe:
named_result | describe:
```

`describe:` always follows a `|`, like other operations. It describes the table the pipe left off at: after joins, the table joined last. Nothing meaningful follows it.

## The result

One row per column, then one per join, then when the schema was read. The result has five columns:

| Column | What it holds |
|---|---|
| `kind` | `column`, `joins to`, `joined from` or `schema` |
| `name` | The column's name, or the other table's |
| `type` | The column's type as the database reports it. Empty for a join. |
| `detail` | For a column: `primary key`, `nullable`, `masked`. For a join: how Pine knows it, and the columns it joins on. |
| `pine` | What to type: the column under its alias, or the join to pipe in next |

```
company | describe:
```

| kind | name | type | detail | pine |
|---|---|---|---|---|
| column | id | integer | primary key | `c_0.id` |
| column | name | text | | `c_0.name` |
| joined from | person | | foreign key: person.company_id -> company.id | `\| person .company_id` |
| schema | read | | Read at 2026-11-08T22:04:26Z. Reindex the connection after the schema changes. | |

- **`joins to`** is a table this one holds a key of: `person | describe:` lists `company`.
- **`joined from`** is a table that holds a key of this one.
- **How Pine knows a join:** `foreign key` (declared in the database), `inferred` (from a column's name, see [joins](joins.md)), `same rows` (a named result and its own table) or `named with as`.
- **The joins are the ones hints offer after `| `.** So what `describe:` lists is what a pipe can do.

### Named results

On a named result, `describe:` lists the columns it exposes, under the names it exposes them by:

```
company | select: id |= x

x | describe:
```

lists only `x.id`. No database can say this: a named result exists only in Pine.

### The masking policy

A column the masking policy masks says `masked` in `detail`. Its name still shows: the policy masks values, not names. See [access-policy.md](access-policy.md).

### When the schema was read

Pine reads a database's schema when it connects, and keeps it in memory. After a migration, what it knows is out of date until the connection is reindexed. The last row says when it was read.

## Implementation

| File | Role |
|---|---|
| `src/pine/pine.bnf` | `DESCRIBE`, the `describe:` operation |
| `src/pine/parser.clj` | Parses it to `{:type :describe}` |
| `src/pine/ast/main.clj` | Adds nothing to the state: `:current` stays the table to describe |
| `src/pine/describe.clj` | Builds the rows from the references map and the relation hints |
| `src/pine/eval.clj` | `run-query` returns those rows without touching the database |
| `src/pine/db/main.clj` | Records `:read-at` when a schema is read |
