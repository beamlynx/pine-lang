# Describe

`describe:` lists a table's columns: one row each, with its type and whether it is the key, may be empty or is masked. It reads the schema Pine already holds for the connected database. Nothing runs on the database.

To see how the table connects to others, use [`references:`](references.md).

## Why

To write a query, you need to know a table's columns. Hints give them one step at a time, while typing. `describe:` gives them all at once, as a result you can read, sort and copy.

It is also how an AI agent learns a table. An agent connected through Beamlynx's MCP server runs `user | describe:` the way it runs any other query, instead of asking for one hint after another.

## Syntax

```
table | describe:
table | other_table | describe:
named_result | describe:
```

`describe:` always follows a `|`, like other operations. It describes the table the pipe left off at: after joins, the table joined last.

It ends the query for now. Its rows come from the schema, not from SQL, so nothing can follow it: `user | describe: | where: key = 'primary'` is refused with "describe: ends a query for now".

## The result

One row per column, with five columns:

| Column | What it holds |
|---|---|
| `column` | The column's name |
| `type` | Its type, as the database reports it |
| `key` | `primary` for a column of the primary key, otherwise empty. Each column of a key of several columns says `primary`. |
| `nullable` | `yes` when it may be empty. A key column never is. |
| `masked` | `yes` when the masking policy masks it |

```
company | describe:
```

| column | type | key | nullable | masked |
|---|---|---|---|---|
| id | integer | primary | | |
| name | text | | | |
| email | text | | yes | yes |

### Named results

On a named result, `describe:` lists the columns it exposes, under the names it exposes them by:

```
company | select: id |= x

x | describe:
```

lists only `id`. No database can say this: a named result exists only in Pine.

### The masking policy

A masked column says `yes` under `masked`. Its name still shows: the policy masks values, not names. See [access-policy.md](access-policy.md).

### When the schema is out of date

Pine reads a database's schema when it connects, and keeps it in memory. After a migration, what it knows is out of date until the connection is reindexed.

## Implementation

| File | Role |
|---|---|
| `src/pine/pine.bnf` | `DESCRIBE`, the `describe:` operation |
| `src/pine/parser.clj` | Parses it to `{:type :describe}` |
| `src/pine/ast/main.clj` | Adds nothing to the state: `:current` stays the table to describe |
| `src/pine/describe.clj` | `describe-rows` builds the rows from the references map; `check-ends-query` refuses anything after it |
| `src/pine/eval.clj` | `run-query` returns those rows without touching the database |
