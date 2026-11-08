# References

`references:` lists the relations that touch a table, both ways: the keys it holds of other tables, and the keys other tables hold of it. Each row is one relation, as Pine holds it. It reads the schema Pine already holds for the connected database. Nothing runs on the database.

To see a table's columns, use [`describe:`](describe.md).

## Why

Pine joins tables through relations: a foreign key the database declares, or one Pine infers from a column's name (see [joins](joins.md)). Hints show them one step at a time, after `| `. `references:` shows every relation of a table at once, with how Pine knows it, so you can see why a join goes the way it does.

It is also how an AI agent learns how a table connects. It runs `user | references:` the way it runs any other query.

## Syntax

```
table | references:
table | other_table | references:
named_result | references:
```

`references:` always follows a `|`. It describes the table the pipe left off at: after joins, the table joined last.

It ends the query for now. Its rows come from the schema, not from SQL, so nothing can follow it: `user | references: | where: how = 'naming'` is refused with "references: ends a query for now".

## The result

One row per relation, with four columns:

| Column | What it holds |
|---|---|
| `from` | The side that holds the key: `table.column`, or `table.(a, b)` for a key of several columns |
| `to` | The side the key points at, the same way |
| `how` | How Pine knows the relation: `foreign key` when the database declares it, `naming` when Pine infers it from a column's name |
| `pine` | What to type after `| ` to follow the relation from this table, as the hints offer it |

```
person | references:
```

| from | to | how | pine |
|---|---|---|---|
| person.company_id | company.id | foreign key | `\| company .company_id :parent` |
| note.person_id | person.id | naming | `\| note .person_id` |

- **Both ways.** The first row is a key `person` holds. The second is a key `note` holds of `person`.
- **A table that refers to itself** (`employee.reports_to` → `employee.id`) shows twice, once each way, with the Pine for each.
- **`pine` can be empty**, where no hint follows the relation. A named result that doesn't expose the key's column is one case.

### A new way of finding relations

The rows are built straight from the relation records in Pine's schema index (`pine.db.references`), not from a list of kinds. `how` names each record's kind. A new way of finding relations shows up as it is, under its own name, with no change to this result.

### Named results

On a named result, `references:` lists the relations of the table behind it.

### The masking policy

The masking policy masks values, not names, so every relation is listed. See [access-policy.md](access-policy.md).

## Implementation

| File | Role |
|---|---|
| `src/pine/pine.bnf` | `REFERENCES`, the `references:` operation |
| `src/pine/parser.clj` | Parses it to `{:type :references}` |
| `src/pine/ast/main.clj` | Adds nothing to the state: `:current` stays the table to describe |
| `src/pine/describe.clj` | `references-rows` builds the rows from the relation records, and matches each to the hint that follows it |
| `src/pine/eval.clj` | `run-query` returns those rows without touching the database |
