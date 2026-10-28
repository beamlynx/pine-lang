# Masking policy

A masking policy decides which column values a query may return. A column the policy masks comes back as the text `xxxxx`. The real value is replaced in the SQL itself, so it never leaves the database.

beamlynx calls it a masking policy. The API and the code still call it an access policy (`access-policy`), and so does older text. Both names mean the same thing.

## Why

beamlynx lets an AI agent query a database. The owner decides what the agent may see. Pine holds no policy of its own: the caller sends the rules with each request, and Pine applies them. beamlynx-desktop stores each connection's policy.

## What it protects: output, not inference

The policy is **output masking**. It stops the contents of a masked column from reaching whoever sent the query. It does not stop that query from *using* the column.

- **Allowed:** a masked column in `where:`, `order:`, `limit: N per` and joins. The value it is compared with comes from the query, not from the database. An agent that already knows an email may ask which rows hold it, even though `email` is masked.
- **Masked:** a masked column in `select:`, and any computed value that reads one.
- **Refused:** the two other places a masked value would come back.
  - A `group:` key, because the key is a column of the result.
  - A `::uuid` cast of a masked column, because Postgres's error for a value that isn't a uuid prints that value.

This is a deliberate choice, and not an oversight to fix. The other approach is **inference control**, which is what Postgres's row-level security and its leakproof functions aim for. It closes every way a value could be worked out: through filters, row counts or joins. A masking policy doesn't. An agent could rebuild a masked email by asking `where: email like 'a%'`, then `'ab%'`, and so on. Each of those queries opens in the owner's tab. SQL Server's *Dynamic Data Masking* draws the line in the same place.

When a change to the policy is considered, ask whether a masked value can come back in the result or in an error. Filtering, sorting and joining on a masked column are fine.

## Sending a policy

`/api/v1/build` and `/api/v1/eval` take two optional parameters:

- `access-policy`: a list of rules. Absent or empty means no policy: every value is returned.
- `access-policy-mode`: `"enforce"` (the default) or `"annotate"`. Only the exact string `"annotate"` annotates.

**Enforce** masks what the rules hide. It also refuses a `group:` key on a masked column, and a `::uuid` cast of one (see [What it protects](#what-it-protects-output-not-inference)). Filtering, sorting and joining on a masked column run.

**Annotate** hides nothing and refuses nothing. It only reports how the rules would treat each column (see below). beamlynx uses it in the owner's own tab, to mark the columns an agent can't see.

## Rules

A column is hidden unless a **show** rule matches it. A **hide** rule overrides every show rule.

The order of checks:

1. A hide rule matches: hidden.
2. A show rule matches: shown.
3. Otherwise: hidden.

### Show rules

| Rule | Shows |
|------|-------|
| `{"type": "column-type", "allow": ["integer", "uuid"]}` | Columns whose database type is in the list. |
| `{"type": "foreign-key"}` | Columns that join to another table: a real foreign key, or a name like `company_id`. |
| `{"type": "column-name", "suffix": "_id"}` | Columns whose name ends with, contains, or equals a text. Use one of `suffix`, `contains` or `equals`. Case is ignored. |
| `{"type": "column", "schema": "public", "table": "user", "column": "email"}` | One column of one table. Without `schema`, the table in any schema. |

### Hide rules

A hide rule wraps one of the show rules above as its `match`:

```json
{"type": "hide", "match": {"type": "column-name", "contains": "token"}}
{"type": "hide", "match": {"type": "column", "schema": "public", "table": "user", "column": "email"}}
```

Hiding is its own rule type, not a flag on the show rules. A version of Pine that doesn't know `hide` ignores the rule. The column then stays hidden unless a show rule matches. If hiding were a flag on a show rule, an older Pine would read the rule as a show rule and return the value.

A hide rule whose `match` Pine doesn't recognise hides every column. For a hide rule, the safe way to fail is to hide.

## What is always shown, and always hidden

- Postgres's catalog views used to discover the schema (`information_schema.columns`, `pg_catalog.pg_class` and others) are always shown, even when a hide rule matches. They hold names, not values.
- A primary key column called `id` is shown, and can be a `group:` key, unless a hide rule matches it. Pine adds it to each row so the row can be edited. An `id` column that isn't the primary key is checked like any other.
- A computed value is hidden when any column it reads is hidden.
- A column whose type Pine can't tell is hidden.
- A table missing from the schema index is refused when its columns are selected with `*`, not returned unhidden.
- A column the schema index doesn't have, of a table it does have, is refused. Re-index the connection after adding a column.

## Names and case

The rules judge each column and table by the name the schema index has for it. MySQL and SQLite ignore case in names, so there `CUSTOMER_ID` is the column `customer_id`, and a rule about `customer_id` covers it. A `column` rule's own names are compared ignoring case there too. Postgres doesn't ignore case, since Pine quotes every name: `CUSTOMER_ID` is a different column, and refused if the table has no such column.

On SQLite, a column's type is the type it was declared with. Integer, real, decimal, text and blob names are reduced to `integer`, `real`, `numeric`, `text` and `blob`. Any other declared type keeps its own name in lowercase, such as `string` or `jsonb`. A `column-type` rule matches that name.

## The report on each column

When rules are sent, each column in `/eval`'s `columns` and `/build`'s `ast.columns` has a `policy` entry:

```json
"policy": {
  "hidden": true,
  "by": {"type": "hide", "match": {"type": "column-name", "contains": "token"}},
  "table": {"schema": "public", "table": "user"}
}
```

- `hidden`: whether the policy hides it.
- `by`: the rule that decided. Absent when no rule matched, so the column is hidden by default.
- `table`: the column's real table, which a `column` rule would name. `schema` is `null` when the table's name is in several schemas and none of them is `public`.

A computed value has `hidden` only. A column of a named result or a checkpoint has no `policy` entry. Its query was checked where it was built.

## A refused query

A query that uses a masked column as a `group:` key, or casts one with `::uuid`, is refused with `error-type: "policy"`. The error also says which column, in `hidden-column`:

```json
"hidden-column": {
  "alias": "a_0",
  "column": "type",
  "hidden": true,
  "table": {"schema": "public", "table": "activity"}
}
```

It has `by`, the rule that hid it, when a hide rule did. Without `by`, no Allow rule matched, so an owner can allow the column with a `column` rule. With a hide rule in `by`, a `column` rule wouldn't help: the hide rule always wins.

A query that names a column the schema index doesn't have is refused with `error-type: "policy"` too, but has no `hidden-column`: there is no column to allow.

When Pine can't tell which schema an unqualified table is in, a `column` rule with a `schema` matches it as a hide rule and doesn't match it as a show rule. The doubt keeps the column hidden.
