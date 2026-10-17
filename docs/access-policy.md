# Access policy

An access policy decides which column values a query may return. A column the policy hides comes back as the text `xxxxx`. The real value is replaced in the SQL itself, so it never leaves the database.

## Why

beamlynx lets an AI agent query a database. The owner decides what the agent may see. Pine holds no policy of its own: the caller sends the rules with each request, and Pine applies them. beamlynx-desktop stores each connection's policy.

## Sending a policy

`/api/v1/build` and `/api/v1/eval` take two optional parameters:

- `access-policy`: a list of rules. Absent or empty means no policy: every value is returned.
- `access-policy-mode`: `"enforce"` (the default) or `"annotate"`. Only the exact string `"annotate"` annotates.

**Enforce** hides what the rules hide. It also refuses a query that names a hidden column in `where:`, `order:`, `group:` or after `per`. Filtering on a hidden column would reveal it through row counts.

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
- The `id` column Pine adds to each row so it can be edited is shown, unless a hide rule matches it.
- A computed value is hidden when any column it reads is hidden.
- A column whose type Pine can't tell is hidden.
- A table missing from the schema index is refused, not returned unhidden.

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

When Pine can't tell which schema an unqualified table is in, a `column` rule with a `schema` matches it as a hide rule and doesn't match it as a show rule. The doubt keeps the column hidden.
