# Variables

A `$name` in an expression, whose value is sent along with the request instead of written into the expression.

> Before 2026-10, "variable" meant `expr |= name`. Those are now called **named results**; see
> [named-results.md](named-results.md). A named result is a table you made. A variable is a value you plug in.

## Why

A query you'll run again usually differs only in a value or two: which company, which tenant, since when. Writing
the value into the text means editing the query each time, and anything that edits it (a saved recipe, an AI agent)
has to do so safely. A variable keeps the query fixed and moves the value out of it:

```
company | where: name = $company_name
```

The value travels as a SQL parameter, like any literal Pine writes, so it can never change the shape of the query.

## Syntax

`$` followed by a name made of letters, digits and underscores. A variable can go anywhere a literal value can:

| Where | Example |
|---|---|
| After a comparison | `where: name = $company_name`, `where: created_at > $since`, `where: title ilike $pattern` |
| With a cast | `where: id = $id ::text` |
| As the list for `in` | `where: tenant_id in $tenant_ids`, `where: status not in $done` |
| In `update!` | `update! status = $status` |

Not after `is` or `is not`: those only take `null`, and a variable can't be null.

## Values

The request's `variables` field maps each name to `{"value": ...}`:

```json
{
  "expressions": ["request | where: tenant_id in $tenant_ids | where: status = $status"],
  "variables": {
    "tenant_ids": {"value": [17, 23]},
    "status":     {"value": "failed"}
  }
}
```

- **A value is a string, a number or a boolean.** With `in`, it's a list of them. `null` is refused: to match nulls,
  write `is null`.
- **It's typed by its column, like a literal.** `"7"` for an integer column is the number 7. A string for a date
  column is a date.
- **It's bound as a parameter, never pasted into the SQL.** A value like `x' or 1=1 --` is just that string.

## Examples

### A value

```
company | where: name = $company_name
```

With `{"company_name": {"value": "Acme"}}`, this runs exactly like `company | where: name = 'Acme'`.

### A list

```
request | where: tenant_id in $tenant_ids
```

With `{"tenant_ids": {"value": [17, 23]}}`: `WHERE "r_0"."tenant_id" IN (?, ?)`.

### In an earlier block

A variable can be used in any block of the request, including one that defines a named result:

```
company | where: name = $company_name |= acme

acme | employee
```

## What comes back

- **`/build` never fails because a value is missing.** A query that's still a template keeps its hints and its SQL
  preview, which shows `$name` where the value goes. The response says which variables are used, which have no
  value, and which are used with `in` and so take a list:

  ```json
  "variables": {"used": ["company_name", "tenant_ids"], "unbound": ["tenant_ids"], "lists": ["tenant_ids"]}
  ```

- **`/eval` refuses to run with a value missing**, before anything reaches the database:

  ```json
  {"error-type": "unbound-variable", "error": "No value for $status.", "unbound": ["status"]}
  ```

## Errors

| Case | Message |
|---|---|
| A list where one value goes | "$c is a list, but it's used where one value goes. Use \`in $c\`." |
| One value with `in` | "$c is used with \`in\`, so it needs a list of values, like ["a", "b"]." |
| An empty list | "$c is an empty list. \`in\` needs at least one value." |
| `null` | "$c has no value. A variable can't be null; to match nulls, write \`is null\`." |
| After `is` | "A $variable can't follow \`is\`." |
| A malformed `variables` field | Error type `variables`, with a message saying what's wrong |

Limits: 50 variables per request, 5,000 values in a list, 10,000 characters in a string.

## Not yet

Binding a variable to another Pine query, so one query's result feeds the next, is planned. Passing
`{"expression": ...}` today is refused with a message that says so. See
`beamlynx-plans/pending/2026-10-03-pine-variables.md`.

## Implementation

- **Grammar** (`pine.bnf`): `variable := <"$"> name`, accepted where a value goes in `condition-default`,
  `condition-in` and `update-assignment`. A half-typed `name = $` parses as a partial condition, so hints keep working
  while the name is typed.
- **Parsing** (`parser.clj`): a variable becomes `{:type :variable :value name}`, with `:list true` after `in`.
- **Binding** (`variables.clj`): `api.clj`'s `generate-state` calls `bind` on each parsed expression before the AST
  sees it. Every variable with a value becomes the literal it stands for (`{:type :string}`, `{:type :number}`, …), so
  the rest of Pine (column typing in `where.clj`, `?` parameters in `eval.clj`) treats it exactly like a literal.
- **Unbound**: a variable with no value stays `{:type :variable}`. `where.clj` and `update_action.clj` skip column
  typing for it, `eval.clj` renders it as one `?`, and `formatted-query` shows it as `$name`.
- **Requests**: the `/build` and `/eval` routes check the `variables` field (`variables/normalize`) and bind it for
  the request. `/eval` checks for missing values first.
