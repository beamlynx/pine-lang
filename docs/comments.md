# Comments

Notes in an expression, and the doc comment at the top of one.

## Why

An expression says what it fetches. It does not say what it was for — which tenants
count as churned, why a date is hardcoded, what question someone was actually
answering. That belongs next to the query, not in a ticket somewhere.

Pine has always accepted comments; they were simply thrown away, so the only place
one could be seen was the editor it was typed into. A comment at the top of an
expression is now a **doc comment**: the build endpoint returns its text, so a client
can render it as prose above the query instead of as grey text to scroll past.

This is what lets an agent explain itself. An agent writing a query through beamlynx's
MCP server leads with a doc comment saying what it is looking for, and the person
watching sees the reasoning next to the result rather than only the SQL.

## Syntax

Two comment styles, anywhere whitespace is legal:

```
-- to the end of the line

/* across
   as many lines
   as you like */
```

A comment is a **doc comment** when it is the first thing in the expression, before
any operation. Either style works — a `/* ... */` block, or a run of consecutive
`--` lines:

```
/* Tenants that signed up last month but never completed onboarding. */
tenant
 | where: created_at > '2026-08-01'
 | public.onboarding .tenantId
```

```
-- Tenants that signed up last month
-- but never completed onboarding.
tenant | public.onboarding .tenantId
```

A blank line ends a run of `--` lines. A bare `--` does not, so that is how you space
out paragraphs:

```
-- What this finds.
--
-- Why it is filtered this way.
tenant
```

## What comes back

`POST /api/v1/build` returns one field, `doc`: the comment at the top of the
**first** expression sent. One expression describes one thing, so there is one
description, even when the text is split into several blank-line-separated
blocks (see [expressions.md](expressions.md)) and a later block has a comment of
its own. A comment on a later block is still a comment; it just isn't the
description of the whole.

The text is cleaned for display, not returned verbatim: comment delimiters removed,
a javadoc-style leading `*` stripped when every line has one, shared indentation
removed, surrounding blank lines dropped. So this:

```
/*
 * Tenants that never onboarded.
 *
 * Excludes the internal test tenant.
 */
tenant
```

comes back as:

```
Tenants that never onboarded.

Excludes the internal test tenant.
```

A comment containing only whitespace is not a doc comment.

## Prettifying

`prettified` (returned by both `/api/v1/build` and `/api/v1/eval`) rebuilds the
expression from the parsed operations, one per line. A doc comment is copied through
onto its own lines, exactly as written — prettifying an expression never loses it,
and prettifying an already-prettified expression leaves it unchanged.

Comments **between** operations are still dropped. They live in the whitespace
between two operations' text spans, which is not part of either one, so there is
nowhere to put them back. Attaching a comment to the single operation it annotates
needs an anchor the expression does not currently have.

## Implementation

- `src/pine/pine.bnf` — `ws` already accepts `line-comment` and `block-comment`
  anywhere whitespace is legal. Unchanged by this feature.
- `src/pine/parser.clj` — `extract-doc` reads the leading comment off the raw
  expression string and returns `{:text <cleaned> :end <offset past the comment>}`.
  It does not go through the grammar: the grammar is already correct about comments,
  it only hides them, and a new top-level rule would have to compete with `OPERATION`
  for the same leading position. `prettify` uses `:end` to copy the comment through.
- `src/pine/api.clj` — `api-build` adds `doc` from the first expression. It reads
  `extract-doc` directly rather than going through the generated state, since the
  state describes the *last* expression and this field describes the tab.
