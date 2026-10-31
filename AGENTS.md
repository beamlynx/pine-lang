# Pine Project Instructions

## What is Pine?

Pine is a domain-specific language (DSL) for querying databases. It provides a concise, pipe-based syntax that compiles to SQL.

**Tech Stack**: Clojure 1.11.3, Instaparse (BNF grammar-based parsing), PostgreSQL, MySQL 8.0+ and SQLite (a local file, desktop only: see `PINE_SQLITE`) (MySQL 5.7 isn't supported — Pine emits `WITH`/CTEs for `count:`, `group:`, every `|=` variable, and auto-checkpoints, which 5.7 can't run)

## Key Files

- `src/pine/pine.bnf` — Grammar definition (source of truth for syntax)
- `src/pine/parser.clj` — Parses Pine syntax into AST
- `src/pine/ast/` — AST processing modules (one per SQL clause/operation)
- `src/pine/eval.clj` — Evaluates AST to SQL
- `test/pine/` — Tests mirror src structure

## Key Concepts

- **Operations**: Pipe-separated commands (e.g., `table | where: id = 1 | select: name`)
- **Table Modifiers**: `:parent`, `:child`, `:left`, `:right` for join types
- **Aliases**: `table as t` syntax for table aliasing
- **Hints**: Column hints with `.column` syntax for disambiguation

---

## The masking policy masks output only

The access policy (beamlynx calls it a masking policy) stops the contents of a masked column from coming back to whoever sent the query. A query may still filter, sort, partition and join on a masked column, because the value it compares with comes from the query. This is a deliberate choice, not a gap to close. Don't add refusals for these, even after a security review.

Guard only the places where a masked value would come back: the SELECT list, a `group:` key, and database error text. A `::uuid` cast, for example, makes Postgres print the value in its error. See `docs/access-policy.md`, "What it protects".

---

## Feature Development Workflow

When adding new features, follow this order:

### 1. Update the Grammar
**File**: `src/pine/pine.bnf`

### 2. Update the Parser
**Files**: `src/pine/parser.clj`, `test/pine/parser_test.clj`

### 3. Update the AST
**Files**: `src/pine/ast/main.clj` (or new file in `ast/`), `test/pine/ast_test.clj`

### 4. Update the Evaluator
**Files**: `src/pine/eval.clj`, `test/pine/eval_test.clj`

### Common Feature Patterns

| Feature Type | Key Files |
|-------------|-----------|
| New operation (e.g., `update!`) | `pine.bnf` → `parser.clj` → `ast/*.clj` → `eval.clj` |
| Table modifier (e.g., `:left`) | `pine.bnf` → `parser.clj` → `ast/table.clj` → `eval.clj` |
| New data type (e.g., dates) | `pine.bnf` → `parser.clj` → `ast/where.clj` → `eval.clj` |

---

## Tests

Tests are in three levels, as the code is:

- **Parser:** `test/pine/parser_test.clj`. Pine text to its parse.
- **AST:** `test/pine/ast_test.clj`. The parse to the state Pine builds.
- **SQL:** `test/pine/eval_test.clj` and the feature files beside it, such as `json_paths_test.clj`. The SQL text Pine writes for each dialect, against made-up schemas. Nothing runs on a database.

**Real databases.** `test/pine/db/` runs Pine on real SQLite, MySQL and Postgres databases:

- **Topic files**, such as `writes_test.clj`, `queries_test.clj` and `policy_test.clj`. Each test runs on all three engines, on a fresh database, through `pine.db.engines/on-every-engine`.
- **`engines/`.** Checks that only one engine has: SQLite's affinity types and file safety, how MySQL's schema is read, and SQL to Pine on Postgres.

A write test reads every row of every table before and after, with plain SQL rather than Pine, and asserts the whole difference (`pine.db.engines/changes`). Do the same for a new write. A feature that runs differently per engine gets its cases in a topic file, so all three run them.

SQLite runs on every `clojure -M:test`. MySQL and Postgres run when `PINE_TEST_MYSQL_URL` and `PINE_TEST_POSTGRES_URL` name servers. CI starts both and sets `PINE_TEST_REQUIRE_SERVERS=1`, so a missing server fails the run instead of skipping it. To run everything locally, in Docker:

```
scripts/test-databases.sh
```

---

## Formatting

Run the formatter before committing:

```
clj -M:fmt fix
```

---

## Changelog

Every fix/feat PR must add its own entry under `## [Unreleased]` in `CHANGELOG.md` — don't leave it empty and defer the writeup to the later release PR. The release checklist (`RELEASING.md`) only moves an already-written `Unreleased` section into a dated one; it doesn't author the content.

---

## Version Synchronization

**Important**: `src/pine/version.clj` must match `playground.docker-compose.yml` Docker image version.

Check with: `./scripts/check-version-sync.sh`

---

## Self-Update Instructions

When working on this codebase, actively look for opportunities to improve this file. If you discover new patterns, conventions, or project knowledge, **propose updates to AGENTS.md**.
