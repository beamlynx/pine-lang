# Pine Project Instructions

## What is Pine?

Pine is a domain-specific language (DSL) for querying databases. It provides a concise, pipe-based syntax that compiles to SQL.

**Tech Stack**: Clojure 1.11.3, Instaparse (BNF grammar-based parsing), PostgreSQL

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

## Version Synchronization

**Important**: `src/pine/version.clj` must match `playground.docker-compose.yml` Docker image version.

Check with: `./scripts/check-version-sync.sh`

---

## Self-Update Instructions

When working on this codebase, actively look for opportunities to improve this file. If you discover new patterns, conventions, or project knowledge, **propose updates to AGENTS.md**.
