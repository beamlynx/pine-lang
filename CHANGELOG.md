# Change Log
All notable changes to this project will be documented in this file. This change
log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]
### Added
- A string can contain an apostrophe by writing it twice, as in SQL: `where: name = 'O''Brien'`. This works in values blocks too.
- `/build` reports `writes`, whether the expression changes data, as `/eval` already did. When the SQL can't be built (a refused write, an unresolved join), `/build` still returns the AST and hints, with the reason in `query-error` and `query-error-type`. An error that has a kind now carries it as `error-type` from `/eval` too.
- **Launch token.** When `PINE_TOKEN` is set, every `/api/` request must send `Authorization: Bearer <token>`, or it is refused with HTTP 401 and `error-type: "unauthorized"`. beamlynx-desktop will set a fresh token each launch, so web pages and other programs on the machine can no longer use its server. Without the variable the server behaves as before, and logs that anyone on the machine can use it.
- Date-time literals: `where: created_at > '2024-01-01 10:00'`, with optional seconds and a space or a T. They bind as timestamps. A time of day used to make the value a string, which Postgres refuses to compare with a timestamp column.

### Changed
- **Breaking:** `/build`'s AST lists named results under `named-results`, not `variables`. "Variable" means a `$name` since 0.48.0, and its report is still `variables` in the response. Inside pine-lang, the state key and the code that passes named results between blocks are renamed the same way. beamlynx-ui's matching change reads the new key.
- An expression or raw SQL query longer than 65 536 characters is refused with `error-type: "too-long"` before it is parsed. A request body over 1 MB is refused with HTTP 413.
- **Breaking:** `delete!` and `update!` refuse to change every row of a table: add a `where:` or a `limit:` first. A `limit:` sealed into a checkpoint counts, so `company | l: 10 | employee | delete! .id` still works. They also refuse after `group:`, and on a named result. These come back with `error-type: "write-refused"`. See `docs/side-effects.md`.
- **Breaking:** a join Pine can't resolve is an error naming both tables, with `error-type: "unresolved-join"`, for every operation. It used to produce SQL with no `ON` clause: a syntax error on Postgres, and a cross join on MySQL.
- **Breaking:** `update!` finds rows by the table's primary key, not by a column called `id`. A key of several columns is matched as a row: `WHERE ("a", "b") IN ( SELECT ... )`. A table without a primary key is refused with `error-type: "write-refused"`, even when it has an `id` column. That includes views.
- **Breaking:** the hidden columns added to each result for editing follow the primary key too: one per key column, named `__<alias>__<column>`, so `__c_0__id` for a table keyed on `id`. A table without a primary key gets none. An unqualified table uses the key of the schema it resolves to.
- Every `delete!` and `update!` runs in a transaction, not only an `update!` across several tables.
- `/eval` refuses an `update!` that ends in a comma, with `error-type: "incomplete"`, instead of running the assignments before the comma.
- Every statement is cancelled after 60 seconds.
- `limit:` accepts 0 to 10 000. A bigger number is an error that says so; one past the range of a Java integer used to surface as a raw `NumberFormatException`.
- A query ending in `group:` returns at most 10 000 groups. A `group:` sealed into a checkpoint keeps every group.
- Raw SQL (`/api/v1/sql`) returns at most 10 000 rows, followed by a row saying it was cut.
- Each connection keeps up to three database connections, not one, so a slow query no longer makes every other request on that connection wait 10 seconds and fail.
- The Docker image runs on Java 21 (Temurin), like the desktop runtime. It used the end-of-life openjdk:11 Debian buster image. Jetty 12 needs Java 17 or later.
- The desktop runtime's module list (`desktop/jpackage/modules.list`) adds `java.instrument`, which Jetty 12 references.
- A parse error starts with a sentence naming the likely mistake when it is a common one: an unknown operation (with the closest real one), a missing colon, a missing closing quote, or a comma or `and` between conditions. Instaparse's own report follows unchanged.

### Removed
- `sum`, `avg`, `min`, `max` and `string_agg` after `=>` in `group:`. They take no column, so they computed over the constant 1: a sum equal to the count, an average of 1, and `STRING_AGG(1)`, which Postgres refuses. Only `count` remains until a column can be named.

### Fixed
- A `/build` cursor before the start of the expression, which beamlynx-ui sent with the cursor in a values block above the query, made the build fail with `"error": null`. Such a cursor is now ignored, and a failed build always says something: an exception with no message reports its type.
- `/build`'s `doc` was empty when the tab started with a values block. It now comes from the first doc comment in the leading values blocks or the first query block.
- Block comments after a `select:` column could take seconds to parse, or run the server out of memory: ten took 4 s, twelve exhausted a 2 GB heap. A 1 MB string literal took 7.5 s. Whitespace with comments, and each string literal, are now one token, so parsing is linear. Twelve comments now take under a millisecond and a 1 MB literal 17 ms.
- `update! name = other_column` set the column to NULL on every row it targeted. It now copies the other column's value. The other column has to belong to the table being changed; naming one from a joined table is an error.
- The development entry point (`clj -M:run-dev`) listened on every network interface. It listens on loopback, like the real one, unless `PINE_HOST` says otherwise.
- Registering the same database twice at the same moment (a double click, a retry) left one connection pool open for good. Only one is kept now.
- A request with no connection selected, or naming a connection that isn't connected, returned HTTP 500. It returns a normal error with `error-type: "no-connection"`. A parameter of the wrong type returns HTTP 400 with `error-type: "bad-request"` naming it. `/connection/stats` with nothing selected, and `/build-with-params` without an expression, no longer fail.
- Selecting a connection whose schema can't be read left it selected, so every later request failed on it. The previous selection now stays.
- An impossible date such as '2024-02-31' is an error. It used to roll over to 2024-03-02 without a word.
- A column on the right of a comparison, as in `where: name = country`, is qualified with the current table like the column on the left. Written bare, it was ambiguous, or named the wrong table, in a join.
- A number given for a text column, such as a `$variable` set to 7, is compared as text. Postgres used to refuse `character varying = bigint`.
- An unqualified table that exists in several schemas takes its columns from the one Postgres would use (`public`, when it's one of them). The columns of every schema used to be merged, typed by whichever came first, which gave wrong value types, join columns and access-policy decisions.
- Table typeahead ignores case: `Comp` finds `company`.
- `docs/named-results.md` said a named result's `LIMIT` was dropped. It is kept, and checkpoints depend on it.
- A date-shaped value against a text column is kept exactly as written. Since the date-time literals added above, `where: body = '2024-01-01 10:00'` on a text column compared against '2024-01-01 10:00:00.0', and a cell saved as that text was stored that way. Something date-shaped that isn't a real date, such as '2024-02-31', is ordinary text; it's an error only against a date or time column.
- `update! col = null` on a text column wrote the text 'NULL' instead of NULL, and `null` against a boolean, JSON or UUID column was converted to a string. NULL is now left as NULL whatever the column's type.

### Security
- When the server listens on loopback (the default), a request whose Host header isn't `localhost`, `127.0.0.1` or `[::1]` is refused with HTTP 403. This stops a web page from reaching the server by rebinding its own domain to 127.0.0.1 (DNS rebinding), with or without a token.
- Parameters are read from the JSON body only, not from form-encoded bodies or the query string. A form POST is a request a web page can send to another site without the browser asking it first.
- A connection's host, port, database name and schema are checked before the server builds the connection string. A value containing `?` or `&` used to add driver options: `allowLoadLocalInfile=true` on MySQL, `socketFactory=...` on Postgres. Such a value is now refused with `error-type: "bad-connection"`, naming the field.
- MySQL connections set `allowLoadLocalInfile=false` and `allowUrlInLocalInfile=false`, so a MySQL server can never ask pine-lang for a local file.
- An unexpected error returns HTTP 500 with a generic message. The real exception goes only to the server log, since its message can contain internal details.
- Updated the PostgreSQL driver from 42.2.24 to 42.7.14, closing CVE-2022-21724, CVE-2022-26520, CVE-2022-31197 and CVE-2024-1597. The web server moves from Jetty 9.4.44 (September 2021) to Jetty 12.1 through ring-jetty-adapter 1.15.5, and the JSON library to cheshire 6.2 with jackson-core 2.21. ring-core, ring-defaults and compojure are updated too.
- Under an access policy, a table missing from the schema index is refused with `error-type: "policy"` instead of returned unredacted. That covered a table created after the connection was indexed, a MySQL table in another database, and catalog views. Re-index the connection to query a new table.
- Under an access policy, a hidden column can't be used in `where:`, `order:` or `group:`. Redaction only covered the selected columns, so `where: title like 'a%'`, repeated letter by letter, recovered a hidden value from row counts. The `id` column and columns of a named result (already redacted at its source) can still be used. Grouping by a hidden column used to run and collapse every group to the placeholder; it is now refused.
- Only the catalog relations schema discovery needs are exempt from the access policy: in `information_schema`, tables, columns, key_column_usage, table_constraints, referential_constraints, constraint_column_usage, schemata and views; in `pg_catalog`, pg_class, pg_attribute, pg_namespace, pg_type, pg_constraint, pg_index, pg_description, pg_tables, pg_views and pg_indexes. The whole of both schemas used to be exempt, including pg_stat_activity (the text of every running query) and pg_shadow (password hashes).

## [0.48.1] - 2026-10-06
### Fixed
- 0.48.0 couldn't be built into the standalone jar the desktop app and the Docker image run. Compiling `pine.parser` ahead of time failed with "File name too long": the `where:` conditions grew past what core.match can turn into short enough class names. The `in` and `not in` conditions are now matched on their own. CI now builds the jar on every change, so this can't happen silently again.

## [0.48.0] - 2026-10-06
### Added
- Set `PINE_PORT` to run the server on a port other than 33333. beamlynx-desktop uses it so its dev build and the installed app can run at the same time.
- **A named result after `in`:** `employee | where: id in acme_emps` matches the values `acme_emps`'s one column returns, so one query's result can feed another: `company | where: name = 'Acme' | employee .company_id | s: id |= acme_emps`. It's built as `IN ( SELECT "id" FROM "acme_emps" )`, with the named result's CTE emitted as when it's used as a table. `not in` works too. A named result that selects every column or more than one is an error naming its columns. After `=` or another comparison, a named result is an error that suggests `in`, or `not in` for `!=`: `where: id = x` would otherwise read `x` as a column. A column with a named result's name can still be compared by writing its alias, like `t.x`. See `docs/named-results.md`.
- `/build`'s AST shows a `$variable` as written, even when a values block or the request gives it a value. Only the SQL preview (`query`) has the value. The app's canvas shows `$name` in a condition this way.
- **Values blocks:** a block of only `$name = value` lines sets values for the query blocks in the request, so a query and its values are all in the text: `$company_name = 'Acme'` above `company | where: name = $company_name`. A value is written like a literal, or a list in brackets: `$statuses = ('failed', 'stuck')`. A value passed in `variables` overrides one written in the text. `/build`'s report adds `values`, the ones written. A query in the same block as values is an error asking for a blank line between them. See `docs/variables.md`.
- **Variables:** `$name` in an expression, with its value sent in the request's `variables` field instead of written into the query: `company | where: name = $company_name` with `{"company_name": {"value": "Acme"}}`. A value is a string, number or boolean, or a list of them for `in $name`. It's typed by its column and bound as a SQL parameter, like a literal. It works after any comparison, with a cast, as the list for `in` and `not in`, and in `update!`. `/build` still builds a query with a missing value, so a template keeps its hints, and reports `variables: {used, unbound, lists}`, where `lists` are the variables used with `in`, which take a list. `/eval` refuses to run with a value missing (`error-type: "unbound-variable"`) and names each one. See `docs/variables.md`.

### Changed
- What `docs/variables.md` described (`expr |= name`) is now called a **named result**, in `docs/named-results.md`. The syntax is unchanged. "Variable" now means a `$name`.

### Fixed
- A cast after an `in` list, as in `where: name in ('a') ::text`, was read as one more value, so the query matched `'text'` too. The cast now applies to the column, as with every other operator: `"name"::text IN (...)`. The same goes for `in` followed by a `$variable` or a named result.
- The SQL preview from `/build` put a value's `?` in place of the next value when a string contained one, and showed a `'` inside a value without doubling it. It now fills each placeholder once, in order. Running a query was never affected: values are sent separately.
- The SQL preview from `/build` threw on any string literal containing `$`, such as `'price $5'`, because the value was inserted with a regex replacement that read `$` as a group reference.

## [0.47.0] - 2026-09-27
### Changed
- **Breaking:** conditions inside one `where:` are joined with `or`, not a comma: `where: status = 'blocked' or status = 'active'`. A comma there is now a parse error, and so is `and`. A comma read as AND to anyone used to SQL, while Pine treated it as OR, so the same expression meant different things to the person writing it and to Pine. To require several conditions, chain `where:` steps as before: `where: status = 'active' | where: country = 'SE'`. `or` needs a space on both sides, so a column like `color` or `order_id` is never read as containing it.

### Fixed
- Hints for a condition typed after a complete one (`where: id = 1 or e.`) ignored what was being typed and listed the current table's columns. The partial condition was dropped whenever a complete condition came before it.
- A half-typed `where:` with several complete conditions filtered for all of them (AND), while the finished expression filters for any of them (OR). Both now filter for any.

## [0.46.0] - 2026-09-25
### Changed
- **Breaking:** a join in the AST (`joins`, returned by `/api/v1/build` and `/api/v1/eval`) is now a map instead of a positional array inside another positional array. It was `["c_0", "e_1", ["c_0", "id", "has", "c_1", "company_id", "fk", false], null]` -- nothing in that said what any slot meant, both ends had to count positions, and the two aliases were stored twice. It is now `{"from": "c_0", "to": "e_1", "columns": [{"from": "id", "to": "company_id"}], "parent": "from", "resolution": "fk", "type": null, "cast": null}`. `parent` (`"from"` or `"to"`) replaces the `"has"`/`"of"` tag and says the same thing in words. `type` is the `LEFT`/`RIGHT` modifier, `cast` replaces the trailing `needs-cast?` flag. See `docs/joins.md`.
- **Breaking:** an unresolved join -- two tables nothing connects, or a `.hint_col` matching no relation -- now has one spelling instead of two. It used to be either a missing relation or a present one with every column nil; it is now always a join map with `resolution: null` and no column pairs, and renders with no `ON` clause instead of comparing two zero-length identifiers. A client checks `resolution` and nothing else.
- A join's `ON` clause is now built from a *list* of column pairs rather than a single pair. Every column pair is rendered and ANDed together. The list always holds exactly one pair for now -- nothing that reads the schema produces more yet -- but nothing downstream assumes that, which is what makes joining on all the columns of a composite foreign key a change in one place rather than everywhere.

### Added
- The columns after a table name which relation you mean, and there can be more than one of them: `note | note_ref .note_id, .other_id`. They **identify** a relation rather than specify one -- name as many as it takes to pick one out, and Pine joins on the whole key whichever you named. Naming nothing, or naming a column two relations share, still picks the first relation indexed, exactly as before. This makes a join expressible that was not: where two keys between the same pair of tables share a column, every spelling reachable with one column resolved to the same relation, and the second was unreachable. Order doesn't matter and a subset is enough. See `docs/joins.md`.
- Explicit join columns take more than one pair: `a | b .x = .p, .y = .q`. That is the only way to write a join on several columns that no foreign key describes -- and, the other way round, to join on part of a key on purpose.
- A table hint now spells out every column of the key it names, so two keys sharing a column are two different suggestions instead of the same text twice. Picking either used to give the first-indexed one.
- A table hint for a foreign key made of more than one column now carries a `columns` array holding every pair of the key, so a client that has to name the whole key -- scoping a `delete!` to the rows that relation reaches, for instance -- doesn't have to rediscover the rest. Absent on a single-column key, where it would only repeat `column`/`related-column` across what can be thousands of hints.
- `delete!` takes more than one column: `delete! .case_id, .search_id`. They are matched as a row -- `WHERE (case_id, search_id) IN ( SELECT case_id, search_id ... )`. A table whose key is made of several columns has no single column that picks out one of its rows, so deleting on one of them at a time removed rows belonging to other records without saying so. `delete! .id` is unchanged. See `docs/side-effects.md`.
- `/api/v1/eval` now reports whether an expression changes data, as a `writes` field on every response. An operation that writes is one the grammar marks with `!` -- `delete!`/`d!` and `update!`/`u!`. Previously a client that needed to know had to match the expression text itself.
- `/api/v1/eval` accepts `allow-writes: false` and refuses to run an expression that changes data, before it runs. The error comes back with `error-type: "write-refused"` and names the operations it refused. Leaving the field out means writes are allowed, so every existing caller is unaffected. This is for a caller that must not change data -- an AI agent, for instance -- and it replaces having each client keep its own list of which operations write. See `docs/side-effects.md`.

### Fixed
- **Breaking (behaviour):** a foreign key made of more than one column is now one join on all of its columns, instead of one join per column. Pine saw `case_ref (case_id, search_id) -> case (id, search_id)` as two unrelated relations and joined on whichever column the user happened to name. Naming `case_id` was right by accident -- `case.id` is unique on its own -- but naming `search_id` was not: `case.search_id` is not unique, so the join returned `case_ref` rows belonging to *other* cases that shared a search id, and said nothing about it. Naming any column of a key now joins on the whole key, so all three spellings (`| case_ref`, `| case_ref .case_id`, `| case_ref .search_id`) produce the same `ON ... AND ...`. An existing expression that named the non-unique column of a composite key will return fewer rows than it did -- the rows it should have returned all along. Joining on part of a key on purpose still works through the explicit `.col1 = .col2` form. See `docs/joins.md`.
- The table hints list a composite key once, named by its first column, instead of once per column with nothing to say which to pick.
- A foreign key made of more than one column produced joins that were never declared. Postgres keeps a constraint's own columns and the columns they point at in two lists that line up position by position; pine paired every column on one side with every column on the other. A two-column key came out as four relations instead of two -- the two real ones, plus two pine invented. The invented ones looked exactly like real foreign keys, so pine offered them as joins and built SQL comparing columns that were never meant to be compared, which Postgres rejects (for example `operator does not exist: uuid = character varying`). Only Postgres was affected; MySQL's foreign keys were always read one column pair at a time. A composite key still reaches pine as one relation per column pair -- pine does not yet build a single join from all of them at once.
- Evaluating a partially-typed `update!` returned the wrong column headers. The result rows already had the `Table`/`Rows updated` shape every other write produces, but the response described them with the expression's own columns instead. Complete `update!` and `delete!` were always correct.

### Removed
- **Breaking:** the `delete:` operation (and its `d:` short form). It parsed into an operation that built no SQL at all -- it existed purely as a marker for a client to notice and run its own recursive-delete routine against. That routine lives in beamlynx, and it is built from operations that already exist (`count:`, a join, `delete!`), so there was nothing left for the marker to mark. An expression containing `delete:` is now a parse error. `delete!`, which does the actual deleting, is untouched.

## [0.45.0] - 2026-09-20
### Added
- A comment at the top of an expression is now a doc comment. `/api/v1/build` returns its text as `doc`, so a client can render it as prose instead of leaving it as grey text in an editor. Either style works: a `/* ... */` block, or a run of `--` lines. The text comes back ready to display -- delimiters removed, javadoc-style leading `*` stripped, shared indentation removed. `doc` is the *first* expression's comment, since that is the one describing the whole tab. See `docs/comments.md`.
- `prettify` (and the `prettified` field both endpoints return) now keeps a doc comment instead of deleting it. It rebuilds an expression from the parsed operations' own text spans, and a leading comment sits outside all of them -- so any client that prettifies, which beamlynx does on every canvas gesture, used to lose the comment immediately. Comments between operations are still dropped, unchanged.

### Changed
- A connection's id now folds in the database name (`host:port:dbname`). Two databases on the same server can now be registered and connected to at the same time; previously the second one was rejected outright. Registering the same database again as a different user is still rejected, same as before.

### Fixed
- `? table` no longer suggests paths that route back through a table the expression has already joined. Only the *last* table in the pipe was excluded, so `company | employee | ? document` came back with a detour back up through `company`. Those routes did evaluate, they were just never answers to the question being asked. A target reachable only through already-joined tables now correctly returns nothing. Two things stay joinable on purpose: a table sealed into a CTE by a checkpoint (`l:`/`group:`), since the outer query no longer joins it, and the real table behind a variable. Table-name suggestions served while the target is still being typed apply the same exclusion.
- A comma inside one `where:` now means `OR`, as the syntax always suggested: `where: id = 1, id = 2` produces `WHERE (id = ? OR id = ?)`. The grammar accepted this, but the parser kept only the first condition and silently dropped the rest. `AND` is unaffected -- chain separate `where:` steps (`where: id = 1 | name = 'Acme'`).
- A Postgres role with no `SELECT` grant on a table (or no `USAGE` on its schema) never saw that table's columns, so the table never appeared in hints at all. Column indexing now reads `pg_catalog` (`pg_attribute`/`pg_class`/`pg_namespace`/`pg_type`) instead of `information_schema`, and every role can read that regardless of grants -- schema metadata was never gated the way row data is. Running an actual query against a table with no grant still fails, same as always; only introspection changes.
- A table, column, or alias starting with an underscore (`_user`) failed to parse -- `symbol`'s grammar required a leading letter. An identifier can now start with a letter or an underscore.

## [0.44.0] - 2026-09-12
### Added
- MySQL 8.0+ as a second, fully-parallel database backend alongside Postgres — identifier quoting, date bucketing, casts, and update!/delete! all render MySQL-correct SQL, and schema introspection is scoped to the connected database via `DATABASE()`. A query touching a `DATETIME`/`DATE`/`TIME` column correctly returns its value as JSON (MySQL's JDBC driver returns `java.time.LocalDateTime`/`LocalDate`/`LocalTime`, which needed their own Cheshire encoders alongside Postgres's existing ones). MySQL 5.7 isn't supported: Pine emits `WITH` (CTEs) for `count:`, `group:`, every `|=` variable, and auto-checkpoints, and 5.7 has no CTE support. Covered by the same fixture-based test suite Postgres already has (no live database in CI), plus manual verification against a real MySQL server (`dev.docker-compose.yml`).

### Changed
- Every request that fails now prints to the server's stdout: a new `wrap-exception-logging` middleware catches anything that escapes every route handler (including a failure while encoding the response itself), and every route's own `catch` blocks now log too. Previously a caught-and-handled failure (a bad connection, an unreachable database) produced a normal error response but printed nothing, making it undebuggable without reproducing by hand outside the server.

## [0.43.0] - 2026-09-06
### Added
- `? table` finds every join chain between wherever a pipe expression currently is and a named table, e.g. `company | ? document` — not just the direct next hop, but every path through the schema graph (including multi-hop ones), ordered by fewest transitively-redundant hops first, then fewest direction changes, then fewest total hops as the final tiebreak. A redundant hop lands on a table that's already reachable another, longer way (e.g. a denormalized `company_id` column that duplicates what a longer chain of real ownership relations already reaches) — the least meaningful reason two tables are "connected", so it ranks last even when shortest. A direction change is a route that hops "up" then "down" (or vice versa) partway through — a detour through a branch unrelated to either table, worth taking only when nothing more direct exists; a route that stays "zoomed in" (child hops) or "zoomed out" (parent hops) the whole way isn't favored either way over the other. It composes on top of any existing Pine expression (`company | where: active = true | ? document`), but is terminal: nothing meaningful follows it, and there's currently no bare `table1 ? table2` form — the `|` is required, same as any other operation. Results come back under a new `hints.paths` bucket, each a full pine expression ready to splice in place of the `? target` segment; while the target table name is still being typed, `hints.table` continues to serve suggestions, narrowed to tables actually reachable from the current context (not every table in the schema, since anything else is guaranteed to resolve to zero paths once fully typed). `? table` never builds a runnable query itself — evaluating it is a no-op, same as bare `delete:`. Capped at 10 results and 150ms of search time (down from an earlier 50/uncapped-time during development), so a densely-connected schema — or a target that's unreachable or only reachable very deep — can't turn into a long-running search; a 150-table synthetic case runs in ~14ms. See `docs/paths.md`.

## [0.42.0] - 2026-09-02
### Added
- `/api/v1/eval` now also returns `prettified` (a nicely formatted rendering of the expression that ran), the same value `/api/v1/build` already returned. It was already computed internally on every request; callers that want to show an evaluated expression cleanly no longer need a second `/api/v1/build` round trip to get it.

## [0.41.0] - 2026-08-31
### Added
- `/api/v1/build` and `/api/v1/eval` accept an optional `access-policy` rule array. Any column no rule allows comes back as `xxxxx` in the generated SQL instead of its real value; `.*` expansion is checked column-by-column. Rule types: `column-type` (allow-listed Postgres types), `foreign-key` (relation source columns), `column-name` (suffix match, e.g. `_id`). No policy given -- no change in behavior.
- `information_schema`/`pg_catalog` are always exempt -- they're schema metadata, not application data.

## [0.40.0] - 2026-08-26
### Fixed
- A query could hang forever, taking every other database-backed request down with it, if whatever launched the server was not reading the server's own standard output. The server printed the full SQL of every query it ran; once nothing drained that stream, its buffer filled and each print blocked permanently. Endpoints that never touch the database kept working normally, including `/api/v1/build`, which made the server look healthy while every query sat unanswered. Per-query logging is now off unless `PINE_LOG_QUERIES=1` is set. Startup messages, which are few and only appear when a connection is indexed, are unchanged.
- Registering a connection that was already registered leaked a database connection every time. A connection's id is derived from its own host and port, so re-registering the same database always overwrote the existing entry — but the pool it displaced was never closed, and the pool settings keep one connection open at all times. Each stale pool therefore held a real database connection for the life of the process; 32 of them accumulated in a single desktop session, against a default server limit of 100. Registering the same database as the same user now reuses the existing pool instead of building another one.


### Changed
- Registering a **different** database or user under a connection id that is already taken now fails with an explanatory error, instead of silently taking that id over. Because a connection id is only a host and port, two databases on the same server share one id and could never both be registered — the old behaviour pointed the existing id at the new database, so queries a caller believed were running against the first database quietly ran against the second. Disconnect the existing connection first to reuse the id. The error names the id and says why.

### Security
- The server no longer prints every query's SQL, including literal values, to standard output by default. Set `PINE_LOG_QUERIES=1` to opt back in when debugging.

## [0.39.0] - 2026-08-22
### Fixed
- A heuristic join (a naming-convention guess, not a real foreign key) whose two columns had different DB types — e.g. one stored as `varchar`, the other as `uuid` — generated a join Postgres rejected outright (`operator does not exist: character varying = uuid`), since nothing checked the columns' types were even compatible before joining them. Each committed join's relation tuple now carries a 7th `needs-cast?` element; when true, both sides of the generated `ON` clause are cast to `text`. Real FK joins are never affected — the constraint already guarantees the types are compatible.
- A table with no foreign key and no heuristic match to another table never showed up as a table hint, even though its columns were fully indexed — for example, a lookup table nothing else references. Table hints now fall back to the plain schema index for a table like this, so it can still be a starting point for a query. It still won't show up as a join target, since it genuinely has nothing to join through.
- Fixing the above meant Postgres's own `pg_catalog` and `information_schema` tables could start appearing in hints too. Column indexing now excludes both schemas outright.

### Added
- A connection's schema used to be indexed once, on first connect, and cached forever. A table or column added to the database afterward stayed invisible until the whole server restarted. A new endpoint, `POST /api/v1/connections/:id/reindex`, re-reads a connection's tables and columns on demand, so a restart is no longer needed. The server also logs each time it indexes or reindexes a connection, naming the connection, so a reindex request is easy to confirm from the logs.

## [0.38.2] - 2026-08-17
### Security
- The server now binds to `127.0.0.1` (loopback only) by default, instead of every network interface. The server has no authentication, so the old default left it reachable from the whole LAN, not just the local machine. Set `PINE_HOST` to change it -- the dockerized playground sets it to `0.0.0.0`, since Docker's own port publish already restricts external access to loopback.

## [0.38.1] - 2026-08-13
### Fixed
- The build response now includes the columns a `group:` clause is grouping by — previously missing entirely, so a client had no way to tell a `group:` was present once committed, even though it was being evaluated correctly.

## [0.38.0] - 2026-08-11
### Added
- Each committed join in `ast.joins` now carries its own resolution confidence (`"fk"`, `"heuristic"`, `"synthetic"`, or `"manual"`) as a 6th element of the relation tuple, matching what join hints already exposed — so a client no longer has to guess (or re-derive from the picker) whether an already-drawn join is backed by a real foreign key.

## [0.37.3] - 2026-08-09
### Fixed
- A checkpoint (named via `|=` or auto-named) feeding into a pipeline's terminal `group:` had its own CTE silently dropped from the generated SQL, leaving the group's wrapper CTE referencing a relation that was never defined.

## [0.37.2] - 2026-08-04
### Fixed
- Relation/join hints for a column like `tenant_id` were lost whenever a checkpoint (`l:`/`group:`) sealed the selection into an anonymous CTE and `id` wasn't also selected — even though that relation never needed `id` in the first place. Only the synthetic self-join hint actually needs `id` to be selected; other relations no longer require it.
- `POST /api/v1/connections` now returns a normal `{"error": "..."}` response instead of an uncaught server error when the target database is unreachable (e.g. down, wrong host/port).

## [0.37.1] - 2026-08-02
### Fixed
- The grammar (`pine.bnf`) now loads from the classpath instead of a `user.dir`-relative path, so the server no longer depends on being launched from the project root/a specific working directory.

## [0.37.0] - 2026-07-31
### Added
- Variables: name and reuse an intermediate query result across expressions with `|= name` (e.g. `company | where: active = true |= active_companies`). Results — including auto-sealed `group:`/`limit:` checkpoints — compile to CTEs, joins through a variable resolve using the real table(s) it traces back to, and a variable name can be used as a column qualifier (`x.col`) anywhere, including mid-expression.
- `DELETE /api/v1/connections/:id` closes and removes a database connection pool.

### Fixed
- Pressing Tab on an empty expression now shows all tables instead of nothing.

## [0.36.0] - 2026-05-21
### Added
- Structured `GET /api/v1/connections` response: returns an object with `version`, `selected-connection-id`, and a `connections` list of `{id, label}` entries (where `label` is formatted as `host:port · dbname`) (by @Koziar).

## [0.35.0] - 2026-05-05
### Added
- Per-session database connections: `build`, `eval`, and `sql` endpoints now accept an optional `connection-id` parameter. Queries run against that specific connection pool; when absent, the global connection is used (backward compatible).

## [0.34.0] - 2026-05-04
### Added
- Update partial column hints: `u!` can be followed by an incomplete column token (for example `company | u! i`), parsed like `where` partials and suggesting matching assignable columns. After completed assignments, `u! id = '1', col` supports partial completion for the next column name.

## [0.33.0] - 2026-04-20
### Added
- Column hints for the `update!` / `u!` operation. Typing `u!` or `u! col = val,` now suggests remaining assignable columns, excluding those already assigned.


### Changed
- The `=> count` in the `group` operation is now optional. `count` is used by default when omitted:
```
email | g: status
```

## [0.32.0] - 2026-03-30
### Added
- Multi-table `update!` support: when assignments target different tables (e.g. `c.deleted_at` and `d.deleted_at`), multiple UPDATE queries are run—one per table.
- API eval response for `update!` now includes per-table results: `[["Table" "Rows updated"] ["company" 5] ["document" 3]]`.

### Fixed
- Recursive delete no longer follows heuristic relations — only real foreign key constraints are traversed. Heuristic relations are now flagged in `ast.hints.table` via a `heuristic` boolean so clients can distinguish them.
- `update!` now uses the table alias when columns are qualified (e.g. `c.name`), so updates target the correct table when multiple tables are in context:
```
company as c | w: id = 1 | document | w: type = 'invoice' | update! c.deleted_at = '2026-01-01'
```

## [0.31.0] - 2026-02-16
### Added
Build endpoint returns:
- Prettified expression in the `ast.prettified` property.
- Ranges for the operations in the `ast.ranges` property.

## [0.30.0] - 2026-02-04
### Added
- Support for heuristic relations based on column naming conventions. This is helpful when foreign keys are not explicitly specified.


## [0.29.0] - 2025-12-25
### Added
- Support for cursor position aware hints. This is helpful when the user isn't at the end of the expression. Hints are generated based on the cursor position. The build endpoint supports a new parameter `cursor` which must contain the `line` and `character` position of the cursor.

## [0.28.0] - 2025-12-08
### Added
- Support for date extraction functions in the select operation e.g.
```
employee | select: created_at => year
employee | select: created_at => year as created_at_year
```

Supported functions are: `year`, `month`, `week`, `day`, `hour`, `minute`

- Group on derived columns e.g.
```
employee | select: created_at => month | group: month => count
```

- Explicit join columns are supported in the join operation e.g.
```
company | employee .company_id = .id
```

### Removed
- Internal state field `:join-map` has been removed. This was legacy dead code kept since v0.8.0 that was never actually used. The `:joins` vector format continues to be used for SQL generation.

## [0.27.0] - 2025-10-19
### Added
- Column aliases are supported in the order operation e.g.
```
company as c | o: c.name asc
```

- Support comments in the expressions e.g.
```
company | -- This is a line comment
company | /* This is a multi-line block comment */
```

## [0.26.1] - 2025-09-07

### Changed
- Using a readonly db user for the playground

## [0.26.0] - 2025-09-06
### Added
- Support for raw SQL queries:
```
POST /api/v1/sql
```

## [0.25.0] - 2025-08-28

### Added
- Values are type casted to the appropriate database column type.

### Fixed
- As we use the correct typecase for the values, it is possible to update a jsonb column.

- It is possible to use a LIKE operator on a uuid using a type cast e.g.
```
company | where: id like '9cd%' ::uuid
```

## [0.24.0] - 2025-08-25
### Added
- Support for `update!` operation:
```
customers | w: id = 1 | update! name = 'John Doe'
```
- Return id columns for all tables in the result. This allows in-place updates on a query result.

### Fixed
- All columns were being returned in some cases instead of the explicitly selected columns. e.g.
```
company | s: id,
company | s: id | l: 1
```

## [0.23.0] - 2025-08-15
### Added
- Setup for playground

### Fixed
- Numbers are parsed as longs e.g. if the column is an integer:
```
customers | id = 1
```

## [0.22.0] - 2025-07-12
### Added
- Column hints for `where:` operation, supporting partial expressions:
```
company | where:           # Shows all columns
company | w: i             # Shows columns matching 'i' (like 'id')  
company | w: id =          # Shows all columns after specifying column + operator
y.employee | w: comp       # Shows columns matching 'comp' (like 'company_id')
```

## [0.21.0] - 2025-07-02
### Fixed
- Docker image wasn't running.Updated the base image to `openjdk:11-jre-slim`

### Added
- Support for `ilike`, `not like`, and `not ilike` operators:
```
company | where: name ilike 'acme%'
company | where: name not like 'test%'
company | where: name not ilike 'admin%'
```
- Support for casting columns as `::uuid`
- Support for dates in conditions e.g.
```
company | where: created_at > '2025-01-01' | created_at < '2026-01-01'
```

## [0.20.0] - 2025-06-22
### Added

- Specify join types i.e. `LEFT JOIN` or `RIGHT JOIN`:
```
x | y :left
x | y :right
```

### Breaking
- Syntax for specifying parent and child relations is changed (introduced in `0.6.0`). This avoids the need for backtracking.
```
x | of: y
x | has: y
```
is now:
```
x | y :parent
x | y :child
```

- `^` is removed from the syntax to specific the directionality of the join. (introduced in `0.6.0`)

## [0.19.0] - 2025-06-21
### Added
- Support for casting columns in conditions e.g.
```
company | where: id like '9cd%' ::text
```

## [0.18.0] - 2025-06-04
### Added
- Support for `group` operation:
```
email | group: status => count
```

- Column aliases are supported in conditions e.g.
```
tenant as t | company | where: t.id = 'xxx'
```


### Changed
- Default limit is removed for `count:` and `delete:` operations.
- For `count:` operations, the `with` SQL clause is used to build the nested query e.g.

```pine
company | count:
```

is evaluated to:

```sql
WITH x AS (SELECT * FROM "public"."company") SELECT COUNT(*) FROM x;
```

## [0.17.0] - 2025-05-03
### Fixed
- Using database connection pooling
- Using UTC dates

## [0.16.0] - 2025-02-09
### Added
- Support for connection stats which contain the number of db connections.
```
GET /connection/stats

{
  "connection-count": 10,
  "time": "2025-02-10T01:49:53.808120858"
}
```



## [0.15.0] - 2025-02-02
### Added
- Column hints when using the order operation:

```
company | o:
company | o: id,
```

### Fixed
- Columns hints for the correct table are show e.g. the following was showing hints for `company` to begin with:
```
company | s: id | document | s:
```


## [0.14.1] - 2025-01-09
### Fixed
- By default all columns are selected. When columns are specified, all columns are not returned e.g. this didn't work:

```
employee as e | document as d | s: e.id
```

## [0.14.0] - 2025-01-07

### Added
- Support for columns e.g. hints are generated for a partial select:
```
company | s:
company | s: id,
```


### Changed
- Connection id format is `host`:`port` instead of just the `host`.

## [0.13.0] - 2024-10-25
### Added
- DB connection management i.e. create a new connection and connect to it
```
POST /connections
POST /connections/:id/connect
```

- Support for booleans:
```
company | is_public = true
```

## [0.12.0] - 2024-10-18
### Added
- Support for `not in` operator
- Support for no operations e.g. `delete:`. Such operations are evaluated client side.

## [0.11.0] - 2024-09-12
### Added

- `where:` supports comparing values between columns of different tables

```
folder as f | document | where: name = f.name
folder as f | document | name != f.name
```


## [0.10.0] - 2024-09-12
### Added

- Support for `count:`:

```
company | count:
```

## [0.9.0] - 2024-09-04
### Added

- Support for `NULL`:

```
company | name is null
company | name is not null
```

which also works with the `=` operator:
```
company | name = null
company | name != null
```

- Support for `order`:
```
company | order: created_at
company | order: country, created_at asc
```

## [0.8.1] - 2024-07-30
### Fixed

- Specifying the join column in case of ambigious relations wasn't working.

## [0.8.0] - 2024-07-30
### Added
- Change the context using the `from:` keyword. This is helpful when the tables relations are not linear and look like a tree.
```
company as c | document | from: c | employee
```

### Breaking
- State: `joins` is a vector e.g. `[ "x" "y" ["x" "id" :has "y" "x_id"]]`


### Changed
- State: `join-map` is kept for legacy reasons but it is only used internally.

## [0.7.2] - 2024-07-26

### Changed
- No difference in functionality. Removed a lot of deprecated code - only keeping the code for reborn.

## [0.7.1] - 2024-07-26
### Fixed
- Allow spaces in the start of a pine expression

## [0.7.0] - 2024-07-26
### Added
- Support for `in` operator


### Changed
- Error type is returned. It is either nothing or `parse`.

## [0.6.0] - 2024-07-22
### Added
- Support for directional joins:
```
employee | has: employee
employee | of: employee
employee | employee^
```
- Columns can be qualified by table aliases:
```
employee as e | s: e.name
```

## [0.5.4] - 2024-07-16
### Fixed
- Incorrect hints were generated in case of ambiguity

## [0.5.3] - 2024-07-16
### Fixed
- Incorrect schema being returned in hints when joining from child to parent

## [0.5.2] - 2024-07-14

### Changed
- Default `limit` is `250` if not specified

### Fixed
- All columns weren't being select in some cases e.g. using `company | s: id | employee`, the columns from `employee` table weren't being selected

## [0.5.1] - 2024-07-11
### Added
- Context sensitive columns selection e.g. `company | s: id | employee | s: id`

## [0.5.0] - 2024-07-10

### Added
- Hints can be provided to resolve ambigious joins e.g. instead of `company | employee`, you can explicitly specify the join column i.e. `column | employee .company_id`
- The delete operation uses a nested query. The column used for deletes must be specified:

```pine
public.company | delete! .id
```

evaluates to:

```sql
DELETE FROM
  "public"."company"
WHERE
  "id" IN (
    SELECT
      "c_0"."id"
    FROM
      "public"."company" AS "c_0"
  );
```
- Conditions can be composed. Following are allowed:

```pine
company | where: id='xxx'
company | w: id='xxx'
company | id='xxx'
```


### Changed
- Conditions can't be combined with the tables e.g. `company id='xxx'`. Instead compose them using pipes: `company | id='xxx'`
- Double quotes around strings aren't supported anymore. Use single quotes i.e. instead of `id="xxx"`, use `id='xxx'`

### Removed
- Support for `group`, `order`, `set!` is dropped. It will be added soon in the up coming versions.
- Context sensitive columns selection

## [0.4.8] - 2024-06-24
### Fixed
- Strings can contain a `+` character.

## [0.4.7] - 2024-06-13
### Fixed
- Db host can be configrued using an environment variable: `DB_HOST`

## [0.4.6] - 2024-06-13

### Changed
- The host is returned as the connection id instead of an internal identifier.

## [0.4.5] - 2024-06-13

### Changed
- Updated configuration to require environment variables: `DB_NAME`, `DB_USER`, `DB_PASSWORD`


## [0.4.4] - 2024-06-13
### Fixed
- Support for multiple architectures i.e. amd64 and arm64

## [0.4.3] - 2024-06-11
### Fixed
- It wasn't possible to get the relations between tables using a readonly user.
- Generating an uberjar so that dependencies are not loaded when the server starts.

## [0.4.2] - 2024-05-04
### Added
- The context contains the schema as well.

### Fixed
- The values for the filters weren't being quoted properly in some cases

### Breaking
- The hints for tables contain an object of schema and table instead of just a string i.e. table.

## [0.4.1] - 2023-08-11
### Added
- Better hints i.e. taking into consideration the context e.g. for expression `document | ..`, only tables related to `document` will be suggested. Also only schemas of the related tables will be suggested.


### Changed
- Reverted the change for getting all the columns. Instead of listing all the columns, we are relying on the `*` again. The change was a remnant of bug related to the ordering of the columns which had to do nothing with explicitly specifying the columns.
- The `connection` protocol doesn't expose the `get-schema` method.

### Fixed
- Numbers as parameters wasn't working e.g. `file version>1`

### Breaking
- Dropped support for MySQL.
- All endpoints are prefixed with `/api/v1`


## [0.4.0] - 2023-07-28
### Added
- Disabled CORS
- API endpoint for getting the active connection:
```
GET /connection

{
  ...
  "connection-id": "..."
}

```
- When using `POST /build` the response also includes the `connection-id`, and `params`
- When using `POST /eval` the response also includes the `connection-id`, `query`, and `params`.
- In case of an error, it is handled and the error message is returned in the API response as `error`
- Limited support for showing hints based on the input

### Fixed
- Pine expression build/eval was failing if the db connection isn't initialized
- An error was being thrown when using `uuid` values in the expressions: `operator does not exist: uuid = character varying`
- Order of the columns in the result was sometimes not the same as the order in the query. Also, all columns are explicitly selected in the sql instead of relying on `*`

## [0.3.1] - 2022-02-14
### Added
- API endpoint for building expressions:
```
POST /build
{
  "expression": "user"
}


{
  ...
  "query": "\nSELECT user_0.* FROM \"user\" AS user_0 WHERE true;\n"
}
```

- API endpoint for evaluating expressions:
```
POST /eval
{
  "expression": "user"
}

{
  ...
  "result": [
    {
      "email": "john@acme.com",
      "name": "John Doe",
      ...
    },
    ...
  ]
}
```
- API endpoint for setting the connection:
```
PUT /connection
{
  "connection-id": "default"
}

{
  "connection-id": "default"
}
```
- API endpoint for getting the connections
```
GET /connections

[
  "result": [ "default", "mysql-test" ]
]
```

### Deprecated
- API endpoint for building sql expressions `POST /pine/build`. Use the new endpoint: `POST /build`.

## [0.3.0] - 2022-02-10
### Added
- Support for Postgres

### Breaking changes
- Default limit of `50` is removed for updates and `1` for deletes
- Unselecting of columns is disabled. This will be enabled again in a future release.
```
customers | unselect: id
```
- Format of the config file is changed. This was done to support multiple
  connection configurations. The `:connection-id` property can be set to select
  the default connection.

## [0.2.0] - 2019-04-26
### Added
- Unselecting of columns
```
customers | unselect: id
```

### Fixed
- It wasn''t working:
```
customers industry=""
```
- Setting string values wasn't working e.g.
```
customers 1 | set! industry="Test"
customers 1 | set! industry=123
```

## 0.1.0 - 2019-04-21
### Added
- Check out the [features][features] document for a list of features

[Unreleased]: https://github.com/ahmadnazir/pine/compare/0.3.1...HEAD
[0.3.1]: https://github.com/ahmadnazir/pine/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/ahmadnazir/pine/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/ahmadnazir/pine/compare/0.1.0...0.2.0
[features]: FEATURES.md
