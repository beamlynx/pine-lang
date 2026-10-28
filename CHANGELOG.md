# Change Log
All notable changes to this project will be documented in this file. This change
log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]
### Added
- **Join on ids inside a JSON array:** `documents_package as dp | officer .id = .officers[*].id` joins each package to the officers its `officers` array lists. `[*]` means any element of the array, and keys before it lead to the array. This works on Postgres, MySQL and SQLite.

## [0.51.1] - 2026-10-28
### Fixed
- **Access policy: a join on a hidden column is refused.** A join only matches rows whose values are equal, so it could reveal a hidden value, just as `where:` could. A real foreign key still joins, unless only one of its two columns is hidden.
- **Access policy: writing a column name in another case no longer gets past a rule.** On MySQL and SQLite, which ignore case, `CUSTOMER_ID` is judged as the column `customer_id`. Under a policy, a column Pine doesn't know, on a table it does know, is refused.
- **Access policy: SQLite column types keep their declared names.** A type such as `STRING`, `JSONB` or `SSN` used to be read as `numeric`, so a rule that shows numbers showed it.
- **Access policy: only a primary key called `id` is always allowed.** An `id` column that isn't the table's primary key is checked like any other column.

## [0.51.0] - 2026-10-26
### Added
- **`group:` without keys gives one row of totals:** `orders | group: with count as n, total => sum as revenue`. Use it to name a count, since `count:` always calls its column `count`. SQL to Pine uses it too, so `COUNT(*) AS n` keeps its name and aggregates without `GROUP BY` convert.
- **SQL to Pine converts JSON keys.** `data->>'plan'` becomes `data.plan`, and `data->'tags'->0` becomes `data.tags[0]`, in selected values and function arguments. On MySQL and SQLite, `$` paths convert too. A key in `WHERE` or `GROUP BY` is still refused, with a message about JSON keys.
- **A condition can start with a function:** `where: jsonb_typeof(data) = 'array'`, or `where: data => jsonb_typeof = 'array'`. This works in `where:`, in conditions used as values, and in `if`.
- **New functions `jsonb_typeof()` and `jsonb_array_length()` on Postgres, MySQL and SQLite.** They are named after Postgres's, and `jsonb_typeof` gives Postgres's type names on every database. `jsonb_array_length` of a value that isn't an array is an error on Postgres, and NULL elsewhere.
- **`order:` takes `nulls first` and `nulls last`:** `order: closed_at desc nulls last`. This also works after `per`, and SQL to Pine converts them.
- **SQL to Pine converts `GROUP BY 1` and `ORDER BY 3`,** which name a column by its position.
- **SQL to Pine converts `EXISTS` and `NOT EXISTS` in `WHERE`,** when one equality ties the subquery to the outer query. `NOT EXISTS` gives the same rows as SQL, even with NULLs.
- **SQL to Pine converts `COUNT(column)`.** It counts the same rows as SQL, in `group:`, in `HAVING` and on its own.

### Fixed
- **Dates and times in results are exact on Postgres.** A timestamp keeps its fraction of a second. A `DATE` reads `1990-07-22` instead of midnight UTC, and a `TIME` reads as a time.
- **SQL to Pine: selecting only an earlier table's columns works.** The result used to include every column of the last table too.

## [0.50.0] - 2026-10-18
### Added
- **Access policy: hide rules.** A rule can hide columns as well as show them: `{"type": "hide", "match": {"type": "column-name", "contains": "token"}}`. A hide rule beats every show rule, so a `uuid` column called `reset_token` stays hidden even when uuids are shown. It applies wherever a column can be used, including `where:`, `order:`, `group:`, `per`, computed values and the `id` column. A hide rule Pine doesn't understand hides every column. See [access-policy.md](docs/access-policy.md).
- **Access policy: more ways to name a column.** `column-name` rules take `contains` and `equals` as well as `suffix`. A new `column` rule names one column of one table: `{"type": "column", "schema": "public", "table": "user", "column": "email"}`.
- **Access policy: a refused query names the hidden column.** When a query filters, sorts or groups on a hidden column, the error says which column, on which table, and which rule hid it. The app can then ask the owner to allow that column.
- **Access policy: a report on each column.** With a policy, each result column says whether it is hidden, which rule decided, and its real table. A report-only mode, `access-policy-mode: "annotate"`, hides and refuses nothing, so the owner's own tab can mark what an agent can't see.

### Changed
- **`column-name` rules ignore case.** `{"suffix": "_id"}` also matches `user_ID`.

### Fixed
- **SQL to Pine: a column without its table, in a query with a join.** `select * from tenant as t join company on t.id = tenantId` used to be refused, because `tenantId` could belong to either table. The schema decides, as in SQL: the column belongs to the one table that has it. A column that several tables have, or none has, is still refused, and so is one without a connection to check.

## [0.49.0] - 2026-10-17
### Added
- **The arrow `=>` applies a function.** `x => f` means `f(x)`, and `x => f(a, b)` means `f(x, a, b)`: `s: title => coalesce('untitled') as label`. Steps chain left to right, as in `created_at => year => concat('!')`. The date buckets are functions too, so `month(created_at)` and `created_at => month` both work. When Pine writes Pine, as in SQL to Pine, a function of one value uses the arrow and anything else the call form. See [computed-values.md](docs/computed-values.md).
- **SQL to Pine converts SQL that needs computed values.** `COALESCE` becomes `coalesce()`, `a || b` becomes `concat(a, b)`, `CASE WHEN` becomes `if ... then ... else`, `NOW()` becomes `now()`, and `(x IS NOT NULL) AS k` becomes `x is not null as k`. `MIN`, `MAX`, `SUM`, `AVG`, `BOOL_OR`, `BOOL_AND` and `FILTER (WHERE ...)` become aggregates in `group:`, and `HAVING` a `where:` after it. Every `GROUP BY` converts to exactly the SQL's columns. On Postgres, SQL's `CONCAT()` is refused because it skips NULLs; on MySQL it converts, and `||` is refused, since MySQL reads it as OR. See `docs/sql-to-pine.md`.
- **Computed values: a step can use a value that isn't just a column.** In `select:`, name it with `as`: `s: coalesce(reference_id, concat('att:', a.id)) as dedupe_key, id in edited as was_edited`. A value can be a column (with a cast), a literal, a `$variable`, a function, a condition (true or false), or `if <condition> then <value> else <value>`. The functions are `coalesce()`, `concat()` (NULL when any part is NULL) and `now()`. Later steps use the name like a column: `where: was_edited = true`, `group: dedupe_key`, `limit: 1 per company_id, dedupe_key`. See `docs/computed-values.md`.
- **Aggregates in `group:` take a value and a filter:** `group: d.id with a.id::text => min where a.type = 'sharepoint' as sp_attachment, any(a.type = 'sharepoint') as on_sp`. The aggregates are `min`, `max`, `sum`, `avg`, `any` and `all`. A `where:` after the `group:` filters the groups by those names, like `HAVING`. `now()` also works in `where:` and in `update! deleted_at = now()`, and an explicit join pair takes a cast: `activity_resource .resourceableId = .id ::text`. Under an access policy, a computed value that reads a hidden column is hidden whole.
- **SQL to Pine converts `WITH`, subqueries, top N per group, `UPDATE` and `DELETE`.** This covers more of the SQL an AI agent writes, so you can see it on the canvas before deciding to run it. Each `WITH` query becomes its own block ending in `|= name`, and a subquery becomes a named block or a list for `in`. `ROW_NUMBER() ... rn = 1` and `DISTINCT ON` become `limit: N per`, `LEFT JOIN ... IS NULL` becomes `not in`, and `IS DISTINCT FROM 'v'` becomes `!= 'v' or x is null`. `UPDATE` ends in `update!` and `DELETE` in `delete! .<primary key>`, and converting never runs them. What can't convert is refused with its line. See `docs/sql-to-pine.md`.
- **A line in a values block can have no value yet: `$status =`.** The variable is then unset, as if the line weren't there, and the query won't run until it has a value. It used to make the whole values block an error. See `docs/variables.md`.
- **Pine describes every checkpoint in the expression.** It reports each one's name, whether it comes from a `limit:` or a `group:`, the tables it seals and the columns of its result, even before it is sealed. The app uses this to label checkpoints on the canvas. See `docs/checkpoints.md`.
- **Top N per group: `limit: N per <columns>`.** It keeps the first N rows of each group: `order | o: created_at desc | l: 1 per customer_id` is each customer's latest order. The `order:` before it ranks each group's rows; with none, the primary key does, so the same rows come back every time. Several columns, alias-qualified columns and JSON keys can follow `per`. Like a plain `limit:`, it is a checkpoint, so later steps work on the rows kept. A column the access policy hides can't follow `per`. See `docs/limit-per.md`.
- **SQL to Pine.** Send a `SELECT` to `POST /api/v1/sql-to-pine` and get the same query back as Pine, with warnings. The conversion is exact or it refuses: anything Pine can't say, like `OFFSET` or `lower()`, is an error naming it and its line. Nothing runs. Beamlynx's SQL panel uses it, and so does `run_sql_as_pine`, the tool AI agents use to send SQL. See `docs/sql-to-pine.md`.
- **The SQL panel can suggest joins.** After `JOIN`, Pine suggests the tables that join to the ones already in the query, with the `ON` clause in the query's aliases: `employee e ON e.company_id = c.id`. See `docs/sql-hints.md`.
- **The SQL panel can complete table and column names.** Pine gives it the connection's tables and columns. See `docs/sql-hints.md`.
- **SQL to Pine spells a bare column name the way the database does.** `c.tenantId` becomes `.tenantId` when the column is `tenantId`, instead of `.tenantid`. A name that matches two columns differing only in case, like `Name` and `NAME`, is refused with a request to quote it.
- **Typing SQL where Pine goes tells you where it belongs.** A parse error on text that looks like SQL starts with "This looks like SQL. Paste it in the SQL panel, and it converts to Pine as you type."
- **`>=` and `<=` in `where:`:** `where: age >= 18`. They work wherever `>` and `<` do, including with a `$variable` and on a JSON key.
- **An `in` list can hold numbers:** `where: id in (1, 2, 3)`. Each value is typed by the column, so against a text column a number is compared as text. On a JSON key, `in` compares text.
- **JSON paths: use a key inside a `json` or `jsonb` column like a column.** `customer | where: data.country = 'SE' | s: data.address.city, data.tags[0]`. Quote a key that isn't a Pine name: `data.'home address'`. It works in `select:`, `where:`, `order:` and `group:`, on Postgres, MySQL and SQLite. Comparisons such as `data.seats > 10` compare JSON values and skip rows where `seats` isn't a number, while `like`, `in` and `is null` compare text. A hidden JSON column hides every key inside it. See `docs/json-paths.md`.
- **`update!` can write one key inside a JSON column:** `update! data.plan = 'pro'`. The rest of the value stays. `'5'` writes a string and `5` a number. A missing key is added, but a row without the object or array that would hold it is left alone and not counted.
- **Editing a JSON key in the results keeps the value's type.** Pine tells the app whether each selected key held a string, number, boolean, null, object or array.
- **Pine also returns each query's SQL without the hidden columns it adds for the results grid.** These are the key and JSON type columns used for editing.
- **SQLite.** A connection can be a SQLite file: set `dbtype` to `"sqlite"` and `dbname` to the file's absolute path. Pine reads its tables, columns and foreign keys, and JSON paths, date buckets, `update!` and `delete!` work as on Postgres and MySQL. Column types follow SQLite's rules, so `VARCHAR(255)` is `text`, and a date is compared as the text it was written as. SQLite is refused unless the server was started with `PINE_SQLITE=1`, which beamlynx-desktop sets, so a shared server never reads its own disk because a request asked. The file must already exist.
- **Numbers can be negative or have decimals:** `where: balance < -10`, `update! price = 19.99`. A decimal is sent exactly, not as a floating-point number. `limit:` and an array index still take a whole number of zero or more.
- **A string can contain an apostrophe written twice, as in SQL:** `where: name = 'O''Brien'`. This works in values blocks too.
- **Pine says whether an expression changes data before it runs, and keeps giving hints when its SQL can't be built.** A refused write or an unresolved join still returns hints, with the reason.
- **Launch token: only the program that started the server can use it.** When `PINE_TOKEN` is set, every request must send `Authorization: Bearer <token>`, or it is refused. beamlynx-desktop sets a fresh token each launch, so web pages and other programs on the machine can't use its server. Without the variable the server works as before, and logs that anyone on the machine can use it.
- **A running query can be stopped.** A query sent with a `run-id` is stopped in the database by `POST /api/v1/cancel` with the same `run-id`, and a stopped write is rolled back. See `docs/stopping-queries.md`.
- **Date-time literals:** `where: created_at > '2024-01-01 10:00'`, with optional seconds and a space or a `T`. A time of day used to turn the value into a string, which Postgres refused to compare with a timestamp column.

### Changed
- **Breaking: `with` divides a group's keys from its aggregates:** `group: status with count, total => max`. `=>` used to do this, and now only applies a function. `group: status => count` is a parse error that names the new form. `group: status` still counts each group.
- **Breaking: an unnamed checkpoint is called `result_1`, `result_2`, and so on, instead of `__pine_0__`.** A checkpoint is the step Pine adds when a table, `select:`, `where:` or `order:` follows `group:` or `limit:`. You see the name in the SQL and on the canvas, and you can use it in a later expression like any `|= name`. Pine skips names already taken by a named result, an alias or a real table. See `docs/checkpoints.md`.
- **A dotted name that is neither an alias nor a column is an error:** `s: nope.x` says so. It used to reach the database.
- **A quote inside a table, column or result name is escaped in the SQL.** It used to end the name.
- **Breaking for clients: each condition in the built query's `where` is a map instead of a list.** beamlynx-ui reads the new shape, so upgrade the two together.
- **Testing a connection runs `SELECT CURRENT_TIMESTAMP` instead of `SELECT NOW()`,** which SQLite doesn't have.
- **Breaking for clients: the built query lists named results under `named-results`, not `variables`.** "Variable" means a `$name` since 0.48.0. beamlynx-ui reads the new key.
- **Very long queries are refused before they are parsed.** This applies to an expression or SQL query over 65 536 characters, and to a request over 1 MB.
- **Breaking: `delete!` and `update!` refuse to change every row of a table.** Add a `where:` or `limit:` first. A `limit:` sealed into a checkpoint counts, so `company | l: 10 | employee | delete! .id` still works. They also refuse after `group:` and on a named result. See `docs/side-effects.md`.
- **Breaking: a join Pine can't resolve is an error naming both tables.** It used to produce SQL with no `ON` clause: a syntax error on Postgres, and a cross join on MySQL.
- **Breaking: `update!` finds rows by the table's primary key, not by a column called `id`.** A key of several columns is matched as a whole. A table without a primary key, including a view, is refused even if it has an `id` column.
- **Breaking: editing results also follows the primary key.** Pine adds one hidden column per key column for the results grid to edit by. A table without a primary key gets none, so its rows can't be edited. Under an access policy, a table gets none when any column of its key is hidden.
- **Every `delete!` and `update!` runs in a transaction,** not only an `update!` across several tables.
- **An `update!` that ends in a comma is refused as incomplete.** It used to run the assignments before the comma.
- **Every statement is cancelled after 60 seconds.**
- **`limit:` accepts 0 to 10 000.** A bigger number is an error that says so. A very large one used to give a raw Java error.
- **A query ending in `group:` returns at most 10 000 groups.** A `group:` sealed into a checkpoint keeps every group.
- **Raw SQL returns at most 10 000 rows,** followed by a row saying it was cut.
- **Each connection keeps up to three database connections, not one.** A slow query used to make every other request on that connection wait 10 seconds and fail.
- **The Docker image runs on Java 21 (Temurin), like the desktop app.** It used an end-of-life Java 11 image, and Jetty 12, the new web server, needs Java 17 or later.
- **Parse errors name the likely mistake when it is a common one.** These are an unknown operation (with the closest real one), a missing colon, a missing closing quote, and a comma or `and` between conditions. The parser's full report follows.

### Removed
- **`string_agg`, and `sum`, `avg`, `min` and `max` without a value, are gone from `group:`.** They took no column, so they computed over the constant 1, giving a sum equal to the count and an average of 1. `sum`, `avg`, `min` and `max` now take a value: `group: status with total => sum`.

### Fixed
- **An earlier table's column after a checkpoint works:** `group: c.id | s: c.id`. It used to build SQL naming a table that wasn't in the query. When the checkpoint didn't keep that column, or has two of that name, the error says so.
- **A database that can't be reached is reported as such.** The error reads like "Can't reach the database. Connection refused", instead of the connection pool's message about a timeout. The app can tell a database that's down from a mistake in the expression.
- **A table written with its schema, like `public.company`, is always the table.** A named result of the same name (`|= company`) used to take its place, so `update!` or `delete!` on the table was refused.
- **A `group:` after a named result called `x_1` works.** It used to fail with "WITH query name specified more than once".
- **`update! active = false` works on a Postgres boolean column.** The value was sent as text, which Postgres refused.
- **A cast column takes a value of the cast's type.** `where: id = '1' ::text` compares text with text; Postgres used to refuse `text = bigint`. A `::uuid` cast sends the value as a uuid.
- **A named result that selects `t.*` gives the right column hints.** Hints used to list a column with an empty name.
- **Comparing an enum column with a string works on Postgres:** `attachment | where: type = 'sharepoint'`. It used to fail with `operator does not exist: attachment_type_enum = character varying`. This covers `=`, `!=`, `<`, `>`, `in`, `not in` and `update!`, and an index on the column is still used.
- **A cursor in a values block above the query no longer breaks the build.** The build used to fail with an empty error. A failed build always says why.
- **A tab's doc comment shows when the tab starts with a values block.**
- **Many comments or a very long string no longer make parsing slow.** Twelve block comments after a `select:` column used to run the server out of memory, and a 1 MB string took 7.5 s. Both take milliseconds.
- **`update! name = other_column` copies the other column's value.** It used to set the column to NULL on every row it targeted. The other column must belong to the table being changed.
- **The development server (`clj -M:run-dev`) listens only on this machine,** like the real one, unless `PINE_HOST` says otherwise.
- **Registering the same database twice at once no longer leaks a connection pool.** A double click or a retry used to leave one open for good.
- **A request with no connection selected returns a clear error instead of a server error.** So does one naming a connection that isn't connected, or a parameter of the wrong type.
- **Selecting a connection whose schema can't be read keeps the previous selection.** The broken one used to stay selected, so every later request failed.
- **An impossible date such as '2024-02-31' is an error against a date or time column.** It used to roll over to 2024-03-02 without a word. Against a text column it is ordinary text.
- **A column on the right of a comparison is qualified like the one on the left:** `where: name = country`. In a join it used to be ambiguous, or name the wrong table.
- **A number given for a text column is compared as text,** such as a `$variable` set to 7. Postgres used to refuse `character varying = bigint`.
- **A table that exists in several schemas takes its columns from the one Postgres would use** (`public`, when it's one of them). The columns of every schema used to be merged, which gave wrong types, joins and access policy decisions.
- **Table hints ignore case:** `Comp` finds `company`.
- **`docs/named-results.md` no longer says a named result's `LIMIT` is dropped.** It is kept, and checkpoints depend on it.
- **`update! col = null` writes NULL.** On a text column it used to write the text 'NULL', and against a boolean, JSON or UUID column NULL became a string.

### Security
- **A web page can't reach the server through DNS rebinding.** When the server listens only on this machine (the default), a request addressed to any host other than `localhost`, `127.0.0.1` or `[::1]` is refused. A web page used to be able to point its own domain at 127.0.0.1 to reach it.
- **Requests are read only from a JSON body.** Form posts and query strings are ignored, because a web page can send a form post to another site without the browser asking first.
- **Connection details can't add driver options.** A host, port, database name or schema containing `?` or `&` is refused. Such a value used to add options like `allowLoadLocalInfile=true` on MySQL.
- **A MySQL server can never ask Pine for a local file.**
- **An unexpected error returns a generic message.** The details go only to the server log, since they can contain internal information.
- **Updated the Postgres driver, the web server and the JSON library.** The Postgres driver moves from 42.2.24 to 42.7.14, closing CVE-2022-21724, CVE-2022-26520, CVE-2022-31197 and CVE-2024-1597. The web server moves from Jetty 9.4 to Jetty 12.1.
- **Under an access policy, a table Pine hasn't indexed is refused.** It used to come back with nothing hidden. This covered a table created after the connection was indexed, a MySQL table in another database, and catalog views. Re-index the connection to query a new table.
- **Under an access policy, a hidden column can't be used in `where:`, `order:` or `group:`.** Repeating `where: title like 'a%'` letter by letter used to recover a hidden value from row counts. The `id` column and a named result's columns can still be used.
- **Only the catalog tables needed to read the schema are exempt from the access policy.** All of `information_schema` and `pg_catalog` used to be, including `pg_stat_activity` (the text of every running query) and `pg_shadow` (password hashes).

## [0.48.1] - 2026-10-06
### Fixed
- **0.48.0 couldn't be built into the server that the desktop app and the Docker image run.** The build failed with "File name too long". This release fixes it, and every change is now built in CI, so it can't happen silently again.

## [0.48.0] - 2026-10-06
### Added
- **`PINE_PORT` runs the server on a port other than 33333.** beamlynx-desktop uses it so its dev build and the installed app can run at the same time.
- **A named result after `in`:** `employee | where: id in acme_emps`. This feeds one query's result into another, as with `company | where: name = 'Acme' | employee .company_id | s: id |= acme_emps`. `not in` works too. The named result must select exactly one column. After `=`, a named result is an error that suggests `in`. See `docs/named-results.md`.
- **The canvas shows a `$variable` in a condition as written,** even when it has a value. Only the SQL preview shows the value.
- **Values blocks: write a query's values above it.** A block of only `$name = value` lines sets values for the queries in the tab: `$company_name = 'Acme'` above `company | where: name = $company_name`. A list goes in brackets: `$statuses = ('failed', 'stuck')`. Leave a blank line between the values and the query. See `docs/variables.md`.
- **Variables: `$name` in an expression,** with its value given separately instead of written into the query: `company | where: name = $company_name`. A value is a string, number or boolean, or a list of them for `in $name`. It is typed by its column and sent as a SQL parameter, like a literal. It works after any comparison, with a cast, with `in` and `not in`, and in `update!`. A query with a missing value still gives hints, but won't run until every value is set. See `docs/variables.md`.

### Changed
- **What was called a variable (`expr |= name`) is now a named result.** The syntax is unchanged, and its doc is `docs/named-results.md`. "Variable" now means a `$name`.

### Fixed
- **A cast after an `in` list applies to the column.** `where: name in ('a') ::text` used to read `::text` as one more value, so the query matched `'text'` too. The same goes for `in` with a `$variable` or a named result.
- **The SQL preview shows values correctly.** A `?` inside a string used to take the next value's place, and a `'` inside a value wasn't doubled. Running a query was never affected.
- **The SQL preview works with a `$` inside a string,** such as `'price $5'`. It used to fail.

## [0.47.0] - 2026-09-27
### Changed
- **Breaking: conditions inside one `where:` are joined with `or`, not a comma:** `where: status = 'blocked' or status = 'active'`. A comma or `and` there is a parse error. To someone used to SQL a comma read as AND, while Pine treated it as OR. To require several conditions, chain `where:` steps: `where: status = 'active' | where: country = 'SE'`. `or` needs a space on both sides, so a column like `color` or `order_id` is never misread.

### Fixed
- **Hints for a condition typed after a complete one follow what you type.** In `where: id = 1 or e.`, hints used to ignore `e.` and list the current table's columns.
- **A half-typed `where:` with several conditions filters for any of them,** as the finished expression does. It used to filter for all of them.

## [0.46.0] - 2026-09-25
### Changed
- **Breaking for clients: a join in the built query is a map with named fields instead of a positional array.** For example `{"from": "c_0", "to": "e_1", "columns": [{"from": "id", "to": "company_id"}], "parent": "from", "resolution": "fk", "type": null, "cast": null}`. See `docs/joins.md`.
- **Breaking for clients: an unresolved join has one shape.** It is a join with `resolution: null` and no columns, and its SQL has no `ON` clause.

### Added
- **Pick a join by naming several of its columns:** `note | note_ref .note_id, .other_id`. Name as many as it takes to pick out one relation, and Pine joins on the whole key. Order doesn't matter, and a subset is enough. Before, when two keys between the same tables shared a column, the second couldn't be reached. See `docs/joins.md`.
- **An explicit join can have several column pairs:** `a | b .x = .p, .y = .q`. Use it to join on several columns no foreign key describes, or on part of a key on purpose.
- **Table hints spell out every column of the key they name.** Two keys sharing a column are two different suggestions. Picking either used to give the first one.
- **Table hints for a key of several columns list all its column pairs,** so the app can name the whole key, for example to scope a `delete!`.
- **`delete!` takes several columns:** `delete! .case_id, .search_id`. They are matched as a row. Deleting by one column of a multi-column key used to remove rows belonging to other records. `delete! .id` is unchanged. See `docs/side-effects.md`.
- **Running an expression reports whether it changes data.** The operations that do are the ones ending in `!`: `delete!` and `update!`.
- **A caller can refuse writes before they run.** With `allow-writes: false`, an expression that changes data is refused before it runs, naming the operations. This is for a caller that must not change data, such as an AI agent. See `docs/side-effects.md`.

### Fixed
- **Breaking (behaviour): a foreign key of several columns is one join on all of them.** Pine used to join on whichever column you named. For `case_ref (case_id, search_id) -> case (id, search_id)`, naming `search_id` returned rows of other cases that shared a search id. `| case_ref`, `| case_ref .case_id` and `| case_ref .search_id` now give the same join. An expression that named the non-unique column will return fewer rows: the ones it should have returned all along. See `docs/joins.md`.
- **Table hints list a key of several columns once,** instead of once per column.
- **Pine no longer invents joins from a Postgres foreign key of several columns.** It paired every column on one side with every column on the other, so a two-column key gave four joins, two of them never declared. Picking one built SQL that Postgres rejected, such as `operator does not exist: uuid = character varying`. MySQL wasn't affected.
- **A half-typed `update!` returns the right column headers.**

### Removed
- **Breaking: `delete:` (and `d:`) is gone.** It built no SQL. It only told beamlynx to run its own step-by-step delete, which beamlynx builds from `count:`, joins and `delete!`. An expression with `delete:` is a parse error. `delete!` is unchanged.

## [0.45.0] - 2026-09-20
### Added
- **A comment at the top of an expression is its doc comment:** `/* Customers who signed up this week */`. The app can show it as text above the query instead of a grey comment. Use a `/* ... */` block or a run of `--` lines. See `docs/comments.md`.
- **Formatting an expression keeps its doc comment.** The app formats on every canvas change, so the comment used to vanish right away. Comments between operations are still dropped.

### Changed
- **Two databases on the same server can be connected at once.** A connection's id includes the database name (`host:port:dbname`). The second one used to be refused. Registering the same database as a different user is still refused.

### Fixed
- **`? table` no longer suggests routes back through a table already in the expression.** `company | employee | ? document` used to suggest a detour through `company`. A table sealed into a checkpoint, or the table behind a named result, can still be joined.
- **A comma inside one `where:` means OR, as the syntax suggested:** `where: id = 1, id = 2`. Pine used to drop every condition after the first. For AND, chain `where:` steps.
- **On Postgres, tables you have no `SELECT` grant on appear in hints.** Pine reads columns from `pg_catalog`, which every role can read. Querying such a table still fails, as before.
- **A name starting with an underscore, like `_user`, parses.**

## [0.44.0] - 2026-09-12
### Added
- **MySQL 8.0 and later.** Pine writes MySQL SQL for every operation, including dates, casts, `update!` and `delete!`, and reads the schema of the connected database. MySQL 5.7 isn't supported, because Pine uses `WITH` queries, which 5.7 lacks.

### Changed
- **Every failed request is logged to the server's output.** An error that came back as a normal error response used to print nothing.

## [0.43.0] - 2026-09-06
### Added
- **`? table` finds every join route to a table:** `company | ? document`. It searches through several hops and lists up to 10 routes, best first. Routes through a shortcut column, such as a copied `company_id`, rank last, then routes that go up and back down. Each result is a full Pine expression that replaces `? document`. It can follow any expression, like `company | where: active = true | ? document`, but nothing can follow it. The search stops after 150 ms, so a large schema stays fast. See `docs/paths.md`.

## [0.42.0] - 2026-09-02
### Added
- **Running an expression also returns it formatted,** as building it already did. A client no longer needs a second request to show it cleanly.

## [0.41.0] - 2026-08-31
### Added
- **Access policy: hide column values.** A query can carry rules saying which columns may be shown. Any other column comes back as `xxxxx`, and `*` is checked column by column. Rules can allow column types, foreign key columns, or names ending in a suffix such as `_id`. Without a policy, nothing changes.
- **`information_schema` and `pg_catalog` are always allowed.** They describe the schema, not your data.

## [0.40.0] - 2026-08-26
### Fixed
- **Queries no longer hang when nothing reads the server's output.** The server printed every query's SQL, and once that output filled up, every query waited forever. Building expressions kept working, so the server looked healthy. Per-query logging is off unless `PINE_LOG_QUERIES=1` is set.
- **Registering a connection again no longer leaks a database connection.** Each one used to hold a connection open for the life of the server: 32 in one desktop session, against a usual database limit of 100. The same database and user reuse the existing one.

### Changed
- **Registering a different database or user under a connection id that is taken is refused.** A connection id is only the host and port, so the new database used to replace the old one silently, and queries meant for the first ran against the second. Disconnect the existing one first.

### Security
- **The server no longer prints every query's SQL and values by default.** Set `PINE_LOG_QUERIES=1` to see them when debugging.

## [0.39.0] - 2026-08-22
### Fixed
- **A guessed join between columns of different types works.** A join guessed from column names rather than a foreign key, such as `varchar` to `uuid`, used to be rejected by Postgres with `operator does not exist: character varying = uuid`. Both sides are cast to text when needed. Real foreign keys are unaffected.
- **A table that joins to nothing appears in table hints,** such as a lookup table nothing references. You can start a query from it, though it is never a join target.
- **Postgres's own `pg_catalog` and `information_schema` tables stay out of hints.**

### Added
- **Re-read a connection's schema without restarting the server:** `POST /api/v1/connections/:id/reindex`. A table or column added after connecting used to stay invisible until a restart. Each index or reindex is logged with the connection's name.

## [0.38.2] - 2026-08-17
### Security
- **The server listens only on this machine (`127.0.0.1`) by default.** It has no sign-in, so listening on every interface left it open to the local network. Set `PINE_HOST` to change it. The Docker playground sets `0.0.0.0`, since Docker's port publish already limits access to this machine.

## [0.38.1] - 2026-08-13
### Fixed
- **Pine reports the columns a `group:` groups by.** A client couldn't tell a `group:` was there once committed, though it ran correctly.

## [0.38.0] - 2026-08-11
### Added
- **Each join reports how it was found:** `"fk"` (a foreign key), `"heuristic"` (a guess from column names), `"synthetic"` (a table joined on `id` to something built from that same table, such as a variable) or `"manual"` (written by hand). A client can tell whether a drawn join is backed by a real foreign key.

## [0.37.3] - 2026-08-09
### Fixed
- **A `group:` at the end of an expression, after a checkpoint, works.** The checkpoint's query used to be left out of the SQL, so the query failed.

## [0.37.2] - 2026-08-04
### Fixed
- **Join hints for a column like `tenant_id` survive a checkpoint.** After `l:` or `group:`, they used to vanish unless `id` was also selected.
- **Connecting to a database that can't be reached returns a normal error** instead of a server error.

## [0.37.1] - 2026-08-02
### Fixed
- **The server starts from any directory.** It used to need the project folder to find its grammar.

## [0.37.0] - 2026-07-31
### Added
- **Variables: name a result with `|= name` and reuse it:** `company | where: active = true |= active_companies`. The name can be used as a table, joins through it follow the real tables behind it, and `active_companies.col` names its columns. 0.48.0 renamed these named results.
- **`DELETE /api/v1/connections/:id` closes and removes a connection.**

### Fixed
- **Pressing Tab on an empty expression shows all tables.** It used to show nothing.

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
