# Joins

Pipe two table names together and Pine figures out the JOIN condition automatically.

## Why

SQL joins require you to spell out `ON table_a.col = table_b.col` every time, even when the relationship is
already encoded in the schema as a foreign key. Pine reads the FK graph once at startup and resolves the join
condition for you. When there is no FK (e.g. a multi-tenant column added by convention), Pine falls back to
heuristic detection based on column naming.

## Syntax

```
table_a | table_b
table_a | table_b .hint_col
table_a | table_b .left_col = .right_col
table_a | table_b :parent
table_a | table_b :left
```

- **No modifier** — Pine picks the join direction automatically.
- **`.hint_col`** — disambiguate when two tables share more than one FK.
- **`.col1 = .col2`** — override both sides explicitly; bypasses the reference map entirely.
- **`:parent`** — force the join to treat `table_b` as the parent (i.e. the table `table_a` refers to,
  not the table that refers to `table_a`).
- **`:child`** — inverse of `:parent`; explicit but rarely needed since it is the default.
- **`:left` / `:right`** — emit `LEFT JOIN` / `RIGHT JOIN`.

## Examples

### Basic (FK-resolved)

```
company | employee
```

```sql
SELECT "c_0".id AS "__c_0__id", "e_1".*
FROM "company" AS "c_0"
JOIN "employee" AS "e_1" ON "c_0"."id" = "e_1"."company_id"
LIMIT 250
```

### Disambiguation hint

`document` has two FKs to `employee` (`employee_id` and `created_by`). Without a hint Pine picks the first
one alphabetically.

```
employee | document .created_by
```

```sql
JOIN "document" AS "d_1" ON "e_0"."id" = "d_1"."created_by"
```

### Forcing parent direction

```
employee | company :parent
```

Forces Pine to treat `company` as the parent even though `:has` would also match. Equivalent to the default
here, but necessary when the automatic direction would be wrong.

### Left join

```
company | employee :left
```

```sql
LEFT JOIN "employee" AS "e_1" ON "c_0"."id" = "e_1"."company_id"
```

### Explicit columns

```
employee | document .company_id = .id
```

Bypasses the reference map. `company_id` is on `document` (right table); `id` is on `employee` (left table).

## How it works

1. At startup, Pine queries the database for all foreign keys and scans all column names. It builds a
   **references map** used for every subsequent join resolution.
2. When the parser sees `table_a | table_b`, it emits two consecutive `:table` operations.
3. `table/handle` in `ast/table.clj` calls `update-joins`, which calls `join-tables`, which calls `join-helper`.
4. `join-helper` looks up the resolved join vector from the references map and records it on the AST
   state's `:joins` vector.
5. `eval/build-join-clause` turns each entry in `:joins` into a SQL `JOIN … ON …` fragment.

## Constraints

- Circular joins are not detected — the query will compile but the SQL may be nonsensical.
- Heuristic joins are only inferred when no FK already covers the same pair.
- Self-referential heuristic joins are suppressed. Real self-referential FKs (e.g.
  `employee.reports_to → employee.id`) are supported.

---

## Implementation

### Reference map structure (`db/postgres.clj`)

`index-references` builds the map in three passes:

1. **`index-foreign-keys`** — queries `pg_constraint` and indexes every FK in both directions:
   - `refs[:table f-table :referred-by table :via col]` — child direction ("who points at me")
   - `refs[:table table :refers-to f-table :via col]` — parent direction ("who I point at")

   Each entry is a list of **join vectors**:
   ```
   [f-schema f-table f-col :referred-by schema table col :foreign-key]
   [schema table col :refers-to f-schema f-table f-col :foreign-key]
   ```

2. **`index-columns`** — adds column metadata to each table entry. Needed before the next pass.

3. **`index-heuristic-relations`** — scans every column looking for `_id` / `Id` suffixes:
   - `extract-table-from-column` strips the suffix: `company_id` → `company`, `tenantId` → `tenant`.
   - `normalize-plural` generates candidate names: `company` → `#{"company" "companies"}`.
   - Matches against all known tables via `build-table-lookup`.
   - Skips if the candidate table has no `id` column, if the FK already exists, or if it would be a
     self-referential heuristic.
   - Adds the same two-direction index entries as FK detection, tagged `:heuristic` instead of `:foreign-key`.

The same map structure is used for both FK and heuristic entries; the only difference is the tag in position 7
of the join vector. Callers can inspect it if they want to surface confidence level.

### Join direction resolution (`ast/table.clj`)

`join-tables` tries two strategies in order:

1. **`:has`** — `join-helper` looks up `refs[:table t1 :referred-by t2]`. This succeeds when `t2` has a
   FK (or heuristic) pointing at `t1`. Returns `[a1 col :has a2 f-col]`.
2. **`:of`** — arguments swapped: `join-helper` looks up `refs[:table t2 :referred-by t1]`.
   Returns `[a2 f-col :of a1 col]`.

`:has` is tried first unless the `:parent` modifier is set, in which case only `:of` is attempted.

When `.hint_col` is present, `join-column` is set. `join-helper` uses it to select a specific key from the
`via` map instead of taking `first`.

When `.col1 = .col2` is present, `join-left-column` and `join-right-column` are set. `update-joins` bypasses
`join-tables` entirely and records the explicit pair directly.

### Grammar (`pine.bnf`)

```
TABLE      := table table-mods
table-mods := (<ws+> table-mod)*
table-mod  := <":"> ("parent"|"child"|"left"|"right") | as-alias | hint-columns
hint-columns     := hint-column | explicit-columns
explicit-columns := hint-column <ws*> <"="> <ws*> hint-column
```

### SQL generation (`eval.clj`)

`build-join-clause` maps each `[from-alias to-alias relation join-type]` entry in `:joins` to:

```sql
[LEFT|RIGHT] JOIN "schema"."table" AS "alias" ON "a1"."col1" = "a2"."col2"
```

The `ON` columns are positions 1 and 4 of the relation vector: `[a1 col _ a2 f-col]`.
