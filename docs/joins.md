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
4. `join-helper` looks up the relation in the references map and records a **join map** on the AST
   state's `:joins` vector.
5. `eval/build-join-clause` turns each entry in `:joins` into a SQL `JOIN … ON …` fragment.

## Constraints

- Circular joins are not detected — the query will compile but the SQL may be nonsensical.
- A foreign key made of several columns is one relation, joined on all of its columns at once. Naming any
  one of them with `.column` selects the whole key, so the `ON` clause is the same whichever you pick.
  Deliberately joining on *part* of a key needs the explicit `.col1 = .col2` form, which bypasses the
  reference map.
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

   Both point at the **same relation map**, written once:
   ```clojure
   {:child      {:schema "z" :table "document"}
    :parent     {:schema "y" :table "employee"}
    :columns    [{:child "employee_id" :parent "id"}]
    :resolution :foreign-key}
   ```

   Which side is the child and which is the parent belongs to the relation, so it lives in the value.
   Which direction a caller is travelling in belongs to the lookup, so it stays in the path - and every
   caller already knows which of the two it asked for. Nothing has to be mirrored.

   `:columns` is a **list of pairs**, one per column of the key, each labelled by the side that owns it.
   A key made of several columns is one relation with a longer list, not several relations, and every
   consumer maps over the list rather than reading a first pair.

   The extraction queries return one column pair per row, alongside the constraint it belongs to and its
   position in that constraint. `group-by-constraint` groups those rows back into one relation before
   indexing. A row with **no** constraint name becomes a group of its own - the behaviour every row had
   before grouping existed - so a dialect that doesn't report one degrades to single-column joins rather
   than collapsing a table's every key into one invented composite.

   The relation is then filed under **every** column of the key, on both sides: `.case_id` and
   `.search_id` both resolve to the same complete join.

2. **`index-columns`** — adds column metadata to each table entry. Needed before the next pass.

3. **`index-heuristic-relations`** — scans every column looking for `_id` / `Id` suffixes:
   - `extract-table-from-column` strips the suffix: `company_id` → `company`, `tenantId` → `tenant`.
   - `normalize-plural` generates candidate names: `company` → `#{"company" "companies"}`.
   - Matches against all known tables via `build-table-lookup`.
   - Skips if the candidate table has no `id` column, if the FK already exists, or if it would be a
     self-referential heuristic.
   - Adds the same two-direction index entries as FK detection, tagged `:heuristic` instead of `:foreign-key`.
   - A naming convention names one column at a time, so a heuristic relation always has exactly one pair.

The same map structure is used for both FK and heuristic entries; the only difference is `:resolution`.
Callers can inspect it if they want to surface confidence level — see "Hint-facing resolution" below for
where that actually surfaces.

### Hint-facing resolution (`ast/hints.clj`)

Every table hint (`ast.hints.table[]`, the autocomplete suggestions for what to pipe in next) carries a
`:resolution` field, so a client can distinguish a confirmed relationship from a guessed one:

- **`"fk"`** — a real foreign key. Read straight from the relation's `:resolution` (`:foreign-key`, see
  above) by `resolution-of`.
- **`"heuristic"`** — a naming-convention guess (`company_id` → `company`), no FK constraint behind it. Same
  tag mechanism, `:heuristic` instead.
- **`"synthetic"`** — a made-up `id = id` join with *no* reference-map entry behind it at all, fabricated on
  the fly rather than read from a tag. Currently the only source of this is the same-source join described
  in [variables.md](variables.md#join-resolution-through-variables) — two references to the same table (at
  least one a variable) with no real FK connecting them. The name is deliberately not variable-specific:
  anything Pine ever has to invent a join for, rather than discover one for, gets this tag — a future
  self-join between two real tables (once Pine can tell two occurrences of the same table apart) would use
  it too.

A fourth value, `"manual"`, is reserved on the frontend type for the explicit `.col1 = .col2` case — but
since that syntax bypasses the reference map entirely (see Syntax above), there's nothing for a hint to
suggest there, so the backend never actually emits it.

### Join direction resolution (`ast/table.clj`)

`join-tables` tries two strategies in order:

1. **`:has`** — `join-helper` looks up `refs[:table t1 :referred-by t2]`. This succeeds when `t2` has a
   FK (or heuristic) pointing at `t1`. The `from` side is the parent, so the join map says `:parent "from"`.
2. **`:of`** — arguments swapped: `join-helper` looks up `refs[:table t2 :referred-by t1]`.
   The `to` side is the parent, so the join map says `:parent "to"`.

Either way the lookup is `:referred-by`, so `t1` is always the parent and `t2` always the child - which is
how `join-helper` knows which side of the relation each of its own arguments is, without the relation
carrying a direction.

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

Each entry in `:joins` is one map — the shape a client reads too, since `:joins` is returned on both
`/api/v1/build` and `/api/v1/eval`:

```json
{
  "from": "c_0",
  "to": "e_1",
  "columns": [{ "from": "id", "to": "company_id" }],
  "parent": "from",
  "resolution": "fk",
  "type": null,
  "cast": null
}
```

- `from`/`to` — the two aliases, in pipeline order (the order the user typed them). Stored once.
- `columns` — the column pairs the `ON` clause is built from, each labelled by the side that owns it.
  `build-join-clause` renders every pair and joins them with `AND`, so a longer list needs nothing new.
- `parent` — `"from"` or `"to"`: which side owns the key being pointed at.
- `resolution` — `"fk" | "heuristic" | "synthetic" | "manual"`, or `null`.
- `type` — `"LEFT" | "RIGHT"`, or `null` for an inner join.
- `cast` — `"text"` when a heuristic join's two columns have different types, otherwise `null`. A property
  of the join rather than of a pair: only a heuristic guess is ever cast, and a heuristic relation always
  has exactly one pair.

```sql
[LEFT|RIGHT] JOIN "schema"."table" AS "alias" ON "from"."col" = "to"."col" [AND …]
```

**Unresolved joins.** Nothing connects the two tables, or an explicit `.hint_col` matched no relation: the
join is still recorded, with `resolution: null` and no column pairs, and renders with no `ON` clause at all.
There is one spelling for "unresolved", so a client checks one thing.

### Composite foreign keys

A key can be made of more than one column:

```
k.case_ref (case_id, search_id)  ->  k.case (id, search_id)
```

```
k.case | k.case_ref
k.case | k.case_ref .case_id      -- the same join
k.case | k.case_ref .search_id    -- also the same join
```

```sql
JOIN "k"."case_ref" AS "cr_1"
  ON "c_0"."id" = "cr_1"."case_id" AND "c_0"."search_id" = "cr_1"."search_id"
```

Pine used to see this as two unrelated single-column relations and join on whichever one the user named.
One of them is usually right by accident — `case.id` is unique on its own, so matching on it alone selects
the same rows as matching on both. The other is not: `case.search_id` is not unique, so the join pulled in
`case_ref` rows belonging to *other* cases that happened to share a search id. It returned plausible rows
and said nothing.

The table hints list such a key **once**, named by its first column. That hint also carries a `columns`
array holding every pair of the key, for a client that has to name the whole key rather than join on it —
scoping a `delete!` to the rows the relation reaches, for instance (see
[side-effects.md](side-effects.md#which-rows-delete-removes)). It is left off a single-column key, where it
would only repeat `column`/`related-column` across what can be thousands of hints.

A variable (see [variables.md](variables.md)) that exposes only some of a key's columns can't serve the
join at all, so neither the join nor the hint is offered — the same rule a single unexposed column already
followed.
