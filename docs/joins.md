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
table_a | table_b .hint_col, .hint_col
table_a | table_b .left_col = .right_col
table_a | table_b .left_col = .right_col, .left_col = .right_col
table_a | table_b .left_col = .json_col.key[*].key
table_a | table_b :parent
table_a | table_b :left
```

- **No modifier** — Pine picks the join direction automatically, and the first relation it indexed.
- **`.hint_col`** — name which relation you mean, when two tables are connected more than one way.
  Several, comma-separated, when one column isn't enough to tell them apart. See
  [Naming a relation](#naming-a-relation).
- **`.col1 = .col2`** — override both sides explicitly; bypasses the reference map entirely. Several
  pairs, comma-separated, for a join on more than one column.
- **`.col = .json_col[*].key`** — join on the values inside a JSON array. See
  [Joining on values in a JSON array](#joining-on-values-in-a-json-array).
- **A name given with `as`** — `s: resourceId as company_id | company` joins `company` on that column. See
  [Joining on a name you give](#joining-on-a-name-you-give).
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
SELECT "c_0"."id" AS "__c_0__id", "e_1".*
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

### Joining on values in a JSON array

```
documents_package as dp | officer .id = .officers[*].id
```

Joins each package to every officer whose `id` is in the package's `officers` array. `[*]` means "any
element". Keys after it are read inside each element; keys before it lead to the array:
`.id = .data.staff[*].id`.

The sides follow the same rule as any explicit pair: the first column is the new table's (`officer.id`),
the second the previous table's (`dp.officers`). The path may be on either side:

```
officer | documents_package .officers[*].id = .id
```

```sql
-- Postgres
JOIN "officer" AS "o_1" ON "dp"."officers"::jsonb @> jsonb_build_array(jsonb_build_object(?::text, "o_1"."id"))
-- MySQL
JOIN `officer` AS `o_1` ON JSON_CONTAINS(JSON_EXTRACT(`dp`.`officers`, ?), JSON_ARRAY(`o_1`.`id`))
-- SQLite
JOIN "officer" AS "o_1" ON EXISTS (SELECT 1 FROM json_each("dp"."officers", ?) AS "pine_each"
  WHERE CASE WHEN "pine_each"."type" IN ('object', 'array') THEN json_extract("pine_each"."value", ?) END = "o_1"."id")
```

- **A row joins once per match, not once per element.** An officer listed twice in one package appears
  once. An element that isn't an object, or lacks the key, is skipped.
- **Values compare as JSON.** The other column's value becomes the JSON value it is: a uuid or text becomes
  a string, an integer a number. So the number `5` doesn't match the string `"5"`. A cast on the other side
  changes what is looked for: `.id ::text = .data[*].id`.
- **On Postgres, with nothing before `[*]`, the test is containment (`@>`),** which a GIN index on the JSON
  column can answer.
- **Not supported:** a path without `[*]`, more than one `[*]`, an index after `[*]`, a path on both sides,
  and a cast on the path side. Each is refused with a message.
- **Written the wrong way round,** `dp | officer .officers[*].id = .id` names an `officers` column of
  `officer`. When the previous table has that JSON column, the error shows the pair turned round.
- **Masking policy.** A join on a masked JSON column runs. The column itself still comes back masked. See
  [what the policy protects](access-policy.md#what-it-protects-output-not-inference).

### Joining on a name you give

```
activity_resource | where: resourceableType = 'Company' | s: id, resourceableId as company_id | company
```

A name you give a column with `as` tells Pine what its values are. When the name follows the naming rule
for joins, a bare table joins on it. Here `company_id` leads to `company`, so the join is
`company.id = activity_resource.resourceableId`. No foreign key or column name could say this, because
`resourceableId` holds the ids of several tables.

The rule is the one Pine uses for column names (see [How it works](#how-it-works)): `company_id` and
`companyId` lead to a table called `company` or `companies` that has an `id` column.

The name can be on:

- **A column of `select:`**: `s: resourceableId as company_id`.
- **A key inside a JSON column**: `s: data.ownerId as employee_id`. The key's text is compared with `id`.
- **The elements of `unnest:`**: `unnest: companyIds as company_id | company` joins each element to the
  company it names. Elements named after the table itself join on their own `id` key:
  `unnest: companies as company | company` joins `company.id` to each element's `id`.
- **A key of an element**: `unnest: data as e | s: e.companyId as company_id | company`.

```sql
-- s: resourceableId as company_id | company (text compared with uuid, so both are cast)
JOIN "company" AS "c_1" ON "ar_0"."resourceableId"::text = "c_1"."id"::text
-- unnest: companyIds as company_id | company
JOIN "company" AS "c_2" ON ("company_id"."value"::jsonb #>> '{}') = "c_2"."id"::text
```

- **Only names you give count.** A column without `as`, or a computed value, doesn't. The column's own
  name is already in the index.
- **The name comes before the index.** A bare table joins on the newest name that leads to it, even when a
  foreign key also connects the two tables. Name a join column, `company .company_id`, to use the index
  instead.
- **The name must be on the table the join starts from**, or on the elements of an `unnest:` on it.
- **Pine can't check what the values are.** In the example, only the `where:` makes sure the ids are
  company ids. Without it, ids of other resources are matched too.
- **Types that don't compare are cast to text,** as for a join guessed from column names. A key inside
  JSON is always compared as text.
- **Hints.** After the name, the table it leads to is suggested as a bare table, with the resolution
  `"alias"`.

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
  in [named-results.md](named-results.md#join-resolution-through-named-results) — two references to the same table (at
  least one a variable) with no real FK connecting them. The name is deliberately not variable-specific:
  anything Pine ever has to invent a join for, rather than discover one for, gets this tag — a future
  self-join between two real tables (once Pine can tell two occurrences of the same table apart) would use
  it too.

- **`"alias"`** — a join from a name given with `as` in the query, `s: resourceId as company_id`. It comes
  from the query, not the index, so its hint is the bare table. See
  [Joining on a name you give](#joining-on-a-name-you-give).

A fifth value, `"manual"`, is reserved on the frontend type for the explicit `.col1 = .col2` case — but
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

Before either, when no join column is named, `named-value-join` looks for a name given with `as`
(`named-values` lists them, newest first). Its join map has `:resolution "alias"`. A pair on a JSON value
carries `:from-value-path`, the keys inside the value (`[]` for an element itself), and `eval.clj`
compares that value's text with the other column.

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

### Naming a relation

The columns after a table **identify** a relation. They don't specify one.

That distinction only started to matter once a foreign key made of several columns became a single
relation. Before that, a relation *was* a column, so `.created_by` both picked the relationship and
described the `ON` clause. Now a relation is a key, and a key can have more than one column.

So: name as many columns as it takes to pick one relation out, and Pine joins on the whole key
whichever you named.

| What you name | What you get |
|---|---|
| nothing | the first relation indexed |
| a column only one relation has | that relation |
| a column two relations share | the first of them indexed |
| enough columns to pick one out | that relation |
| columns no single relation holds | nothing — an unresolved join |

Order doesn't matter, and a subset is enough: `.a, .b` names the key `(a, b, c)`.

This is deliberate. **Pine is terse — it doesn't make you be explicit, it resolves what you left
out, and it shows you what it resolved.** The showing happens on beamlynx's canvas, which draws the
relation Pine actually settled on, column handles and all. Being explicit is there when you want it,
not a toll on every join.

One consequence worth stating: the text you typed is left exactly as you typed it. `prettify` does
*not* expand `.search_id` into the full key. It rebuilds an expression from the parsed operations'
own text spans and knows nothing about the schema — and, more to the point, rewriting a terse
expression into a verbose one takes away the terseness the user chose. The canvas is the feedback
channel, not the text.

#### Two keys sharing a column

```
note_ref (note_id, search_id) -> note (id, search_id)
note_ref (note_id, other_id)  -> note (id, other_id)
```

`.note_id` belongs to both, so it names whichever was indexed first. The second one is reached by
naming a column only it has:

```
note | note_ref .note_id, .other_id
```

Before column lists, that join could not be asked for at all — every spelling reachable with a
single column resolved to the same relation. The table hints spell out every column of a key for the
same reason: two keys sharing a column would otherwise be offered as the same text twice.

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

A named result (see [named-results.md](named-results.md)) that exposes only some of a key's columns can't serve the
join at all, so neither the join nor the hint is offered — the same rule a single unexposed column already
followed.

### Explicit columns, more than one pair

```
note | note_ref .note_id = .id, .search_id = .other_id
```

```sql
JOIN "k"."note_ref" AS "nr_1"
  ON "n_0"."id" = "nr_1"."note_id" AND "n_0"."other_id" = "nr_1"."search_id"
```

Bypasses the reference map entirely, so this is both how you join on columns no foreign key connects
and how you join on *part* of a key on purpose.
