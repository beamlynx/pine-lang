# Top N per group

Keep the first N rows of each group with `limit: N per <columns>`.

## Why

"Each customer's latest order" and "the three best-paid people in each department" are common questions. A plain `limit:` can't answer them: it caps the whole result, not each group. In SQL the answer is a window function in a subquery, which is long to write and easy to get wrong.

`per` says it in one step. The rows are ranked by the `order:` before it, as a plain `limit:` uses the current order.

## Syntax

```
<expression> [| order: ...] | limit: <N> per <column>[, <column>...]
<expression> [| o: ...]     | l: <N> per <column>[, <column>...]
```

- `per` is a keyword, like `as` or `desc`. It needs whitespace on both sides, so a column called `perimeter` is never read as `per`.
- The columns after `per` are written as anywhere else: `customer_id`, `c.id` after a join, or a key inside a JSON column, `data.region`.
- A date bucket such as `created_at => month` can't be used after `per`.
- N is checked as for a plain `limit:`: 0 to 10 000.

## Examples

### The latest row of each group

```
order | o: created_at desc | limit: 1 per customer_id
```

Each customer's latest order:

```sql
SELECT "__pine_rows__"."__o_0__id", "__pine_rows__"."id", "__pine_rows__"."customer_id", "__pine_rows__"."user_id"
FROM (
  SELECT "o_0"."id" AS "__o_0__id", "o_0"."id", "o_0"."customer_id", "o_0"."user_id",
         ROW_NUMBER() OVER (PARTITION BY "o_0"."customer_id" ORDER BY "o_0"."created_at" DESC) AS "__pine_row__",
         ROW_NUMBER() OVER (ORDER BY "o_0"."created_at" DESC) AS "__pine_order__"
  FROM "order" AS "o_0"
) AS "__pine_rows__"
WHERE "__pine_rows__"."__pine_row__" <= 1
ORDER BY "__pine_rows__"."__pine_order__"
LIMIT 250
```

### Top 3 per group

```
employee | o: salary desc | l: 3 per department_id
```

### Several columns

```
order | o: created_at desc | l: 1 per customer_id, region
```

A group is the rows that share every one of the columns: `PARTITION BY "o_0"."customer_id", "o_0"."region"`.

### After a join

```
customer as c | order .customer_id | o: created_at desc | l: 1 per c.id
```

The column can belong to any table in the expression. Name it by its alias.

### With no `order:`

```
employee | l: 1 per company_id
```

Without an `order:`, each group is ranked by primary key, so the same rows come back every time:

```sql
ROW_NUMBER() OVER (PARTITION BY "e_0"."company_id" ORDER BY "e_0"."id" ASC) AS "__pine_row__"
```

The key is the table's declared primary key, not a column called `id`. After a join, the current table's key comes first, then the other tables' keys. A table with no primary key, such as a view, or a named result, adds nothing. When nothing is left, the window has no `ORDER BY` and the database picks the rows.

### Steps after it

```
ticket_status | o: created_at desc | l: 1 per ticket_id | where: status = 'open'
```

Tickets whose latest status is open. The `where:` filters the rows kept per group: it doesn't choose which rows are kept. Like a plain `limit:`, `per` is a [checkpoint](checkpoints.md). Anything that follows it, such as `where:`, `select:`, `order:`, a join or `|= name`, works on the per-group result, which is sealed into a CTE:

```sql
WITH "__pine_0__" AS (
  SELECT "__pine_rows__"."id", "__pine_rows__"."ticket_id", "__pine_rows__"."status", "__pine_rows__"."created_at"
  FROM ( SELECT ..., ROW_NUMBER() OVER (PARTITION BY "ts_0"."ticket_id" ORDER BY "ts_0"."created_at" DESC) AS "__pine_row__", ...
         FROM "ticket_status" AS "ts_0" ) AS "__pine_rows__"
  WHERE "__pine_rows__"."__pine_row__" <= 1
  ORDER BY "__pine_rows__"."__pine_order__"
)
SELECT "__pine_0__".* FROM "__pine_0__" AS "__pine_0__" WHERE "__pine_0__"."status" = ? LIMIT 250
```

To filter first and then rank, put the `where:` before `limit:`.

## How it works

- The query is built as usual, with one more column: each row's place in its group, `ROW_NUMBER() OVER (PARTITION BY <per columns> ORDER BY <order>)`.
- A row number can't be filtered on in the query that computes it, so that query becomes a derived table, `__pine_rows__`. An outer query keeps the rows numbered up to N.
- The outer query lists its columns one by one, so the row numbers never reach the result. `*` is spelled out from the schema index.
- The result keeps the `order:` as its order. The columns `order:` names may not be in the result, so the inner query adds a second row number in that order, `__pine_order__`, and the outer query sorts by it. The partition columns are not put first.
- The result is capped at 250 rows, like any query. The cap applies to the outer query, after the per-group cut. To raise it, add a plain limit after it: `| limit: 1000`.
- A derived table can't have two columns with the same name. When names clash, for example `c.id` next to `o.*`, each clashing column is named after its position inside (`__pine_c0__`) and gets its own name back outside.
- `ROW_NUMBER()` works on Postgres, MySQL 8 and SQLite. Pine doesn't use `DISTINCT ON`, which only Postgres has.

## Constraints

- Only `limit:` takes `per`. `select:` doesn't.
- A table that isn't in the schema index can't be used with `*`: its columns can't be listed. Re-index the connection, or name the columns with `select:` first.
- A `delete!` or `update!` after `per` changes the rows kept per group.
- A column the access policy hides can't follow `per`. Grouping by it would give its values away, as `group:` would.
- Ties in the `order:` are broken by the database, as with a plain `limit:`. No key is added to break them.
- The SQL of a `per` query doesn't convert back to Pine in the SQL panel yet: it has a subquery, which [SQL to Pine](sql-to-pine.md) refuses.
- Steps after `per` don't keep its order, as after a plain `limit:`. Add an `order:` after them to sort the final result.

See also: [checkpoints.md](checkpoints.md), [named-results.md](named-results.md).

---

## Implementation

### Grammar (`pine.bnf`)

```
LIMIT         := <limit-op> integer (<per-op> columns)?  |  integer
LIMIT-PARTIAL := ... (* `l: 1 per`, `l: 1 per a, `, `l: 1 per c.` *)
per-op        := <ws+> <"per"> <ws+>
```

### Parser (`parser.clj`)

`limit: 1 per customer_id, c.id` becomes:

```clojure
{:type :limit :value 1 :per [{:column "customer_id"} {:alias "c" :column "id"}]}
```

`:value` stays the number, so code that reads a plain limit is unchanged. A plain `limit:` has no `:per`. While the columns are typed, the operation is `:limit-partial`, with the columns so far in `:per` and an optional `:partial-alias`.

### AST (`ast/limit.clj`)

A `per` limit sets `:limit-per` on the state, and leaves `:limit` empty:

```clojure
{:n 1
 :columns [{:alias "o_0" :column "customer_id" :operation-index 2}]  ; resolved like order: columns
 :order [{:alias "o_0" :column "created_at" :direction "DESC" ...}]  ; ranks rows in a group
 :star-columns {"o_0" ["id" "customer_id" "user_id"]}}               ; what `o_0.*` is
```

- `:order` is the state's `:order` at that point, or the primary keys when there is none.
- `:star-columns` is recorded here because the schema index is gone by the time the SQL is built.
- `:limit` and `:limit-partial` are checkpoint operations (`checkpoint-op-types` in `ast/main.clj`).

### Hints (`ast/hints.clj`)

After `per`, column hints work as for `order:`: all columns of the current table (or of the alias before a dot), less the ones already listed, or the ones that match a column being typed. They are filed under `:hints :select`, which clients complete as `column, `.

### SQL (`eval.clj`)

`build-bare-select` sends a state with `:limit-per` to `build-per-group-select`. The access policy check (`access-policy/check-references`) covers the per columns. `scoped?` counts `:limit-per`, so a `delete!` or `update!` after it is allowed.
