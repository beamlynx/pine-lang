# Paths

Ask Pine how two tables connect and it searches the same FK/heuristic graph [joins](joins.md) resolves one hop
at a time, but for every join chain between them — not just the next hop.

## Why

A single pipe (`table_a | table_b`) already shows what you can join to *next*. It doesn't answer "how would I
even get from `company` to `document`" when the real connection is two or three hops away through a table you
hadn't thought to name. `? table` runs that search and hands back every route it found, each one a real Pine
expression you can drop straight into the pipe.

## Syntax

```
table_a ? table_b
table_a | table_b | ? table_c
```

`?` is its own operation (`PATHS` in the grammar), composed with `|` exactly like any other operation — so it
can follow an arbitrary Pine expression, not just a bare table name. It has no table-mods of its own (no
`as alias`, no `.hint_col`, no `:parent`): the named table is a search destination, not something being
joined directly, so there's nothing to disambiguate. It's also terminal — nothing meaningful follows it in the
same pipe, though the grammar doesn't specifically forbid it, the same as `count:`/`delete:` today.

## Examples

### Direct and multi-hop

`company`, `employee`, and `document` are related: `employee.company_id → company.id`,
`document.company_id → company.id`, and `document` also has two FKs to `employee` (`employee_id`,
`created_by`).

```
company | ? document
```

Returns three paths. All three routes here stay in one direction the whole way (company owns employee owns
document), so the ordering is just shortest-first — see [Constraints](#constraints) for what changes once a
route has to double back through a *different* direction instead:

```
document .company_id
employee .company_id | document .employee_id
employee .company_id | document .created_by
```

### Composed on top of an existing expression

```
company | where: active = true | ? document
```

The search starts from wherever the pipe left off — here, `company` with a `where` already applied — not from
a bare table name. Everything before the `?` is an ordinary, fully composable Pine expression.

### Typing the target

While the table name after `?` is still being typed (or doesn't match a real table), `hints.paths` stays
empty and `hints.table` serves table-name suggestions instead — narrowed to tables actually reachable from the
current context within the hop cap below, not every table in the schema. It isn't limited to *direct* joins
(a path can be several hops away), but a table with no path there at all is guaranteed to resolve to zero
paths the moment it's fully typed, so it's excluded from the suggestions too. Once the token names a real
table, `hints.paths` fills with the search results.

## Constraints

- Only simple paths (no table visited twice) are considered, so `company | ? company` and a self-referential
  FK (e.g. `employee.reports_to`) never produce a path back to the table already in scope.
- Ordered by fewest *direction changes* first, then by fewest total hops as the tiebreak — not shortest-first
  alone. A run of hops that's all "child" (zooming in, e.g. `company` to its own `employee`) or all "parent"
  (zooming out, e.g. `document` back up to the `employee` that owns it) is a coherent route in one direction;
  neither direction is favored over the other. What's penalized is a route that switches — a parent hop
  followed by a child hop, or vice versa — since that's a detour through a branch unrelated to either table.
  Concretely: if `department` has a denormalized shortcut column pointing straight at a `worker` two levels
  down, and that same `worker` has its own `shift`s, then `department | ? shift` returns the three-hop route
  through the real `team`/`worker` hierarchy (all child hops) ahead of the two-hop route that takes the
  shortcut and then has to double back down to `shift` (a direction change) — both are valid paths, but the
  direction-pure one is the more meaningful answer. When every candidate route already stays in one direction
  (as in the example above), this ordering and shortest-first coincide.
- Capped at 4 hops and 50 total results, to keep a densely-connected schema (e.g. most tables FK'd to a shared
  tenant/company hub) from exploding combinatorially. This is not a guarantee that every existing path is
  found — only that the search stays bounded, and that whichever paths it does return are the best-ordered
  ones (see above), not an arbitrary depth-level slice.
- `? table` never produces SQL. Evaluating a pine expression that ends in one is a no-op (same as bare
  `delete:`) — pick one of the returned paths and build *that* expression instead.

## Implementation

`ast/hints.clj`'s `find-table-paths` runs a *turn-penalty* shortest-path search — the technique road-network
routers use to penalize a U-turn/reversal, not just distance — over the same `:refers-to`/`:referred-by`
reference map [joins.md](joins.md#reference-map-structure-dbpostgresclj) already builds, calling
`real-relation-hints` (the real-relation half of `relation-hints`, generalized to run again at every
intermediate hop) at each step. A hop's cost depends on the hop before it, not just the hop itself — 0 if it
continues in the same direction (both parent, or both child), 1 if it changes direction — so candidate routes
are popped from the search frontier in `path-priority` order (`[direction-change-count, total-hop-count]`,
ascending) rather than level-by-level: this is a uniform-cost search (Dijkstra's algorithm), the same family of
algorithm as plain breadth-first search, just generalized from unweighted edges to edges whose cost depends on
the direction of the previous one. The search starts from `resolve-table` on `:current` — whatever real table
or variable the preceding pipe left off at, since the `:paths` operation itself adds no table/join of its own
(see `ast/main.clj`'s `handle-op`). Each hop keeps the same shape a single-hop table hint already has
(schema/table/column/related-column/parent/resolution/pine) — a path is a subset of that, not a new shape.
