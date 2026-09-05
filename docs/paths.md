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

Returns three paths - but not shortest first:

```
employee .company_id | document .employee_id
employee .company_id | document .created_by
document .company_id
```

`document.company_id` is the shortest route, but it's also a denormalized copy of what
`company | employee | document` already reaches — the classic "every row also stores its tenant id" column —
so it ranks last despite being one hop instead of two. See [Constraints](#constraints) for exactly what that
means and why.

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
- Ordered by fewest *transitively redundant* hops first, then fewest *direction changes*, then fewest total
  hops as the final tiebreak — not shortest-first alone.
  - A hop is **transitively redundant** if the table it lands on is already reachable another way, through a
    longer chain of hops in the same direction. `document.company_id` is the textbook case: it duplicates
    what `company | employee | document` already reaches, the same "every row also stores its tenant id"
    column that denormalized multi-tenant schemas commonly add for query convenience. It isn't wrong to use,
    but it's the least meaningful reason two tables are "connected", so it ranks behind any non-redundant
    route regardless of length. A shortcut like `department.lead_worker_id` (a department's directly-assigned
    lead worker) is different: `department` has no *other* route to `worker` at all, so it isn't a duplicate
    of anything and isn't penalized here — it's a genuinely distinct relationship that only happens to also
    be reachable, less directly, through `team`.
  - A **direction change** is a route that hops "up" (parent) then "down" (child), or vice versa, partway
    through. A run of hops that's all child (zooming in, e.g. `company` to its own `employee`) or all parent
    (zooming out, e.g. `document` back up to the `employee` that owns it) is a coherent route in one
    direction; neither direction is favored over the other. What's penalized is only the switch between them,
    since that's a detour through a branch unrelated to either table. Concretely: `department | ? shift`
    returns the three-hop route through the real `team`/`worker` hierarchy (all child hops) ahead of a
    two-hop route that takes the `lead_worker_id` shortcut and then has to double back down to `shift` (one
    direction change) — both are valid, but the direction-pure one is the more meaningful answer.

  When every candidate route is equally non-redundant and equally direction-pure (or the schema has no
  redundant/shortcut edges at all, as most of it doesn't), this ordering and shortest-first coincide.
- Capped at 4 hops, 10 total results, and 150ms of search time, to keep a densely-connected schema (e.g. most
  tables FK'd to a shared tenant/company hub) from exploding combinatorially. The first two bound any one
  path's length and how many are collected once real matches start turning up; the time cap covers what
  neither does on its own — an unreachable, or genuinely rare, target never trips the result-count cap (nothing
  is ever found to count), and a dense enough schema can still take a while to exhaust even at a cheap
  per-step cost. None of this guarantees every existing path is found — only that the search stays bounded, and
  that whichever paths it does return are the best-ordered ones (see above), not an arbitrary depth-level
  slice or a race against the clock.
- `? table` never produces SQL. Evaluating a pine expression that ends in one is a no-op (same as bare
  `delete:`) — pick one of the returned paths and build *that* expression instead.

## Implementation

`ast/hints.clj`'s `find-table-paths` runs a uniform-cost search (Dijkstra's algorithm) over the same
`:refers-to`/`:referred-by` reference map [joins.md](joins.md#reference-map-structure-dbpostgresclj) already
builds, calling `real-relation-hints` (the real-relation half of `relation-hints`, generalized to run again at
every intermediate hop) at each step. A hop's cost isn't fixed - it depends on context, checked fresh each time
by `path-priority`:

- **Redundant?** (`redundant-hop?`) - is the hop's destination already reachable via some *other*
  same-direction route of at least two hops? This is transitive reduction (Aho/Garey/Ullman 1972): the
  standard operation for finding which edges in a graph are shortcuts already implied by a longer path. Costs
  1 if so, 0 otherwise.
- **Direction change?** - does this hop's direction (parent/child) differ from the one before it? This is
  turn-penalty shortest path, the technique road-network routers use to penalize a U-turn/reversal rather than
  just distance - a hop's cost depends on the hop immediately before it. Costs 1 if the direction changed, 0
  otherwise.

Candidate routes are popped from the search frontier in `path-priority` order (`[redundant-hop-count,
direction-change-count, total-hop-count]`, ascending) rather than level-by-level: this is still the same
family of algorithm as plain breadth-first search, just generalized from unweighted edges to edges whose cost
depends on the wider reference graph and the hop before them, rather than being fixed. The search starts from
`resolve-table` on `:current` — whatever real table or variable the preceding pipe left off at, since the
`:paths` operation itself adds no table/join of its own (see `ast/main.clj`'s `handle-op`). Each hop keeps the
same shape a single-hop table hint already has (schema/table/column/related-column/parent/resolution/pine) — a
path is a subset of that, not a new shape.
