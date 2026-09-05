(ns pine.ast.hints
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [pine.ast.table :as table]))

(defn- filter-relations
  "Given a partial token (which can be completed to a table) and all the
  candidate relations, we filter out the unrelated candidates"
  [token candidates]
  (filter #(-> %1
               first
               clojure.string/lower-case
               (clojure.string/includes? token)) candidates))
;; ---------------------------------------------------------------------------
;; Table Hints
;; ---------------------------------------------------------------------------

(defn- known-variables
  "Every variable currently in scope: already-defined ones plus any assigned
  earlier in this same expression."
  [state]
  (merge (:variables state) (:pending-assignments state)))

(defn- schemas-containing-table
  "Every schema (from the schema-qualified index) that has this table. Used
  as a fallback for a table with no FK/heuristic relation of its own (so
  :in was never set on it) but that's still a real, indexed table."
  [state table]
  (for [[schema-name schema-data] (-> state :references :schema)
        :when (contains? (:table schema-data) table)]
    schema-name))

(defn create-hint-from-table [state tables]
  (let [refs      (-> state :references :table)
        variables (known-variables state)]
    (mapcat identity
            (for [table tables
                  :let [in-schemas (->> table refs :in keys)
                        schemas (if (seq in-schemas)
                                  in-schemas
                                  (schemas-containing-table state table))]]
              (if (contains? variables table)
                [{:schema nil :table table}]
                (for [schema schemas]
                  {:schema schema :table table}))))))

(defn table-hints
  "No context - get all the tables matching the token. Includes both real
  tables (from the schema-derived references map) and any variables in
  scope, which aren't real tables and so never appear in that map."
  [state token]
  (let [real-candidates (-> state :references :table)
        variable-candidates (map vector (keys (known-variables state)) (repeat nil))
        suggestions (filter-relations token (concat real-candidates variable-candidates))]
    (create-hint-from-table state (->> suggestions (map first) distinct (sort-by count)))))
;; ---------------------------------------------------------------------------
;; Relation Hints
;; ---------------------------------------------------------------------------

(defn- generate-expression [{:keys [schema table column related-column parent resolution alias]}]
  ;; The `.hint_col` a user types to disambiguate always names the *child*'s FK
  ;; column (see docs/joins.md - the via map is keyed by it), which is `column`
  ;; when this table is the child (parent false) but `related-column` (the
  ;; context table's own column) when this table is the parent (parent true).
  ;; Skip it entirely for a synthetic join: there's exactly one synthetic
  ;; id=id entry, never ambiguous, so no disambiguator is needed - keeps
  ;; `var_x | var_y` as the canonical form instead of `var_x | var_y .id`.
  (let [hint-column (when-not (= resolution "synthetic")
                      (if parent related-column column))]
    (str (if schema (str schema ".") "") table
         (if alias (str " as " alias) "")
         (if hint-column (str " ." hint-column) "")
         (if parent " :parent" ""))))

;; via-details look like:
;; ["z"  "document"      "created_by"  :refers-to   "y"  "employee" "id"]
;;
;; Position 1/2 (table/col) is always the context table (the one we're generating
;; hints from) and position 5/6 (f-table/f-col) is always the suggested table -
;; regardless of :refers-to vs :referred-by direction - because relation-hints
;; always looks the via map up keyed by the context table. See docs/joins.md.
;;
;; resolution-of (table/resolution-of - shared with ast/table.clj, which tags
;; committed joins in ast.joins the same way) only ever sees :foreign-key/
;; :heuristic here - a real reference-map entry, tagged at schema-index time
;; (db/postgres.clj). "synthetic" (the third :resolution value - see
;; docs/joins.md) never comes from a via-details tag at all; it's set
;; directly wherever a same-source join is fabricated on the fly (below),
;; since nothing in the references map describes it.
(defn- create-hint-from-relation-array
  "context-rename/target-rename translate the context's/suggested table's own
  column back through whatever name a variable exposes it under on that side
  (see table/translate-column) - dropped entirely when either side's column
  isn't actually exposed there (a restricted variable that never selected it),
  since that relation isn't reachable through the CTE at all."
  [table via-details context-rename target-rename]
  (keep (fn [vd]
          (let [direction (nth vd 3)
                column (table/translate-column target-rename (nth vd 6))
                related-column (table/translate-column context-rename (nth vd 2))]
            (when (and column related-column)
              {:schema (nth vd 4)
               :table table
               :column column
               :related-column related-column
               :parent (= direction :refers-to)
               :resolution (table/resolution-of vd)})))
        via-details))

(defn- variables-resolving-to
  "Every known variable that includes real-table among its own resolved
  source(s), alongside the rename it applies for that specific source -
  i.e. every variable that could stand in for real-table as a join target."
  [state real-table]
  (for [[vname var-ast] (known-variables state)
        candidate (table/resolve-table {:table vname :schema nil :ast var-ast})
        :when (= real-table (:table candidate))]
    [vname (:rename candidate)]))

(defn- has-id-column? [state real-table]
  (some #(= "id" (:column %)) (get-in state [:references :table real-table :columns])))

(defn relation-hints
  "Suggestions for what can be piped in next from the current table/variable.
  Resolves the context through its real source table(s) first (a no-op for a
  real table), so the same schema-derived relationships used for real joins
  drive hints too - then layers in cases the plain schema can't express on
  its own, all variants of the same same-source synthetic join (see
  docs/variables.md): another variable standing in for a real target (mirrors
  seeding a real relationship onto a variable), two variables sharing the
  same source with no real FK between them, and - symmetrically - the source
  table itself, when the context is a variable that only restrictedly
  resolves to it (e.g. `aggregate | tenant`, the reverse of `tenant |
  aggregate`)."
  [state token]
  (let [from-alias (state :context)
        from-entry (-> state :aliases (get from-alias))
        from-name  (:table from-entry)
        sources    (table/resolve-table from-entry)
        hints
        (mapcat
         (fn [{:keys [table schema rename]}]
           (let [parents    (-> state :references :table (get table) :refers-to)
                 children   (-> state :references :table (get table) :referred-by)
                 real-rel   (seq (concat parents children))
                 real-hints (mapcat
                             (fn [[target relation]]
                               (let [via (get-in relation [:via])
                                     via-details (mapcat identity (vals via))]
                                 (create-hint-from-relation-array target via-details rename {})))
                             real-rel)
                 variable-hints (mapcat
                                 (fn [[target relation]]
                                   (let [via (get-in relation [:via])
                                         via-details (mapcat identity (vals via))]
                                     (mapcat
                                      (fn [[vname var-rename]]
                                        (when (not= vname from-name)
                                          (create-hint-from-relation-array vname via-details rename var-rename)))
                                      (variables-resolving-to state target))))
                                 real-rel)
                 same-source-hints (when (has-id-column? state table)
                                     (keep
                                      (fn [[vname var-rename]]
                                        (when (not= vname from-name)
                                          (let [column (table/translate-column var-rename "id")
                                                related-column (table/translate-column rename "id")]
                                            (when (and column related-column)
                                              {:schema nil :table vname
                                               :column column
                                               :related-column related-column
                                               :parent false
                                               :resolution "synthetic"}))))
                                      (variables-resolving-to state table)))
                 ;; The context is itself a restricted variable (non-empty
                 ;; rename) - the source table it traces back to is just as
                 ;; valid a same-source candidate as another variable would
                 ;; be, since it's the same table both times, just reached
                 ;; through a snapshot on one side. Only fires if the
                 ;; context's own rename actually exposes "id" - a restricted
                 ;; variable that never selected id can't reach this join.
                 self-source-hint (when (and (seq rename) (has-id-column? state table) (not= table from-name))
                                    (when-let [related-column (table/translate-column rename "id")]
                                      [{:schema schema :table table
                                        :column "id"
                                        :related-column related-column
                                        :parent false
                                        :resolution "synthetic"}]))]
             (concat real-hints variable-hints same-source-hints self-source-hint)))
         sources)]
    (->> hints
         (filter #(str/includes? (str/lower-case (:table %)) token))
         (map (fn [h]
                (if (contains? (known-variables state) (:table h))
                  (assoc h :schema nil)
                  h)))
         distinct
         ;; Smaller (shorter-named) tables first, e.g. `company` before
         ;; `company_structure` — mirrors the sort already done in table-hints.
         (sort-by (comp count :table)))))

;; ---------------------------------------------------------------------------
;; Path Hints (`? target-table` - all join chains between two tables)
;; ---------------------------------------------------------------------------

;; A densely-connected schema (e.g. most tables FK'd to a shared tenant/company
;; hub) can otherwise make the search explode combinatorially - these keep it
;; bounded rather than trying to be individually "smart" about which paths
;; matter more.
(def ^:private max-path-depth 4)
(def ^:private max-paths 50)

(defn- real-relation-hints
  "Single-hop candidates from a REAL table (own schema, with `rename`
  translating its columns back through whatever a variable might expose them
  as). This is the real-relation half of relation-hints's per-source lookup,
  generalized so multi-hop path search can call it again for every
  intermediate hop - always with an empty rename past the first hop, since
  every hop after the first is a real table, never a variable."
  [state table rename]
  (let [parents  (-> state :references :table (get table) :refers-to)
        children (-> state :references :table (get table) :referred-by)]
    (mapcat
     (fn [[target relation]]
       (create-hint-from-relation-array
        target (mapcat identity (vals (:via relation))) rename {}))
     (concat parents children))))

(defn- path-priority
  "A path's sort key: fewest direction changes first, then fewest total
  hops as the tiebreak. Lower sorts first.

  A run of hops all in the same direction (all child, e.g.
  company->employee->document, or all parent, e.g. the same route read
  backwards) is a coherent 'zoom in' or 'zoom out' - equally meaningful
  either way, which is why this only counts CHANGES of direction, not
  parent hops specifically (an earlier version of this penalized parent
  hops directly, which wrongly favored the child direction over the
  parent one - they're symmetric). A path that hops up and then back down
  (or vice versa) is a detour through a branch unrelated to either table,
  and is only worth taking when nothing more direct exists.

  This is turn-penalty shortest path (the technique road-network routers
  use to penalize a U-turn/reversal, not just distance) - a hop's cost
  depends on the direction of the hop *before* it, not just the hop
  itself. It's solved here the standard way: rather than track '(table,
  last-direction)' as explicit search state, the cost is just recomputed
  from the path so far each time (cheap at these lengths) - a direction
  change between consecutive hops costs 1, staying the course costs 0.
  Since a path can only ever gain turns as it's extended (never lose
  one), this still makes find-table-paths below a uniform-cost search
  (Dijkstra's algorithm) rather than plain breadth-first search: paths
  are popped from the frontier in non-decreasing total cost, so a longer
  but direction-pure route can outrank a shorter one that backtracks."
  [hops]
  (let [turns (->> (partition 2 1 hops)
                   (filter (fn [[a b]] (not= (:parent a) (:parent b))))
                   count)]
    [turns (count hops)]))

(defn- find-table-paths
  "All simple paths from each of `sources` to `target-table` (optionally
  narrowed to `target-schema`), walking the same FK/heuristic graph
  relation-hints walks one hop at a time, popped off the search frontier in
  path-priority order (see above) rather than level-by-level - so results
  already come back best-first, not just grouped by depth. A table already
  used earlier in a given path is never revisited (simple paths only) -
  this is also why searching a table against itself (`company | ? company`)
  comes back empty with no special-casing: the source is in its own path's
  visited set from the start.

  A path is only ever tested against the target when it's POPPED off the
  frontier (in priority order), never at the moment it's created - since
  path-priority only ever grows as a path is extended (a hop is worth 0 or
  1, monotonically), whatever gets popped next is guaranteed to be at
  least as good as anything still waiting, so `found` fills up in true
  priority order rather than whatever order paths happened to be
  discovered in (this is the standard uniform-cost/Dijkstra optimality
  argument, just enumerating every hit instead of stopping at the first).
  A path stops being expanded once it reaches max-path-depth hops - unlike
  a global depth counter, this lets a genuinely longer path keep being
  explored even while shorter, lower-priority paths elsewhere in the
  frontier are still being popped first."
  [state sources target-table target-schema]
  (let [target? (fn [hop]
                  (and (= (:table hop) target-table)
                       (or (nil? target-schema) (= (:schema hop) target-schema))))]
    (loop [frontier (for [{:keys [table rename]} sources]
                      {:visited #{table} :hops [] :table table :rename rename})
           found []]
      (if (or (empty? frontier) (>= (count found) max-paths))
        found
        (let [sorted (sort-by #(path-priority (:hops %)) frontier)
              {:keys [visited hops table rename]} (first sorted)
              rest-frontier (rest sorted)]
          (if (and (seq hops) (target? (last hops)))
            (recur rest-frontier (conj found hops))
            (let [expanded (when (< (count hops) max-path-depth)
                             (->> (real-relation-hints state table rename)
                                  (remove #(contains? visited (:table %)))
                                  (map (fn [hop] {:visited (conj visited (:table hop))
                                                  :hops (conj hops hop)
                                                  :table (:table hop)
                                                  :rename {}}))))]
              (recur (concat rest-frontier expanded) found))))))))

(defn- reachable-table-names
  "Every real table name reachable from `sources` within max-path-depth hops
  - not full paths (see find-table-paths), just which table *names* are
  reachable at all. Cheaper than full path enumeration (a table is only
  ever expanded once, however many parallel edges lead to it) and is what
  actually answers 'is this a valid destination', since a table this
  doesn't include is guaranteed to come back with zero paths once fully
  named anyway. A source table itself is never included - it's excluded
  from its own reachable set from the start, the same reason
  find-table-paths always returns empty for `t | ? t`."
  [state sources]
  (let [source-tables (into #{} (map :table sources))]
    (loop [frontier (for [{:keys [table rename]} sources] {:table table :rename rename})
           seen source-tables
           depth 0]
      (if (or (empty? frontier) (>= depth max-path-depth))
        (set/difference seen source-tables)
        (let [next (->> frontier
                        (mapcat (fn [{:keys [table rename]}] (real-relation-hints state table rename)))
                        (remove #(contains? seen (:table %)))
                        (map (fn [hop] {:table (:table hop) :rename {}}))
                        distinct)]
          (recur next (into seen (map :table next)) (inc depth)))))))

(defn- path->hint
  "Render one discovered path as {:pine ... :length N :hops [...]}. Each hop
  keeps the same shape a single-hop table hint already has (schema/table/
  column/related-column/parent/resolution/pine) - a path is a subset of that,
  not a new shape - and the path's own :pine is just those hops' :pine joined
  the same way the user would type them: piped, in order."
  [hops]
  (let [hops (mapv #(assoc % :pine (generate-expression %)) hops)]
    {:pine (str/join " | " (map :pine hops))
     :length (count hops)
     :hops hops}))

(defn generate-path-hints
  "Dispatch for a `? token` operation. While `token` isn't (yet) the exact
  name of a real table, this is still table-name typeahead (:table bucket)
  rather than the path search itself - but narrowed to table-hints ∩
  reachable-table-names, not every table in the schema: a path can be many
  hops away, so this isn't limited to direct joins from the current
  context, but it IS limited to tables a path could actually reach, since
  anything outside that set is guaranteed to resolve to zero paths the
  moment it's fully typed. Only once `token` names a real table does this
  run the actual path search (:paths bucket)."
  [state]
  (let [{token :table target-schema :schema} (-> state :operation :value)
        current-entry (-> state :aliases (get (state :current)))
        sources (when current-entry (table/resolve-table current-entry))]
    (if (and (seq token) (contains? (-> state :references :table) token))
      (let [paths (find-table-paths state sources token target-schema)]
        {:key :paths :hints (map path->hint paths)})
      (let [reachable (reachable-table-names state sources)]
        {:key :table
         :hints (->> (table-hints state token)
                     (filter #(contains? reachable (:table %)))
                     (map (fn [h] (assoc h :pine (generate-expression h)))))}))))

(defn generate-table-hints [state]
  (let [{token :table parent :parent} (-> state :tables reverse first)
        from-alias (state :context)
        table-hints (if from-alias
                      ;; This is not the first table, then filter out the related tables
                      (relation-hints state token)
                      ;; This is the first table - get all the tables matching the token
                      (table-hints state token))
        ;; Filter by parent if specified
        table-hints (if parent
                      (filter #(= (:parent %) true) table-hints)
                      table-hints)
        ;; Add pine expression to each hint
        add-pine-expression (fn [h] (assoc h :pine (generate-expression h)))]
    (map add-pine-expression table-hints)))

(defn generate-all-column-hints
  ([state] (generate-all-column-hints state (state :current))) ;; Overload for default `a`
  ([state a]
   (let [entry (get (state :aliases) a)
         explicit-cols (when (:ast entry) (remove :auto-id (:columns (:ast entry))))]
     (if (or (not (:ast entry)) (empty? explicit-cols))
       ;; Real table, or a variable with no explicit columns (`*`) - the CTE
       ;; implicitly selects everything, so the column list (names, types)
       ;; is exactly the real underlying table's own schema. Resolve through
       ;; the same one-hop rule join resolution uses to reach it, even
       ;; through a chain of variables.
       (mapcat (fn [{:keys [table schema]}]
                 (let [columns (if schema
                                 (get-in state [:references :schema schema :table table :columns])
                                 (get-in state [:references :table table :columns]))]
                   (map #(-> % (select-keys [:column]) (assoc :alias a)) columns)))
               (table/resolve-table entry))
       ;; Explicit columns: only what was actually selected, under its exposed name.
       (->> explicit-cols
            (map #(or (:column-alias %) (:column %)))
            distinct
            (map (fn [column-name] {:column column-name :alias a})))))))

(defn find-relevant-columns [hints column]
  (if column
    (filter #(str/includes? (:column %) (:column column)) hints)
    hints))

(defn exclude-columns [hints columns]
  (if (seq columns)
    (filter (fn [hint]
              (not (some #(= (:column hint) (:column %)) columns)))
            hints)
    hints))

(defn- column-hint-sort-key [hint]
  (let [c (:column hint)]
    [(count c) (str/lower-case c)]))

(defn- sort-column-hints [hints]
  (vec (sort-by column-hint-sort-key hints)))

(defn generate-where-hints [state]
  ;; Where conditions need custom logic unlike order/select partials because:
  ;; - Order partial: simple exclude-already-selected logic works with generic generate-column-hints
  ;; - Where partial: complex state-dependent filtering based on partial completion:
  ;;   * `where:` → show all columns
  ;;   * `w: i` → filter to columns matching "i"
  ;;   * `w: e.i` → filter to columns matching "i" from alias "e"
  ;;   * `w: id =` → show all columns (ready for next condition)
  (let [operation (state :operation)
        where-data (get operation :value {})
        partial-condition (:partial-condition where-data)
        current-alias (state :current)
        lookup-alias (or (and partial-condition (:alias partial-condition)) current-alias)]
    (if (nil? partial-condition)
      ;; No partial condition yet, show all columns
      (generate-all-column-hints state current-alias)
      ;; Has partial condition, filter based on it
      (if (and (:column partial-condition) (not (contains? partial-condition :operator)))
        ;; Just a column specified — use the condition's alias if present, else current
        (find-relevant-columns
         (generate-all-column-hints state lookup-alias)
         partial-condition)
        ;; Column + operator, show all columns for next condition
        (generate-all-column-hints state current-alias)))))

(defn generate-update-hints [state]
  (let [assignments (get-in state [:update :assignments] [])
        partial-column (get-in state [:update :partial-column])
        assigned-columns (map (fn [a] {:column (get-in a [:column :column])}) assignments)
        lookup-alias (or (and partial-column (:alias partial-column)) (state :current))
        hints (-> (generate-all-column-hints state lookup-alias)
                  (exclude-columns assigned-columns))]
    (if partial-column
      (find-relevant-columns hints partial-column)
      hints)))

(defn generate-column-hints [state columns]
  ;; Generic column hints logic that works for most operations:
  ;; - Complete operations (select, order): filter hints to match the column being typed, using its alias
  ;; - Partial operations (select-partial, order-partial): exclude already-selected columns,
  ;;   defaulting to the current context table so the next slot shows relevant columns
  ;; - Note: where-partial needs custom logic (see generate-where-hints)
  (let [column (some-> columns reverse first)
        type (-> state :operation :type)
        ;; For partial ops, always use the current context table — the user hasn't started typing
        ;; the next column yet, so the last completed column's alias is irrelevant.
        ;; For complete ops, use the typed column's alias when it was added in the current scope.
        a (if (or (= type :select-partial) (= type :order-partial))
            (or (get-in state [:operation :partial-alias :alias]) (state :current))
            (if (and (seq column)
                     (column :operation-index)
                     (> (column :operation-index) (state :current-index)))
              (column :alias)
              (state :current)))
        hints (generate-all-column-hints state a)]
    (cond
      ;; If the type is :select, :order, or :where and columns exist, filter hints using the columns
      (and (or (= type :select) (= type :order) (= type :where)) column)
      (find-relevant-columns hints column)

      ;; If the type is :select-partial, :order-partial, or :where-partial and columns exist
      (and (or (= type :select-partial) (= type :order-partial) (= type :where-partial)) columns)
      (exclude-columns hints (filter #(= (:alias %) a) columns))

      ;; Otherwise, return all hints
      :else hints)))

(defn handle
  "Generate hints based on the current operation.
   If truncated-state is provided, use it for hint generation context."
  ([state]
   (handle state nil))
  ([state truncated-state]
   (let [state-for-hints (or truncated-state state)
         op-type (-> state-for-hints :operation :type)]
     ;; :paths is handled separately: unlike every other op-type, which bucket
     ;; it lands in (:table vs :paths) depends on the search's runtime result,
     ;; not on op-type alone (see generate-path-hints).
     (if (= op-type :paths)
       (let [{:keys [key hints]} (generate-path-hints state-for-hints)]
         (assoc-in state [:hints key] (or hints [])))
       (let [hints (case op-type
                     :table (generate-table-hints state-for-hints)
                     :select (generate-column-hints state-for-hints (state-for-hints :columns))
                     :select-partial (generate-column-hints state-for-hints (state-for-hints :columns))
                     :order-partial (generate-column-hints state-for-hints (state-for-hints :order))
                     :order (generate-column-hints state-for-hints (state-for-hints :order))
                     :where-partial (generate-where-hints state-for-hints)
                     :where (generate-all-column-hints state-for-hints)
                     :update-partial (generate-update-hints state-for-hints)
                     [])
             hints (if (#{:select :select-partial :order :order-partial
                          :where :where-partial :update-partial}
                        op-type)
                     (sort-column-hints hints)
                     hints)
             hint-key (case op-type
                        :select-partial :select
                        :order-partial :order
                        :where-partial :where
                        :update-partial :update
                        op-type)]
         (assoc-in state [:hints hint-key] (or hints [])))))))
