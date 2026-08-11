(ns pine.ast.table
  (:require
   [clojure.string :as s]))

(defn resolve-table
  "Given a table-alias entry (as found in state's :aliases), return the real
  source table(s) it resolves to for join purposes: itself, unchanged, if it's
  already real; or the variable's own already-resolved :source(s) if it's a
  variable - a multi-source variable (`s: t.id, c.id`) can resolve to more
  than one. A variable with no explicit columns at all (`*`) implicitly
  selects everything, so it resolves through :current instead - one hop,
  recursing only if :current is itself another variable, since that
  variable's own :source is already fully resolved by the time it can be
  referenced here (see ast/select.clj's column-source).

  Each entry also carries a `:rename` map ({raw-column -> exposed-column}), so
  a column found via the real table's schema can be translated back to
  whatever name the variable actually exposes it under - empty for a real
  table, where no translation is ever needed. Once a variable's data is
  sealed into a CTE, the outer query can only see what the CTE's own
  :columns actually selected - Pine never adds an `id` on its own - but that
  restriction is enforced per-column by translate-column at each actual use
  site, not here: a source table is a valid join candidate regardless of
  which of its columns survived, since most relations (e.g. an FK column
  other than `id`) don't need `id` at all. Only the synthetic same-source
  id=id join genuinely requires it, and that requirement is checked directly
  where that join is built (see same-source-join below, and
  same-source-hints/self-source-hint in ast/hints.clj)."
  [{:keys [table schema ast]}]
  (if ast
    (let [columns (remove :auto-id (:columns ast))]
      (if (empty? columns)
        (when-let [current-alias (:current ast)]
          (resolve-table (get (:aliases ast) current-alias)))
        (->> columns
             (filter :source)
             (group-by :source)
             (map (fn [[source cols]]
                    {:table (:table source)
                     :schema (:schema source)
                     :rename (into {} (map (fn [c] [(:column c) (or (:column-alias c) (:column c))]) cols))})))))
    [{:table table :schema schema :rename {}}]))

(defn translate-column
  "Translate a real column through a resolve-table candidate's rename map.
  Unchanged for an unrestricted candidate - a real table, or a variable's
  implicit `*` - where the rename map is empty and every column is fair game.
  For a restricted candidate (a variable with explicit columns), only the
  columns actually selected are keys in the map at all, so anything else
  returns nil - that column was never exposed by the CTE, so this join path
  isn't reachable through it, full stop. Without this, a variable exposing
  only `id` could still be joined on some *other* column of its source table
  that was never selected, referencing a column the CTE doesn't have."
  [rename col]
  (if (empty? rename) col (get rename col)))

;; via-details look like:
;; ["z"  "document"      "created_by"  :refers-to   "y"  "employee" "id" :foreign-key]
;;
;; Only :foreign-key/:heuristic ever reach here as a real reference-map
;; via-tuple tag (schema-index time, db/postgres.clj) - "synthetic"/"manual"
;; are never derived from one, since neither corresponds to an actual FK or
;; heuristic reference at all (see same-source-join and update-joins below).
(defn resolution-of [vd]
  (case (last vd)
    :foreign-key "fk"
    :heuristic   "heuristic"))

(defn- join-helper
  "Find the references between the tables, get the columns for the first
  reference and return the pair of alias and columns that will be used for the join.
  rename1/rename2 translate t1's/t2's own column back through whatever name a
  variable exposes it under - see translate-column. A restricted candidate
  (non-empty rename) that fails to translate a *real* column (one the via
  lookup actually found) rejects the whole join outright - that relation
  isn't reachable through the CTE. A raw column that was already nil (e.g. an
  invalid explicit .hint_col against a real table) is left alone, unchanged
  from the pre-existing behavior of surfacing a hint-less, unresolved join
  rather than no join at all."
  [references t1 t2 a1 a2 c direction rename1 rename2]
  (when-let [refs (get-in references [:table t1 :referred-by t2 :via])] ;; get references for the tables
    (let [get-col-fn            (if c (fn [_] c) (fn [xs] (if xs (first xs) nil)))
          col-key               (-> refs keys get-col-fn)
          join                  (-> (get-in refs [col-key]) reverse first)
                                ;; Normally there is only one foreign key but if there
                                ;; multiple, then we use the last one which is `id`

          [_ _ raw-col _ _ _ raw-f-col] join ;; [ schema table col r f-schema f-table f-col ]
          col                   (translate-column rename1 raw-col)
          f-col                 (translate-column rename2 raw-f-col)
          rejected?             (or (and (some? raw-col) (seq rename1) (nil? col))
                                    (and (some? raw-f-col) (seq rename2) (nil? f-col)))
                                ;; `join` is nil for an invalid explicit .hint_col (no via
                                ;; entry matched col-key) - resolution-of throws on a nil/
                                ;; unmatched tag, so guard it the same way the columns above
                                ;; already tolerate a nil raw-col/raw-f-col.
          resolution            (when join (resolution-of join))]
      (when-not rejected?
        (if (= direction :of)
          [a2 f-col :of a1 col resolution]
          [a1 col :has a2 f-col resolution])))))

(defn- has-id-column? [references {:keys [table schema]}]
  (let [columns (if schema
                  (get-in references [:schema schema :table table :columns])
                  (get-in references [:table table :columns]))]
    (some #(= "id" (:column %)) columns)))

(defn- same-source-join
  "Fallback for two DISTINCT sides that resolve to the same real source table
  but aren't connected by a real FK: allow a synthetic id=id join, each side
  using its own exposed name for it. This covers two variables both wrapping
  `customer`, and just as much a real table joined to a variable that
  happens to trace back to that same table (e.g. `tenant | aggregate`,
  where `aggregate` was built from `tenant` via a restricted `s: id, ...`
  chain) - the variable case isn't special, both are just two references to
  the same table that the schema alone can't connect.

  Requires at least one side to actually be a variable, though: two RAW
  references to the same table (`customer | customer`) still don't resolve -
  Pine has no way yet to tell those two occurrences apart (no `t | t as t2`
  self-aliasing), so joining them would be ambiguous about which occurrence
  is which. A variable is never ambiguous this way - it's already a distinct,
  named snapshot - so the same-source join is only actually meaningless when
  NEITHER side has that identity. Also never fires for a variable joined to
  itself (same identity on both sides) - matching Pine's general lack of
  self-join support (see docs/variables.md).

  Each side's `id` must also actually survive translation - a restricted
  variable that never selected `id` doesn't expose it, so translate-column
  returns nil and that pairing is skipped, same as any other unreachable
  column (see translate-column)."
  [references variable1? variable2? distinct-variables? candidates1 candidates2 a1 a2]
  (when (and (or variable1? variable2?) distinct-variables?)
    (first
     (for [{t1 :table rename1 :rename :as c1} candidates1
           {t2 :table rename2 :rename} candidates2
           :let [id1 (translate-column rename1 "id")
                 id2 (translate-column rename2 "id")]
           :when (and (= t1 t2) (has-id-column? references c1) id1 id2)]
       [a1 id1 :has a2 id2 "synthetic"]))))

;; TODO: use spec for the state value i.e. first arg
(defn- join-tables [{:keys [references aliases]} x y c parent]
  (let [a1 (x :alias)
        a2 (y :alias)
        alias1 (aliases a1)
        alias2 (aliases a2)
        candidates1 (resolve-table alias1)
        candidates2 (resolve-table alias2)
        try-direction (fn [cs1 cs2 aa1 aa2 direction]
                        (first
                         (for [{t1 :table rename1 :rename} cs1
                               {t2 :table rename2 :rename} cs2
                               :let [result (join-helper references t1 t2 aa1 aa2 c direction rename1 rename2)]
                               :when result]
                           result)))]
    (or
     ;; By default we narrow the results i.e.
     ;; We get the children first and if a resultis not found, only
     ;; then we look at parents
     (if (not parent) (try-direction candidates1 candidates2 a1 a2 :has) nil)
     (try-direction candidates2 candidates1 a2 a1 :of)
     (same-source-join references (boolean (:ast alias1)) (boolean (:ast alias2))
                       (not= (:table alias1) (:table alias2))
                       candidates1 candidates2 a1 a2))))

(defn- update-joins
  "Use the tables in the state to create a join between the last 2 tables. The
  reason to get the tables from the state is that they have been assigned an
  alias. We only use the join column from the current value being processed."
  [state current]
  (let [{:keys [join-column join-left-column join-right-column parent join]} current
        from-alias                   (state :context)]
    (cond
      (nil? from-alias) state
      ;; Explicit columns case: left table's column = right table's column
      ;; In "a | b .a_id = .id", left-col is "id" (from a), right-col is "a_id" (from b)
      (and join-left-column join-right-column)
      (let [x (-> state :aliases (get from-alias))
            join-result [(x :alias) join-left-column :has (current :alias) join-right-column "manual"]]
        (update state :joins conj [(x :alias) (current :alias) join-result join]))

      :else (let [x (-> state :aliases (get from-alias))
                  join-result (join-tables state x current join-column parent)]
              (update state :joins conj [(x :alias) (current :alias) join-result join])))))
(defn make-alias [s]
  (let [words (if (not-empty s) (s/split s #"_") ["x"])
        initials (map #(subs % 0 1) words)]
    (apply str initials)))

(defn- handle-as-table [state value]
  (let [index (state :index)
        {:keys [table alias schema parent join-column join-left-column join-right-column join]} value
        a (or alias (str (make-alias table) "_" (state :table-count)))
        current {:schema schema :table table :alias a :parent parent
                 :join-column join-column :join-left-column join-left-column
                 :join-right-column join-right-column :join join
                 :index index}]
    (-> state
        (assoc  :context (state :current))
        (assoc  :current a)
        (assoc  :current-index index)
        (update :tables conj current)
        (update :aliases assoc a current)
        (update-joins current)
        (update :table-count inc))))

(defn- handle-as-variable [state value var-ast]
  (let [index (state :index)
        {:keys [table alias join-column join-left-column join-right-column join]} value
        a (or alias table)
        current {:schema nil :table table :ast var-ast :alias a
                 :join-column join-column :join-left-column join-left-column
                 :join-right-column join-right-column :join join
                 :index index}]
    (-> state
        (assoc  :context (state :current))
        (assoc  :current a)
        (assoc  :current-index index)
        (update :tables conj current)
        (update :aliases assoc a current)
        (update-joins current)
        (update :table-count inc))))

;; todo: spec for the :value for a :table
(defn handle [state value]
  (let [{:keys [table]} value
        var-ast (or (get-in state [:variables table])
                    (get-in state [:pending-assignments table]))]
    (if var-ast
      (handle-as-variable state value var-ast)
      (handle-as-table state value))))

