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

(defn resolution-of
  "The confidence tag a committed join carries, from the relation it was
  built out of. Only :foreign-key/:heuristic ever reach here - those are
  the two kinds db/references.clj puts in the index. \"synthetic\" and
  \"manual\" are set directly wherever such a join is fabricated (see
  same-source-join and update-joins below), since neither corresponds to a
  relation in the index at all."
  [{:keys [resolution]}]
  (case resolution
    :foreign-key "fk"
    :heuristic   "heuristic"))

(defn- column-db-type
  "Look up a real column's DB type straight from the indexed references -
  same lookup as pine.data-types/get-column-type, duplicated locally to
  avoid a cycle (that namespace already requires this one)."
  [references schema table column]
  (let [columns (if schema
                  (get-in references [:schema schema :table table :columns])
                  (get-in references [:table table :columns]))]
    (some #(when (= (:column %) column) (:type %)) columns)))

(def ^:private type-families
  "Postgres types that already compare across each other with a plain `=` -
  same numeric/string/temporal family, just spelled differently
  (`integer` vs `bigint`, `character varying` vs `text`). Grouped so
  mismatched-heuristic-types? only flags a *genuine* cross-family mismatch
  (e.g. `character varying` vs `uuid`), not two column types that were
  always going to join fine as-is."
  {"smallint" :number "integer" :number "bigint" :number
   "numeric" :number "decimal" :number "real" :number "double precision" :number
   "character varying" :string "character" :string "text" :string
   "date" :temporal "timestamp" :temporal
   "timestamp without time zone" :temporal "timestamp with time zone" :temporal
   "time" :temporal "time without time zone" :temporal "time with time zone" :temporal})

(defn- type-family
  "The comparability bucket for a DB type - falls back to the type itself
  for anything not in type-families (uuid, boolean, jsonb, ...), so two
  columns of the same unlisted type still count as compatible, but never
  compatible with a *different* unlisted type."
  [db-type]
  (get type-families db-type db-type))

(defn- mismatched-heuristic-types?
  "A heuristic join is only a naming-convention guess, not a real FK, so
  nothing guarantees the two sides even share a type - e.g. one table's
  \"_id\" column stored as varchar while the other's \"id\" is a native
  uuid. A real FK can't have this problem: Postgres requires a usable
  equality operator between the two column types to create the constraint
  in the first place. Compares type *families* rather than exact type
  strings, so e.g. integer vs bigint (already comparable) isn't flagged -
  only a genuine cross-family mismatch is. Returns false (no cast needed)
  when either side's type is unknown, since that's not evidence of a
  mismatch."
  [resolution s1 t1 raw-col s2 t2 raw-f-col references]
  (boolean
   (and (= resolution "heuristic")
        (let [type1 (column-db-type references s1 t1 raw-col)
              type2 (column-db-type references s2 t2 raw-f-col)]
          (and type1 type2 (not= (type-family type1) (type-family type2)))))))

(defn- join-helper
  "Find the relation between the tables and turn it into the join map the
  rest of pine reads.

  The lookup is always [:table t1 :referred-by t2], so t1 is the parent
  and t2 the child, whichever direction the caller is travelling in - that
  is how this knows which side of the relation each of its own arguments
  is. rename1/rename2 translate t1's/t2's own column back through whatever
  name a variable exposes it under - see translate-column. A restricted
  candidate (non-empty rename) that fails to translate a *real* column (one
  the relation actually names) rejects the whole join outright - that
  relation isn't reachable through the CTE. A relation that was never found
  at all (e.g. an invalid explicit .hint_col against a real table) is left
  alone, unchanged from the pre-existing behavior of surfacing an
  unresolved join rather than no join at all.

  The returned map's :columns holds one entry per column of the relation,
  each already labelled by the side it belongs to - :from and :to name the
  two aliases in pipeline order, so nothing downstream has to work out
  which end a column came from. Every pair is translated and rendered;
  nothing here assumes there is exactly one, even though today there
  always is.

  :cast is \"text\" only for a heuristic join whose two columns turn out to
  have different DB types - so eval.clj can cast both sides instead of
  handing Postgres an operator it doesn't have (e.g. `character varying =
  uuid`). It is a property of the join, not of a pair: a cast only ever
  applies to a heuristic guess, and a heuristic relation always has
  exactly one column pair."
  [references t1 t2 s1 s2 a1 a2 c direction rename1 rename2]
  (when-let [refs (get-in references [:table t1 :referred-by t2 :via])] ;; get relations for the tables
    (let [get-col-fn            (if c (fn [_] c) (fn [xs] (if xs (first xs) nil)))
          col-key               (-> refs keys get-col-fn)
          rel                   (-> (get refs col-key) reverse first)
                                ;; A column usually carries one relation. It carries
                                ;; more when the same table name exists in several
                                ;; schemas: this lookup is by bare table name, so every
                                ;; schema's copy of the relation lands under one key.
                                ;; Entries are conj'd onto a list, so `reverse first`
                                ;; takes the one indexed earliest. A foreign key and a
                                ;; heuristic never share a key - db/references.clj only
                                ;; adds a heuristic where no foreign key already
                                ;; connects that same table pair and column.

          ;; t1 is the parent side of the relation, t2 the child side.
          pairs                 (mapv (fn [{:keys [child parent]}]
                                        {:raw1 parent :raw2 child
                                         :col1 (translate-column rename1 parent)
                                         :col2 (translate-column rename2 child)})
                                      (:columns rel))
          rejected?             (boolean
                                 (some (fn [{:keys [raw1 raw2 col1 col2]}]
                                         (or (and (some? raw1) (seq rename1) (nil? col1))
                                             (and (some? raw2) (seq rename2) (nil? col2))))
                                       pairs))
                                ;; `rel` is nil for an invalid explicit .hint_col (no via
                                ;; entry matched col-key) - resolution-of throws on a nil/
                                ;; unmatched tag, so guard it the same way the pairs above
                                ;; already tolerate being empty.
          resolution            (when rel (resolution-of rel))
          {:keys [raw1 raw2]}   (first pairs)
          cast                  (when (and resolution
                                           (mismatched-heuristic-types? resolution s1 t1 raw1 s2 t2 raw2 references))
                                  "text")]
      (when-not rejected?
        (if (= direction :of)
          {:from a2 :to a1 :parent "to"
           :columns (mapv (fn [{:keys [col1 col2]}] {:from col2 :to col1}) pairs)
           :resolution resolution :cast cast}
          {:from a1 :to a2 :parent "from"
           :columns (mapv (fn [{:keys [col1 col2]}] {:from col1 :to col2}) pairs)
           :resolution resolution :cast cast})))))

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
       {:from a1 :to a2 :parent "from"
        :columns [{:from id1 :to id2}]
        :resolution "synthetic" :cast nil}))))

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
                         (for [{t1 :table s1 :schema rename1 :rename} cs1
                               {t2 :table s2 :schema rename2 :rename} cs2
                               :let [result (join-helper references t1 t2 s1 s2 aa1 aa2 c direction rename1 rename2)]
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
            join-result {:from (x :alias) :to (current :alias) :parent "from"
                         :columns [{:from join-left-column :to join-right-column}]
                         :resolution "manual" :cast nil}]
        (update state :joins conj (assoc join-result :type join)))

      :else (let [x (-> state :aliases (get from-alias))
                  ;; Nothing connects these two tables. Still a join - the
                  ;; user wrote one - but an unresolved one, and it says so
                  ;; with a nil :resolution rather than by being absent. One
                  ;; spelling for "unresolved", so a client checks one thing.
                  join-result (or (join-tables state x current join-column parent)
                                  {:from (x :alias) :to (current :alias) :parent "from"
                                   :columns [] :resolution nil :cast nil})]
              (update state :joins conj (assoc join-result :type join))))))
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

