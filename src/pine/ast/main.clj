(ns pine.ast.main
  "Folds a parsed operation list into a state map that fully describes the query.

  Why a state map: rather than building SQL strings incrementally, each operation
  updates a data structure (tables, columns, joins, where clauses, etc.). This
  separates intent (what the query means) from rendering (how to express it in SQL),
  which makes hints, prettification, and multiple output formats possible without
  re-parsing.

  How named results join: a named result's own :columns (see ast/select.clj) each carry a
  :source - the real table they trace back to, resolved one hop at a time as each
  named result is built, so it's always a real table by the time anything reads it, no
  matter how many named results are chained. ast/table.clj's resolve-table reads that
  directly at join time, live - there's no separate pre-seeding step; a named result's
  references entry is never faked into looking like a real table's."
  (:require
   [clojure.string :as str]
   [pine.ast.assign :as assign]
   [pine.ast.count :as pine-count]
   [pine.ast.delete-action :as delete-action]
   [pine.ast.elements :as elements]
   [pine.ast.from :as from]
   [pine.ast.group :as group]
   [pine.ast.hints :as hints]
   [pine.ast.limit :as limit]
   [pine.ast.order :as order]
   [pine.ast.path :as path]
   [pine.ast.select :as select]
   [pine.ast.table :as table]
   [pine.ast.update-action :as update-action]
   [pine.ast.where :as where]
   [pine.db.main :as db]
   [pine.parser :as parser]))

(def state {;; ---
            ;; PRE
            ;; ---
            ;; - connection
            :connection-id nil
            :references {}
            :access-policy []      ;; vector of rule maps (pine.access-policy); empty/absent = no redaction
            :named-results {}           ;; {"name" <nested-AST>}, populated from |= assignments in prior expressions
            :assign    nil              ;; named result name from the last |= op in this expression
            :pending-assignments {}     ;; {"varname" <state-snapshot>} accumulated by |= ops in this expression
            :expression      nil          ;; Expression string for cursor-aware hints
            :cursor          nil          ;; Cursor position {:line N :character M} (zero-indexed)

            ;; ---
            ;; AST
            ;; ---
            ;; - tables
            ;; Needed for backend operations e.g. SQL generation, Hints, etc
            :tables          []           ;; e.g. [{ :table "user" :schema "public" :alias "u" }] ;; schema is nilable
            ;; - selected-tables
            ;; Needed for frontend operations e.g. visualize the graph with the already selected tables
            :selected-tables []           ;; e.g. [{ :table "user" :schema "public" :alias "u" }] ;; schema is nilable
            :columns         []           ;; e.g. [{ :alias "u" :column "name"  }]
            :limit           nil          ;; number ;; nilable
            :limit-per       nil          ;; limit: N per ... e.g. {:n 1 :columns [{:alias "o_0" :column "customer_id" ...}]
                                          ;;   :order [...] :star-columns {"o_0" ["id" ...]}} ;; nilable
            :aliases         {}           ;; e.g. [{ :schema "public" :table "user" }] ;; schema is nilable
            :joins           []           ;; e.g. [{:from "u_0" :to "c_1" :parent "to"
                                          ;;         :columns [{:from "company_id" :to "id"}]
                                          ;;         :resolution "fk" :cast nil :type nil}]
            :where           []           ;; e.g. {:alias "c" :column "name" :cast nil :operator "=" :value ...}
            :order           []           ;; e.g. [{ :alias "u" :column "name" :direction "DESC" }]
            :group           []           ;; e.g. [{ :alias "u" :column "name" }]
            :aggregates      []           ;; group: ... with count, e.g. ["count"]
            :update          nil          ;; e.g. { :assignments [{ :column {...} :value {...} }] }
            ;; state
            :index           0
            :operation       {:type  nil
                              :value nil} ;; [ ] 1. For post-handle. e.g. set hints if operation is table.
            ;; [ ] 2. For backwards compat with version < 0.5.
            ;;        If op is :table, then the context  in the api handler has one less table

            :current        nil           ;; alias of the current table
            :context        nil           ;; alias of the table in context

            :table-count    0
            :pending-count  0

            ;; ------
            ;; Parsed
            ;; ------
            :prettified     nil
            :ranges         nil

            ;; ---
            ;; POST
            ;; ---
            ;; - hints
            :hints          {:table [] :select [] :order [] :where [] :update [] :paths [] :unnest []}

            ;; ---
            ;; Checkpoint
            ;; ---
            ;; Names an auto-named checkpoint must not take: every table alias
            ;; and |= name written anywhere in this expression, including
            ;; ones after the checkpoint. Filled in by handle-ops - see
            ;; next-auto-cte-name.
            :reserved-cte-names nil
            ;; Set after a checkpoint op (group/limit) to signal the next table op
            ;; should become a CTE instead of a direct join.
            ;; :index is the operation index of the group:/limit: step.
            ;; nil | {:needs-assign true :index 2} | {:name "n" :needs-table true :index 2}
            :pending-checkpoint nil
            ;; Every checkpoint in this expression, in pipe order, for the
            ;; client: see checkpoint-entry. Survives reset-for-cte.
            :checkpoints []})

(defn- expression-names
  "Every table alias and |= name written anywhere in ops, lower-cased."
  [ops]
  (into #{}
        (keep (fn [{:keys [type value]}]
                (some-> (case type
                          :table  (:alias value)
                          :elements (:as value)
                          :assign value
                          nil)
                        str/lower-case)))
        ops))

(defn- schema-table-names
  "Every table name in the connected database's schema index, from any
  schema, lower-cased. Empty when there is no index."
  [references]
  (into #{}
        (map (comp str/lower-case name))
        (concat (keys (:table references))
                (mapcat (comp keys :table val) (:schema references)))))

(defn next-auto-cte-name
  "The name for a checkpoint sealed without `|= name`: `result_N`, with the
  smallest N >= 1 that is free. The canvas shows this name on the node, and
  the UI writes it back into the text as `|= result_N` when the user builds
  on it, so it has to read like a name a person would pick.

  A name is taken, and skipped, if it is any of these:

  - A named result already known, from this or an earlier expression
    (:named-results). Each blank-line-separated expression starts from a
    fresh state, and its auto-names become top-level named results just
    like explicit |= names. eval.clj's collect-ctes dedupes CTEs by name,
    so if two expressions both produced `result_1`, it would silently drop
    one of two different CTEs. (This happened with the old `__pine_0__`
    names before they were seeded from the named-result count.)
  - A name already used in this expression (:pending-assignments). That
    covers both explicit |= names written earlier and the auto-names of
    earlier checkpoints, so a second checkpoint gets `result_2`.
  - A table alias or |= name written anywhere in this expression
    (:reserved-cte-names), including after the checkpoint. The CTE is also
    used as its own alias (`FROM result_1 AS result_1`), so a later
    `employee as result_1` would give two relations one alias, and a later
    `|= result_1` would overwrite this checkpoint.
  - The name of a real table in the connected database, in any schema. A
    CTE shadows an unqualified table of the same name, so the query would
    silently read the CTE instead of the table. When there is no schema
    index (some tests), this check finds nothing to skip.

  Names are compared lower-cased, so a table `Result_1` also blocks
  `result_1`: some databases fold or ignore identifier case. The result
  depends only on the expression, the earlier named results and the
  schema, so it is the same every time the same text is evaluated."
  [state]
  (let [taken (into (or (:reserved-cte-names state) #{})
                    (comp cat (map str/lower-case))
                    [(keys (:named-results state))
                     (keys (:pending-assignments state))
                     (keys (:aliases state))
                     (schema-table-names (:references state))])]
    (->> (iterate inc 1)
         (map #(str "result_" %))
         (remove taken)
         first)))

(defn pre-handle
  ([state connection-id ops-count expression cursor named-results]
   (pre-handle state connection-id ops-count expression cursor named-results []))
  ([state connection-id ops-count expression cursor named-results access-policy]
   (-> state
       (assoc :references (db/init-references connection-id))
       (assoc :connection-id connection-id)
       (assoc :pending-count ops-count)
       (assoc :expression expression)
       (assoc :cursor cursor)
       (assoc :named-results named-results)
       (assoc :access-policy (or access-policy [])))))

;; ---------------------------------------------------------------------------
;; Checkpoint helpers
;; ---------------------------------------------------------------------------

(declare handle-op)

(def ^:private checkpoint-op-types #{:group :limit :limit-partial})

;; Ops that consume/query the checkpoint's result rather than composing another
;; table join onto it. Fired on regardless of partial-vs-complete: an -partial op
;; (e.g. order-partial from a dangling trailing comma) already carries whatever was
;; fully typed before the comma, so it needs the same sealed scope a complete op
;; would. count/delete/update are deliberately excluded — they build their own
;; wrapper query generically (see build-count-query) and don't need the checkpoint's
;; group-shaped state separated into a CTE first.
(def ^:private checkpoint-consuming-op-types
  #{:select :select-partial :where :where-partial :order :order-partial})

(defn- reset-for-cte [state]
  (assoc state
         :tables    [] :columns [] :limit nil :limit-per nil :joins  []
         :where     [] :order   [] :group [] :aggregates [] :update nil
         :current nil :context nil :current-index 0
         :table-count 0 :operation {:type nil :value nil}))

(defn- result-columns
  "The names of the columns a sealed state returns: what it selects, or,
  with no select (`*`), every column of its current table. A current table
  that is itself a named result or checkpoint returns what that one does,
  so `group: name | l: 3` gives name and count, not the grouped table's
  columns."
  [snapshot]
  (let [explicit (remove table/added-column? (:columns snapshot))
        current  (:current snapshot)
        entry    (get (:aliases snapshot) current)]
    (cond
      (seq explicit) (mapcat (fn [{:keys [alias column column-alias]}]
                               (if (and (str/blank? column) (not column-alias))
                                 ;; `s: c.*`
                                 (map :column (hints/generate-all-column-hints snapshot alias))
                                 [(or column-alias column)]))
                             explicit)
      (:ast entry)   (result-columns (:ast entry))
      :else          (map :column (hints/generate-all-column-hints snapshot current)))))

(defn- checkpoint-entry
  "What the client is told about one checkpoint: enough to label it and to
  draw the result it seals, without the snapshot itself (the client finds
  that under :pending-assignments by :name).

  - :name     the CTE name, `|= name` or the auto name (result_1, ...)
  - :sealed   false only for a checkpoint still pending at the end of the
              expression; its name is the one it will get when sealed
  - :index    the operation index of its group:/limit: step
  - :kind     \"limit\" or \"group\"
  - :limit    N, and :per the columns after `per`, if any (limit only)
  - :group    the grouped columns, and :aggregates e.g. [\"count\"] (group only)
  - :tables   the aliases of the tables it seals, in pipe order
  - :columns  the names of the columns its result has

  The result columns need the schema index, so this runs while
  :references is still on the state (post-handle drops it)."
  [cname snapshot index sealed?]
  (let [limit?  (boolean (or (:limit snapshot) (:limit-per snapshot)))
        column  #(select-keys % [:alias :column :path :column-alias])]
    (cond-> {:name    cname
             :sealed  sealed?
             :index   index
             :kind    (if limit? "limit" "group")
             :tables  (mapv :alias (:tables snapshot))
             :columns (vec (distinct (result-columns snapshot)))}
      limit?               (assoc :limit (or (:limit snapshot) (get-in snapshot [:limit-per :n])))
      (:limit-per snapshot) (assoc :per (mapv column (get-in snapshot [:limit-per :columns])))
      (not limit?)         (assoc :group (mapv column (:group snapshot))
                                  :aggregates (vec (:aggregates snapshot))))))

(defn- seal-as-cte
  "Store snapshot under cname, reset the query-building state, then inject
  cname as the first table so subsequent ops compose on top of it. Nothing
  needs seeding into :references - table/resolve-table reads cname's own
  :source-tagged :columns live, the same as any other named result."
  [state cname snapshot index]
  (-> state
      (update :checkpoints conj (checkpoint-entry cname snapshot index true))
      (assoc-in [:pending-assignments cname] snapshot)
      reset-for-cte
      (handle-op {:type :table :value {:table cname}})))

(defn- add-pending-checkpoint
  "Describe a checkpoint still pending at the end of the expression, under
  the name sealing it would give it. The canvas shows it, and writes that
  name back into the text when the user builds on it."
  [state]
  (if-let [{:keys [name index]} (:pending-checkpoint state)]
    (let [cname    (or name (next-auto-cte-name state))
          snapshot (if name
                     (get-in state [:pending-assignments name])
                     state)]
      (update state :checkpoints conj (checkpoint-entry cname snapshot index false)))
    state))

(defn- flush-checkpoint
  "State-machine step: called at the start of each handle-ops iteration.
  Converts a pending checkpoint into a CTE when the right op type is seen.

  Fires when the incoming op is a table (join composition), another checkpoint
  op (e.g. LIMIT after GROUP), or an op that queries the checkpoint's result
  (select/where/order, complete or partial). Does not fire for count/delete/update
  since those have their own query-building paths that do not need CTE separation."
  [state op]
  (let [checkpoint (:pending-checkpoint state)
        op-type    (:type op)
        fire?      (or (= op-type :table)
                       (contains? checkpoint-op-types op-type)
                       (contains? checkpoint-consuming-op-types op-type))]
    (cond
      (nil? checkpoint)
      state

      ;; Explicit assign after a checkpoint op: record the user name, wait
      (and (:needs-assign checkpoint) (= op-type :assign))
      (assoc state :pending-checkpoint {:name (:value op) :needs-table true :index (:index checkpoint)})

      ;; Auto-named: fire when a table or another checkpoint op arrives
      (and (:needs-assign checkpoint) fire?)
      (let [cname    (next-auto-cte-name state)
            snapshot (dissoc state :pending-assignments)]
        (-> state
            (assoc :pending-checkpoint nil)
            (seal-as-cte cname snapshot (:index checkpoint))))

      ;; Waiting for assign, non-triggering op (count, where, etc.) — hold
      (:needs-assign checkpoint)
      state

      ;; User-named: fire when a table or another checkpoint op arrives
      (and (:needs-table checkpoint) fire?)
      (let [cname    (:name checkpoint)
            snapshot (get-in state [:pending-assignments cname])]
        (-> state
            (assoc :pending-checkpoint nil)
            (seal-as-cte cname snapshot (:index checkpoint))))

      :else state)))

(defn handle-op [state {:keys [type value per]}]
  (case type
    :select (select/handle state value)
    :select-partial (select/handle state value)
    ;; A table still being typed, `| `, isn't a join yet.
    :table (path/check-join-paths (table/handle state value))
    :elements (elements/handle state value)
    ;; Nothing to add while the array is typed: see hints.
    :unnest-partial state
    :limit (limit/handle state value per)
    :limit-partial (limit/handle state value per)
    :where (where/handle state value)
    :where-partial (where/handle-partial state value)
    :from (from/handle state value)
    :group (group/handle state value)
    :order (order/handle state value)
    :order-partial (order/handle state value)
    :count (pine-count/handle state value)
    :delete-action (delete-action/handle state value)
    :update-action (update-action/handle state value)
    :update-partial (update-action/handle state value)
    :assign (assign/handle state value)
    ;; :paths adds no table/join of its own - the target it names is already
    ;; fully captured in :operation by handle-ops, which is all
    ;; hints/generate-path-hints needs. :current is deliberately left
    ;; untouched, so it still points at whatever real table/named result the
    ;; pipe left off at - that's the path search's starting point.
    :paths state
    ;; No operations
    :no-op state
    (update state :errors conj [type "Unknown operation type in parse tree"])))

(defn handle-ops
  "Folds ops into state. :reserved-cte-names is filled from ops unless the
  caller already set it: the cursor-truncated parse passes the full
  expression's names, so both name a checkpoint the same way."
  [state ops]
  (reduce (fn [s [i o]]
            ;; flush-checkpoint runs before handle-op; it may reset state and inject
            ;; a synthetic CTE table — the :index must be set first so the injected
            ;; table gets the correct operation index.
            (let [s (-> s (assoc :index i) (flush-checkpoint o))]
              (cond-> (-> s
                          (handle-op o)
                          (update :pending-count dec))
                (not= (:type o) :assign) (assoc :operation o)
                (checkpoint-op-types (:type o)) (assoc :pending-checkpoint {:needs-assign true :index i}))))
          (update state :reserved-cte-names #(or % (expression-names ops)))
          (map-indexed vector ops)))

(declare generate)

(defn- truncate-at-cursor
  "Truncate expression at cursor position. Cursor is {:line N :character M} (zero-indexed)"
  [expression cursor]
  (if (nil? cursor)
    expression
    (let [{:keys [line character]} cursor
          lines (str/split-lines expression)]
      (if (>= line (count lines))
        expression
        (let [lines-before (take line lines)
              current-line (nth lines line)
              cut (min character (count current-line))
              truncated-current (subs current-line 0 cut)
              ;; A cursor sitting in the leading whitespace before a
              ;; continuation line's `|` (i.e. it hasn't reached the pipe
              ;; character yet) still means the previous operation is
              ;; complete and this line is a fresh (if empty) one - cutting
              ;; strictly at `character` here drops the `|` entirely,
              ;; collapsing what should be two operations into one and
              ;; making hint generation treat the already-complete previous
              ;; table as a still-being-typed, context-less prefix. Extend
              ;; the cut through the `|` in that case so the truncated parse
              ;; still sees a fresh, empty second operation.
              truncated-current (if (str/blank? truncated-current)
                                  (if-let [pipe-idx (str/index-of current-line "|")]
                                    (subs current-line 0 (inc pipe-idx))
                                    truncated-current)
                                  truncated-current)]
          (str/join "\n" (concat lines-before [truncated-current])))))))

(defn- generate-truncated-state
  "Generate state for truncated expression at cursor position.
   Keep references for hint generation."
  [expression cursor connection-id named-results reserved-cte-names]
  (let [truncated-expr (truncate-at-cursor expression cursor)
        {:keys [result error]} (parser/parse truncated-expr)]
    (if (or error (nil? result))
      ;; Parse error or no result, return nil
      nil
      ;; Successfully parsed, build state without going through post-handle
      ;; to preserve references for hint generation
      (-> state
          (pre-handle connection-id (count result) nil nil named-results)
          (assoc :reserved-cte-names reserved-cte-names)
          (handle-ops result)))))

(defn- offset->position
  "Convert a 0-based character offset to {:line N :character M} (both 0-based)
   by counting newlines in the expression up to that offset."
  [expression offset]
  (let [prefix (subs expression 0 (min offset (count expression)))
        lines (str/split prefix #"\n" -1)
        line (dec (count lines))
        character (count (last lines))]
    {:line line :character character}))

(defn- compute-ranges
  "Compute alias ranges for each operation in the original expression.
   selected-tables entries have :index indicating which operation added them.
   operations is a vector of {:expression ... :start ... :end ...} from prettify."
  [expression selected-tables operations]
  (let [table-entries (sort-by :index selected-tables)]
    (mapv
     (fn [i {:keys [start end]}]
       (let [alias (->> table-entries
                        (filter #(<= (:index %) i))
                        last
                        :alias)]
         {:alias alias
          :start (offset->position expression start)
          :end (offset->position expression end)}))
     (range (count operations))
     operations)))

(defn- add-prettify
  "Add :prettified and :ranges to the state using the original expression."
  [state]
  (let [expression (:expression state)]
    (if expression
      (let [{:keys [result operations]} (parser/prettify expression)
            selected-tables (:selected-tables state)
            ranges (when operations (compute-ranges expression selected-tables operations))]
        (-> state
            (assoc :prettified result)
            (assoc :ranges ranges)))
      state)))

(defn- unnest-entries
  "Each `unnest:` for the client, in pipe order: `table`, the alias of the
  table whose column holds the array, and whose node shows it, and `text`,
  the array's path from there: `data.changes`, or `o.items` for the
  elements of the elements `o`."
  [state]
  (let [aliases (:aliases state)
        table-of (fn table-of [a] (if-let [source (get-in aliases [a :elements :alias])] (table-of source) a))]
    (->> (:tables state)
         (filter :elements)
         (mapv (fn [{e :alias {:keys [alias column path]} :elements :keys [index]}]
                 {:alias e
                  :table (table-of alias)
                  :index index
                  :text (if (path/element-alias? state alias)
                          (path/path-text alias path)
                          (path/path-text column path))})))))

(defn post-handle [state truncated-state]
  (-> state
      (hints/handle truncated-state)
      ;; Each table's primary key, kept for update!, which is built after
      ;; :references is dropped below
      select/add-row-keys
      ;; Add auto-ID columns based on final operation type
      select/add-auto-id-columns
      ;; After the key columns: only a table that has them is editable
      select/add-json-type-columns
      (assoc :unnests (unnest-entries state))
      ;; A node is a table. The elements of unnest: are shown on the node
      ;; of the table they came from, as :unnests.
      (assoc :selected-tables (let [tables (remove :elements (state :tables))
                                    type (-> state :operation :type)]
                                (if
                                 (= type :table)
                                  (-> tables reverse rest reverse)
                                  tables)))
      add-prettify
      ;; :references is normally internal-only scaffolding, dropped before the
      ;; state is used further - but when :access-policy is non-empty, eval.clj
      ;; needs it at SQL-render time to resolve each column's real DB type/table
      ;; (see pine.access-policy). Nested named result/CTE snapshots (taken earlier,
      ;; via assign/handle) already carry :references regardless, since this
      ;; dissoc only ever runs once, on the final top-level state.
      (#(if (seq (:access-policy %)) % (dissoc % :references)))))

(defn generate
  ([parse-tree]
   (generate parse-tree @db/connection-id nil nil {}))
  ([parse-tree connection-id]
   (generate parse-tree connection-id nil nil {}))
  ([parse-tree connection-id expression cursor]
   (generate parse-tree connection-id expression cursor {}))
  ([parse-tree connection-id expression cursor named-results]
   (generate parse-tree connection-id expression cursor named-results []))
  ([parse-tree connection-id expression cursor named-results access-policy]
   (let [full-state (-> state
                        (pre-handle connection-id (count parse-tree) expression cursor named-results access-policy)
                        (handle-ops parse-tree)
                        add-pending-checkpoint)
         truncated-state (when (and cursor expression)
                           (generate-truncated-state expression cursor connection-id named-results
                                                     (:reserved-cte-names full-state)))]
     (-> (post-handle full-state truncated-state)
         ;; Every operation's type, in order - not just the terminal one that
         ;; :operation holds and that build-query dispatches on. Callers that
         ;; need to know whether an expression writes (pine.ast.effects) have
         ;; to see all of them. Internal: prune-ast (api.clj) does not pass
         ;; this through to the client's :ast.
         (assoc :operation-types (mapv :type parse-tree))))))

