(ns pine.ast.main
  "Folds a parsed operation list into a state map that fully describes the query.

  Why a state map: rather than building SQL strings incrementally, each operation
  updates a data structure (tables, columns, joins, where clauses, etc.). This
  separates intent (what the query means) from rendering (how to express it in SQL),
  which makes hints, prettification, and multiple output formats possible without
  re-parsing.

  Why variables are seeded before handle-ops: joins are resolved using a references
  map built from the DB schema. Variables (CTEs from earlier expressions) need to
  appear in that map — with the same FK relationships as their source tables — so
  join resolution treats them like real tables."
  (:require
   [clojure.string :as str]
   [pine.ast.assign :as assign]
   [pine.ast.count :as pine-count]
   [pine.ast.delete-action :as delete-action]
   [pine.ast.from :as from]
   [pine.ast.group :as group]
   [pine.ast.hints :as hints]
   [pine.ast.limit :as limit]
   [pine.ast.order :as order]
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
            :variables {}               ;; {"varname" <nested-AST>}, populated from |= assignments in prior expressions
            :assign    nil              ;; variable name from the last |= op in this expression
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
            :aliases         {}           ;; e.g. [{ :schema "public" :table "user" }] ;; schema is nilable
            :joins           []           ;; Vector of joins e.g. [ "u" "c" ".. relation .."]
            :where           []           ;; e.g. [ "name" "=" "john" ]
            :order           []           ;; e.g. [{ :alias "u" :column "name" :direction "DESC" }]
            :group           []           ;; e.g. [{ :alias "u" :column "name" }]
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
            :hints          {:table [] :select [] :order [] :where [] :update []}

            ;; ---
            ;; Checkpoint
            ;; ---
            ;; Tracks auto-generated CTE names (__pine_0__, __pine_1__, ...)
            :auto-cte-count   0
            ;; Set after a checkpoint op (group/limit) to signal the next table op
            ;; should become a CTE instead of a direct join.
            ;; nil | {:needs-assign true} | {:name "n" :needs-table true}
            :pending-checkpoint nil})

(defn- variable-output-columns
  "Return the column list a variable's CTE actually exposes, for hint generation.
  Returns nil when the CTE selects *, meaning the source table's columns apply."
  [var-ast]
  (let [user-cols (remove :auto-id (:columns var-ast))
        op-type   (-> var-ast :operation :type)]
    (when (seq user-cols)
      (let [names     (distinct (map #(or (:column-alias %) (:column %)) user-cols))
            with-count (if (= op-type :group) (concat names ["count"]) names)]
        (mapv #(hash-map :column %) with-count)))))

(defn- get-source-tables
  "Return the tables whose columns are exposed by a variable's CTE.
  Used by both seeding and bidirectional patching."
  [var-ast]
  (let [columns  (:columns var-ast)
        aliases  (:aliases var-ast)
        explicit (remove :auto-id columns)]
    (or (if (empty? explicit)
          (when-let [current-alias (:current var-ast)]
            [(get-in aliases [current-alias :table])])
          (->> explicit
               (map #(get-in aliases [(:alias %) :table]))
               (remove nil?)
               distinct))
        [])))

(defn- seed-variable-references
  "Copy reference entries from the real source tables of a variable into the
  local references map under the variable name, so join resolution treats the
  variable identically to a real table. Column hints are overridden with the
  CTE's actual output columns when they can be determined."
  [refs var-ast varname]
  (let [source-tables (get-source-tables var-ast)
        seeded (reduce (fn [r source-table]
                         (let [source-refs (get-in r [:table source-table])]
                           (if source-refs
                             (update-in r [:table varname] merge source-refs)
                             r)))
                       refs
                       source-tables)]
    (if-let [output-cols (variable-output-columns var-ast)]
      (-> seeded
          (assoc-in [:table varname :columns] output-cols)
          (assoc-in [:table varname :column-set] (set output-cols)))
      seeded)))

(defn- build-direction-index
  "Reverse index: source-table -> {entity-name -> existing-relation}, for one
  direction. Lets patch-direction look up 'who already relates to S' directly
  instead of scanning every table in the schema per variable."
  [refs direction]
  (reduce-kv (fn [idx entity-name entity-refs]
               (reduce-kv (fn [idx target existing]
                            (assoc-in idx [target entity-name] existing))
                          idx
                          (get entity-refs direction)))
             {}
             (get refs :table {})))

(defn- patch-direction
  "For each variable V wrapping source S, copy T[direction][S] → T[direction][V]
  for every entity T that already relates to S. Used by patch-variable-relations.

  Uses a reverse index (source-table -> referring entities) built once instead
  of scanning every schema table per variable, which turns this from O(v · T)
  into O(T + v · k) where k is the actual fan-in for each source table. The
  index is kept in sync with entries this pass adds, since a later variable's
  source table can itself be an earlier variable processed in this same pass
  (variable-of-variable composition)."
  [refs variables direction]
  (:refs
   (reduce
    (fn [{:keys [refs index]} [varname var-ast]]
      (reduce
       (fn [{:keys [refs index]} source-table]
         (reduce
          (fn [{:keys [refs index]} [entity-name existing]]
            (let [refs*  (update-in refs [:table entity-name direction varname] merge existing)
                  merged (get-in refs* [:table entity-name direction varname])]
              {:refs refs* :index (assoc-in index [varname entity-name] merged)}))
          {:refs refs :index index}
          (get index source-table)))
       {:refs refs :index index}
       (get-source-tables var-ast)))
    {:refs refs :index (build-direction-index refs direction)}
    variables)))

(defn- patch-variable-relations
  "Bidirectional pass: for each variable V wrapping source S, find every
  entity T where T already knows about S via :referred-by or :refers-to,
  and register V there too.

  This enables:
  - 'T | V'  and  'V | T'  (real table ↔ variable)
  - 'V | W'  and  'W | V'  (variable ↔ variable)

  Must run after all variables have been seeded."
  [refs variables]
  (-> refs
      (patch-direction variables :referred-by)
      (patch-direction variables :refers-to)))

(defn- patch-same-source-variable-joins
  "For each ordered pair of distinct variables (v1, v2) that share a source table
  with an 'id' column, register a synthetic id=id join at refs[:table v1 :referred-by v2].
  Only adds entries where no join path already exists, so self-referential FK propagation
  (e.g. employee/reports_to) is preserved.

  Pairs are generated only within groups of variables that actually share a source
  table (via an inverted source-table -> variables index), instead of scanning every
  ordered pair among all variables. This turns the common case (variables spread
  across distinct source tables) from O(v²) into roughly O(v), while the worst case
  (all variables sharing one source) stays O(g²) for that group — unavoidable since
  the output itself is a g² set of pairwise joins."
  [refs variables]
  (let [var-sources (->> variables
                         (map (fn [[vname var-ast]]
                                [vname (set (get-source-tables var-ast))]))
                         (into {}))
        by-source (reduce-kv (fn [idx vname sources]
                                (reduce (fn [idx source] (update idx source (fnil conj []) vname))
                                        idx
                                        sources))
                              {}
                              var-sources)
        pairs (distinct (for [[_ vnames] by-source
                              v1 vnames v2 vnames
                              :when (not= v1 v2)]
                          [v1 v2]))]
    (reduce (fn [r [v1 v2]]
              (let [shared-sources (filter (var-sources v1) (var-sources v2))
                    has-id? (fn [tbl] (some #(= "id" (:column %)) (get-in r [:table tbl :columns])))]
                (if (and (some has-id? shared-sources)
                         (not (get-in r [:table v1 :referred-by v2])))
                  (update-in r [:table v1 :referred-by v2 :via "id"]
                             (fnil conj [])
                             [nil v1 "id" :referred-by nil v2 "id" :variable-join])
                  r)))
            refs
            pairs)))

(defn pre-handle [state connection-id ops-count expression cursor variables]
  (let [refs       (db/init-references connection-id)
        seeded-refs (reduce (fn [r [varname var-ast]]
                              (seed-variable-references r var-ast varname))
                            refs
                            variables)
        aug-refs   (-> seeded-refs
                       (patch-variable-relations variables)
                       (patch-same-source-variable-joins variables))]
    (-> state
        (assoc :references aug-refs)
        (assoc :connection-id connection-id)
        (assoc :pending-count ops-count)
        (assoc :expression expression)
        (assoc :cursor cursor)
        (assoc :variables variables))))

;; ---------------------------------------------------------------------------
;; Checkpoint helpers
;; ---------------------------------------------------------------------------

(declare handle-op)

(def ^:private checkpoint-op-types #{:group :limit})

(defn- reset-for-cte [state]
  (assoc state
         :tables    [] :columns [] :limit nil :joins  []
         :where     [] :order   [] :group [] :update nil
         :current nil :context nil :current-index 0
         :table-count 0 :operation {:type nil :value nil}))

(defn- seal-as-cte
  "Store snapshot under cname, seed its join references, reset the query-building
  state, then inject cname as the first table so subsequent ops compose on top of it."
  [state cname snapshot]
  (let [new-refs (-> (:references state)
                     (seed-variable-references snapshot cname)
                     (patch-variable-relations {cname snapshot})
                     (patch-same-source-variable-joins {cname snapshot}))]
    (-> state
        (assoc :references new-refs)
        (assoc-in [:pending-assignments cname] snapshot)
        reset-for-cte
        (handle-op {:type :table :value {:table cname}}))))

(defn- flush-checkpoint
  "State-machine step: called at the start of each handle-ops iteration.
  Converts a pending checkpoint into a CTE when the right op type is seen.

  Fires when the incoming op is a table (join composition) or another checkpoint
  op (e.g. LIMIT after GROUP). Does not fire for count/delete/update since those
  have their own query-building paths that do not need CTE separation."
  [state op]
  (let [checkpoint (:pending-checkpoint state)
        op-type    (:type op)
        fire?      (or (= op-type :table)
                       (contains? checkpoint-op-types op-type))]
    (cond
      (nil? checkpoint)
      state

      ;; Explicit assign after a checkpoint op: record the user name, wait
      (and (:needs-assign checkpoint) (= op-type :assign))
      (assoc state :pending-checkpoint {:name (:value op) :needs-table true})

      ;; Auto-named: fire when a table or another checkpoint op arrives
      (and (:needs-assign checkpoint) fire?)
      (let [n        (:auto-cte-count state)
            cname    (str "__pine_" n "__")
            snapshot (dissoc state :pending-assignments)]
        (-> state
            (update :auto-cte-count inc)
            (assoc :pending-checkpoint nil)
            (seal-as-cte cname snapshot)))

      ;; Waiting for assign, non-triggering op (count, where, etc.) — hold
      (:needs-assign checkpoint)
      state

      ;; User-named: fire when a table or another checkpoint op arrives
      (and (:needs-table checkpoint) fire?)
      (let [cname    (:name checkpoint)
            snapshot (get-in state [:pending-assignments cname])]
        (-> state
            (assoc :pending-checkpoint nil)
            (seal-as-cte cname snapshot)))

      :else state)))

(defn handle-op [state {:keys [type value]}]
  (case type
    :select (select/handle state value)
    :select-partial (select/handle state value)
    :table (table/handle state value)
    :limit (limit/handle state value)
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
    ;; No operations
    :no-op state
    (update state :errors conj [type "Unknown operation type in parse tree"])))

(defn handle-ops [state ops]
  (reduce (fn [s [i o]]
            ;; flush-checkpoint runs before handle-op; it may reset state and inject
            ;; a synthetic CTE table — the :index must be set first so the injected
            ;; table gets the correct operation index.
            (let [s (-> s (assoc :index i) (flush-checkpoint o))]
              (cond-> (-> s
                          (handle-op o)
                          (update :pending-count dec))
                (not= (:type o) :assign) (assoc :operation o)
                (checkpoint-op-types (:type o)) (assoc :pending-checkpoint {:needs-assign true}))))
          state
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
              truncated-current (subs current-line 0 (min character (count current-line)))]
          (str/join "\n" (concat lines-before [truncated-current])))))))

(defn- generate-truncated-state
  "Generate state for truncated expression at cursor position.
   Keep references for hint generation."
  [expression cursor connection-id variables]
  (let [truncated-expr (truncate-at-cursor expression cursor)
        {:keys [result error]} (parser/parse truncated-expr)]
    (if (or error (nil? result))
      ;; Parse error or no result, return nil
      nil
      ;; Successfully parsed, build state without going through post-handle
      ;; to preserve references for hint generation
      (-> state
          (pre-handle connection-id (count result) nil nil variables)
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

(defn post-handle [state truncated-state]
  (-> state
      (hints/handle truncated-state)
      ;; Add auto-ID columns based on final operation type
      select/add-auto-id-columns
      (assoc :selected-tables (let [tables (state :tables)
                                    type (-> state :operation :type)]
                                (if
                                 (= type :table)
                                  (-> tables reverse rest reverse)
                                  tables)))
      add-prettify
      (dissoc :references)))

(defn generate
  ([parse-tree]
   (generate parse-tree @db/connection-id nil nil {}))
  ([parse-tree connection-id]
   (generate parse-tree connection-id nil nil {}))
  ([parse-tree connection-id expression cursor]
   (generate parse-tree connection-id expression cursor {}))
  ([parse-tree connection-id expression cursor variables]
   (let [full-state (-> state
                        (pre-handle connection-id (count parse-tree) expression cursor variables)
                        (handle-ops parse-tree))
         truncated-state (when (and cursor expression)
                           (generate-truncated-state expression cursor connection-id variables))]
     (post-handle full-state truncated-state))))

