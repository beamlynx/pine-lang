(ns pine.ast.main
  "Folds a parsed operation list into a state map that fully describes the query.

  Why a state map: rather than building SQL strings incrementally, each operation
  updates a data structure (tables, columns, joins, where clauses, etc.). This
  separates intent (what the query means) from rendering (how to express it in SQL),
  which makes hints, prettification, and multiple output formats possible without
  re-parsing.

  How variables join: a variable's own :columns (see ast/select.clj) each carry a
  :source - the real table they trace back to, resolved one hop at a time as each
  variable is built, so it's always a real table by the time anything reads it, no
  matter how many variables are chained. ast/table.clj's resolve-table reads that
  directly at join time, live - there's no separate pre-seeding step; a variable's
  references entry is never faked into looking like a real table's."
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
            ;; Tracks auto-generated CTE names (__pine_0__, __pine_1__, ...).
            ;; This default is immediately overridden by pre-handle, which
            ;; derives the real starting count from already-known variables -
            ;; see next-auto-cte-count.
            :auto-cte-count   0
            ;; Set after a checkpoint op (group/limit) to signal the next table op
            ;; should become a CTE instead of a direct join.
            ;; nil | {:needs-assign true} | {:name "n" :needs-table true}
            :pending-checkpoint nil})

(defn- next-auto-cte-count
  "Anonymous checkpoint CTEs (__pine_0__, __pine_1__, ...) are named from
  :auto-cte-count, which used to always start at 0 for every expression - but
  each blank-line-separated expression gets its own fresh state, so two
  expressions that each auto-name exactly one checkpoint both produced
  \"__pine_0__\". Since these are genuinely different CTEs that only share a
  name, eval.clj's collect-ctes (which dedupes by name, correctly, for
  explicit |= names that really are unique) silently dropped one of them.

  The names only need to be unique, not contiguous - so instead of parsing
  __pine_N__ back out of variables to find the highest N used, just start
  counting from how many variables already exist. variables only ever grows
  across expressions (see api.clj's evaluate-expressions), and each
  expression's own auto-names always add at least that many new keys to it -
  every one becomes its own top-level entry, exactly like an explicit |= name
  would - so the count after an expression is always past every number it
  used. By induction, seeding from the current count can never collide with a
  number a prior expression already claimed, even though later expressions
  will end up skipping some numbers (e.g. ones used by explicit |= names)."
  [variables]
  (count variables))

(defn pre-handle [state connection-id ops-count expression cursor variables]
  (-> state
      (assoc :references (db/init-references connection-id))
      (assoc :connection-id connection-id)
      (assoc :pending-count ops-count)
      (assoc :expression expression)
      (assoc :cursor cursor)
      (assoc :variables variables)
      (assoc :auto-cte-count (next-auto-cte-count variables))))

;; ---------------------------------------------------------------------------
;; Checkpoint helpers
;; ---------------------------------------------------------------------------

(declare handle-op)

(def ^:private checkpoint-op-types #{:group :limit})

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
         :tables    [] :columns [] :limit nil :joins  []
         :where     [] :order   [] :group [] :update nil
         :current nil :context nil :current-index 0
         :table-count 0 :operation {:type nil :value nil}))

(defn- seal-as-cte
  "Store snapshot under cname, reset the query-building state, then inject
  cname as the first table so subsequent ops compose on top of it. Nothing
  needs seeding into :references - table/resolve-table reads cname's own
  :source-tagged :columns live, the same as any other variable."
  [state cname snapshot]
  (-> state
      (assoc-in [:pending-assignments cname] snapshot)
      reset-for-cte
      (handle-op {:type :table :value {:table cname}})))

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

