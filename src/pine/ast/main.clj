(ns pine.ast.main
  (:require
   [clojure.string :as str]
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
            :variables {}               ;; {"varname" <nested-AST>}, populated from |= assignments
            :assign    nil              ;; variable name from |=, set after handle-ops
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
            :hints          {:table [] :select [] :order [] :where [] :update []}})

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

(defn- patch-direction
  "For each variable V wrapping source S, copy T[direction][S] → T[direction][V]
  for every entity T in the references map. Used by patch-variable-relations."
  [refs variables direction]
  (reduce (fn [r [varname var-ast]]
            (let [source-tables (get-source-tables var-ast)]
              (reduce (fn [r source-table]
                        (reduce (fn [r entity-name]
                                  (let [existing (get-in r [:table entity-name direction source-table])]
                                    (if existing
                                      (update-in r [:table entity-name direction varname] merge existing)
                                      r)))
                                r
                                (keys (get r :table {}))))
                      r
                      source-tables)))
          refs
          variables))

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

(defn pre-handle [state connection-id ops-count expression cursor variables]
  (let [refs       (db/init-references connection-id)
        seeded-refs (reduce (fn [r [varname var-ast]]
                              (seed-variable-references r var-ast varname))
                            refs
                            variables)
        aug-refs   (patch-variable-relations seeded-refs variables)]
    (-> state
        (assoc :references aug-refs)
        (assoc :connection-id connection-id)
        (assoc :pending-count ops-count)
        (assoc :expression expression)
        (assoc :cursor cursor)
        (assoc :variables variables))))

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
    ;; No operations
    :no-op state
    (update state :errors conj [type "Unknown operation type in parse tree"])))

(defn handle-ops [state ops]
  (reduce (fn [s [i o]]
            (-> s
                (assoc :index i)
                (handle-op o)  ; Pass the index and operation
                (update :pending-count dec)
                (assoc :operation o)))
          state
          (map-indexed vector ops)))  ; Pair each operation with its index

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
   (generate parse-tree @db/connection-id nil nil {} nil))
  ([parse-tree connection-id]
   (generate parse-tree connection-id nil nil {} nil))
  ([parse-tree connection-id expression cursor]
   (generate parse-tree connection-id expression cursor {} nil))
  ([parse-tree connection-id expression cursor variables assign]
   (let [full-state (-> state
                        (pre-handle connection-id (count parse-tree) expression cursor variables)
                        (handle-ops parse-tree)
                        (assoc :assign assign))
         truncated-state (when (and cursor expression)
                           (generate-truncated-state expression cursor connection-id variables))]
     (post-handle full-state truncated-state))))

