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

(def state {;; pre
            ;; - connection
            :connection-id nil
            :references {}
            :expression      nil          ;; Expression string for cursor-aware hints
            :cursor          nil          ;; Cursor position {:line N :character M} (zero-indexed)

            ;; ast
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

            ;; post
            ;; - hints
            :hints          {:table [] :select [] :order [] :where []}})

(defn pre-handle [state connection-id ops-count expression cursor]
  (-> state
      (assoc :references (db/init-references connection-id))
      (assoc :connection-id connection-id)
      (assoc :pending-count ops-count)
      (assoc :expression expression)
      (assoc :cursor cursor)))

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
  [expression cursor connection-id]
  (let [truncated-expr (truncate-at-cursor expression cursor)
        {:keys [result error]} (parser/parse truncated-expr)]
    (if (or error (nil? result))
      ;; Parse error or no result, return nil
      nil
      ;; Successfully parsed, build state without going through post-handle
      ;; to preserve references for hint generation
      (-> state
          (pre-handle connection-id (count result) nil nil)
          (handle-ops result)))))

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

      (dissoc :references)))

(defn generate
  ([parse-tree]
   (generate parse-tree @db/connection-id nil nil))
  ([parse-tree connection-id]
   (generate parse-tree connection-id nil nil))
  ([parse-tree connection-id expression cursor]
   (let [full-state (-> state
                        (pre-handle connection-id (count parse-tree) expression cursor)
                        (handle-ops parse-tree))
         truncated-state (when (and cursor expression)
                           (generate-truncated-state expression cursor connection-id))]
     (post-handle full-state truncated-state))))

