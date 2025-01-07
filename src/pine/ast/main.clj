(ns pine.ast.main
  (:require [pine.parser :as parser]
            [pine.ast.table :as table]
            [pine.ast.limit :as limit]
            [pine.db.main :as db]
            [pine.ast.where :as where]
            [pine.ast.hints :as hints]
            [pine.ast.select :as select]
            [pine.ast.from :as from]
            [pine.ast.order :as order]
            [pine.ast.count :as pine-count]
            [pine.ast.delete-action :as delete-action]))

(def state {;; pre
            ;; - connection
            :connection-id nil
            :references {}

            ;; ast
            :tables          []           ;; e.g. [{ :table "user" :schema "public" :alias "u" }] ;; schema is nilable
            :selected-tables []           ;; e.g. [{ :table "user" :schema "public" :alias "u" }] ;; schema is nilable
            :columns         []           ;; e.g. [{ :alias "u" :column "name" }]
            :limit           nil          ;; number ;; nilable
            :aliases         {}           ;; e.g. [{ :schema "public" :table "user" }] ;; schema is nilable
            :joins           []           ;; Vector of joins e.g. [ "u" "c" ".. relation .."]
            :join-map        {}           ;; Map of aliases of the joined tables e.g. { "u" { "c" [".. relation .."]}}
            :where           []           ;; e.g. [ "name" "=" "john" ]
            :order           []           ;; e.g. [{ :alias "u" :column "name" :direction "DESC" }]

            ;; state
            :operation      {:type  nil
                             :value nil} ;; [ ] 1. For post-handle. e.g. set hints if operation is table.
                                         ;; [ ] 2. For backwards compat with version < 0.5.
                                         ;;        If op is :table, then the context  in the api handler has one less table

            :current        nil          ;; alias of the current table
            :context        nil          ;; alias of the table in context

            :table-count    0
            :pending-count  0

            ;; post
            ;; - hints
            :hints          {:table [] :select []}})

(defn pre-handle [state connection-id ops-count]
  (-> state
      (assoc :references (db/init-references connection-id))
      (assoc :connection-id connection-id)
      (assoc :pending-count ops-count)))

(defn handle-op [state {:keys [type value]}]
  (case type
    :select (select/handle state value)
    :select-partial (select/handle state value)
    :table (table/handle state value)
    :limit (limit/handle state value)
    :where (where/handle state value)
    :from (from/handle state value)
    :order (order/handle state value)
    :count (pine-count/handle state value)
    :delete-action (delete-action/handle state value)
    ;; No operations
    :no-op state
    (update state :errors conj [type "Unknown operation type in parse tree"])))

(defn handle-ops [state ops]
  (reduce (fn [s o] (-> s
                        (handle-op o)
                        (update :pending-count dec)
                        (assoc :operation o))) state ops))

(defn post-handle [state]
  (-> state
      hints/handle
      (assoc :selected-tables (let [tables (state :tables)
                                    type (-> state :operation :type)]
                                (if
                                 (= type :table)
                                  (-> tables reverse rest reverse)
                                  tables)))

      (dissoc :references)))

(defn generate
  ([parse-tree]
   (generate parse-tree @db/connection-id))
  ([parse-tree connection-id]
   (-> state
       (pre-handle connection-id (count parse-tree))
       (handle-ops parse-tree)
       post-handle)))

