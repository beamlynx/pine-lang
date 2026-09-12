(ns pine.db.main
  (:require [pine.db.connections :as connections]
            [pine.db.fixtures :as fixtures]
            [pine.db.mysql :as mysql]
            [pine.db.postgres :as postgres]
            [pine.db.references :as refs]))

;; Application state
(def connection-id "Currently selected connection" (atom nil))
(def references "References indexed by the connection id" (atom {}))

;; Memoization
;;
(def memoize-references? true)
;; (def memoize-references? false)

;; Schema / Initialization
;;
(defn- get-references-helper [id]
  (case (connections/get-dialect id)
    :mysql (mysql/get-references-helper id)
    (postgres/get-references-helper id)))

(defn get-indexed-references
  "The one seam both init-references and reindex-references call through -
  the :test/:test-mysql fixture short-circuit lives here, checked once
  before any dialect-specific namespace is ever entered, rather than
  duplicated inside postgres.clj and mysql.clj."
  [id]
  (refs/index-references
   (if (connections/test-connection? id)
     fixtures/references
     (get-references-helper id))))

(defn init-references
  "Get the references for a given key"
  [id]
  (or
   (and memoize-references?
        (@references id))
   (do
     (prn (format "Indexing schema for connection: %s" id))
     (swap! references assoc id (get-indexed-references id))
     (@references id))))

;; Connections
;;
(defn set-connection [id]
  (reset! connection-id id)
  (init-references id)
  id)

(defn clear-connection-if
  "Clears the selected connection and its cached references when it matches id
  (a no-op when a different connection is selected)."
  [id]
  (swap! connection-id (fn [current] (if (= current id) nil current)))
  (swap! references dissoc id))

(defn reindex-references
  "Re-index the schema for a connection, bypassing the memoized cache -
  for when the underlying database's tables/columns changed after the
  connection was first indexed."
  [id]
  (prn (format "Reindexing schema for connection: %s" id))
  (swap! references assoc id (get-indexed-references id))
  id)

;; Query
;;
(defn run-query [id query]
  (case (connections/get-dialect id)
    :mysql (mysql/run-query id query)
    (postgres/run-query id query)))

(defn run-action-query [id query]
  (case (connections/get-dialect id)
    :mysql (mysql/run-action-query id query)
    (postgres/run-action-query id query)))

(defn run-action-queries-in-transaction [id queries]
  (case (connections/get-dialect id)
    :mysql (mysql/run-action-queries-in-transaction id queries)
    (postgres/run-action-queries-in-transaction id queries)))

(defn run-sql [id sql-query]
  (case (connections/get-dialect id)
    :mysql (mysql/run-sql id sql-query)
    (postgres/run-sql id sql-query)))

(defn get-connection-count [id]
  (let [count-sql (case (connections/get-dialect id)
                    :mysql mysql/connection-count-sql
                    postgres/connection-count-sql)
        result (run-query id {:query count-sql :params []})]
    (-> result second first)))
