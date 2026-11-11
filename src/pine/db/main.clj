(ns pine.db.main
  (:require [pine.db.connections :as connections]
            [pine.db.fixtures :as fixtures]
            [pine.db.mysql :as mysql]
            [pine.db.postgres :as postgres]
            [pine.db.references :as refs]
            [pine.db.sqlite :as sqlite]))

;; Application state
;;
;; There is no "selected" connection. Every request names the connection it
;; runs on. A server-wide default used to fill in for a request that named
;; none, and that default was whichever connection any client had last
;; picked: a tab with no connection of its own then ran its queries on
;; another tab's database.
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
    :sqlite (sqlite/get-references-helper id)
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

(def ^:private indexed-through
  "The pool each cached schema in `references` was read through. A schema
  counts only while that pool is still the one registered under its id.
  Without this, a connection removed and opened again under the same id,
  on another database, could be given the first database's schema: by a
  read that started before the swap and finished after it."
  (atom {}))

(defn- current-pool
  "The pool registered under id now, opened if it was lazy, so the one an
  index reads through is the one compared afterwards. Nil for the test
  sentinels and for an id with no pool."
  [id]
  (when (and (not (connections/test-connection? id)) (contains? @connections/pools id))
    (connections/get-connection-pool id)))

(defn- index-through!
  "Reads id's schema and caches it, unless the pool changed while it was
  read: then the schema is another database's, and reading again is up to
  the caller."
  [id]
  (let [pool (current-pool id)
        indexed (get-indexed-references id)]
    (when-not (identical? pool (current-pool id))
      (throw (ex-info (str "Connection `" id "` changed while its schema was read. Try again.")
                      {:error-type "no-connection" :id id})))
    (swap! references assoc id indexed)
    (swap! indexed-through assoc id pool)
    indexed))

(defn init-references
  "The schema for a connection: cached while its pool is the one it was
  read through, read again otherwise."
  [id]
  (or
   (and memoize-references?
        (contains? @references id)
        (identical? (get @indexed-through id) (current-pool id))
        (@references id))
   (do
     (prn (format "Indexing schema for connection: %s" id))
     (index-through! id))))

;; Connections
;;
(defn index-connection
  "Indexes a connection's schema, so its first query doesn't wait for it.
  Throws when the database is down or the user can't read the catalog."
  [id]
  (init-references id)
  id)

(defn forget-connection
  "Drops a connection's cached schema, once its pool is closed."
  [id]
  (swap! references dissoc id)
  (swap! indexed-through dissoc id))

(defn reindex-references
  "Re-index the schema for a connection, bypassing the memoized cache -
  for when the underlying database's tables/columns changed after the
  connection was first indexed."
  [id]
  (prn (format "Reindexing schema for connection: %s" id))
  (index-through! id)
  id)

;; Query
;;
(defn run-query [id query]
  (case (connections/get-dialect id)
    :sqlite (sqlite/run-query id query)
    :mysql (mysql/run-query id query)
    (postgres/run-query id query)))

(defn run-action-query [id query]
  (case (connections/get-dialect id)
    :sqlite (sqlite/run-action-query id query)
    :mysql (mysql/run-action-query id query)
    (postgres/run-action-query id query)))

(defn run-action-queries-in-transaction [id queries]
  (case (connections/get-dialect id)
    :sqlite (sqlite/run-action-queries-in-transaction id queries)
    :mysql (mysql/run-action-queries-in-transaction id queries)
    (postgres/run-action-queries-in-transaction id queries)))

(defn run-sql [id sql-query]
  (case (connections/get-dialect id)
    :sqlite (sqlite/run-sql id sql-query)
    :mysql (mysql/run-sql id sql-query)
    (postgres/run-sql id sql-query)))

(defn get-connection-count [id]
  (let [count-sql (case (connections/get-dialect id)
                    :mysql mysql/connection-count-sql
                    :sqlite sqlite/connection-count-sql
                    postgres/connection-count-sql)
        result (run-query id {:query count-sql :params []})]
    (-> result second first)))
