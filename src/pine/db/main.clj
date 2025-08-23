(ns pine.db.main
  (:require [pine.db.postgres :as postgres]))

;; Application state
(def connection-id "Currently selected connection" (atom nil))
(def references "References indexed by the connection id" (atom {}))

;; Memoization
;;
(def memoize-references? true)
;; (def memoize-references? false)

;; Schema / Initialization
;;
(defn init-references
  "Get the references for a given key"
  [id]
  (or
   (and memoize-references?
        (@references id))
   (do
     (swap! references assoc id (postgres/get-indexed-references id))
     (@references id))))

;; Connections
;;
(defn set-connection [id]
  (reset! connection-id id)
  (init-references id)
  id)

;; Query
;;
(defn run-query [id query]
  (postgres/run-query id query))

(defn run-action-query [id query]
  (postgres/run-action-query id query))

(defn get-connection-count [id]
  (let [result (run-query id {:query "SELECT COUNT(*) as connection_count FROM pg_stat_activity" :params []})]
    (-> result second first)))
