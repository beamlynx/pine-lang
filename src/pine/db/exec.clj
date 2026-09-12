(ns pine.db.exec
  "Dialect-agnostic clojure.java.jdbc plumbing. Every fn takes the param
  conversion appropriate to whichever dialect is calling in (postgres.clj's
  convert-param vs. mysql.clj's) rather than hardcoding one - the JDBC calls
  themselves (query/execute!/db-transaction*) don't differ across dialects."
  (:require [clojure.java.jdbc :as jdbc]
            [pine.db.connections :as connections]))

(def ^:private log-queries?
  "Per-query logging, opt-in via PINE_LOG_QUERIES=1.

  These prints used to be unconditional, which was a real hazard rather than
  just noise. Whatever launches pine-server has to drain its stdout; if
  nothing does, the pipe buffer fills and every `prn` blocks forever, which
  deadlocks every endpoint that runs SQL. beamlynx-desktop hit exactly that
  -- 17 threads parked in StreamEncoder.write inside run-query, while
  /api/v1/build kept answering because it reads an in-memory index and never
  prints. The consumer side is fixed too (beamlynx-desktop's
  server-process.ts now drains stdout), but a firehose that prints every
  query's full SQL should not be on by default regardless: it is also the
  query text, including literal values, going to a stream nobody asked to
  receive it on."
  (= "1" (System/getenv "PINE_LOG_QUERIES")))

(defn- log-query [fmt & args]
  (when log-queries?
    (prn (apply format fmt args))))

(defn run-query [id query convert-param]
  (let [pool (connections/get-connection-pool id)
        {:keys [query params]} query
        params (map convert-param params)
        _ (log-query "Running query: %s" query)
        result (with-open [conn (.getConnection pool)]
                 (jdbc/query {:connection conn} (cons query params) {:as-arrays? true :identifiers identity}))
        _ (log-query "Done!")]
    result))

(defn run-action-query [id query convert-param]
  (let [pool (connections/get-connection-pool id)
        {:keys [query params]} query
        params (map convert-param params)
        _ (log-query "Running action: %s" query)
        result (with-open [conn (.getConnection pool)]
                 (jdbc/execute! {:connection conn} (cons query params)))
        affected-rows (first result)
        _ (log-query "Affected rows: %d" affected-rows)]
    affected-rows))

(defn run-action-queries-in-transaction
  "Runs multiple action queries in a single transaction using jdbc/db-transaction*.
   If any query fails, the entire transaction is rolled back.
   Uses a connection from the HikariCP pool."
  [id queries convert-param]
  (let [pool (connections/get-connection-pool id)]
    (with-open [conn (.getConnection pool)]
      (jdbc/db-transaction*
       {:connection conn}
       (fn [tx]
         (mapv (fn [{:keys [table query params]}]
                 (let [params (map convert-param (or params []))
                       _ (log-query "Running action (tx): %s" query)
                       result (jdbc/execute! tx (cons query params) {:transaction? false})
                       affected (first result)]
                   (log-query "Affected rows: %d" affected)
                   [(or table "table") affected]))
               queries))))))

(defn run-sql [id sql-query]
  "Execute raw SQL query. Automatically detects if it's a SELECT or action query."
  (when (or (nil? sql-query) (clojure.string/blank? sql-query))
    (throw (IllegalArgumentException. "SQL query cannot be null or empty")))

  (let [pool (connections/get-connection-pool id)
        trimmed-query (clojure.string/trim (clojure.string/upper-case sql-query))
        is-select? (or (clojure.string/starts-with? trimmed-query "SELECT")
                       (clojure.string/starts-with? trimmed-query "WITH")
                       (clojure.string/starts-with? trimmed-query "SHOW")
                       (clojure.string/starts-with? trimmed-query "EXPLAIN"))
        _ (log-query "Running raw SQL: %s" sql-query)
        result (with-open [conn (.getConnection pool)]
                 (if is-select?
                   (jdbc/query {:connection conn} sql-query {:as-arrays? true :identifiers identity})
                   (jdbc/execute! {:connection conn} sql-query)))
        _ (log-query "Done!")]
    (if is-select?
      result
      ;; Return array format for action queries to match expected structure
      [["Rows affected"] [(first result)]])))
