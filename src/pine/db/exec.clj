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

(def query-timeout-seconds
  "Every statement is cancelled by the driver after this long. A pool holds
  only a few connections, so a query that runs for an hour would otherwise
  hold one of them for an hour."
  60)

(def max-raw-sql-rows
  "Rows /api/v1/sql returns at most. Pine queries have their own limits
  (eval.clj); raw SQL had none, and a big table filled the server's memory."
  10000)

(defn- log-query [fmt & args]
  (when log-queries?
    (prn (apply format fmt args))))

;; ---------------------------------------------------------------------------
;; Stopping a running query
;; ---------------------------------------------------------------------------
;;
;; A client names each run with an id it makes up (`run-id` on /eval and
;; /sql), and can then ask for that run to be stopped (/cancel). The route
;; binds *run-id* for the length of the request, so nothing between it and
;; the statements below has to pass it along.

(def ^:dynamic *run-id*
  "The id the client gave the current request, or nil."
  nil)

(def ^:private statements
  "Run id -> the statement it is executing right now."
  (atom {}))

(def ^:private stopped
  "Run id -> when it was asked to stop, in milliseconds. A stop request can
  arrive before the run's first statement does, so it is kept until the run
  finishes, or for ten minutes if the run never comes."
  (atom {}))

(def ^:private stopped-ttl-ms (* 10 60 1000))

(defn- stopped-error []
  (ex-info "Query stopped." {:error-type "cancelled"}))

(defn cancel!
  "Stops the run with this id: cancels the statement it is executing, and
  refuses any statement it starts after this. Returns whether a statement
  was running."
  [run-id]
  (let [now (System/currentTimeMillis)]
    (swap! stopped (fn [m]
                     (assoc (into {} (remove (fn [[_ t]] (> (- now t) stopped-ttl-ms))) m)
                            run-id now))))
  (if-let [^java.sql.Statement stmt (get @statements run-id)]
    (do (.cancel stmt) true)
    false))

(defn finish-run!
  "Forgets a run once its request is done."
  [run-id]
  (swap! stopped dissoc run-id)
  (swap! statements dissoc run-id))

(defn- run-statement
  "Calls (f stmt) with a statement prepared on conn. While it runs, the
  statement is registered under *run-id*, so cancel! can stop it. A
  statement that fails because it was stopped throws `Query stopped.`, with
  `error-type: \"cancelled\"`, not the database's message, which on
  Postgres is the same for a stop and for the 60-second timeout."
  [conn sql opts f]
  (let [run-id *run-id*]
    (when (and run-id (contains? @stopped run-id))
      (throw (stopped-error)))
    (with-open [stmt (jdbc/prepare-statement conn sql opts)]
      (when run-id (swap! statements assoc run-id stmt))
      (try
        ;; A stop that came in between the check above and the line above
        ;; found no statement to cancel.
        (when (and run-id (contains? @stopped run-id))
          (throw (stopped-error)))
        (f stmt)
        (catch java.sql.SQLException e
          (if (and run-id (contains? @stopped run-id))
            (throw (stopped-error))
            (throw e)))
        (finally
          (when run-id (swap! statements dissoc run-id)))))))

(defn run-query [id query convert-param]
  (let [pool (connections/get-connection-pool id)
        {:keys [query params]} query
        params (map convert-param params)
        _ (log-query "Running query: %s" query)
        result (with-open [conn (.getConnection pool)]
                 (run-statement conn query {:timeout query-timeout-seconds}
                                #(jdbc/query {:connection conn} (cons % params)
                                             {:as-arrays? true :identifiers identity})))
        _ (log-query "Done!")]
    result))

(defn run-action-query [id query convert-param]
  (let [pool (connections/get-connection-pool id)
        {:keys [query params]} query
        params (map convert-param params)
        _ (log-query "Running action: %s" query)
        result (with-open [conn (.getConnection pool)]
                 (run-statement conn query {:timeout query-timeout-seconds}
                                #(jdbc/execute! {:connection conn} (cons % params))))
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
                       result (run-statement (jdbc/db-find-connection tx) query {:timeout query-timeout-seconds}
                                             #(jdbc/execute! tx (cons % params) {:transaction? false}))
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
                   ;; One row past the cap, to know whether it was reached.
                   (run-statement conn sql-query {:timeout query-timeout-seconds :max-rows (+ 2 max-raw-sql-rows)}
                                  #(jdbc/query {:connection conn} [%]
                                               {:as-arrays? true :identifiers identity}))
                   (run-statement conn sql-query {:timeout query-timeout-seconds}
                                  #(jdbc/execute! {:connection conn} [%]))))
        _ (log-query "Done!")]
    (if is-select?
      ;; as-arrays: the first row is the header.
      (if (> (count result) (inc max-raw-sql-rows))
        (conj (vec (take (inc max-raw-sql-rows) result))
              (into [(str "... cut at " max-raw-sql-rows " rows")]
                    (repeat (dec (count (first result))) nil)))
        result)
      ;; Return array format for action queries to match expected structure
      [["Rows affected"] [(first result)]])))
