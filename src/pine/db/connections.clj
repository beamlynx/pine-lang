(ns pine.db.connections
  (:require
   [clojure.string :as s])
  (:import
   (com.zaxxer.hikari HikariConfig HikariDataSource)))

(defn- create-hikari-config [config]
  (doto (HikariConfig.)
    (.setJdbcUrl (str "jdbc:postgresql://" (:host config) ":" (:port config) "/" (:dbname config)))
    (.setUsername (:user config))
    (.setPassword (:password config))
    (.setSchema (:schema config))
    (.setMaximumPoolSize 1)       ; Only need one connection
    (.setMinimumIdle 1)           ; Keep one idle connection
    (.setIdleTimeout 600000)      ; 10 minutes idle timeout
    (.setConnectionTimeout 10000) ; 10 seconds connection timeout
    (.setMaxLifetime 3600000)     ; 1 hour max lifetime
    (.setAutoCommit true)         ; Disable auto-commit
    (.setReadOnly false)))        ; Read-only mode disabled

(defn create-pool [config]
  (let [config (merge {:dbtype "postgres" :port 5432} config)]
    (when (some nil? (vals (select-keys config [:host :dbname :user :password])))
      (throw (ex-info "Missing required database configuration" {:config config})))
    (HikariDataSource. (create-hikari-config config))))

(def pools "Database connection pools" (atom {}))

(def test-connection-id
  "Sentinel connection id that bypasses real connection pools and live schema
  lookups in favor of fixtures. Defined here — the db namespace nothing else
  in pine.db depends on — so every place that needs to recognize it (schema
  lookup in postgres.clj, connection-name lookup below) checks the one
  predicate instead of each hardcoding :test separately."
  :test)

(defn test-connection? [id]
  (= id test-connection-id))

(defn get-connection-pool [id]
  (let [pool-or-fn (@pools id)]
    (if pool-or-fn
      (if (fn? pool-or-fn)
        (let [pool (pool-or-fn)]
          (swap! pools assoc id pool)
          pool)
        pool-or-fn)
      (throw (ex-info "Connection not found" {:id id})))))

(defn make-connection-id [pool]
  (-> pool .getJdbcUrl (s/split #"/") (nth 2)))

(defn jdbc-url->label [url]
  (let [parts (s/split url #"/")
        host-port (nth parts 2)
        dbname (nth parts 3)]
    (str host-port " · " dbname)))

(defn make-connection-label [pool]
  (jdbc-url->label (.getJdbcUrl pool)))

(defn get-connection-name [id]
  (if (test-connection? id)
    "test"
    (-> id get-connection-pool make-connection-id)))

(defn list-connections []
  (mapv (fn [[id _]]
          {:id id
           :label (make-connection-label (get-connection-pool id))})
        @pools))

(defn- same-target?
  "Whether an already-registered pool points at the same database, as the
  same user, as a newly built one. Compared on the JDBC URL and username
  rather than the connection id, because the id is only `host:port` -- two
  different databases on one server share an id, so matching ids alone does
  not mean the pools are interchangeable."
  [existing candidate]
  (and (instance? HikariDataSource existing)
       (= (.getJdbcUrl ^HikariDataSource existing) (.getJdbcUrl candidate))
       (= (.getUsername ^HikariDataSource existing) (.getUsername candidate))))

(defn add-connection-pool
  "Registers a pool for `connection` and returns its id.

  Never disturbs a pool that is already registered. Registering the same
  database as the same user reuses the existing pool; anything else that
  would land on an id already in use is rejected.

  Both halves fix real problems. A connection id is derived from the pool's
  own `host:port`, so re-registering the same database always lands on the
  same key. This used to be a bare `swap! pools assoc`, which overwrote the
  entry and left the displaced HikariDataSource open with nothing
  referencing it. The pool config sets `minimumIdle 1`, so every orphan held
  a real Postgres connection for the life of the process -- 32 leaked pools
  turned up in a single desktop session, against Postgres's default limit of
  100. A long-running session would eventually be unable to connect at all.

  Rejecting rather than replacing is deliberate. Closing the displaced pool
  would fix the leak, but it would also abort queries still running on it --
  and because the id is only `host:port`, the pool being closed could belong
  to a *different* database the user is actively querying. Note this takes
  nothing away: two databases on one server share an id, so they could never
  coexist in this map anyway. Replacing silently pointed an existing
  connection id at a different database, which is a correctness hazard on
  top of the leak. An explicit error is the honest version of a limitation
  that was already there. Callers that genuinely want to swap targets can
  `remove-connection-pool` first, which closes the pool properly.

  Reuse compares the JDBC URL and username, so a changed *password* reuses
  the existing pool rather than rebuilding it. If credentials were rotated
  server-side, disconnect and reconnect to pick them up."
  [connection]
  (let [candidate (create-pool connection)
        id (make-connection-id candidate)
        existing (@pools id)]
    (cond
      (nil? existing)
      (do (swap! pools assoc id candidate) id)

      (same-target? existing candidate)
      ;; Closing the redundant candidate is what stops the leak -- it has
      ;; already opened its minimumIdle connection by this point.
      (do (.close candidate) id)

      :else
      (do (.close candidate)
          (throw (ex-info
                  (format (str "Connection id \"%s\" is already in use by a different database or user. "
                               "pine identifies a connection by host and port only, so two databases on the "
                               "same server cannot both be registered. Disconnect \"%s\" first.")
                          id id)
                  {:id id}))))))

(defn remove-connection-pool [id]
  (.close (get-connection-pool id))
  (swap! pools dissoc id))