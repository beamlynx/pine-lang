(ns pine.db.connections
  (:require
   [clojure.string :as s])
  (:import
   (com.zaxxer.hikari HikariConfig HikariDataSource)))

(defn default-port [dbtype]
  (if (= dbtype "mysql") 3306 5432))

(defn jdbc-url
  "Pure JDBC URL builder, one branch per dialect. Exposed standalone (not
  just inlined into create-hikari-config) so its shape is directly testable
  without spinning up a real HikariDataSource.

  MySQL's connection params: allowPublicKeyRetrieval=true is required for
  MySQL 8's default caching_sha2_password auth over a non-TLS connection
  (otherwise connecting to a local MySQL 8 fails outright), and
  tinyInt1isBit=false keeps TINYINT(1) as a plain 0/1 integer rather than
  the driver silently turning it into a Java Boolean (pairs with
  data_types.clj treating tinyint as an integer, not a boolean). sslMode is
  left at its default (PREFERRED) rather than disabled."
  [{:keys [dbtype host port dbname] :or {dbtype "postgres"}}]
  (let [port (or port (default-port dbtype))]
    (case dbtype
      "mysql" (str "jdbc:mysql://" host ":" port "/" dbname
                   "?connectionTimeZone=SERVER&forceConnectionTimeZoneToSession=false"
                   "&preserveInstants=false&allowPublicKeyRetrieval=true"
                   "&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false"
                   "&characterEncoding=UTF-8"
                   ;; Never let a server ask this process for local files.
                   ;; A malicious MySQL server can otherwise request any
                   ;; file pine-lang can read, during any query.
                   "&allowLoadLocalInfile=false&allowUrlInLocalInfile=false")
      (str "jdbc:postgresql://" host ":" port "/" dbname))))

(defn- create-hikari-config [{:keys [dbtype schema] :as config}]
  (let [hc (doto (HikariConfig.)
             (.setJdbcUrl (jdbc-url config))
             (.setUsername (:user config))
             (.setPassword (:password config))
             ;; More than one, so a slow query doesn't block every other
             ;; request on this connection (they used to wait 10 s and fail
             ;; with "Connection is not available").
             (.setMaximumPoolSize 3)
             (.setMinimumIdle 1)           ; Keep one idle connection
             (.setIdleTimeout 600000)      ; 10 minutes idle timeout
             (.setConnectionTimeout 10000) ; 10 seconds connection timeout
             (.setMaxLifetime 3600000)     ; 1 hour max lifetime
             (.setAutoCommit true)         ; Each statement commits; writes use their own transaction
             (.setReadOnly false))]
    (case dbtype
      ;; MySQL's "database" is already fully determined by the URL path -
      ;; there's no separate schema concept to set. Set the driver class
      ;; explicitly too: more robust than relying on META-INF/services
      ;; driver discovery inside the AOT'd uberjar/jpackage build.
      "mysql" (.setDriverClassName hc "com.mysql.cj.jdbc.Driver")
      (.setSchema hc schema))
    hc))

(defn- bad-connection [message]
  (throw (ex-info message {:error-type "bad-connection"})))

;; jdbc-url builds the URL by concatenation, so these fields must not carry
;; URL syntax. A `?` or `&` in a database name used to add driver options:
;; `db?allowLoadLocalInfile=true` on MySQL, `db?socketFactory=...` on
;; Postgres. The user and password are set on the pool, never in the URL,
;; so they may contain anything.
(def ^:private host-pattern #"^(?:[A-Za-z0-9._-]+|\[[0-9A-Fa-f:.]+\])$")
(def ^:private name-pattern #"^[\p{L}\p{N}_.$-]+$")

(defn validate-connection
  "The connection config with its port as an integer (or nil for the
  default), or an ex-info with :error-type \"bad-connection\" naming the
  field that's wrong. Never puts the password in ex-data."
  [{:keys [dbtype host port dbname schema user password] :as config}]
  (let [dbtype (or dbtype "postgres")]
    (when-not (#{"postgres" "mysql"} dbtype)
      (bad-connection (str "Unknown database type `" dbtype "`. Use postgres or mysql.")))
    (when-not (and (string? host) (re-matches host-pattern host))
      (bad-connection "The host must be a hostname or an IP address, with no port, path or options."))
    (when-not (and (string? dbname) (re-matches name-pattern dbname))
      (bad-connection "The database name may contain only letters, digits, and _ . $ -"))
    (when-not (or (nil? schema) (= "" schema) (and (string? schema) (re-matches name-pattern schema)))
      (bad-connection "The schema name may contain only letters, digits, and _ . $ -"))
    (when-not (string? user)
      (bad-connection "The user is required."))
    (when-not (string? password)
      (bad-connection "The password is required."))
    (let [port (cond
                 (or (nil? port) (= "" port)) nil
                 (integer? port) port
                 (and (string? port) (re-matches #"[0-9]{1,5}" port)) (Long/parseLong port)
                 :else (bad-connection "The port must be a number."))]
      (when (and port (not (<= 1 port 65535)))
        (bad-connection "The port must be between 1 and 65535."))
      (assoc config :dbtype dbtype :port port :schema (not-empty schema)))))

(defn create-pool [config]
  (HikariDataSource. (create-hikari-config (validate-connection config))))

(def pools "Database connection pools" (atom {}))

(def test-connection-ids
  "Sentinel connection ids that bypass real connection pools and live schema
  lookups in favor of fixtures - mapped to the dialect each one pretends to
  be, so get-dialect and every schema/query-building call resolves them the
  same way it would a real connection. Defined here - the db namespace
  nothing else in pine.db depends on - so every place that needs to
  recognize them (schema lookup in pine.db.main, connection-name lookup
  below) checks the one predicate instead of each hardcoding the ids
  separately."
  {:test :postgres
   :test-mysql :mysql})

(defn test-connection? [id]
  (contains? test-connection-ids id))

(defn get-dialect
  "Resolve the dialect for a connection id: the test sentinels first, then
  the registered pool's own JDBC URL scheme, defaulting to :postgres on
  anything else (no pool registered yet, or a fake/lazy value in tests).
  This sits on the hot path of every query build, so it reads the raw pools
  map directly rather than going through get-connection-pool - that fn can
  realize a lazy thunk and register it, which schema-only query building
  must never trigger as a side effect."
  [id]
  (or (get test-connection-ids id)
      (let [pool (@pools id)]
        (when (and (instance? HikariDataSource pool)
                   (s/starts-with? (.getJdbcUrl ^HikariDataSource pool) "jdbc:mysql:"))
          :mysql))
      :postgres))

(defn get-connection-pool [id]
  (let [pool-or-fn (@pools id)]
    (if pool-or-fn
      (if (fn? pool-or-fn)
        (let [pool (pool-or-fn)]
          (swap! pools assoc id pool)
          pool)
        pool-or-fn)
      (throw (ex-info "Connection not found" {:id id})))))

(defn parse-jdbc-url
  "Parses jdbc:<scheme>://host:port/dbname, stopping the dbname segment at
  a `?` or `;` suffix. A bare (s/split url #\"/\") used to take the dbname
  segment naively - fine for Postgres, but a MySQL URL's ?connectionTimeZone=...
  suffix lands in that same segment and corrupted the label
  (e.g. \"pine?connectionTimeZone=SERVER&...\")."
  [url]
  (let [[_ host-port dbname] (re-find #"^jdbc:[^:]+://([^/]+)/([^?;]*)" url)]
    {:host-port host-port :dbname dbname}))

(defn make-connection-id
  "host:port alone can't identify a connection -- two different databases on
  the same server need distinct ids so both can be registered at once, so
  dbname is folded in too. Never parsed back apart (checked: nothing in this
  codebase splits an id), only ever compared for equality or used as a `@pools`
  key and a Compojure route segment (`/api/v1/connections/:id/...`) -- which
  is also why dbname is joined with `:` rather than `/`: a literal `/` would
  split into two path segments and 404."
  [pool]
  (let [{:keys [host-port dbname]} (parse-jdbc-url (.getJdbcUrl pool))]
    (str host-port ":" dbname)))

(defn jdbc-url->label [url]
  (let [{:keys [host-port dbname]} (parse-jdbc-url url)]
    (str host-port " · " dbname)))

(defn make-connection-label [pool]
  (jdbc-url->label (.getJdbcUrl pool)))

(defn get-connection-name [id]
  (if (test-connection? id)
    (name id)
    (-> id get-connection-pool make-connection-id)))

(defn list-connections []
  (mapv (fn [[id _]]
          {:id id
           :label (make-connection-label (get-connection-pool id))})
        @pools))

(defn- same-target?
  "Whether an already-registered pool points at the same database, as the
  same user, as a newly built one. Compared on the JDBC URL and username
  rather than just the connection id: two ids only collide here when host,
  port, and dbname all match (make-connection-id folds dbname in), so the
  one remaining case this distinguishes is the same database registered
  again under a different user."
  [existing candidate]
  (and (instance? HikariDataSource existing)
       (= (.getJdbcUrl ^HikariDataSource existing) (.getJdbcUrl candidate))
       (= (.getUsername ^HikariDataSource existing) (.getUsername candidate))))

(defn add-connection-pool
  "Registers a pool for `connection` and returns its id.

  Never disturbs a pool that is already registered. Registering the same
  database as the same user reuses the existing pool; registering it again
  as a *different* user is rejected.

  Both halves fix real problems. A connection id is derived from the pool's
  own `host:port:dbname`, so re-registering the same database always lands
  on the same key. This used to be a bare `swap! pools assoc`, which
  overwrote the entry and left the displaced HikariDataSource open with
  nothing referencing it. The pool config sets `minimumIdle 1`, so every
  orphan held a real Postgres connection for the life of the process -- 32
  leaked pools turned up in a single desktop session, against Postgres's
  default limit of 100. A long-running session would eventually be unable to
  connect at all.

  Rejecting rather than replacing is deliberate. Closing the displaced pool
  would fix the leak, but it would also abort queries still running on it.
  Replacing silently pointed an existing connection id at a different user's
  session, which is a correctness hazard on top of the leak. An explicit
  error is the honest version of that limitation. Callers that genuinely
  want to swap targets can `remove-connection-pool` first, which closes the
  pool properly.

  Reuse compares the JDBC URL and username, so a changed *password* reuses
  the existing pool rather than rebuilding it. If credentials were rotated
  server-side, disconnect and reconnect to pick them up."
  [connection]
  (let [candidate (create-pool connection)
        id (make-connection-id candidate)
        ;; Decided inside the atom: two registrations of the same database at
        ;; once (a double click, a retry) used to both read nil here, and the
        ;; second assoc orphaned the first pool, open, for good.
        [before _] (swap-vals! pools #(if (contains? % id) % (assoc % id candidate)))
        existing (get before id)]
    (cond
      (nil? existing)
      id

      (same-target? existing candidate)
      ;; Closing the redundant candidate is what stops the leak -- it has
      ;; already opened its minimumIdle connection by this point.
      (do (.close candidate) id)

      :else
      (do (.close candidate)
          (throw (ex-info
                  (format (str "Connection id \"%s\" is already registered under a different user. "
                               "Disconnect \"%s\" first.")
                          id id)
                  {:id id}))))))

(defn remove-connection-pool [id]
  (.close (get-connection-pool id))
  (swap! pools dissoc id))
