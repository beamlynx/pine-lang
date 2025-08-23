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

(defn get-connection-name [id]
  (-> id get-connection-pool make-connection-id))

(defn add-connection-pool [connection]
  (let [pool (create-pool connection)
        id (make-connection-id pool)]
    (swap! pools assoc id pool)
    id))