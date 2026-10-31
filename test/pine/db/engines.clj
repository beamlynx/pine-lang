(ns pine.db.engines
  "Runs the same test on real SQLite, MySQL and Postgres databases.

  Each topic file in test/pine/db (queries, writes, JSON paths, ...) calls
  `on-every-engine` with its own fixture. Every call gets a fresh database
  on each engine, filled with the fixture, and dropped afterwards, so tests
  never see each other's rows.

  SQLite runs on every `clojure -M:test`, in a temporary file. MySQL and
  Postgres need a server each, named by PINE_TEST_MYSQL_URL and
  PINE_TEST_POSTGRES_URL. Without one, its engine is left out, and the run
  says so. With PINE_TEST_REQUIRE_SERVERS=1, as in CI, a missing server is a
  failure instead, so a typo can't skip the tests unnoticed.

  The user in each URL must be allowed to create a database. To run
  everything locally, see scripts/test-databases.sh."
  (:require [cheshire.core :as json]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.test :refer [is testing]]
            [pine.api :as api]
            [pine.db.connections :as connections]
            [pine.db.main :as db])
  (:import (java.io File)
           (java.net URI)
           (java.util UUID)))

(def ^:private servers
  {:mysql (System/getenv "PINE_TEST_MYSQL_URL")
   :postgres (System/getenv "PINE_TEST_POSTGRES_URL")})

(def ^:private require-servers? (= "1" (System/getenv "PINE_TEST_REQUIRE_SERVERS")))

(def ^:private skipped (atom #{}))

(defn server-url
  "The URL of the test server for `engine` (:mysql or :postgres), or nil
  when none is named, which is said once. With PINE_TEST_REQUIRE_SERVERS=1,
  a missing one throws instead."
  [engine]
  (or (servers engine)
      (let [variable (str "PINE_TEST_" (s/upper-case (name engine)) "_URL")]
        (when require-servers?
          (throw (ex-info (str "PINE_TEST_REQUIRE_SERVERS is set, but " variable " isn't.") {:engine engine})))
        (when-not (@skipped engine)
          (swap! skipped conj engine)
          (println (str "Skipping the " (name engine) " runs: set " variable " to run them.")))
        nil)))

(defn- engines
  "The engines to run on: SQLite, and each server that is named."
  []
  (into [:sqlite] (filter server-url [:mysql :postgres])))

;; ---------------------------------------------------------------------------
;; The fixture's SQL
;; ---------------------------------------------------------------------------

(def ^:private types
  "The column types that differ between the engines, by placeholder. A key
  in MySQL can't be TEXT, so {text} there is VARCHAR."
  {:sqlite {"{json}" "JSON" "{bool}" "BOOLEAN" "{timestamp}" "DATETIME" "{text}" "TEXT"}
   :mysql {"{json}" "JSON" "{bool}" "BOOLEAN" "{timestamp}" "DATETIME" "{text}" "VARCHAR(255)"}
   :postgres {"{json}" "jsonb" "{bool}" "boolean" "{timestamp}" "timestamp" "{text}" "text"}})

(defn sql-for
  "A fixture statement for `engine`, with its placeholders for types filled
  in: `CREATE TABLE doc (id INTEGER PRIMARY KEY, data {json})`."
  [engine statement]
  (reduce-kv s/replace statement (types engine)))

;; ---------------------------------------------------------------------------
;; Fresh databases
;; ---------------------------------------------------------------------------

(defn- fresh-name [] (str "pine_test_" (System/currentTimeMillis) "_" (subs (str (UUID/randomUUID)) 0 8)))

(defn- server-spec [engine ^URI uri dbname]
  (let [[user password] (s/split (.getUserInfo uri) #":" 2)]
    {:connection-uri (str "jdbc:" (if (= engine :mysql) "mysql" "postgresql") "://"
                          (.getHost uri) ":" (.getPort uri) "/" dbname
                          "?user=" user "&password=" password)
     :user user :password password :host (.getHost uri) :port (.getPort uri) :dbname dbname}))

(defn- with-pine-connection [config f]
  (let [id (with-redefs [connections/sqlite-enabled? (constantly true)]
             (connections/add-connection-pool config))]
    (try (f id)
         (finally
           (connections/remove-connection-pool id)
           (db/clear-connection-if id)))))

(defn- on-sqlite [statements f]
  (let [file (File/createTempFile "pine-test" ".db")]
    (try
      (let [spec {:connection-uri (str "jdbc:sqlite:" (.getAbsolutePath file))}]
        (doseq [statement statements] (jdbc/execute! spec [(sql-for :sqlite statement)]))
        (with-pine-connection {:dbtype "sqlite" :dbname (.getAbsolutePath file)}
          #(f {:engine :sqlite :id % :spec spec :file file})))
      (finally (.delete file)))))

(defn- on-server [engine statements f]
  (let [uri (URI. (servers engine))
        admin (server-spec engine uri (subs (.getPath uri) 1))
        dbname (fresh-name)]
    (jdbc/execute! admin [(str "CREATE DATABASE " dbname)] {:transaction? false})
    (try
      (let [{:keys [host port user password] :as spec} (server-spec engine uri dbname)]
        (doseq [statement statements] (jdbc/execute! spec [(sql-for engine statement)]))
        (with-pine-connection {:dbtype (name engine) :host host :port port :dbname dbname :user user :password password}
          #(f {:engine engine :id % :spec spec})))
      (finally
        (jdbc/execute! admin [(str "DROP DATABASE IF EXISTS " dbname (when (= engine :postgres) " WITH (FORCE)"))]
                       {:transaction? false})))))

(defn on-engine
  "Calls (f db) on a fresh database of one engine, holding `fixture`. For a
  check only that engine has (see test/pine/db/engines/). Left out, as
  on-every-engine leaves it out, when its server isn't named."
  [engine fixture f]
  (when (or (= engine :sqlite) (server-url engine))
    (testing (name engine)
      (if (= engine :sqlite)
        (on-sqlite fixture f)
        (on-server engine fixture f)))))

(defn on-every-engine
  "Calls (f db) once for each engine, each time on a fresh database holding
  `fixture`, a list of statements (see sql-for). `db` is {:engine :id :spec}:
  the engine, Pine's connection id, and a clojure.java.jdbc spec to check
  results with, bypassing Pine. Failures name the engine."
  [fixture f]
  (doseq [engine (engines)]
    (on-engine engine fixture f)))

;; ---------------------------------------------------------------------------
;; Running Pine
;; ---------------------------------------------------------------------------

(defn pine
  "The result of an expression, run the way the app runs it (pine.api's
  /eval), as {:header :rows :columns :error}. `opts` may have :policy and
  :allow-writes."
  ([db expression] (pine db expression {}))
  ([{:keys [id]} expression {:keys [policy allow-writes] :or {policy [] allow-writes true}}]
   (let [{:keys [result columns error]} (api/api-eval [expression] id policy allow-writes)]
     {:header (mapv name (first result)) :rows (vec (rest result)) :columns columns :error error})))

(defn- plain
  "A value as the same Clojure value on every engine, so expectations are
  written once: a number as a long or a bigdec, true and false (MySQL and
  SQLite return 1 and 0 for a boolean column, and tests of booleans say so),
  a JSON value as the data it holds, anything else as text."
  [v]
  (cond
    (nil? v) nil
    (instance? Boolean v) v
    (integer? v) (long v)
    (number? v) (bigdec v)
    (and (string? v) (re-matches #"(?s)\s*[\[{].*" v)) (try (json/parse-string v) (catch Exception _ v))
    (instance? org.postgresql.util.PGobject v) (plain (.getValue ^org.postgresql.util.PGobject v))
    :else (str v)))

(defn rows
  "The visible rows of an expression, each value made plain. Asserts that it
  ran."
  [db expression]
  (let [{:keys [rows columns error]} (pine db expression)
        shown (keep-indexed (fn [i c] (when-not (:hidden c) i)) columns)]
    (is (nil? error) (str expression ": " error))
    (mapv (fn [row] (mapv #(plain (nth row %)) shown)) rows)))

(defn column
  "The first visible column of an expression's rows."
  [db expression]
  (mapv first (rows db expression)))

(defn write!
  "Runs an update! or delete!, the way the app does. Asserts that it ran, and
  returns its result rows: [[table count]] for update!, [[count]] for
  delete!."
  [db expression]
  (let [{:keys [rows error]} (pine db expression)]
    (is (nil? error) (str expression ": " error))
    rows))

(defn refused
  "The error an expression is refused with, or nil if it ran."
  ([db expression] (refused db expression {}))
  ([db expression opts] (:error (pine db expression opts))))

;; ---------------------------------------------------------------------------
;; Checking results without Pine
;; ---------------------------------------------------------------------------

(defn query
  "Rows of plain SQL, run with JDBC, not Pine, as maps with plain values."
  [{:keys [spec]} sql & params]
  (mapv (fn [row] (into {} (map (fn [[k v]] [k (plain v)]) row)))
        (jdbc/query spec (into [sql] params))))

(defn table
  "Every row of `table-name`, by `key-columns` (keywords: `[:id]`), read
  with JDBC. A table without a key is read by all its columns."
  [db table-name key-columns]
  (into {} (map (fn [row] [(mapv row key-columns) row])
                (query db (str "SELECT * FROM " table-name)))))

(defn snapshot
  "Every row of each table, by its key: {table {key row}}. `tables` is
  {table-name [key-column ...]}, the key columns as keywords."
  [db tables]
  (into {} (map (fn [[t k]] [t (table db t k)]) tables)))

(defn changes
  "What differs between two snapshots, by table: {:deleted #{key}, :added
  #{key}, :changed {key {column [before after]}}}, leaving out what is
  empty. A write test asserts the whole of it, so a write that touched one
  row too many fails."
  [before after]
  (into {}
        (keep (fn [t]
                (let [b (before t) a (after t)
                      deleted (set (remove a (keys b)))
                      added (set (remove b (keys a)))
                      changed (into {}
                                    (keep (fn [k]
                                            (when-let [was (b k)]
                                              (when-let [now (a k)]
                                                (let [diff (into {} (keep (fn [c] (when (not= (was c) (now c)) [c [(was c) (now c)]]))
                                                                          (keys now)))]
                                                  (when (seq diff) [k diff])))))
                                          (keys a)))
                      result (cond-> {}
                               (seq deleted) (assoc :deleted deleted)
                               (seq added) (assoc :added added)
                               (seq changed) (assoc :changed changed))]
                  (when (seq result) [t result])))
              (keys before))))

(defn json-type
  "The JSON type a database holds at a path of a JSON column, read with its
  own function: string, number, boolean, null, object or array. The JSON
  read back as data can't tell a number from a numeric string in every
  engine's driver, so a write test of a value's type asks the database."
  [{:keys [engine] :as db} table-name column-name key-column key-value path]
  (let [mysql-path (apply str "$" (map #(if (integer? %) (str "[" % "]") (str ".\"" % "\"")) path))
        sql (case engine
              :postgres (str "SELECT jsonb_typeof(" column-name "::jsonb #> ?::text[]) AS t FROM " table-name " WHERE " key-column " = ?")
              :mysql (str "SELECT JSON_TYPE(JSON_EXTRACT(" column-name ", ?)) AS t FROM " table-name " WHERE " key-column " = ?")
              :sqlite (str "SELECT json_type(" column-name ", ?) AS t FROM " table-name " WHERE " key-column " = ?"))
        path-param (if (= engine :postgres)
                     (str "{" (s/join "," (map str path)) "}")
                     mysql-path)
        t (:t (first (query db sql path-param key-value)))]
    (case (some-> t str s/lower-case)
      ("integer" "real" "double" "decimal" "unsigned integer") "number"
      ("true" "false") "boolean"
      "text" "string"
      (some-> t str s/lower-case))))
