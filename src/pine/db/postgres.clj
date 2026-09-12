(ns pine.db.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [pine.db.connections :as connections]
            [pine.db.exec :as exec]))

(defn- get-foreign-keys
  "Get the foreign keys from the database."
  [pool]
  (prn (format "Loading all references..."))
  (let [opts {:as-arrays? true}
        sql "SELECT
  n.nspname AS table_schema,
  c.relname AS table_name,
  a.attname AS column_name,
  fn.nspname AS foreign_table_schema,
  f.relname AS foreign_table_name,
  fa.attname AS foreign_column_name
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = c.oid
JOIN pg_class f ON f.oid = con.confrelid
JOIN pg_namespace fn ON fn.oid = f.relnamespace
JOIN pg_attribute fa ON fa.attnum = ANY(con.confkey) AND fa.attrelid = f.oid
WHERE con.contype = 'f'
"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn- get-columns
  "Get the columns for all tables"
  [pool]
  (prn (format "Loading all columns..."))
  (let [opts {:as-arrays? true}
        sql "SELECT
  table_schema,
  table_name,
  column_name,
  ordinal_position,
  data_type,
  character_maximum_length,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name, ordinal_position"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn get-references-helper
  "Return [foreign-keys columns] for a live Postgres connection."
  [id]
  (let [pool (connections/get-connection-pool id)
        columns (get-columns pool)
        foreign-keys (get-foreign-keys pool)]
    [foreign-keys columns]))

(def connection-count-sql "SELECT COUNT(*) as connection_count FROM pg_stat_activity")

(defn convert-param
  "Convert parameter values to appropriate types for PostgreSQL"
  [param]
  (let [v (:value param)]
    (case (:type param)
      :uuid (try
              (java.util.UUID/fromString v)
              (catch Exception _e v))
      :jsonb v  ; Let PostgreSQL handle JSON parsing from string
      :boolean v
      v)))

(defn run-query [id query]
  (exec/run-query id query convert-param))

(defn run-action-query [id query]
  (exec/run-action-query id query convert-param))

(defn run-action-queries-in-transaction [id queries]
  (exec/run-action-queries-in-transaction id queries convert-param))

(defn run-sql [id sql-query]
  (exec/run-sql id sql-query))
