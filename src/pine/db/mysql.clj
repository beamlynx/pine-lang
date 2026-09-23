(ns pine.db.mysql
  (:require [clojure.java.jdbc :as jdbc]
            [pine.db.connections :as connections]
            [pine.db.exec :as exec]))

(defn- get-foreign-keys
  "Get the foreign keys from the database. MySQL's information_schema is
  server-global (unlike Postgres, where a connection is already scoped to
  one database), so both sides are pinned to DATABASE() - this maps MySQL's
  database-as-schema into the existing :schema slot in the reference model
  and keeps tables in other databases on the same server invisible.

  constraint_name/ordinal_position come along so db/references.clj can
  group a composite key's column pairs back into one relation. MySQL
  returns one correctly paired row per column already, so the pairing
  itself needs no work here - only the constraint they belong to."
  [pool]
  (prn (format "Loading all references..."))
  (let [opts {:as-arrays? true}
        sql "SELECT kcu.table_schema, kcu.table_name, kcu.column_name,
       kcu.referenced_table_schema, kcu.referenced_table_name, kcu.referenced_column_name,
       kcu.constraint_name, kcu.ordinal_position
FROM information_schema.key_column_usage kcu
WHERE kcu.referenced_table_name IS NOT NULL
  AND kcu.table_schema = DATABASE()
  AND kcu.referenced_table_schema = DATABASE()
ORDER BY kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn- get-columns
  "Get the columns for all tables. Column names/order match Postgres's
  query exactly (both are ANSI information_schema), so references.clj
  needs no changes to consume either."
  [pool]
  (prn (format "Loading all columns..."))
  (let [opts {:as-arrays? true}
        sql "SELECT table_schema, table_name, column_name, ordinal_position,
       data_type, character_maximum_length, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
ORDER BY table_schema, table_name, ordinal_position"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn get-references-helper
  "Return [foreign-keys columns] for a live MySQL connection."
  [id]
  (let [pool (connections/get-connection-pool id)
        columns (get-columns pool)
        foreign-keys (get-foreign-keys pool)]
    [foreign-keys columns]))

(def connection-count-sql
  "Degrades gracefully to \"just my own connections\" without PROCESS
  privilege, rather than erroring."
  "SELECT COUNT(*) AS connection_count FROM information_schema.processlist")

(defn convert-param
  "Convert parameter values to appropriate types for MySQL. Much simpler
  than Postgres's: MySQL has no native UUID type, so :uuid values just pass
  through as strings."
  [param]
  (:value param))

(defn run-query [id query]
  (exec/run-query id query convert-param))

(defn run-action-query [id query]
  (exec/run-action-query id query convert-param))

(defn run-action-queries-in-transaction [id queries]
  (exec/run-action-queries-in-transaction id queries convert-param))

(defn run-sql [id sql-query]
  (exec/run-sql id sql-query))
