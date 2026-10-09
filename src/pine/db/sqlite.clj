(ns pine.db.sqlite
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [pine.db.connections :as connections]
            [pine.db.exec :as exec]))

(def ^:private schema-name
  "A SQLite file has no schemas of its own; its one database is called
  `main`. It goes in the same :schema slot MySQL's database name does, so
  the reference model needs no special case."
  "main")

(defn- affinity-type
  "SQLite doesn't enforce column types: the declared type is free text
  (`VARCHAR(255)`, `UNSIGNED BIG INT`, or nothing at all) that only picks an
  affinity. Reduce it to the plain lowercase names the rest of pine already
  matches on (data_types.clj, access policies), the way SQLite itself would:
  the names pine knows pass straight through, everything else follows
  SQLite's affinity rules in order - INT, then CHAR/CLOB/TEXT, then BLOB,
  then REAL/FLOA/DOUB, otherwise NUMERIC. No declared type is nil: unknown,
  not a type called \"\", so pine adds no conversion or cast on its account."
  [declared]
  (let [t (-> (or declared "")
              s/lower-case
              (s/replace #"\(.*$" "")
              s/trim)]
    (cond
      (s/blank? t) nil
      (contains? #{"boolean" "bool" "date" "datetime" "timestamp" "json" "uuid" "time"} t) t
      (s/includes? t "int") "integer"
      (some #(s/includes? t %) ["char" "clob" "text"]) "text"
      (s/includes? t "blob") "blob"
      (some #(s/includes? t %) ["real" "floa" "doub"]) "real"
      :else "numeric")))

(defn- get-foreign-keys
  "Get the foreign keys from the database.

  pragma_foreign_key_list returns one row per column of a constraint, with
  the constraint's number (`id`) and the column's position in it (`seq`).
  SQLite names no constraints, so one is made from the table and `id`;
  db/references.clj groups the rows back into one relation per constraint,
  so a composite key becomes a single join.

  A key may leave out the column it points at (`REFERENCES parent`), meaning
  the parent's primary key: `to` is then NULL, and the parent's PK column at
  the same position is used. The parent table's name is taken from the
  schema, not the constraint, because SQLite matches names without regard
  to case."
  [pool]
  (prn (format "Loading all references..."))
  (let [opts {:as-arrays? true}
        sql "SELECT 'main' AS table_schema,
       m.name AS table_name,
       f.\"from\" AS column_name,
       'main' AS referenced_table_schema,
       COALESCE(t.name, f.\"table\") AS referenced_table_name,
       COALESCE(f.\"to\", pk.name) AS referenced_column_name,
       'fk_' || m.name || '_' || f.id AS constraint_name,
       f.seq + 1 AS ordinal_position
FROM sqlite_schema m
JOIN pragma_foreign_key_list(m.name) f
LEFT JOIN sqlite_schema t ON t.type = 'table' AND lower(t.name) = lower(f.\"table\")
LEFT JOIN pragma_table_info(COALESCE(t.name, f.\"table\")) pk ON f.\"to\" IS NULL AND pk.pk = f.seq + 1
WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
ORDER BY m.name, f.id, f.seq"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn- get-columns
  "Get the columns for all tables and views, in the same shape as Postgres's
  and MySQL's queries, so references.clj consumes any of them. SQLite's own
  tables (`sqlite_%`) are skipped. The length is always NULL: a declared
  `VARCHAR(255)` is not enforced, so there is nothing to report."
  [pool]
  (prn (format "Loading all columns..."))
  (let [opts {:as-arrays? true}
        sql "SELECT 'main' AS table_schema,
       m.name AS table_name,
       p.name AS column_name,
       p.cid + 1 AS ordinal_position,
       p.type AS data_type,
       NULL AS character_maximum_length,
       CASE WHEN p.\"notnull\" = 1 THEN 'NO' ELSE 'YES' END AS is_nullable,
       p.dflt_value AS column_default
FROM sqlite_schema m
JOIN pragma_table_info(m.name) p
WHERE m.type IN ('table', 'view') AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
ORDER BY m.name, p.cid"]
    (with-open [conn (.getConnection pool)]
      (let [[_header & rows] (jdbc/query {:connection conn} sql opts)]
        (mapv #(update (vec %) 4 affinity-type) rows)))))

(defn get-references-helper
  "Return [foreign-keys columns] for a live SQLite connection."
  [id]
  (let [pool (connections/get-connection-pool id)
        columns (get-columns pool)
        foreign-keys (get-foreign-keys pool)]
    [foreign-keys columns]))

(def connection-count-sql
  "A file database has no server and so no connections to count. One is what
  this process holds."
  "SELECT 1 AS connection_count")

(defn convert-param
  "Pass the value through, except a date: that is bound as the text the user
  wrote. A SQLite date is text, so `created_at > '2024-01-31'` has to compare
  text with text; the parsed java.sql.Date would be written by the driver as
  `2024-01-31 00:00:00`, which never equals a stored `2024-01-31`. A `T`
  between date and time becomes the space SQLite's own date functions
  produce."
  [param]
  (if (and (= :date (:type param)) (:text param))
    (s/replace-first (:text param) "T" " ")
    (:value param)))

(defn run-query [id query]
  (exec/run-query id query convert-param))

(defn run-action-query [id query]
  (exec/run-action-query id query convert-param))

(defn run-action-queries-in-transaction [id queries]
  (exec/run-action-queries-in-transaction id queries convert-param))

(defn run-sql [id sql-query]
  (exec/run-sql id sql-query))
