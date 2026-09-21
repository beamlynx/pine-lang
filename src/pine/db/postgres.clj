(ns pine.db.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [pine.db.connections :as connections]
            [pine.db.exec :as exec]))

(defn- get-foreign-keys
  "Get the foreign keys from the database.

  A constraint's local columns live in `con.conkey` and the columns they
  point at in `con.confkey`, as two arrays that line up position by
  position. They have to be paired by position, which is what the two
  `unnest(...) WITH ORDINALITY` joins below do.

  Matching each array with `= ANY(...)` instead - as this query used to -
  pairs every local column with every foreign column. A single-column
  constraint survives that (one times one is still one row), but a
  two-column one turns into four rows: the two real pairs, plus two
  inventions. Those inventions are indistinguishable from real foreign
  keys downstream, so pine offered them as joins and built SQL comparing
  columns that were never meant to be compared - e.g. a `uuid` id against
  a `varchar` reference, which Postgres rejects with \"operator does not
  exist: uuid = character varying\".

  Each column pair is returned as its own row, alongside the constraint
  it belongs to (`conname`) and its position within that constraint
  (`k.ord`, the ordinality the pairing above already produces).
  db/references.clj groups the rows back into one relation per
  constraint, so a composite key becomes a single join on all of its
  columns."
  [pool]
  (prn (format "Loading all references..."))
  (let [opts {:as-arrays? true}
        sql "SELECT
  n.nspname AS table_schema,
  c.relname AS table_name,
  a.attname AS column_name,
  fn.nspname AS foreign_table_schema,
  f.relname AS foreign_table_name,
  fa.attname AS foreign_column_name,
  con.conname AS constraint_name,
  k.ord AS ordinal_position
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class f ON f.oid = con.confrelid
JOIN pg_namespace fn ON fn.oid = f.relnamespace
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ord) ON fk.ord = k.ord
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
JOIN pg_attribute fa ON fa.attrelid = f.oid AND fa.attnum = fk.attnum
WHERE con.contype = 'f'
"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn- get-columns
  "Get the columns for all tables.

  Queries pg_catalog directly (pg_attribute/pg_class/pg_namespace/pg_type)
  rather than information_schema.columns, which Postgres filters to only the
  columns a role has some privilege on (owner, or any of SELECT/INSERT/
  UPDATE/DELETE/REFERENCES/TRIGGER) -- a role with no grants at all saw no
  columns, so no tables ever reached hints, even though get-foreign-keys
  above already used pg_catalog and so already saw (and hinted) relations to
  those same invisible tables. pg_catalog's own tables carry no such
  filter -- like get-foreign-keys, this is metadata every role can read
  regardless of grants; it never touches row data, so running an actual
  query against an ungranted table is unaffected and still fails as normal.

  data_type is pg_type.typname (Postgres's internal short name: e.g. int4,
  bool, timestamptz, bpchar) rather than information_schema's ANSI display
  name (integer, boolean, timestamp with time zone, character) --
  convert-value-to-db-type (data_types.clj) already matches both spellings
  side by side for every case this mattered for."
  [pool]
  (prn (format "Loading all columns..."))
  (let [opts {:as-arrays? true}
        sql "SELECT
  n.nspname AS table_schema,
  c.relname AS table_name,
  a.attname AS column_name,
  a.attnum AS ordinal_position,
  t.typname AS data_type,
  (CASE WHEN t.typname IN ('varchar', 'bpchar') AND a.atttypmod > 4
        THEN a.atttypmod - 4 END) AS character_maximum_length,
  (CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END) AS is_nullable,
  pg_get_expr(ad.adbin, ad.adrelid) AS column_default
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
WHERE a.attnum > 0
  AND NOT a.attisdropped
  AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname, a.attnum"]
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
