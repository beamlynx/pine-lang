(ns pine.db.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [pine.db.connections :as connections]
            [pine.db.fixtures :as fixtures]))

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

(defn- index-foreign-keys [foreign-keys]
  (reduce (fn [acc [schema table col f-schema f-table f-col]]
            (let [has [f-schema f-table f-col :referred-by   schema   table   col]
                  of  [schema     table   col :refers-to   f-schema f-table f-col]]
              (-> acc
                  ;; Case: Ambiguity / Schema not specified
                  ;;
                  ;; Relations between tables (in case of ambiguity)
                  ;; - Value is multiple join vectors
                  ;; - Even if the column to join on is not known,
                  ;;   we get a list of join vectors to choose from.
                  ;;
                  ;; This shouldn't be needed as we should be able to
                  ;; figure out which schema is being used and that value can be
                  ;; stored in the context. For now, this is convenient. For
                  ;; consider the 'No ambiguity' approach below
                  ;;
                  (update-in [:table  f-table :referred-by table :via col] conj has)
                  (update-in [:table  table   :refers-to f-table :via col] conj of)
                  ;;
                  ;; Case: No Ambiguity / Schema specified
                  ;;
                  ;; - Value is a single a join vector
                  ;;
                  (assoc-in [:table  f-table  :in  f-schema :referred-by table :in schema :via col] has)
                  (assoc-in [:table  table    :in  schema   :refers-to f-table :in f-schema :via col] of)
                  ;;
                  ;;
                  ;; Metadata
                  ;;
                  ;; (assoc-in [:schema f-schema :contains f-table :columns] {:columns nil})
                  ;; (assoc-in [:schema schema   :contains table :columns] {:columns nil})
                  ;; ;; ;; For the ambiguous case
                  ;; (update-in [:table f-table :columns] {})
                  ;; (update-in [:table table :columns] {})
                  )))
          {}
          foreign-keys))

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
FROM information_schema.columns"]
    (with-open [conn (.getConnection pool)]
      (rest (jdbc/query {:connection conn} sql opts)))))

(defn- index-columns [acc columns]
  (reduce (fn [acc [schema table col _pos type _len nullable default]]
            (let [col {:column col :type type :nullable nullable :default default}]
              (-> acc
                  (update-in [:schema schema :table table :columns] conj col)
                  (update-in [:schema schema :table table :column-set] (fnil conj #{}) col)
                  (update-in [:table table :columns] conj col)
                  (update-in [:table table :column-set] (fnil conj #{}) col))))
          acc
          columns))

(defn- index-references
  "Finding forward and inverse relations for the table Example: A 'user' has
  'document' i.e. the document has a `user_id` column that points to
  `user`.`id`. Alternatively, 'document' of 'user'. When we find a foreign key,
  then we index create both forward and inverse relations i.e. `:has` and `:of`
  relations / or `:refered-by` and `:refers-to` relations."
  [[foreign-keys columns]]

  ;; Index the foreign keys
  (->
   (index-foreign-keys foreign-keys)
   (index-columns columns)))

(defn get-references-helper
  "Return the foreign keys. TODO: also return the columns."
  [id]
  (let [pool (connections/get-connection-pool id)
        columns (get-columns pool)
        foreign-keys (get-foreign-keys pool)]
    [foreign-keys columns]))

(defn get-indexed-references [id]
  (let [references (cond
                     (= id :test) fixtures/references
                     :else (get-references-helper id))]
    (index-references references)))

(defn convert-param-for-postgres
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
  (let [pool (connections/get-connection-pool id)
        {:keys [query params]} query
        params (map convert-param-for-postgres params)
        _ (prn (format "Running query: %s" query))
        result (with-open [conn (.getConnection pool)]
                 (jdbc/query {:connection conn} (cons query params) {:as-arrays? true :identifiers identity}))
        _ (prn "Done!")]
    result))

(defn run-action-query [id query]
  (let [pool (connections/get-connection-pool id)
        {:keys [query params]} query
        params (map convert-param-for-postgres params)
        _ (prn (format "Running action: %s" query))
        result (with-open [conn (.getConnection pool)]
                 (jdbc/execute! {:connection conn} (cons query params)))
        affected-rows (first result)
        _ (prn (format "Affected rows: %d" affected-rows))]
    affected-rows))

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
        _ (prn (format "Running raw SQL: %s" sql-query))
        result (with-open [conn (.getConnection pool)]
                 (if is-select?
                   (jdbc/query {:connection conn} sql-query {:as-arrays? true :identifiers identity})
                   (jdbc/execute! {:connection conn} sql-query)))
        _ (prn "Done!")]
    (if is-select?
      result
      ;; Return array format for action queries to match expected structure
      [["Rows affected"] [(first result)]])))