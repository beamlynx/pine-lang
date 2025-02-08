(ns pine.db.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [pine.db.connections :as connections]
            [pine.db.fixtures :as fixtures]))

(defn- get-foreign-keys
  "Get the foreign keys from the database."
  [config]
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
    (rest (jdbc/query config sql opts))))

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
  [config]
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
    (rest (jdbc/query config sql opts))))

(defn- index-columns [acc columns]
  (reduce (fn [acc [schema table col _pos type _len nullable default]]
            (let [col {:column col :type type :nullable nullable :default default}]
              (-> acc
                  (update-in [:schema schema :table table :columns] conj col)
                  (update-in [:table table :columns] conj col))))
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
  (let [connection (connections/get-connection id)
        columns (get-columns connection)
        foreign-keys (get-foreign-keys connection)]
    [foreign-keys columns]))

(defn get-indexed-references [id]
  (let [references (cond
                     (= id :test) fixtures/references
                     :else (get-references-helper id))]
    (index-references references)))

(defn try-cast-to-uuid [x]
  (try
    (java.util.UUID/fromString x)
    (catch Exception _e x)))

(defn run-query [id query]
  (let [connection (@connections/connections id)
        {:keys [query params]} query
        params (map try-cast-to-uuid params)
        _ (prn (format "Running query: %s" query))
        result (jdbc/query connection (cons query params) {:as-arrays? true :identifiers identity})
        _ (prn "Done!")]
    result))
