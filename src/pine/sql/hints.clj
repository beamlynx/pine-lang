(ns pine.sql.hints
  "What the SQL editor completes from: the connection's tables and columns.
  See docs/sql-hints.md.")

(defn default-schema
  "The schema an unqualified table name means: `public` in Postgres. MySQL
  and SQLite have one schema per connection (the database, and `main`)."
  [dialect references]
  (if (= :postgres dialect)
    "public"
    (first (sort (keys (:schema references))))))

(defn schema
  "Every table with its columns in order, by schema and name."
  [dialect references]
  {:dialect (name dialect)
   :default-schema (default-schema dialect references)
   :tables (vec (for [[schema-name {:keys [table]}] (sort-by key (:schema references))
                      [table-name {:keys [columns]}] (sort-by key table)]
                  {:schema schema-name
                   :table table-name
                   :columns (mapv :column columns)}))})
