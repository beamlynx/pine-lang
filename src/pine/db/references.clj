(ns pine.db.references
  "Dialect-agnostic schema indexing: turns [foreign-keys columns] tuples (in
  the shared ANSI information_schema shape both postgres.clj and mysql.clj
  query into) into the :table/:schema index the rest of pine reads from.
  Nothing here knows which database produced the tuples.")

(defn- relation
  "One relation between two tables, as every part of pine reads it.

  Written ONCE per foreign key and then indexed under both directions,
  rather than written out twice with the two sides swapped. Which side is
  the child and which is the parent is a property of the relation itself,
  so it lives in the value; which direction a caller is travelling in is a
  property of the lookup, so it stays in the index path
  ([:table X :referred-by Y] vs [:table X :refers-to Y]) and every caller
  already knows which of the two it asked for.

  :columns is a list of pairs, one per column of the key, each labelled by
  the side that owns it. Today it always holds exactly one pair -- the
  extraction queries hand over one column pair at a time -- but nothing
  downstream may assume that. A foreign key made of several columns is
  simply a longer list, and reaching that point is meant to be a change
  here and nowhere else."
  [resolution child-schema child-table parent-schema parent-table pairs]
  {:child      {:schema child-schema  :table child-table}
   :parent     {:schema parent-schema :table parent-table}
   :columns    pairs
   :resolution resolution})

(defn- index-relation
  "File one relation under every way of reaching it: by bare table name in
  both directions (a list, since the same table name can exist in several
  schemas) and by schema-qualified name in both directions (a single
  entry). `col` is the key it is filed under -- the child's own column,
  which is what a user names in a `.hint_col`."
  [acc {{child-schema :schema child :table} :child
        {parent-schema :schema parent :table} :parent
        :as rel}
   col]
  (-> acc
      ;; Case: Ambiguity / Schema not specified
      ;;
      ;; Relations between tables (in case of ambiguity)
      ;; - Value is multiple relations
      ;; - Even if the column to join on is not known,
      ;;   we get a list of relations to choose from.
      ;;
      ;; This shouldn't be needed as we should be able to
      ;; figure out which schema is being used and that value can be
      ;; stored in the context. For now, this is convenient. For
      ;; consider the 'No ambiguity' approach below
      ;;
      (update-in [:table parent :referred-by child :via col] conj rel)
      (update-in [:table child  :refers-to parent :via col] conj rel)
      ;;
      ;; Case: No Ambiguity / Schema specified
      ;;
      ;; - Value is a single relation
      ;;
      (assoc-in [:table parent :in parent-schema :referred-by child :in child-schema :via col] rel)
      (assoc-in [:table child  :in child-schema  :refers-to parent :in parent-schema :via col] rel)))

(defn- index-foreign-keys [foreign-keys]
  (reduce (fn [acc [schema table col f-schema f-table f-col]]
            (index-relation acc
                            (relation :foreign-key schema table f-schema f-table
                                      [{:child col :parent f-col}])
                            col))
          {}
          foreign-keys))

(defn- index-columns
  "Index columns per schema+table and per bare table name. :columns is kept
  in the order given (ordinal position, per get-columns' ORDER BY) - the
  access policy's `.*` expansion relies on this to lay out redacted columns
  in the same order a real `SELECT *` would. Plain `conj` here would
  prepend (the accumulator starts from nil, and `(conj nil x)` builds a
  list, not a vector), silently reversing that order - `(fnil conj [])`
  seeds a vector instead, so conj appends."
  [acc columns]
  (reduce (fn [acc [schema table col _pos type _len nullable default]]
            (let [col {:column col :type type :nullable nullable :default default}]
              (-> acc
                  (update-in [:schema schema :table table :columns] (fnil conj []) col)
                  (update-in [:schema schema :table table :column-set] (fnil conj #{}) col)
                  (update-in [:table table :columns] (fnil conj []) col)
                  (update-in [:table table :column-set] (fnil conj #{}) col))))
          acc
          columns))

;; ---------------------------------------------------------------------------
;; Heuristic Relation Detection
;; ---------------------------------------------------------------------------

(defn- extract-table-from-column
  "Returns potential table name from column, or nil.
   'tenant_id' -> 'tenant', 'tenantId' -> 'tenant', 'foo' -> nil"
  [col-name]
  (let [col-lower (clojure.string/lower-case col-name)]
    (cond
      ;; Snake case: tenant_id -> tenant
      (clojure.string/ends-with? col-lower "_id")
      (subs col-name 0 (- (count col-name) 3))

      ;; Camel case: tenantId -> tenant (look for uppercase I followed by d)
      (re-find #"[a-z]Id$" col-name)
      (subs col-name 0 (- (count col-name) 2))

      :else nil)))

(defn- normalize-plural
  "Returns set of normalized forms: #{singular plural}
   'tenant' -> #{'tenant' 'tenants'}
   'companies' -> #{'company' 'companies'}"
  [name]
  (let [lower-name (clojure.string/lower-case name)]
    (cond
      ;; Already plural ending in 'ies' -> singular ends in 'y'
      (clojure.string/ends-with? lower-name "ies")
      #{lower-name (str (subs lower-name 0 (- (count lower-name) 3)) "y")}

      ;; Already plural ending in 's' -> try removing it
      (clojure.string/ends-with? lower-name "s")
      #{lower-name (subs lower-name 0 (- (count lower-name) 1))}

      ;; Singular ending in 'y' -> plural ends in 'ies'
      (clojure.string/ends-with? lower-name "y")
      #{lower-name (str (subs lower-name 0 (- (count lower-name) 1)) "ies")}

      ;; Default: add 's' for plural
      :else #{lower-name (str lower-name "s")})))

(defn- table-has-id-column?
  "Check if table has 'id' column in indexed structure"
  [acc table]
  (some #(= "id" (:column %)) (get-in acc [:table table :columns])))

(defn- relation-exists?
  "Check if relation already exists (from FK indexing)"
  [acc target-table source-table col]
  (get-in acc [:table target-table :referred-by source-table :via col]))

(defn- build-table-lookup
  "Build lookup map from columns: {lowercase-table-name -> #{[schema table] ...}}"
  [columns]
  (reduce (fn [acc [schema table & _]]
            (update acc (clojure.string/lower-case table) (fnil conj #{}) [schema table]))
          {}
          columns))

(defn- find-matching-tables
  "Find tables that match any of the normalized name forms"
  [table-lookup name-forms]
  (mapcat #(get table-lookup %) name-forms))

(defn- add-heuristic-relation
  "Add a heuristic relation to the accumulator. A naming convention names
  one column at a time, so a heuristic relation always has exactly one
  column pair - unlike a foreign key, which can have several."
  [acc schema table col f-schema f-table]
  (index-relation acc
                  (relation :heuristic schema table f-schema f-table
                            [{:child col :parent "id"}])
                  col))

(defn- index-heuristic-relations
  "Detect relations heuristically based on column naming conventions.
   Runs after index-columns so we can check if target tables have 'id' column."
  [acc columns]
  (let [table-lookup (build-table-lookup columns)]
    (reduce
     (fn [acc [schema table col & _]]
       (if-let [extracted (extract-table-from-column col)]
         (let [name-forms (normalize-plural extracted)
               matching-tables (find-matching-tables table-lookup name-forms)]
           (reduce
            (fn [acc [f-schema f-table]]
              (if (and (table-has-id-column? acc f-table)
                       (not (relation-exists? acc f-table table col))
                       ;; Don't create self-referential heuristic relations
                       (not (and (= table f-table) (= schema f-schema))))
                (add-heuristic-relation acc schema table col f-schema f-table)
                acc))
            acc
            matching-tables))
         acc))
     acc
     columns)))

(defn index-references
  "Finding forward and inverse relations for the table Example: A 'user' has
  'document' i.e. the document has a `user_id` column that points to
  `user`.`id`. Alternatively, 'document' of 'user'. When we find a foreign key,
  then we index the same relation under both directions i.e. `:referred-by`
  (reached from the parent) and `:refers-to` (reached from the child).

  Heuristic relations are also detected based on column naming conventions
  (e.g., tenant_id -> tenant table) for tables without explicit foreign keys."
  [[foreign-keys columns]]

  ;; Index foreign keys first, then columns (so we have column data),
  ;; then detect heuristic relations (which need both FK data and column data)
  (->
   (index-foreign-keys foreign-keys)
   (index-columns columns)
   (index-heuristic-relations columns)))
