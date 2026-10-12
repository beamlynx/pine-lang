(ns pine.ast.limit
  "`limit: N` caps the rows. `limit: N per <columns>` keeps the first N rows
  of each group of rows that share those columns. See docs/limit-per.md."
  (:require [pine.ast.hints :as hints]
            [pine.ast.path :as path]
            [pine.db.references :as refs]))

(defn- key-order
  "What ranks the rows of each group when no order: came first, so the same
  rows are picked every time: the primary key of each table in the query,
  the current table's first, ascending. A named result, or a table without
  a primary key, adds nothing. When nothing is left, the database picks."
  [{:keys [references aliases current tables]}]
  (let [table-aliases (distinct (cons current (map :alias tables)))]
    (vec (for [a table-aliases
               :let [{:keys [table schema ast]} (get aliases a)]
               :when (not ast)
               column (refs/primary-key references schema table)]
           {:alias a :column column :direction "ASC"}))))

(defn- star-columns
  "{alias [column-name ...]}: what `alias.*` returns, for every table in the
  state. The SQL names the columns of a per-group result one by one, to
  leave its row numbers out, and the schema is gone by the time the SQL is
  built. A table missing from the schema index has an empty list."
  [state]
  (into {}
        (for [{:keys [alias]} (:tables state)]
          [alias (vec (distinct (map :column (hints/generate-all-column-hints state alias))))])))

(defn handle
  "`per` is the columns after `per`, as the parser gives them: empty or nil
  for a plain limit."
  ([state value] (handle state value nil))
  ([state value per]
   (if (seq per)
     (let [i (:index state)
           columns (mapv #(assoc (path/resolve-column state %) :operation-index i) per)]
       (assoc state :limit-per {:n value
                                :columns columns
                                ;; Ranks the rows inside each group: the order:
                                ;; steps so far, or the primary keys.
                                :order (if (seq (:order state)) (:order state) (key-order state))
                                :star-columns (star-columns state)}))
     (assoc state :limit value))))
