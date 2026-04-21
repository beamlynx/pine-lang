(ns pine.eval
  (:require
   [clojure.string :as s]
   [pine.db.main :as db]))

(defn q
  ([a b]
   (if a (str (q a) "." (q b)) (q b)))
  ([a]
   (str "\"" a "\"")))

(defn- col-fn-format
  "Map column function names to TO_CHAR format strings"
  [col-fn]
  (case col-fn
    "year"   "YYYY"
    "month"  "YYYY-MM"
    "day"    "YYYY-MM-DD"
    "week"   "YYYY-MM-DD"
    "hour"   "YYYY-MM-DD HH24"
    "minute" "YYYY-MM-DD HH24:MI"))

(defn- build-join-clause [{:keys [tables joins aliases]}]
  (when (not-empty (rest tables))
    (let [join-statements (map (fn [[_from-alias to-alias relation join]]
                                 (let [[a1 t1 _ a2 t2] relation
                                       {to-table :table to-schema :schema} (get aliases to-alias)
                                       join-keyword (if join (str join " JOIN") "JOIN")]
                                   (str join-keyword " " (q to-schema to-table) " AS " (q to-alias)
                                        " ON " (q a1 t1)
                                        " = " (q a2 t2))))
                               ;; (reverse joins)
                               joins)]
      (s/join " " join-statements))))

(defn- build-columns-clause [{:keys [operation columns current]}]
  (let [type (-> operation :type)
        ;; Separate auto-ID columns from user-selected columns
        {auto-id-columns true user-columns nil} (group-by #(:auto-id %) columns)
        ;; Check if any non-auto-ID columns are selected for the current table
        current-table-has-columns? (some #(= (:alias %) current) user-columns)
        select-all (cond
                     (contains? #{:select :delete-action :group} type) ""
                     current-table-has-columns? ""  ; Don't add .* if current table has explicit columns
                     :else (str (if (seq columns) ", " "") (q current) ".*"))]
    (str
     "SELECT "
     (s/join
      ", "
      (map (fn [{:keys [column alias column-alias symbol auto-id col-fn]}]
             (let [c (cond
                       ;; Auto-ID columns should render as unquoted id
                       auto-id (str (q alias) ".id")
                       ;; Column function (currently date functions)
                       col-fn (str "TO_CHAR(DATE_TRUNC('" col-fn "', " (q alias column) "), '" (col-fn-format col-fn) "')")
                       ;; Symbol-based columns (like aggregates)
                       (empty? column) (if alias (str (q alias) "." symbol) symbol)
                       ;; Regular columns
                       :else (q alias column))]
               (if column-alias (str c " AS " (q column-alias)) c))) columns))
     select-all
     " FROM")))

(defn- build-order-clause [{:keys [order]}]
  (if (empty? order) nil
      (str
       "ORDER BY "
       (s/join
        ", "
        (map (fn [{:keys [alias column direction]}]
               (str (q alias column) " " direction)) order)))))

(defn- remove-symbols
  "Remove symbols or columns from a vector of values"
  [vs]
  (filter #(not (or (= (:type %) :symbol) (= (:type %) :column))) vs))

(defn- build-group-clause [{:keys [group]}]
  (if (empty? group) nil
      (str
       "GROUP BY "
       (s/join
        ", "
        ;; For each group column, determine the appropriate reference
        (map (fn [{:keys [alias column column-alias col-fn]}]
               (if col-fn
                 ;; Use the column alias for columns with functions applied
                 (q column-alias)
                 ;; Use the full qualified column for regular columns
                 (q alias column)))
             group)))))

(defn build-select-query [state]
  (let [{:keys [tables _columns limit where aliases]} state
        from         (let [{a :alias} (first tables)
                           {table :table schema :schema} (get aliases a)]
                       (str (q schema table) " AS " (q a)))
        join         (build-join-clause state)
        select       (build-columns-clause state)
        where-clause (when (not-empty where)
                       (str "WHERE "
                            (s/join " AND "
                                    (for [[alias col cast operator value] where]
                                      (if (or (= operator "IN") (= operator "NOT IN"))
                                        (str (q alias col) " " operator " (" (s/join ", " (repeat (count value) "?"))  ")")
                                        (str (q alias col) (when cast (str "::" cast)) " " operator " " (cond
                                                                                                          (= (:type value) :symbol) (:value value)
                                                                                                          (= (:type value) :column) (let [[a col] (:value value)] (q a col))
                                                                                                                                                ;; Cast the parameter/value, not the column (unless explicit cast)
                                                                                                          (and (= (:type value) :jsonb) (not cast)) "?::jsonb"
                                                                                                          (and (= (:type value) :uuid) (not cast)) "?::uuid"
                                                                                                          (and (= (:type value) :date) (not cast)) "?::timestamp"
                                                                                                          :else "?")))))))
        group (build-group-clause state)
        order (build-order-clause state)
        limit (when limit (str "LIMIT " limit))
        query (s/join " " (filter some? [select from join where-clause group order limit]))
        params (when (not-empty where)
                 (->> where
                      (map (fn [[_alias _col _cast _operator value]] (if (coll? value) value [value])))
                      remove-symbols
                      flatten))]

    {:query query :params params}))

(defn build-count-query [state]
  (let [{:keys [query params]} (build-select-query state)]
    {:query (str "WITH x AS ( " query " ) SELECT COUNT(*) FROM x")
     :params params}))

(defn- build-inner-select-for-group
  "Build the inner SELECT for a GROUP query CTE. Includes non-aggregate columns only."
  [state]
  (let [{:keys [tables columns where aliases joins]} state
        {a :alias} (first tables)
        {table :table schema :schema} (get aliases a)
        ;; Filter out aggregate function columns (those with :symbol but no :column)
        non-aggregate-cols (filter #(or (:column %) (:auto-id %)) columns)
        ;; Create a temporary state for building the SELECT clause with only non-aggregate columns
        temp-state (assoc state
                          :columns non-aggregate-cols
                          :operation {:type :group})
        ;; Build SELECT clause using the same logic as regular queries, but add aliases to all columns
        select-parts (s/join
                      ", "
                      (map (fn [{:keys [column alias column-alias symbol auto-id col-fn]}]
                             (let [c (cond
                                       ;; Auto-ID columns should render as unquoted id
                                       auto-id (str (q alias) ".id")
                                       ;; Column function (currently date functions)
                                       col-fn (str "TO_CHAR(DATE_TRUNC('" col-fn "', " (q alias column) "), '" (col-fn-format col-fn) "')")
                                       ;; Regular columns
                                       :else (q alias column))
                                   ;; Always use an alias: either column-alias or column name
                                   col-alias (or column-alias column)]
                               (str c " AS " (q col-alias))))
                           non-aggregate-cols))
        select-clause (str "SELECT " select-parts)
        from (str "FROM " (q schema table) " AS " (q a))
        join (build-join-clause {:tables tables :joins joins :aliases aliases})
        where-clause (when (not-empty where)
                       (str "WHERE "
                            (s/join " AND "
                                    (for [[alias col cast operator value] where]
                                      (if (or (= operator "IN") (= operator "NOT IN"))
                                        (str (q alias col) " " operator " (" (s/join ", " (repeat (count value) "?"))  ")")
                                        (str (q alias col) (when cast (str "::" cast)) " " operator " " (cond
                                                                                                          (= (:type value) :symbol) (:value value)
                                                                                                          (= (:type value) :column) (let [[a col] (:value value)] (q a col))
                                                                                                          (and (= (:type value) :jsonb) (not cast)) "?::jsonb"
                                                                                                          (and (= (:type value) :uuid) (not cast)) "?::uuid"
                                                                                                          (and (= (:type value) :date) (not cast)) "?::timestamp"
                                                                                                          :else "?")))))))]
    (s/join " " (filter some? [select-clause from join where-clause]))))

(defn- build-outer-select-for-group
  "Build the outer SELECT for a GROUP query. References CTE columns and includes aggregates."
  [cte-alias {:keys [columns group]}]
  (let [;; Get group columns - use column-alias if present, otherwise column name
        group-cols (map #(or (:column-alias %) (:column %)) group)
        select-items (map (fn [{:keys [column column-alias symbol col-fn]}]
                            (cond
                             ;; Aggregate function (has symbol, no column)
                              (and symbol (empty? column))
                              (if column-alias
                                (str symbol " AS " (q column-alias))
                                symbol)
                             ;; Non-aggregate column - reference from CTE
                             ;; Use the same alias that was assigned in the inner query
                              :else (q cte-alias (or column-alias column))))
                          columns)
        group-by (str "GROUP BY " (s/join ", " (map #(q cte-alias %) group-cols)))]
    {:select (str "SELECT " (s/join ", " select-items) " FROM " (q cte-alias))
     :group-by group-by}))

(defn build-group-query [state]
  (let [{:keys [index]} state
        cte-alias (str "x_" index)
        ;; Build inner query (base SELECT with non-aggregate columns)
        inner-query (build-inner-select-for-group state)
        ;; Build outer query (SELECT from CTE with aggregates and GROUP BY)
        {:keys [select group-by]} (build-outer-select-for-group cte-alias state)
        ;; Combine into CTE
        query (str "WITH " (q cte-alias) " AS ( " inner-query " ) " select " " group-by)
        ;; Extract params from WHERE clause
        params (when (not-empty (:where state))
                 (->> (:where state)
                      (map (fn [[_alias _col _cast _operator value]] (if (coll? value) value [value])))
                      remove-symbols
                      flatten))]
    {:query query :params params}))

(defn build-delete-query [state]
  (let [{:keys [delete current aliases]} state
        {table :table schema :schema}     (get aliases current)
        {:keys [column]}                  delete
        state                             (assoc state :columns [{:column column :alias current}])
        {:keys [query params]}            (build-select-query state)]
    {:query (str "DELETE FROM " (q schema table) " WHERE " (q column) " IN ( "  query " )")
     :params params}))

(defn- build-single-update-query [state update-alias assignments]
  (let [{:keys [aliases]}              state
        {table :table schema :schema}  (get aliases update-alias)
        set-clause (s/join ", "
                           (map (fn [{:keys [column value]}]
                                  (let [{:keys [alias column]} column]
                                    (str (q column) " = " (cond
                                                            (= (:type value) :symbol) (:value value)
                                                            (= (:type value) :column) (let [{:keys [alias column]} value] (q alias column))
                                                            (= (:type value) :jsonb) "?::jsonb"
                                                            (= (:type value) :uuid) "?::uuid"
                                                            (= (:type value) :date) "?::timestamp"
                                                            :else "?"))))
                                assignments))
        state-for-subquery (-> state
                               (assoc :columns [{:column "id" :alias update-alias}])
                               (assoc :operation {:type :select :value nil}))
        {:keys [query params]} (build-select-query state-for-subquery)
        update-params (->> assignments
                           (map :value)
                           (filter #(not (or (= (:type %) :symbol) (= (:type %) :column)))))]
    {:table (if schema (str schema "." table) table)
     :query (str "UPDATE " (q schema table) " SET " set-clause " WHERE id IN ( " query " )")
     :params (concat update-params params)}))

(defn build-update-queries [state]
  "Returns a list of {:table table-name :query query :params params}, one per table being updated."
  (let [{:keys [update current aliases]} state
        {:keys [assignments]}             update
        ;; Group assignments by table alias (use current when column has no alias)
        grouped (group-by (fn [{:keys [column]}]
                            (or (:alias column) current))
                          assignments)]
    (mapv (fn [[update-alias table-assignments]]
            (build-single-update-query state update-alias table-assignments))
          grouped)))

(defn build-query [state]
  (let [{:keys [type]} (state :operation)]
    (cond
      (= (-> state :current) "x_0") {:query "" :params nil}
      (= type :delete-action) (build-delete-query state)
      (= type :update-action) {:queries (build-update-queries state)}
      (= type :update-partial) {:queries (build-update-queries state)}
      (= type :count) (build-count-query state)
      (= type :group) (build-group-query state)
      ;; no op
      (= type :delete) {:query " /* No SQL. Evaluate the pine expression for results */ "}
      :else (build-select-query (update state :limit #(or % 250))))))

(defn formatted-query [build-result]
  (let [replacer (fn [s param]
                   (let [v (:value param)
                         param-str (if (= (:type param) :boolean)
                                     (str v)
                                     (str "'" v "'"))]
                     (clojure.string/replace-first s #"\?" param-str)))]
    (if-let [queries (:queries build-result)]
      ;; Multiple update queries
      (s/join "\n" (map (fn [{:keys [query params]}]
                          (if (empty? query) "" (str (reduce replacer query params) ";")))
                        queries))
      ;; Single query (legacy format or other operations)
      (let [{:keys [query params]} build-result]
        (if (empty? query) "" (str "\n" (reduce replacer query params) ";\n"))))))

(defn run-query [state]
  (if (= (-> state :operation :type) :no-op)
    [["No operation"] ["-"]]
    (let [connection-id (state :connection-id)
          build-result  (build-query state)
          operation-type (-> state :operation :type)]
      (cond
        (contains? #{:update-action :update-partial} operation-type)
        ;; Run update queries; use transaction when multiple tables to rollback all on failure
        (let [queries (or (:queries build-result)
                          [{:table nil :query (:query build-result) :params (:params build-result)}])
              results (if (> (count queries) 1)
                        (db/run-action-queries-in-transaction connection-id queries)
                        (mapv (fn [{:keys [table query params]}]
                                (let [affected (db/run-action-query connection-id {:query query :params params})]
                                  [(or table "table") affected]))
                              queries))]
          (into [["Table" "Rows updated"]]
                (map (fn [[t n]] [t n]) results)))

        (contains? #{:delete-action} operation-type)
        (let [{:keys [query params]} build-result
              affected-rows (db/run-action-query connection-id {:query query :params params})]
          [["Rows deleted"] [affected-rows]])

        :else
        ;; Select and other operations
        (db/run-query connection-id (select-keys build-result [:query :params]))))))
