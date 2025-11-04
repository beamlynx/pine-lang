(ns pine.eval
  (:require
   [clojure.string :as s]
   [pine.db.main :as db]))

(defn q
  ([a b]
   (if a (str (q a) "." (q b)) (q b)))
  ([a]
   (str "\"" a "\"")))

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
                       col-fn (let [cast (if (contains? #{"hour" "minute"} col-fn)
                                           "::timestamp"  ; hour/minute need timestamp
                                           "::date")]     ; year/month/day can be date
                                (str "DATE_TRUNC('" col-fn "', " (q alias column) ")" cast))
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

(defn build-delete-query [state]
  (let [{:keys [delete current aliases]} state
        {table :table schema :schema}     (get aliases current)
        {:keys [column]}                  delete
        state                             (assoc state :columns [{:column column :alias current}])
        {:keys [query params]}            (build-select-query state)]
    {:query (str "DELETE FROM " (q schema table) " WHERE " (q column) " IN ( "  query " )")
     :params params}))

(defn build-update-query [state]
  (let [{:keys [update current aliases]} state
        {table :table schema :schema}     (get aliases current)
        {:keys [assignments]}             update
        ;; Build the SET clause
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
        ;; Create a modified state for the subquery that only selects id
        ;; and has the operation type set to :select to avoid adding .*
        state-for-subquery (-> state
                               (assoc :columns [{:column "id" :alias current}])
                               (assoc :operation {:type :select :value nil}))
        {:keys [query params]} (build-select-query state-for-subquery)
        ;; Extract parameters from update assignments
        update-params (->> assignments
                           (map :value)
                           (filter #(not (or (= (:type %) :symbol) (= (:type %) :column)))))]
    {:query (str "UPDATE " (q schema table) " SET " set-clause " WHERE id IN ( " query " )")
     :params (concat update-params params)}))

(defn build-query [state]
  (let [{:keys [type]} (state :operation)]
    (cond
      (= (-> state :current) "x_0") {:query "" :params nil}
      (= type :delete-action) (build-delete-query state)
      (= type :update-action) (build-update-query state)
      (= type :count) (build-count-query state)
      (= type :group) (build-select-query state)
      ;; no op
      (= type :delete) {:query " /* No SQL. Evaluate the pine expression for results */ "}
      :else (build-select-query (update state :limit #(or % 250))))))

(defn formatted-query [{:keys [query params]}]
  (let [replacer (fn [s param]
                   (let [v (:value param)
                         param-str (if (= (:type param) :boolean)
                                     (str v)
                                     (str "'" v "'"))]
                     (clojure.string/replace-first s #"\?" param-str)))]
    (if (empty? query) "" (str "\n" (reduce replacer query params) ";\n"))))

(defn run-query [state]
  (if (= (-> state :operation :type) :no-op)
    [["No operation"] ["-"]]
    (let [connection-id (state :connection-id)
          {query :query params :params} (build-query state)
          operation-type (-> state :operation :type)]
      (if (contains? #{:update-action :delete-action} operation-type)
        ;; For action operations, return the number of affected rows
        (let [affected-rows (db/run-action-query connection-id {:query query :params params})]
          [[(case operation-type
              :update-action "Rows updated"
              :delete-action "Rows deleted")]
           [affected-rows]])
        ;; For select operations, return the result set
        (db/run-query connection-id {:query query :params params})))))
