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
    (let [join-statements (map (fn [[_from-alias to-alias relation]]
                                 (let [[a1 t1 _ a2 t2] relation
                                       {to-table :table to-schema :schema} (get aliases to-alias)]
                                   (str "JOIN " (q to-schema to-table) " AS " (q to-alias)
                                        " ON " (q a1 t1)
                                        " = " (q a2 t2))))
                               ;; (reverse joins)
                               joins)]
      (s/join " " join-statements))))

(defn- build-columns-clause [{:keys [operation columns current]}]
  (let [type (-> operation :type)
        select-all (cond
                     (contains? #{:select :delete-action} type) ""
                     :else (str (if (seq columns) ", " "") (q current) ".*"))]
    (str
     "SELECT "
     (s/join
      ", "
      (map (fn [{:keys [column alias column-alias symbol]}]
             (let [c (if (empty? column) (str (q alias) "." symbol) (q alias column))]
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
                                    (for [[a col op value] where]
                                      (if (or (= op "IN") (= op "NOT IN"))
                                        (str (q a col) " " op " (" (s/join ", " (repeat (count value) "?"))  ")")
                                        (str (q a col) " " op " " (cond
                                                                    (= (:type value) :symbol) (:value value)
                                                                    (= (:type value) :column) (let [[a col] (:value value)] (q a col))
                                                                    :else "?")))))))
        order (build-order-clause state)
        limit (when limit (str "LIMIT " limit))
        query (s/join " " (filter some? [select from join where-clause order limit]))
        params (when (not-empty where)
                 (->> where
                      (map (fn [[_a _col _op value]] (if (coll? value) value [value])))
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

(defn build-query [state]
  (let [{:keys [type]} (state :operation)]
    (cond
      (= (-> state :current) "x_0") {:query "" :params nil}
      (= type :delete-action) (build-delete-query state)
      (= type :count) (build-count-query state)
      ;; no op
      (= type :delete) {:query " /* No SQL. Evaluate the pine expression for results */ "}
      :else (build-select-query (update state :limit #(or % 250))))))
(defn formatted-query [{:keys [query params]}]
  (let [replacer (fn [s param]
                   (let [v (:value param)
                         param-str (if (= (:type param) :string)
                                     (str "'" v "'")
                                     (str v))]
                     (s/replace-first s "?" param-str)))]
    (if (empty? query) "" (str "\n" (reduce replacer query params) ";\n"))))

(defn run-query [state]
  (if (= (-> state :operation :type) :no-op)
    [["No operation"] ["-"]]
    (let [connection-id (state :connection-id)
          {query :query params :params} (build-query state)]
      (db/run-query connection-id {:query query :params (map #(:value %) params)}))))
