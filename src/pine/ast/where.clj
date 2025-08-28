(ns pine.ast.where
  (:require [pine.data-types :as dt]))

(defn- convert-condition-value
  "Convert a condition value to the appropriate database type based on the column's schema"
  [value alias col state]
  (let [table-info (get (:aliases state) alias)
        db-type (dt/get-column-type (:references state) alias col table-info)]
    (if db-type
      (if (and (coll? value) (not (map? value)))
        ;; For collections (like IN operator values), convert each item
        ;; Check for (not (map? value)) to avoid treating data type maps as collections
        (map #(dt/convert-value-to-db-type % db-type) value)
        ;; For single values, convert directly
        (dt/convert-value-to-db-type value db-type))
      value)))

(defn handle [state [column operator value]]
  (let [a (state :current)
        [alias col cast] (:value column)
        alias (or alias a)
        converted-value (if (and (not= (:type value) :symbol) (not= (:type value) :column))
                          (convert-condition-value value alias col state)
                          value)]
    (update state :where conj [alias col cast operator converted-value])))

(defn handle-partial [state {:keys [complete-conditions partial-condition]}]
  ;; For WHERE-PARTIAL, we only store the complete conditions in :where
  ;; The partial condition is used for hints, not for query generation
  (let [a (state :current)]
    (reduce (fn [s condition]
              (let [[column operator value] (:value condition)
                    [alias col cast] (:value column)
                    alias (or alias a)
                    converted-value (if (and (not= (:type value) :symbol) (not= (:type value) :column))
                                      (convert-condition-value value alias col s)
                                      value)]
                (update s :where conj [alias col cast operator converted-value])))
            state
            complete-conditions)))
