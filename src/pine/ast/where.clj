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

(defn- make-resolve-alias [state]
  #(if (contains? (:aliases state) %) % (or (get-in state [:pending-assignments % :current]) %)))

(defn- resolve-condition
  "Turn one parsed [column operator value] triple into the flat 5-tuple
  [alias col cast operator converted-value] stored in state's :where."
  [state current resolve-alias [column operator value]]
  (let [[alias col cast] (:value column)
        alias (resolve-alias (or alias current))
        converted-value (if (and (not= (:type value) :symbol) (not= (:type value) :column))
                          (convert-condition-value value alias col state)
                          value)]
    [alias col cast operator converted-value]))

(defn handle [state value]
  (let [current (state :current)
        resolve-alias (make-resolve-alias state)]
    (if-let [conditions (:or value)]
      ;; Comma-separated conditions inside one where: segment combine with OR,
      ;; stored as a single group so the evaluator can tell them apart from the
      ;; AND-ed entries produced by separate where: pipe-steps.
      (update state :where conj {:or (mapv #(resolve-condition state current resolve-alias %) conditions)})
      (update state :where conj (resolve-condition state current resolve-alias value)))))

(defn handle-partial [state {:keys [complete-conditions partial-condition]}]
  ;; For WHERE-PARTIAL, we only store the complete conditions in :where
  ;; The partial condition is used for hints, not for query generation
  (let [current (state :current)
        resolve-alias (make-resolve-alias state)]
    (reduce (fn [s condition]
              (update s :where conj (resolve-condition s current resolve-alias (:value condition))))
            state
            complete-conditions)))
