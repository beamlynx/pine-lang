(ns pine.ast.group
  (:require
   [clojure.string :as s]))

(defn handle [state value]
  (let [i (state :index)
        current (state :current)
        ;; Process group columns with defaults
        group-columns (map #(-> %1
                                (assoc :alias (or (:alias %1) current))
                                (assoc :operation-index i))
                           (:columns value))
        ;; Create aggregate function symbols
        fn-columns (map (fn [name] {:symbol (str (s/upper-case name) "(1)")}) (:functions value))
        ;; Get existing columns from state (e.g., from previous select with date extraction)
        existing-columns (state :columns)
        ;; Filter out auto-id columns from existing columns
        non-auto-existing (filter #(not (:auto-id %)) existing-columns)

        ;; Merge group columns with existing selected columns
        ;; We want: group columns + any col-fn columns from selected columns
        merged-columns (if (seq non-auto-existing)
                         ;; We have existing selected columns (possibly with col-fn applied)
                         ;; Strategy: for each group column, check if there's a matching col-fn column
                         ;; Then also add any remaining col-fn columns that aren't covered
                         (let [;; Process group columns and find matching col-fn
                               group-with-fn (mapcat (fn [g-col]
                                                       (let [matching-fn (filter #(and (= (:column %) (:column g-col))
                                                                                       (= (:alias %) (:alias g-col))
                                                                                       (:col-fn %))
                                                                                 non-auto-existing)]
                                                         (if (seq matching-fn)
                                                           ;; Use the col-fn column (should be exactly 1)
                                                           (take 1 matching-fn)
                                                           ;; No col-fn, use the group column itself
                                                           [g-col])))
                                                     group-columns)
                               ;; Find col-fn columns that aren't associated with any group column
                               used-cols (set (map #(select-keys % [:column :alias]) group-columns))
                               other-fns (filter #(and (:col-fn %)
                                                       (not (contains? used-cols (select-keys % [:column :alias]))))
                                                 non-auto-existing)]
                           ;; Combine: group columns (with their fns) + other fns
                           (concat group-with-fn other-fns))
                         ;; No existing selected columns, just use group columns
                         group-columns)

        ;; SELECT clause: merged columns + aggregate functions
        select-columns (concat merged-columns fn-columns)]
    (-> state
        (assoc :columns select-columns)
        ;; GROUP BY uses all the merged columns
        (assoc :group merged-columns))))

