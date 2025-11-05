(ns pine.ast.group
  (:require
   [clojure.string :as s]))

(defn handle [state value]
  (let [i (state :index)
        current (state :current)
        ;; Get existing columns from state (e.g., from previous select with date extraction)
        existing-columns (state :columns)
        ;; Filter out auto-id columns from existing columns
        non-auto-existing (filter #(not (:auto-id %)) existing-columns)

        ;; Process group columns - but DON'T set alias yet
        raw-group-columns (map #(assoc %1 :operation-index i) (:columns value))

        ;; Create aggregate function symbols with aliases
        fn-columns (map (fn [name] {:symbol (str (s/upper-case name) "(1)")
                                    :column-alias name}) (:functions value))

        ;; Merge group columns with existing selected columns
        ;; ONLY include columns that are explicitly mentioned in the GROUP operation
        merged-columns (if (seq non-auto-existing)
                         ;; We have existing selected columns (possibly with col-fn applied)
                         ;; Strategy: for each group column, find matching existing column
                         ;; Match by column-alias (for derived columns) or column name (for regular columns)
                         (mapcat (fn [g-col]
                                   (let [g-col-name (:column g-col)
                                         ;; Try to find existing column by column-alias or column name
                                         ;; This preserves the original alias from the SELECT
                                         matching (filter #(or (= (:column-alias %) g-col-name)
                                                               (= (:column %) g-col-name))
                                                          non-auto-existing)]
                                     (if (seq matching)
                                       ;; Use the existing column (preserves original alias like "t")
                                       (take 1 matching)
                                       ;; No match, use the group column with default alias
                                       [(assoc g-col :alias (or (:alias g-col) current))])))
                                 raw-group-columns)
                         ;; No existing selected columns, use group columns with default alias
                         (map #(assoc % :alias (or (:alias %) current)) raw-group-columns))

        ;; SELECT clause: merged columns + aggregate functions
        select-columns (concat merged-columns fn-columns)]
    (-> state
        (assoc :columns select-columns)
        ;; GROUP BY uses all the merged columns
        (assoc :group merged-columns))))

