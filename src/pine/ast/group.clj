(ns pine.ast.group
  (:require
   [clojure.string :as s]
   [pine.ast.path :as path]
   [pine.ast.select :as select]
   [pine.ast.table :as table]
   [pine.ast.value :as value]))

(defn- aggregate-columns
  "[state columns] for the aggregates after `=>`. A bare `count` stays the
  COUNT(1) it always was. Any other is a computed column marked :aggregate,
  named by `as` or by value/default-name."
  [state functions]
  (let [[state columns]
        (reduce (fn [[st acc] {:keys [column-alias] :as f}]
                  (let [n (or column-alias (value/default-name f))]
                    (if (and (= "count" (:fn f)) (nil? (:filter f)))
                      [st (conj acc {:symbol "COUNT(1)" :column-alias n})]
                      (let [[st node] (value/resolve-node st (dissoc f :column-alias) true)]
                        [st (conj acc {:computed node :column-alias n :aggregate true})]))))
                [state []]
                functions)]
    (doseq [[n k] (frequencies (map :column-alias columns))
            :when (> k 1)]
      (throw (ex-info (str "Two aggregates are named `" n "`. Name them apart with `as`, like `min(a.id) as first_id`.")
                      {:error-type "duplicate-name" :column n})))
    [state columns]))

(defn handle [state value]
  (let [i (state :index)
        current (state :current)
        ;; A live alias (e.g. re-bound via `as`) always wins over a stale |= snapshot
        resolve-alias #(if (contains? (:aliases state) %) % (or (get-in state [:pending-assignments % :current]) %))
        ;; Get existing columns from state (e.g., from previous select with date extraction)
        existing-columns (state :columns)
        ;; Filter out auto-id columns from existing columns
        non-auto-existing (remove table/added-column? existing-columns)

        ;; Resolve what each name means first: `data.plan` is a path into
        ;; `data`, not alias `data`. Its alias may still be replaced below by
        ;; the alias of a matching selected column.
        ;; A computed column's name is the computed column itself, which
        ;; the merge below finds among the selected columns.
        raw-group-columns (map #(assoc (or (path/computed-ref state %1) (path/resolve-column state %1)) :operation-index i) (:columns value))
        column-name #(or (:column-alias %) (:column %))

        [state fn-columns] (aggregate-columns state (:functions value))

        ;; Merge group columns with existing selected columns
        ;; ONLY include columns that are explicitly mentioned in the GROUP operation
        merged-columns (if (seq non-auto-existing)
                         ;; We have existing selected columns (possibly with col-fn applied)
                         ;; Strategy: for each group column, find matching existing column
                         ;; Match by column-alias (for derived columns) or column name (for regular columns)
                         (mapcat (fn [g-col]
                                   (let [g-col-name (column-name g-col)
                                         ;; Try to find existing column by column-alias or column name
                                         ;; This preserves the original alias from the SELECT
                                         matching (filter #(or (= (:column-alias %) g-col-name)
                                                               (and (not (:path %)) (= (:column %) g-col-name)))
                                                          non-auto-existing)]
                                     (if (seq matching)
                                       ;; Use the existing column (preserves original alias like "t")
                                       (take 1 matching)
                                       ;; No match, use the group column with default alias
                                       (let [alias (resolve-alias (or (:alias g-col) current))]
                                         [(assoc g-col :alias alias
                                                 :source (select/column-source state alias (:column g-col)))]))))
                                 raw-group-columns)
                         ;; No existing selected columns, use group columns with default alias
                         (map (fn [g-col]
                                (let [alias (resolve-alias (or (:alias g-col) current))]
                                  (assoc g-col :alias alias
                                         :source (select/column-source state alias (:column g-col)))))
                              raw-group-columns))

        ;; SELECT clause: merged columns + aggregate functions
        ;; Must be a vector: :columns is later grown with (update :columns into ...)
        ;; elsewhere (e.g. select.clj), and `into`/`conj` prepend onto a plain seq
        ;; (concat's return type) instead of appending, silently reversing order.
        select-columns (vec (concat merged-columns fn-columns))]
    (-> state
        (assoc :columns select-columns)
        ;; The aggregates' names, e.g. ["count"], for the client's label of
        ;; a group checkpoint (main.clj's checkpoint-entry)
        (assoc :aggregates (mapv :column-alias fn-columns))
        ;; GROUP BY uses all the merged columns
        (assoc :group (vec merged-columns)))))

