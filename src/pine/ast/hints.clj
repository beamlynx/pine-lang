(ns pine.ast.hints
  (:require [clojure.string :as str]))

(defn- filter-relations
  "Given a partial token (which can be completed to a table) and all the
  candidate relations, we filter out the unrelated candidates"
  [token candidates]
  (filter #(-> %1
               first
               clojure.string/lower-case
               (clojure.string/includes? token)) candidates))
;; ---------------------------------------------------------------------------
;; Table Hints
;; ---------------------------------------------------------------------------

(defn create-hint-from-table [state tables]
  (let [refs (-> state :references :table)]
    (mapcat identity
            (for [table tables
                  :let [schemas (->> table refs :in keys)]]
              (for [schema schemas]
                {:schema schema :table table})))))

(defn table-hints
  "No context - get all the tables matching the token"
  [state token]
  (let [candidates (-> state :references :table)
        suggestions (filter-relations token candidates)]
    (create-hint-from-table state (->> suggestions keys distinct (sort-by count)))))
;; ---------------------------------------------------------------------------
;; Relation Hints
;; ---------------------------------------------------------------------------

(defn- generate-expression [{:keys [schema table column parent alias]}]
  (str (if schema (str schema ".") "") table
       (if alias (str " as " alias) "")
       (if column (str " ." column) "")
       (if parent " :parent" "")))

;; via-details look like:
;; ["z"  "document"      "created_by"  :refers-to   "y"  "employee" "id"]
(defn- create-hint-from-relation-array [table via-details]
  (map (fn [vd]
         (let [direction (nth vd 3)
               parent? (= direction :refers-to)
               schema (nth vd 4)
               column (nth vd (if parent? 2 6))]
           {:schema schema
            :table table
            :column column
            :parent parent?}))
       via-details))

(defn- create-hint-from-relations [relations]
  (mapcat (fn [relation]
            (let [table (first relation)
                  via (get-in relation [1 :via])
                  via-details (mapcat identity (vals via))]
              (create-hint-from-relation-array table via-details)))
          relations))

(defn relation-hints [state token]
  (let [from-alias (state :context)
        from-table (-> state :aliases (get from-alias) :table)
        parents (-> state :references :table (get from-table) :refers-to)
        children (-> state :references :table (get from-table) :referred-by)
        suggestions (filter-relations token (concat parents children))]
    (-> suggestions
        create-hint-from-relations
        ;; TODO: instead of doing a distinct, we can do a reduce and keep track
        ;; of duplicates
        distinct)))

(defn generate-table-hints [state]
  (let [{token :table parent :parent} (-> state :tables reverse first)
        from-alias (state :context)
        table-hints (if from-alias
                      ;; This is not the first table, then filter out the related tables
                      (relation-hints state token)
                      ;; This is the first table - get all the tables matching the token
                      (table-hints state token))
        ;; Filter by parent if specified
        table-hints (if parent
                      (filter #(= (:parent %) true) table-hints)
                      table-hints)
        ;; Add pine expression to each hint
        add-pine-expression (fn [h] (assoc h :pine (generate-expression h)))]
    (map add-pine-expression table-hints)))

(defn generate-all-column-hints
  ([state] (generate-all-column-hints state (state :current))) ;; Overload for default `a`
  ([state a]
   (let [aliases (state :aliases)
         {table :table schema :schema} (->> a (get aliases))
         columns (if schema
                   (get-in state [:references :schema schema :table table :columns])
                   (get-in state [:references :table table :columns]))]
     (for [column columns]
       (-> column
           (select-keys [:column])
           (assoc :alias a))))))

(defn find-relevant-columns [hints column]
  (if column
    (filter #(str/includes? (:column %) (:column column)) hints)
    hints))

(defn exclude-columns [hints columns]
  (if (seq columns)
    (filter (fn [hint]
              (not (some #(= (:column hint) (:column %)) columns)))
            hints)
    hints))

(defn generate-where-hints [state]
  ;; Where conditions need custom logic unlike order/select partials because:
  ;; - Order partial: simple exclude-already-selected logic works with generic generate-column-hints
  ;; - Where partial: complex state-dependent filtering based on partial completion:
  ;;   * `where:` → show all columns
  ;;   * `w: i` → filter to columns matching "i" 
  ;;   * `w: id =` → show all columns (ready for next condition)
  (let [operation (state :operation)
        where-data (get operation :value {})
        partial-condition (:partial-condition where-data)
        current-alias (state :current)]
    (if (nil? partial-condition)
      ;; No partial condition yet, show all columns
      (generate-all-column-hints state current-alias)
      ;; Has partial condition, filter based on it
      (if (and (:column partial-condition) (not (contains? partial-condition :operator)))
        ;; Just a column specified, filter hints for that column
        (find-relevant-columns
         (generate-all-column-hints state current-alias)
         partial-condition)
        ;; Column + operator, show all columns for next condition
        (generate-all-column-hints state current-alias)))))

(defn generate-column-hints [state columns]
  ;; Generic column hints logic that works for most operations:
  ;; - Complete operations (select, order, where): filter hints to match current column being typed
  ;; - Partial operations (select-partial, order-partial): exclude already-selected columns
  ;; - Note: where-partial needs custom logic (see generate-where-hints) due to complex state-dependent filtering
  (let [column (some-> columns reverse first)
        ;; Use operation-index to determine table context for hints
        ;; If column was added in a later operation than current context, use that column's alias
        a (if (and (seq column)
                   (> (column :operation-index) (state :current-index)))
            (column :alias)
            (state :current))
        hints (generate-all-column-hints state a)
        type (-> state :operation :type)]
    (cond
      ;; If the type is :select, :order, or :where and columns exist, filter hints using the columns
      (and (or (= type :select) (= type :order) (= type :where)) column)
      (find-relevant-columns hints column)

      ;; If the type is :select-partial, :order-partial, or :where-partial and columns exist
      (and (or (= type :select-partial) (= type :order-partial) (= type :where-partial)) columns)
      (exclude-columns hints (filter #(= (:alias %) a) columns))

      ;; Otherwise, return all hints
      :else hints)))

(defn handle
  "Generate hints based on the current operation. 
   If truncated-state is provided, use it for hint generation context."
  ([state]
   (handle state nil))
  ([state truncated-state]
   (let [state-for-hints (or truncated-state state)
         type (-> state-for-hints :operation :type)
         hints (case type
                 :table (generate-table-hints state-for-hints)
                 :select (generate-column-hints state-for-hints (state-for-hints :columns))
                 :select-partial (generate-column-hints state-for-hints (state-for-hints :columns))
                 :order-partial (generate-column-hints state-for-hints (state-for-hints :order))
                 :order (generate-column-hints state-for-hints (state-for-hints :order))
                 :where-partial (generate-where-hints state-for-hints)
                 :where (generate-all-column-hints state-for-hints)
                 [])
         type (case type :select-partial :select :order-partial :order :where-partial :where type)]
     (assoc-in state [:hints type] (or hints [])))))
