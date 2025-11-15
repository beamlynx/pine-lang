(ns pine.ast.table
  (:require
   [clojure.string :as s]))

(defn- join-helper
  "Find the references between the tables, get the columns for the first
  reference and return the pair of alias and columns that will be used for the join"
  [references t1 t2 a1 a2 c direction]
  (when-let [refs (get-in references [:table t1 :referred-by t2 :via])] ;; get references for the tables
    (let [get-col-fn            (if c (fn [_] c) (fn [xs] (if xs (first xs) nil)))
          col                   (-> refs keys get-col-fn)
          join                  (-> (get-in refs [col]) reverse first)
                                ;; Normally there is only one foreign key but if there
                                ;; multiple, then we use the last one which is `id`

          [_ _ col _ _ _ f-col] join ;; [ schema table col r f-schema f-table f-col ]
          ]
      (if (= direction :of)
        [a2 f-col :of a1 col]
        [a1 col :has a2 f-col]))))
;; TODO: use spec for the state value i.e. first arg
(defn- join-tables [{:keys [references aliases]} x y c parent]
  (let [a1 (x :alias)
        a2 (y :alias)
        {t1 :table _ :schema} (aliases a1) ;; t1 :table s1
        {t2 :table _ :schema} (aliases a2) ;; t2 :table s2
        result (or
                ;; By default we narrow the results i.e.
                ;; We get the children first and if a resultis not found, only
                ;; then we look at parents
                (if (not parent) (join-helper references t1 t2 a1 a2 c :has) nil)
                (join-helper references t2 t1 a2 a1 c :of))]
    result))

(defn- update-joins
  "Use the tables in the state to create a join between the last 2 tables. The
  reason to get the tables from the state is that they have been assigned an
  alias. We only use the join column from the current value being processed."
  [state current]
  (let [{:keys [join-column join-left-column join-right-column parent join]} current
        from-alias                   (state :context)]
    (cond
      (nil? from-alias) state
      ;; Explicit columns case: left table's column = right table's column
      ;; In "a | b .a_id = .id", left-col is "id" (from a), right-col is "a_id" (from b)
      (and join-left-column join-right-column)
      (let [x (-> state :aliases (get from-alias))
            join-result [(x :alias) join-left-column :has (current :alias) join-right-column]]
        (-> state
            (assoc-in [:join-map (x :alias) (current :alias)] join-result)
            (update :joins conj [(x :alias) (current :alias) join-result join])))

      :else (let [x (-> state :aliases (get from-alias))
                  join-result (join-tables state x current join-column parent)]
              (-> state
                  (assoc-in [:join-map (x :alias) (current :alias)] join-result)
                  (update :joins conj [(x :alias) (current :alias) join-result join]))))))
(defn make-alias [s]
  (let [words (if (not-empty s) (s/split s #"_") ["x"])
        initials (map #(subs % 0 1) words)]
    (apply str initials)))

;; todo: spec for the :value for a :table
(defn handle [state value]
  (let [index (state :index)
        {:keys [table alias schema parent join-column join-left-column join-right-column join]} value
        a (or alias (str (make-alias table) "_" (state :table-count)))
        current {:schema schema :table table :alias a :parent parent
                 :join-column join-column :join-left-column join-left-column
                 :join-right-column join-right-column :join join}]
    (-> state
        ;; pre
        (assoc  :context (state :current))
        (assoc  :current a)
        (assoc  :current-index index)
        ;;
        (update :tables conj current)
        (update :aliases assoc a current)
        (update-joins current)
        ;; post
        ;;
        ;; TODO: These are metadata - mayebe I should move them to a different
        ;; ns e.g. if the operation is table, then I update the following there
        ;;
        (update :table-count inc))))

