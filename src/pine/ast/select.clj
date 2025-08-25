(ns pine.ast.select)

(defn handle [state value]
  (let [i       (state :index)
        current (state :current)
        columns (map #(-> %1
                          (assoc :alias (or (:alias %1) current))
                          (assoc :operation-index i))
                     value)]
    (-> state
        (update :columns into columns))))

(defn- has-id-column?
  "Check if a table has an 'id' column by looking up the table info in references"
  [references aliases alias]
  (when-let [{:keys [table schema]} (get aliases alias)]
    (let [columns (if schema
                    (get-in references [:schema schema :table table :columns])
                    (get-in references [:table table :columns]))]
      (some #(= "id" (:column %)) columns))))

(defn- create-auto-id-column
  "Create a hidden auto-ID column for a table alias"
  [alias operation-index]
  {:column "id"  ; Use "id" column instead of empty column with symbol
   :alias alias
   :column-alias (str "__" alias "__id")
   :hidden true  ; Mark as hidden for UI purposes
   :auto-id true  ; Mark as auto-generated ID
   :operation-index operation-index}) ; Add operation index for hints context

(defn- should-add-auto-ids?
  "Check if we should add auto-ID columns based on the operation type"
  [state]
  (let [operation-type (-> state :operation :type)]
    (not (contains? #{:count :group :delete-action :update-action} operation-type))))

(defn add-auto-id-columns
  "Add auto-ID columns for all tables in the state, but only if the table actually has an 'id' column"
  [state]
  (if (should-add-auto-ids? state)
    (let [table-aliases (map :alias (:tables state))
          ;; Use the current operation index as the starting point for auto-ID columns
          ;; This ensures they come after all other operations
          next-operation-index (inc (:index state))
          references (:references state)
          aliases (:aliases state)
          ;; Only create auto-ID columns for tables that actually have an 'id' column
          valid-aliases (filter #(has-id-column? references aliases %) table-aliases)
          auto-id-columns (map-indexed #(create-auto-id-column %2 (+ next-operation-index %1)) valid-aliases)]
      (update state :columns into auto-id-columns))
    state))
