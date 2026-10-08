(ns pine.ast.select
  (:require [pine.db.references :as refs]))

(defn column-source
  "The real table (and schema) this column ultimately traces back to - itself
  if selecting from a real table, or copied forward from a variable's own
  already-resolved source for the matching column if selecting from a
  variable. Never recomputed past this one hop: a variable is always defined
  before it's used, so whatever it's built from has already gone through this
  same step, and its own :columns already carry a fully-real :source."
  [state alias raw-column]
  (let [{:keys [table schema ast]} (get (:aliases state) alias)]
    (if ast
      (some #(when (= raw-column (or (:column-alias %) (:column %))) (:source %))
            (:columns ast))
      {:table table :schema schema})))

(defn handle [state value]
  (let [i       (state :index)
        current (state :current)
        ;; Process each column and handle date functions
        ;; A live alias (e.g. re-bound via `as`) always wins over a stale |= snapshot
        resolve-alias #(if (contains? (:aliases state) %) % (or (get-in state [:pending-assignments % :current]) %))
        columns (mapcat (fn [col]
                          (let [col-with-defaults (-> col
                                                      (assoc :alias (resolve-alias (or (:alias col) current)))
                                                      (assoc :operation-index i))
                                source (column-source state (:alias col-with-defaults) (:column col))]
                            (if-let [col-fn (:column-function col)]
                              ;; Column function: apply function to column
                              [{:column (:column col)
                                :alias (:alias col-with-defaults)
                                :column-alias (or (:column-alias col) col-fn)  ; Use custom alias or function name
                                :col-fn col-fn                                 ; Mark which function to apply
                                :source source
                                :operation-index i}]
                              ;; Regular column - return as is
                              [(assoc col-with-defaults :source source)])))
                        value)]
    (-> state
        (update :columns into columns))))

(defn- key-columns
  "The primary key columns of the real table behind an alias, or nil."
  [references aliases alias]
  (when-let [{:keys [table schema]} (get aliases alias)]
    (refs/primary-key references schema table)))

(defn- create-auto-id-column
  "A hidden column carrying one primary key column of a table alias. The
  results grid reads these to know which row an edit changes."
  [alias column operation-index]
  {:column column
   :alias alias
   :column-alias (str "__" alias "__" column)
   :hidden true  ; Mark as hidden for UI purposes
   :auto-id true  ; Mark as a key column Pine added, not one the user selected
   :operation-index operation-index}) ; Add operation index for hints context

(defn- should-add-auto-ids?
  "Check if we should add auto-ID columns based on the operation type"
  [state]
  (let [operation-type (-> state :operation :type)]
    (not (contains? #{:count :group :delete-action :update-action} operation-type))))

(defn add-row-keys
  "Record the primary key of every real table in the state, as
  `:row-keys {alias [column ...]}`. A table without one is left out."
  [state]
  (let [{:keys [references aliases]} state]
    (assoc state :row-keys
           (into {}
                 (for [{:keys [alias]} (:tables state)
                       :when (not (:ast (get aliases alias)))
                       :let [columns (key-columns references aliases alias)]
                       :when columns]
                   [alias (vec columns)])))))

(defn add-auto-id-columns
  "Add a hidden column for each primary key column of every table in the
  state. A table without a primary key gets none, and its rows can't be
  edited from the results grid."
  [state]
  (if (should-add-auto-ids? state)
    (let [table-aliases (map :alias (:tables state))
          ;; Use the current operation index as the starting point for auto-ID columns
          ;; This ensures they come after all other operations
          next-operation-index (inc (:index state))
          row-keys (:row-keys state)
          ;; Only real tables (not variables/CTEs) that have a primary key
          keyed (for [alias table-aliases
                      column (get row-keys alias)]
                  [alias column])
          auto-id-columns (map-indexed (fn [i [alias column]]
                                         (create-auto-id-column alias column (+ next-operation-index i)))
                                       keyed)]
      (update state :columns into auto-id-columns))
    state))
