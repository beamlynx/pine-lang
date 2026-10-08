(ns pine.ast.update-action
  (:require [pine.ast.path :as path]
            [pine.data-types :as dt]))

(defn- convert-assignment-value
  "Convert an assignment value to the appropriate database type based on the column's schema"
  [assignment state]
  (let [{:keys [column value]} assignment
        {:keys [alias column]} column
        current-alias (or alias (:current state))
        table-info (get (:aliases state) current-alias)
        db-type (dt/get-column-type (:references state) current-alias column table-info)]
    ;; An unbound $variable keeps its name; see where.clj's resolve-condition.
    ;; Another column is not a literal, so there is nothing to convert.
    (if (and db-type (not (#{:variable :column} (:type value))))
      (assoc assignment :value (dt/convert-value-to-db-type value db-type))
      assignment)))

(defn- refuse-json-path
  "update! writes whole columns. `data.plan` reads as alias `data` until it's
  resolved, so it's resolved here only to find out whether it's a path."
  [state {:keys [column]}]
  (let [{:keys [path] resolved-column :column} (path/resolve-column state column)]
    (when path
      (throw (ex-info (str "update! can't write into a key of a JSON column yet. Open the cell in the results to edit `"
                           resolved-column "`.")
                      {})))))

(defn handle [state {:keys [assignments partial-column] :or {assignments []}}]
  (run! #(refuse-json-path state %) assignments)
  (let [converted-assignments (mapv #(convert-assignment-value % state) assignments)]
    (assoc state :update (cond-> {:assignments converted-assignments}
                           partial-column (assoc :partial-column partial-column)))))