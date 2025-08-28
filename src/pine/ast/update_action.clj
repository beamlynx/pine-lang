(ns pine.ast.update-action
  (:require [pine.data-types :as dt]))

(defn- convert-assignment-value
  "Convert an assignment value to the appropriate database type based on the column's schema"
  [assignment state]
  (let [{:keys [column value]} assignment
        {:keys [alias column]} column
        current-alias (or alias (:current state))
        table-info (get (:aliases state) current-alias)
        db-type (dt/get-column-type (:references state) current-alias column table-info)]
    (if db-type
      (assoc assignment :value (dt/convert-value-to-db-type value db-type))
      assignment)))

(defn handle [state assignments]
  (let [converted-assignments (mapv #(convert-assignment-value % state) (:assignments assignments))]
    (assoc state :update (assoc assignments :assignments converted-assignments))))