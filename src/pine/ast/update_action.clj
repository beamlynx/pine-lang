(ns pine.ast.update-action
  (:require [pine.ast.path :as path]
            [pine.ast.where :as where]
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

(defn- into-key
  "An assignment into a key of a JSON column: `update! data.plan = 'pro'`.
  The literal becomes a JSON value the way it does in `where: data.plan =
  'pro'`, so 'pro' is the JSON string \"pro\", 5 the number 5 and null the
  JSON null. The column keeps its type in :db-type: a Postgres `json` column
  takes the jsonb that jsonb_set gives back only when it is cast."
  [state {:keys [value] :as assignment} {:keys [alias column] :as resolved}]
  (when (= :column (:type value))
    (throw (ex-info (str "update! can only write a value into a key of a JSON column, not another column. `"
                         (path/path-text column (:path resolved)) "` was given one.")
                    {})))
  (assoc assignment
         :column (select-keys resolved [:alias :column :path])
         :db-type (dt/get-column-type (:references state) alias column (get (:aliases state) alias))
         ;; An unbound $variable stays as it is; /eval refuses to run it.
         :value (if (= :variable (:type value)) value (where/json-literal "=" value))))

(defn- resolve-assignment
  "Find what an assignment writes. `data.plan` is read as alias `data` until
  it's resolved, so it is resolved here to find out whether it's a key inside
  a JSON column."
  [state {:keys [column] :as assignment}]
  (let [resolved (path/resolve-column state column)]
    (if (seq (:path resolved))
      (into-key state assignment resolved)
      (convert-assignment-value assignment state))))

(defn- refuse-key-and-column
  "`update! data = '{}', data.plan = 'pro'` gives `data` two values. A
  database would refuse a column set twice, or keep only one."
  [state assignments]
  (let [by-column (group-by (fn [{{:keys [alias column]} :column}] [(or alias (:current state)) column]) assignments)]
    (doseq [[[_ column] group] by-column
            :when (and (some (comp :path :column) group)
                       (some (comp empty? :path :column) group))]
      (throw (ex-info (str "update! can't write `" column "` and a key inside it at once. Write the keys, or the whole column.")
                      {})))))

(defn handle [state {:keys [assignments partial-column] :or {assignments []}}]
  (let [converted-assignments (mapv #(resolve-assignment state %) assignments)]
    (refuse-key-and-column state converted-assignments)
    (assoc state :update (cond-> {:assignments converted-assignments}
                           partial-column (assoc :partial-column partial-column)))))
