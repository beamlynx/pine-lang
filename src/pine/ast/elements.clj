(ns pine.ast.elements
  "`unnest: a.data as e`: one row for each element of the JSON array at a path,
  joined to the row it came from. It works on the table the pipe is on and
  leaves the pipe there, as where: does. `e.status` is a key of the element,
  and `e` the element itself (see pine.ast.path). In the SQL the elements are
  a table whose one column, `value`, holds the element as JSON. A row whose
  value there isn't an array, or is missing, has no elements, so it drops
  out, as in an inner join. See docs/json-paths.md."
  (:require [pine.ast.path :as path]
            [pine.ast.table :as table]))

(defn- default-alias
  "Named like a table: `unnest: data.changes` is `c_2`."
  [state {:keys [column path]}]
  (let [k (or (last (filter string? path)) column)]
    (str (table/make-alias k) "_" (:table-count state))))

(defn handle [state value]
  (when-not (:current state)
    (throw (ex-info "`unnest:` reads an array in a table's JSON column, so a table comes first: `activity | unnest: data as e`." {})))
  (let [{:keys [alias column path] :as col} (path/resolve-column state (select-keys value [:alias :column :path]))
        _ (path/check-array-column state col)
        a (or (:as value) (default-alias state {:column column :path path}))
        _ (when (contains? (:aliases state) a)
            (throw (ex-info (str "`" a "` is already an alias. Name the elements with `as`, like `as " a "_e`.") {})))
        ;; :on is the table the pipe was on. Without select:, the result
        ;; shows `e` in place of that table's columns (see pine.eval).
        entry {:alias a :table nil :schema nil
               :elements {:alias alias :column column :path (vec path) :on (:current state)}
               :index (:index state)}]
    (-> state
        (update :tables conj entry)
        (update :aliases assoc a entry)
        (update :table-count inc))))
