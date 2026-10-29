(ns pine.ast.elements
  "`a.data[*] as e`: one row for each element of the JSON array at a path,
  joined to the row it came from. The elements act like a table whose one
  column, `value`, holds the element as JSON. `e.status` is a key of the
  element, and `e` the element itself (see pine.ast.path). A row whose value
  there isn't an array, or is missing, has no elements, so it drops out, as
  in an inner join. See docs/json-paths.md."
  (:require [pine.ast.path :as path]
            [pine.ast.table :as table]))

(defn- default-alias
  "Named like a table: `changes[*]` is `c_2`."
  [state {:keys [column path]}]
  (let [k (or (last (filter string? path)) column)]
    (str (table/make-alias k) "_" (:table-count state))))

(defn handle [state value]
  (when-not (:current state)
    (throw (ex-info "`[*]` reads an array in a table's JSON column, so a table comes first: `activity | data[*] as e`." {})))
  (let [{:keys [alias column path] :as col} (path/resolve-column state (select-keys value [:alias :column :path]))
        _ (path/check-array-column state col)
        a (or (:as value) (default-alias state {:column column :path path}))
        _ (when (contains? (:aliases state) a)
            (throw (ex-info (str "`" a "` is already an alias. Name the elements with `as`, like `as " a "_e`.") {})))
        entry {:alias a :table nil :schema nil
               :elements {:alias alias :column column :path (vec path)}
               :index (:index state)}]
    (-> state
        (assoc :context (:current state))
        (assoc :current a)
        (assoc :current-index (:index state))
        (update :tables conj entry)
        (update :aliases assoc a entry)
        (update :table-count inc))))

(defn check-join-from
  "Refuses a join from an elements step. A join compares a column of each
  table, and the elements have keys, not columns. Pine doesn't join on a key
  yet."
  [state]
  (when (path/element-alias? state (:current state))
    (throw (ex-info (str "A join can't start from the elements `" (:current state) "`: it joins on columns, and "
                         "elements have keys. Join the table before `[*]`, or go back to a table first with `from:`.")
                    {})))
  state)
