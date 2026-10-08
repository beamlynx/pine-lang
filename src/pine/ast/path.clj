(ns pine.ast.path
  "What a dotted name means. The grammar reads `a.b` as alias `a` and column
  `b`, because it doesn't know the aliases. This decides, once the aliases
  are known:

  - `a` is an alias in scope: column `b` of that alias. Anything after `b`
    is a path into its JSON value.
  - Otherwise `a` is a column of the current table, and `b` and the rest
    are a path into its JSON value: `data.address.city`.

  When `a` is both, the alias wins. A migration that adds a column then
  can't change what an existing expression means, and the column is still
  reachable through its own table's alias: `e.data.plan`. See
  docs/json-paths.md."
  (:require [clojure.string :as s]
            [pine.data-types :as dt]))

(defn- in-scope?
  "An alias of a table in the expression, or a named result."
  [state a]
  (or (contains? (:aliases state) a)
      (contains? (:pending-assignments state) a)))

(defn- resolve-alias
  "A live alias (e.g. re-bound via `as`) always wins over a stale |= snapshot."
  [state a]
  (if (contains? (:aliases state) a) a (or (get-in state [:pending-assignments a :current]) a)))

(defn- column-type [state alias column]
  (dt/get-column-type (:references state) alias column (get (:aliases state) alias)))

(defn- json-type? [db-type]
  (contains? #{"json" "jsonb"} (some-> db-type s/lower-case)))

(defn- table-label [state alias]
  (or (get-in state [:aliases alias :table]) alias))

(defn- step-text [step]
  (cond
    (integer? step) (str "[" step "]")
    (re-matches #"[A-Za-z_][A-Za-z0-9_-]*" step) (str "." step)
    :else (str ".'" (s/replace step "'" "''") "'")))

(defn path-text
  "A path as it would be typed, without the alias: `data.address.city`,
  `data.tags[0]`, `data.'home address'`. A path column is named this."
  [column path]
  (apply str column (map step-text path)))

(defn- refuse-non-json [state {:keys [alias column path]}]
  (when (seq path)
    (let [db-type (column-type state alias column)]
      ;; An unknown type (a column of a named result Pine can't trace) is let
      ;; through: the database reports it if it isn't JSON.
      (when (and db-type (not (json-type? db-type)))
        (throw (ex-info (str "`" column "` is not a JSON column, so `" (path-text column path) "` has no keys.")
                        {}))))))

(defn- named-result-column
  "A named result names a selected path after the path: after
  `customer | s: data.plan |= p`, `p` has a column called `data.plan`. Read
  `p | s: data.plan` as that column, not as a path into a `data` column `p`
  doesn't have."
  [state {:keys [alias column path] :as col}]
  (let [ast (get-in state [:aliases alias :ast])
        named (path-text column path)]
    (if (and ast (seq path)
             (some #(= named (or (:column-alias %) (:column %))) (remove :auto-id (:columns ast))))
      (-> col (assoc :column named) (dissoc :path :column-alias))
      col)))

(defn- unknown-name [state alias]
  (let [current (:current state)
        table-alias (some (fn [[a {:keys [table]}]] (when (= table alias) a)) (:aliases state))]
    (ex-info (if table-alias
               (str "`" alias "` is a table. Name it by its alias, `" table-alias "`, like `" table-alias ".id`.")
               (str "`" alias "` is neither an alias nor a column of `" (table-label state current) "`."))
             {})))

(defn resolve-column
  "Resolve the alias of a column descriptor ({:alias :column :path}) and,
  when the name goes into a JSON value, name the column after its path with
  :column-alias (unless `as` already named it). Two paths into the same
  column then keep apart wherever columns are told apart by name.

  An alias that hides a JSON column of the current table is marked
  :alias-hides-column, so the canvas can show which one was read."
  [state {:keys [alias column path] :as col}]
  (let [current (:current state)
        resolved (cond
                   (nil? alias)
                   (assoc col :alias current)

                   (in-scope? state alias)
                   (cond-> (assoc col :alias (resolve-alias state alias))
                     (and (not= alias current) (json-type? (column-type state current alias)))
                     (assoc :alias-hides-column true))

                   (column-type state current alias)
                   (assoc col :alias current :column alias :path (into [column] path))

                   :else
                   (throw (unknown-name state alias)))
        resolved (named-result-column state resolved)]
    (refuse-non-json state resolved)
    (cond-> resolved
      (and (seq (:path resolved)) (not (:column-alias resolved)))
      (assoc :column-alias (path-text (:column resolved) (:path resolved))))))
