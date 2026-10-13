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
            [pine.ast.table :as table]
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
             (some #(= named (or (:column-alias %) (:column %))) (remove table/added-column? (:columns ast))))
      (-> col (assoc :column named) (dissoc :path :column-alias))
      col)))

(defn- unknown-name [state alias]
  (let [current (:current state)
        table-alias (some (fn [[a {:keys [table]}]] (when (= table alias) a)) (:aliases state))]
    (ex-info (if table-alias
               (str "`" alias "` is a table. Name it by its alias, `" table-alias "`, like `" table-alias ".id`.")
               (str "`" alias "` is neither an alias nor a column of `" (table-label state current) "`."))
             {})))

;; ---------------------------------------------------------------------------
;; Aliases sealed into a checkpoint
;; ---------------------------------------------------------------------------
;;
;; A checkpoint seals the tables before it into a CTE, and the pipe goes on
;; from that CTE. The sealed tables' aliases stay in :aliases (auto-naming,
;; hints and named results read them), but they are no longer in the query.
;; `company as c | employee .company_id | l: 10 | s: c.id` must read `c.id`
;; as the column of result_1 that came from c.id, not as "c"."id", which
;; the outer query doesn't have.

(defn- live-aliases [state]
  (set (map :alias (:tables state))))

(defn- sealed-alias?
  "An alias of a table that a checkpoint sealed: known, but not one of the
  tables the pipe is on now."
  [state a]
  (and a
       (contains? (:aliases state) a)
       (not (contains? (live-aliases state) a))))

(defn- output-name [col]
  (or (:column-alias col) (:column col)))

(defn- explicit-columns [snapshot]
  (remove table/added-column? (:columns snapshot)))

(defn- exposed
  "How `snapshot`'s result names the column `alias.column` (with `path`)
  of one of its own tables: [name remaining-path], or nil when its result
  doesn't have that column. A path selected as a column (`s: data.plan`)
  is used by its name; otherwise the path goes on from the column."
  [snapshot alias column path]
  (let [explicit (explicit-columns snapshot)]
    (if (seq explicit)
      (or (some #(when (and (= alias (:alias %)) (= column (:column %)) (seq path) (= path (:path %)))
                   [(output-name %) nil])
                explicit)
          (some #(when (and (= alias (:alias %)) (= column (:column %)) (empty? (:path %))
                            (not (:col-fn %)))
                   [(output-name %) path])
                explicit)
          ;; `s: c.*`
          (some #(when (and (= alias (:alias %)) (s/blank? (:column %)) (= "*" (:symbol %)))
                   [column path])
                explicit))
      ;; No select: the result is every column of its current table.
      (when (= alias (:current snapshot))
        [column path]))))

(defn- snapshot-of [state a]
  (get-in state [:aliases a :ast]))

(defn- unseal-in
  "[name path] of `alias.column` in the result of `snapshot`, looking
  through the checkpoints inside it, or nil."
  [snapshot alias column path]
  (let [tables (set (map :alias (:tables snapshot)))]
    (if (contains? tables alias)
      (exposed snapshot alias column path)
      (some (fn [t]
              (when-let [inner (snapshot-of snapshot t)]
                (when-let [[n p] (unseal-in inner alias column path)]
                  (exposed snapshot t n p))))
            tables))))

(defn- seals?
  "Whether the snapshot, or a checkpoint inside it, has `alias` as a table."
  [snapshot alias]
  (let [tables (map :alias (:tables snapshot))]
    (boolean (or (some #{alias} tables)
                 (some #(some-> (snapshot-of snapshot %) (seals? alias)) tables)))))

(defn- checkpoint-step [state cte]
  (case (:kind (some #(when (= cte (:name %)) %) (:checkpoints state)))
    "group" "the group:"
    "limit" "the limit:"
    "the step"))

(defn- refuse-ambiguous
  "A name that is two columns of a named result or checkpoint can't pick
  either: the database would refuse it as ambiguous."
  [state cte n written]
  (when-let [snapshot (snapshot-of state cte)]
    (when (< 1 (count (filter #{n} (map output-name (explicit-columns snapshot)))))
      (throw (ex-info (str "`" cte "` has two columns named `" n "`, so `" written "` can't pick one. "
                           "Name them apart with `as` before " (checkpoint-step state cte)
                           ", like `s: c.id as c_id, id`.")
                      {:error-type "ambiguous-column" :alias cte :column n})))))

(defn- unseal
  "Read a column written with a sealed alias as the column of the
  checkpoint that kept it. Throws when the checkpoint didn't keep it."
  [state {:keys [alias column path] :as col}]
  (let [cte (some #(when (some-> (snapshot-of state %) (seals? alias)) %) (map :alias (:tables state)))
        written (str alias "." column)]
    (if-not cte
      col
      (if-let [[n p] (unseal-in (snapshot-of state cte) alias column path)]
        (do (refuse-ambiguous state cte n written)
            (cond-> (-> col (assoc :alias cte :column n) (dissoc :path))
              (seq p) (assoc :path p)))
        (let [kept (->> (:checkpoints state) (some #(when (= cte (:name %)) %)) :columns)]
          (throw (ex-info (str "`" written "` isn't a column of `" cte "`. " (s/capitalize (checkpoint-step state cte))
                               " before sealed `" alias "` into it"
                               (if (seq kept) (str ", and it keeps " (s/join ", " kept) ". ") ". ")
                               "To use `" written "` after " (checkpoint-step state cte) ", select it before: `s: "
                               written " as " alias "_" column ", ...`.")
                          {:error-type "sealed-column" :alias alias :column column :checkpoint cte})))))))

(defn unseal-column
  "A column of a sealed table, as the checkpoint column that kept it;
  any other column unchanged. For a column that doesn't go through
  resolve-column, such as one on the right of a where: comparison."
  [state col]
  (if (sealed-alias? state (:alias col))
    (unseal state col)
    col))

(defn resolve-column
  "Resolve the alias of a column descriptor ({:alias :column :path}) and,
  when the name goes into a JSON value, name the column after its path with
  :column-alias (unless `as` already named it). Two paths into the same
  column then keep apart wherever columns are told apart by name.

  A column of a table a checkpoint sealed is read as the checkpoint's
  column that kept it (see unseal). A bare name that is two columns of the
  named result the pipe is on is refused.

  An alias that hides a JSON column of the current table is marked
  :alias-hides-column, so the canvas can show which one was read."
  [state col]
  (let [{:keys [alias column path] :as col} (unseal-column state col)
        current (:current state)
        _ (when (nil? alias) (refuse-ambiguous state current column column))
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

;; ---------------------------------------------------------------------------
;; Computed names
;; ---------------------------------------------------------------------------

(defn computed-column
  "The computed column of the select: steps so far named `n`, or nil."
  [state n]
  (when n
    (some #(when (and (:computed %) (= n (:column-alias %))) %) (:columns state))))

(defn computed-ref
  "A bare name that is a computed column of an earlier select: step, as that
  column's value, to inline where the name is used: SQL can't use a SELECT
  list name in WHERE, PARTITION BY or GROUP BY. The name wins over a table
  column with the same name, which is still reachable with its alias.
  nil for any other column. See docs/computed-values.md."
  [state {:keys [alias column path] :as col}]
  (when (and (nil? alias) (empty? path))
    (when-let [c (computed-column state column)]
      (-> col
          (assoc :alias (:current state) :computed (:computed c))
          (dissoc :path)))))
