(ns pine.ast.where
  (:require [clojure.string :as s]
            [pine.data-types :as dt]))

(defn- convert-condition-value
  "Convert a condition value to the appropriate database type based on the column's schema"
  [value alias col state]
  (let [table-info (get (:aliases state) alias)
        db-type (dt/get-column-type (:references state) alias col table-info)]
    (if db-type
      (if (and (coll? value) (not (map? value)))
        ;; For collections (like IN operator values), convert each item
        ;; Check for (not (map? value)) to avoid treating data type maps as collections
        (map #(dt/convert-value-to-db-type % db-type) value)
        ;; For single values, convert directly
        (dt/convert-value-to-db-type value db-type))
      value)))

(defn- make-resolve-alias [state]
  #(if (contains? (:aliases state) %) % (or (get-in state [:pending-assignments % :current]) %)))

(declare refuse-named-result-as-column)

(defn- resolve-condition
  "Turn one parsed [column operator value] triple into the flat 5-tuple
  [alias col cast operator converted-value] stored in state's :where."
  [state current resolve-alias [column operator value]]
  (let [[alias col cast] (:value column)
        alias (resolve-alias (or alias current))
        ;; Before the right-hand column gets its default alias below: a bare
        ;; name there that is a named result is a mistake this reports.
        _ (refuse-named-result-as-column state [alias col cast operator value])
        ;; A $variable still here has no value in this request (pine.variables
        ;; replaced every bound one with its literal). Typing it by its column
        ;; would turn its name into a string, so it stays as it is: /build
        ;; shows it, /eval refuses to run it.
        converted-value (cond
                          ;; A column on the right without an alias belongs to
                          ;; the current table, as the column on the left
                          ;; does. It used to be written bare, which is
                          ;; ambiguous (or names the wrong table) in a join.
                          (= :column (:type value))
                          (update value :value (fn [[a c ca]] [(resolve-alias (or a current)) c ca]))

                          (#{:symbol :variable :named-result} (:type value))
                          value

                          :else
                          (convert-condition-value value alias col state))]
    [alias col cast operator converted-value]))

(defn- named-result-column
  "The one column a named result used after `in` exposes, as it's named in
  its CTE. Throws when the name isn't a named result, or when it selects
  every column or more than one."
  [state n]
  (let [var-ast (or (get-in state [:named-results n]) (get-in state [:pending-assignments n]))
        columns (remove :auto-id (:columns var-ast))
        column-name #(or (:column-alias %) (:column %))]
    (cond
      (nil? var-ast)
      (throw (ex-info (str "`" n "` after `in` must be a named result. Define it in an earlier block, ending with `|= " n "`.") {}))

      ;; `s: c.*` selects every column of a table: its column has no name.
      (or (empty? columns) (some #(s/blank? (column-name %)) columns))
      (throw (ex-info (str "The named result " n " selects every column. To use it after `in`, select the one column to match, like `| s: id |= " n "`.") {}))

      (next columns)
      (throw (ex-info (str "The named result " n " selects " (count columns) " columns (" (s/join ", " (map column-name columns))
                           "). To use it after `in`, select only the one column to match.") {}))

      :else [var-ast (column-name (first columns))])))

(defn- named-result? [state n]
  (or (contains? (:named-results state) n) (contains? (:pending-assignments state) n)))

(defn- refuse-named-result-as-column
  "`where: id = x` reads `x` as a column of the current table. When `x` is a
  named result, that's almost never what was meant, and nothing would say so
  until the database refused an unknown column. Point to `in` instead. A
  column that really has a named result's name can still be written with its
  alias, like `t.x`."
  [state [alias col _ operator value]]
  (let [[value-alias n] (when (= (:type value) :column) (:value value))]
    (when (and n (nil? value-alias) (named-result? state n))
      (let [left (str alias "." col)
            advice (case operator
                     "=" (str "To match its values, write `" left " in " n "`.")
                     "!=" (str "To exclude its values, write `" left " not in " n "`.")
                     (str "A named result can only be used after `in` or `not in`, like `" left " in " n "`."))]
        (throw (ex-info (str "`" n "` is a named result, not a column. " advice
                             " If you meant a column called " n ", write `" alias "." n "`.")
                        {}))))))

(defn- with-named-results
  "A named result used after `in` becomes `IN ( SELECT column FROM name )`:
  record which column on the condition, and the named result in
  :value-ctes so the evaluator emits its CTE."
  [state condition]
  (refuse-named-result-as-column state condition)
  (let [value (nth condition 4)]
    (if (= (:type value) :named-result)
      (let [n (:value value)
            [var-ast column] (named-result-column state n)]
        [(update state :value-ctes (fnil conj []) [n var-ast])
         (assoc condition 4 (assoc value :column column))])
      [state condition])))

(defn- add-conditions
  "Resolve conditions, register any named results they use, and return
  [state resolved-conditions]."
  [state conditions]
  (let [current (state :current)
        resolve-alias (make-resolve-alias state)]
    (reduce (fn [[st acc] c]
              (let [[st c] (with-named-results st (resolve-condition st current resolve-alias c))]
                [st (conj acc c)]))
            [state []]
            conditions)))

(defn handle [state value]
  (if-let [conditions (:or value)]
    ;; Conditions joined with `or` inside one where: segment combine with OR,
    ;; stored as a single group so the evaluator can tell them apart from the
    ;; AND-ed entries produced by separate where: pipe-steps.
    (let [[state resolved] (add-conditions state conditions)]
      (update state :where conj {:or resolved}))
    (let [[state [resolved]] (add-conditions state [value])]
      (update state :where conj resolved))))

(defn handle-partial [state {:keys [complete-conditions]}]
  ;; For WHERE-PARTIAL, we only store the complete conditions in :where
  ;; The partial condition is used for hints, not for query generation.
  ;; The complete conditions are the ones already joined with `or`, so they
  ;; are stored the way `handle` stores them: one group, not one AND-ed entry
  ;; each. Otherwise a half-typed `where: a = 1 or b = 2 or c` would filter
  ;; for both a and b, and finishing it would switch to either.
  (let [[state resolved] (add-conditions state (mapv :value complete-conditions))]
    (case (count resolved)
      0 state
      1 (update state :where conj (first resolved))
      (update state :where conj {:or resolved}))))
