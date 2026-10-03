(ns pine.variables
  "Variables: `$name` in an expression, with its value supplied alongside the
  request instead of written into the expression.

    company | where: name = $company_name
    request | where: tenant_id in $tenant_ids

  The request's `variables` map holds each value as {\"value\": ...}: a
  string, number or boolean, or a list of them for `in`. A value is turned into
  the same typed value a literal would be (a string, a number) before the AST
  sees it, so it is typed by its column and bound as a `?` parameter like any
  literal. It is never pasted into the SQL text.

  A variable with no value is left as {:type :variable} in the parsed
  expression. /build still builds it, so a template keeps its hints and the
  SQL preview shows `$name`. /eval refuses to run it and names what's missing.

  Binding a variable to another Pine query ({\"expression\": ...}) is planned
  but not here yet. See beamlynx-plans/pending/2026-10-03-pine-variables.md."
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [pine.parser :as parser]))

(def ^:dynamic *bindings*
  "The current request's variable values: {\"name\" value}, already checked by
  normalize. Bound by api.clj for the duration of a build or eval."
  {})

(def max-variables 50)
(def max-list-items 5000)
(def max-string-length 10000)

(def ^:private name-pattern #"[A-Za-z_][A-Za-z0-9_]*")

(defn- fail [message]
  (throw (ex-info message {:error-type "variables"})))

(defn- scalar? [v]
  (or (string? v) (number? v) (boolean? v)))

(defn- check-scalar [n v]
  (cond
    (nil? v) (fail (str "$" n " has no value. A variable can't be null; to match nulls, write `is null`."))
    (and (string? v) (> (count v) max-string-length))
    (fail (str "$" n " is longer than " max-string-length " characters."))
    (not (scalar? v)) (fail (str "$" n " must be a string, a number or a boolean, or a list of them."))
    :else v))

(defn normalize
  "Check the request's `variables` map and return {\"name\" value}. Keys may be
  keywords (ring's JSON params) or strings. Each entry is {:value v}. Throws
  with a message for the caller to show when anything is malformed."
  [raw]
  (cond
    (nil? raw) {}
    (not (map? raw)) (fail "`variables` must be an object of {name: {value: ...}}.")
    (> (count raw) max-variables) (fail (str "At most " max-variables " variables can be passed."))
    :else
    (into {}
          (for [[k binding] raw
                :let [n (name k)]]
            (do
              (when-not (re-matches name-pattern n)
                (fail (str "\"" n "\" isn't a valid variable name. Use letters, digits and underscores.")))
              (when-not (map? binding)
                (fail (str "$" n " must be passed as {\"value\": ...}.")))
              (when (contains? binding :expression)
                (fail (str "$" n ": binding a variable to a query isn't supported yet. Pass {\"value\": ...}.")))
              (when-not (contains? binding :value)
                (fail (str "$" n " must be passed as {\"value\": ...}.")))
              (let [v (:value binding)]
                [n (if (sequential? v)
                     (do (when (> (count v) max-list-items)
                           (fail (str "$" n " has more than " max-list-items " values.")))
                         (mapv #(check-scalar n %) v))
                     (check-scalar n v))]))))))

(defn- typed
  "The literal a value stands for, as the parser would have produced it."
  [v]
  (cond
    (string? v) {:type :string :value v}
    (integer? v) {:type :number :value (long v)}
    (number? v) {:type :number :value (double v)}
    (boolean? v) {:type :boolean :value v}))

(defn- bind-one [{n :value list? :list :as variable} bindings]
  (if-not (contains? bindings n)
    variable
    (let [v (get bindings n)]
      (cond
        (and list? (not (sequential? v)))
        (fail (str "$" n " is used with `in`, so it needs a list of values, like [\"a\", \"b\"]."))

        (and list? (empty? v))
        (fail (str "$" n " is an empty list. `in` needs at least one value."))

        (and (not list?) (sequential? v))
        (fail (str "$" n " is a list, but it's used where one value goes. Use `in $" n "`."))

        list? (map typed v)
        :else (typed v)))))

(defn variable? [x]
  (and (map? x) (= (:type x) :variable)))

(defn bind
  "Replace every bound `$name` in a parsed expression (the parser's vector of
  operations) with the literal its value stands for. Unbound ones stay as
  they are."
  ([ops] (bind ops *bindings*))
  ([ops bindings]
   (if (empty? bindings)
     ops
     (walk/prewalk #(if (variable? %) (bind-one % bindings) %) ops))))

(defn- occurrences
  "Every `$variable` in a parsed expression, in order, each as its
  {:type :variable :value name} map (with :list true after `in`)."
  [ops]
  (let [found (atom [])]
    (walk/postwalk #(do (when (variable? %) (swap! found conj %)) %) ops)
    @found))

(defn used
  "The names of the `$variables` in a parsed expression, in order of first use."
  [ops]
  (distinct (map :value (occurrences ops))))

(defn- occurrences-in-expressions
  [expressions]
  ;; parse can throw instead of returning {:error} (an unknown condition,
  ;; `is $x`). That error belongs to /build or /eval, which report it the
  ;; usual way, so here it just means no variables.
  (mapcat #(try (some-> % parser/parse :result occurrences) (catch Exception _ nil)) expressions))

(defn used-in-expressions
  "The `$variables` across a request's expressions. An expression that doesn't
  parse contributes none; its parse error is reported elsewhere."
  [expressions]
  (distinct (map :value (occurrences-in-expressions expressions))))

(defn report
  "What /build tells the client: every variable used, those with no value,
  and those used with `in`, which take a list."
  [expressions bindings]
  (let [found (occurrences-in-expressions expressions)
        names (vec (distinct (map :value found)))]
    {:used names
     :unbound (vec (remove #(contains? bindings %) names))
     :lists (vec (distinct (map :value (filter :list found))))}))

(defn missing-message [names]
  (str "No value for " (str/join ", " (map #(str "$" %) names)) "."))
