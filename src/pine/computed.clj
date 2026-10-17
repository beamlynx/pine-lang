(ns pine.computed
  "Computed values: a value that isn't just a column. `coalesce(a, b)`,
  `id in edited`, `if x is null then 'a' else 'b'`, and the aggregates of
  group:, `min(a.id) where a.type = 'x'`. See docs/computed-values.md.

  A computed value is a tree of nodes, each a map with a :kind:

    {:kind :column :alias \"a\" :column \"id\" :path [...] :cast \"text\"}
    {:kind :literal :value <a pine.data-types value>}  ; string, number, true, $variable ...
    {:kind :call :fn \"coalesce\" :args [<node> ...] :cast \"text\"}
    {:kind :condition :conditions [<where: condition> ...]}  ; joined with or
    {:kind :if :when <condition node> :then <node> :else <node or nil>}
    {:kind :aggregate :fn \"min\" :arg <node or nil> :filter <condition node or nil>}

  The parser builds the tree with conditions as [column operator value]
  triples; pine.ast.value resolves it, after which conditions are the same
  maps pine.ast.where stores in :where.

  This namespace has no dependencies on the rest of Pine, so the parser, the
  AST, the evaluator and the access policy can all use it. It holds the one
  table of functions, the walk that finds the columns a value reads, and the
  printer that writes a value back as Pine text."
  (:require [clojure.string :as s]))

;; ---------
;; FUNCTIONS
;; ---------
;;
;; One entry per function. Adding a function is adding an entry: the grammar
;; accepts any name before `(`, and the parser, the AST and the evaluator all
;; read this table.
;;
;; - :arity      [min max], max nil for no limit
;; - :aggregate  true for a function only group: takes
;; - :render     (fn [dialect args]) for a function, where args are the
;;               arguments' SQL; (fn [dialect arg filter]) for an
;;               aggregate, where arg and filter are SQL or nil

(defn- concat-sql
  "NULL when any argument is NULL, like SQL's `||`, on every database.
  Postgres's and SQLite's concat() skip NULLs instead, so they aren't used.
  MySQL's CONCAT() is NULL on a NULL argument already, and its `||` means
  OR. Postgres has no `||` for two non-text values, such as two integers, so
  each argument is cast to text there."
  [dialect args]
  (case dialect
    :mysql (str "CONCAT(" (s/join ", " args) ")")
    :sqlite (str "(" (s/join " || " args) ")")
    (str "(" (s/join " || " (map #(str "(" % ")::text") args)) ")")))

(defn- filtered
  "An aggregate over `arg`, counting only the rows where `filter` is true.
  Postgres and SQLite have FILTER (WHERE ...). MySQL doesn't: the value is
  NULL on the other rows instead, which every aggregate skips."
  [dialect sql-fn arg filter]
  (cond
    (nil? filter) (str sql-fn "(" arg ")")
    (= dialect :mysql) (str sql-fn "(CASE WHEN " filter " THEN " arg " END)")
    :else (str sql-fn "(" arg ") FILTER (WHERE " filter ")")))

(defn- aggregate [sql-fn]
  {:arity [1 1] :aggregate true
   :render (fn [dialect arg filter] (filtered dialect sql-fn arg filter))})

;; Date buckets: a date as text, cut to its year, month, ... `created_at =>
;; month` is `'2024-05'`.

(defn- col-fn-format
  "Map column function names to TO_CHAR format strings"
  [col-fn]
  (case col-fn
    "year"   "YYYY"
    "month"  "YYYY-MM"
    "day"    "YYYY-MM-DD"
    "week"   "YYYY-MM-DD"
    "hour"   "YYYY-MM-DD HH24"
    "minute" "YYYY-MM-DD HH24:MI"))

(defn bucket-sql
  "A date bucket's SQL: `created_at => month`, or `month(created_at)`.
  MySQL's shape isn't just a different format string like Postgres's TO_CHAR(DATE_TRUNC(...)) - week in particular needs its own
  DATE_SUB/WEEKDAY expression, since MySQL has no DATE_TRUNC. WEEKDAY()
  returns 0 for Monday, matching Postgres's DATE_TRUNC('week') boundary."
  [dialect col-fn col-ref]
  (case dialect
    :mysql (case col-fn
             "year"   (str "DATE_FORMAT(" col-ref ", '%Y')")
             "month"  (str "DATE_FORMAT(" col-ref ", '%Y-%m')")
             "day"    (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d')")
             "week"   (str "DATE_FORMAT(DATE_SUB(" col-ref ", INTERVAL WEEKDAY(" col-ref ") DAY), '%Y-%m-%d')")
             "hour"   (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d %H')")
             "minute" (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d %H:%i')"))
    ;; SQLite has no date type: a date is text (`2024-01-31 10:00:00`), which
    ;; strftime reads. %w counts Sunday as 0, so (%w + 6) % 7 is the days
    ;; since Monday - the boundary Postgres's DATE_TRUNC('week') uses.
    :sqlite (case col-fn
              "year"   (str "strftime('%Y', " col-ref ")")
              "month"  (str "strftime('%Y-%m', " col-ref ")")
              "day"    (str "strftime('%Y-%m-%d', " col-ref ")")
              "week"   (str "strftime('%Y-%m-%d', " col-ref ", '-' || ((CAST(strftime('%w', " col-ref ") AS INTEGER) + 6) % 7) || ' days')")
              "hour"   (str "strftime('%Y-%m-%d %H', " col-ref ")")
              "minute" (str "strftime('%Y-%m-%d %H:%M', " col-ref ")"))
    (str "TO_CHAR(DATE_TRUNC('" col-fn "', " col-ref "), '" (col-fn-format col-fn) "')")))

(defn- bucket [unit]
  {:arity [1 1]
   :render (fn [dialect [arg]] (bucket-sql dialect unit arg))})

(def functions
  {"coalesce" {:arity [2 nil]
               :render (fn [_ args] (str "COALESCE(" (s/join ", " args) ")"))}
   "concat"   {:arity [2 nil] :render concat-sql}
   ;; SQLite has no NOW().
   "now"      {:arity [0 0]
               :render (fn [dialect _] (if (= dialect :sqlite) "CURRENT_TIMESTAMP" "NOW()"))}
   "minute"   (bucket "minute")
   "hour"     (bucket "hour")
   "day"      (bucket "day")
   "week"     (bucket "week")
   "month"    (bucket "month")
   "year"     (bucket "year")

   ;; `count` is written bare: `group: a with count`.
   "count"    {:arity [0 0] :aggregate true
               :render (fn [dialect _ filter]
                         (if (and filter (= dialect :mysql))
                           (str "COUNT(CASE WHEN " filter " THEN 1 END)")
                           (filtered dialect "COUNT" "1" filter)))}
   "min"      (aggregate "MIN")
   "max"      (aggregate "MAX")
   "sum"      (aggregate "SUM")
   "avg"      (aggregate "AVG")
   ;; True when the condition is true for any row. MySQL and SQLite have no
   ;; BOOL_OR; a condition there is 1, 0 or NULL, so MAX is the same, NULL
   ;; over only NULLs included.
   "any"      {:arity [1 1] :aggregate true
               :render (fn [dialect arg filter]
                         (filtered dialect (if (= dialect :postgres) "BOOL_OR" "MAX") arg filter))}
   "all"      {:arity [1 1] :aggregate true
               :render (fn [dialect arg filter]
                         (filtered dialect (if (= dialect :postgres) "BOOL_AND" "MIN") arg filter))}})

(defn function-names
  "The names of the functions, aggregates or not, for messages."
  [aggregate?]
  (sort (keep (fn [[n {a :aggregate}]] (when (= (boolean a) aggregate?) n)) functions)))

(defn check-call
  "Throws when a call names no function, or the wrong number of arguments.
  `aggregates?` is whether aggregates are allowed where the call is."
  [fn-name n-args aggregates?]
  (let [{[lo hi] :arity aggregate? :aggregate :as f} (get functions (s/lower-case fn-name))]
    (cond
      (nil? f)
      (throw (ex-info (str "`" fn-name "()` isn't a Pine function. There are " (s/join ", " (map #(str % "()") (function-names false)))
                           ", and in group: " (s/join ", " (function-names true)) ".")
                      {:error-type "unknown-function" :function fn-name}))

      (and aggregate? (not aggregates?))
      (throw (ex-info (str "`" fn-name "()` is an aggregate. Use it after `with` in group:, like `group: a with b => "
                           fn-name " as " fn-name "_b`.")
                      {:error-type "aggregate-outside-group" :function fn-name}))

      (and (not aggregate?) aggregates?)
      (throw (ex-info (str "`" fn-name "()` isn't an aggregate. After `with` in group: come "
                           (s/join ", " (function-names true)) ".")
                      {:error-type "not-an-aggregate" :function fn-name}))

      (or (< n-args lo) (and hi (> n-args hi)))
      (throw (ex-info (str "`" fn-name "()` takes "
                           (cond (= lo hi) (str lo (if (= 1 lo) " argument" " arguments"))
                                 (nil? hi) (str lo " or more arguments")
                                 :else (str lo " to " hi " arguments"))
                           ", not " n-args ".")
                      {:error-type "arity" :function fn-name})))))

;; -------
;; COLUMNS
;; -------

(declare columns)

(defn- condition-columns
  "{:alias :column ...} for each column a resolved where: condition reads."
  [{:keys [alias column value] :as condition}]
  (concat
   (cond
     (:computed condition) (columns (:computed condition))
     (seq column) [(select-keys condition [:alias :column :path :source])])
   (case (:type value)
     :column (let [[a c] (:value value)] [{:alias a :column c}])
     :computed (columns (:node value))
     nil)))

(defn columns
  "Every column a resolved computed value reads, as {:alias :column} maps
  (with :path and :source when known), in the order they're written."
  [node]
  (case (:kind node)
    :column [(select-keys node [:alias :column :path :source])]
    :call (mapcat columns (:args node))
    :condition (mapcat (fn [c] (if-let [group (:or c)] (mapcat condition-columns group) (condition-columns c)))
                       (:conditions node))
    :if (concat (columns (:when node)) (columns (:then node)) (some-> (:else node) columns))
    :aggregate (concat (some-> (:arg node) columns) (some-> (:filter node) columns))
    nil))

(defn uses
  "The distinct {:alias :column} pairs a computed value reads: what the
  canvas draws it from."
  [node]
  (vec (distinct (map #(select-keys % [:alias :column]) (columns node)))))

;; -------
;; PRINTER
;; -------
;;
;; A resolved value written back as Pine. /build sends it to the client as a
;; computed column's `text`.
;;
;; A function of one value is written with the arrow, `created_at => month`,
;; `a.id => min`: nothing about its argument can be misread, and it is how a
;; person appends a step. Anything else is a call: `coalesce(a, b)` keeps
;; every argument in order, and `any(type = 'x')` keeps a condition whole.

(defn- quote-string [x]
  (str "'" (s/replace (str x) "'" "''") "'"))

(defn- step-text [step]
  (cond
    (integer? step) (str "[" step "]")
    (re-matches #"[A-Za-z_][A-Za-z0-9_-]*" step) (str "." step)
    :else (str ".'" (s/replace step "'" "''") "'")))

(defn- column-text [alias column path]
  (str (when alias (str alias ".")) column (apply str (map step-text path))))

(defn- literal-text [{:keys [type value text list] :as v}]
  (case type
    :string (quote-string value)
    :date (quote-string (or text value))
    :number (str value)
    :boolean (str value)
    :symbol (s/lower-case (str value))
    :variable (str "$" value)
    :uuid (quote-string value)
    ;; A bound `in $ids` becomes a list of literals.
    (if (sequential? v) (str "(" (s/join ", " (map literal-text v)) ")")
        (if list (str "$" value) (quote-string value)))))

(def ^:private operator-text
  {"=" "=" "!=" "!=" ">" ">" "<" "<" ">=" ">=" "<=" "<=" "LIKE" "like" "NOT LIKE" "not like"
   "ILIKE" "ilike" "NOT ILIKE" "not ilike" "IN" "in" "NOT IN" "not in" "IS" "is" "IS NOT" "is not"})

(declare text)

(defn- value-text [value]
  (cond
    (sequential? value) (str "(" (s/join ", " (map literal-text value)) ")")
    (= :column (:type value)) (let [[a c] (:value value)] (column-text a c nil))
    (= :named-result (:type value)) (:value value)
    (= :computed (:type value)) (text (:node value))
    (= :jsonb (:type value)) (:value value)
    :else (literal-text value)))

(defn- condition-text [{:keys [alias column path cast operator value] :as condition}]
  (str (if-let [node (:computed condition)] (text node) (column-text alias column path))
       " " (get operator-text operator (s/lower-case operator)) " "
       (value-text value)
       (when cast (str " ::" cast))))

(defn- conditions-text [conditions]
  (s/join " or " (map (fn [c] (if-let [group (:or c)] (s/join " or " (map condition-text group)) (condition-text c)))
                      conditions)))

(defn- cast-text [cast] (when cast (str "::" cast)))

(defn- arrow?
  "Whether a function of `args` is written with the arrow: one argument,
  which is neither a condition nor an if."
  [args]
  (and (= 1 (count args)) (not (#{:condition :if} (:kind (first args))))))

(defn text
  "A resolved computed value as Pine text."
  [node]
  (case (:kind node)
    :column (str (column-text (:alias node) (:column node) (:path node)) (cast-text (:cast node)))
    :literal (literal-text (:value node))
    :call (if (and (arrow? (:args node)) (not (:cast node)))
            (str (text (first (:args node))) " => " (:fn node))
            (str (:fn node) "(" (s/join ", " (map text (:args node))) ")" (cast-text (:cast node))))
    :condition (conditions-text (:conditions node))
    :if (str "if " (text (:when node)) " then "
             (let [t (text (:then node))] (if (= :if (:kind (:then node))) (str "(" t ")") t))
             (when-let [e (:else node)] (str " else " (text e))))
    :aggregate (str (cond
                      (nil? (:arg node)) (:fn node)
                      (arrow? [(:arg node)]) (str (text (:arg node)) " => " (:fn node))
                      :else (str (:fn node) "(" (text (:arg node)) ")"))
                    (when-let [f (:filter node)] (str " where " (text f))))
    ""))
