(ns pine.sql.transpile
  "One SQL SELECT, UPDATE or DELETE to a Pine expression that picks the same
  rows. A statement with WITH queries becomes several blocks; see the
  STATEMENTS section for how they are formed.

  `sql->pine` converts only what Pine can say exactly. Anything else is
  refused with a message naming the construct and its line, never guessed
  at. See docs/sql-to-pine.md.

  The steps for each block, in order:

  1. pine.sql.parser reads the SQL. Its refusals name the words the grammar
     keeps out (OFFSET, DISTINCT, ...).
  2. plan-tables gives each table its place in the pipe and turns each
     JOIN's ON into `.new = .anchor` column pairs. A join tied to an earlier
     table than the last one gets a `from:` first.
  3. plan-where puts WHERE into conjunctive normal form: NOT is pushed down
     to the comparisons, OR is distributed over AND. Each AND-ed clause is
     one where:, since Pine joins conditions inside a where: with `or` only.
  4. plan-select picks select:, group: ... with count, or count:, then
     order: and limit:.
  5. render writes the operations, pine.parser/prettify lays them out, and
     pine.parser/parse checks the result reads as Pine.

  A column is referred to internally as {:t <table index> :name <column>}.
  It is written bare when it belongs to the table the pipe ends on, since
  Pine reads a bare column as that table's, and as alias.column otherwise.
  A table gets an alias only when something refers to it by one.

  With the connection's schema, a bare name takes the database's spelling:
  `c.tenantId` is written `.tenantId` when the table has a `tenantId`
  column, though Postgres would fold it to `tenantid`. See `spelling`."
  (:require [clojure.string :as s]
            [clojure.walk :as walk]
            [pine.ast.main :as ast]
            [pine.db.references :as refs]
            [pine.eval :as eval]
            [pine.parser :as pine]
            [pine.sql.parser :as sp]))

(def ^:private max-where-clauses
  "How many where: steps one WHERE may become. Distributing OR over AND can
  multiply clauses: (a AND b) OR (c AND d) is four."
  16)

(defn- refuse
  "Stop the conversion. `at` is the offset the message's line comes from."
  [at message]
  (throw (ex-info message {::refusal true :at at})))

;; ------
;; NAMES
;; ------

(defn- pine-name!
  "`n`, when Pine can write it as a name. Pine names are letters, digits,
  `_` and `-`, not starting with a digit."
  [n at]
  (when-not (re-matches #"[A-Za-z_][A-Za-z0-9_-]*" n)
    (refuse at (str "`" n "` can't be written as a name in Pine.")))
  n)

(def ^:private ^:dynamic *schema*
  "The connection's indexed references (pine.db.references), or nil when
  the conversion runs without a connection."
  nil)

(defn- spelling
  "The database's spelling of the bare name `n`, written in any case. `n`
  itself when the database has it, since that is how Postgres reads an
  unquoted name. Otherwise the one name equal to it ignoring case. With
  several, the SQL can't say which it means, so the conversion refuses.
  With none, `n` as written."
  [n candidates at]
  (let [matches (distinct (filter #(.equalsIgnoreCase ^String n ^String %) candidates))]
    (cond
      (some #{n} matches) n
      (empty? matches) n
      (= 1 (count matches)) (first matches)
      :else (refuse at (str "`" n "` could be " (s/join " or " (map #(str "`" % "`") (sort matches)))
                            ". Quote the one you mean, like \"" (first (sort matches)) "\".")))))

(def ^:private ^:dynamic *dialect*
  "The connection's dialect (:postgres, :mysql, :sqlite), or nil without
  one. MySQL reads `||` as OR, so it changes what converts."
  nil)

(def ^:private ^:dynamic *connection-id*
  "The connection the converted Pine is built against as a check, or nil
  to skip the check."
  nil)

(def ^:private ^:dynamic *warn!*
  "Adds a warning, from where no warn! is passed: converting a value."
  (fn [_]))

(def ^:private ^:dynamic *sql-tables*
  "Every table name the statement mentions, lowercased. A name Pine makes
  up for a block must not be one of them: the block would shadow the table."
  #{})

(def ^:private ^:dynamic *ctes*
  "The WITH queries defined so far, by name: {:columns [...]}, with
  :columns nil when they aren't known. A table with one of these names and
  no schema is that query's result, not a table in the database."
  {})

(defn- cte-name? [{:keys [schema table]}]
  (and (nil? schema) (contains? *ctes* table)))

(defn- db-table
  "The table's schema, name and columns as the database spells them, or {}
  without a schema. A WITH query's result has the columns it selects."
  [{:keys [schema table quoted schema-quoted at] :as t}]
  (cond
    (cte-name? t)
    {:cte true :db-columns (get-in *ctes* [table :columns])}

    (not *schema*)
    {}

    :else
    (let [schema (if (and schema (not schema-quoted)) (spelling schema (keys (:schema *schema*)) at) schema)
          tables (if schema (get-in *schema* [:schema schema :table]) (:table *schema*))
          table (if quoted table (spelling table (keys tables) at))]
      {:db-schema schema :db-table table :db-columns (map :column (get-in tables [table :columns]))
       :db-column-info (get-in tables [table :columns])})))

(defn- column-name
  "A column of `tbl`, in the database's spelling when it isn't quoted."
  [tbl n quoted at]
  (pine-name! (if quoted n (spelling n (:db-columns tbl) at)) at))

(defn- table-key
  "The name SQL refers to a table by: its alias, or its own name."
  [{:keys [alias table]}]
  (or alias table))

(defn- has-column?
  "Whether `tbl` has the column, ignoring case unless it is quoted."
  [tbl n quoted]
  (some #(if quoted (= n %) (.equalsIgnoreCase ^String n ^String %)) (:db-columns tbl)))

(defn- resolve-column
  "{:t :name} for a SQL column, looking only at `tables` (the ones in scope).
  An unqualified column with several tables belongs to the one table that
  has it, as in SQL. That needs every table's columns; without them, the
  column must be qualified."
  [tables {:keys [qualifier name quoted at]}]
  (pine-name! name at)
  (let [owners (when (every? #(seq (:db-columns %)) tables)
                 (keep-indexed (fn [i tbl] (when (has-column? tbl name quoted) i)) tables))]
    (cond
      qualifier
      (if-let [t (first (keep-indexed (fn [i tbl] (when (= qualifier (table-key tbl)) i)) tables))]
        {:t t :name (column-name (nth tables t) name quoted at)}
        (refuse at (str "`" qualifier "` isn't a table in this query.")))

      (= 1 (count tables))
      {:t 0 :name (column-name (first tables) name quoted at)}

      (= 1 (count owners))
      (let [t (first owners)]
        {:t t :name (column-name (nth tables t) name quoted at)})

      (and owners (empty? owners))
      (refuse at (str "`" name "` isn't a column of "
                      (s/join " or " (map #(str "`" (table-key %) "`") tables)) "."))

      :else
      (let [candidates (if (next owners) (map #(nth tables %) owners) tables)]
        (refuse at (str "`" name "` could belong to "
                        (s/join " or " (map #(str "`" (table-key %) "`") candidates))
                        ". Qualify it, like `" (table-key (first candidates)) "." name "`."))))))

;; -------
;; VALUES
;; -------

(def ^:private dropped-value-casts
  "Casts on a literal that Pine adds itself, from the column's type. Pine's
  own SQL preview writes '2024-01-01'::timestamp, so these must read back."
  #{"timestamp" "timestamptz" "timestamp with time zone" "timestamp without time zone"
    "date" "uuid" "json" "jsonb" "text" "varchar" "character varying"})

(def ^:private column-casts
  "A cast on the column side, as Pine writes it after the condition."
  {"text" "text" "varchar" "text" "character varying" "text" "uuid" "uuid"})

(def ^:private zoned-time
  "A date and time with a zone: `Z`, `+02`, `-05:30`."
  #"\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?\s*(?:[Zz]|[+-]\d{1,2}(?::?\d{2})?)")

(defn- check-literal
  "Refuses a literal Pine would read differently: a time with a zone, which
  Pine refuses, and a cast to date that drops a time of day, which Pine
  would keep."
  [{:keys [kind value cast at]}]
  (when (= :string kind)
    (when (re-matches zoned-time (s/trim value))
      (refuse at (str "'" value "' has a time zone, which isn't in Pine. Write the time without it, in the database's time zone.")))
    (when (and (= "date" cast) (re-find #"\d[ T]\d" value))
      (refuse at (str "'" value "'::date drops the time of day, which Pine wouldn't. Write the date alone, like '" (subs value 0 (min 10 (count value))) "'.")))))

(defn- special-value [{:keys [name at]}]
  (refuse at (case name
               ("CURRENT_DATE" "CURRENT_TIME" "LOCALTIME") (str name " isn't in Pine. now() is the time now, with the date.")
               (str name " isn't in Pine."))))

(def ^:private json-key-here
  "A JSON key with -> or ->> converts in a selected value or a function's argument, not here.")

(defn- value
  "A literal or variable operand as a Pine value."
  [{:keys [kind cast at] :as operand}]
  (when (and cast (not (dropped-value-casts cast)))
    (refuse at (str "A cast to " cast " isn't in Pine. Pine casts a column to text or uuid.")))
  (check-literal operand)
  (case kind
    :special (special-value operand)
    (:string :number :boolean :variable) (dissoc operand :at :cast)
    ;; Left for the caller, which knows whether a column can go here.
    :column (dissoc operand :at)
    :now {:kind :now}
    :null (refuse at "NULL can only be tested with IS NULL or IS NOT NULL.")
    :call (refuse at (str "Functions like " (:fn operand) "() aren't in Pine here."))
    :json (refuse at json-key-here)
    :count (refuse at "COUNT(*) here needs GROUP BY, with HAVING.")
    (refuse at "A computed value here isn't in Pine. A condition compares a column with a value.")))

(defn- column-cast [{:keys [cast at]}]
  (when cast
    (or (column-casts cast)
        (refuse at (str "A cast to " cast " isn't in Pine. Pine casts a column to text or uuid.")))))

;; ------
;; WHERE
;; ------
;;
;; An atom is one Pine condition: {:col :operator :value :cast}. :value is
;; nil for `is null`, a vector for `in`, and {:kind :column :col} for a
;; column on the right.

(def ^:private mirrored {"=" "=" "!=" "!=" "<" ">" ">" "<" "<=" ">=" ">=" "<="})
(def ^:private negated-op {"=" "!=" "!=" "=" "<" ">=" ">=" "<" ">" "<=" "<=" ">"})

(defn- column-operand? [o] (= :column (:kind o)))

(def ^:private keyword-values
  "Names Pine reads as a value, not a column, where a value can go: in a
  computed value, and on the right of a condition (true, false and null)."
  #{"true" "false" "null" "if" "then" "else"})

(defn- keyword-column!
  "Refuses a column whose name Pine would read as `names`' keyword here."
  [names {:keys [name at]}]
  (when (names (s/lower-case name))
    (refuse at (str "A column called `" name "` can't be written here in Pine: it reads as the keyword " (s/lower-case name) "."))))

(declare expr-pine expr-columns)

(def ^:private json-functions
  "SQL functions that read JSON, and Pine has under the same name."
  #{"jsonb_typeof" "jsonb_array_length"})

(defn- computed-operand?
  "A value Pine can compare a column with: COALESCE(...) or a || b."
  [{:keys [kind] f :fn :as o}]
  (or (= :concat kind) (and (= :call kind) (or (= "coalesce" f) (and (= "concat" f) (= :mysql *dialect*))))))

(defn- left-function?
  "A function Pine can put on the left of a condition: jsonb_typeof(x) = 'array'."
  [{:keys [kind] f :fn :as o}]
  (and (= :call kind) (not (:filter o)) (or (json-functions f) (computed-operand? o))))

(defn- function-atom
  "A comparison of a function with a value, `jsonb_typeof(x) = 'array'`, as
  an atom with :expr in place of :col."
  [tables {:keys [operator at]} left right]
  (when (#{:column :json :call :count :concat :pred :case} (:kind right))
    (refuse at "A function on the left of a condition is compared with a value in Pine, not with a column or another function."))
  (when (and (= :null (:kind right)) (#{"=" "!="} operator))
    (refuse (:at right) (str "`" operator " NULL` is never true in SQL. Write IS NULL or IS NOT NULL.")))
  (expr-pine {:tables tables :col-ref :name} left)
  {:expr left
   :tables tables
   :refs (set (map #(:t (resolve-column tables %)) (expr-columns left)))
   :operator operator
   :value (value right)})

(defn- compare-column-atom [tables {:keys [at]} left right operator]
  (doseq [o [left right]
          :when (and (#{:call :count :concat :pred :case :json} (:kind o))
                     (not (and (identical? o right) (column-operand? left) (computed-operand? o))))]
    (case (:kind o)
      :call (refuse (:at o) (str "Functions like " (:fn o) "() aren't in Pine here. A condition compares a column with a value, or now()."))
      :json (refuse (:at o) json-key-here)
      (refuse (:at o) "A comparison here needs a column on one side and a value on the other.")))
  (when-not (column-operand? left)
    (refuse at "A comparison needs a column on one side."))
  (when (and (= :null (:kind right)) (#{"=" "!="} operator))
    (refuse (:at right) (str "`" operator " NULL` is never true in SQL. Write IS NULL or IS NOT NULL.")))
  (when (and (column-operand? right) (not (#{"=" "!="} operator)))
    (refuse at (str "Comparing two columns with " operator " isn't in Pine. Two columns compare with = or != only.")))
  {:col (resolve-column tables (:column left))
   :operator operator
   :cast (column-cast left)
   :value (cond
            (column-operand? right)
            (do (when (:cast right)
                  (refuse (:at right) "A cast on the right-hand column isn't in Pine. Cast the left one."))
                (keyword-column! #{"true" "false" "null"} (:column right))
                {:kind :column :col (resolve-column tables (:column right))})

            (computed-operand? right)
            ;; Rendered once the column references are known (render-value).
            (do (expr-pine {:tables tables :col-ref :name} right)
                {:kind :computed :expr right :tables tables :refs (set (map #(:t (resolve-column tables %)) (expr-columns right)))})

            :else (value right))})

(defn- compare-atom
  "A comparison as an atom: a column with a value or another column, or a
  function of a column with a value. A value on the left is moved right."
  [tables {:keys [operator left right] :as p}]
  (let [[left right operator] (if (or (and (not (column-operand? left)) (column-operand? right))
                                      (and (not (column-operand? left)) (not (left-function? left)) (left-function? right)))
                                [right left (mirrored operator)]
                                [left right operator])]
    (if (and (left-function? left) (not (column-operand? right)))
      (function-atom tables p left right)
      (compare-column-atom tables p left right operator))))

(defn- left-column [tables {:keys [left at]}]
  (when (= :json (:kind left))
    (refuse (:at left) json-key-here))
  (when-not (column-operand? left)
    (refuse at "This test needs a column on its left."))
  (resolve-column tables (:column left)))

(defn- atom-of
  "The Pine condition for one comparison, `negate`d if asked."
  [tables {:keys [op] :as p} negate]
  (case op
    :compare (let [a (compare-atom tables p)]
               (if-not negate
                 a
                 (if (and (= :boolean (get-in a [:value :kind])) (= "=" (:operator a)))
                   (update-in a [:value :value] not)
                   (update a :operator negated-op))))
    :like (let [v (value (:right p))]
            (when-not (#{:string :variable} (:kind v))
              (refuse (:at p) "LIKE needs a string on its right."))
            {:col (left-column tables p)
             :cast (column-cast (:left p))
             :operator (str (when (not= (:negated p) (boolean negate)) "not ") (:operator p))
             :value v})
    :is-null {:col (left-column tables p)
              :cast (column-cast (:left p))
              :operator (if (not= (:negated p) (boolean negate)) "is not null" "is null")}
    :in-set (cond-> {:col (left-column tables p)
                     :cast (column-cast (:left p))
                     :operator (if (not= (:negated p) (boolean negate)) "not in" "in")
                     :value {:kind :set :name (:set p)}}
              (:guard p) (assoc :guard true))
    :call-truthy (refuse (:at p) (str "Functions like " (get-in p [:call :fn]) "() aren't in Pine here."))
    :exists (refuse (:at p) "EXISTS converts only in WHERE.")
    :in (let [_ (when (:subquery p) (refuse (:at p) "A subquery here isn't in Pine. One converts after IN in WHERE."))
              vs (mapv value (:values p))]
          (when-not (every? #(#{:string :number} (:kind %)) vs)
            (refuse (:at p) "An IN list in Pine holds strings and numbers only."))
          {:col (left-column tables p)
           :cast (column-cast (:left p))
           :operator (if (not= (:negated p) (boolean negate)) "not in" "in")
           :value vs})
    :truthy {:col (resolve-column tables (:column p))
             :operator "="
             :value {:kind :boolean :value (not negate)}}))

(defn- distinct->predicate
  "IS DISTINCT FROM is never NULL. Against a value, x IS DISTINCT FROM v is
  (x != v OR x IS NULL), and IS NOT DISTINCT FROM is (x = v AND x IS NOT
  NULL): both are true or false for every x, NULL included, so NOT flips
  them exactly. Against NULL, they are IS NOT NULL and IS NULL."
  [{:keys [negated left right at]}]
  (cond
    (= :null (:kind right))
    {:op :is-null :negated (not negated) :left left :at at}

    (column-operand? right)
    (refuse at "IS DISTINCT FROM another column isn't in Pine. Compare with a value.")

    negated
    {:op :and :args [{:op :compare :operator "=" :left left :right right :at at}
                     {:op :is-null :negated true :left left :at at}]}

    :else
    {:op :or :args [{:op :compare :operator "!=" :left left :right right :at at}
                    {:op :is-null :negated false :left left :at at}]}))

(defn- between->predicate
  "x BETWEEN a AND b is x >= a AND x <= b. NOT BETWEEN is x < a OR x > b."
  [{:keys [negated left low high at]}]
  (let [cmp (fn [op v] {:op :compare :operator op :left left :right v :at at})]
    (if negated
      {:op :or :args [(cmp "<" low) (cmp ">" high)]}
      {:op :and :args [(cmp ">=" low) (cmp "<=" high)]})))

(defn- nnf
  "The predicate with every NOT pushed down onto a comparison, as a tree of
  {:and [...]}, {:or [...]} and atoms."
  [tables {:keys [op args arg] :as p} negate]
  (case op
    :and {(if negate :or :and) (mapv #(nnf tables % negate) args)}
    :or {(if negate :and :or) (mapv #(nnf tables % negate) args)}
    :not (nnf tables arg (not negate))
    :between (nnf tables (between->predicate p) negate)
    :is-distinct (nnf tables (distinct->predicate p) negate)
    {:atom (atom-of tables p negate)}))

(defn- cnf
  "A vector of clauses, each a vector of atoms to join with `or`. AND
  concatenates clauses; OR takes one clause from each side and joins them,
  for every combination."
  [node at]
  (cond
    (:atom node) [[(:atom node)]]
    (:and node) (vec (mapcat #(cnf % at) (:and node)))
    (:or node) (reduce (fn [acc clauses]
                         (let [combined (vec (for [a acc c clauses] (vec (distinct (into a c)))))]
                           (when (> (count combined) max-where-clauses)
                             (refuse at (str "This WHERE would become more than " max-where-clauses
                                             " where: steps in Pine. Simplify its mix of AND and OR.")))
                           combined))
                       [[]]
                       (map #(cnf % at) (:or node)))))

(defn- check-guards
  "A test on a LEFT JOIN that became `x in k` (semi-joins) is exact where a
  condition is only kept when true, unless it was negated and a NULL could
  make `not in` unknown. Then it is refused."
  [clauses at]
  (doseq [clause clauses, {:keys [guard operator]} clause
          :when (and guard (= "not in" operator))]
    (refuse at "This IS NULL test on a LEFT JOIN can't be said exactly in Pine without knowing the columns are NOT NULL. Convert with the connection."))
  clauses)

(defn- plan-where
  "Clauses for the WHERE and for any inner-join ON conditions moved into it."
  [tables predicates]
  (let [clauses (vec (mapcat (fn [p] (check-guards (cnf (nnf tables p false) (:at p)) (:at p))) predicates))]
    (when (> (count clauses) max-where-clauses)
      (refuse (:at (last predicates)) (str "This WHERE would become more than " max-where-clauses
                                           " where: steps in Pine. Simplify its mix of AND and OR.")))
    clauses))

;; ---------------
;; COMPUTED VALUES
;; ---------------
;;
;; COALESCE, ||, CASE, NOW(), a condition used as a value, and aggregates
;; become Pine computed values (docs/computed-values.md). A value is
;; rendered to Pine text here, against the tables in scope.

(def ^:private aggregates
  "SQL aggregate to Pine aggregate."
  {"min" "min" "max" "max" "sum" "sum" "avg" "avg" "bool_or" "any" "bool_and" "all" "every" "all"})

(defn- strip-at
  "A value without its offsets, to compare two values as written."
  [x]
  (walk/postwalk #(if (map? %) (dissoc % :at) %) x))

(defn- aggregate? [e]
  (and (map? e) (or (= :count (:kind e)) (and (= :call (:kind e)) (contains? aggregates (:fn e))))))

(defn- agg-key [e] (strip-at (dissoc e :as)))

(defn- find-nodes
  "Every map in `x` that `pred` is true of, not looking inside those."
  [pred x]
  (cond (and (map? x) (pred x)) [x]
        (map? x) (mapcat #(find-nodes pred %) (vals x))
        (sequential? x) (mapcat #(find-nodes pred %) x)
        :else []))

(defn- expr-columns
  "Every column a SQL value or predicate mentions, as column maps."
  [x]
  (concat (map :column (find-nodes #(#{:column :json} (:kind %)) x))
          (map :column (find-nodes #(= :truthy (:op %)) x))))

(defn- rewrite
  "`x` with each value or predicate node that `f` returns a node for
  replaced by it, looking inside the others."
  [f x]
  (or (f x)
      (let [r (partial rewrite f)]
        (cond
          (:op x) (cond-> x
                    (:args x) (update :args #(mapv r %))
                    (:arg x) (update :arg r)
                    (:left x) (update :left r)
                    (:right x) (update :right r)
                    (:low x) (update :low r)
                    (:high x) (update :high r)
                    (:values x) (update :values #(mapv r %))
                    (:call x) (update :call r))
          (:kind x) (cond-> x
                      (:args x) (update :args #(mapv r %))
                      (:filter x) (update :filter r)
                      (:pred x) (update :pred r)
                      (:whens x) (update :whens #(mapv (fn [[p v]] [(r p) (r v)]) %))
                      (:else x) (update :else r))
          :else x))))

(defn- unique-name [taken base]
  (if (taken base) (first (remove taken (map #(str base "_" %) (iterate inc 2)))) base))

(defn- make-col-ref
  "How a column is written: bare when it belongs to the table the pipe ends
  on, alias.column otherwise."
  [tables current]
  (let [alias-of (fn [t] (let [{:keys [alias table]} (nth tables t)] (or alias table)))]
    (fn [{:keys [t name]}] (if (= t current) name (str (alias-of t) "." name)))))

(declare render-atom quote-string concat-pine)

(defn- conditions-pine
  "A SQL predicate as Pine conditions joined with `or`. `value?` is true
  for a condition used as a true or false value. There a test that became
  `x in k` (semi-joins) and could be unknown becomes `if x in k then true
  else false`, which is never NULL, as the LEFT JOIN's test isn't."
  [{:keys [tables col-ref]} p value?]
  (let [clauses (cnf (nnf tables p false) (:at p))]
    (when (next clauses)
      (refuse (:at p) (cond
                        (seq (find-nodes #(= :between (:op %)) p))
                        "BETWEEN inside a value isn't in Pine: it is two conditions joined with AND, and the conditions of one value are joined with `or`."
                        (seq (find-nodes #(= :is-distinct (:op %)) p))
                        "IS [NOT] DISTINCT FROM inside a value isn't in Pine: it is two conditions joined with AND, and the conditions of one value are joined with `or`."
                        :else
                        "AND inside a value isn't in Pine. The conditions of one value are joined with `or`.")))
    (let [[clause] clauses
          guarded (some #(when (:guard %) %) clause)]
      (if (and value? guarded)
        (do (when (next clause)
              (refuse (:at p) "This test on a LEFT JOIN, next to others in one value, can't be said exactly in Pine. Convert with the connection."))
            (let [in? (= "in" (:operator guarded))]
              (str "if " (render-atom col-ref (assoc guarded :operator "in"))
                   " then " (if in? "true" "false") " else " (if in? "false" "true"))))
        (do (check-guards clauses (:at p))
            (s/join " or " (map #(render-atom col-ref %) clause)))))))

(defn- literal-pine [{:keys [kind value name cast at] :as e}]
  (when (and cast (not (dropped-value-casts cast)))
    (refuse at (str "A cast to " cast " isn't in Pine. Pine casts to text or uuid.")))
  (check-literal e)
  (case kind
    :string (quote-string value)
    :number value
    :boolean (str value)
    :variable (str "$" name)
    :null "null"))

(declare expr-pine)

(defn- if-text? [t] (s/starts-with? t "if "))

(defn- case-pine
  "CASE WHEN a THEN x WHEN b THEN y ELSE z END as `if a then x else if b
  then y else z`. An if right after `then` goes in parentheses."
  [ctx {:keys [whens else]}]
  (let [then-text (fn [v] (let [t (expr-pine ctx v)] (if (if-text? t) (str "(" t ")") t)))]
    (str (s/join " else " (map (fn [[p v]] (str "if " (conditions-pine ctx p false) " then " (then-text v))) whens))
         (when else (str " else " (expr-pine ctx else))))))

(defn- concat-args [e]
  (mapcat #(if (= :concat (:kind %)) (concat-args %) [%]) (:args e)))

(defn- dollar-path
  "MySQL's and SQLite's `->` take a path, `$.a.\"b c\"[0]`, as steps. Only
  keys and indexes: a wildcard or `last` isn't converted."
  [path at]
  (let [tokens (re-seq #"\.([A-Za-z_][A-Za-z0-9_]*)|\.\"((?:[^\"\\]|\\.)*)\"|\[([0-9]+)\]" (subs path 1))]
    (when-not (= (subs path 1) (apply str (map first tokens)))
      (refuse at (str "The JSON path '" path "' isn't converted. Pine follows keys and indexes, like '$.a.b[0]'.")))
    (mapv (fn [[_ k q i]] (cond k {:key k} q {:key (s/replace q #"\\(.)" "$1")} :else {:index i})) tokens)))

(defn- json-steps
  "The keys and indexes of t.data->'a'->0. A key that starts with `$` is a
  path on MySQL and SQLite, and a key on Postgres."
  [{:keys [steps at]}]
  (vec (mapcat (fn [{:keys [key] :as step}]
                 (if (and key (s/starts-with? key "$"))
                   (case *dialect*
                     (:mysql :sqlite) (dollar-path key at)
                     :postgres [step]
                     (refuse at (str "'" key "' is a path on MySQL and SQLite, and a key on Postgres. Convert with the connection, so the database is known.")))
                   [step]))
               steps)))

(defn- path-pine
  "A column's Pine name followed by JSON steps: data.a.b, data.tags[0].
  The first key is quoted after a bare name that is also a table's alias,
  which Pine would read as that table: data.'a'.b."
  [tables base steps at]
  (let [aliases (set (keep #(or (:alias %) (:table %)) tables))
        quote-first? (and (not (s/includes? base ".")) (aliases base))]
    (str base
         (apply str (map-indexed
                     (fn [i {:keys [key index]}]
                       (cond
                         index (if (re-matches #"[0-9]+" index)
                                 (str "[" index "]")
                                 (refuse at "A JSON index converts when it is a whole number, 0 or more."))
                         (and (re-matches #"[A-Za-z_][A-Za-z0-9_-]*" key) (not (and quote-first? (zero? i)))) (str "." key)
                         :else (str "." (quote-string key))))
                     steps)))))

(defn- json-path-pine
  "t.data->'a'->>'b' as Pine's data.a.b."
  [{:keys [tables col-ref]} {:keys [at] :as e}]
  (path-pine tables (col-ref (resolve-column tables (:column e))) (json-steps e) at))

(defn- json-text?
  "Whether a SQL value is a `->` key, or a CASE or COALESCE giving one:
  JSON in SQL, which Pine shows as text."
  [{:keys [kind] :as e}]
  (case kind
    :json (= "->" (:op (peek (:steps e))))
    :case (boolean (some json-text? (concat (map second (:whens e)) [(:else e)])))
    :call (and (= "coalesce" (:fn e)) (boolean (some json-text? (:args e))))
    false))

(defn- check-json-branches
  "A CASE or COALESCE outside a JSON function whose values mix a `->` key
  with another JSON value. Pine shows the key as text, and the other value
  as JSON, which Postgres can't put in one column."
  [{:keys [at] :as e} values]
  (when (and (some json-text? values)
             (some #(not (or (json-text? %) (#{:string :number :boolean :null :variable} (:kind %)))) values))
    (refuse at (str "A " (if (= :case (:kind e)) "CASE" "COALESCE") " that mixes a `->` key with another value isn't converted: "
                    "Pine shows the key as text. Use `->>`, or put it inside jsonb_typeof() or jsonb_array_length()."))))

(defn- expr-pine
  "A SQL value as a Pine computed value. `ctx` has :tables and :col-ref,
  and :agg, a function giving an aggregate's Pine text, where aggregates
  may appear. :json is true inside a function that reads JSON, where Pine
  reads a key as JSON, not text."
  [{:keys [tables col-ref agg] :as ctx} {:keys [kind at] :as e}]
  (when (and (:cast e) (#{:pred :case :concat :now} kind))
    (refuse at (str "A cast on " (case kind :pred "a condition" :case "CASE" :concat "a || b" :now "now()") " isn't converted.")))
  (when (and (:json ctx) (#{:call :concat :pred} kind) (not (json-functions (:fn e))) (seq (find-nodes #(= :json (:kind %)) e)))
    (refuse at "A JSON key inside this function isn't converted. Pine reads a key as JSON only directly inside jsonb_typeof() or jsonb_array_length(), or in a CASE there."))
  (case kind
    :json (do (when (and (not (:json ctx)) (= "->" (:op (peek (:steps e)))))
                (*warn!* (str "In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows "
                              (json-path-pine ctx e) " as text, without them, as `->>` does.")))
              (json-path-pine ctx e))
    :column (let [_ (keyword-column! keyword-values (:column e))
                  c (resolve-column tables (:column e))
                  cast (column-cast e)]
              (str (col-ref c) (when cast (str "::" cast))))
    (:string :number :boolean :variable :null) (literal-pine e)
    :special (special-value e)
    :now "now()"
    ;; `a || b` is NULL when either part is, and so is Pine's concat(). On
    ;; MySQL `||` is OR, unless the server is told otherwise.
    :concat (do (when (= :mysql *dialect*)
                  (refuse at "In MySQL, || means OR, unless the server sets PIPES_AS_CONCAT. Write CONCAT(a, b), which converts to Pine's concat()."))
                (concat-pine ctx (concat-args e)))
    :pred (conditions-pine ctx (:pred e) true)
    :case (do (when-not (:json ctx) (check-json-branches e (concat (map second (:whens e)) (some-> (:else e) vector))))
              (case-pine ctx e))
    :count (if agg (agg e) (refuse at "COUNT(*) here needs GROUP BY."))
    :call (let [{f :fn args :args cast :cast} e]
            (when (and (:filter e) (not (aggregate? e)))
              (refuse at (str "FILTER only goes with an aggregate, not " f "().")))
            (cond
              (aggregate? e)
              (if agg (agg e) (refuse at (str "An aggregate like " f "() needs GROUP BY.")))

              (= "coalesce" f)
              (do (when (< (count args) 2) (refuse at "COALESCE() needs two or more arguments."))
                  (check-json-branches e args)
                  (str "coalesce(" (s/join ", " (map #(expr-pine ctx %) args)) ")"
                       (when cast (str "::" (column-cast e)))))

              ;; MySQL's CONCAT() is NULL when any part is, as Pine's is.
              (and (= "concat" f) (= :mysql *dialect*))
              (do (when cast (refuse at "A cast on CONCAT() isn't converted."))
                  (concat-pine ctx args))

              (= "concat" f)
              (refuse at "CONCAT() skips NULL arguments on Postgres, and Pine's concat() doesn't. Write a || b: Pine's concat() matches that.")

              ;; Pine has these under the same names. Inside them, a key is
              ;; read as JSON, as -> gives it.
              (json-functions f)
              (do (when (not= 1 (count args)) (refuse at (str f "() takes one argument.")))
                  (str f "(" (expr-pine (assoc ctx :json true) (first args)) ")"
                       (when cast (str "::" (column-cast e)))))

              :else
              (refuse at (str "Functions like " f "() aren't in Pine."))))))

(defn- concat-pine
  "Pine's concat() of SQL values. It makes text of each part itself, so a
  cast to text on a part is left out: Pine's own SQL writes one on each."
  [ctx parts]
  (str "concat(" (s/join ", " (map #(expr-pine ctx (if (#{"text" "varchar" "character varying"} (:cast %)) (dissoc % :cast) %)) parts)) ")"))

(defn- default-agg-name
  "Pine's own default name for an aggregate: `min_id` for min(a.id)."
  [e]
  (if (= :count (:kind e))
    "count"
    (let [first-col (:name (first (expr-columns (:args e))))]
      (str (aggregates (:fn e)) (when first-col (str "_" first-col))))))

(defn- agg-pine
  "An aggregate as Pine, without its name: `id::text => min where type = 'x'`."
  [ctx e]
  (if (= :count (:kind e))
    (do (when (and (:column e) (= :and (:op (:filter e))))
          (refuse (:at e) "COUNT(column) with FILTER isn't in Pine. It needs the column to be NOT NULL and the FILTER to hold, and the conditions of a count's filter are joined with `or`."))
        (str "count" (when-let [p (:filter e)] (str " where " (conditions-pine (dissoc ctx :agg) p false)))))
    (let [{f :fn args :args filter-p :filter at :at} e]
      (when (not= 1 (count args))
        (refuse at (str (s/upper-case f) "() takes one argument in Pine.")))
      (when (and (= :call (:kind (first args))) (aggregate? (first args)))
        (refuse at "An aggregate inside another isn't in Pine."))
      ;; One value takes the arrow, `a.id => min`; a condition or a CASE
      ;; stays a call, `any(type = 'x')`, so it reads whole.
      (str (if (#{:pred :case} (:kind (first args)))
             (str (aggregates f) "(" (expr-pine (dissoc ctx :agg) (first args)) ")")
             (str (expr-pine (dissoc ctx :agg) (first args)) " => " (aggregates f)))
           (when filter-p (str " where " (conditions-pine (dissoc ctx :agg) filter-p false)))))))

;; -------
;; TABLES
;; -------

(defn- conjuncts [p]
  (if (= :and (:op p)) (mapcat conjuncts (:args p)) [p]))

(defn- join-pair
  "[anchor-table {:new column :anchor column}] when `p` is new.col =
  earlier.col, else nil."
  [tables i {:keys [op operator left right]}]
  (when (and (= :compare op) (= "=" operator)
             (column-operand? left) (column-operand? right))
    (let [l (assoc (resolve-column tables (:column left)) :cast (column-cast left))
          r (assoc (resolve-column tables (:column right)) :cast (column-cast right))
          [new old] (cond (and (= i (:t l)) (< (:t r) i)) [l r]
                          (and (= i (:t r)) (< (:t l) i)) [r l])]
      (when new
        ;; A cast stays with its column: `.resourceableId = .id ::text`.
        [(:t old) (cond-> {:new (:name new) :anchor (:name old)}
                    (:cast new) (assoc :new-cast (:cast new))
                    (:cast old) (assoc :anchor-cast (:cast old)))]))))

(defn- plan-join
  "{:anchor :pairs :moved [predicates]} for the i-th table's join."
  [tables i {:keys [type on using at]}]
  (let [in-scope (subvec tables 0 (inc i))]
    (if using
      (do (when (> i 1)
            (refuse at "USING after more than one table can't say which table it joins to. Write it with ON."))
          {:anchor 0
           :pairs (mapv (fn [{:keys [name quoted]}]
                          {:new (column-name (nth tables i) name quoted at)
                           :anchor (column-name (first tables) name quoted at)})
                        using)
           :moved []})
      (let [parts (map (fn [c] [c (join-pair in-scope i c)]) (conjuncts on))
            pairs (keep second parts)
            others (map first (remove second parts))
            anchors (distinct (map first pairs))]
        (when (empty? pairs)
          (refuse at (str "This join needs a column of `" (table-key (nth tables i)) "` = a column of an earlier table.")))
        (when (next anchors)
          (refuse at (str "This join ties `" (table-key (nth tables i)) "` to more than one table. Pine joins a table to one other.")))
        (when (and (seq others) type)
          (refuse (:at (first others))
                  (str "In a " (s/upper-case (name type)) " JOIN, an ON condition other than column = column isn't in Pine."
                       " Moved to WHERE it would mean something else.")))
        {:anchor (first anchors) :pairs (mapv second pairs) :moved (vec others)}))))

(defn- plan-tables
  "Tables in pipe order, each with its join, and the ON conditions an inner
  join moves to WHERE."
  [{:keys [from joins]}]
  (let [tables (mapv #(merge % (db-table %)) (cons from (map :table joins)))]
    (doseq [{:keys [table schema alias at]} tables]
      (pine-name! table at)
      (some-> schema (pine-name! at))
      (some-> alias (pine-name! at)))
    (when-let [dupe (some (fn [[k n]] (when (> n 1) k)) (frequencies (map table-key tables)))]
      (refuse (:at (last tables)) (str "Two tables are called `" dupe "`. Give one an alias.")))
    (let [planned (map-indexed (fn [i j] (assoc (plan-join tables (inc i) j) :type (:type j))) joins)]
      {:tables (vec (map-indexed (fn [i t] (merge t (when (pos? i) (nth planned (dec i))))) tables))
       :moved (vec (mapcat :moved planned))})))

;; -------
;; SELECT
;; -------

(defn- hidden-column?
  "A hidden key column Pine adds to its own SQL: \"c_0\".\"id\" AS
  \"__c_0__id\", named exactly after the alias and column it selects."
  [{:keys [kind as] {:keys [qualifier name]} :column}]
  (and (= :column kind) as qualifier (= as (str "__" qualifier "__" name))))

(defn- order-target
  "The select item an unqualified ORDER BY name refers to by its alias."
  [items {:keys [kind column]}]
  (when (and (= :column kind) (not (:qualifier column)))
    (first (filter #(= (:name column) (:as %)) items))))

(defn- sort-word
  "An ORDER BY item's direction as Pine writes it: `asc`, `desc nulls first`."
  [{d :direction n :nulls}]
  (str (if (= d :desc) "desc" "asc") (when n (str " nulls " (name n)))))

(defn- missing-function
  "The first function in `x` that Pine has no equivalent for, or nil."
  [x]
  (some (fn [{f :fn :as c}]
          (when-not (or (aggregate? c) (= "coalesce" f) (json-functions f) (and (= "concat" f) (= :mysql *dialect*))) f))
        (find-nodes #(= :call (:kind %)) x)))

(defn- selected-name
  "The name the select list gives the value `e`, when it selects it."
  [items e]
  (let [k (strip-at (dissoc e :at))]
    (some #(when (and (= :expr (:kind %)) (:as %) (= k (strip-at (:expr %)))) (:as %)) items)))

(defn- json-order-item
  "An order: item for ORDER BY a JSON key. `->` gives JSON, which SQL and
  Pine sort the same way. `->>` gives text, which SQL sorts as text and
  Pine as JSON, so 10 and '9' would swap."
  [tables {:keys [at] :as e} item]
  (when (= "->>" (:op (peek (:steps e))))
    (refuse at "ORDER BY a ->> key isn't converted: SQL sorts it as text, and Pine sorts a key as JSON, so numbers come out in another order. ORDER BY the -> key sorts the same in both."))
  {:json-col (resolve-column tables (:column e)) :steps (json-steps e) :at at :direction (sort-word item)})

(defn- order-items
  "order: items for ORDER BY items that name columns. `what` names where
  they are, for a refusal."
  [tables items order what]
  (mapv (fn [{:keys [target at] :as item}]
          (let [aliased (order-target items target)
                json-key (cond
                           (= :json (get-in target [:expr :kind])) (:expr target)
                           (= :json (get-in aliased [:expr :kind])) (:expr aliased))]
            (case (if json-key :json (:kind target))
              :json (json-order-item tables json-key item)
              :count (refuse at (str "ORDER BY COUNT(*) " what "needs GROUP BY."))
              :position (refuse at (str "ORDER BY a position " what "isn't in Pine. Name the column."))
              :expr (if-let [n (selected-name items (:expr target))]
                      {:name n :direction (sort-word item)}
                      (if-let [f (missing-function (:expr target))]
                        (refuse at (str "ORDER BY " f "(...) " what "isn't in Pine: " f "() isn't."))
                        (refuse at (str "ORDER BY an expression " what "isn't converted yet. Select it with a name, and sort by the name."))))
              :column (if (= :expr (:kind aliased))
                        ;; A computed value's name: Pine sorts by its value.
                        {:name (:as aliased) :direction (sort-word item)}
                        {:col (resolve-column tables (:column (or aliased target)))
                         :direction (sort-word item)}))))
        order))

(defn- window-ops
  "order: and limit: N per for a ROW_NUMBER() that a later query keeps the
  first N rows of (see plan-windows). `named` gives the name of the
  computed column an expression in PARTITION BY or ORDER BY is selected
  as (see plan-plain)."
  [tables {:keys [partition order at]} {:keys [n]} named warn!]
  (when-not n
    (refuse at "ROW_NUMBER() converts only when a later query keeps its first rows, with WHERE rn = 1 or rn <= N."))
  (when (empty? partition)
    (refuse at "ROW_NUMBER() without PARTITION BY numbers the whole result. Use ORDER BY and LIMIT."))
  (when (empty? order)
    (warn! "ROW_NUMBER() has no ORDER BY, so the SQL keeps any row of each group. Pine keeps the first in primary key order."))
  (cond-> []
    (seq order) (conj {:op :order
                       :items (vec (map-indexed (fn [i {:keys [target] :as item}]
                                                  (if (= :expr (:kind target))
                                                    {:name (named (:expr target) "order" (inc i)) :direction (sort-word item)}
                                                    (first (order-items tables [] [item] "inside ROW_NUMBER() "))))
                                                order))})
    true (conj {:op :limit :n n
                :per (vec (map-indexed (fn [i p]
                                         (if (:expr p)
                                           {:name (named (:expr p) "partition" (inc i))}
                                           (resolve-column tables p)))
                                       partition))})))

(defn- distinct-on-ops
  "SELECT DISTINCT ON (p) ... ORDER BY p, o keeps the first row of each p in
  that order: order: p, o | limit: 1 per p. Ordering by p too changes no
  group's first row, since p is the same within a group, and keeps the
  SQL's order of the rows."
  [tables items {:keys [distinct-on order limit]} warn!]
  (when (empty? order)
    (warn! "DISTINCT ON has no ORDER BY, so the SQL keeps any row of each group. Pine keeps the first in primary key order."))
  (let [at (:at (first distinct-on))
        per (mapv #(resolve-column tables %) distinct-on)
        sorted (order-items tables items order "")
        leading (set (map :col (take (count per) sorted)))]
    (when limit
      (refuse (:at limit) "LIMIT after DISTINCT ON isn't converted. Remove it."))
    (when (and (seq order) (not= leading (set per)))
      (refuse (:at (first order)) "With DISTINCT ON, ORDER BY must start with the DISTINCT ON columns."))
    (cond-> []
      (seq order) (conj {:op :order :items sorted})
      true (conj {:op :limit :n 1 :per per}))))

(defn- expr-refs
  "The tables a SQL value reads, by index."
  [tables e]
  (set (map #(:t (resolve-column tables %)) (expr-columns e))))

(defn- plan-plain [tables current items {:keys [order limit distinct-on] :as stmt} window warn!]
  (let [stars (fn [] (mapv (fn [t] {:star t}) (range (count tables))))
        row-number (first (filter #(= :row-number (:kind %)) items))
        expanded (vec (mapcat (fn [{:keys [kind qualifier column as at expr]}]
                                (case kind
                                  :star (stars)
                                  :alias-star [{:star (:t (resolve-column tables {:qualifier qualifier :name "x" :at at}))}]
                                  :row-number []
                                  :column (let [col (resolve-column tables column)]
                                            ;; select: true would read as the value.
                                            (keyword-column! #{"true" "false" "null"} column)
                                            [(cond-> {:col col}
                                               (and as (not= as (:name col))) (assoc :as (pine-name! as at)))])
                                  ;; A computed value. Pine needs its name.
                                  :expr (do (when-not as
                                              (refuse at "Name this value with AS, like `... AS name`: Pine's select: names every computed value."))
                                            [{:expr expr :as (pine-name! as at) :refs (expr-refs tables expr)}])))
                              items))
        ;; An expression in ROW_NUMBER()'s PARTITION BY or ORDER BY is a
        ;; computed column Pine ranks by name: the selected one with the
        ;; same value, or a new one.
        extras (atom [])
        ;; Columns a `*` brings in are taken too, when the schema says what they are.
        taken (set (concat (keep :as expanded) (keep (comp :name :col) expanded)
                           (mapcat #(when (contains? % :star) (:db-columns (nth tables (:star %)))) expanded)))
        named (fn [e what i]
                (let [k (strip-at e)]
                  (or (some #(when (and (:expr %) (= k (strip-at (:expr %)))) (:as %)) (concat expanded @extras))
                      (let [n (unique-name (into taken (map :as @extras)) (str (or (:as row-number) "rn") "_" what "_" i))]
                        ;; Said only where the column reaches a result: see
                        ;; transpile-statement.
                        (warn! (str "`" n "` is a column the SQL doesn't have. Pine ranks rows by named columns, so an expression in ROW_NUMBER()'s "
                                    (s/upper-case what) " BY is selected as one.")
                               :extra-column)
                        (swap! extras conj {:expr e :as n :refs (expr-refs tables e)})
                        n))))
        window-steps (when row-number (window-ops tables row-number window named warn!))
        expanded (into expanded @extras)]
    (when (and row-number (or (seq order) limit distinct-on))
      (refuse (:at row-number) "ROW_NUMBER() next to ORDER BY, LIMIT or DISTINCT ON in the same query isn't converted."))
    (when (empty? expanded)
      (refuse (:at (first items)) "Nothing is left to select."))
    (cond-> []
      (not= expanded [{:star current}])
      (conj {:op :select :items expanded})
      row-number
      (into window-steps)
      distinct-on
      (into (distinct-on-ops tables items stmt warn!))
      (and (seq order) (not distinct-on))
      (conj {:op :order :items (order-items tables items order "")})
      limit (conj {:op :limit :n (:value limit)}))))

(defn- plan-aggregated
  "GROUP BY, as steps whose result has exactly the SQL's columns, under
  its names:

    group: <grouped columns> with <each aggregate, named>
    | where: <HAVING, by the names>
    | order: <ORDER BY, when it sorts on the group's columns>
    | select: <the SQL's columns, by the names>, when they differ
    | order: <ORDER BY, when it sorts on the selected names>
    | limit: N

  The where:, order: and select: work on the group's result, which a
  checkpoint seals (docs/checkpoints.md), so there they name its columns
  bare. A group: with no aggregate adds a count column, which the select:
  then leaves out."
  [tables current items {:keys [group having order limit at]} warn!]
  (when-let [k (first (find-nodes #(= :json (:kind %)) [items having order]))]
    (refuse (:at k) "A JSON key with -> or ->> isn't converted in a query with GROUP BY."))
  (let [col-ref (make-col-ref tables current)
        ctx {:tables tables :col-ref col-ref}
        group-cols (mapv #(resolve-column tables %) group)
        _ (when-let [dupe (some (fn [[k n]] (when (> n 1) k)) (frequencies (map :name group-cols)))]
            (refuse (:at (first group)) (str "Two grouped columns are both called `" dupe "`. Pine names a grouped column by its name alone.")))
        group-name (into {} (map (fn [c] [(select-keys c [:t :name]) (:name c)]) group-cols))
        _ (doseq [{:keys [kind at]} items
                  :when (not (#{:column :count :expr} kind))]
            (refuse at "Only grouped columns, aggregates and values over them can be selected with GROUP BY."))
        order-targets (map :target order)
        _ (when-let [c (first (filter #(missing-function [%]) (find-nodes #(= :call (:kind %)) [items having order-targets])))]
            (refuse (:at c) (str "Functions like " (:fn c) "() aren't in Pine.")))
        ;; An aggregate, without a cast after it or a name.
        base #(dissoc % :cast :as)
        akey #(strip-at (base %))
        aggs (reduce (fn [acc e] (if (some #(= (akey e) (akey %)) acc) acc (conj acc (base e))))
                     [] (find-nodes aggregate? [items having order-targets]))
        ;; An aggregate selected on its own takes the SQL's name for it: its
        ;; AS, or else the function's name, as Postgres names it. The others
        ;; get Pine's default, made unique.
        direct-item? (fn [{:keys [kind expr] :as item}]
                       (or (= :count kind) (and (= :expr kind) (aggregate? expr) (not (:cast expr)))))
        item-agg (fn [{:keys [kind expr] :as item}] (if (= :count kind) item expr))
        sql-name (fn [{:keys [as] :as item}]
                   (or as (let [e (item-agg item)] (if (= :count (:kind e)) "count" (:fn e)))))
        explicit (keep (fn [i] (when (and (direct-item? i) (:as i)) (:as i))) items)
        _ (when-let [dupe (some (fn [[k n]] (when (> n 1) k))
                                (frequencies (concat explicit (keep #(when (= :column (:kind %)) (:as %)) items)
                                                     (keep #(when (and (= :expr (:kind %)) (not (direct-item? %))) (:as %)) items))))]
            (refuse at (str "Two selected columns are both called `" dupe "`. Pine needs each name once.")))
        taken (set (concat (map :name group-cols) (keep :as items)))
        agg-names (reduce (fn [m item]
                            (let [k (akey (item-agg item))]
                              (if (contains? m k)
                                m
                                (let [wanted (sql-name item)
                                      n (if (:as item) wanted (unique-name (into taken (vals m)) wanted))]
                                  (when (not= n wanted)
                                    (warn! (str "`" n "` is the SQL's column `" wanted "`: Pine needs each name once.") :columns))
                                  (assoc m k n)))))
                          {} (filter direct-item? items))
        agg-names (reduce (fn [m e]
                            (let [k (akey e)]
                              (if (contains? m k) m (assoc m k (unique-name (into taken (vals m)) (default-agg-name e))))))
                          agg-names aggs)
        agg-name #(get agg-names (akey %))
        group-text (str "group: " (s/join ", " (map col-ref group-cols))
                        (when (seq aggs)
                          (str " with " (s/join ", " (map #(if (= "count" (agg-name %) (default-agg-name %) (agg-pine ctx %))
                                                         ;; A plain count is already called count.
                                                             (agg-pine ctx %)
                                                             (str (agg-pine ctx %) " as " (agg-name %)))
                                                          aggs)))))
        refs (into (set (map :t group-cols)) (mapcat #(expr-refs tables %) aggs))
        ;; After the group:, a column is the result's column by its name.
        ;; With no aggregate, Pine adds a count.
        result-columns (vec (concat (map :name group-cols) (if (seq aggs) (map agg-name aggs) ["count"])))
        pseudo [{:table "result" :db-columns result-columns}]
        pseudo-ctx {:tables pseudo :col-ref :name}
        sealed (fn [x]
                 (rewrite (fn [n]
                            (cond
                              (aggregate? n) (cond-> {:kind :column :column {:name (agg-name n) :at (:at n)} :at (:at n)}
                                               (:cast n) (assoc :cast (:cast n)))
                              (and (= :call-truthy (:op n)) (aggregate? (:call n)))
                              {:op :truthy :column {:name (agg-name (:call n)) :at (:at n)} :at (:at n)}
                              (= :column (:kind n))
                              (let [c (select-keys (resolve-column tables (:column n)) [:t :name])]
                                (if-let [g (group-name c)]
                                  (assoc n :column {:name g :at (:at n)})
                                  (refuse (:at n) (str "`" (:name (:column n)) "` is neither grouped nor inside an aggregate."))))
                              (= :truthy (:op n))
                              (let [c (select-keys (resolve-column tables (:column n)) [:t :name])]
                                (assoc n :column {:name (or (group-name c) (refuse (:at n) (str "`" (:name (:column n)) "` isn't grouped."))) :at (:at n)}))))
                          x))
        having-ops (when having
                     (for [clause (plan-where pseudo [(sealed having)])]
                       {:op :text :text (str "where: " (s/join " or " (map #(render-atom :name %) clause)))}))
        ;; The SQL's columns, in its order, under its names.
        outputs (mapv (fn [{:keys [kind column as expr at] :as item}]
                        (cond
                          (= :column kind)
                          (let [c (select-keys (resolve-column tables column) [:t :name])
                                g (or (group-name c) (refuse at (str "`" (:name column) "` is selected but not grouped.")))]
                            {:text (str g (when (and as (not= as g)) (str " as " (pine-name! as at)))) :name (or as g) :group g})

                          (direct-item? item)
                          (let [n (agg-name (item-agg item))] {:text n :name n :agg n})

                          ;; An aggregate with a cast after it: the cast goes in the select:.
                          (aggregate? expr)
                          (let [n (pine-name! (or as (:fn expr)) at)]
                            {:text (str (expr-pine pseudo-ctx (sealed expr)) " as " n) :name n})

                          :else
                          (do (when-not as
                                (refuse at "Name this value with AS, like `... AS name`: Pine's select: names every computed value."))
                              {:text (str (expr-pine pseudo-ctx (sealed expr)) " as " (pine-name! as at)) :name as})))
                      items)
        names (mapv :name outputs)
        select-needed? (not= (map :text outputs) result-columns)
        ;; Each ORDER BY item as a name before the select: (the group's
        ;; columns) and after it (the SQL's names), where it has one.
        order-names (for [{:keys [target at] :as item} order]
                      (let [output-of (fn [k v] (some #(when (= v (k %)) (:name %)) outputs))
                            [pre post]
                            (case (:kind target)
                              :count (let [n (agg-name target)] [n (output-of :agg n)])
                              :expr (if (and (aggregate? (:expr target)) (not (:cast (:expr target))))
                                      (let [n (agg-name (:expr target))] [n (output-of :agg n)])
                                      [nil (or (selected-name items (:expr target))
                                               (refuse at "ORDER BY an expression after GROUP BY isn't converted. Select it with a name, and sort by the name."))])
                              :column (let [col (:column target)
                                            by-alias (when-not (:qualifier col) (some #(when (= (:name col) (:name %)) %) outputs))]
                                        (if by-alias
                                          [(or (:group by-alias) (:agg by-alias)) (:name by-alias)]
                                          (let [c (select-keys (resolve-column tables col) [:t :name])
                                                g (or (group-name c) (refuse at (str "`" (:name col) "` isn't grouped, so it can't be sorted on after GROUP BY.")))]
                                            [g (output-of :group g)])))
                              :position (refuse at "ORDER BY a position isn't in Pine. Name the column."))]
                        {:pre pre :post post :direction (sort-word item) :at at}))
        order-place (cond
                      (empty? order-names) nil
                      (not select-needed?) :pre
                      (every? :post order-names) :post
                      (every? :pre order-names) :pre
                      :else (refuse (:at (first order)) "This ORDER BY sorts on a value the SELECT names and on a column it leaves out. That isn't converted."))
        order-op (when order-place
                   {:op :order :items (mapv (fn [o] {:name (get o order-place) :direction (:direction o)}) order-names)})]
    (cond-> [{:op :text :text group-text :refs refs}]
      true (into having-ops)
      (= :pre order-place) (conj order-op)
      select-needed? (conj {:op :text :text (str "select: " (s/join ", " (map :text outputs)))})
      (= :post order-place) (conj order-op)
      limit (conj {:op :limit :n (:value limit)})
      true (conj {:op :columns :columns names}))))

(defn- plan-select [tables current {:keys [select group order limit distinct distinct-on having] :as stmt} warn! {:keys [window in-set-only]}]
  (let [hidden (filter hidden-column? select)
        items (vec (remove hidden-column? select))]
    (when (seq hidden)
      (warn! (str "Left out " (s/join ", " (map :as hidden)) ": Pine adds its hidden columns itself.") :columns))
    (when (empty? items)
      (refuse (:at (first select)) "Nothing is left to select."))
    (when (and distinct (not in-set-only))
      (refuse (:at distinct) "DISTINCT isn't in Pine. (group: isn't the same: it adds a count.)"))
    ;; Used only after `in`, DISTINCT changes nothing: the set has the same
    ;; values. With a LIMIT, it does: the LIMIT counts distinct values.
    (when (and distinct limit)
      (refuse (:at distinct) "DISTINCT with LIMIT isn't in Pine: the LIMIT counts distinct values, and Pine has no DISTINCT."))
    (when (and limit (> (:value limit) pine/max-limit))
      (refuse (:at limit) (str "LIMIT " (:value limit) " is more than Pine's " pine/max-limit ".")))
    (when-let [r (and (or (seq group) (some #(= :count (:kind %)) items))
                      (first (filter #(= :row-number (:kind %)) items)))]
      (refuse (:at r) "ROW_NUMBER() next to GROUP BY or COUNT(*) isn't converted."))
    (when (and distinct-on (or (seq group) (some #(= :count (:kind %)) items)))
      (refuse (:at (first distinct-on)) "DISTINCT ON next to GROUP BY or COUNT(*) isn't converted."))
    (when (and having (empty? group))
      (refuse (:at stmt) "HAVING without GROUP BY isn't converted."))
    (cond
      (seq group)
      (plan-aggregated tables current items stmt warn!)

      (some #(= :count (:kind %)) items)
      (do (when-let [f (first (filter #(and (:filter %) (not (:column %))) items))]
            (refuse (:at f) "COUNT(*) FILTER needs GROUP BY. Without one, put the condition in WHERE."))
          (when-let [c (first (filter #(and (= :count (:kind %)) (:column %)) items))]
            (when (or (next items) (= :and (:op (:filter c))))
              (refuse (:at c) "Without GROUP BY, COUNT(column) converts only on its own, with no FILTER.")))
          (when (next items)
            (refuse (:at (first (remove #(= :count (:kind %)) items))) "A column next to COUNT(*) needs GROUP BY."))
          (when (seq order) (refuse (:at (first order)) "ORDER BY on a single COUNT(*) isn't converted. Remove it."))
          (when limit (refuse (:at limit) "LIMIT on a single COUNT(*) isn't converted. Remove it."))
          (when (and (:as (first items)) (not= "count" (:as (first items))))
            (warn! (str "COUNT(*) AS " (:as (first items)) ": Pine names the count column `count`.") :columns))
          ;; COUNT(x) on its own counts the rows where x isn't NULL.
          (conj (mapv (fn [c] {:op :where :clause c}) (some->> (:filter (first items)) vector (plan-where tables)))
                {:op :count}))

      :else
      (plan-plain tables current items stmt window warn!))))

(defn- output-columns
  "The names of the columns a query returns, or nil when they aren't known:
  a `*` over a table whose columns aren't known."
  [tables ops]
  (let [table-columns (fn [t] (:db-columns (nth tables t)))]
    (if-let [sel (or (first (filter #(= :columns (:op %)) ops)) (first (filter #(= :select (:op %)) ops)))]
      (let [cols (if (= :columns (:op sel))
                   (map vector (:columns sel))
                   (map (fn [item]
                          (if (contains? item :star)
                            (table-columns (:star item))
                            [(or (:as item) (:name (:col item)))]))
                        (:items sel)))]
        (when (every? some? cols) (vec (apply concat cols))))
      (cond
        (some #(= :count (:op %)) ops) ["count"]
        (some #(= :group (:op %)) ops) (conj (mapv :name (:cols (first (filter #(= :group (:op %)) ops)))) "count")
        :else (some-> (table-columns (dec (count tables))) vec)))))

;; -------
;; RENDER
;; -------

(defn- quote-string [v]
  (str "'" (s/replace v "'" "''") "'"))

(defn- render-value [{:keys [kind value name] :as v} col-ref]
  (case kind
    :computed (expr-pine {:tables (:tables v) :col-ref col-ref} (:expr v))
    :string (quote-string value)
    :number value
    :boolean (str value)
    :variable (str "$" name)
    :now "now()"
    :column (col-ref value)))

(defn- render-atom [col-ref {:keys [col operator value cast] :as a}]
  (str (if-let [e (:expr a)] (expr-pine {:tables (:tables a) :col-ref col-ref} e) (col-ref col)) " " operator
       (cond
         (#{"is null" "is not null"} operator) ""
         (= :set (:kind value)) (str " " (:name value))
         (#{"in" "not in"} operator) (str " (" (s/join ", " (map #(render-value % col-ref) value)) ")")
         (= :column (:kind value)) (str " " (col-ref (:col value)))
         :else (str " " (render-value value col-ref)))
       (when cast (str " ::" cast))))

(defn- references
  "Every table index the operations refer to by alias."
  [ops current]
  (let [cols (fn [c] (when (and (:t c) (not= current (:t c))) [(:t c)]))]
    (set (mapcat (fn [{:keys [op t clause items cols-of] :as o}]
                   (case op
                     :from [t]
                     :text (remove #{current} (:refs o))
                     :where (mapcat (fn [{:keys [col value] :as a}]
                                      (concat (cols col)
                                              (when (:expr a) (remove #{current} (:refs a)))
                                              (when (= :column (:kind value)) (cols (:col value)))
                                              (when (= :computed (:kind value)) (remove #{current} (:refs value)))))
                                    clause)
                     :select (mapcat #(cond (contains? % :star) [(:star %)]
                                            (:expr %) (remove #{current} (:refs %))
                                            :else (cols (:col %)))
                                     items)
                     :group (mapcat cols (:cols o))
                     :order (mapcat #(some-> (or (:col %) (:json-col %)) cols) items)
                     :limit (mapcat cols (:per o))
                     nil))
                 ops))))

(defn- render [tables ops current]
  (let [referenced (references ops current)
        alias-of (fn [t] (let [{:keys [alias table]} (nth tables t)] (or alias table)))
        col-ref (make-col-ref tables current)
        ctx {:tables tables :col-ref col-ref}]
    (s/join
     " | "
     (for [{:keys [op t n clause items cols] :as o} ops
           :when (not= :columns op)]
       (case op
         :table (let [{:keys [schema table db-schema db-table alias type pairs]} (nth tables t)
                      schema (or db-schema schema)]
                  (str (when schema (str schema "."))
                       (or db-table table)
                       (when (or alias (referenced t)) (str " as " (alias-of t)))
                       (when type (str " :" (name type)))
                       (when (seq pairs)
                         (str " " (s/join ", " (map #(str "." (:new %) (when (:new-cast %) (str " ::" (:new-cast %)))
                                                          " = ." (:anchor %) (when (:anchor-cast %) (str " ::" (:anchor-cast %))))
                                                    pairs))))))
         :text (:text o)
         :from (str "from: " (alias-of t))
         :where (str "where: " (s/join " or " (map #(render-atom col-ref %) clause)))
         :select (str "select: " (s/join ", " (map #(cond
                                                      (contains? % :star) (str (alias-of (:star %)) ".*")
                                                      (:expr %) (str (expr-pine ctx (:expr %)) " as " (:as %))
                                                      :else (str (col-ref (:col %)) (when (:as %) (str " as " (:as %)))))
                                                   items)))
         :group (str "group: " (s/join ", " (map col-ref cols)) " with count")
         :count "count:"
         :order (str "order: " (s/join ", " (map #(str (cond
                                                         (:col %) (col-ref (:col %))
                                                         (:json-col %) (path-pine tables (col-ref (:json-col %)) (:steps %) (:at %))
                                                         :else (:name %))
                                                       " " (:direction %))
                                                 items)))
         :limit (str "limit: " n (when (seq (:per o)) (str " per " (s/join ", " (map #(if (:t %) (col-ref %) (:name %)) (:per o))))))
         :update (str "update! " (s/join ", " (map (fn [{:keys [column value]}]
                                                     (str column " = " (case (:kind value)
                                                                         :null "null"
                                                                         ;; update! reads the changed table's columns bare.
                                                                         :computed (render-value value :name)
                                                                         (render-value value col-ref))))
                                                   (:assignments o))))
         :delete (str "delete! " (s/join ", " (map #(str "." %) cols))))))))

;; -----
;; MAIN
;; -----

(defn- table-ops
  "The table and from: steps, and the table the pipe ends on."
  [tables]
  (reduce (fn [{:keys [ops current]} i]
            (let [anchor (:anchor (nth tables i))]
              {:ops (cond-> ops
                      (and (pos? i) (not= anchor current)) (conj {:op :from :t anchor})
                      true (conj {:op :table :t i}))
               :current i}))
          {:ops [] :current 0}
          (range (count tables))))

(defn- assignment-value
  "The value an UPDATE's SET writes: a literal, NULL, now(), COALESCE(...)
  or a || b of the changed table's columns. Another column on its own isn't
  converted."
  [tables {:keys [kind at] :as operand}]
  (cond
    (= :null kind) {:kind :null}
    (= :now kind) {:kind :now}
    (= :column kind) (refuse at "update! here takes a value. Setting a column from another column isn't converted.")
    (computed-operand? operand)
    (do (doseq [c (expr-columns operand)
                :when (pos? (:t (resolve-column tables c)))]
          (refuse (:at c) (str "update! can only read the columns of the table it changes, not `" (:qualifier c) "." (:name c) "`.")))
        (expr-pine {:tables tables :col-ref :name} operand)
        {:kind :computed :expr operand :tables tables})
    (#{:call :concat :case :pred :count} kind)
    (refuse at (str (if (= :call kind) (str (:fn operand) "() isn't in Pine's update!.") "This value isn't in Pine's update!.")
                    " update! writes a value, coalesce(...), a || b, or now()."))
    :else (value operand)))

(defn- action-ops
  "The steps that end an UPDATE or DELETE: `from:` back to the table it
  changes when the pipe ended elsewhere, then update! or delete!."
  [tables current {:keys [kind at] assignments :set} where-count]
  (let [target (first tables)
        what (if (= kind :update) "UPDATE" "DELETE")]
    (when (:cte target)
      (refuse at (str "An " what " of a WITH query isn't in Pine.")))
    (when (zero? where-count)
      (refuse at (str "This " what " has no condition of its own, so it would change every row it reaches."
                      " Pine refuses that: add a WHERE.")))
    ;; update! finds rows by the primary key: a table with none is refused.
    (when (and (= kind :update) *schema* (not (:cte target))
               (empty? (refs/primary-key *schema* (:db-schema target) (or (:db-table target) (:table target)))))
      (refuse at (str "`" (:table target) "` has no primary key, so update! can't name its rows.")))
    (into (if (zero? current) [] [{:op :from :t 0}])
          (case kind
            :update
            [{:op :update
              :assignments (mapv (fn [{:keys [column value at]}]
                                   (when (and (:qualifier column) (not= (:qualifier column) (table-key target)))
                                     (refuse at (str "`" (:qualifier column) "." (:name column) "` isn't a column of the table the UPDATE changes.")))
                                   {:column (column-name target (:name column) (:quoted column) at)
                                    :value (assignment-value tables value)})
                                 assignments)}]
            :delete
            (let [pk (when *schema* (refs/primary-key *schema* (:db-schema target) (or (:db-table target) (:table target))))]
              (cond
                (not *schema*)
                (refuse at "Converting a DELETE needs the connection, to find the table's primary key: delete! names the rows by it.")
                (empty? pk)
                (refuse at (str "`" (:table target) "` has no primary key, so delete! can't name its rows."))
                :else
                [{:op :delete :cols (vec pk)}]))))))

(defn- end-on-selected-table
  "Pine shows every column of the table its pipe ends on, unless select:
  names one of them. A SQL select list of only earlier tables' columns
  would then come back with all of the last table's too. So such a pipe
  ends with `from:` the first selected table, which leaves the last
  table's columns out. At the end, so no step after it reads its bare
  column names differently."
  [ops current]
  (let [sel (first (filter #(= :select (:op %)) ops))
        selected (when sel
                   (mapcat (fn [item]
                             (cond
                               (contains? item :star) [(:star item)]
                               (:expr item) (:refs item)
                               :else [(:t (:col item))]))
                           (:items sel)))]
    (if (and (seq selected) (not-any? #{current} selected))
      (conj ops {:op :from :t (first selected)})
      ops)))

(defn- transpile-query
  "One query as one Pine block: {:expression :columns}. A named one, a WITH
  query, ends in `|= name`. A warning that changes a named query's columns
  is a refusal there, since a later query may depend on them."
  [{:keys [name query in-set-only window at]} warn!]
  (let [local (atom [])
        warn-q! (fn ([m] (swap! local conj [m nil])) ([m kind] (swap! local conj [m kind])))
        {:keys [tables moved]} (plan-tables query)
        {:keys [ops current]} (table-ops tables)
        _ (when (seq moved)
            (warn-q! "A condition in an inner join's ON moved to where:. It means the same there."))
        clauses (plan-where tables (cond-> moved (:where query) (conj (:where query))))
        ops (into ops (map (fn [c] {:op :where :clause c}) clauses))
        ops (if-let [action (:action query)]
              (into ops (action-ops tables current action (count clauses)))
              (end-on-selected-table
               (into ops (plan-select tables current query warn-q! {:window window :in-set-only in-set-only}))
               current))]
    (when name
      (when-let [[m] (first (filter #(= :columns (second %)) @local))]
        (refuse at (str "In the WITH query `" name "`: " m " Its columns would then differ from the SQL's."))))
    (doseq [[m kind] @local :when (not= :extra-column kind)] (warn! m))
    {:expression (render tables ops current)
     :columns (when-not (:action query) (output-columns tables ops))
     :extra-warnings (vec (keep (fn [[m kind]] (when (= :extra-column kind) m)) @local))}))

;; ----------
;; STATEMENTS
;; ----------
;;
;; A statement becomes a list of entries, {:name :query}, one Pine block
;; each: its WITH queries, then the subqueries it uses, then itself (with no
;; name). Before they are rendered, these passes rewrite them:
;;
;; - flatten: a subquery in FROM or JOIN becomes an entry named by its
;;   alias. A subquery after IN becomes an entry, and the condition `x in
;;   <entry>`, Pine's in with a named result.
;; - narrow-in-sets: `IN (SELECT c FROM cte)`, when that is all the WITH
;;   query is used for, uses the WITH query itself, selecting just c.
;; - anti-joins: `LEFT JOIN k ON k.c = t.x ... WHERE k.c IS NULL` becomes
;;   `x not in k`.
;; - plan-windows: a ROW_NUMBER() query whose users keep `rn <= N` becomes
;;   `limit: N per`.

(defn- and-of [ps]
  (case (count ps) 0 nil 1 (first ps) {:op :and :args (vec ps)}))

(defn- query-tables [q]
  (vec (cons (:from q) (map :table (:joins q)))))

(defn- fresh-name
  "`base`, or `base_2`, `base_3`, ..., whichever is first not taken, nor a
  table in the connection's schema."
  [taken base]
  (let [taken? #(or (taken %) (contains? (:table *schema*) %) (*sql-tables* (s/lower-case %)))]
    (if (taken? base) (first (remove taken? (map #(str base "_" %) (iterate inc 2)))) base)))

(declare flatten-query query-columns predicate-columns)

(defn- outer-columns!
  "With the schema, refuses a bare column in a subquery after IN that none
  of its tables has. In SQL it is the outer query's column; in Pine it would
  be read as the subquery's own."
  [sq]
  (when *schema*
    (let [ts (map #(merge % (db-table %)) (query-tables sq))]
      (when (every? #(and (:db-table %) (seq (:db-columns %))) ts)
        (doseq [c (concat (query-columns sq) (some-> (:where sq) predicate-columns))
                :when (and (:name c) (nil? (:qualifier c)) (not (some #{(:name c)} (keep :as (:select sq)))))
                :when (not-any? (fn [t] (some #(if (:quoted c) (= (:name c) %) (.equalsIgnoreCase ^String (:name c) ^String %)) (:db-columns t))) ts)]
          (refuse (:at c) (str "`" (:name c) "` isn't a column of the subquery's " (if (next ts) "tables" (str "table `" (table-key (first ts)) "`"))
                               ". A subquery that reads a column of the outer query isn't in Pine.")))))))

(defn- flatten-table
  "A subquery in FROM or JOIN becomes an entry named by its alias."
  [state tref]
  (if-let [d (:derived tref)]
    (let [n (pine-name! (:alias tref) (:at tref))
          _ (when ((:taken state) n)
              (refuse (:at tref) (str "`" n "` names two queries. Give the subquery another alias.")))
          [state q] (flatten-query (update state :taken conj n) d)]
      [(update state :entries conj {:name n :query q :at (:at tref)})
       {:table n :at (:at tref)}])
    [state tref]))

(defn- exists->in
  "EXISTS (SELECT ... FROM k WHERE k.c = t.x AND ...) as x IN (SELECT k.c
  FROM k WHERE ...), marked :exists. The subquery must refer to the outer
  query through that one equality and nowhere else. A column qualified by
  none of the subquery's tables is the outer query's."
  [{sq :subquery at :at}]
  (when-let [e (first (find-nodes #(= :exists (:op %)) (:where sq)))]
    (refuse (:at e) "An EXISTS inside another EXISTS isn't converted."))
  (let [inner (set (map table-key (query-tables sq)))
        outer? (fn [c] (boolean (and (:qualifier c) (not (inner (:qualifier c))))))
        refers? (fn [x] (some outer? (expr-columns x)))
        parts (some-> (:where sq) conjuncts)
        [pair & more] (filter refers? parts)
        {:keys [op operator left right]} pair
        [k-col t-col] (when (and (= :compare op) (= "=" operator)
                                 (= :column (:kind left)) (= :column (:kind right))
                                 (not (:cast left)) (not (:cast right)))
                        (let [[l r] [(:column left) (:column right)]]
                          (cond (and (outer? r) (not (outer? l))) [l r]
                                (and (outer? l) (not (outer? r))) [r l])))
        tied "EXISTS converts when its subquery is tied to the outer query by one equality, such as `r.user_id = u.id`, joined to the rest of its WHERE with AND."
        sq (-> sq
               (assoc :where (and-of (remove #{pair} parts))
                      :select [{:kind :column :column k-col :at (:at k-col)}])
               (dissoc :order :limit :distinct))]
    (cond
      (nil? pair) (refuse at (str tied " This one doesn't refer to the outer query."))
      (or more (nil? k-col)) (refuse at tied)
      (some outer? (concat (query-columns sq) (some-> (:where sq) predicate-columns)))
      (refuse at (str tied " This one also refers to it elsewhere."))
      (or (seq (:group sq)) (:having sq) (:distinct-on sq))
      (refuse at "EXISTS with GROUP BY, HAVING or DISTINCT ON in its subquery isn't converted.")
      (some-> (:limit sq) :value zero?)
      (refuse at "EXISTS with LIMIT 0 is always false."))
    {:op :in :negated false :exists true :at at
     :left {:kind :column :column t-col :at (:at t-col)}
     :subquery sq}))

(defn- flatten-predicate
  "A subquery after IN becomes an entry, and the condition `x in <entry>`.
  EXISTS is first an IN (exists->in)."
  [state {:keys [op] :as p}]
  (case op
    :exists (flatten-predicate state (exists->in p))
    (:and :or) (let [[state args] (reduce (fn [[st acc] a] (let [[st a] (flatten-predicate st a)] [st (conj acc a)]))
                                          [state []] (:args p))]
                 [state (assoc p :args args)])
    :not (let [[state a] (flatten-predicate state (:arg p))] [state (assoc p :arg a)])
    :in (if-let [sq (:subquery p)]
          (let [_ (outer-columns! sq)
                [state sq] (flatten-query state sq)
                [item & more] (:select sq)
                _ (when (or more (not= :column (:kind item)))
                    (refuse (:at p) "A subquery after IN must select one column."))
                src (:from sq)
                col (:column item)
                out (or (:as item) (:name col))
                n (pine-name! (fresh-name (:taken state) (str (:table src) "_" out "s")) (:at p))
                simple? (and (empty? (:joins sq)) (nil? (:where sq)) (empty? (:group sq)) (nil? (:order sq))
                             (nil? (:limit sq)) (nil? (:distinct-on sq)) (nil? (:schema src)) (nil? (:as item))
                             (or (nil? (:qualifier col)) (= (:qualifier col) (table-key src))))]
            [(-> state
                 (update :taken conj n)
                 (update :entries conj (cond-> {:name n :query sq :in-set-only true :at (:at p)}
                                         simple? (assoc :simple {:src (:table src) :col (:name col)}))))
             (cond-> {:op :in-set :negated (:negated p) :left (:left p) :set n :at (:at p)}
               (:exists p) (assoc :exists true))])
          [state p])
    [state p]))

(defn- flatten-select
  "A subquery after IN in a selected value, `(a IN (SELECT ...)) AS k`,
  becomes an entry too, and the value `a in <entry>`."
  [state items]
  (when-let [e (first (find-nodes #(= :exists (:op %)) items))]
    (refuse (:at e) "EXISTS converts only in WHERE."))
  (let [st (atom state)
        items (walk/prewalk (fn [n]
                              (if (and (map? n) (= :in (:op n)) (:subquery n))
                                (let [[s2 p] (flatten-predicate @st n)] (reset! st s2) p)
                                n))
                            items)]
    [@st items]))

(defn- positions
  "GROUP BY 1 and ORDER BY 1 as what they number in the SELECT. GROUP BY
  takes the selected column, and ORDER BY sorts by the item's name, or by
  the item itself when it has none."
  [{:keys [select] :as q}]
  (let [item (fn [n at clause]
               (let [i (dec n)]
                 (when-not (< -1 i (count select))
                   (refuse at (str clause " " n ": the SELECT has only " (count select) (if (= 1 (count select)) " column." " columns."))))
                 (when (some #(#{:star :alias-star} (:kind %)) (take (inc i) select))
                   (refuse at (str clause " " n " counts past a `*`, whose columns aren't known here. Name the column.")))
                 (nth select i)))
        group-col (fn [{:keys [position at] :as g}]
                    (if-not position
                      g
                      (let [{:keys [kind column]} (item position at "GROUP BY")]
                        (if (= :column kind)
                          (assoc column :at at)
                          (refuse at (str "GROUP BY " position " numbers a value, not a column. Only a column is grouped in Pine."))))))
        order-target (fn [{:keys [target at] :as o}]
                       (if-not (= :position (:kind target))
                         o
                         (let [n (Long/parseLong (:value target))
                               {:keys [kind as column expr] :as it} (item n at "ORDER BY")]
                           (assoc o :target
                                  (cond
                                    as {:kind :column :column {:name as :at at}}
                                    (= :column kind) {:kind :column :column column}
                                    (= :count kind) (dissoc it :as)
                                    (= :expr kind) {:kind :expr :expr expr}
                                    :else (refuse at (str "ORDER BY " n " numbers a ROW_NUMBER() without a name, which isn't converted.")))))))]
    (cond-> q
      (seq (:group q)) (update :group #(mapv group-col %))
      (seq (:order q)) (update :order #(mapv order-target %)))))

(defn- flatten-query [state q]
  (let [q (positions q)
        [state from] (flatten-table state (:from q))
        [state joins] (reduce (fn [[st acc] j] (let [[st t] (flatten-table st (:table j))] [st (conj acc (assoc j :table t))]))
                              [state []] (:joins q))
        [state where] (if (:where q) (flatten-predicate state (:where q)) [state nil])
        [state select] (if (:select q) (flatten-select state (:select q)) [state nil])]
    [state (cond-> (assoc q :from from :joins joins :where where)
             (:select q) (assoc :select select))]))

(defn- entry-named [entries n]
  (first (filter #(and (:name %) (= n (:name %))) entries)))

(defn- uses
  "How many times each entry's name is used as a table, across `entries`."
  [entries]
  (frequencies (for [{:keys [query]} entries
                     t (query-tables query)
                     :when (and (nil? (:schema t)) (entry-named entries (:table t)))]
                 (:table t))))

(defn- output-item
  "The select item of `q` that returns column `c`: a column, or `c` itself
  when `q` selects every column of its one table."
  [q c]
  (or (first (filter #(and (= :column (:kind %)) (= c (or (:as %) (:name (:column %))))) (:select q)))
      (when (and (empty? (:joins q)) (some #(#{:star :alias-star} (:kind %)) (:select q)))
        {:kind :column :column {:name c :at (:at q)} :at (:at q)})))

(defn- narrowable?
  "Whether `q` can select only column `c` without changing its rows."
  [q c]
  (and (empty? (:group q)) (nil? (:distinct-on q)) (nil? (:limit q))
       (not-any? #(#{:count :row-number} (:kind %)) (:select q))
       (output-item q c)))

(defn- narrow
  "The entry selecting only column `c`, used only after `in`. not-null adds
  `c IS NOT NULL`, so a NULL can't make `not in` match nothing."
  [entry c not-null]
  (let [item (output-item (:query entry) c)]
    (-> entry
        (assoc :in-set-only true)
        (assoc-in [:query :select] [item])
        ;; Without a LIMIT (narrowable?), the order changes no value of the
        ;; set, and it may sort on a column no longer selected.
        (update :query dissoc :order)
        (update-in [:query :where] #(if not-null
                                      (and-of (cond-> [] % (conj %) true (conj {:op :is-null :negated true
                                                                                :left {:kind :column :column (:column item) :at (:at item)}
                                                                                :at (:at item)})))
                                      %)))))

(defn- rename-sets [p renames]
  (case (:op p)
    (:and :or) (update p :args (fn [args] (mapv #(rename-sets % renames) args)))
    :not (update p :arg #(rename-sets % renames))
    :in-set (update p :set #(get renames % %))
    p))

(defn- narrow-in-sets
  "`IN (SELECT c FROM cte)`, when every use of the WITH query is such an IN
  on the same column, uses the WITH query itself, selecting only c."
  [entries]
  (let [counts (uses entries)
        merges (for [{:keys [name query cte]} entries
                     :when cte
                     :let [users (filter #(some (fn [t] (and (nil? (:schema t)) (= name (:table t)))) (query-tables (:query %))) entries)
                           cols (distinct (map (comp :col :simple) users))]
                     :when (and (seq users) (every? #(= name (get-in % [:simple :src])) users)
                                (= (count users) (get counts name)) (= 1 (count cols))
                                (narrowable? query (first cols)))]
                 {:name name :col (first cols) :users (set (map :name users))})
        renames (into {} (for [{:keys [name users]} merges, u users] [u name]))
        by-name (into {} (map (juxt :name identity) merges))]
    (vec (for [e entries
               :when (not (renames (:name e)))]
           (cond-> (update-in e [:query :where] #(some-> % (rename-sets renames)))
             (by-name (:name e)) (narrow (:col (by-name (:name e))) false))))))

(defn- predicate-columns
  "Every column a predicate mentions, inside computed values too."
  [p]
  (expr-columns p))

(defn- query-columns
  "Every column a query mentions, outside its WHERE, with `*` as
  {:star true} and `t.*` as {:star \"t\"}."
  [q]
  (concat (mapcat (fn [{:keys [kind column qualifier partition order expr]}]
                    (case kind
                      :star [{:star true}]
                      :alias-star [{:star qualifier}]
                      :column [column]
                      :expr (expr-columns expr)
                      :row-number (concat (mapcat #(if (:expr %) (expr-columns (:expr %)) [%]) partition)
                                          (mapcat #(expr-columns (:target %)) order))
                      (expr-columns kind)))
                  (:select q))
          (mapcat #(some-> (:on %) predicate-columns) (:joins q))
          (:group q)
          (some-> (:having q) expr-columns)
          (mapcat #(expr-columns (:target %)) (:order q))
          (:distinct-on q)
          (map :column (get-in q [:action :set]))))

(defn- nullable-side?
  "Whether an outer join can fill table `idx` of `q` with NULLs."
  [q idx]
  (let [joins (:joins q)]
    (boolean (or (and (pos? idx) (= :left (:type (nth joins (dec idx)))))
                 (some #(= :right (:type %)) (drop idx joins))))))

(defn- not-null?
  "Whether column `col` of query `q` is known never to be NULL: it comes,
  through WITH queries, from a column the schema says is NOT NULL, and no
  outer join can make it NULL. Without a schema, nothing is known."
  [entries q col depth]
  (let [tables (query-tables q)
        idx (if (:qualifier col)
              (first (keep-indexed #(when (= (:qualifier col) (table-key %2)) %1) tables))
              (when (= 1 (count tables)) 0))]
    (boolean
     (when (and idx (< depth 16) (not (nullable-side? q idx)))
       (let [t (nth tables idx)]
         (if-let [e (and (nil? (:schema t)) (entry-named entries (:table t)))]
           (when-let [item (output-item (:query e) (:name col))]
             (not-null? entries (:query e) (:column item) (inc depth)))
           (when *schema*
             (let [{:keys [db-column-info db-columns]} (db-table t)
                   n (spelling (:name col) db-columns (:at col))]
               (= "NO" (some #(when (= n (:column %)) (:nullable %)) db-column-info))))))))))

(defn- output-not-null?
  "Whether the column named `c` in the result of query `q` is known never
  to be NULL: the column it selects under that name is."
  [entries q c at]
  (boolean (when-let [item (output-item q c)]
             (not-null? entries q (assoc (:column item) :at at) 0))))

(defn- bare-may-be-k?
  "Whether a bare column among `cols` may belong to the joined table `k-ref`,
  in a query of several tables. The rewrites below remove k, and with it
  such a column. With the schema, a real table's columns are known."
  [q k-ref cols]
  (let [set-columns (set (map :column (get-in q [:action :set])))
        aliases (set (keep :as (:select q)))
        bare (filter #(and (:name %) (nil? (:qualifier %)) (not (set-columns %)) (not (aliases (:name %)))) cols)]
    (boolean
     (and (seq bare) (next (query-tables q))
          (let [{:keys [db-table db-columns]} (when *schema* (db-table k-ref))]
            (or (nil? db-table) (empty? db-columns)
                (some (fn [c] (some #(.equalsIgnoreCase ^String (:name c) ^String %) db-columns)) bare)))))))

(defn- anti-join
  "{:join <index> :conjunct <p> :k-col :t-col} when join `ji` of `q` is
  `LEFT JOIN k ON k.c = t.x` and WHERE has `k.c IS NULL` at its top, with k
  used nowhere else. Otherwise nil."
  [q ji]
  (let [{:keys [type on] k-ref :table} (nth (:joins q) ji)
        k (table-key k-ref)
        earlier (set (map table-key (take (inc ji) (query-tables q))))
        [pair & more] (some-> on conjuncts)
        {:keys [op operator left right]} pair
        cols (when (and (= :compare op) (= "=" operator) (not more)
                        (= :column (:kind left)) (= :column (:kind right))
                        (not (:cast left)) (not (:cast right)))
               (let [[l r] [(:column left) (:column right)]]
                 (cond (and (= k (:qualifier l)) (earlier (:qualifier r)) (not= k (:qualifier r))) [l r]
                       (and (= k (:qualifier r)) (earlier (:qualifier l)) (not= k (:qualifier l))) [r l])))
        [k-col t-col] cols
        where-parts (some-> (:where q) conjuncts)
        is-null? (fn [{:keys [op negated left]}]
                   (and (= :is-null op) (not negated) (= :column (:kind left)) (not (:cast left))
                        (= k (:qualifier (:column left))) (= (:name k-col) (:name (:column left)))))
        conjunct (first (filter is-null? where-parts))
        others (concat (query-columns (assoc q :joins (vec (concat (take ji (:joins q)) (drop (inc ji) (:joins q))))))
                       (mapcat predicate-columns (remove #{conjunct} where-parts)))]
    (when (and (= :left type) cols conjunct
               (not-any? #(or (true? (:star %)) (= k (:star %)) (= k (:qualifier %))) others)
               (not (bare-may-be-k? q k-ref others)))
      {:join ji :conjunct conjunct :k-col k-col :t-col t-col})))

(defn- anti-joins
  "`LEFT JOIN k ON k.c = t.x ... WHERE k.c IS NULL` keeps the rows of t that
  have no k. That is `x not in k`, with k selecting only c: the WITH query
  itself when nothing else uses it, or a new entry. NOT IN differs on
  NULLs, so unless the schema shows the columns are NOT NULL, the condition
  is `x is null or x not in k` and k leaves out NULL."
  [entries]
  (let [counts (uses entries)]
    (reduce
     (fn [acc [i entry]]
       (loop [acc acc entry entry]
         (let [q (:query entry)
               found (some #(anti-join q %) (range (count (:joins q))))]
           (if-not found
             (conj acc entry)
             (let [{:keys [join conjunct k-col t-col]} found
                   {k-ref :table at :at} (nth (:joins q) join)
                   all (into acc (drop (inc i) entries))
                   src (and (nil? (:schema k-ref)) (entry-named acc (:table k-ref)))
                   c (:name k-col)
                   narrow? (and src (= 1 (get counts (:table k-ref))) (narrowable? (:query src) c))
                   right-ok (if narrow?
                              (output-not-null? all (:query src) c at)
                              (not-null? all {:from k-ref :joins []} k-col 0))
                   left-ok (not-null? all q t-col 0)
                   [acc set-name] (if narrow?
                                    [(mapv #(if (= % src) (narrow src c (not right-ok)) %) acc) (:name src)]
                                    (let [n (pine-name! (fresh-name (set (keep :name all)) (str (:table k-ref) "_" c "s")) at)]
                                      [(conj acc {:name n :in-set-only true :at at
                                                  :query {:kind :select :joins [] :at at
                                                          :select [{:kind :column :column {:name c :at at} :at at}]
                                                          :from (dissoc k-ref :alias)
                                                          :where (when-not right-ok
                                                                   {:op :is-null :negated true :at at
                                                                    :left {:kind :column :column {:name c :at at} :at at}})}})
                                       n]))
                   t-operand {:kind :column :column t-col :at (:at t-col)}
                   in-set {:op :in-set :negated true :left t-operand :set set-name :at (:at conjunct)}
                   condition (if left-ok
                               in-set
                               {:op :or :args [{:op :is-null :negated false :left t-operand :at (:at conjunct)} in-set]})
                   where (and-of (concat (remove #{conjunct} (conjuncts (:where q))) [condition]))
                   q (-> q
                         (update :joins #(vec (concat (take join %) (drop (inc join) %))))
                         (assoc :where where))]
               (recur acc (assoc entry :query q)))))))
     []
     (map-indexed vector entries))))

(defn- exists-guards
  "EXISTS is true or false, never NULL. Its `x in k` is NULL when x is NULL,
  or when x isn't in k and k holds a NULL. Under no NOT, NULL drops the row
  as false would, so the in stays as it is. Under a NOT, the rows with no
  match must stay: the in becomes `x is not null and x in k`, and k leaves
  out NULL, each unless the schema shows it can't be NULL."
  [entries]
  (let [nullable (atom #{})
        guard (fn guard [all q p negate]
                (case (:op p)
                  (:and :or) (assoc p :args (mapv #(guard all q % negate) (:args p)))
                  :not (assoc p :arg (guard all q (:arg p) (not negate)))
                  :in-set (if-not (:exists p)
                            p
                            (let [p (dissoc p :exists)
                                  src (entry-named all (:set p))
                                  c (get-in src [:query :select 0 :column :name])]
                              (when-not (or (not negate) (output-not-null? all (:query src) c (:at p)))
                                (swap! nullable conj (:set p)))
                              (if (or (not negate) (not-null? all q (get-in p [:left :column]) 0))
                                p
                                {:op :and :at (:at p)
                                 :args [{:op :is-null :negated true :left (:left p) :at (:at p)} p]})))
                  p))
        entries (mapv (fn [{q :query :as e}]
                        (if (:where q) (assoc-in e [:query :where] (guard entries q (:where q) false)) e))
                      entries)]
    (mapv (fn [{n :name q :query :as e}]
            (if (@nullable n)
              (let [{c :column at :at} (first (:select q))]
                ;; Not :simple any more, so narrow-in-sets keeps the filter.
                (assoc-in (dissoc e :simple) [:query :where]
                          (and-of (concat (some-> (:where q) conjuncts)
                                          [{:op :is-null :negated true :at at
                                            :left {:kind :column :column c :at at}}]))))
              e))
          entries)))

(defn- semi-join
  "{:join <index> :k-col :t-col :set <entry name>} when join `ji` of `q` is
  `LEFT JOIN k ON k.c = t.x`, `k` is a WITH query of distinct values of c
  used nowhere else (`counts`), and every other mention of k in q is a
  test `k.c IS [NOT] NULL`. Otherwise nil."
  [entries counts q ji]
  (let [{:keys [type on] k-ref :table} (nth (:joins q) ji)
        k (table-key k-ref)
        src (and (nil? (:schema k-ref)) (entry-named entries (:table k-ref)))
        sq (:query src)
        earlier (set (map table-key (take (inc ji) (query-tables q))))
        [pair & more] (some-> on conjuncts)
        {:keys [op operator left right]} pair
        [k-col t-col] (when (and (= :compare op) (= "=" operator) (not more)
                                 (= :column (:kind left)) (= :column (:kind right))
                                 (not (:cast left)) (not (:cast right)))
                        (let [[l r] [(:column left) (:column right)]]
                          (cond (and (= k (:qualifier l)) (earlier (:qualifier r)) (not= k (:qualifier r))) [l r]
                                (and (= k (:qualifier r)) (earlier (:qualifier l)) (not= k (:qualifier l))) [r l])))
        [item & more-items] (:select sq)
        distinct-values? (and sq (:distinct sq) (nil? (:limit sq)) (nil? more-items) (= :column (:kind item))
                              (= (:name k-col) (or (:as item) (:name (:column item))))
                              (empty? (:group sq)) (nil? (:having sq)) (nil? (:distinct-on sq)))
        tests (find-nodes #(and (= :is-null (:op %)) (= :column (get-in % [:left :kind]))
                                (= k (get-in % [:left :column :qualifier])))
                          (dissoc q :joins))
        test? (set tests)
        ;; Every mention of k outside the tests, as anti-join looks for
        ;; them: the select list, the other joins, GROUP BY, HAVING, ORDER
        ;; BY, DISTINCT ON and WHERE.
        untested (walk/prewalk #(if (test? %) {:kind :null} %) q)
        others (concat (query-columns (assoc untested :joins (vec (concat (take ji (:joins q)) (drop (inc ji) (:joins q))))))
                       (some-> (:where untested) predicate-columns))]
    (when (and (= :left type) k-col distinct-values? (= 1 (get counts (:table k-ref)))
               ;; `WHERE k.c IS NULL` on its own is the anti-join's.
               (not-any? (fn [p] (and (= :is-null (:op p)) (not (:negated p)) (some #{p} tests)))
                         (some-> (:where q) conjuncts))
               (every? #(= (:name k-col) (get-in % [:left :column :name])) tests)
               (not-any? #(or (= k (:qualifier %)) (true? (:star %)) (= k (:star %))) others)
               (not (bare-may-be-k? q k-ref others))
               (not-any? #(or (= :star (:kind %)) (and (= :alias-star (:kind %)) (= k (:qualifier %)))) (:select q)))
      {:join ji :k-col k-col :t-col t-col :set (:name src)})))

(defn- semi-joins
  "`LEFT JOIN k ON k.c = t.x`, where k holds distinct values of c and q
  only tests `k.c IS NOT NULL` (or IS NULL), keeps every row of t once and
  asks whether x is in k. That is Pine's `x in k`, with no join. k then is
  only used after `in`. When x or c could be NULL, `x in k` could be
  unknown where the test is false, so a value uses `if x in k then true
  else false` (see conditions-pine)."
  [entries]
  (let [counts (uses entries)]
    (reduce
     (fn [acc [i entry]]
       (loop [acc acc entry entry]
         (let [q (:query entry)
               all (into acc (drop (inc i) entries))
               found (some #(semi-join all counts q %) (range (count (:joins q))))]
           (if-not found
             (conj acc entry)
             (let [{:keys [join k-col t-col set]} found
                   k (table-key (:table (nth (:joins q) join)))
                   src (entry-named all set)
                   guard (not (and (not-null? all q t-col 0)
                                   (output-not-null? all (:query src) (:name k-col) (:at k-col))))
                   t-operand {:kind :column :column t-col :at (:at t-col)}
                   q (-> (walk/prewalk (fn [n]
                                         (if (and (map? n) (= :is-null (:op n)) (= k (get-in n [:left :column :qualifier])))
                                           (cond-> {:op :in-set :negated (not (:negated n)) :left t-operand :set set :at (:at n)}
                                             guard (assoc :guard true))
                                           n))
                                       (dissoc q :joins))
                         (assoc :joins (vec (concat (take join (:joins q)) (drop (inc join) (:joins q))))))
                   acc (mapv #(if (= (:name %) set) (assoc % :in-set-only true) %) acc)]
               (recur acc (assoc entry :query q)))))))
     []
     (map-indexed vector entries))))

(defn- rn-limit
  "N when conjunct `p` keeps the first N rows of each group: rn = 1,
  rn <= N or rn < N+1, on column `rn` of table `k`. Refuses another test of
  rn. nil when `p` doesn't test rn."
  [p rn k]
  (let [{:keys [op operator left right at]} p
        rn? (fn [o] (and (= :column (:kind o)) (= rn (:name (:column o))) (contains? #{k nil} (:qualifier (:column o)))))
        [operator other] (cond (and (= :compare op) (rn? left)) [operator right]
                               (and (= :compare op) (rn? right)) [(mirrored operator) left])]
    (when operator
      (let [n (when (= :number (:kind other)) (parse-long (:value other)))]
        (cond
          (nil? n) (refuse at (str "`" rn "` from ROW_NUMBER() can only be compared with a number here."))
          (and (= operator "=") (= n 1)) 1
          (= operator "=") (refuse at (str "WHERE " rn " = " n " keeps only row " n " of each group. Pine keeps the first N: "
                                           rn " = 1 or " rn " <= N."))
          (and (= operator "<=") (pos? n)) n
          (and (= operator "<") (> n 1)) (dec n)
          (#{">" ">="} operator) (refuse at (str "WHERE " rn " " operator " " n " keeps the rows after the first of each group, which isn't in Pine yet."))
          :else (refuse at (str "WHERE " rn " " operator " " n " isn't converted. Pine keeps the first N rows of each group: "
                                rn " = 1 or " rn " <= N.")))))))

(defn- drop-unused-windows
  "A ROW_NUMBER() in a WITH query that no later query mentions numbers rows
  for nothing. It is left out, with a warning, so another ROW_NUMBER() in
  the same query can still become limit: N per."
  [entries warn!]
  (mapv (fn [{:keys [name query] :as e}]
          (if-not name
            e
            (let [users (filter (fn [u] (some #(and (nil? (:schema %)) (= name (:table %))) (query-tables (:query u)))) entries)
                  mentions (mapcat #(concat (query-columns (:query %)) (some-> (:query %) :where predicate-columns)) users)
                  mentioned (set (map :name mentions))
                  ;; A `*` over it would return the row number: that is a use.
                  starred? (some (fn [u] (let [ks (set (keep #(when (and (nil? (:schema %)) (= name (:table %))) (table-key %))
                                                             (query-tables (:query u))))]
                                           (some #(or (true? (:star %)) (ks (:star %))) (query-columns (:query u)))))
                                 users)
                  unused (when-not starred?
                           (filter #(and (= :row-number (:kind %)) (:as %) (not (mentioned (:as %)))) (:select query)))]
              (doseq [r unused]
                (warn! (str "Left out " (:as r) ": a ROW_NUMBER() in `" name "` that no later query uses.")))
              (cond-> e
                (seq unused) (update-in [:query :select] #(vec (remove (set unused) %)))))))
        entries))

(defn- plan-windows
  "A query with ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn, whose
  every user keeps WHERE rn <= N, becomes order: o | limit: N per p. The
  rn test leaves the users, and so does every other use of rn."
  [entries warn!]
  (reduce
   (fn [entries {:keys [name query]}]
     (let [[r & more] (filter #(= :row-number (:kind %)) (:select query))]
       (cond
         (nil? r) entries
         more (refuse (:at (first more)) "Only one ROW_NUMBER() per query converts.")
         (nil? name) entries
         :else
         (let [rn (or (:as r) (refuse (:at r) "ROW_NUMBER() needs a name, like AS rn, for a later query to keep its first rows."))
               results (for [e entries
                             :let [q (:query e)
                                   tables (query-tables q)
                                   idx (first (keep-indexed #(when (and (nil? (:schema %2)) (= name (:table %2))) %1) tables))]
                             :when idx]
                         (let [tref (nth tables idx)
                               k (table-key tref)
                               _ (when (next (filter #(and (nil? (:schema %)) (= name (:table %))) tables))
                                   (refuse (:at tref) (str "`" name "` numbers rows with ROW_NUMBER(), and this query reads it twice."
                                                           " That isn't converted: Pine keeps the first rows of each group once.")))
                               _ (when (and (pos? idx) (:type (nth (:joins q) (dec idx))))
                                   (refuse (:at (nth (:joins q) (dec idx)))
                                           (str "`" name "` numbers rows with ROW_NUMBER(). Joining it with LEFT or RIGHT JOIN isn't converted.")))
                               parts (some-> (:where q) conjuncts)
                               found (keep (fn [p] (when-let [n (rn-limit p rn k)] [p n])) parts)
                               _ (when (empty? found)
                                   (refuse (:at tref) (str "`" name "` numbers rows with ROW_NUMBER(), so a query that uses it must keep its first rows,"
                                                           " with WHERE " rn " = 1 or " rn " <= N.")))
                               _ (when (next found)
                                   (refuse (:at (first (second found))) (str "Test `" rn "` once.")))
                               [p n] (first found)
                               q (assoc q :where (and-of (remove #{p} parts)))
                               multi (next tables)
                               mentions (filter #(and (= rn (:name %)) (or (= k (:qualifier %)) (and (nil? (:qualifier %)) (not multi))))
                                                (concat (query-columns q) (some-> (:where q) predicate-columns)))]
                           (when (seq mentions)
                             (refuse (:at (first mentions)) (str "`" rn "` comes from ROW_NUMBER(), which becomes limit: N per in Pine."
                                                                 " Only WHERE " rn " = 1 or " rn " <= N can use it.")))
                           (when (some #(or (true? (:star %)) (= k (:star %))) (query-columns q))
                             (warn! (str "`" rn "` from ROW_NUMBER() isn't a column in Pine: `" name "` keeps the first rows of each group instead.")))
                           [(:name e) n q]))
               ns (distinct (map second results))]
           (when (next ns)
             (refuse (:at r) (str "The queries that use `" name "` keep different numbers of rows per group.")))
           (let [n (first ns)
                 updated (into {} (map (fn [[en _ q]] [en q]) results))]
             (mapv (fn [e]
                     (cond
                       (= (:name e) name) (cond-> e n (assoc :window {:n n}))
                       (contains? updated (:name e)) (assoc e :query (get updated (:name e)))
                       :else e))
                   entries))))))
   entries
   entries))
(defn- tie?
  "Whether `p` is t.a = f.b between the table an UPDATE or DELETE changes
  (`tk`) and the first table of its FROM or USING (`fk`)."
  [tk fk {:keys [op operator left right]}]
  (and (= :compare op) (= "=" operator)
       (= :column (:kind left)) (= :column (:kind right))
       (not (:cast left)) (not (:cast right))
       (= #{tk fk} (set [(:qualifier (:column left)) (:qualifier (:column right))]))
       (not= tk fk)))

(defn- write->query
  "An UPDATE or DELETE as the query that picks its rows, with :action. Its
  FROM (or USING) joins the table it changes, on the WHERE conditions that
  tie the two."
  [{:keys [kind target from joins where at] assignments :set}]
  (let [parts (some-> where conjuncts)
        tk (table-key target)
        ties (when from (filter #(tie? tk (table-key from) %) parts))
        what (if (= kind :update) "UPDATE" "DELETE")]
    (when (and from (empty? ties))
      (refuse (:at from) (str "This " what "'s " (if (= kind :update) "FROM" "USING") " needs a WHERE condition that ties `"
                              (table-key from) "` to `" tk "`, like " tk ".id = " (table-key from) ".id, with both columns qualified.")))
    {:kind :select :at at
     :from target
     :joins (if from (into [{:type nil :table from :on (and-of ties) :at (:at from)}] joins) [])
     :where (and-of (remove (set ties) parts))
     :action {:kind kind :set assignments :at at}}))

(defn- transpile-statement
  "The Pine blocks for one statement, in order."
  [stmt warn!]
  (let [ctes (:ctes stmt)]
    (doseq [{:keys [name at]} ctes] (pine-name! name at))
    (when-let [dupe (some (fn [[k n]] (when (> n 1) k)) (frequencies (map :name ctes)))]
      (refuse (:at (last (filter #(= dupe (:name %)) ctes))) (str "Two WITH queries are called `" dupe "`.")))
    ;; Pine reads a named result for a table of the same name, even one
    ;; written with its schema.
    (doseq [{:keys [name at]} ctes
            t (tree-seq coll? seq stmt)
            :when (and (map? t) (:schema t) (string? (:table t)) (.equalsIgnoreCase ^String name ^String (:table t)))]
      (refuse at (str "The WITH query `" name "` has the name of the table `" (:schema t) "." (:table t)
                      "`, which this statement also reads. Pine would read the WITH query for both. Give it another name.")))
    (let [main (if (= :select (:kind stmt)) (dissoc stmt :ctes) (write->query stmt))
          state (reduce (fn [st {:keys [name query at]}]
                          (let [[st q] (flatten-query st query)]
                            (update st :entries conj {:name name :query q :at at :cte true})))
                        {:entries [] :taken (set (map :name ctes))}
                        ctes)
          [state main-q] (flatten-query state main)
          entries (-> (conj (:entries state) {:name nil :query main-q :at (:at main)})
                      exists-guards
                      narrow-in-sets
                      semi-joins
                      anti-joins
                      (drop-unused-windows warn!)
                      (plan-windows warn!))]
      (loop [[e & more] entries ctes {} blocks []]
        (if-not e
          blocks
          (let [{:keys [expression columns extra-warnings]} (binding [*ctes* ctes] (transpile-query e warn!))
                ;; A column Pine added reaches a result when this is the
                ;; statement itself, or a later query selects `*` from it.
                seen? (or (nil? (:name e))
                          (some (fn [u] (let [ts (query-tables (:query u))
                                              ks (set (keep #(when (and (nil? (:schema %)) (= (:name e) (:table %))) (table-key %)) ts))]
                                          (and (seq ks)
                                               (some #(or (true? (:star %)) (ks (:star %))) (query-columns (:query u))))))
                                more))]
            (when seen? (doseq [m extra-warnings] (warn! m)))
            (recur more (cond-> ctes (:name e) (assoc (:name e) {:columns columns})) (conj blocks {:pine expression :name (:name e)}))))))))

(defn- script-statement
  "The statement of a script that converts, and warnings for what is left
  out. BEGIN, COMMIT and ROLLBACK are dropped. Of the rest, the one that
  changes data converts, or else the first."
  [sql]
  (let [pieces (map (fn [{:keys [start end] :as p}]
                      (let [text (subs sql start end)]
                        (assoc p :tx (sp/transaction-word text) :kind (sp/statement-kind text)
                               ;; As written, for the warning: BEGIN WORK, END.
                               :written (-> text (s/replace #"(?s)--[^\n]*|/\*.*?\*/" " ") s/trim (s/replace #"\s+" " ") s/upper-case))))
                    (sp/split-script sql))
        tx (filter :tx pieces)
        data (remove :tx pieces)
        writes (filter #(#{:update :delete :other-write} (:kind %)) data)
        chosen (or (first writes) (first data))
        rollback (first (filter #(= "rollback" (:tx %)) tx))
        start #(+ (:start %) (count (re-find #"^\s*" (subs sql (:start %) (:end %)))))
        line #(sp/line-of sql (start %))]
    (when (next writes)
      (refuse (start (second writes)) "This script changes data in more than one statement. Convert them one at a time."))
    (when (and (empty? data) (seq tx))
      (refuse (start (first tx)) "There is no statement to convert, only BEGIN, COMMIT or ROLLBACK."))
    (when (and rollback (seq writes))
      (refuse (start rollback) "The script ends in ROLLBACK, which undoes its change. Pine's update! and delete! keep theirs, so it doesn't convert."))
    {:statement chosen
     :warnings (concat
                (when (seq tx)
                  [(str "Left out " (s/join " and " (distinct (map :written tx)))
                        ". Pine runs each update! and delete! in a transaction of its own.")])
                (for [p data :when (not= p chosen)]
                  (str "Left out the statement on line " (line p) ". Pine converts one statement at a time.")))}))

(defn- build-error
  "The first block that doesn't build, with Pine's reason, as [block
  error], or nil. Each block is built against the connection, after the
  ones before it, as /build would. Nothing runs."
  [texts]
  (when *connection-id*
    (loop [[t & more] texts named {}]
      (when t
        (let [[state error] (try
                              (let [{:keys [result error]} (pine/parse t)
                                    _ (when error (throw (ex-info (str error) {})))
                                    state (ast/generate result *connection-id* nil nil named [])]
                                (eval/build-query state)
                                [state nil])
                              (catch Exception e [nil (or (ex-message e) (.getName (class e)))]))]
          (if error
            [t error]
            (recur more (merge named (:pending-assignments state)))))))))

(defn- render-blocks
  "The blocks laid out one operation per line, blank lines between them, or
  an internal error when one doesn't read back as Pine or doesn't build."
  [blocks warnings]
  (let [text (fn [{:keys [pine name]}] (str pine (when name (str " |= " name))))
        checked (map (fn [b] [(text b) (try (pine/parse (text b)) (catch Exception e {:error (ex-message e)}))]) blocks)
        unbuilt (when-not (some #(:error (second %)) checked) (build-error (map first checked)))]
    (cond
      (some #(:error (second %)) checked)
      (let [[b {:keys [error]}] (first (filter #(:error (second %)) checked))]
        {:error (str "The SQL converted to Pine that doesn't read back. This is a bug in the converter.\n\n" b "\n\n" error)
         :error-type "internal"})

      unbuilt
      {:error (str "The SQL converted to Pine that doesn't build. This is a bug in the converter.\n\n" (first unbuilt) "\n\n" (second unbuilt))
       :error-type "internal"}

      :else
      {:expression (s/join "\n\n" (map (fn [{:keys [pine name]}]
                                         (str (or (:result (pine/prettify pine)) pine) (when name (str " |= " name))))
                                       blocks))
       :warnings (vec (distinct warnings))})))

(defn- check-strings
  "Refuses a string with a line break in it. Pine's blocks are separated by
  a blank line, and so is each block a client sends, so such a string would
  split the block it is in."
  [stmt]
  (doseq [n (find-nodes #(and (= :string (:kind %)) (re-find #"[\r\n]" (or (:value %) ""))) stmt)]
    (refuse (:at n) "A string with a line break in it isn't converted. Pine's blocks are separated by blank lines.")))

(defn- statement-tables
  "Every table name a statement mentions, lowercased."
  [stmt]
  (set (for [n (tree-seq coll? seq stmt)
             :when (and (map? n) (string? (:table n)))]
         (s/lower-case (:table n)))))

(defn- convert [sql]
  (try
    (let [{:keys [statement warnings]} (script-statement sql)
          text (if statement (sp/mask sql (:start statement) (:end statement)) sql)
          {:keys [result error error-type]} (sp/parse text)]
      (if error
        {:error error :error-type error-type}
        (let [found (atom (vec warnings))
              _ (check-strings result)
              warn! #(swap! found conj %)
              blocks (binding [*sql-tables* (statement-tables result)
                               *warn!* warn!]
                       (transpile-statement result warn!))]
          (render-blocks blocks @found))))
    (catch clojure.lang.ExceptionInfo e
      (if (::refusal (ex-data e))
        {:error (str "Line " (sp/line-of sql (:at (ex-data e))) ": " (ex-message e))
         :error-type "sql-unsupported"}
        (throw e)))
    ;; Each level of nesting is a level of recursion, in the reader and here.
    (catch StackOverflowError _
      {:error "The SQL is nested too deeply to convert. Simplify it, or write it in Pine."
       :error-type "sql-unsupported"})))

(defn sql->pine
  "{:expression :warnings} for one SQL SELECT, UPDATE or DELETE, or
  {:error :error-type}. Each WITH query is a block of its own, ending in
  `|= name`, with blank lines between the blocks.

  error-type is sql-parse-error when the text isn't SQL this reads,
  sql-unsupported when Pine can't say it, and internal when the Pine
  produced doesn't parse or build, which is a bug here, not in the SQL.

  `references` is the connection's indexed schema, or nil. With it, a bare
  name takes the database's spelling.

  `opts` may have :connection-id, to build the converted Pine against that
  connection as a check (nothing runs), and :dialect, the connection's."
  ([sql] (sql->pine sql nil {}))
  ([sql references] (sql->pine sql references {}))
  ([sql references {:keys [connection-id dialect]}]
   (binding [*schema* references
             *connection-id* connection-id
             *dialect* dialect]
     (convert sql))))
