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
  4. plan-select picks select:, group: ... => count, or count:, then
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
            [pine.db.references :as refs]
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

(defn- resolve-column
  "{:t :name} for a SQL column, looking only at `tables` (the ones in scope)."
  [tables {:keys [qualifier name quoted at]}]
  (pine-name! name at)
  (cond
    qualifier
    (if-let [t (first (keep-indexed (fn [i tbl] (when (= qualifier (table-key tbl)) i)) tables))]
      {:t t :name (column-name (nth tables t) name quoted at)}
      (refuse at (str "`" qualifier "` isn't a table in this query.")))

    (= 1 (count tables))
    {:t 0 :name (column-name (first tables) name quoted at)}

    :else
    (refuse at (str "`" name "` could belong to "
                    (s/join " or " (map #(str "`" (table-key %) "`") tables))
                    ". Qualify it, like `" (table-key (first tables)) "." name "`."))))

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

(defn- value
  "A literal or variable operand as a Pine value."
  [{:keys [kind cast at] :as operand}]
  (when (and cast (not (dropped-value-casts cast)))
    (refuse at (str "A cast to " cast " isn't in Pine. Pine casts a column to text or uuid.")))
  (case kind
    (:string :number :boolean :variable) (dissoc operand :at :cast)
    ;; Left for the caller, which knows whether a column can go here.
    :column (dissoc operand :at)
    :null (refuse at "NULL can only be tested with IS NULL or IS NOT NULL.")))

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

(defn- compare-atom [tables {:keys [operator left right at]}]
  (let [[left right operator] (if (and (not (column-operand? left)) (column-operand? right))
                                [right left (mirrored operator)]
                                [left right operator])]
    (when-not (column-operand? left)
      (refuse at "A comparison needs a column on one side."))
    (when (and (= :null (:kind right)) (#{"=" "!="} operator))
      (refuse (:at right) (str "`" operator " NULL` is never true in SQL. Write IS NULL or IS NOT NULL.")))
    {:col (resolve-column tables (:column left))
     :operator operator
     :cast (column-cast left)
     :value (if (column-operand? right)
              (do (when (:cast right)
                    (refuse (:at right) "A cast on the right-hand column isn't in Pine. Cast the left one."))
                  {:kind :column :col (resolve-column tables (:column right))})
              (value right))}))

(defn- left-column [tables {:keys [left at]}]
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
    :in-set {:col (left-column tables p)
             :cast (column-cast (:left p))
             :operator (if (not= (:negated p) (boolean negate)) "not in" "in")
             :value {:kind :set :name (:set p)}}
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

(defn- plan-where
  "Clauses for the WHERE and for any inner-join ON conditions moved into it."
  [tables predicates]
  (vec (mapcat (fn [p] (cnf (nnf tables p false) (:at p))) predicates)))

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
             (column-operand? left) (column-operand? right)
             (not (:cast left)) (not (:cast right)))
    (let [l (resolve-column tables (:column left))
          r (resolve-column tables (:column right))
          [new old] (cond (and (= i (:t l)) (< (:t r) i)) [l r]
                          (and (= i (:t r)) (< (:t l) i)) [r l])]
      (when new
        [(:t old) {:new (:name new) :anchor (:name old)}]))))

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
          (when-let [cast (some (fn [{:keys [op left right]}]
                                  (when (= :compare op) (or (:cast left) (:cast right))))
                                (conjuncts on))]
            (refuse at (str "A cast to " cast " in a join's ON isn't in Pine. Pine joins on columns as they are.")))
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
  "A hidden key or type column Pine adds to its own SQL: AS \"__c_0__id\"."
  [{:keys [kind as]}]
  (and (= :column kind) as (re-matches #"__.+__.+" as)))

(defn- order-target
  "The select item an unqualified ORDER BY name refers to by its alias."
  [items {:keys [kind column]}]
  (when (and (= :column kind) (not (:qualifier column)))
    (first (filter #(= (:name column) (:as %)) items))))

(defn- sort-word [d]
  (if (= d :desc) "desc" "asc"))

(defn- plan-grouped [tables items {:keys [group order limit]} warn!]
  (let [group-cols (mapv #(resolve-column tables %) group)
        group-set (set group-cols)]
    (when-let [dupe (some (fn [[k n]] (when (> n 1) k)) (frequencies (map :name group-cols)))]
      (refuse (:at (first group)) (str "Two grouped columns are both called `" dupe "`. Pine names a grouped column by its name alone.")))
    (doseq [{:keys [kind column as at]} items]
      (case kind
        :count (when (and as (not= as "count"))
                 (warn! (str "COUNT(*) AS " as ": Pine names the count column `count`.") :columns))
        :column (do (when-not (group-set (resolve-column tables column))
                      (refuse at (str "`" (:name column) "` is selected but not grouped. Pine selects only the grouped columns and the count.")))
                    (when (and as (not= as (:name column)))
                      (warn! (str (:name column) " AS " as ": a grouped column keeps its own name in Pine.") :columns)))
        (refuse at "Only grouped columns and COUNT(*) can be selected with GROUP BY.")))
    (when-not (some #(= :count (:kind %)) items)
      (warn! "Pine adds a count column to a group:." :columns))
    (let [selected (set (for [i items :when (= :column (:kind i))] (resolve-column tables (:column i))))]
      (when (some (complement selected) group-cols)
        (warn! "Pine returns every grouped column, including ones the SELECT left out." :columns)))
    (cond-> [{:op :group :cols group-cols}]
      (seq order)
      (conj {:op :order
             :items (mapv (fn [{:keys [target direction at]}]
                            (let [aliased (order-target items target)
                                  target (if aliased {:kind (:kind aliased) :column (:column aliased)} target)]
                              {:name (case (:kind target)
                                       :count "count"
                                       :column (let [c (resolve-column tables (:column target))]
                                                 (if (group-set c)
                                                   (:name c)
                                                   (refuse at (str "`" (:name c) "` isn't grouped, so it can't be sorted on after GROUP BY."))))
                                       :position (refuse at "ORDER BY a position isn't in Pine. Name the column."))
                               :direction (sort-word direction)}))
                          order)})
      limit (conj {:op :limit :n (:value limit)}))))

(defn- order-items
  "order: items for ORDER BY items that name columns. `what` names where
  they are, for a refusal."
  [tables items order what]
  (mapv (fn [{:keys [target direction at]}]
          (let [aliased (order-target items target)]
            (case (:kind target)
              :count (refuse at (str "ORDER BY COUNT(*) " what "needs GROUP BY."))
              :position (refuse at (str "ORDER BY a position " what "isn't in Pine. Name the column."))
              :column {:col (resolve-column tables (:column (or aliased target)))
                       :direction (sort-word direction)})))
        order))

(defn- window-ops
  "order: and limit: N per for a ROW_NUMBER() that a later query keeps the
  first N rows of (see plan-windows)."
  [tables {:keys [partition order at]} {:keys [n]}]
  (when-not n
    (refuse at "ROW_NUMBER() converts only when a later query keeps its first rows, with WHERE rn = 1 or rn <= N."))
  (when (empty? partition)
    (refuse at "ROW_NUMBER() without PARTITION BY numbers the whole result. Use ORDER BY and LIMIT."))
  (cond-> []
    (seq order) (conj {:op :order :items (order-items tables [] order "inside ROW_NUMBER() ")})
    true (conj {:op :limit :n n :per (mapv #(resolve-column tables %) partition)})))

(defn- distinct-on-ops
  "SELECT DISTINCT ON (p) ... ORDER BY p, o keeps the first row of each p in
  that order: order: p, o | limit: 1 per p. Ordering by p too changes no
  group's first row, since p is the same within a group, and keeps the
  SQL's order of the rows."
  [tables items {:keys [distinct-on order limit]}]
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

(defn- plan-plain [tables current items {:keys [order limit distinct-on] :as stmt} window]
  (let [stars (fn [] (mapv (fn [t] {:star t}) (range (count tables))))
        row-number (first (filter #(= :row-number (:kind %)) items))
        expanded (vec (mapcat (fn [{:keys [kind qualifier column as at]}]
                                (case kind
                                  :star (stars)
                                  :alias-star [{:star (:t (resolve-column tables {:qualifier qualifier :name "x" :at at}))}]
                                  :row-number []
                                  :column (let [col (resolve-column tables column)]
                                            [(cond-> {:col col}
                                               (and as (not= as (:name col))) (assoc :as (pine-name! as at)))])))
                              items))]
    (when (and row-number (or (seq order) limit distinct-on))
      (refuse (:at row-number) "ROW_NUMBER() next to ORDER BY, LIMIT or DISTINCT ON in the same query isn't converted."))
    (when (empty? expanded)
      (refuse (:at (first items)) "Nothing is left to select."))
    (cond-> []
      (not= expanded [{:star current}])
      (conj {:op :select :items expanded})
      row-number
      (into (window-ops tables row-number window))
      distinct-on
      (into (distinct-on-ops tables items stmt))
      (and (seq order) (not distinct-on))
      (conj {:op :order :items (order-items tables items order "")})
      limit (conj {:op :limit :n (:value limit)}))))

(defn- plan-select [tables current {:keys [select group order limit distinct distinct-on] :as stmt} warn! {:keys [window in-set-only]}]
  (let [hidden (filter hidden-column? select)
        items (vec (remove hidden-column? select))]
    (when (seq hidden)
      (warn! (str "Left out " (s/join ", " (map :as hidden)) ": Pine adds its hidden columns itself.") :columns))
    (when (empty? items)
      (refuse (:at (first select)) "Nothing is left to select."))
    (when (and distinct (not in-set-only))
      (refuse (:at distinct) "DISTINCT isn't in Pine. (group: isn't the same: it adds a count.)"))
    (when (and limit (> (:value limit) pine/max-limit))
      (refuse (:at limit) (str "LIMIT " (:value limit) " is more than Pine's " pine/max-limit ".")))
    (when-let [r (and (or (seq group) (some #(= :count (:kind %)) items))
                      (first (filter #(= :row-number (:kind %)) items)))]
      (refuse (:at r) "ROW_NUMBER() next to GROUP BY or COUNT(*) isn't converted."))
    (when (and distinct-on (or (seq group) (some #(= :count (:kind %)) items)))
      (refuse (:at (first distinct-on)) "DISTINCT ON next to GROUP BY or COUNT(*) isn't converted."))
    (cond
      (seq group)
      (plan-grouped tables items stmt warn!)

      (some #(= :count (:kind %)) items)
      (do (when (next items)
            (refuse (:at (first (remove #(= :count (:kind %)) items))) "A column next to COUNT(*) needs GROUP BY."))
          (when (seq order) (refuse (:at (first order)) "ORDER BY on a single COUNT(*) isn't converted. Remove it."))
          (when limit (refuse (:at limit) "LIMIT on a single COUNT(*) isn't converted. Remove it."))
          (when (and (:as (first items)) (not= "count" (:as (first items))))
            (warn! (str "COUNT(*) AS " (:as (first items)) ": Pine names the count column `count`.") :columns))
          [{:op :count}])

      :else
      (plan-plain tables current items stmt window))))

(defn- output-columns
  "The names of the columns a query returns, or nil when they aren't known:
  a `*` over a table whose columns aren't known."
  [tables ops]
  (let [table-columns (fn [t] (:db-columns (nth tables t)))]
    (if-let [sel (first (filter #(= :select (:op %)) ops))]
      (let [cols (map (fn [item]
                        (if (contains? item :star)
                          (table-columns (:star item))
                          [(or (:as item) (:name (:col item)))]))
                      (:items sel))]
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

(defn- render-value [{:keys [kind value name]} col-ref]
  (case kind
    :string (quote-string value)
    :number value
    :boolean (str value)
    :variable (str "$" name)
    :column (col-ref value)))

(defn- render-atom [col-ref {:keys [col operator value cast]}]
  (str (col-ref col) " " operator
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
  (let [cols (fn [c] (when (not= current (:t c)) [(:t c)]))]
    (set (mapcat (fn [{:keys [op t clause items cols-of] :as o}]
                   (case op
                     :from [t]
                     :where (mapcat (fn [{:keys [col value]}]
                                      (concat (cols col) (when (= :column (:kind value)) (cols (:col value)))))
                                    clause)
                     :select (mapcat #(if (contains? % :star) [(:star %)] (cols (:col %))) items)
                     :group (mapcat cols (:cols o))
                     :order (mapcat #(some-> (:col %) cols) items)
                     :limit (mapcat cols (:per o))
                     nil))
                 ops))))

(defn- render [tables ops current]
  (let [referenced (references ops current)
        alias-of (fn [t] (let [{:keys [alias table]} (nth tables t)] (or alias table)))
        col-ref (fn [{:keys [t name]}] (if (= t current) name (str (alias-of t) "." name)))]
    (s/join
     " | "
     (for [{:keys [op t n clause items cols] :as o} ops]
       (case op
         :table (let [{:keys [schema table db-schema db-table alias type pairs]} (nth tables t)
                      schema (or db-schema schema)]
                  (str (when schema (str schema "."))
                       (or db-table table)
                       (when (or alias (referenced t)) (str " as " (alias-of t)))
                       (when type (str " :" (name type)))
                       (when (seq pairs)
                         (str " " (s/join ", " (map #(str "." (:new %) " = ." (:anchor %)) pairs))))))
         :from (str "from: " (alias-of t))
         :where (str "where: " (s/join " or " (map #(render-atom col-ref %) clause)))
         :select (str "select: " (s/join ", " (map #(if (contains? % :star)
                                                      (str (alias-of (:star %)) ".*")
                                                      (str (col-ref (:col %)) (when (:as %) (str " as " (:as %)))))
                                                   items)))
         :group (str "group: " (s/join ", " (map col-ref cols)) " => count")
         :count "count:"
         :order (str "order: " (s/join ", " (map #(str (if (:col %) (col-ref (:col %)) (:name %)) " " (:direction %)) items)))
         :limit (str "limit: " n (when (seq (:per o)) (str " per " (s/join ", " (map col-ref (:per o))))))
         :update (str "update! " (s/join ", " (map (fn [{:keys [column value]}]
                                                     (str column " = " (if (= :null (:kind value)) "null" (render-value value col-ref))))
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
  "The value an UPDATE's SET writes: a literal or NULL. Pine's update! takes
  a function's result nowhere, and another column isn't converted."
  [{:keys [kind at] :as operand}]
  (case kind
    :null {:kind :null}
    :column (refuse at "update! here takes a value. Setting a column from another column isn't converted.")
    (value operand)))

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
    (into (if (zero? current) [] [{:op :from :t 0}])
          (case kind
            :update
            [{:op :update
              :assignments (mapv (fn [{:keys [column value at]}]
                                   (when (and (:qualifier column) (not= (:qualifier column) (table-key target)))
                                     (refuse at (str "`" (:qualifier column) "." (:name column) "` isn't a column of the table the UPDATE changes.")))
                                   {:column (column-name target (:name column) (:quoted column) at)
                                    :value (assignment-value value)})
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
              (into ops (plan-select tables current query warn-q! {:window window :in-set-only in-set-only})))]
    (when name
      (when-let [[m] (first (filter #(= :columns (second %)) @local))]
        (refuse at (str "In the WITH query `" name "`: " m " Its columns would then differ from the SQL's."))))
    (doseq [[m] @local] (warn! m))
    {:expression (render tables ops current)
     :columns (when-not (:action query) (output-columns tables ops))}))

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
  (let [taken? #(or (taken %) (contains? (:table *schema*) %))]
    (if (taken? base) (first (remove taken? (map #(str base "_" %) (iterate inc 2)))) base)))

(declare flatten-query)

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

(defn- flatten-predicate
  "A subquery after IN becomes an entry, and the condition `x in <entry>`."
  [state {:keys [op] :as p}]
  (case op
    (:and :or) (let [[state args] (reduce (fn [[st acc] a] (let [[st a] (flatten-predicate st a)] [st (conj acc a)]))
                                          [state []] (:args p))]
                 [state (assoc p :args args)])
    :not (let [[state a] (flatten-predicate state (:arg p))] [state (assoc p :arg a)])
    :in (if-let [sq (:subquery p)]
          (let [[state sq] (flatten-query state sq)
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
             {:op :in-set :negated (:negated p) :left (:left p) :set n :at (:at p)}])
          [state p])
    [state p]))

(defn- flatten-query [state q]
  (let [[state from] (flatten-table state (:from q))
        [state joins] (reduce (fn [[st acc] j] (let [[st t] (flatten-table st (:table j))] [st (conj acc (assoc j :table t))]))
                              [state []] (:joins q))
        [state where] (if (:where q) (flatten-predicate state (:where q)) [state nil])]
    [state (assoc q :from from :joins joins :where where)]))

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
  (and (empty? (:group q)) (nil? (:distinct-on q))
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
  "Every column a predicate mentions."
  [p]
  (case (:op p)
    (:and :or) (mapcat predicate-columns (:args p))
    :not (predicate-columns (:arg p))
    :truthy [(:column p)]
    (keep #(when (= :column (:kind %)) (:column %))
          (concat [(:left p) (:right p) (:low p) (:high p)] (:values p)))))

(defn- query-columns
  "Every column a query mentions, outside its WHERE, with `*` as
  {:star true} and `t.*` as {:star \"t\"}."
  [q]
  (concat (mapcat (fn [{:keys [kind column qualifier partition order]}]
                    (case kind
                      :star [{:star true}]
                      :alias-star [{:star qualifier}]
                      :column [column]
                      :row-number (concat partition (keep #(get-in % [:target :column]) order))
                      []))
                  (:select q))
          (mapcat #(some-> (:on %) predicate-columns) (:joins q))
          (:group q)
          (keep #(get-in % [:target :column]) (:order q))
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
               (not-any? #(or (true? (:star %)) (= k (:star %)) (= k (:qualifier %))) others))
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
                              (not-null? all (:query src) {:name c :at at} 0)
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
    (let [main (if (= :select (:kind stmt)) (dissoc stmt :ctes) (write->query stmt))
          state (reduce (fn [st {:keys [name query at]}]
                          (let [[st q] (flatten-query st query)]
                            (update st :entries conj {:name name :query q :at at :cte true})))
                        {:entries [] :taken (set (map :name ctes))}
                        ctes)
          [state main-q] (flatten-query state main)
          entries (-> (conj (:entries state) {:name nil :query main-q :at (:at main)})
                      narrow-in-sets
                      anti-joins
                      (plan-windows warn!))]
      (loop [[e & more] entries ctes {} blocks []]
        (if-not e
          blocks
          (let [{:keys [expression columns]} (binding [*ctes* ctes] (transpile-query e warn!))]
            (recur more (cond-> ctes (:name e) (assoc (:name e) {:columns columns})) (conj blocks {:pine expression :name (:name e)}))))))))

(defn- script-statement
  "The statement of a script that converts, and warnings for what is left
  out. BEGIN, COMMIT and ROLLBACK are dropped. Of the rest, the one that
  changes data converts, or else the first."
  [sql]
  (let [pieces (map (fn [{:keys [start end] :as p}]
                      (let [text (subs sql start end)]
                        (assoc p :tx (sp/transaction-word text) :kind (sp/statement-kind text))))
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
                  [(str "Left out " (s/join " and " (distinct (map (comp s/upper-case :tx) tx)))
                        ". Pine runs each update! and delete! in a transaction of its own.")])
                (for [p data :when (not= p chosen)]
                  (str "Left out the statement on line " (line p) ". Pine converts one statement at a time.")))}))

(defn- render-blocks
  "The blocks laid out one operation per line, blank lines between them, or
  an internal error when one doesn't read back as Pine."
  [blocks warnings]
  (let [text (fn [{:keys [pine name]}] (str pine (when name (str " |= " name))))
        checked (map (fn [b] [(text b) (try (pine/parse (text b)) (catch Exception e {:error (ex-message e)}))]) blocks)]
    (if-let [[b {:keys [error]}] (first (filter #(:error (second %)) checked))]
      {:error (str "The SQL converted to Pine that doesn't read back. This is a bug in the converter.\n\n" b "\n\n" error)
       :error-type "internal"}
      {:expression (s/join "\n\n" (map (fn [{:keys [pine name]}]
                                         (str (or (:result (pine/prettify pine)) pine) (when name (str " |= " name))))
                                       blocks))
       :warnings (vec (distinct warnings))})))

(defn- convert [sql]
  (try
    (let [{:keys [statement warnings]} (script-statement sql)
          text (if statement (sp/mask sql (:start statement) (:end statement)) sql)
          {:keys [result error error-type]} (sp/parse text)]
      (if error
        {:error error :error-type error-type}
        (let [found (atom (vec warnings))
              blocks (transpile-statement result #(swap! found conj %))]
          (render-blocks blocks @found))))
    (catch clojure.lang.ExceptionInfo e
      (if (::refusal (ex-data e))
        {:error (str "Line " (sp/line-of sql (:at (ex-data e))) ": " (ex-message e))
         :error-type "sql-unsupported"}
        (throw e)))))

(defn sql->pine
  "{:expression :warnings} for one SQL SELECT, UPDATE or DELETE, or
  {:error :error-type}. Each WITH query is a block of its own, ending in
  `|= name`, with blank lines between the blocks.

  error-type is sql-parse-error when the text isn't SQL this reads,
  sql-unsupported when Pine can't say it, and internal when the Pine
  produced doesn't parse, which is a bug here, not in the SQL.

  `references` is the connection's indexed schema, or nil. With it, a bare
  name takes the database's spelling."
  ([sql] (sql->pine sql nil))
  ([sql references]
   (binding [*schema* references]
     (convert sql))))
