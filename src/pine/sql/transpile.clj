(ns pine.sql.transpile
  "One SQL SELECT to a Pine expression that returns the same rows.

  `sql->pine` converts only what Pine can say exactly. Anything else is
  refused with a message naming the construct and its line, never guessed
  at. See docs/sql-to-pine.md.

  The steps, in order:

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

(defn- db-table
  "The table's schema, name and columns as the database spells them, or {}
  without a schema."
  [{:keys [schema table quoted schema-quoted at]}]
  (if-not *schema*
    {}
    (let [schema (if (and schema (not schema-quoted)) (spelling schema (keys (:schema *schema*)) at) schema)
          tables (if schema (get-in *schema* [:schema schema :table]) (:table *schema*))
          table (if quoted table (spelling table (keys tables) at))]
      {:db-schema schema :db-table table :db-columns (map :column (get-in tables [table :columns]))})))

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
    :in (let [vs (mapv value (:values p))]
          (when-not (every? #(#{:string :number} (:kind %)) vs)
            (refuse (:at p) "An IN list in Pine holds strings and numbers only."))
          {:col (left-column tables p)
           :cast (column-cast (:left p))
           :operator (if (not= (:negated p) (boolean negate)) "not in" "in")
           :value vs})
    :truthy {:col (resolve-column tables (:column p))
             :operator "="
             :value {:kind :boolean :value (not negate)}}))

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
                 (warn! (str "COUNT(*) AS " as ": Pine names the count column `count`.")))
        :column (do (when-not (group-set (resolve-column tables column))
                      (refuse at (str "`" (:name column) "` is selected but not grouped. Pine selects only the grouped columns and the count.")))
                    (when (and as (not= as (:name column)))
                      (warn! (str (:name column) " AS " as ": a grouped column keeps its own name in Pine."))))
        (refuse at "Only grouped columns and COUNT(*) can be selected with GROUP BY.")))
    (when-not (some #(= :count (:kind %)) items)
      (warn! "Pine adds a count column to a group:."))
    (let [selected (set (for [i items :when (= :column (:kind i))] (resolve-column tables (:column i))))]
      (when (some (complement selected) group-cols)
        (warn! "Pine returns every grouped column, including ones the SELECT left out.")))
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

(defn- plan-plain [tables current items {:keys [order limit]}]
  (let [stars (fn [] (mapv (fn [t] {:star t}) (range (count tables))))
        expanded (vec (mapcat (fn [{:keys [kind qualifier column as at]}]
                                (case kind
                                  :star (stars)
                                  :alias-star [{:star (:t (resolve-column tables {:qualifier qualifier :name "x" :at at}))}]
                                  :column [(cond-> {:col (resolve-column tables column)}
                                             as (assoc :as (pine-name! as at)))]))
                              items))]
    (cond-> []
      (not= expanded [{:star current}])
      (conj {:op :select :items expanded})
      (seq order)
      (conj {:op :order
             :items (mapv (fn [{:keys [target direction at]}]
                            (let [aliased (order-target items target)]
                              (case (:kind target)
                                :count (refuse at "ORDER BY COUNT(*) needs GROUP BY.")
                                :position (refuse at "ORDER BY a position isn't in Pine. Name the column.")
                                :column {:col (resolve-column tables (:column (or aliased target)))
                                         :direction (sort-word direction)})))
                          order)})
      limit (conj {:op :limit :n (:value limit)}))))

(defn- plan-select [tables current {:keys [select group order limit] :as stmt} warn!]
  (let [hidden (filter hidden-column? select)
        items (vec (remove hidden-column? select))]
    (when (seq hidden)
      (warn! (str "Left out " (s/join ", " (map :as hidden)) ": Pine adds its hidden columns itself.")))
    (when (empty? items)
      (refuse (:at (first select)) "Nothing is left to select."))
    (when (and limit (> (:value limit) pine/max-limit))
      (refuse (:at limit) (str "LIMIT " (:value limit) " is more than Pine's " pine/max-limit ".")))
    (cond
      (seq group)
      (plan-grouped tables items stmt warn!)

      (some #(= :count (:kind %)) items)
      (do (when (next items)
            (refuse (:at (first (remove #(= :count (:kind %)) items))) "A column next to COUNT(*) needs GROUP BY."))
          (when (seq order) (refuse (:at (first order)) "ORDER BY on a single COUNT(*) isn't converted. Remove it."))
          (when limit (refuse (:at limit) "LIMIT on a single COUNT(*) isn't converted. Remove it."))
          (when (and (:as (first items)) (not= "count" (:as (first items))))
            (warn! (str "COUNT(*) AS " (:as (first items)) ": Pine names the count column `count`.")))
          [{:op :count}])

      :else
      (plan-plain tables current items stmt))))

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
         :limit (str "limit: " n))))))

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

(defn- transpile [stmt]
  (let [warnings (atom [])
        warn! #(swap! warnings conj %)
        {:keys [tables moved]} (plan-tables stmt)
        {:keys [ops current]} (table-ops tables)
        _ (when (seq moved)
            (warn! "A condition in an inner join's ON moved to where:. It means the same there."))
        clauses (plan-where tables (cond-> moved (:where stmt) (conj (:where stmt))))
        ops (-> ops
                (into (map (fn [c] {:op :where :clause c}) clauses))
                (into (plan-select tables current stmt warn!)))]
    {:expression (render tables ops current)
     :warnings (vec (distinct @warnings))}))

(defn- convert [sql]
  (let [{:keys [result error error-type]} (sp/parse sql)]
    (if error
      {:error error :error-type error-type}
      (try
        (let [{:keys [expression warnings]} (transpile result)
              pretty (pine/prettify expression)
              checked (try (pine/parse expression) (catch Exception e {:error (ex-message e)}))]
          (if (:error checked)
            {:error (str "The SQL converted to Pine that doesn't read back. This is a bug in the converter.\n\n"
                         expression "\n\n" (:error checked))
             :error-type "internal"}
            {:expression (or (:result pretty) expression) :warnings warnings}))
        (catch clojure.lang.ExceptionInfo e
          (if (::refusal (ex-data e))
            {:error (str "Line " (sp/line-of sql (:at (ex-data e))) ": " (ex-message e))
             :error-type "sql-unsupported"}
            (throw e)))))))

(defn sql->pine
  "{:expression :warnings} for one SQL SELECT, or {:error :error-type}.

  error-type is sql-parse-error when the text isn't SQL this reads,
  sql-unsupported when Pine can't say it, and internal when the Pine
  produced doesn't parse, which is a bug here, not in the SQL.

  `references` is the connection's indexed schema, or nil. With it, a bare
  name takes the database's spelling."
  ([sql] (sql->pine sql nil))
  ([sql references]
   (binding [*schema* references]
     (convert sql))))
