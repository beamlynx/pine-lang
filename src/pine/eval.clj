(ns pine.eval
  (:require
   [clojure.string :as s]
   [pine.access-policy :as access-policy]
   [pine.db.connections :as connections]
   [pine.db.main :as db]))

(def ^:dynamic *dialect*
  "Which SQL dialect build-query is rendering for - bound once at the top of
  build-query from the state's :connection-id, then read by every pure
  string-builder below it in the same call tree (q, casts, date bucketing,
  the action-subquery wrap). Defaults to :postgres so eval/q and friends
  stay usable standalone, outside any build-query call, exactly as before
  dialects existed."
  :postgres)

(defn q
  ([a b]
   (if a (str (q a) "." (q b)) (q b)))
  ([a]
   (let [quote-char (if (= *dialect* :mysql) "`" "\"")]
     (str quote-char a quote-char))))

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

(defn- col-fn-expr
  "Render a date-bucketing expression for col-fn (e.g. `select: created_at =>
  month`). MySQL's shape isn't just a different format string like
  Postgres's TO_CHAR(DATE_TRUNC(...)) - week in particular needs its own
  DATE_SUB/WEEKDAY expression, since MySQL has no DATE_TRUNC. WEEKDAY()
  returns 0 for Monday, matching Postgres's DATE_TRUNC('week') boundary."
  [col-fn col-ref]
  (case *dialect*
    :mysql (case col-fn
             "year"   (str "DATE_FORMAT(" col-ref ", '%Y')")
             "month"  (str "DATE_FORMAT(" col-ref ", '%Y-%m')")
             "day"    (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d')")
             "week"   (str "DATE_FORMAT(DATE_SUB(" col-ref ", INTERVAL WEEKDAY(" col-ref ") DAY), '%Y-%m-%d')")
             "hour"   (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d %H')")
             "minute" (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d %H:%i')"))
    (str "TO_CHAR(DATE_TRUNC('" col-fn "', " col-ref "), '" (col-fn-format col-fn) "')")))

(def ^:private mysql-cast-types
  "Translation table for a user's explicit `::cast` (and the internal
  ::text cast used to reconcile a mismatched heuristic join) into MySQL's
  CAST() target vocabulary, which is closed - CAST(x AS TEXT) is a hard
  syntax error, unlike Postgres's x::text. Anything not listed here is
  uppercased and passed through, same \"trust the user\" behavior Postgres
  already has for casts it doesn't specifically know about."
  {"text" "CHAR" "varchar" "CHAR" "char" "CHAR"
   "json" "JSON" "jsonb" "JSON"
   "uuid" "CHAR(36)"
   "timestamp" "DATETIME" "datetime" "DATETIME"
   "int" "SIGNED" "integer" "SIGNED" "bigint" "SIGNED" "smallint" "SIGNED"
   "numeric" "DECIMAL" "decimal" "DECIMAL"
   "bool" "SIGNED" "boolean" "SIGNED"})

(defn- render-cast
  "Apply an explicit cast to an already-rendered expression, dialect-aware:
  Postgres's `expr::cast` suffix vs. MySQL's `CAST(expr AS TYPE)` wrapper."
  [expr cast]
  (case *dialect*
    :mysql (str "CAST(" expr " AS " (get mysql-cast-types (s/lower-case cast) (s/upper-case cast)) ")")
    (str expr "::" cast)))

(defn- column-ref-with-cast
  "A column reference, optionally cast - the WHERE-clause column side."
  [alias column cast]
  (let [ref (q alias column)]
    (if cast (render-cast ref cast) ref)))

(defn- auto-cast-placeholder
  "The `?` placeholder for a value whose column type demands an implicit
  cast pine adds on the user's behalf (as opposed to an explicit `::cast`
  the user wrote, which is rendered on the column side instead - see
  render-cast). Kept deliberately minimal for MySQL: :jsonb needs
  CAST(? AS JSON), but :uuid and :date render as a bare `?` - MySQL has no
  UUID type (the value's already a string) and Connector/J binds dates
  correctly without help, so every unneeded CAST is just a new
  syntax-error surface for no benefit."
  [value-type]
  (case *dialect*
    :mysql (case value-type :jsonb "CAST(? AS JSON)" "?")
    (case value-type :jsonb "?::jsonb" :uuid "?::uuid" :date "?::timestamp" "?")))

(defn- render-operator
  "MySQL has no ILIKE/NOT ILIKE - map to the closest MySQL equivalent."
  [operator]
  (if (= *dialect* :mysql)
    (case operator "ILIKE" "LIKE" "NOT ILIKE" "NOT LIKE" operator)
    operator))

(defn- in-subquery
  "MySQL rejects `<verb> <target> ... WHERE id IN ( SELECT ... FROM <same
  target> ... )` on two counts: ER_UPDATE_TABLE_USED (1093) - can't select
  from the update/delete target in its own subquery - and error 1235,
  LIMIT isn't allowed inside an IN subquery (reachable whenever a user
  writes `| limit: N | delete!`). Wrapping the inner SELECT in a derived
  table sidesteps both. Identity for Postgres. Both inner selects are
  already single-column by construction, so SELECT * here is safe."
  [sql]
  (case *dialect*
    :mysql (str "SELECT * FROM ( " sql " ) AS " (q "pine_sub"))
    sql))

(defn- join-column-ref
  "Render a join column, casting to text when the two sides of a heuristic
  join (a naming-convention guess, not a real FK) turn out to have
  different DB types - e.g. one side stored as varchar, the other as uuid.
  Real FK joins are never cast: the constraint already guarantees the types
  line up, so casting would just throw away index usage."
  [cast alias column]
  (let [ref (q alias column)]
    (if cast (render-cast ref cast) ref)))

(defn- build-on-clause
  "The ON condition of one join: every column pair the relation carries,
  ANDed together. A key made of several columns is simply a longer list
  here - this does not care how many there are.

  An unresolved join (nothing connects the two tables, so :columns is
  empty) renders no condition at all. That query is broken either way; it
  used to compare two zero-length identifiers instead. Rejecting it
  outright, with an error naming the tables, is a separate change."
  [{:keys [from to columns cast]}]
  (when (seq columns)
    (str " ON " (s/join " AND "
                        (map (fn [{from-column :from to-column :to}]
                               (str (join-column-ref cast from from-column)
                                    " = " (join-column-ref cast to to-column)))
                             columns)))))

(defn- build-join-clause [{:keys [tables joins aliases]}]
  (when (not-empty (rest tables))
    (let [join-statements (map (fn [{:keys [to type] :as join}]
                                 (let [{to-table :table to-schema :schema} (get aliases to)
                                       join-keyword (if type (str type " JOIN") "JOIN")]
                                   (str join-keyword " " (q to-schema to-table) " AS " (q to)
                                        (build-on-clause join))))
                               ;; (reverse joins)
                               joins)]
      (s/join " " join-statements))))

(defn- build-columns-clause [{:keys [operation columns current] :as state}]
  (let [type (-> operation :type)
        rules (:access-policy state)
        restricted? (seq rules)
        ;; An explicit `select: alias.*` is just as opaque to per-column
        ;; redaction as the implicit current-table `.*` handled below -
        ;; expand it first so the rest of this function only ever sees real
        ;; columns (or, for a variable/CTE alias, is left untouched).
        ;; vec, not mapcat's lazy seq: the later `(into columns expanded)`
        ;; below relies on conj appending (vector semantics) - conj on a
        ;; plain seq prepends instead, which would silently put every
        ;; expanded/auto-id column in reverse order.
        columns (if restricted?
                  (vec (mapcat #(access-policy/expand-explicit-star state %) columns))
                  columns)
        ;; Separate auto-ID columns from user-selected columns
        {auto-id-columns true user-columns nil} (group-by #(:auto-id %) columns)
        ;; Check if any non-auto-ID columns are selected for the current table
        current-table-has-columns? (some #(= (:alias %) current) user-columns)
        star-eligible? (not (contains? #{:select :delete-action :group} type))
        ;; Under the access policy, a bare `current.*` is opaque to the
        ;; per-column check redaction depends on - expand it into an
        ;; explicit column list (pine.access-policy/expand-star) so each one
        ;; can be checked and redacted individually. Policy off: unchanged.
        expanded (when (and restricted? star-eligible? (not current-table-has-columns?))
                   (access-policy/expand-star state current))
        columns (if (seq expanded) (into columns expanded) columns)
        current-table-has-columns? (or current-table-has-columns? (seq expanded))
        select-all (cond
                     (not star-eligible?) ""
                     current-table-has-columns? ""  ; Don't add .* if current table has explicit columns
                     :else (str (if (seq columns) ", " "") (q current) ".*"))]
    (str
     "SELECT "
     (s/join
      ", "
      (map (fn [{:keys [column alias column-alias symbol auto-id col-fn] :as col}]
             (let [redact? (and restricted? (access-policy/sensitive-column? state rules col))
                   c (cond
                       redact? access-policy/redacted-sql-literal
                       ;; Auto-ID columns should render as unquoted id
                       auto-id (str (q alias) ".id")
                       ;; Column function (currently date functions)
                       col-fn (col-fn-expr col-fn (q alias column))
                       ;; Symbol-based columns (like aggregates)
                       (empty? column) (if alias (str (q alias) "." symbol) symbol)
                       ;; Regular columns
                       :else (q alias column))
                   ;; A redacted literal loses Postgres' automatic naming of a
                   ;; bare column reference, so name it explicitly whenever no
                   ;; explicit column-alias was already going to do that job.
                   out-alias (or column-alias (when redact? (if (empty? column) symbol column)))]
               (if out-alias (str c " AS " (q out-alias)) c))) columns))
     select-all
     " FROM")))

(defn- build-order-clause [{:keys [order]}]
  (if (empty? order) nil
      (str
       "ORDER BY "
       (s/join
        ", "
        (map (fn [{:keys [alias column direction]}]
               (str (q alias column) " " direction)) order)))))

(defn- remove-symbols
  "Remove symbols or columns from a vector of values"
  [vs]
  (filter #(not (or (= (:type %) :symbol) (= (:type %) :column))) vs))

(defn- build-group-clause [{:keys [group]}]
  (if (empty? group) nil
      (str
       "GROUP BY "
       (s/join
        ", "
        ;; For each group column, determine the appropriate reference
        (map (fn [{:keys [alias column column-alias col-fn]}]
               (if col-fn
                 ;; Use the column alias for columns with functions applied
                 (q column-alias)
                 ;; Use the full qualified column for regular columns
                 (q alias column)))
             group)))))

(defn- render-condition [[alias col cast operator value]]
  (if (or (= operator "IN") (= operator "NOT IN"))
    (str (q alias col) " " (render-operator operator) " (" (s/join ", " (repeat (count value) "?")) ")")
    (str (column-ref-with-cast alias col cast) " " (render-operator operator) " "
         (cond
           (= (:type value) :symbol) (:value value)
           (= (:type value) :column) (let [[a col] (:value value)] (q a col))
           ;; Cast the parameter/value, not the column (unless explicit cast)
           :else (if cast "?" (auto-cast-placeholder (:type value)))))))

(defn- build-where-clause [where]
  (when (not-empty where)
    (str "WHERE "
         (s/join " AND "
                 (for [entry where]
                   ;; A {:or [...]} entry is the comma-separated conditions from one
                   ;; where: segment -- render as a single parenthesized OR group.
                   (if-let [conditions (:or entry)]
                     (str "(" (s/join " OR " (map render-condition conditions)) ")")
                     (render-condition entry)))))))

(defn- where-condition-values
  "Flat [value ...] seq for one :where entry, whether a plain condition or an
  {:or [...]} group -- each value is still dt-typed (a map or, for IN/NOT IN, a
  collection of maps), matching what remove-symbols/flatten below expect."
  [entry]
  (if-let [conditions (:or entry)]
    (map #(nth % 4) conditions)
    [(nth entry 4)]))

(defn- where-params [where]
  (when (not-empty where)
    (->> where
         (mapcat where-condition-values)
         (map #(if (coll? %) % [%]))
         remove-symbols
         flatten)))

(defn- build-bare-select [state]
  (let [{:keys [tables _columns limit where aliases]} state
        from         (let [{a :alias} (first tables)
                           {table :table schema :schema} (get aliases a)]
                       (str (q schema table) " AS " (q a)))
        join         (build-join-clause state)
        select       (build-columns-clause state)
        where-clause (build-where-clause where)
        group (build-group-clause state)
        order (build-order-clause state)
        limit (when limit (str "LIMIT " limit))
        query (s/join " " (filter some? [select from join where-clause group order limit]))
        params (where-params where)]

    {:query query :params params}))

(defn- build-cte-body
  "Generate the inner SQL for a variable's AST used as a CTE.
  When the current table has no explicit user columns, .* is added and already
  includes id — so the auto-id column is dropped to avoid duplicate id names.
  When explicit columns are present (no .*), the auto-id is kept but its alias
  is stripped so id is accessible for join conditions.
  Returns {:query ... :params ...}."
  [ast]
  (let [current-alias    (:current ast)
        user-columns     (remove :auto-id (:columns ast))
        has-explicit?    (some #(= (:alias %) current-alias) user-columns)
        columns          (keep (fn [col]
                                 (if (and (:auto-id col) (= (:alias col) current-alias))
                                   (when has-explicit? (dissoc col :column-alias))
                                   col))
                               (:columns ast))]
    (build-bare-select (assoc ast :columns columns))))

(defn- collect-ctes
  "Recursively collect [name query params] triples from variable tables in
  topological order (deepest dependencies first). Deduplicates by name."
  [tables aliases]
  (->> tables
       (mapcat (fn [{:keys [alias]}]
                 (let [entry (get aliases alias)]
                   (when-let [ast (:ast entry)]
                     (let [var-name    (:table entry)
                           nested-ctes (collect-ctes (:tables ast) (:aliases ast))
                           {:keys [query params]} (build-cte-body ast)]
                       (conj nested-ctes [var-name query params]))))))
       (reduce (fn [[seen acc] [name _ _ :as cte]]
                 (if (contains? seen name)
                   [seen acc]
                   [(conj seen name) (conj acc cte)]))
               [#{} []])
       second))

(defn build-select-query [state]
  (let [ctes        (collect-ctes (:tables state) (:aliases state))
        result      (build-bare-select state)
        cte-params  (mapcat #(nth % 2 nil) ctes)
        cte-prefix  (when (seq ctes)
                      (str "WITH "
                           (s/join ", " (map (fn [[name body _]]
                                               (str (q name) " AS ( " body " )"))
                                             ctes))
                           " "))]
    (-> result
        (update :query  #(str cte-prefix %))
        (update :params #(seq (concat cte-params %))))))

(defn build-count-query [state]
  (let [{:keys [query params]} (build-select-query state)]
    {:query (str "WITH x AS ( " query " ) SELECT COUNT(*) FROM x")
     :params params}))

(defn- build-inner-select-for-group
  "Build the inner SELECT for a GROUP query CTE. Includes non-aggregate columns only."
  [state]
  (let [{:keys [tables columns where aliases joins]} state
        {a :alias} (first tables)
        {table :table schema :schema} (get aliases a)
        ;; Filter out aggregate function columns (those with :symbol but no :column)
        non-aggregate-cols (filter #(or (:column %) (:auto-id %)) columns)
        ;; Create a temporary state for building the SELECT clause with only non-aggregate columns
        temp-state (assoc state
                          :columns non-aggregate-cols
                          :operation {:type :group})
        ;; Build SELECT clause using the same logic as regular queries, but add aliases to all columns
        rules (:access-policy state)
        restricted? (seq rules)
        select-parts (s/join
                      ", "
                      (map (fn [{:keys [column alias column-alias symbol auto-id col-fn] :as col}]
                             (let [redact? (and restricted? (access-policy/sensitive-column? state rules col))
                                   c (cond
                                       redact? access-policy/redacted-sql-literal
                                       ;; Auto-ID columns should render as unquoted id
                                       auto-id (str (q alias) ".id")
                                       ;; Column function (currently date functions)
                                       col-fn (col-fn-expr col-fn (q alias column))
                                       ;; Regular columns
                                       :else (q alias column))
                                   ;; Always use an alias: either column-alias or column name
                                   col-alias (or column-alias column)]
                               (str c " AS " (q col-alias))))
                           non-aggregate-cols))
        select-clause (str "SELECT " select-parts)
        from (str "FROM " (q schema table) " AS " (q a))
        join (build-join-clause {:tables tables :joins joins :aliases aliases})
        where-clause (build-where-clause where)]
    (s/join " " (filter some? [select-clause from join where-clause]))))

(defn- build-outer-select-for-group
  "Build the outer SELECT for a GROUP query. References CTE columns and includes aggregates."
  [cte-alias {:keys [columns group]}]
  (let [;; Get group columns - use column-alias if present, otherwise column name
        group-cols (map #(or (:column-alias %) (:column %)) group)
        select-items (map (fn [{:keys [column column-alias symbol col-fn]}]
                            (cond
                             ;; Aggregate function (has symbol, no column)
                              (and symbol (empty? column))
                              (if column-alias
                                (str symbol " AS " (q column-alias))
                                symbol)
                             ;; Non-aggregate column - reference from CTE
                             ;; Use the same alias that was assigned in the inner query
                              :else (q cte-alias (or column-alias column))))
                          columns)
        group-by (str "GROUP BY " (s/join ", " (map #(q cte-alias %) group-cols)))]
    {:select (str "SELECT " (s/join ", " select-items) " FROM " (q cte-alias))
     :group-by group-by}))

(defn build-group-query [state]
  (let [{:keys [index tables aliases]} state
        ;; A checkpoint/variable feeding into a terminal GROUP (e.g. `|= x |
        ;; g: ...`) needs its own CTE emitted too -- this path used to skip
        ;; collect-ctes entirely (unlike build-select-query, which already
        ;; calls it), so the user-named CTE was never defined and the group's
        ;; wrapper CTE (below) referenced it as a dangling bare relation.
        ctes        (collect-ctes tables aliases)
        cte-params  (mapcat #(nth % 2 nil) ctes)
        cte-prefix  (when (seq ctes)
                      (str (s/join ", " (map (fn [[name body _]]
                                               (str (q name) " AS ( " body " )"))
                                             ctes))
                           ", "))
        cte-alias (str "x_" index)
        ;; Build inner query (base SELECT with non-aggregate columns)
        inner-query (build-inner-select-for-group state)
        ;; Build outer query (SELECT from CTE with aggregates and GROUP BY)
        {:keys [select group-by]} (build-outer-select-for-group cte-alias state)
        ;; Combine into CTE
        query (str "WITH " cte-prefix (q cte-alias) " AS ( " inner-query " ) " select " " group-by)
        ;; Extract params from WHERE clause
        params (where-params (:where state))]
    {:query query :params (seq (concat cte-params params))}))

(defn build-delete-query [state]
  (let [{:keys [delete current aliases]} state
        {table :table schema :schema}     (get aliases current)
        {:keys [column]}                  delete
        state                             (assoc state :columns [{:column column :alias current}])
        {:keys [query params]}            (build-select-query state)]
    {:query (str "DELETE FROM " (q schema table) " WHERE " (q column) " IN ( "  (in-subquery query) " )")
     :params params}))

(defn- build-single-update-query [state update-alias assignments]
  (let [{:keys [aliases]}              state
        {table :table schema :schema}  (get aliases update-alias)
        set-clause (s/join ", "
                           (map (fn [{:keys [column value]}]
                                  (let [{:keys [alias column]} column]
                                    (str (q column) " = " (cond
                                                            (= (:type value) :symbol) (:value value)
                                                            (= (:type value) :column) (let [{:keys [alias column]} value] (q alias column))
                                                            :else (auto-cast-placeholder (:type value))))))
                                assignments))
        state-for-subquery (-> state
                               (assoc :columns [{:column "id" :alias update-alias}])
                               (assoc :operation {:type :select :value nil}))
        {:keys [query params]} (build-select-query state-for-subquery)
        update-params (->> assignments
                           (map :value)
                           (filter #(not (or (= (:type %) :symbol) (= (:type %) :column)))))]
    {:table (if schema (str schema "." table) table)
     :query (str "UPDATE " (q schema table) " SET " set-clause " WHERE id IN ( " (in-subquery query) " )")
     :params (concat update-params params)}))

(defn build-update-queries [state]
  "Returns a list of {:table table-name :query query :params params}, one per table being updated."
  (let [{:keys [update current aliases]} state
        {:keys [assignments]}             update
        ;; Group assignments by table alias (use current when column has no alias)
        grouped (group-by (fn [{:keys [column]}]
                            (or (:alias column) current))
                          assignments)]
    (mapv (fn [[update-alias table-assignments]]
            (build-single-update-query state update-alias table-assignments))
          grouped)))

(defn build-query [state]
  (binding [*dialect* (connections/get-dialect (:connection-id state))]
    (let [{:keys [type]} (state :operation)]
      (cond
        (let [cur (-> state :current)]
          (or (nil? cur)
              (= "" (get-in state [:aliases cur :table])))) {:query "" :params nil}
        (= type :delete-action) (build-delete-query state)
        (= type :update-action) {:queries (build-update-queries state)}
        (= type :update-partial) {:queries (build-update-queries state)}
        (= type :count) (build-count-query state)
        (= type :group) (build-group-query state)
        ;; :paths only generates candidate pine expressions (see hints.paths) -
        ;; it never builds a query of its own.
        (= type :paths) {:query " /* No SQL. Pick a path from hints.paths and build that expression instead */ "}
        :else (build-select-query (update state :limit #(or % 250)))))))

(defn formatted-query [build-result]
  (let [replacer (fn [s param]
                   (let [v (:value param)
                         param-str (if (= (:type param) :boolean)
                                     (str v)
                                     (str "'" v "'"))]
                     (clojure.string/replace-first s #"\?" param-str)))]
    (if-let [queries (:queries build-result)]
      ;; Multiple update queries
      (s/join "\n" (map (fn [{:keys [query params]}]
                          (if (empty? query) "" (str (reduce replacer query params) ";")))
                        queries))
      ;; Single query (legacy format or other operations)
      (let [{:keys [query params]} build-result]
        (if (empty? query) "" (str "\n" (reduce replacer query params) ";\n"))))))

(defn run-query [state]
  (if (= (-> state :operation :type) :no-op)
    [["No operation"] ["-"]]
    (let [connection-id (state :connection-id)
          build-result  (build-query state)
          operation-type (-> state :operation :type)]
      (cond
        (contains? #{:update-action :update-partial} operation-type)
        ;; Run update queries; use transaction when multiple tables to rollback all on failure
        (let [queries (or (:queries build-result)
                          [{:table nil :query (:query build-result) :params (:params build-result)}])
              results (if (> (count queries) 1)
                        (db/run-action-queries-in-transaction connection-id queries)
                        (mapv (fn [{:keys [table query params]}]
                                (let [affected (db/run-action-query connection-id {:query query :params params})]
                                  [(or table "table") affected]))
                              queries))]
          (into [["Table" "Rows updated"]]
                (map (fn [[t n]] [t n]) results)))

        (contains? #{:delete-action} operation-type)
        (let [{:keys [query params]} build-result
              affected-rows (db/run-action-query connection-id {:query query :params params})]
          [["Rows deleted"] [affected-rows]])

        :else
        ;; Select and other operations
        (db/run-query connection-id (select-keys build-result [:query :params]))))))
